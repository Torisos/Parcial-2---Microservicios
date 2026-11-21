"""Mensajería con RabbitMQ: publicar, consumir y patrón Outbox.

- Un exchange 'topic' durable llamado 'academico'.
- Cada servicio consumidor declara su propia cola durable: si el servicio se cae,
  los mensajes esperan en el broker y se entregan al reiniciar.
- Outbox: el evento se guarda en la MISMA transacción que el cambio de datos y se
  publica después. Si el broker está caído, un hilo reintenta cada pocos segundos.
  La entrega es "al menos una vez": los consumidores deben ser idempotentes. Para eso cada
  evento lleva un 'event_id' único (servicio:id del outbox).
- Publisher confirms: un evento se marca como enviado solo cuando el broker confirma que lo recibió.
- Reintentos y Dead Letter Queue: si un handler falla, el mensaje se reintenta hasta MAX_RETRIES
  veces y después se aparta en la cola '<cola>.dlq' para revisarlo, en vez de reintentarse para
  siempre (mensaje envenenado). Un TransientError (una dependencia caída) se reintenta sin límite,
  porque no es culpa del mensaje.
"""
import json
import logging
import os
import threading
import time

import pika

from common import correlation
from common.db import get_db

log = logging.getLogger("events")
logging.getLogger("pika").setLevel(logging.WARNING)  # pika registra cada conexión en INFO: solo ruido

RABBIT_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBIT_USER = os.getenv("RABBITMQ_USER", "guest")
RABBIT_PASS = os.getenv("RABBITMQ_PASS", "guest")
EXCHANGE = "academico"
MAX_RETRIES = int(os.getenv("EVENT_MAX_RETRIES", "5"))
RETRY_DELAY = float(os.getenv("EVENT_RETRY_DELAY_SECONDS", "3"))
RETRY_HEADER = "x-retries"
ROUTING_HEADER = "x-original-routing-key"


# Estado de cada consumidor ('cola' -> True si está conectado y escuchando). Lo muestra /health.
CONSUMERS = {}

# Janitor: los eventos ya enviados se borran del outbox pasado este tiempo (7 días por defecto).
OUTBOX_RETENTION = float(os.getenv("OUTBOX_RETENTION_SECONDS", str(7 * 24 * 3600)))


class TransientError(Exception):
    """Fallo pasajero que no depende del mensaje (por ejemplo, un servicio caído).
    El mensaje se reencola sin gastar reintentos y nunca termina en la DLQ."""


def _connect():
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        credentials=pika.PlainCredentials(RABBIT_USER, RABBIT_PASS),
        heartbeat=30,
        connection_attempts=1,
        socket_timeout=3,
        blocked_connection_timeout=10,
    )
    return pika.BlockingConnection(params)


def _properties(event_id=None, headers=None):
    return pika.BasicProperties(
        delivery_mode=2, content_type="application/json", message_id=event_id, headers=headers or None
    )


class Publisher:
    """Una conexión y un canal con confirmaciones para publicar varios eventos seguidos.
    basic_publish lanza excepción si el broker no confirma el mensaje."""

    def __enter__(self):
        self.conn = _connect()
        self.ch = self.conn.channel()
        self.ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
        self.ch.confirm_delivery()
        return self

    def publish(self, routing_key, payload):
        self.ch.basic_publish(
            exchange=EXCHANGE,
            routing_key=routing_key,
            body=json.dumps(payload).encode(),
            properties=_properties(payload.get("event_id")),
        )

    def __exit__(self, *exc):
        try:
            self.conn.close()
        except Exception:
            pass
        return False


def publish(routing_key, payload):
    """Publica un evento suelto. Devuelve True si el broker confirmó que lo recibió."""
    try:
        with Publisher() as p:
            p.publish(routing_key, payload)
        return True
    except Exception as exc:  # broker caído, red, mensaje no confirmado...
        log.warning("No se pudo publicar '%s': %s", routing_key, exc)
        return False


def start_consumer(queue, binding_keys, handler):
    """Consume eventos en un hilo de fondo y se reconecta solo si el broker cae.

    handler(routing_key, payload). Si lanza TransientError, el mensaje se reencola sin límite.
    Si lanza otra excepción, se reintenta hasta MAX_RETRIES veces y luego va a '<queue>.dlq'.
    """
    dlq = f"{queue}.dlq"
    CONSUMERS[queue] = False

    def run():
        while True:
            try:
                conn = _connect()
                ch = conn.channel()
                ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
                ch.queue_declare(queue=queue, durable=True)
                ch.queue_declare(queue=dlq, durable=True)
                for key in binding_keys:
                    ch.queue_bind(queue=queue, exchange=EXCHANGE, routing_key=key)
                ch.basic_qos(prefetch_count=10)

                def on_message(channel, method, properties, body):
                    headers = dict(properties.headers or {})
                    # en un reintento el mensaje se reenvía directo a la cola: su routing key real va en la cabecera
                    routing_key = headers.get(ROUTING_HEADER, method.routing_key)
                    try:
                        payload = json.loads(body)
                    except ValueError:
                        log.error("Mensaje inválido enviado a %s: %r", dlq, body[:200])
                        channel.basic_publish("", dlq, body, _properties(properties.message_id, headers))
                        channel.basic_ack(method.delivery_tag)
                        return
                    # el handler trabaja con el Correlation ID de quien publicó el evento
                    token = correlation.set(payload.get("correlation_id") if isinstance(payload, dict) else None)
                    try:
                        log.info("Evento recibido: %s (%s)", routing_key,
                                 payload.get("event_id", "sin id") if isinstance(payload, dict) else "-")
                        handler(routing_key, payload)
                        channel.basic_ack(method.delivery_tag)
                    except TransientError as exc:
                        log.warning("%s: fallo pasajero (%s); se reintenta", routing_key, exc)
                        time.sleep(RETRY_DELAY)
                        channel.basic_nack(method.delivery_tag, requeue=True)
                    except Exception as exc:
                        retries = int(headers.get(RETRY_HEADER, 0)) + 1
                        headers[RETRY_HEADER] = retries
                        headers[ROUTING_HEADER] = routing_key
                        if retries > MAX_RETRIES:
                            log.error("%s falló %d veces (%s); se aparta en %s", routing_key, MAX_RETRIES, exc, dlq)
                            target = dlq
                        else:
                            log.warning("Handler falló para %s (%s); reintento %d de %d",
                                        routing_key, exc, retries, MAX_RETRIES)
                            time.sleep(RETRY_DELAY)
                            target = queue
                        # se publica la copia con el contador antes de confirmar el original:
                        # si algo falla en medio, el mensaje se duplica pero no se pierde
                        channel.basic_publish("", target, body, _properties(properties.message_id, headers))
                        channel.basic_ack(method.delivery_tag)
                    finally:
                        correlation.reset(token)

                ch.basic_consume(queue=queue, on_message_callback=on_message)
                log.info("Consumidor '%s' escuchando %s", queue, binding_keys)
                CONSUMERS[queue] = True
                ch.start_consuming()
            except Exception as exc:
                CONSUMERS[queue] = False
                log.warning("Consumidor '%s' sin conexión (%s); reintento en 5 s", queue, exc)
                time.sleep(5)

    threading.Thread(target=run, daemon=True, name=f"consumer-{queue}").start()


class Outbox:
    DDL = (
        "CREATE TABLE IF NOT EXISTS outbox ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, routing_key TEXT NOT NULL, "
        "payload TEXT NOT NULL, sent INTEGER NOT NULL DEFAULT 0)"
    )

    def __init__(self, db_path):
        self.db_path = db_path
        # prefijo del event_id: el nombre del servicio, tomado del archivo de su base de datos
        self.source = os.path.splitext(os.path.basename(db_path))[0]
        self._lock = threading.Lock()

    def init(self):
        with get_db(self.db_path) as c:
            c.execute(self.DDL)
            # migración: cuándo se envió cada evento, para que el Janitor sepa qué borrar
            cols = {r["name"] for r in c.execute("PRAGMA table_info(outbox)")}
            if "sent_at" not in cols:
                c.execute("ALTER TABLE outbox ADD COLUMN sent_at REAL")

    def stats(self):
        """Eventos pendientes de publicar y ya enviados (para /health y el Doctor Monkey)."""
        with get_db(self.db_path) as c:
            row = c.execute(
                "SELECT COALESCE(SUM(sent = 0), 0) AS pendientes, COALESCE(SUM(sent = 1), 0) AS enviados FROM outbox"
            ).fetchone()
        return {"pendientes": row["pendientes"], "enviados": row["enviados"]}

    def purge_sent(self, older_than_seconds=OUTBOX_RETENTION):
        """Janitor: borra los eventos YA ENVIADOS con más de 'older_than_seconds'. Los pendientes
        nunca se tocan. Sin esto el outbox crece para siempre. Devuelve cuántos borró."""
        cutoff = time.time() - older_than_seconds
        with get_db(self.db_path) as c:
            deleted = c.execute(
                "DELETE FROM outbox WHERE sent = 1 AND (sent_at IS NULL OR sent_at <= ?)", (cutoff,)
            ).rowcount
        if deleted:
            log.info("Janitor: %d eventos ya enviados borrados del outbox", deleted)
        return deleted

    @staticmethod
    def add(conn, routing_key, payload):
        """Guarda el evento usando la conexión (transacción) de quien llama.
        Le agrega el Correlation ID de la operación en curso, para seguir el flujo en los logs."""
        cid = correlation.get()
        if cid and "correlation_id" not in payload:
            payload = dict(payload, correlation_id=cid)
        conn.execute(
            "INSERT INTO outbox(routing_key, payload) VALUES (?, ?)",
            (routing_key, json.dumps(payload)),
        )

    def flush(self):
        """Publica los eventos pendientes en orden, con una sola conexión. Se detiene al primer fallo.
        La lectura y cada marca de 'enviado' son transacciones cortas: la base no queda bloqueada
        mientras se habla con el broker."""
        with self._lock:
            with get_db(self.db_path) as c:
                rows = c.execute(
                    "SELECT id, routing_key, payload FROM outbox WHERE sent = 0 ORDER BY id"
                ).fetchall()
            if not rows:
                return
            try:
                with Publisher() as p:
                    for row in rows:
                        payload = json.loads(row["payload"])
                        payload["event_id"] = f"{self.source}:{row['id']}"
                        p.publish(row["routing_key"], payload)
                        token = correlation.set(payload.get("correlation_id"))
                        log.info("Evento publicado y confirmado por RabbitMQ: %s (%s)", row["routing_key"], payload["event_id"])
                        correlation.reset(token)
                        with get_db(self.db_path) as c:
                            c.execute("UPDATE outbox SET sent = 1, sent_at = ? WHERE id = ?",
                                      (time.time(), row["id"]))
            except Exception as exc:  # broker caído: lo pendiente se reintenta en el próximo ciclo
                log.warning("Outbox: no se pudo publicar (%s); se reintenta luego", exc)

    def flush_async(self):
        threading.Thread(target=self._safe_flush, daemon=True).start()

    def _safe_flush(self):
        try:
            self.flush()
        except Exception as exc:
            log.warning("Error en flush del outbox: %s", exc)

    def start_flusher(self, interval=5.0, purge_every=3600.0):
        def loop():
            last_purge = 0.0
            while True:
                time.sleep(interval)
                self._safe_flush()
                if time.time() - last_purge >= purge_every:
                    last_purge = time.time()
                    try:
                        self.purge_sent()
                    except Exception as exc:
                        log.warning("Janitor del outbox falló: %s", exc)

        threading.Thread(target=loop, daemon=True, name="outbox-flusher").start()
