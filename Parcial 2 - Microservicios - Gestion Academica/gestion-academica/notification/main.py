"""Notification Service: solo escucha eventos. No lo llama nadie, así que nadie depende de él."""
import logging
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException

from common.correlation import install as install_correlation, setup_logging
from common.db import get_db
from common.errors import install_error_handlers
from common.events import CONSUMERS, start_consumer

setup_logging()  # como basicConfig, pero cada línea lleva [cid=...]
log = logging.getLogger("notification")

DB = os.getenv("DB_PATH", "/data/notification.db")

TEMPLATES = {
    "matricula.creada": "Tu solicitud de matrícula en {course_name} fue registrada. Tienes un pago pendiente de ${amount}.",
    "pago.confirmado": "Recibimos tu pago de ${amount} para {course_name}.",
    "pago.fallido": "Tu pago para {course_name} no pudo procesarse. La matrícula será cancelada.",
    "pago.reembolsado": "Tu matrícula en {course_name} se canceló antes de confirmar tu pago. Te reembolsamos ${amount}.",
    "matricula.confirmada": "¡Matrícula confirmada en {course_name}!",
    "matricula.cancelada": "Tu matrícula en {course_name} fue cancelada. Motivo: {reason}.",
}


class _Safe(dict):
    def __missing__(self, key):
        return "?"


def init_db():
    os.makedirs(os.path.dirname(DB), exist_ok=True)
    with get_db(DB) as c:
        c.execute(
            "CREATE TABLE IF NOT EXISTS notifications ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT NOT NULL, event TEXT NOT NULL, "
            "message TEXT NOT NULL, created_at REAL NOT NULL)"
        )
        # migración: event_id permite descartar eventos repetidos (la entrega es "al menos una vez")
        cols = {r["name"] for r in c.execute("PRAGMA table_info(notifications)")}
        if "event_id" not in cols:
            c.execute("ALTER TABLE notifications ADD COLUMN event_id TEXT")
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_notifications_event ON notifications(event_id)")


def handle_event(routing_key: str, payload: dict):
    template = TEMPLATES.get(routing_key)
    user_id = payload.get("student_id")
    if not template or not user_id:
        return
    data = dict(payload)
    if isinstance(data.get("amount"), (int, float)):
        data["amount"] = format(data["amount"], ",.0f").replace(",", ".")  # 1.500.000
    message = template.format_map(_Safe(data))
    with get_db(DB) as c:
        # INSERT OR IGNORE + índice único en event_id: si el mismo evento llega dos veces, el aviso
        # no se duplica (Idempotent Consumer). Eventos sin event_id (versiones viejas) se guardan siempre.
        inserted = c.execute(
            "INSERT OR IGNORE INTO notifications(user_id, event, message, created_at, event_id) "
            "VALUES (?, ?, ?, ?, ?)",
            (str(user_id), routing_key, message, time.time(), payload.get("event_id")),
        ).rowcount
    if inserted:
        log.info("[correo/push simulado] usuario %s: %s", user_id, message)
    else:
        log.info("Evento repetido %s ignorado", payload.get("event_id"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    start_consumer("notification.q", ["matricula.*", "pago.*"], handle_event)
    yield


app = FastAPI(title="Notification Service", lifespan=lifespan)
install_correlation(app, "notification")  # Correlation ID (libro, p. 223)
install_error_handlers(app)


@app.get("/health")
def health():
    return {"service": "notification", "status": "ok", "consumers": dict(CONSUMERS)}


@app.get("/notifications")
def list_notifications(x_user_id: str = Header(...), x_user_role: str = Header(...)):
    with get_db(DB) as c:
        if x_user_role == "admin":
            rows = c.execute("SELECT * FROM notifications ORDER BY id DESC LIMIT 200").fetchall()
        else:
            rows = c.execute(
                "SELECT * FROM notifications WHERE user_id = ? ORDER BY id DESC LIMIT 100", (x_user_id,)
            ).fetchall()
    return [dict(r) for r in rows]
