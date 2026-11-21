"""Las 6 categorías de Chaos Monkey del libro, aplicadas a la plataforma de gestión académica.

Libro: Pacheco, "Microservice Patterns and Best Practices", cap. 12, p. 304. Chaos Monkey tiene estas
categorías (la "Simian Army" de Netflix):

  gorilla      Chaos Gorilla: simula que se cae una zona de disponibilidad completa.
  conformity   Conformity Monkey: busca instancias que no siguen las buenas prácticas.
  doctor       Doctor Monkey: revisa la salud y el rendimiento de cada instancia.
  janitor      Janitor Monkey: busca recursos que ya no se usan y los limpia.
  latency      Latency Monkey: mete demoras artificiales en la comunicación cliente-servidor.
  security     Security Monkey: busca vulnerabilidades de seguridad y configuraciones inseguras.

scripts/chaos.py ya cubre el Chaos Monkey clásico (apagar un servicio a la vez). Este script agrega
las otras seis categorías.

Uso:
  python scripts/simian.py --todas                       las 6 categorías (8-12 minutos)
  python scripts/simian.py --categoria latency doctor    solo algunas
  python scripts/simian.py --listar

Requiere:  docker compose up -d --build  corriendo.
Salida:    resultados/simian_<fecha>.md
"""
import argparse
import json
import math
import os
import re
import statistics
import subprocess
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common_http as http  # noqa: E402
from chaos import Ctx, api, compose, field, wait_until  # noqa: E402
from fuzz import b64, b64d, forge  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVICES = ["gateway", "auth", "student", "teacher", "course", "enrollment", "payment", "notification"]
RECORDS = []      # comprobaciones: (categoría, paso, esperado, observado, ok)
FINDINGS = []     # hallazgos que no rompen la prueba pero hay que reportar: (categoría, severidad, texto)
CURRENT = {"cat": ""}

CATEGORY_INFO = {
    "gorilla": ("Chaos Gorilla", "Simula que se cae una zona de disponibilidad completa."),
    "conformity": ("Conformity Monkey", "Busca instancias que no siguen las buenas prácticas."),
    "doctor": ("Doctor Monkey", "Revisa la salud y el rendimiento de cada instancia."),
    "janitor": ("Janitor Monkey", "Busca recursos que ya no se usan y los limpia."),
    "latency": ("Latency Monkey", "Mete demoras artificiales en la comunicación cliente-servidor."),
    "security": ("Security Monkey", "Busca vulnerabilidades y configuraciones inseguras."),
}


# ---------------------------------------------------------------- utilidades
def rec(step, expected, observed, ok):
    RECORDS.append((CURRENT["cat"], step, expected, str(observed), bool(ok)))
    print("  [%s] %s\n        esperado: %s\n        observado: %s" % ("OK " if ok else "XX ", step, expected, observed),
          flush=True)


def finding(severity, text):
    FINDINGS.append((CURRENT["cat"], severity, text))
    print("  [hallazgo %s] %s" % (severity, text), flush=True)


def run(args, timeout=120):
    """Ejecuta 'docker ...' y devuelve (código, stdout) sin mezclar stderr (para leer JSON)."""
    try:
        p = subprocess.run(["docker"] + args, cwd=ROOT, capture_output=True, text=True,
                           encoding="utf-8", errors="replace", timeout=timeout)
        return p.returncode, p.stdout or ""
    except (OSError, subprocess.TimeoutExpired) as exc:
        return 1, str(exc)


def in_container(service, code, timeout=60):
    """Ejecuta código Python dentro del contenedor y devuelve la última línea que imprimió."""
    rc, out = compose("exec", "-T", service, "python", "-c", code, timeout=timeout)
    lines = [l for l in out.strip().splitlines() if l.strip()]
    return lines[-1] if lines else ""


def health(service):
    code = "import urllib.request;print(urllib.request.urlopen('http://localhost:8000/health',timeout=5).read().decode())"
    line = in_container(service, code)
    try:
        return json.loads(line)
    except ValueError:
        return None


def rabbit_queues():
    """{cola: (mensajes, consumidores, durable)} leído con rabbitmqctl dentro del contenedor."""
    rc, out = compose("exec", "-T", "rabbitmq", "rabbitmqctl", "-q", "list_queues",
                      "name", "messages", "consumers", "durable")
    queues = {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) == 4 and parts[1].isdigit():
            queues[parts[0]] = (int(parts[1]), int(parts[2]), parts[3] == "true")
    return queues


def compose_config():
    rc, out = run(["compose", "config", "--format", "json"])
    try:
        return json.loads(out)
    except ValueError:
        return None


def pause(service):
    print("    $ docker compose pause %s" % service)
    compose("pause", service)


def unpause(service):
    print("    $ docker compose unpause %s" % service)
    compose("unpause", service)


def stop(*services):
    print("    $ docker compose stop %s" % " ".join(services))
    compose("stop", *services)


def start(*services):
    print("    $ docker compose start %s" % " ".join(services))
    for s in services:
        compose("start", s)


def wait_healthy(services, timeout=90):
    ok, secs = wait_until(lambda: all(health(s) for s in services), timeout, interval=3)
    return ok, secs


def logs_since(since):
    rc, out = compose("logs", "--no-color", "--since", str(int(since) - 2), timeout=60)
    return out


def touched_services(cid, since):
    """Servicios que registraron una línea con este Correlation ID (ver common/correlation.py)."""
    found = set()
    for line in logs_since(since).splitlines():
        m = re.match(r"^\s*([a-z]+)(?:[-_]\d+)?\s*\|.*\[cid=%s\]" % re.escape(cid), line)
        if m:
            found.add(m.group(1))
    return found


def percentile(values, p):
    """Percentil por rango más cercano: con 20 muestras, el p95 es la 19.ª más rápida."""
    ordered = sorted(values)
    k = max(0, min(len(ordered) - 1, math.ceil(p / 100.0 * len(ordered)) - 1))
    return ordered[k]


# ---------------------------------------------------------------- Chaos Gorilla
def cat_gorilla(ctx):
    """Una zona completa cae a la vez. En un solo equipo, una 'zona' es un grupo de contenedores que
    en producción estarían juntos en el mismo centro de datos."""
    # Zona 1: el núcleo de matrícula (Course + Enrollment + Payment) cae completo.
    zone = ("course", "enrollment", "payment")
    c = ctx.new_course(capacity=5)
    print("  Zona 1: núcleo de matrícula %s" % (zone,))
    stop(*zone)
    try:
        s, _, t = http.request("POST", "/auth/login", body={"username": "estudiante1", "password": "est123"})
        rec("Zona 1 caída: iniciar sesión (Auth está en otra zona)", "200", "%s (%.1f s)" % (s, t), s == 200)
        s, _, t = api("GET", "/notifications", ctx.est)
        rec("Zona 1 caída: consultar notificaciones", "200", "%s (%.1f s)" % (s, t), s == 200)
        s, _, t = api("GET", "/teachers/me", ctx.doc)
        rec("Zona 1 caída: perfil del docente", "200", "%s (%.1f s)" % (s, t), s == 200)
        s, _, t = ctx.enroll(c["id"])
        rec("Zona 1 caída: matricular", "503 rápido (falla rápido, sin colgarse)", "%s (%.1f s)" % (s, t),
            s == 503 and t < 4)
        s, _, t = api("GET", "/courses", ctx.est)
        rec("Zona 1 caída: consultar cursos", "503 rápido", "%s (%.1f s)" % (s, t), s == 503 and t < 4)
    finally:
        start(*zone)
    ok, secs = wait_healthy(list(zone))
    rec("Zona 1 recuperada", "los 3 servicios sanos", "sanos a los %.0f s" % secs if ok else "no se recuperaron", ok)
    def enroll_ok():
        r = ctx.enroll(c["id"])
        return r if r[0] == 201 else None

    result, _ = wait_until(enroll_ok, 45, interval=3)  # el Circuit Breaker puede tardar ~10 s en cerrarse
    eid = field(result[1], "id") if result else None
    pay, _ = wait_until(lambda: ctx.pending_payment(eid), 45)
    if pay:
        api("POST", "/payments/%s/pay" % pay["id"], ctx.est, {})
    done, secs = wait_until(lambda: ctx.status_of(eid) == "CONFIRMADA", 45)
    rec("Zona 1 recuperada: flujo completo (matricular, cobrar, pagar)", "CONFIRMADA",
        "CONFIRMADA a los %.0f s" % secs if done else "no se completó (estado %s)" % ctx.status_of(eid), done)
    rec("Zona 1: cupos consistentes", "4 de 5 (solo la matrícula confirmada ocupa cupo)",
        "%s de 5" % ctx.available(c["id"]), ctx.available(c["id"]) == 4)

    # Zona 2: toda la mensajería (RabbitMQ + Payment + Notification) cae a la vez.
    zone = ("rabbitmq", "payment", "notification")
    c = ctx.new_course(capacity=5)
    print("  Zona 2: mensajería %s" % (zone,))
    stop(*zone)
    try:
        s, e, t = ctx.enroll(c["id"])
        rec("Zona 2 caída: matricular (el evento queda en el outbox)", "201 PENDIENTE_PAGO",
            "%s %s (%.1f s)" % (s, field(e, "status", e), t), s == 201 and field(e, "status") == "PENDIENTE_PAGO")
        eid = field(e, "id")
        s, _, t = api("GET", "/courses", ctx.est)
        rec("Zona 2 caída: consultar cursos", "200", "%s (%.1f s)" % (s, t), s == 200)
    finally:
        start("rabbitmq")
        wait_until(lambda: rabbit_queues(), 90, interval=3)
        start("payment", "notification")
    pay, secs = wait_until(lambda: ctx.pending_payment(eid), 120, interval=3)
    rec("Zona 2 recuperada: el evento guardado llega a Payment", "el cobro aparece",
        "aparece a los %.0f s" % secs if pay else "no apareció en 120 s", pay)

    def notified():
        s, b, _ = api("GET", "/notifications", ctx.est)
        return s == 200 and any(c["name"] in n.get("message", "") for n in b)

    ok, secs = wait_until(notified, 60, interval=3)
    rec("Zona 2 recuperada: el aviso llega a Notification", "el aviso aparece",
        "aparece a los %.0f s" % secs if ok else "no apareció", ok)
    h = health("enrollment") or {}
    pending = (h.get("outbox") or {}).get("pendientes")
    rec("Zona 2 recuperada: outbox de Enrollment vacío", "0 eventos pendientes", pending, pending == 0)


# ---------------------------------------------------------------- Conformity Monkey
def cat_conformity(ctx):
    """El Conformity Monkey de Netflix APAGA las instancias que no cumplen. Aquí hay una sola
    instancia por servicio, así que apagarla dejaría el sistema sin ese servicio: se reportan."""
    healthy = {s: health(s) for s in SERVICES}
    bad = [s for s, h in healthy.items() if not h or h.get("service") != s or h.get("status") != "ok"]
    rec("Todos los servicios exponen /health con su nombre", "8 de 8", "%d de 8%s" % (
        8 - len(bad), (" (fallan: %s)" % bad) if bad else ""), not bad)

    cfg = compose_config()
    if not cfg:
        rec("Leer la configuración de Docker Compose", "docker compose config", "no se pudo leer", False)
        return
    services = cfg.get("services", {})
    published = {name: svc.get("ports", []) for name, svc in services.items() if svc.get("ports")}
    business = [n for n in published if n not in ("gateway", "rabbitmq")]
    rec("Punto de entrada único: ningún servicio de negocio publica puertos (API Gateway, p. 324)",
        "solo gateway (y rabbitmq en localhost)", "publican: %s" % sorted(published), not business)

    owners = {}
    for name, svc in services.items():
        for v in svc.get("volumes", []):
            if v.get("type") == "volume":
                owners.setdefault(v.get("source"), []).append(name)
    shared = {vol: names for vol, names in owners.items() if len(names) > 1}
    no_db = [s for s in SERVICES if s != "gateway" and not any(s in n for n in owners.values())]
    rec("Base de datos propia por servicio (sin almacenamiento compartido, pp. 136-137, 161)",
        "cada servicio con su volumen, ninguno compartido",
        "compartidos: %s; sin volumen: %s" % (shared or "ninguno", no_db or "ninguno"), not shared and not no_db)

    images = {n: s.get("image", "") for n, s in services.items() if s.get("image")}
    latest = [n for n, img in images.items() if img.endswith(":latest") or ":" not in img.split("/")[-1]]
    froms = []
    for s in SERVICES:
        try:
            with open(os.path.join(ROOT, s, "Dockerfile"), encoding="utf-8") as f:
                froms += [l.split()[1] for l in f if l.startswith("FROM")]
        except OSError:
            pass
    loose_from = [f for f in froms if ":" not in f or f.endswith(":latest")]
    rec("Imágenes con versión fija (nada de ':latest')", "todas con versión",
        "externas: %s; base de los servicios: %s" % (sorted(set(images.values())), sorted(set(froms))),
        not latest and not loose_from)

    loose = []
    for s in SERVICES:
        try:
            with open(os.path.join(ROOT, s, "requirements.txt"), encoding="utf-8") as f:
                loose += ["%s: %s" % (s, l.strip()) for l in f if l.strip() and "==" not in l and not l.startswith("#")]
        except OSError:
            pass
    rec("Dependencias con versión exacta (==) en cada requirements.txt", "todas fijas",
        "sin fijar: %s" % (loose or "ninguna"), not loose)

    queues = rabbit_queues()
    consumer_queues = ["enrollment.q", "payment.q", "notification.q"]
    missing = [q for q in consumer_queues if q not in queues or not queues[q][2]]
    no_dlq = [q for q in consumer_queues if "%s.dlq" % q not in queues]
    rec("Cada consumidor tiene cola durable y su Dead Letter Queue", "3 colas durables con .dlq",
        "sin cola durable: %s; sin DLQ: %s" % (missing or "ninguna", no_dlq or "ninguna"), not missing and not no_dlq)

    # Cadena síncrona de un solo salto (p. 224): con el Correlation ID se ve qué servicios tocó cada petición.
    since = time.time()
    checks = [
        ("GET /teachers/me/courses (Chained)", "/teachers/me/courses", ctx.doc, {"gateway", "teacher", "enrollment"}),
        ("GET /enrollments/me/summary (Aggregator)", "/enrollments/me/summary", ctx.est,
         {"gateway", "enrollment", "student", "course", "payment"}),
    ]
    cids = []
    for label, path, token, expected in checks:
        cid = "conformidad-%d" % (len(cids) + 1) + datetime.now().strftime("%H%M%S")
        http.request("GET", path, token=token, headers={"X-Correlation-ID": cid})
        cids.append((label, cid, expected))
    time.sleep(2)
    for label, cid, expected in cids:
        seen = touched_services(cid, since)
        rec("Cadena síncrona de un salto: %s" % label, "toca solo %s" % sorted(expected),
            "tocó %s" % sorted(seen), seen == expected)


# ---------------------------------------------------------------- Doctor Monkey
def cat_doctor(ctx):
    slow = []
    for s in SERVICES:
        h = health(s)
        if not h or h.get("status") != "ok":
            slow.append("%s sin respuesta" % s)
    rec("Salud de los 8 servicios (/health)", "todos 'ok'", "problemas: %s" % (slow or "ninguno"), not slow)

    enr, tea, pay, notif = health("enrollment") or {}, health("teacher") or {}, health("payment") or {}, health("notification") or {}
    breakers = {k: v for k, v in list(enr.items()) + list(tea.items()) if k.endswith("_breaker")}
    rec("Circuit Breakers cerrados (ninguna dependencia fallando)", "todos CLOSED", breakers,
        breakers and all(v == "CLOSED" for v in breakers.values()))
    pend = {"enrollment": (enr.get("outbox") or {}).get("pendientes"), "payment": (pay.get("outbox") or {}).get("pendientes")}
    rec("Outbox sin eventos atascados", "0 pendientes en Enrollment y Payment", pend, all(v == 0 for v in pend.values()))
    cons = {}
    for h in (enr, pay, notif):
        cons.update(h.get("consumers") or {})
    rec("Consumidores conectados a RabbitMQ", "enrollment.q, payment.q y notification.q conectados", cons,
        len(cons) == 3 and all(cons.values()))

    # Rendimiento (p. 316): el promedio puede verse bien y esconder picos; se miran p95 y máximo.
    targets = [("GET /courses", "GET", "/courses", None, ctx.est),
               ("GET /enrollments/me/summary (Aggregator)", "GET", "/enrollments/me/summary", None, ctx.est),
               ("POST /auth/login (PBKDF2)", "POST", "/auth/login", {"username": "estudiante1", "password": "est123"}, None)]
    for label, method, path, body, token in targets:
        times = []
        for _ in range(20):
            s, _, t = http.request(method, path, token=token, body=body)
            if s == 200:
                times.append(t * 1000)
            time.sleep(0.12)  # ritmo por debajo del rate limit del Gateway (100 en 10 s)
        if len(times) < 20:
            rec("Latencia %s" % label, "20 de 20 respuestas 200", "%d de 20" % len(times), False)
            continue
        avg, p95, mx = statistics.mean(times), percentile(times, 95), max(times)
        rec("Latencia %s" % label, "p95 < 300 ms", "promedio %.0f ms, p95 %.0f ms, máximo %.0f ms" % (avg, p95, mx),
            p95 < 300)
        if mx > 1000:
            finding("bajo", "%s tuvo un pico de %.0f ms (promedio %.0f ms)" % (label, mx, avg))

    queues = rabbit_queues()
    dead = {q: m for q, (m, _, _) in queues.items() if q.endswith(".dlq") and m}
    if dead:
        finding("medio", "Hay mensajes apartados en las DLQ (revisarlos): %s" % dead)
    rc, out = run(["stats", "--no-stream", "--format", "{{json .}}"], timeout=60)
    usage = []
    for line in out.splitlines():
        try:
            d = json.loads(line)
        except ValueError:
            continue
        name = d.get("Name", "")
        if any(s in name for s in SERVICES + ["rabbitmq"]):
            usage.append("%s CPU %s, memoria %s" % (name, d.get("CPUPerc"), d.get("MemUsage", "").split(" / ")[0]))
    if usage:
        rec("Uso de recursos de los contenedores (docker stats)", "reporte", "; ".join(usage), True)


# ---------------------------------------------------------------- Janitor Monkey
def cat_janitor(ctx):
    # Genera "basura" real: una matrícula con pago fallido deja eventos enviados y una lápida en Course.
    c = ctx.new_course(capacity=3)
    s, e, _ = ctx.enroll(c["id"])
    eid = field(e, "id")
    pay, _ = wait_until(lambda: ctx.pending_payment(eid), 45)
    if pay:
        api("POST", "/payments/%s/pay" % pay["id"], ctx.est, {"simulate_failure": True})
    wait_until(lambda: ctx.status_of(eid) == "CANCELADA", 45)
    time.sleep(6)  # el flusher marca los eventos como enviados

    before = {s: (health(s) or {}).get("outbox") for s in ("enrollment", "payment")}
    tomb_before = (health("course") or {}).get("lapidas")
    rec("Recursos sin uso antes de limpiar", "hay eventos ya enviados y lápidas",
        "outbox: %s; lápidas en Course: %s" % (before, tomb_before),
        all((v or {}).get("enviados", 0) > 0 for v in before.values()) and (tomb_before or 0) > 0)

    # El Janitor corre solo cada hora con retención de días; aquí se fuerza con retención de 2 s.
    code = ("import logging,os;logging.disable(logging.CRITICAL);from common.events import Outbox;"
            "print(Outbox(os.environ.get('DB_PATH','/data/{s}.db')).purge_sent(2))")
    deleted = {s: in_container(s, code.replace("{s}", s)) for s in ("enrollment", "payment")}
    tomb_deleted = in_container("course", "import logging;logging.disable(logging.CRITICAL);"
                                          "import main;print(main.purge_tombstones(2))")
    after = {s: (health(s) or {}).get("outbox") for s in ("enrollment", "payment")}
    tomb_after = (health("course") or {}).get("lapidas")
    rec("Limpieza: eventos ya enviados borrados; pendientes intactos",
        "enviados = 0 y pendientes sin cambios",
        "borrados %s; después %s" % (deleted, after),
        all((after[s] or {}).get("enviados") == 0 and (after[s] or {}).get("pendientes") == (before[s] or {}).get("pendientes")
            for s in after))
    rec("Limpieza: lápidas viejas borradas en Course", "0 lápidas",
        "borradas %s; quedan %s" % (tomb_deleted, tomb_after), tomb_after == 0)

    c2 = ctx.new_course(capacity=3)
    s, e, _ = ctx.enroll(c2["id"])
    pay, _ = wait_until(lambda: ctx.pending_payment(field(e, "id")), 45)
    rec("El sistema sigue funcionando después de limpiar", "matrícula 201 y cobro creado",
        "%s, cobro %s" % (s, "creado" if pay else "no apareció"), s == 201 and bool(pay))

    queues = rabbit_queues()
    unused = [q for q, (m, consumers, _) in queues.items() if consumers == 0 and not q.endswith(".dlq")]
    rec("Colas sin consumidor en RabbitMQ", "ninguna (cada cola tiene su servicio)",
        unused or "ninguna", not unused)
    rc, out = run(["images", "-f", "dangling=true", "-q"])
    dangling = [l for l in out.split() if l.strip()]
    if dangling:
        finding("bajo", "%d imágenes de Docker sin etiqueta (builds viejos). Se pueden borrar con "
                        "'docker image prune' (no se borran solas para no tocar otros proyectos)." % len(dangling))


# ---------------------------------------------------------------- Latency Monkey
def cat_latency(ctx):
    # 1) Course congelado: la reserva llega pero no hay respuesta (latencia en la cadena síncrona).
    c = ctx.new_course(capacity=5)
    before = ctx.available(c["id"])
    pause("course")
    try:
        s, e, t = ctx.enroll(c["id"])
        rec("Course congelado 6 s: matricular", "503 o 504 en menos de 6 s (no se cuelga)",
            "%s (%.1f s)" % (s, t), s in (503, 504) and t < 6)
        time.sleep(max(0, 6 - t))
    finally:
        unpause("course")
    _, enrs, _ = api("GET", "/enrollments", ctx.est)
    eid = max((x["id"] for x in enrs or [] if x["course_id"] == c["id"]), default=None)
    ok, secs = wait_until(lambda: ctx.status_of(eid) == "CANCELADA", 60, interval=3)
    rec("Course congelado: la matrícula incierta se compensa", "CANCELADA (LIBERANDO -> CANCELADA)",
        "CANCELADA a los %.0f s" % secs if ok else ctx.status_of(eid), ok)
    rec("Course congelado: ningún cupo perdido", "%s cupos (igual que antes)" % before,
        ctx.available(c["id"]), ctx.available(c["id"]) == before)

    # 2) Payment congelado: la matrícula no depende de Payment (asíncrono).
    c = ctx.new_course(capacity=5)
    pause("payment")
    try:
        s, e, t = ctx.enroll(c["id"])
        rec("Payment congelado 8 s: matricular", "201 en menos de 1 s (no espera a Payment)",
            "%s (%.2f s)" % (s, t), s == 201 and t < 1)
        time.sleep(8)
    finally:
        unpause("payment")
    pay, secs = wait_until(lambda: ctx.pending_payment(field(e, "id")), 60)
    rec("Payment descongelado: procesa el evento pendiente", "el cobro aparece",
        "aparece a los %.0f s" % secs if pay else "no apareció", pay)

    # 3) RabbitMQ congelado: el outbox reintenta y el usuario no lo nota.
    c = ctx.new_course(capacity=5)
    pause("rabbitmq")
    try:
        s, e, t = ctx.enroll(c["id"])
        rec("RabbitMQ congelado 8 s: matricular", "201 en menos de 1 s (el evento espera en el outbox)",
            "%s (%.2f s)" % (s, t), s == 201 and t < 1)
        time.sleep(8)
    finally:
        unpause("rabbitmq")
    pay, secs = wait_until(lambda: ctx.pending_payment(field(e, "id")), 90, interval=3)
    rec("RabbitMQ descongelado: el evento sale del outbox", "el cobro aparece",
        "aparece a los %.0f s" % secs if pay else "no apareció", pay)

    # 4) Student congelado: el Aggregator responde con lo que tiene, sin esperar indefinidamente.
    pause("student")
    try:
        s, b, t = ctx.summary()
        rec("Student congelado: resumen del estudiante (Aggregator)",
            "200 parcial en menos de 5 s, 'student' en unavailable",
            "%s en %.1f s, unavailable=%s" % (s, t, field(b, "unavailable")),
            s == 200 and t < 5 and "student" in (field(b, "unavailable") or []))
    finally:
        unpause("student")
    def complete():
        s, b, _ = ctx.summary()
        return s == 200 and field(b, "unavailable") == []

    ok, secs = wait_until(complete, 60, interval=3)
    rec("Student descongelado: resumen completo", "unavailable vacío",
        "completo a los %.0f s" % secs if ok else "sigue incompleto", ok)


# ---------------------------------------------------------------- Security Monkey
def cat_security(ctx):
    cfg = compose_config() or {}
    exposed = []
    for name, svc in (cfg.get("services") or {}).items():
        for p in svc.get("ports", []):
            if name != "gateway" and p.get("host_ip") not in ("127.0.0.1", "localhost"):
                exposed.append("%s:%s" % (name, p.get("published")))
    rec("Puertos expuestos a la red", "solo el Gateway; lo demás solo en 127.0.0.1",
        "expuestos: %s" % (exposed or "ninguno fuera del Gateway"), not exposed and cfg)

    s1, _, _ = api("GET", "/internal/courses/1/reserve", ctx.est)
    s2, _, _ = api("POST", "/internal/courses/1/release", ctx.est, {"enrollment_id": 1})
    rec("Rutas internas (/internal/...) inalcanzables desde afuera", "404 y 404", "%s y %s" % (s1, s2),
        s1 == 404 and s2 == 404)

    header, payload, sig = ctx.est.split(".")
    claims = json.loads(b64d(payload))
    tampered = "%s.%s.%s" % (header, b64(json.dumps(dict(claims, role="admin")).encode()), sig)
    none_alg = forge({"alg": "none", "typ": "JWT"}, dict(claims, role="admin"))
    other_key = forge({"alg": "HS256", "typ": "JWT"}, dict(claims, role="admin"), secret="otra-clave")
    results = [api("GET", "/students", tok)[0] for tok in (tampered, none_alg, other_key)]
    rec("Tokens manipulados (rol cambiado, alg=none, otra clave)", "401, 401, 401", results,
        results == [401, 401, 401])

    s, _, _ = http.request("GET", "/students", token=ctx.est, headers={"X-User-Role": "admin", "X-User-Id": "1"})
    rec("Suplantar el rol con la cabecera X-User-Role: admin", "403 (el Gateway reescribe la identidad)", s, s == 403)

    line = in_container("auth", "import sqlite3,os,json;c=sqlite3.connect(os.environ.get('DB_PATH','/data/auth.db'));"
                                "r=c.execute(\"SELECT password_hash,salt FROM users WHERE username='estudiante1'\").fetchone();"
                                "print(json.dumps({'len':len(r[0]),'plano':r[0]=='est123','salt':bool(r[1])}))")
    try:
        info = json.loads(line)
    except ValueError:
        info = {}
    rec("Contraseñas guardadas con hash irreversible (p. 321)", "hash de 64 caracteres con sal, nunca en texto plano",
        info or line, info.get("len") == 64 and info.get("salt") and not info.get("plano"))

    env = {}
    for svc in (cfg.get("services") or {}).values():
        env.update(svc.get("environment") or {})
    if env.get("JWT_SECRET", "").startswith("cambia-este-secreto"):
        finding("medio", "JWT_SECRET usa el valor de ejemplo del docker-compose. En producción debe venir de un "
                         "secreto (variable de entorno o gestor de secretos), no del repositorio.")
    if env.get("RABBITMQ_PASS") in ("academico", "guest"):
        finding("bajo", "RabbitMQ usa credenciales fijas del repositorio. El riesgo se reduce porque sus "
                        "puertos solo escuchan en 127.0.0.1.")
    finding("bajo", "Sin HTTPS: el tráfico va en texto plano (el libro recomienda HTTPS incluso entre servicios, p. 321). "
                    "Aceptable en un entorno local de laboratorio.")
    finding("bajo", "Los IDs son numéricos y secuenciales (p. 321 recomienda evitarlos). El riesgo se mitiga porque "
                    "cada servicio valida que el recurso sea del usuario (404 si no lo es).")

    # Defensa contra abuso (p. 321-322): el rate limit corta ráfagas. Va al final porque bloquea 10 s.
    codes = [http.request("GET", "/courses", token=ctx.est)[0] for _ in range(130)]
    rec("Ráfaga de 130 peticiones (defensa contra DoS)", "aparecen 429 y ningún 5xx",
        "200 x%d, 429 x%d, 5xx x%d" % (codes.count(200), codes.count(429), sum(1 for c in codes if c and c >= 500)),
        codes.count(429) > 0 and not any(c and c >= 500 for c in codes))
    time.sleep(11)


CATEGORIES = {"gorilla": cat_gorilla, "conformity": cat_conformity, "doctor": cat_doctor,
              "janitor": cat_janitor, "latency": cat_latency, "security": cat_security}


def write_report(names, started):
    os.makedirs(os.path.join(ROOT, "resultados"), exist_ok=True)
    path = os.path.join(ROOT, "resultados", "simian_%s.md" % started.strftime("%Y%m%d_%H%M%S"))
    good = sum(r[4] for r in RECORDS)
    lines = ["# Chaos Monkey: las 6 categorías del libro (Simian Army)", "",
             "Fecha: %s" % started.strftime("%Y-%m-%d %H:%M"),
             "Referencia: Pacheco, *Microservice Patterns and Best Practices*, cap. 12, p. 304.", "",
             "Comprobaciones: %d. Cumplidas: %d. No cumplidas: %d. Hallazgos reportados: %d." % (
                 len(RECORDS), good, len(RECORDS) - good, len(FINDINGS)), ""]
    for n in names:
        title, desc = CATEGORY_INFO[n]
        lines += ["## %s" % title, "", "_%s_" % desc, "", "| Paso | Esperado | Observado | Resultado |", "|---|---|---|---|"]
        lines += ["| %s | %s | %s | %s |" % (s.replace("|", "/"), e.replace("|", "/"), o.replace("|", "/"),
                                             "OK" if ok else "NO CUMPLE") for c, s, e, o, ok in RECORDS if c == n]
        found = [(sev, t) for c, sev, t in FINDINGS if c == n]
        if found:
            lines += ["", "Hallazgos:", ""] + ["- **%s**: %s" % (sev, t) for sev, t in found]
        lines.append("")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return path


def main():
    parser = argparse.ArgumentParser(description="Las 6 categorías de Chaos Monkey del libro (Simian Army)")
    parser.add_argument("--categoria", nargs="+", choices=list(CATEGORIES), help="categorías a ejecutar")
    parser.add_argument("--todas", action="store_true", help="las 6 categorías")
    parser.add_argument("--listar", action="store_true")
    parser.add_argument("--base", default=http.BASE, help="URL del Gateway (por defecto %(default)s)")
    args = parser.parse_args()
    http.BASE = args.base
    if args.listar or not (args.categoria or args.todas):
        for key, (title, desc) in CATEGORY_INFO.items():
            print("  %-11s %-18s %s" % (key, title, desc))
        print("\nEjemplo: python scripts/simian.py --todas")
        return
    names = list(CATEGORIES) if args.todas else args.categoria
    rc, out = compose("version")
    if rc != 0:
        sys.exit("'docker compose' no responde: %s" % out.strip()[:200])
    compose("up", "-d")
    try:
        ctx = Ctx()
    except RuntimeError as exc:
        sys.exit(str(exc))
    wait_until(lambda: api("GET", "/courses", ctx.est)[0] == 200, 90, interval=3)
    started = datetime.now()
    for n in names:
        CURRENT["cat"] = n
        print("\n=== %s: %s" % CATEGORY_INFO[n])
        try:
            CATEGORIES[n](ctx)
        except Exception as exc:  # una categoría que se rompe no detiene las demás
            rec("ejecución de la categoría", "sin errores", "error: %s" % exc, False)
    good = sum(r[4] for r in RECORDS)
    print("\n=== Resumen: %d comprobaciones | cumplidas %d | no cumplidas %d | hallazgos %d" % (
        len(RECORDS), good, len(RECORDS) - good, len(FINDINGS)))
    for c, s, e, o, ok in RECORDS:
        if not ok:
            print("  - [%s] %s -> %s" % (c, s, o))
    print("Informe guardado en: %s" % write_report(names, started))


if __name__ == "__main__":
    main()
