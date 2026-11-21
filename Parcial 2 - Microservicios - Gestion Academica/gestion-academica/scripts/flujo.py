"""Evidencia del flujo: recorre una matrícula de principio a fin y muestra por dónde pasó.

Uso (con el sistema arriba: docker compose up -d --build):
    python scripts/flujo.py              # flujo feliz: matrícula -> cobro -> pago -> CONFIRMADA
    python scripts/flujo.py --fallido    # Saga con compensación: pago fallido -> cupo liberado -> CANCELADA

Qué hace:
1. Hace todas las peticiones del flujo con UN MISMO Correlation ID (cabecera X-Correlation-ID).
2. Muestra cada paso: quién actúa, qué responde la API y en qué estado queda la matrícula.
3. Al final lee los logs de los contenedores (docker compose logs) y deja solo las líneas con ese
   ID, ordenadas por hora: es el recorrido real dentro del sistema, servicio por servicio, incluidos
   los eventos que viajaron por RabbitMQ.
4. Guarda todo en resultados/flujo_<fecha>.md.

Solo usa la librería estándar de Python.
"""
import argparse
import datetime
import os
import re
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common_http as http  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(ROOT, "resultados")
LOG_LINE = re.compile(
    r"^(?P<svc>[a-z]+)(?:[-_]\d+)?\s*\|\s*(?P<time>\d{2}:\d{2}:\d{2}\.\d{3}) (?P<level>[A-Z]+) "
    r"(?P<logger>\S+) \[cid=(?P<cid>[^\]]+)\] (?P<msg>.*)$"
)


class Flow:
    def __init__(self, cid):
        self.cid = cid
        self.steps = []

    def call(self, method, path, token=None, body=None, trace=True):
        """trace=True: la petición es parte del flujo y lleva su Correlation ID. Las consultas que el
        script repite mientras espera (trace=False) no lo llevan, para no ensuciar el recorrido."""
        headers = {"X-Correlation-ID": self.cid} if trace else None
        return http.request(method, path, token=token, body=body, headers=headers)

    def step(self, actor, action, result, ok=True):
        n = len(self.steps) + 1
        self.steps.append((n, actor, action, result, ok))
        print("%2d. [%s] %-14s %s\n      -> %s" % (n, "OK" if ok else "XX", actor, action, result), flush=True)


def wait_for(fn, timeout=30, interval=0.5):
    end = time.time() + timeout
    while time.time() < end:
        value = fn()
        if value:
            return value
        time.sleep(interval)
    return None


def login(flow, user, password):
    status, body, _ = flow.call("POST", "/auth/login", body={"username": user, "password": password})
    if status != 200:
        sys.exit("No se pudo iniciar sesión como %s (%s). ¿Está corriendo 'docker compose up'?" % (user, status))
    return body["access_token"]


def pick_course(flow, token):
    """Un curso con cupo en el que el estudiante no tenga matrícula activa."""
    _, courses, _ = flow.call("GET", "/courses", token)
    _, mine, _ = flow.call("GET", "/enrollments", token, trace=False)
    busy = {e["course_id"] for e in (mine or []) if e["status"] in ("SOLICITADA", "PENDIENTE_PAGO", "CONFIRMADA")}
    for c in courses or []:
        if c["id"] not in busy and c["available"] > 0:
            return c
    return None


def available(flow, token, course_id):
    status, body, _ = flow.call("GET", "/courses/%s" % course_id, token, trace=False)
    return body["available"] if status == 200 else None


def run(fail):
    cid = "flujo-" + datetime.datetime.now().strftime("%H%M%S")
    flow = Flow(cid)
    started = flow_started = time.time()
    print("Correlation ID de este flujo: %s\n" % cid)

    token = login(flow, "estudiante1", "est123")
    flow.step("Cliente", "POST /auth/login (estudiante1)",
              "200: Auth valida la contraseña (hash PBKDF2) y entrega un JWT")

    course = pick_course(flow, token)
    if not course:
        sys.exit("No hay un curso libre para estudiante1. Crea uno nuevo o reinicia los volúmenes.")
    before = course["available"]
    flow.step("Cliente", "GET /courses",
              "200: curso '%s' (id %s) con %s cupos libres" % (course["name"], course["id"], before))

    status, enr, secs = flow.call("POST", "/enrollments", token, {"course_id": course["id"]})
    ok = status == 201
    flow.step("Enrollment", "POST /enrollments {course_id: %s}" % course["id"],
              "%s en %.2f s: matrícula %s en %s (Enrollment reservó el cupo en Course por REST)"
              % (status, secs, enr.get("id") if ok else "-", enr.get("status") if ok else enr), ok)
    if not ok:
        return flow, started
    eid = enr["id"]
    flow.step("Course", "cupos del curso después de reservar",
              "%s -> %s" % (before, available(flow, token, course["id"])))

    def payment():
        _, pays, _ = flow.call("GET", "/payments", token, trace=False)
        return next((p for p in pays or [] if p["enrollment_id"] == eid), None)

    pay = wait_for(payment)
    flow.step("Payment", "evento 'matricula.creada' por RabbitMQ",
              "cobro %s creado en estado %s por $%s" % (pay["id"], pay["status"], pay["amount"]) if pay
              else "el cobro no apareció en 30 s", bool(pay))
    if not pay:
        return flow, started

    path = "/payments/%s/pay" % pay["id"]
    status, body, _ = flow.call("POST", path, token, {"simulate_failure": True} if fail else None)
    flow.step("Payment", "POST %s%s" % (path, " {simulate_failure: true}" if fail else ""),
              "%s: pago %s" % (status, body.get("status") if status == 200 else body), status == 200)

    final = "CANCELADA" if fail else "CONFIRMADA"

    def enrollment_state():
        _, e, _ = flow.call("GET", "/enrollments/%s" % eid, token, trace=False)
        return e if e and e.get("status") == final else None

    e = wait_for(enrollment_state)
    via = "'pago.fallido' -> compensación: Enrollment libera el cupo" if fail else "'pago.confirmado'"
    flow.step("Enrollment", "evento %s" % via,
              "matrícula %s: %s%s" % (eid, e["status"] if e else "sin cambio en 30 s",
                                      " (%s)" % e["reason"] if e and e.get("reason") else ""), bool(e))
    if fail:
        after = available(flow, token, course["id"])
        flow.step("Course", "cupos después de la compensación",
                  "%s (antes de matricular: %s)" % (after, before), after == before)

    event = "matricula.cancelada" if fail else "matricula.confirmada"

    def notified():
        # El aviso no trae el número de matrícula: se busca por evento, por nombre del curso y solo
        # entre los avisos creados durante este flujo (los de otras pruebas quedan fuera).
        _, ns, _ = flow.call("GET", "/notifications", token, trace=False)
        return [n for n in ns or [] if n["event"] == event and course["name"] in n.get("message", "")
                and n.get("created_at", 0) >= flow_started - 30]  # margen por reloj del contenedor

    ns = wait_for(notified, timeout=20)
    flow.step("Notification", "evento '%s'" % event, "aviso: %s" % ns[0]["message"] if ns else "no llegó", bool(ns))

    status, summary, secs = flow.call("GET", "/enrollments/me/summary", token)
    flow.step("Aggregator", "GET /enrollments/me/summary",
              "%s en %.2f s: une Student + Course + Payment; no disponibles: %s"
              % (status, secs, (summary or {}).get("unavailable")), status == 200)
    return flow, started


def collect_logs(cid, since):
    """Líneas de log de todos los contenedores con este Correlation ID, ordenadas por hora."""
    try:
        out = subprocess.run(
            ["docker", "compose", "logs", "--no-color", "--since", str(int(since) - 5)],
            cwd=ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=60,
        ).stdout
    except (OSError, subprocess.TimeoutExpired) as exc:
        print("\nNo se pudieron leer los logs de Docker (%s)." % exc)
        return []
    rows = []
    for line in out.splitlines():
        m = LOG_LINE.match(line.strip())
        if m and m.group("cid") == cid:
            rows.append((m.group("time"), m.group("svc"), m.group("msg")))
    return sorted(rows)


def save(flow, logs, fail):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    now = datetime.datetime.now()
    path = os.path.join(RESULTS_DIR, "flujo_%s.md" % now.strftime("%Y%m%d_%H%M%S"))
    lines = [
        "# Evidencia del flujo: %s" % ("pago fallido y compensación" if fail else "matrícula confirmada"),
        "",
        "Fecha: %s  " % now.strftime("%Y-%m-%d %H:%M"),
        "Correlation ID: `%s`" % flow.cid,
        "",
        "## Pasos vistos desde la API",
        "",
        "| # | Actor | Acción | Resultado | OK |",
        "|---|---|---|---|---|",
    ]
    lines += ["| %s | %s | %s | %s | %s |" % (n, a, act.replace("|", "/"), res.replace("|", "/"), "OK" if ok else "FALLA")
              for n, a, act, res, ok in flow.steps]
    lines += ["", "## Recorrido dentro del sistema (logs con el mismo Correlation ID)", ""]
    if logs:
        lines += ["| Hora | Servicio | Qué pasó |", "|---|---|---|"]
        lines += ["| %s | %s | %s |" % (t, s, m.replace("|", "/")) for t, s, m in logs]
    else:
        lines.append("No se pudieron leer los logs de los contenedores.")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fallido", action="store_true", help="simula un pago fallido (Saga con compensación)")
    args = parser.parse_args()

    flow, started = run(args.fallido)
    time.sleep(1.5)  # deja que los servicios terminen de escribir sus logs
    logs = collect_logs(flow.cid, started)
    if logs:
        print("\nRecorrido dentro del sistema (logs con cid=%s):" % flow.cid)
        width = max(len(s) for _, s, _ in logs)
        for t, svc, msg in logs:
            print("  %s  %-*s  %s" % (t, width, svc, msg))
    path = save(flow, logs, args.fallido)
    ok = all(step[4] for step in flow.steps)
    print("\n%s. Evidencia guardada en %s" % ("Flujo completo" if ok else "El flujo NO se completó", path))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
