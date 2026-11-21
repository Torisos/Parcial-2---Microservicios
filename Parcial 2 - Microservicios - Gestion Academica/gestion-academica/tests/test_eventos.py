from fastapi.testclient import TestClient

STUDENT = {"x-user-id": "2", "x-user-role": "student"}
CREADA = {"enrollment_id": 1, "student_id": "2", "course_name": "Arquitectura", "amount": 100.0}


def payment_status(pay):
    with pay.get_db(pay.DB) as c:
        return c.execute("SELECT status FROM payments WHERE enrollment_id=1").fetchone()["status"]


def outbox_keys(pay):
    with pay.get_db(pay.DB) as c:
        return [r["routing_key"] for r in c.execute("SELECT routing_key FROM outbox ORDER BY id")]


def test_pago_tardio_se_reembolsa(service):
    """El estudiante pagó y luego llegó la cancelación (la matrícula venció): se reembolsa una vez."""
    pay = service("payment")
    pay.outbox.flush_async = lambda: None
    client = TestClient(pay.app)
    pay.handle_event("matricula.creada", CREADA)
    assert client.post("/payments/1/pay", headers=STUDENT).json()["status"] == "PAGADO"
    pay.handle_event("matricula.cancelada", {"enrollment_id": 1})
    pay.handle_event("matricula.cancelada", {"enrollment_id": 1})  # evento repetido
    assert payment_status(pay) == "REEMBOLSADO"
    assert outbox_keys(pay) == ["pago.confirmado", "pago.reembolsado"]


def test_cancelacion_antes_del_pago_cancela_el_cobro(service):
    pay = service("payment")
    pay.outbox.flush_async = lambda: None
    client = TestClient(pay.app)
    pay.handle_event("matricula.creada", CREADA)
    pay.handle_event("matricula.cancelada", {"enrollment_id": 1})
    assert payment_status(pay) == "CANCELADO"
    assert client.post("/payments/1/pay", headers=STUDENT).status_code == 409
    assert outbox_keys(pay) == []


def test_notificacion_repetida_no_se_duplica(service):
    notif = service("notification")
    event = dict(CREADA, event_id="enrollment:1")
    notif.handle_event("matricula.creada", event)
    notif.handle_event("matricula.creada", event)
    notif.handle_event("matricula.creada", dict(event, event_id="enrollment:2"))
    rows = TestClient(notif.app).get("/notifications", headers=STUDENT).json()
    assert len(rows) == 2


def test_outbox_agrega_event_id_unico(service, monkeypatch):
    from common import events

    pay = service("payment")
    sent = []

    class FakePublisher:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def publish(self, routing_key, payload):
            sent.append((routing_key, payload["event_id"]))

    monkeypatch.setattr(events, "Publisher", FakePublisher)
    with pay.get_db(pay.DB) as c:
        events.Outbox.add(c, "pago.confirmado", {"enrollment_id": 1})
        events.Outbox.add(c, "pago.fallido", {"enrollment_id": 2})
    pay.outbox.flush()
    pay.outbox.flush()  # lo ya enviado no se reenvía
    assert sent == [("pago.confirmado", "payment:1"), ("pago.fallido", "payment:2")]


def test_janitor_borra_enviados_y_nunca_pendientes(service):
    from common.events import Outbox

    pay = service("payment")
    with pay.get_db(pay.DB) as c:
        for i in range(3):
            Outbox.add(c, "pago.confirmado", {"enrollment_id": i})
        c.execute("UPDATE outbox SET sent = 1, sent_at = 0 WHERE id IN (1, 2)")  # enviados hace mucho
    assert pay.outbox.stats() == {"pendientes": 1, "enviados": 2}
    assert pay.outbox.purge_sent(older_than_seconds=60) == 2
    assert pay.outbox.stats() == {"pendientes": 1, "enviados": 0}


def test_janitor_respeta_la_retencion(service):
    import time

    from common.events import Outbox

    pay = service("payment")
    with pay.get_db(pay.DB) as c:
        Outbox.add(c, "pago.confirmado", {"enrollment_id": 1})
        c.execute("UPDATE outbox SET sent = 1, sent_at = ?", (time.time(),))  # recién enviado
    assert pay.outbox.purge_sent(older_than_seconds=3600) == 0


def test_lapidas_viejas_se_borran(service):
    from fastapi.testclient import TestClient

    course = service("course")
    client = TestClient(course.app)
    client.post("/internal/courses/1/release", json={"enrollment_id": 5})
    assert client.get("/health").json()["lapidas"] == 1
    assert course.purge_tombstones(older_than_seconds=3600) == 0  # recientes: se conservan
    assert course.purge_tombstones(older_than_seconds=0) == 1
    assert client.get("/health").json()["lapidas"] == 0
