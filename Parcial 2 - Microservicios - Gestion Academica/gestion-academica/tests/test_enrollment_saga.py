import time

import httpx
import pytest
from fastapi.testclient import TestClient

from common.events import TransientError

STUDENT = {"x-user-id": "2", "x-user-role": "student", "x-user-name": "estudiante1"}


class FakeCourse:
    """Simula Course. reserve_mode / release_mode: 'ok', 'timeout' (la petición llegó pero no hubo
    respuesta a tiempo) o 'down' (no se pudo conectar)."""

    def __init__(self):
        self.reserve_mode = "ok"
        self.release_mode = "ok"
        self.calls = []

    def __call__(self, request):
        action = request.url.path.rsplit("/", 1)[-1]
        self.calls.append(action)
        mode = self.reserve_mode if action == "reserve" else self.release_mode
        if mode == "timeout":
            raise httpx.ReadTimeout("sin respuesta", request=request)
        if mode == "down":
            raise httpx.ConnectError("conexión rechazada", request=request)
        if action == "reserve":
            return httpx.Response(200, json={"course_id": 1, "name": "Arquitectura", "price": 100.0})
        return httpx.Response(200, json={"released": True})


@pytest.fixture
def enrollment(service):
    enr = service("enrollment", STUCK_REQUEST_SECONDS=60, PAYMENT_TIMEOUT_SECONDS=300)
    course = FakeCourse()
    enr.http = httpx.Client(transport=httpx.MockTransport(course))
    enr.outbox.flush_async = lambda: None  # sin broker en las pruebas unitarias
    return enr, TestClient(enr.app), course


def status_of(enr, enrollment_id):
    with enr.get_db(enr.DB) as c:
        return c.execute("SELECT status FROM enrollments WHERE id=?", (enrollment_id,)).fetchone()["status"]


def outbox_keys(enr):
    with enr.get_db(enr.DB) as c:
        return [r["routing_key"] for r in c.execute("SELECT routing_key FROM outbox ORDER BY id")]


def test_flujo_feliz(enrollment):
    enr, client, _ = enrollment
    r = client.post("/enrollments", json={"course_id": 1}, headers=STUDENT)
    assert r.status_code == 201 and r.json()["status"] == "PENDIENTE_PAGO"
    assert outbox_keys(enr) == ["matricula.creada"]


def test_timeout_de_lectura_libera_el_cupo(enrollment):
    """Antes: se cancelaba sin liberar y el cupo quedaba tomado para siempre."""
    enr, client, course = enrollment
    course.reserve_mode = "timeout"
    assert client.post("/enrollments", json={"course_id": 1}, headers=STUDENT).status_code == 503
    assert course.calls == ["reserve", "release"]
    assert status_of(enr, 1) == "CANCELADA"


def test_si_no_se_pudo_conectar_no_hay_nada_que_liberar(enrollment):
    enr, client, course = enrollment
    course.reserve_mode = "down"
    assert client.post("/enrollments", json={"course_id": 1}, headers=STUDENT).status_code == 503
    assert course.calls == ["reserve"]
    assert status_of(enr, 1) == "CANCELADA"


def test_liberacion_pendiente_la_termina_el_reconciliador(enrollment):
    enr, client, course = enrollment
    course.reserve_mode = "timeout"
    course.release_mode = "down"
    assert client.post("/enrollments", json={"course_id": 1}, headers=STUDENT).status_code == 503
    assert status_of(enr, 1) == "LIBERANDO"
    # LIBERANDO no bloquea un nuevo intento del estudiante
    course.reserve_mode = course.release_mode = "ok"
    assert client.post("/enrollments", json={"course_id": 1}, headers=STUDENT).status_code == 201
    for enrollment_id, reason, status in enr.reconcile_jobs(time.time()):
        enr.cancel_and_release(enrollment_id, reason, from_status=status)
    assert status_of(enr, 1) == "CANCELADA"
    assert status_of(enr, 2) == "PENDIENTE_PAGO"


def test_solicitada_atascada_se_compensa(enrollment):
    """Enrollment se cayó entre la reserva y el cambio a PENDIENTE_PAGO."""
    enr, client, course = enrollment
    old = time.time() - 120
    with enr.get_db(enr.DB) as c:
        c.execute("INSERT INTO enrollments(student_id, course_id, status, created_at, updated_at) "
                  "VALUES ('2', 1, 'SOLICITADA', ?, ?)", (old, old))
    # mientras siga atascada, bloquea un nuevo intento...
    assert client.post("/enrollments", json={"course_id": 1}, headers=STUDENT).status_code == 409
    for enrollment_id, reason, status in enr.reconcile_jobs(time.time()):
        enr.cancel_and_release(enrollment_id, reason, from_status=status)
    assert course.calls == ["release"]
    assert status_of(enr, 1) == "CANCELADA"
    # ...y después de compensarla, el estudiante puede volver a matricularse
    assert client.post("/enrollments", json={"course_id": 1}, headers=STUDENT).status_code == 201


def test_pago_fallido_con_course_caido_es_fallo_pasajero(enrollment):
    enr, client, course = enrollment
    client.post("/enrollments", json={"course_id": 1}, headers=STUDENT)
    course.release_mode = "down"
    with pytest.raises(TransientError):
        enr.handle_event("pago.fallido", {"enrollment_id": 1})
    assert status_of(enr, 1) == "PENDIENTE_PAGO"
    course.release_mode = "ok"
    enr.course_breaker = enr.CircuitBreaker()  # simula que pasó el tiempo de reapertura
    enr.handle_event("pago.fallido", {"enrollment_id": 1})
    assert status_of(enr, 1) == "CANCELADA"
    assert outbox_keys(enr) == ["matricula.creada", "matricula.cancelada"]
