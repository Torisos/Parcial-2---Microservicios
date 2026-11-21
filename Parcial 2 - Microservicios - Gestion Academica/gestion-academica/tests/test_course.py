from fastapi.testclient import TestClient


def available(client, course_id=1):
    return client.get(f"/courses/{course_id}").json()["available"]


def test_reserva_y_liberacion_idempotentes(service):
    course = service("course")
    client = TestClient(course.app)
    start = available(client)
    for _ in range(2):
        assert client.post("/internal/courses/1/reserve", json={"enrollment_id": 7}).status_code == 200
    assert available(client) == start - 1
    for _ in range(2):
        assert client.post("/internal/courses/1/release", json={"enrollment_id": 7}).status_code == 200
    assert available(client) == start


def test_reserva_tardia_tras_compensar_se_rechaza(service):
    """La liberación llegó ANTES que la reserva (Enrollment se rindió por timeout): la reserva
    tardía no debe gastar un cupo que nadie liberaría."""
    course = service("course")
    client = TestClient(course.app)
    start = available(client)
    r = client.post("/internal/courses/1/release", json={"enrollment_id": 9})
    assert r.json() == {"released": False}
    assert client.post("/internal/courses/1/reserve", json={"enrollment_id": 9}).status_code == 410
    assert available(client) == start
