import time

import httpx
import jwt
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def gateway(service):
    gw = service("gateway", JWT_SECRET="secreto-test", RATE_LIMIT=1000)
    seen = []

    def upstream(request):
        seen.append(request.url.raw_path.decode())
        return httpx.Response(200, json={"ok": True})

    gw.client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    token = jwt.encode({"sub": "2", "role": "student", "exp": int(time.time()) + 60}, "secreto-test",
                       algorithm="HS256")
    return gw, TestClient(gw.app), {"Authorization": f"Bearer {token}"}, seen


@pytest.mark.parametrize("path, forwarded", [
    ("/courses/1", "/courses/1"),
    ("/courses/1%3Fx=1", "/courses/1%3Fx%3D1"),   # antes llegaba '/courses/1' (hallazgo abierto)
    ("/courses/1%23x", "/courses/1%23x"),         # antes llegaba '/courses/1'
    ("/courses/%00", "/courses/%00"),
])
def test_reenvia_la_ruta_completa_y_codificada(gateway, path, forwarded):
    _, client, auth, seen = gateway
    assert client.get(path, headers=auth).status_code == 200
    assert seen[-1] == forwarded


def test_query_normal_se_conserva(gateway):
    _, client, auth, seen = gateway
    client.get("/courses?x=1", headers=auth)
    assert seen[-1] == "/courses?x=1"


def test_rutas_internas_no_se_exponen(gateway):
    _, client, auth, seen = gateway
    assert client.get("/internal/courses/1/reserve", headers=auth).status_code == 404
    assert seen == []


def test_rate_limiter_olvida_ips_inactivas(gateway):
    gw = gateway[0]
    gw._hits.clear()
    gw._last_sweep = 0.0
    gw.RATE_WINDOW = 0.05
    gw._too_many_requests("10.0.0.1")
    time.sleep(0.06)
    gw._too_many_requests("10.0.0.2")  # dispara la limpieza
    assert "10.0.0.1" not in gw._hits and "10.0.0.2" in gw._hits
