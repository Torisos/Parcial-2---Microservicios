import time

import httpx
import jwt
from fastapi.testclient import TestClient

from common import correlation


def _gateway(service):
    gw = service("gateway", JWT_SECRET="secreto-test", RATE_LIMIT=1000)
    seen = []

    def upstream(request):
        seen.append(request.headers.get(correlation.HEADER))
        return httpx.Response(200, json={})

    gw.client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    token = jwt.encode({"sub": "2", "role": "student", "exp": int(time.time()) + 60}, "secreto-test",
                       algorithm="HS256")
    return TestClient(gw.app), {"Authorization": f"Bearer {token}"}, seen


def test_gateway_crea_el_id_y_lo_reenvia(service):
    client, auth, seen = _gateway(service)
    r = client.get("/courses", headers=auth)
    cid = r.headers[correlation.HEADER]
    assert cid and seen == [cid]


def test_gateway_respeta_un_id_valido_del_cliente(service):
    client, auth, seen = _gateway(service)
    r = client.get("/courses", headers={**auth, correlation.HEADER: "flujo-demo-1"})
    assert r.headers[correlation.HEADER] == "flujo-demo-1" and seen == ["flujo-demo-1"]


def test_gateway_reemplaza_un_id_malicioso(service):
    client, auth, seen = _gateway(service)
    r = client.get("/courses", headers={**auth, correlation.HEADER: "x" * 500})
    assert len(r.headers[correlation.HEADER]) == 12 and seen == [r.headers[correlation.HEADER]]


def test_llamadas_entre_servicios_llevan_el_id(monkeypatch):
    from common import http_client

    seen = []
    client = http_client.make_client()
    client._transport = httpx.MockTransport(lambda req: (seen.append(req.headers.get(correlation.HEADER)),
                                                          httpx.Response(200))[1])
    token = correlation.set("abc123")
    try:
        client.get("http://127.0.0.1:9/x")
    finally:
        correlation.reset(token)
    assert seen == ["abc123"]


def test_el_outbox_guarda_el_id_en_el_evento(service):
    import json

    from common.events import Outbox

    pay = service("payment")
    token = correlation.set("abc123")
    try:
        with pay.get_db(pay.DB) as c:
            Outbox.add(c, "pago.confirmado", {"enrollment_id": 1})
    finally:
        correlation.reset(token)
    with pay.get_db(pay.DB) as c:
        payload = json.loads(c.execute("SELECT payload FROM outbox").fetchone()["payload"])
    assert payload["correlation_id"] == "abc123"
