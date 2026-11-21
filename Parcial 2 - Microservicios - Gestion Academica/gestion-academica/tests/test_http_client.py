import socket
import time

import httpx
import pytest

from common import http_client


@pytest.fixture
def slow_dns(monkeypatch):
    """DNS como el de Docker con el contenedor detenido: 'apagado' tarda 5 s y luego falla."""
    real = socket.getaddrinfo

    def fake(host, *args, **kwargs):
        if host == "apagado":
            time.sleep(5)
            raise socket.gaierror(-3, "Temporary failure in name resolution")
        if host == "inexistente":
            raise socket.gaierror(-2, "Name or service not known")
        return real("127.0.0.1", *args, **kwargs)

    monkeypatch.setattr(http_client.socket, "getaddrinfo", fake)


def test_dns_lento_se_corta_al_timeout_de_conexion(slow_dns):
    client = http_client.make_client(timeout=3.0, connect=0.5)
    start = time.monotonic()
    with pytest.raises(httpx.ConnectTimeout):
        client.get("http://apagado:8000/health")
    assert time.monotonic() - start < 1.5  # antes esperaba lo que tardara el DNS (aquí, 5 s)


def test_nombre_inexistente_es_error_de_conexion(slow_dns):
    with pytest.raises(httpx.ConnectError):
        http_client.make_client().get("http://inexistente:8000/health")


def test_ip_no_pasa_por_dns(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("no debería resolver una IP")

    transport = http_client.BoundedDNSTransport(dns_timeout=0.5)
    monkeypatch.setattr(http_client.socket, "getaddrinfo", boom)
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", lambda self, req: httpx.Response(200))
    assert transport.handle_request(httpx.Request("GET", "http://127.0.0.1:9/health")).status_code == 200
