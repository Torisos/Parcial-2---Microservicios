"""Cliente HTTP síncrono con la búsqueda DNS limitada en el tiempo.

Problema: httpx síncrono (httpcore) conecta con socket.create_connection(), y la búsqueda DNS que
hace por dentro (getaddrinfo) NO respeta el timeout de conexión. En Docker, cuando un contenedor
está detenido, buscar su nombre puede quedarse esperando varios segundos. Enrollment o Teacher se
quedaban colgados más de lo que espera el Gateway (5 s) y el cliente recibía 504. Además, la
matrícula seguía en SOLICITADA mientras tanto (los reintentos daban 409) y el Circuit Breaker no
llegaba a abrirse.

El Gateway no tenía el problema porque su cliente es asíncrono y ahí el DNS sí queda dentro del
timeout de conexión (por eso los 503 del Gateway tardan exactamente 2,0 s).

Solución: antes de cada petición se resuelve el nombre en un hilo aparte y se espera como máximo
'dns_timeout'. Si no responde a tiempo, se lanza httpx.ConnectTimeout: la petición no salió, así
que quien llama lo trata como "servicio no disponible" (y el Circuit Breaker lo cuenta).
"""
import ipaddress
import socket
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout

import httpx

from common import correlation

# Hilos para las búsquedas DNS. Si alguna queda colgada, el hilo sigue ocupado hasta que el
# sistema operativo se rinde, pero quien llamó ya recibió su error a tiempo.
_dns_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="dns")


def _is_ip(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False


class BoundedDNSTransport(httpx.HTTPTransport):
    def __init__(self, dns_timeout: float, **kwargs):
        super().__init__(**kwargs)
        self.dns_timeout = dns_timeout

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        host = request.url.host
        if host and not _is_ip(host):
            port = request.url.port or (443 if request.url.scheme == "https" else 80)
            future = _dns_pool.submit(socket.getaddrinfo, host, port, 0, socket.SOCK_STREAM)
            try:
                future.result(timeout=self.dns_timeout)
            except FutureTimeout:
                raise httpx.ConnectTimeout(
                    f"La búsqueda DNS de '{host}' tardó más de {self.dns_timeout} s", request=request
                ) from None
            except OSError as exc:  # socket.gaierror: el nombre no existe
                raise httpx.ConnectError(f"No se pudo resolver '{host}': {exc}", request=request) from exc
        return super().handle_request(request)


def _add_correlation_id(request: httpx.Request):
    cid = correlation.get()
    if cid and correlation.HEADER not in request.headers:
        request.headers[correlation.HEADER] = cid


def make_client(timeout: float = 3.0, connect: float = 1.5) -> httpx.Client:
    """httpx.Client con timeout de lectura 'timeout' y de conexión 'connect' (DNS incluido).
    Reenvía el Correlation ID de la operación en curso a cada servicio que llama."""
    return httpx.Client(
        timeout=httpx.Timeout(timeout, connect=connect),
        transport=BoundedDNSTransport(dns_timeout=connect),
        event_hooks={"request": [_add_correlation_id]},
    )
