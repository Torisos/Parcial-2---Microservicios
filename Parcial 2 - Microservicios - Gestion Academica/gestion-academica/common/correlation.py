"""Correlation ID: un identificador que acompaña una operación por todos los servicios.

Libro (Pacheco, cap. 8, p. 223): en flujos que cruzan varios microservicios, los logs de cada uno
por separado no bastan para ver qué pasó. Se manda un identificador en una cabecera de cada
petición y todos los servicios lo escriben en sus logs.

Cómo viaja en este proyecto:
- El Gateway toma la cabecera X-Correlation-ID del cliente, o crea una si no viene.
- Cada servicio la lee en un middleware y la guarda en una variable de contexto.
- Todas las líneas de log llevan [cid=...].
- Las llamadas HTTP entre servicios (common/http_client.py) la reenvían solas.
- Los eventos del Outbox la llevan en el campo 'correlation_id'. El consumidor la restaura antes
  de procesar el evento, así que lo que el handler registre o publique sigue con el mismo ID.

Con eso, `docker compose logs | findstr <cid>` (o scripts/flujo.py) muestra el recorrido completo
de una matrícula: Gateway → Enrollment → Course → RabbitMQ → Payment → Notification.
"""
import contextvars
import logging
import re
import time
import uuid

HEADER = "X-Correlation-ID"
_NONE = "-"
_cid = contextvars.ContextVar("correlation_id", default=_NONE)
_VALID = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")


def get():
    """Correlation ID actual, o None si no hay operación en curso."""
    value = _cid.get()
    return None if value == _NONE else value


def set(value):
    """Fija el Correlation ID del contexto actual. Devuelve un token para restaurar el anterior."""
    return _cid.set(value or _NONE)


def reset(token):
    _cid.reset(token)


def new():
    return uuid.uuid4().hex[:12]


def clean(value):
    """Acepta el ID del cliente solo si es corto y sin caracteres raros (va a parar a los logs)."""
    return value if value and _VALID.match(value) else None


class _CidFilter(logging.Filter):
    def filter(self, record):
        record.cid = _cid.get()
        return True


def setup_logging(level=logging.INFO):
    """Reemplaza logging.basicConfig: mismo nivel, pero cada línea lleva el Correlation ID."""
    # La hora con milisegundos permite ordenar en una sola línea de tiempo los logs de todos los
    # servicios (todos los contenedores comparten el reloj del host de Docker).
    logging.basicConfig(level=level, datefmt="%H:%M:%S",
                        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s [cid=%(cid)s] %(message)s")
    for handler in logging.getLogger().handlers:
        handler.addFilter(_CidFilter())
    # httpx registra cada petición en INFO; el middleware ya deja una línea por petición con su ID
    logging.getLogger("httpx").setLevel(logging.WARNING)


def install(app, service_name):
    """Middleware de FastAPI: toma o crea el Correlation ID, lo devuelve en la respuesta y
    deja una línea de log por petición (menos /health, para no llenar los logs)."""
    log = logging.getLogger(service_name)

    @app.middleware("http")
    async def correlation_middleware(request, call_next):
        cid = clean(request.headers.get(HEADER)) or new()
        token = _cid.set(cid)
        start = time.perf_counter()
        try:
            response = await call_next(request)
            elapsed = (time.perf_counter() - start) * 1000
            response.headers[HEADER] = cid
            if request.url.path != "/health":
                log.info("%s %s -> %s (%.0f ms)", request.method, request.url.path, response.status_code, elapsed)
            return response
        finally:
            _cid.reset(token)
