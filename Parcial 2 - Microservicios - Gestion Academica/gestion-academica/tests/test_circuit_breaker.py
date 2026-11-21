import threading
import time

import pytest

from common.resilience import CircuitBreaker, CircuitOpenError


def boom():
    raise RuntimeError("caído")


def test_se_abre_tras_el_umbral_de_fallos():
    cb = CircuitBreaker(failure_threshold=3, reset_timeout=60)
    for _ in range(3):
        with pytest.raises(RuntimeError):
            cb.call(boom)
    assert cb.state == CircuitBreaker.OPEN
    with pytest.raises(CircuitOpenError):
        cb.call(lambda: "no debería ejecutarse")


def test_half_open_deja_pasar_una_sola_prueba():
    cb = CircuitBreaker(failure_threshold=1, reset_timeout=0.05)
    with pytest.raises(RuntimeError):
        cb.call(boom)
    time.sleep(0.06)

    started, release = threading.Event(), threading.Event()
    calls = []

    def slow_probe():
        calls.append(1)
        started.set()
        release.wait(2)
        return "ok"

    t = threading.Thread(target=lambda: cb.call(slow_probe))
    t.start()
    started.wait(2)
    # mientras la prueba está en curso, las demás llamadas fallan rápido sin tocar el servicio
    with pytest.raises(CircuitOpenError):
        cb.call(slow_probe)
    release.set()
    t.join(2)
    assert len(calls) == 1
    assert cb.state == CircuitBreaker.CLOSED


def test_prueba_fallida_vuelve_a_abrir():
    cb = CircuitBreaker(failure_threshold=1, reset_timeout=0.05)
    with pytest.raises(RuntimeError):
        cb.call(boom)
    time.sleep(0.06)
    with pytest.raises(RuntimeError):
        cb.call(boom)
    assert cb.state == CircuitBreaker.OPEN
    with pytest.raises(CircuitOpenError):
        cb.call(lambda: "ok")
