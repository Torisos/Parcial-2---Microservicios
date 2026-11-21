"""Utilidades para las pruebas unitarias.

Cada servicio tiene un main.py: se cargan con un nombre de módulo distinto y con su propia base
de datos temporal, sin RabbitMQ ni los demás servicios (las llamadas HTTP se simulan).
"""
import importlib.util
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def load_service(name, db_path, **env):
    os.environ["DB_PATH"] = str(db_path)
    os.environ.update({k: str(v) for k, v in env.items()})
    spec = importlib.util.spec_from_file_location(f"svc_{name}_{abs(hash(str(db_path)))}",
                                                  os.path.join(ROOT, name, "main.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if hasattr(module, "init_db"):
        module.init_db()
    return module


@pytest.fixture
def service(tmp_path):
    def _load(name, **env):
        return load_service(name, tmp_path / f"{name}.db", **env)
    return _load
