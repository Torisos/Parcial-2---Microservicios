"""Course Service: oferta académica y cupos. Incluye una Cache en memoria para la lista de cursos."""
import logging
import os
import threading
import time
from contextlib import asynccontextmanager

from typing import Annotated

from fastapi import FastAPI, Header, HTTPException, Path, Response
from pydantic import BaseModel, Field

from common.correlation import install as install_correlation, setup_logging
from common.db import get_db
from common.errors import install_error_handlers
from common.limits import MAX_ID

setup_logging()  # como basicConfig, pero cada línea lleva [cid=...]
log = logging.getLogger("course")

DB = os.getenv("DB_PATH", "/data/course.db")
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "30"))

_cache = {"data": None, "at": 0.0}
_cache_lock = threading.Lock()

SEED_COURSES = [
    ("ARQ101", "Arquitectura de Software", 3, 1500000, 30),
    ("BD201", "Bases de Datos", 3, 1400000, 25),
    ("RED301", "Redes de Computadores", 4, 1800000, 2),  # capacidad 2 para probar "sin cupos"
]


def invalidate_cache():
    with _cache_lock:
        _cache["data"] = None


def init_db():
    os.makedirs(os.path.dirname(DB), exist_ok=True)
    with get_db(DB) as c:
        c.execute(
            "CREATE TABLE IF NOT EXISTS courses ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE NOT NULL, name TEXT NOT NULL, "
            "credits INTEGER NOT NULL, price REAL NOT NULL, capacity INTEGER NOT NULL, "
            "enrolled INTEGER NOT NULL DEFAULT 0)"
        )
        c.execute(
            "CREATE TABLE IF NOT EXISTS reservations ("
            "enrollment_id INTEGER PRIMARY KEY, course_id INTEGER NOT NULL)"
        )
        # "Lápidas" de la compensación: matrículas cuyo cupo ya se liberó. Si una reserva llega
        # DESPUÉS de su liberación (Enrollment se rindió por timeout pero la petición seguía en
        # curso), se rechaza en vez de gastar un cupo que nadie liberaría.
        c.execute(
            "CREATE TABLE IF NOT EXISTS released ("
            "enrollment_id INTEGER PRIMARY KEY, released_at REAL NOT NULL)"
        )
        if c.execute("SELECT COUNT(*) FROM courses").fetchone()[0] == 0:
            c.executemany(
                "INSERT INTO courses(code, name, credits, price, capacity) VALUES (?, ?, ?, ?, ?)",
                SEED_COURSES,
            )
            log.info("Cursos de ejemplo creados")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    threading.Thread(target=_janitor_loop, daemon=True, name="janitor").start()
    yield


app = FastAPI(title="Course Service", lifespan=lifespan)
install_correlation(app, "course")  # Correlation ID (libro, p. 223)
install_error_handlers(app)


def to_course(r):
    return {
        "id": r["id"],
        "code": r["code"],
        "name": r["name"],
        "credits": r["credits"],
        "price": r["price"],
        "capacity": r["capacity"],
        "available": r["capacity"] - r["enrolled"],
    }


class CourseCreate(BaseModel):
    code: str = Field(min_length=1, max_length=20)
    name: str = Field(min_length=1, max_length=120)
    credits: int = Field(ge=1, le=10)
    price: float = Field(ge=0, le=100_000_000)
    capacity: int = Field(ge=1, le=500)


class ReservationRequest(BaseModel):
    enrollment_id: int = Field(ge=-MAX_ID, le=MAX_ID)


# Janitor: una lápida solo hace falta mientras pueda llegar una reserva tardía (segundos). Se guardan
# 30 días por defecto y luego se borran, para que la tabla no crezca sin límite.
TOMBSTONE_RETENTION = float(os.getenv("TOMBSTONE_RETENTION_SECONDS", str(30 * 24 * 3600)))


def purge_tombstones(older_than_seconds=TOMBSTONE_RETENTION):
    with get_db(DB) as c:
        deleted = c.execute("DELETE FROM released WHERE released_at <= ?",
                            (time.time() - older_than_seconds,)).rowcount
    if deleted:
        log.info("Janitor: %d lápidas viejas borradas", deleted)
    return deleted


def _janitor_loop():
    while True:
        try:
            purge_tombstones()
        except Exception as exc:
            log.warning("Janitor de lápidas falló: %s", exc)
        time.sleep(3600)


@app.get("/health")
def health():
    with get_db(DB) as c:
        tombstones = c.execute("SELECT COUNT(*) FROM released").fetchone()[0]
    return {"service": "course", "status": "ok", "lapidas": tombstones}


@app.get("/courses")
def list_courses(response: Response):
    now = time.time()
    with _cache_lock:
        if _cache["data"] is not None and now - _cache["at"] < CACHE_TTL:
            response.headers["X-Cache"] = "HIT"
            return _cache["data"]
    with get_db(DB) as c:
        rows = c.execute("SELECT * FROM courses ORDER BY id").fetchall()
    data = [to_course(r) for r in rows]
    with _cache_lock:
        _cache["data"] = data
        _cache["at"] = now
    response.headers["X-Cache"] = "MISS"
    return data


@app.get("/courses/{course_id}")
def get_course(course_id: Annotated[int, Path(ge=-MAX_ID, le=MAX_ID)]):
    with get_db(DB) as c:
        row = c.execute("SELECT * FROM courses WHERE id = ?", (course_id,)).fetchone()
    if not row:
        raise HTTPException(404, "El curso no existe")
    return to_course(row)


@app.post("/courses", status_code=201)
def create_course(body: CourseCreate, x_user_role: str = Header(...)):
    if x_user_role != "admin":
        raise HTTPException(403, "Solo el área administrativa puede crear cursos")
    with get_db(DB) as c:
        if c.execute("SELECT 1 FROM courses WHERE code = ?", (body.code,)).fetchone():
            raise HTTPException(409, "Ya existe un curso con ese código")
        cur = c.execute(
            "INSERT INTO courses(code, name, credits, price, capacity) VALUES (?, ?, ?, ?, ?)",
            (body.code, body.name, body.credits, body.price, body.capacity),
        )
        row = c.execute("SELECT * FROM courses WHERE id = ?", (cur.lastrowid,)).fetchone()
    invalidate_cache()
    return to_course(row)


# ---- Endpoints internos: los usa Enrollment, el Gateway NO los expone ----

@app.post("/internal/courses/{course_id}/reserve")
def reserve(course_id: Annotated[int, Path(ge=-MAX_ID, le=MAX_ID)], body: ReservationRequest):
    """Reserva un cupo. Es idempotente: repetir la misma matrícula no gasta otro cupo.
    Si el cupo de esa matrícula ya se liberó (compensación), responde 410 y no reserva."""
    with get_db(DB) as c:
        c.execute("BEGIN IMMEDIATE")
        course = c.execute("SELECT * FROM courses WHERE id = ?", (course_id,)).fetchone()
        if not course:
            raise HTTPException(404, "El curso no existe")
        if c.execute("SELECT 1 FROM released WHERE enrollment_id = ?", (body.enrollment_id,)).fetchone():
            raise HTTPException(410, "La reserva de esta matrícula ya fue compensada")
        already = c.execute(
            "SELECT 1 FROM reservations WHERE enrollment_id = ?", (body.enrollment_id,)
        ).fetchone()
        if not already:
            if course["enrolled"] >= course["capacity"]:
                raise HTTPException(409, "Sin cupos disponibles")
            c.execute(
                "INSERT INTO reservations(enrollment_id, course_id) VALUES (?, ?)",
                (body.enrollment_id, course_id),
            )
            c.execute("UPDATE courses SET enrolled = enrolled + 1 WHERE id = ?", (course_id,))
    invalidate_cache()
    log.info("Cupo reservado: curso %s, matrícula %s", course_id, body.enrollment_id)
    return {"course_id": course_id, "name": course["name"], "price": course["price"]}


@app.post("/internal/courses/{course_id}/release")
def release(course_id: Annotated[int, Path(ge=-MAX_ID, le=MAX_ID)], body: ReservationRequest):
    """Libera el cupo (compensación de la Saga). También es idempotente.
    Deja una lápida aunque no haya reserva: así una reserva que llegue tarde se rechaza."""
    released = False
    with get_db(DB) as c:
        c.execute("BEGIN IMMEDIATE")
        c.execute(
            "INSERT OR IGNORE INTO released(enrollment_id, released_at) VALUES (?, ?)",
            (body.enrollment_id, time.time()),
        )
        row = c.execute(
            "SELECT 1 FROM reservations WHERE enrollment_id = ? AND course_id = ?",
            (body.enrollment_id, course_id),
        ).fetchone()
        if row:
            c.execute("DELETE FROM reservations WHERE enrollment_id = ?", (body.enrollment_id,))
            c.execute("UPDATE courses SET enrolled = MAX(enrolled - 1, 0) WHERE id = ?", (course_id,))
            released = True
    invalidate_cache()
    log.info("Liberación de cupo: curso %s, matrícula %s (%s)", course_id, body.enrollment_id,
             "cupo devuelto" if released else "no había reserva; queda la lápida")
    return {"released": released}
