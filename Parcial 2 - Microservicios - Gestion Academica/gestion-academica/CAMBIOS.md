# Cambios: correcciones posteriores a la segunda corrida

Este documento describe las correcciones aplicadas después de la corrida del 2026-11-01
(fuzzing 90/92, chaos 40/40). Cada corrección tiene una prueba unitaria en `tests/` y se
verificó contra el sistema completo.

## Resumen de resultados

| Prueba | Antes | Después |
|---|---|---|
| Fuzzing (`scripts/fuzz.py`) | 90 de 92 | **92 de 92** |
| Prueba del Mono (`scripts/chaos.py --todos`) | 40 de 40 (1-nov) / 36 de 40 (19-nov, DNS lento) | **40 de 40**, también con DNS lento (ver sección 9) |
| Pruebas unitarias (`tests/`) | no existían | **33 de 33** |
| Chaos Monkey por categorías del libro (`scripts/simian.py --todas`) | no existía | **48 de 48** (4 hallazgos reportados) |
| Verificaciones de integración de las correcciones | 1 de 5 | **5 de 5** |

Entorno de verificación: los 8 servicios corrieron como procesos `uvicorn` locales y RabbitMQ 3.13 en
Docker, no con `docker compose up`. Para imitar a Docker, cada servicio se llamó por su nombre
(`course`, `enrollment`…). Al "detener" un servicio, su nombre dejaba de resolver y el DNS tardaba
6 s en fallar, como Docker Desktop con un contenedor detenido. Los tiempos exactos pueden diferir de
los de tu equipo. Las cifras oficiales para el informe son las de `fuzz.py` y `chaos.py` corridos
con `docker compose up --build`.

## 1. Hallazgo abierto: `%3F` y `%23` en la ruta (Gateway) — CERRADO

**Causa raíz.** El Gateway usaba `request.url.path`. Starlette arma `request.url` a partir de la ruta
*ya decodificada* y la vuelve a analizar: en `/courses/1%3Fx=1` el `?` decodificado parte la ruta en
`/courses/1` + query `x=1`, y con `%23` el resto se vuelve fragmento. El `quote()` que ya existía
llegaba tarde: el resto de la ruta ya se había perdido y Course recibía `/courses/1` (200).

**Corrección.** `path = request.scope["path"]` (la ruta decodificada completa) y luego `quote()`.
Ahora Course recibe `/courses/1%3Fx%3D1` y responde 422.

Archivo: `gateway/main.py`. Prueba: `tests/test_gateway.py`.

## 2. Cupo perdido cuando Course responde tarde (Saga)

**Problema.** Ante cualquier error HTTP al reservar, Enrollment cancelaba la matrícula *sin liberar
el cupo*. Con un timeout de lectura (Course recibió la petición pero no respondió en 3 s) el cupo
quedaba reservado para una matrícula CANCELADA. Es probable bajo carga: Course espera hasta 15 s
por el bloqueo de SQLite y Enrollment solo 3 s.
Verificado congelando Course (SIGSTOP) durante una matrícula: **antes, cupos 30 → 29; después, 30 → 30.**

**Corrección.**
- Enrollment distingue si la petición llegó a Course:
  - Circuito abierto, `ConnectError` o `ConnectTimeout`: la petición no salió, así que se cancela directo.
  - Timeout de lectura, 5xx o conexión cortada: el resultado es incierto. La matrícula pasa a
    **LIBERANDO** (ya no cuenta como activa) y se libera el cupo.
- Course guarda una **lápida** (tabla `released`) en cada liberación. Si una reserva llega *después*
  de su liberación, se rechaza con 410. Así liberar antes de que llegue la reserva tardía es seguro.

Archivos: `enrollment/main.py`, `course/main.py`. Pruebas: `tests/test_enrollment_saga.py`, `tests/test_course.py`.

## 3. Matrículas atascadas en SOLICITADA o LIBERANDO (Saga)

**Problema.** Si Enrollment se caía entre la reserva y el paso a PENDIENTE_PAGO, la matrícula quedaba
en SOLICITADA para siempre. Bloqueaba al estudiante en ese curso (cuenta como activa) y retenía el
cupo, porque `expire_loop` solo revisaba PENDIENTE_PAGO.

**Corrección.** `expire_loop` pasa a ser `reconcile_loop` (cada 10 s) y además:
- termina las liberaciones en **LIBERANDO** (cuando Course tampoco respondía al liberar);
- compensa las **SOLICITADA** con más de `STUCK_REQUEST_SECONDS` (60 s por defecto): libera el cupo
  (idempotente) y cancela.

El paso a PENDIENTE_PAGO ahora es condicional (`AND status='SOLICITADA'`), para no revivir una
matrícula que el reconciliador ya canceló.

## 4. Pago tardío sin reembolso (Saga)

**Problema.** Si el estudiante pagaba justo cuando la matrícula vencía, el pago quedaba PAGADO, la
matrícula CANCELADA y `pago.confirmado` se ignoraba: se cobraba sin matricular.

**Corrección.** Paso de compensación en Payment. Al recibir `matricula.cancelada` con el cobro ya
PAGADO, lo pasa a **REEMBOLSADO** y publica `pago.reembolsado` (por Outbox). Notification avisa al
estudiante. El `UPDATE` condicionado evita reembolsar dos veces si el evento llega repetido.
Verificado: **antes, el pago queda PAGADO sin aviso; después, REEMBOLSADO con aviso.**

Archivos: `payment/main.py`, `notification/main.py`. Prueba: `tests/test_eventos.py`.

## 5. Matrículas duplicadas por carrera

**Problema.** La búsqueda de duplicados y el `INSERT` no eran atómicos.
**Corrección.** `BEGIN IMMEDIATE` toma el bloqueo de escritura antes de buscar duplicados.
**Nota:** en las pruebas, 10 peticiones simultáneas no reprodujeron el error ni antes ni después. Es una
corrección preventiva, no un fallo observado.

## 6. Mensajería: DLQ, idempotencia y confirmaciones (`common/events.py`)

- **Mensajes envenenados → Dead Letter Queue.** Antes, un mensaje que siempre hacía fallar al handler
  se reintentaba para siempre. Ahora se reintenta hasta `EVENT_MAX_RETRIES` (5) veces y después se
  aparta en `<cola>.dlq` (por ejemplo `payment.q.dlq`).
  Verificado: **antes, el mensaje seguía en `payment.q`; después, termina en `payment.q.dlq`.**
- **`TransientError`.** Un fallo por una dependencia caída (por ejemplo Course durante la compensación)
  se reencola sin gastar reintentos, así no termina en la DLQ. El escenario de chaos "Course caído y
  pago fallido" sigue cumpliéndose.
- **`event_id` y Notification idempotente.** Cada evento lleva `event_id = <servicio>:<id del outbox>`.
  Notification tiene un índice único en `event_id` y descarta repetidos (patrón Idempotent Consumer).
  Verificado: **antes, 2 avisos por un evento repetido; después, 1.**
- **Publisher confirms.** Un evento solo se marca como enviado cuando RabbitMQ confirma que lo recibió.
  Antes `publish` devolvía True sin confirmación.
- **Una conexión por cada envío del Outbox**, en vez de una por evento. Además, la base no queda
  bloqueada mientras se habla con el broker.

## 7. Circuit Breaker: una sola llamada de prueba en HALF_OPEN

**Problema.** En HALF_OPEN pasaban todas las llamadas concurrentes, no solo una de prueba.
**Corrección.** Una sola llamada de prueba a la vez; las demás reciben `CircuitOpenError` hasta
conocer el resultado. Archivo: `common/resilience.py`. Prueba: `tests/test_circuit_breaker.py`.

## 8. Rate limiter: limpieza de IPs inactivas (Gateway)

**Problema.** `_hits` guardaba para siempre una entrada por cada IP que hubiera llamado alguna vez.
**Corrección.** Limpieza periódica de las IPs sin peticiones dentro de la ventana. El código anota
que, con varias réplicas del Gateway, el contador tendría que compartirse (por ejemplo en Redis).

## 9. Búsqueda DNS sin límite de tiempo en Enrollment y Teacher (`common/http_client.py`)

**Síntoma** (corrida en Docker del 2026-11-19, 36 de 40). Con Course o Enrollment apagados, el
Gateway respondía **504** en vez de 503. En el escenario `course` se observó `[504, 409, 409, 409]`,
el Circuit Breaker quedó CLOSED y 1 matrícula quedó colgada. El docente también recibió 504 al
consultar sus cursos.

**Causa raíz.** El cliente HTTP síncrono (httpx/httpcore) conecta con `socket.create_connection()`.
La búsqueda DNS que hace por dentro **no respeta el timeout de conexión** (1.5 s). En Docker, buscar
el nombre de un contenedor detenido puede tardar varios segundos, así que Enrollment y Teacher
esperaban más que el Gateway (5 s) y este respondía 504. Mientras tanto:
- la matrícula seguía en SOLICITADA, y los reintentos del estudiante chocaban con ella (409);
- como la primera llamada no terminaba, el Circuit Breaker nunca llegaba a 3 fallos y no se abría.

El Gateway no tenía el problema porque su cliente es asíncrono y ahí el DNS sí queda dentro del
timeout. Por eso sus 503 tardan exactamente 2,0 s.

**No es una regresión de estas correcciones.** Con un DNS que tarda 6 s en fallar (imitando a Docker
Desktop), el código original falla en las mismas 5 comprobaciones que el corregido. En la corrida
del 2026-11-01 el DNS de Docker respondió más rápido y la debilidad no se notó. Depende del equipo y
del momento.

**Corrección.** `make_client()` crea el `httpx.Client` con un transporte que primero resuelve el
nombre en un hilo aparte y espera como máximo el timeout de conexión. Si no responde, lanza
`httpx.ConnectTimeout`: la petición no salió, se cancela directo y el Circuit Breaker cuenta el
fallo. Se usa en Enrollment y Teacher, los dos servicios con cliente síncrono.

**Verificado con el DNS lento:** el escenario `course` da `[503, 503, 503, 503]` y el breaker OPEN.
El docente recibe `200 [False, False]`. Chaos completo 40 de 40, fuzzing 92 de 92 y las 5
verificaciones de integración cumplidas. Prueba: `tests/test_http_client.py`.

## 10. Parcial 2: Correlation ID, evidencia del flujo y las 6 categorías de Chaos Monkey

Basado en Pacheco, *Microservice Patterns and Best Practices*.

### Correlation ID (libro, cap. 8, p. 223) — `common/correlation.py`
- El Gateway toma la cabecera `X-Correlation-ID` del cliente, o crea una si no viene o es inválida
  (más de 64 caracteres o con caracteres raros), y la reenvía a los servicios.
- Cada servicio la guarda en un middleware y la escribe en **todas** sus líneas de log como `[cid=...]`,
  con la hora en milisegundos.
- Las llamadas entre servicios (`make_client`) la reenvían solas. Los hilos del Aggregator la conservan
  (`contextvars.copy_context`).
- Los eventos del Outbox la llevan en `correlation_id`, y el consumidor la restaura antes de procesar.
  Así, un mismo ID muestra el recorrido completo, incluido lo que viajó por RabbitMQ.
- Se agregaron líneas de log en cada paso de negocio: SOLICITADA, cupo reservado, PENDIENTE_PAGO,
  evento publicado/recibido, PAGADO, CONFIRMADA, cupo liberado, CANCELADA.

### Evidencia del flujo — `scripts/flujo.py`
`python scripts/flujo.py` (flujo feliz) y `python scripts/flujo.py --fallido` (Saga con compensación).
El script hace la matrícula completa con un mismo Correlation ID y muestra cada paso visto desde la
API. Después lee `docker compose logs`, deja solo las líneas con ese ID y las ordena por hora.
Resultado: el recorrido real servicio por servicio. Se guarda en `resultados/flujo_<fecha>.md`.
El aviso de Notification se busca por nombre del curso y solo entre los creados durante el flujo
(la primera versión podía mostrar un aviso anterior de otra prueba; los logs siempre fueron correctos).

### Las 6 categorías de Chaos Monkey (libro, cap. 12, p. 304) — `scripts/simian.py`
| Categoría | Qué hace en este proyecto |
|---|---|
| Chaos Gorilla | Cae una "zona" completa: primero el núcleo (Course + Enrollment + Payment), luego la mensajería (RabbitMQ + Payment + Notification). Comprueba que el resto siga atendiendo y que todo se recupere sin perder datos. |
| Conformity Monkey | Revisa buenas prácticas: `/health` en todos, un solo punto de entrada, base de datos propia por servicio, versiones fijas, colas durables con DLQ y cadenas síncronas de un salto (verificado con el Correlation ID). |
| Doctor Monkey | Salud de los 8 servicios, breakers, outbox sin atascos, consumidores conectados y latencia (promedio, p95 y máximo, como recomienda la p. 316). |
| Janitor Monkey | Genera recursos sin uso (eventos ya enviados, lápidas), los limpia y comprueba que los pendientes no se toquen y que el sistema siga funcionando. |
| Latency Monkey | Congela (`docker compose pause`) Course, Payment, RabbitMQ y Student para meter demoras reales, y comprueba tiempos acotados y cero datos perdidos. |
| Security Monkey | Puertos expuestos, rutas internas, tokens manipulados, suplantación de rol, hash de contraseñas y ráfaga contra DoS. Además reporta hallazgos de configuración. |

### Cambios de soporte
- **Janitor:** el Outbox guarda `sent_at` y borra cada hora los eventos ya enviados con más de
  `OUTBOX_RETENTION_SECONDS` (7 días). Course borra las lápidas con más de `TOMBSTONE_RETENTION_SECONDS`
  (30 días). Los eventos pendientes nunca se borran.
- **Doctor:** `/health` de Enrollment y Payment informa `outbox` (pendientes y enviados). Enrollment,
  Payment y Notification informan `consumers` (cola conectada o no). Course informa `lapidas`.
- **Security:** los puertos de RabbitMQ (5672 y 15672) ahora solo escuchan en `127.0.0.1`. Siguen
  sirviendo desde el propio equipo (panel web y scripts), pero no quedan expuestos a la red.

## Nuevo estado de matrícula

`SOLICITADA → PENDIENTE_PAGO → CONFIRMADA`, con salidas a `CANCELADA`. Se añade **LIBERANDO**: la
reserva tuvo un resultado incierto y el cupo se está devolviendo. No cuenta como matrícula activa.

## Pruebas unitarias

```
pip install -r requirements-dev.txt
python -m pytest tests
```

No necesitan Docker ni RabbitMQ: cada servicio se carga con una base de datos temporal y las llamadas
HTTP se simulan. Cubren el Gateway, Course, la Saga de Enrollment, Payment, Notification, el Outbox,
el Circuit Breaker, el cliente HTTP, el Correlation ID y el Janitor. Con el código anterior, las pruebas de las correcciones fallan.

## Compatibilidad

- Sin dependencias nuevas en los servicios: los `Dockerfile` y `requirements.txt` no cambian.
- Las bases existentes se migran solas: tabla `released` en Course, columna `event_id` en Notification.
- Las colas existentes no cambian de argumentos. La DLQ es una cola aparte, así que no hace falta
  borrar volúmenes ni colas de RabbitMQ.
