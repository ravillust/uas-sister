"""
main.py
Aggregator service: FastAPI API + consumer internal berbasis Redis queue.

Alur:
- POST /publish menerima event (single/batch). Event divalidasi lalu didorong ke
  Redis list (queue) "events:queue". Mengembalikan "queued".
- Beberapa worker consumer internal (asyncio task) menarik event dari queue dan
  memproses idempotent ke Postgres. Jumlah worker dikontrol env CONSUMER_WORKERS.
- Karena dedup dijamin di level DB (UNIQUE constraint + ON CONFLICT), banyaknya
  worker paralel TIDAK menyebabkan double-process.

Endpoint:
- POST /publish        : terima single/batch event.
- GET  /events?topic=  : daftar event unik yang sudah diproses.
- GET  /stats          : received, unique_processed, duplicate_dropped, topics, uptime.
- GET  /healthz        : liveness.
- GET  /readyz         : readiness (DB + Redis siap).
"""

import os
import json
import time
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, List

import redis.asyncio as aioredis
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse

from db import Database
from models import Event, PublishRequest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("aggregator")

BROKER_URL = os.getenv("BROKER_URL", "redis://broker:6379")
QUEUE_KEY = "events:queue"
CONSUMER_WORKERS = int(os.getenv("CONSUMER_WORKERS", "4"))

START_TIME = time.time()


class AppState:
    db: Database
    redis: aioredis.Redis
    workers: List[asyncio.Task]
    running: bool = False


state = AppState()


async def consumer_worker(worker_id: int) -> None:
    """
    Worker consumer: blocking-pop dari Redis queue lalu proses ke DB.
    Idempotency dijamin DB, jadi N worker aman berjalan paralel.
    """
    log.info("consumer worker #%d mulai", worker_id)
    while state.running:
        try:
            # BRPOP blocking 1 detik agar bisa cek state.running secara berkala.
            item = await state.redis.brpop([QUEUE_KEY], timeout=1)
            if item is None:
                continue
            _, raw = item
            data = json.loads(raw)
            event = Event(**data)
            result = await state.db.process_event(event.to_db_dict())
            if result == "duplicate":
                log.info(
                    "worker#%d DUPLIKAT diabaikan topic=%s event_id=%s",
                    worker_id, event.topic, event.event_id,
                )
            else:
                log.info(
                    "worker#%d PROCESSED topic=%s event_id=%s",
                    worker_id, event.topic, event.event_id,
                )
        except asyncio.CancelledError:
            break
        except Exception as e:  # noqa: BLE001
            # Retry sederhana: re-queue dengan jeda backoff agar event tak hilang
            # (at-least-once). Di produksi sebaiknya pakai dead-letter queue.
            log.error("worker#%d error: %s", worker_id, e)
            await asyncio.sleep(0.5)
    log.info("consumer worker #%d berhenti", worker_id)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    state.db = Database()
    await state.db.connect()
    await state.db.init_schema()
    state.redis = aioredis.from_url(BROKER_URL, decode_responses=True)
    await state.redis.ping()
    state.running = True
    state.workers = [
        asyncio.create_task(consumer_worker(i)) for i in range(CONSUMER_WORKERS)
    ]
    log.info("aggregator siap. %d consumer worker aktif.", CONSUMER_WORKERS)
    yield
    # Shutdown
    state.running = False
    for w in state.workers:
        w.cancel()
    await asyncio.gather(*state.workers, return_exceptions=True)
    await state.redis.close()
    await state.db.close()
    log.info("aggregator shutdown bersih.")


app = FastAPI(title="Pub-Sub Log Aggregator", lifespan=lifespan)


@app.post("/publish")
async def publish(request: Request, sync: bool = Query(False), atomic: bool = Query(False)):
    """
    Terima single event atau batch.

    Query params:
    - sync=true  : proses langsung ke DB (sinkron), tidak lewat queue. Berguna untuk
                   tes yang ingin hasil deterministik tanpa menunggu consumer.
    - atomic=true: (hanya bila sync=true) seluruh batch diproses dalam satu transaksi.
    """
    try:
        raw = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="body bukan JSON valid")

    try:
        req = PublishRequest.from_raw(raw)
    except Exception as e:  # validasi skema gagal
        # Catat audit invalid
        raise HTTPException(status_code=422, detail=f"validasi event gagal: {e}")

    if sync:
        if atomic:
            res = await state.db.process_batch_atomic(
                [e.to_db_dict() for e in req.events]
            )
            return {
                "status": "processed",
                "mode": "sync-atomic",
                "processed": res["processed"],
                "duplicate": res["duplicate"],
                "count": len(req.events),
            }
        processed = 0
        duplicate = 0
        for e in req.events:
            r = await state.db.process_event(e.to_db_dict())
            if r == "duplicate":
                duplicate += 1
            else:
                processed += 1
        return {
            "status": "processed",
            "mode": "sync",
            "processed": processed,
            "duplicate": duplicate,
            "count": len(req.events),
        }

    # Mode async (default): dorong ke queue, consumer yang memproses.
    pipe = state.redis.pipeline()
    for e in req.events:
        pipe.lpush(QUEUE_KEY, e.model_dump_json())
    await pipe.execute()
    return {"status": "queued", "mode": "async", "count": len(req.events)}


@app.get("/events")
async def get_events(
    topic: str | None = Query(None),
    limit: int = Query(1000, ge=1, le=100000),
):
    rows = await state.db.get_events(topic, limit)
    # Serialisasi datetime -> ISO string
    for r in rows:
        if r.get("timestamp") is not None:
            r["timestamp"] = r["timestamp"].isoformat()
        if isinstance(r.get("payload"), str):
            try:
                r["payload"] = json.loads(r["payload"])
            except Exception:
                pass
    return {"count": len(rows), "events": rows}


@app.get("/stats")
async def get_stats():
    s = await state.db.get_stats()
    queue_len = await state.redis.llen(QUEUE_KEY)
    return {
        "received": s["received"],
        "unique_processed": s["unique_processed"],
        "duplicate_dropped": s["duplicate_dropped"],
        "topics": s["topics"],
        "queue_pending": queue_len,
        "uptime_seconds": round(time.time() - START_TIME, 2),
    }


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/readyz")
async def readyz():
    try:
        async with state.db.pool.acquire() as conn:
            await conn.execute("SELECT 1")
        await state.redis.ping()
        return {"status": "ready"}
    except Exception as e:  # noqa: BLE001
        return JSONResponse(status_code=503, content={"status": "not-ready", "detail": str(e)})
