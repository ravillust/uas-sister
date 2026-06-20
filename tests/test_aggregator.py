"""
test_aggregator.py
Suite uji untuk Pub-Sub Log Aggregator.

Cara menjalankan (dari host, butuh Compose hidup dengan port 8080 di-expose):
    pip install -r tests/requirements.txt
    BASE_URL=http://localhost:8080 pytest -v tests/

Cakupan:
- Validasi skema event (valid/invalid).
- Dedup: duplikat hanya diproses sekali.
- Idempotency lintas request.
- Konkurensi: banyak worker kirim event sama -> tidak double-process.
- Batch atomic.
- Konsistensi GET /stats dan GET /events.
- Stress kecil + ukur waktu.
- Health/readiness.

Catatan persistensi (restart container) diuji lewat skrip terpisah / manual,
karena memerlukan 'docker compose restart' di luar proses pytest. Lihat
tests/test_persistence.sh dan satu test marker di bawah.
"""

import os
import uuid
import time
import asyncio

import httpx
import pytest

BASE_URL = os.getenv("BASE_URL", "http://localhost:8080")


def uid() -> str:
    return str(uuid.uuid4())


def ev(topic="logs.test", event_id=None, source="pytest", payload=None):
    return {
        "topic": topic,
        "event_id": event_id or uid(),
        "timestamp": "2025-01-01T00:00:00Z",
        "source": source,
        "payload": payload or {"k": "v"},
    }


@pytest.fixture
async def client():
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as c:
        yield c


# ----------------------------------------------------------------------- #
# 1-2. Health & readiness
# ----------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_healthz(client):
    r = await client.get("/healthz")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_readyz(client):
    r = await client.get("/readyz")
    assert r.status_code == 200
    assert r.json()["status"] == "ready"


# ----------------------------------------------------------------------- #
# 3-5. Validasi skema
# ----------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_publish_valid_single(client):
    r = await client.post("/publish?sync=true", json=ev())
    assert r.status_code == 200
    body = r.json()
    assert body["processed"] == 1
    assert body["duplicate"] == 0


@pytest.mark.asyncio
async def test_publish_invalid_missing_field(client):
    bad = {"topic": "x", "timestamp": "2025-01-01T00:00:00Z", "source": "s"}  # tanpa event_id
    r = await client.post("/publish?sync=true", json=bad)
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_publish_invalid_blank_topic(client):
    bad = ev(topic="   ")
    r = await client.post("/publish?sync=true", json=bad)
    assert r.status_code == 422


# ----------------------------------------------------------------------- #
# 6-7. Dedup & idempotency
# ----------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_dedup_same_request_repeated(client):
    e = ev()
    r1 = await client.post("/publish?sync=true", json=e)
    r2 = await client.post("/publish?sync=true", json=e)
    r3 = await client.post("/publish?sync=true", json=e)
    assert r1.json()["processed"] == 1
    assert r2.json()["duplicate"] == 1
    assert r3.json()["duplicate"] == 1


@pytest.mark.asyncio
async def test_dedup_within_batch(client):
    e = ev()
    batch = [e, dict(e), dict(e)]  # 3x event identik dalam satu batch
    r = await client.post("/publish?sync=true", json=batch)
    body = r.json()
    assert body["processed"] == 1
    assert body["duplicate"] == 2


# ----------------------------------------------------------------------- #
# 8. Konkurensi: banyak request paralel event sama -> hanya 1 processed
# ----------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_concurrent_same_event_no_double_process(client):
    e = ev()
    # 30 request paralel membawa event identik
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as c:
        tasks = [c.post("/publish?sync=true", json=e) for _ in range(30)]
        results = await asyncio.gather(*tasks)
    processed = sum(r.json()["processed"] for r in results)
    duplicate = sum(r.json()["duplicate"] for r in results)
    assert processed == 1, f"harus tepat 1 processed, dapat {processed}"
    assert duplicate == 29


# ----------------------------------------------------------------------- #
# 9. Batch atomic
# ----------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_batch_atomic_all_unique(client):
    batch = [ev() for _ in range(50)]
    r = await client.post("/publish?sync=true&atomic=true", json=batch)
    body = r.json()
    assert body["mode"] == "sync-atomic"
    assert body["processed"] == 50
    assert body["duplicate"] == 0


# ----------------------------------------------------------------------- #
# 10-11. GET /events & GET /stats konsisten
# ----------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_get_events_by_topic(client):
    topic = f"logs.topic-{uid()}"
    sent = [ev(topic=topic) for _ in range(10)]
    await client.post("/publish?sync=true", json=sent)
    r = await client.get(f"/events?topic={topic}")
    body = r.json()
    assert body["count"] == 10
    assert all(e["topic"] == topic for e in body["events"])


@pytest.mark.asyncio
async def test_stats_monotonic_increase(client):
    s0 = (await client.get("/stats")).json()
    n = 25
    await client.post("/publish?sync=true", json=[ev() for _ in range(n)])
    s1 = (await client.get("/stats")).json()
    assert s1["received"] >= s0["received"] + n
    assert s1["unique_processed"] >= s0["unique_processed"] + n


@pytest.mark.asyncio
async def test_stats_duplicate_count(client):
    e = ev()
    s0 = (await client.get("/stats")).json()
    await client.post("/publish?sync=true", json=e)
    await client.post("/publish?sync=true", json=e)
    await client.post("/publish?sync=true", json=e)
    s1 = (await client.get("/stats")).json()
    assert s1["duplicate_dropped"] >= s0["duplicate_dropped"] + 2


# ----------------------------------------------------------------------- #
# 12. Async mode (lewat queue) -> eventually processed
# ----------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_async_publish_eventually_processed(client):
    topic = f"logs.async-{uid()}"
    batch = [ev(topic=topic) for _ in range(20)]
    r = await client.post("/publish", json=batch)  # async default
    assert r.json()["status"] == "queued"
    # tunggu consumer
    deadline = time.time() + 15
    count = 0
    while time.time() < deadline:
        body = (await client.get(f"/events?topic={topic}")).json()
        count = body["count"]
        if count == 20:
            break
        await asyncio.sleep(0.5)
    assert count == 20


# ----------------------------------------------------------------------- #
# 13. Stress kecil + ukur waktu
# ----------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_small_stress_with_duplicates(client):
    topic = f"logs.stress-{uid()}"
    base = [ev(topic=topic) for _ in range(200)]
    dups = [dict(base[i % len(base)]) for i in range(100)]  # 100 duplikat
    stream = base + dups
    t0 = time.time()
    # kirim dalam beberapa batch sync
    for i in range(0, len(stream), 50):
        await client.post("/publish?sync=true", json=stream[i:i + 50])
    elapsed = time.time() - t0
    body = (await client.get(f"/events?topic={topic}")).json()
    assert body["count"] == 200  # hanya unik
    assert elapsed < 30
    print(f"\n[stress] 300 event ({elapsed:.2f}s) -> 200 unik tersimpan")


# ----------------------------------------------------------------------- #
# 14. Idempotency lintas mode (async lalu sync event sama)
# ----------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_idempotency_across_modes(client):
    topic = f"logs.mixed-{uid()}"
    e = ev(topic=topic)
    # async dulu
    await client.post("/publish", json=e)
    # tunggu sampai masuk
    deadline = time.time() + 10
    while time.time() < deadline:
        body = (await client.get(f"/events?topic={topic}")).json()
        if body["count"] >= 1:
            break
        await asyncio.sleep(0.3)
    # lalu sync event sama -> harus duplicate
    r = await client.post("/publish?sync=true", json=e)
    assert r.json()["duplicate"] == 1


# ----------------------------------------------------------------------- #
# 15. Multi-topic stats konsisten
# ----------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_multi_topic_stats(client):
    topics = [f"logs.mt-{uid()}" for _ in range(3)]
    for t in topics:
        await client.post("/publish?sync=true", json=[ev(topic=t) for _ in range(5)])
    s = (await client.get("/stats")).json()
    for t in topics:
        assert s["topics"].get(t) == 5
