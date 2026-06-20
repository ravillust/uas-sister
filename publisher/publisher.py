"""
publisher.py
Simulator/generator event untuk menguji idempotency & dedup.

Perilaku:
- Menghasilkan TOTAL_EVENTS event unik (dikurangi sesuai DUP_RATE).
- Sebagian event dikirim ULANG (duplikat) sesuai DUP_RATE untuk menguji dedup.
- Mendukung mode batch (BATCH_SIZE) dan single.
- Mengukur throughput & melaporkan ringkasan.

Env:
- TARGET_URL    : default http://aggregator:8080/publish
- TOTAL_EVENTS  : jumlah event unik target (default 20000)
- DUP_RATE      : proporsi duplikat tambahan (default 0.3 -> 30%)
- BATCH_SIZE    : ukuran batch per request (default 100; 1 = single)
- NUM_TOPICS    : variasi topik (default 5)
- SYNC          : "1" untuk panggil ?sync=true (proses langsung, deterministik)
"""

import os
import time
import json
import uuid
import random
import asyncio
from datetime import datetime, timezone
from typing import List, Dict, Any

import httpx

TARGET_URL = os.getenv("TARGET_URL", "http://aggregator:8080/publish")
TOTAL_EVENTS = int(os.getenv("TOTAL_EVENTS", "20000"))
DUP_RATE = float(os.getenv("DUP_RATE", "0.3"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
NUM_TOPICS = int(os.getenv("NUM_TOPICS", "5"))
SYNC = os.getenv("SYNC", "0") == "1"
CONCURRENCY = int(os.getenv("CONCURRENCY", "8"))


def make_event(seq: int) -> Dict[str, Any]:
    topic = f"logs.service{seq % NUM_TOPICS}"
    return {
        "topic": topic,
        # event_id collision-resistant: UUID determinstik dari seq + namespace
        "event_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"evt-{seq}")),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": f"publisher-{os.getpid()}",
        "payload": {"seq": seq, "level": random.choice(["INFO", "WARN", "ERROR"]),
                    "msg": f"log line {seq}"},
    }


def build_stream() -> List[Dict[str, Any]]:
    """Bangun daftar event: unik + duplikat acak, lalu diacak urutannya."""
    base = [make_event(i) for i in range(TOTAL_EVENTS)]
    num_dup = int(TOTAL_EVENTS * DUP_RATE)
    dups = [dict(random.choice(base)) for _ in range(num_dup)]
    stream = base + dups
    random.shuffle(stream)
    return stream


async def send_batches(stream: List[Dict[str, Any]]) -> None:
    # Tambahkan ?sync=true dengan benar (pakai & bila URL sudah punya query string).
    if SYNC:
        sep = "&" if "?" in TARGET_URL else "?"
        url = f"{TARGET_URL}{sep}sync=true"
    else:
        url = TARGET_URL
    sem = asyncio.Semaphore(CONCURRENCY)
    batches: List[List[Dict[str, Any]]] = [
        stream[i:i + BATCH_SIZE] for i in range(0, len(stream), BATCH_SIZE)
    ]

    async with httpx.AsyncClient(timeout=60.0) as client:
        async def send_one(batch: List[Dict[str, Any]]):
            async with sem:
                body = batch if BATCH_SIZE > 1 else batch[0]
                for attempt in range(5):
                    try:
                        r = await client.post(url, json=body)
                        if r.status_code < 500:
                            return
                    except Exception:
                        pass
                    await asyncio.sleep(0.2 * (2 ** attempt))  # backoff

        await asyncio.gather(*(send_one(b) for b in batches))


async def main() -> None:
    print(f"[publisher] target={TARGET_URL} total_unik={TOTAL_EVENTS} "
          f"dup_rate={DUP_RATE} batch={BATCH_SIZE} sync={SYNC}")
    stream = build_stream()
    total_sent = len(stream)
    print(f"[publisher] total event dikirim (unik+dup) = {total_sent}")

    t0 = time.time()
    await send_batches(stream)
    elapsed = time.time() - t0

    print(f"[publisher] selesai dalam {elapsed:.2f}s")
    print(f"[publisher] throughput ~ {total_sent / elapsed:.0f} event/s")


if __name__ == "__main__":
    asyncio.run(main())
