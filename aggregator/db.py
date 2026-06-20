"""
db.py
Lapisan akses database (Postgres) untuk aggregator.

Berisi:
- Pembuatan connection pool asyncpg.
- Inisialisasi skema (tabel processed_events, stats, outbox, audit_log).
- Helper transaksi untuk dedup idempotent dan update statistik bebas lost-update.

Catatan desain (Bab 8-9):
- Dedup dijamin oleh UNIQUE constraint (topic, event_id) pada tabel processed_events.
- Insert dilakukan dengan "INSERT ... ON CONFLICT DO NOTHING" sehingga dua worker
  paralel tidak mungkin memproses event yang sama dua kali (idempotent atomik).
- Statistik diupdate dengan "UPDATE ... SET count = count + 1" di dalam transaksi
  yang sama untuk mencegah lost-update.
"""

import os
import asyncio
import asyncpg
from typing import Optional, List, Dict, Any

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgres://user:pass@storage:5432/db",
)

# asyncpg butuh skema "postgresql://", bukan "postgres://"
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)


SCHEMA_SQL = """
-- Tabel utama dedup store. UNIQUE (topic, event_id) adalah inti idempotency.
CREATE TABLE IF NOT EXISTS processed_events (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT        NOT NULL,
    event_id     TEXT        NOT NULL,
    timestamp    TIMESTAMPTZ NOT NULL,
    source       TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    seq          BIGINT      NOT NULL,          -- monotonic counter untuk ordering praktis
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_topic_event UNIQUE (topic, event_id)
);

CREATE INDEX IF NOT EXISTS idx_events_topic ON processed_events (topic);
CREATE INDEX IF NOT EXISTS idx_events_seq   ON processed_events (seq);

-- Counter monotonic global (logical/monotonic ordering, Bab 5).
CREATE TABLE IF NOT EXISTS seq_counter (
    id       INT PRIMARY KEY DEFAULT 1,
    current  BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT one_row CHECK (id = 1)
);
INSERT INTO seq_counter (id, current) VALUES (1, 0)
    ON CONFLICT (id) DO NOTHING;

-- Statistik agregat. Diupdate transaksional agar bebas lost-update.
CREATE TABLE IF NOT EXISTS stats (
    id                INT PRIMARY KEY DEFAULT 1,
    received          BIGINT NOT NULL DEFAULT 0,
    unique_processed  BIGINT NOT NULL DEFAULT 0,
    duplicate_dropped BIGINT NOT NULL DEFAULT 0,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT one_stats CHECK (id = 1)
);
INSERT INTO stats (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

-- Audit log untuk observability (deteksi duplikat, dll).
CREATE TABLE IF NOT EXISTS audit_log (
    id        BIGSERIAL PRIMARY KEY,
    ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
    event_kind TEXT NOT NULL,                   -- 'processed' | 'duplicate' | 'invalid'
    topic     TEXT,
    event_id  TEXT,
    detail    TEXT
);
"""


class Database:
    """Wrapper sederhana di atas asyncpg pool."""

    def __init__(self) -> None:
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self, retries: int = 30, delay: float = 2.0) -> None:
        """Buat pool, dengan retry karena Postgres bisa belum siap saat startup."""
        last_err: Optional[Exception] = None
        for attempt in range(retries):
            try:
                self.pool = await asyncpg.create_pool(
                    dsn=DATABASE_URL,
                    min_size=2,
                    max_size=20,
                    command_timeout=30,
                )
                # Verifikasi koneksi
                async with self.pool.acquire() as conn:
                    await conn.execute("SELECT 1")
                return
            except Exception as e:  # noqa: BLE001
                last_err = e
                await asyncio.sleep(delay)
        raise RuntimeError(f"Gagal connect ke Postgres setelah {retries}x: {last_err}")

    async def init_schema(self) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute(SCHEMA_SQL)

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()

    # ------------------------------------------------------------------ #
    # Operasi inti: proses satu event secara idempotent dalam transaksi.
    # ------------------------------------------------------------------ #
    async def process_event(self, event: Dict[str, Any]) -> str:
        """
        Memproses satu event secara idempotent.

        Mengembalikan:
            "processed"  -> event baru, berhasil disimpan.
            "duplicate"  -> event (topic, event_id) sudah ada, diabaikan.

        Seluruh operasi (ambil seq, insert event, update stats) dilakukan dalam
        SATU transaksi sehingga atomik dan bebas race condition.
        """
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            # SERIALIZABLE tidak wajib di sini karena UNIQUE constraint sudah
            # menjamin atomisitas dedup. READ COMMITTED + ON CONFLICT cukup dan
            # lebih murah (lihat penjelasan isolation di laporan).
            async with conn.transaction(isolation="read_committed"):
                # Ambil & naikkan counter monotonic. Baris dikunci oleh UPDATE
                # sehingga seq selalu unik & meningkat (mencegah lost-update).
                seq = await conn.fetchval(
                    "UPDATE seq_counter SET current = current + 1 "
                    "WHERE id = 1 RETURNING current"
                )

                # Insert idempotent. Jika (topic, event_id) konflik -> tidak insert.
                row = await conn.fetchrow(
                    """
                    INSERT INTO processed_events
                        (topic, event_id, timestamp, source, payload, seq)
                    VALUES ($1, $2, $3, $4, $5::jsonb, $6)
                    ON CONFLICT (topic, event_id) DO NOTHING
                    RETURNING id
                    """,
                    event["topic"],
                    event["event_id"],
                    event["timestamp"],
                    event["source"],
                    event["payload_json"],
                    seq,
                )

                if row is None:
                    # Konflik -> duplikat. received & duplicate_dropped naik.
                    await conn.execute(
                        "UPDATE stats SET received = received + 1, "
                        "duplicate_dropped = duplicate_dropped + 1 WHERE id = 1"
                    )
                    await conn.execute(
                        "INSERT INTO audit_log (event_kind, topic, event_id, detail) "
                        "VALUES ('duplicate', $1, $2, 'event diabaikan (idempotent)')",
                        event["topic"],
                        event["event_id"],
                    )
                    return "duplicate"

                # Event baru. received & unique_processed naik.
                await conn.execute(
                    "UPDATE stats SET received = received + 1, "
                    "unique_processed = unique_processed + 1 WHERE id = 1"
                )
                await conn.execute(
                    "INSERT INTO audit_log (event_kind, topic, event_id, detail) "
                    "VALUES ('processed', $1, $2, 'event baru diproses')",
                    event["topic"],
                    event["event_id"],
                )
                return "processed"

    async def process_batch_atomic(self, events: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Memproses batch event dalam SATU transaksi (batch atomic).
        Jika ada exception di tengah, seluruh batch di-rollback.
        Duplikat di dalam batch tetap diperlakukan idempotent (ON CONFLICT).
        """
        assert self.pool is not None
        processed = 0
        duplicate = 0
        async with self.pool.acquire() as conn:
            async with conn.transaction(isolation="read_committed"):
                for event in events:
                    seq = await conn.fetchval(
                        "UPDATE seq_counter SET current = current + 1 "
                        "WHERE id = 1 RETURNING current"
                    )
                    row = await conn.fetchrow(
                        """
                        INSERT INTO processed_events
                            (topic, event_id, timestamp, source, payload, seq)
                        VALUES ($1, $2, $3, $4, $5::jsonb, $6)
                        ON CONFLICT (topic, event_id) DO NOTHING
                        RETURNING id
                        """,
                        event["topic"],
                        event["event_id"],
                        event["timestamp"],
                        event["source"],
                        event["payload_json"],
                        seq,
                    )
                    if row is None:
                        duplicate += 1
                    else:
                        processed += 1

                await conn.execute(
                    "UPDATE stats SET received = received + $1, "
                    "unique_processed = unique_processed + $2, "
                    "duplicate_dropped = duplicate_dropped + $3 WHERE id = 1",
                    len(events),
                    processed,
                    duplicate,
                )
        return {"processed": processed, "duplicate": duplicate}

    # ------------------------------------------------------------------ #
    # Query untuk endpoint GET.
    # ------------------------------------------------------------------ #
    async def get_events(self, topic: Optional[str], limit: int) -> List[Dict[str, Any]]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            if topic:
                rows = await conn.fetch(
                    "SELECT topic, event_id, timestamp, source, payload, seq "
                    "FROM processed_events WHERE topic = $1 "
                    "ORDER BY seq ASC LIMIT $2",
                    topic,
                    limit,
                )
            else:
                rows = await conn.fetch(
                    "SELECT topic, event_id, timestamp, source, payload, seq "
                    "FROM processed_events ORDER BY seq ASC LIMIT $1",
                    limit,
                )
            return [dict(r) for r in rows]

    async def get_stats(self) -> Dict[str, Any]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT received, unique_processed, duplicate_dropped, started_at "
                "FROM stats WHERE id = 1"
            )
            topics = await conn.fetch(
                "SELECT topic, COUNT(*) AS cnt FROM processed_events GROUP BY topic"
            )
            return {
                "received": row["received"],
                "unique_processed": row["unique_processed"],
                "duplicate_dropped": row["duplicate_dropped"],
                "started_at": row["started_at"],
                "topics": {t["topic"]: t["cnt"] for t in topics},
            }
