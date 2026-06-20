#!/usr/bin/env bash
# test_persistence.sh
# Bukti persistensi & crash tolerance:
# 1) kirim event, 2) hapus/recreate container app+db,
# 3) pastikan dedup store masih mencegah reprocessing & data tetap ada.
#
# Jalankan dari root project (folder berisi docker-compose.yml):
#   bash tests/test_persistence.sh

set -euo pipefail
BASE="http://localhost:8080"
EID="persist-$(date +%s)"
EVENT='{"topic":"logs.persist","event_id":"'"$EID"'","timestamp":"2025-01-01T00:00:00Z","source":"persist-test","payload":{"x":1}}'

echo "[1] Kirim event unik (sync)..."
curl -s "$BASE/publish?sync=true" -H 'Content-Type: application/json' -d "$EVENT" | tee /dev/stderr
echo

echo "[2] Verifikasi tersimpan..."
curl -s "$BASE/events?topic=logs.persist" | grep -q "$EID" && echo "OK tersimpan" || (echo "GAGAL"; exit 1)

echo "[3] Recreate container aggregator + storage (data volume tetap)..."
docker compose stop aggregator storage
docker compose rm -f aggregator storage
docker compose up -d storage
sleep 8
docker compose up -d aggregator
echo "    menunggu aggregator ready..."
for i in $(seq 1 30); do
  if curl -fs "$BASE/readyz" >/dev/null 2>&1; then break; fi
  sleep 2
done

echo "[4] Cek data MASIH ADA setelah recreate..."
curl -s "$BASE/events?topic=logs.persist" | grep -q "$EID" && echo "OK data persisten" || (echo "GAGAL: data hilang"; exit 1)

echo "[5] Kirim ulang event yang sama -> harus DUPLICATE (dedup tahan restart)..."
RESP=$(curl -s "$BASE/publish?sync=true" -H 'Content-Type: application/json' -d "$EVENT")
echo "$RESP"
echo "$RESP" | grep -q '"duplicate":1' && echo "OK dedup tahan restart" || (echo "GAGAL: event diproses ulang"; exit 1)

echo "SEMUA CEK PERSISTENSI LULUS."
