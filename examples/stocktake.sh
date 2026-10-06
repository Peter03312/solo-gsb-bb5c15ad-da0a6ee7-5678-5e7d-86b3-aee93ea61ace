#!/usr/bin/env bash
# Physical stocktake (inventory count) demo against a locally running
# service (docker compose up).
#
# Registers a stock batch, reserves some of it with an inventory plan, then
# reconciles the book inventory against physical counts:
#   * a shortage moves unreserved available volume into the cumulative
#     confirmed loss (lost_nl) -- reservations and used volume untouched;
#   * a surplus only reverses previously confirmed losses (never raises the
#     initial registration); a surplus beyond the recoverable loss is
#     rejected with 409 uncoverable_surplus;
#   * same stocktake id + same content replays the original sealed record
#     (200); same id + different content conflicts (409);
#   * unknown / recalled batches and negative counts are explicitly
#     rejected; a rejected stocktake changes no inventory and seals nothing;
#   * the per-batch stocktake history stays queryable.
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"
post() { curl -sS -X POST "$BASE$1" "${@:2}"; }
avail() { curl -sS "$BASE/batches/$1" | python3 -c "import sys,json;print(json.load(sys.stdin)['available_nl'])"; }

echo "== register batch S1 (1000 uM, 1 mL) =="
post /batches -H "Content-Type: application/json" \
  -d '{"id":"S1","concentration":"1000","initial_volume_nl":1000000}' \
  | python3 -m json.tool

echo "== an inventory plan reserves part of the batch =="
post /inventory/plans -H "Content-Type: application/json" \
  -H "X-Request-ID: stocktake-demo-plan" --data @examples/inventory.json \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(' reserved:', d['reservations'])"

AVAIL=$(avail S1)
COUNTED=$((AVAIL - 900))
echo "== stocktake ST-2026-001: counted $COUNTED of $AVAIL available -> 201 loss =="
post /batches/S1/stocktakes -H "Content-Type: application/json" \
  -H "X-Stocktake-ID: ST-2026-001" \
  -d "{\"counted_available_nl\": $COUNTED}" | python3 -m json.tool

echo "== same id + same content -> 200 (original record, no double count) =="
curl -sS -o /dev/null -w "%{http_code}\n" -X POST "$BASE/batches/S1/stocktakes" \
  -H "Content-Type: application/json" -H "X-Stocktake-ID: ST-2026-001" \
  -d "{\"counted_available_nl\": $COUNTED}"

echo "== same id + different content -> 409 stocktake_conflict =="
curl -sS -X POST "$BASE/batches/S1/stocktakes" \
  -H "Content-Type: application/json" -H "X-Stocktake-ID: ST-2026-001" \
  -d "{\"counted_available_nl\": $((COUNTED + 1))}" | python3 -m json.tool

echo "== recount finds 400 nL back -> 201 gain reverses confirmed loss =="
post /batches/S1/stocktakes -H "Content-Type: application/json" \
  -H "X-Stocktake-ID: ST-2026-002" \
  -d "{\"counted_available_nl\": $((COUNTED + 400))}" | python3 -m json.tool

echo "== surplus beyond the recoverable loss -> 409 uncoverable_surplus =="
curl -sS -X POST "$BASE/batches/S1/stocktakes" \
  -H "Content-Type: application/json" -H "X-Stocktake-ID: ST-2026-003" \
  -d '{"counted_available_nl": 1000001}' | python3 -m json.tool
echo "   ... the rejected id was NOT sealed (GET -> 404):"
curl -sS -o /dev/null -w "   GET /stocktakes/ST-2026-003 -> %{http_code}\n" \
  "$BASE/stocktakes/ST-2026-003"

echo "== negative count -> 400 =="
curl -sS -o /dev/null -w "%{http_code}\n" -X POST "$BASE/batches/S1/stocktakes" \
  -H "Content-Type: application/json" -H "X-Stocktake-ID: ST-neg" \
  -d '{"counted_available_nl": -5}'

echo "== unknown batch -> 404 unknown_batch =="
curl -sS -o /dev/null -w "%{http_code}\n" -X POST "$BASE/batches/NOPE/stocktakes" \
  -H "Content-Type: application/json" -H "X-Stocktake-ID: ST-x" \
  -d '{"counted_available_nl": 0}'

echo "== recalled batch -> 409 batch_recalled =="
post /batches -H "Content-Type: application/json" \
  -d '{"id":"S2","concentration":"100","initial_volume_nl":5000}' > /dev/null
post /batches/S2/recall -H "Content-Type: application/json" \
  -d '{"reason":"QC aliquot tested positive"}' > /dev/null
curl -sS -X POST "$BASE/batches/S2/stocktakes" \
  -H "Content-Type: application/json" -H "X-Stocktake-ID: ST-rec" \
  -d '{"counted_available_nl": 5000}' | python3 -m json.tool

echo "== per-batch stocktake history =="
curl -sS "$BASE/batches/S1/stocktakes" | python3 -m json.tool

echo "== single sealed record by id =="
curl -sS "$BASE/stocktakes/ST-2026-001" | python3 -m json.tool

echo "== batch now carries the cumulative confirmed loss =="
curl -sS "$BASE/batches/S1" | python3 -m json.tool
