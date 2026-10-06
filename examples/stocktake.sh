#!/usr/bin/env bash
# Physical-stocktake demo against a locally running service
# (docker compose up).
#
# Registers a stock batch, confirms one plan (used volume) and leaves one
# plan pending (reserved volume), then reconciles the book balance with
# physical counts:
#   * a deficit moves from the unreserved available pool into the
#     cumulative loss (reserved/used/initial never change), and the
#     before/after snapshot is sealed atomically with the balance change;
#   * same order id + same content replays the original record (200);
#     same order id + different content conflicts (409);
#   * a surplus only offsets losses confirmed by earlier stocktakes --
#     anything beyond that is rejected (409 surplus_exceeds_loss);
#   * negative volumes, unknown batches and recalled batches are rejected
#     without changing inventory;
#   * the per-batch stocktake history is queryable.
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"

post() { curl -sS -X POST "$BASE$1" "${@:2}"; }

echo "== register batch S1 (1000 uM, 1,000,000 nL) =="
post /batches -H "Content-Type: application/json" \
  -d '{"id":"S1","concentration":"1000","initial_volume_nl":1000000}' \
  | python3 -m json.tool

echo "== plan A: CONFIRMED (stock moves to used) =="
post /inventory/plans -H "Content-Type: application/json" \
  -H "X-Request-ID: stocktake-demo-A" --data @examples/inventory.json > /dev/null
post /inventory/plans/stocktake-demo-A/confirm > /dev/null
echo "  (stocktake-demo-A confirmed)"

echo "== plan B: stays RESERVED (pending) =="
post /inventory/plans -H "Content-Type: application/json" \
  -H "X-Request-ID: stocktake-demo-B" --data @examples/inventory.json > /dev/null
echo "  (stocktake-demo-B reserved)"

echo "== book balance before the count =="
curl -sS "$BASE/batches/S1" | python3 -m json.tool

echo
echo "== STOCKTAKE ST-0001: measured 997700 -> deficit 100 into loss_nl =="
post /batches/S1/stocktakes -H "Content-Type: application/json" \
  -H "X-Stocktake-ID: ST-0001" -d '{"measured_available_nl":997700}' \
  | python3 -m json.tool

echo "== batch after the count: loss 100, reserved/used/initial untouched =="
curl -sS "$BASE/batches/S1" | python3 -m json.tool

echo "== retry ST-0001 with the SAME content -> 200, original record =="
curl -sS -o /dev/null -w "%{http_code}\n" -X POST "$BASE/batches/S1/stocktakes" \
  -H "Content-Type: application/json" -H "X-Stocktake-ID: ST-0001" \
  -d '{"measured_available_nl":997700}'

echo "== ST-0001 with DIFFERENT content -> 409 stocktake_conflict =="
post /batches/S1/stocktakes -H "Content-Type: application/json" \
  -H "X-Stocktake-ID: ST-0001" -d '{"measured_available_nl":997000}' \
  | python3 -m json.tool

echo "== surplus beyond the confirmed loss -> 409 surplus_exceeds_loss =="
post /batches/S1/stocktakes -H "Content-Type: application/json" \
  -H "X-Stocktake-ID: ST-0002" -d '{"measured_available_nl":998000}' \
  | python3 -m json.tool

echo "== surplus within the confirmed loss -> 201, loss offset back to 0 =="
post /batches/S1/stocktakes -H "Content-Type: application/json" \
  -H "X-Stocktake-ID: ST-0003" -d '{"measured_available_nl":997800}' \
  | python3 -m json.tool

echo "== negative volume -> 400 invalid_request =="
curl -sS -o /dev/null -w "%{http_code}\n" -X POST "$BASE/batches/S1/stocktakes" \
  -H "Content-Type: application/json" -H "X-Stocktake-ID: ST-neg" \
  -d '{"measured_available_nl":-5}'
echo

echo "== unknown batch -> 404 unknown_batch =="
curl -sS -X POST "$BASE/batches/NOPE/stocktakes" \
  -H "Content-Type: application/json" -H "X-Stocktake-ID: ST-404" \
  -d '{"measured_available_nl":100}' | python3 -m json.tool

echo "== recalled batch -> 409 batch_recalled =="
post /batches -H "Content-Type: application/json" \
  -d '{"id":"S2","concentration":"1000","initial_volume_nl":5000}' > /dev/null
post /batches/S2/recall -H "Content-Type: application/json" \
  -d '{"reason":"QC aliquot tested positive for contamination"}' > /dev/null
curl -sS -X POST "$BASE/batches/S2/stocktakes" \
  -H "Content-Type: application/json" -H "X-Stocktake-ID: ST-rec" \
  -d '{"measured_available_nl":4900}' | python3 -m json.tool

echo "== per-batch stocktake history =="
curl -sS "$BASE/batches/S1/stocktakes" | python3 -m json.tool

echo "== fetch one sealed record by order id =="
curl -sS "$BASE/stocktakes/ST-0001" | python3 -m json.tool
