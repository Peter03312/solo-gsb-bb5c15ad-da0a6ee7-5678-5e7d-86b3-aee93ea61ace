#!/usr/bin/env python3
"""Physical stocktake (inventory count) demo (standard library only).

A stocktake reconciles a batch's book inventory against a physically
counted available volume, identified by batch id + a unique stocktake id
(X-Stocktake-ID header) + the counted integer nanoliters:

* a shortage moves unreserved available volume into the cumulative
  confirmed loss (lost_nl); reservations and used volume are untouched;
* a surplus only reverses previously confirmed losses -- never the initial
  registration; a surplus beyond the recoverable loss is rejected;
* same stocktake id + same content replays the original sealed record
  (200); same id + different content conflicts (409);
* unknown / recalled batches and negative counts are explicitly rejected;
  a rejected stocktake changes no inventory and seals nothing;
* the per-batch stocktake history stays queryable.

Usage:
    python examples/stocktake_demo.py [base_url]
Default base URL: http://localhost:8000
"""
import json
import sys
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"

with open("examples/inventory.json", encoding="utf-8") as fh:
    PLAN_BODY = json.load(fh)


def call(path, body=None, stocktake_id=None, request_id=None, method=None):
    headers = {"Content-Type": "application/json"}
    if stocktake_id:
        headers["X-Stocktake-ID"] = stocktake_id
    if request_id:
        headers["X-Request-ID"] = request_id
    data = json.dumps(body).encode() if body is not None else None
    if method is None:
        method = "POST" if body is not None else "GET"
    req = urllib.request.Request(BASE + path, data=data, headers=headers,
                                 method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def stocktake(bid, counted, sid):
    return call(f"/batches/{bid}/stocktakes",
                {"counted_available_nl": counted}, stocktake_id=sid)


print("setup: batch S1 (1 mL) and an inventory plan reserving part of it")
call("/batches", {"id": "S1", "concentration": "1000",
                  "initial_volume_nl": 1_000_000})
code, plan = call("/inventory/plans", PLAN_BODY, request_id="st-demo-plan")
assert code == 201, (code, plan)
_, batch = call("/batches/S1")
avail = batch["available_nl"]
print(f"   reserved {batch['reserved_nl']} nL; available {avail} nL")

print("1) counted less than the book available -> loss into lost_nl:")
counted = avail - 900
code, doc = stocktake("S1", counted, "ST-DEMO-001")
print(f"   HTTP {code} kind={doc['kind']} delta={doc['delta_nl']} "
      f"lost {doc['before']['lost_nl']} -> {doc['after']['lost_nl']}")
print("   before:", doc["before"])
print("   after: ", doc["after"])

print("2) same id + same content replays the sealed record (200):")
code, again = stocktake("S1", counted, "ST-DEMO-001")
print("   HTTP", code, "identical:", again == doc)

print("3) same id + different content conflicts (409):")
code, body = stocktake("S1", counted + 1, "ST-DEMO-001")
print("   HTTP", code, body["error"])

print("4) a recount finding volume back reverses confirmed loss (gain):")
code, gain = stocktake("S1", counted + 400, "ST-DEMO-002")
print(f"   HTTP {code} kind={gain['kind']} "
      f"gain_applied_nl={gain['gain_applied_nl']} "
      f"lost {gain['before']['lost_nl']} -> {gain['after']['lost_nl']}")

print("5) surplus beyond the recoverable loss is rejected (409):")
code, body = stocktake("S1", 1_000_001, "ST-DEMO-003")
print("   HTTP", code, body["error"],
      "recoverable:", body["recoverable_loss_nl"],
      "max_counted:", body["max_counted_nl"])
code, _ = call("/stocktakes/ST-DEMO-003")
print("   rejected stocktake sealed nothing: GET ->", code)

print("6) negative count / unknown batch / recalled batch are rejected:")
code, body = stocktake("S1", -5, "ST-DEMO-neg")
print("   negative ->", code, body["error"])
code, body = stocktake("NOPE", 0, "ST-DEMO-nope")
print("   unknown  ->", code, body["error"])
call("/batches", {"id": "S2", "concentration": "100",
                  "initial_volume_nl": 5000})
call("/batches/S2/recall", {"reason": "QC aliquot tested positive"})
code, body = stocktake("S2", 5000, "ST-DEMO-rec")
print("   recalled ->", code, body["error"])

print("7) per-batch stocktake history:")
code, hist = call("/batches/S1/stocktakes")
for d in hist["stocktakes"]:
    print(f"   {d['stocktake_id']}: {d['kind']:8s} delta={d['delta_nl']:+d} "
          f"lost {d['before']['lost_nl']} -> {d['after']['lost_nl']}")

print("8) final batch state (loss is cumulative, reservation untouched):")
_, batch = call("/batches/S1")
print("  ", {k: batch[k] for k in
             ("initial_volume_nl", "reserved_nl", "used_nl", "lost_nl",
              "available_nl")})
