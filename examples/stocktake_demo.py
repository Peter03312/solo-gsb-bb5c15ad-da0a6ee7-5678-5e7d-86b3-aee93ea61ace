#!/usr/bin/env python3
"""Physical-stocktake demo using only the standard library.

Registers a stock batch, confirms one plan and leaves one pending, then
reconciles the book balance with physical counts: a deficit moves from the
unreserved available pool into the cumulative loss, a surplus only offsets
previously confirmed losses, same-id retries replay/conflict, and the
per-batch history is listed at the end.

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


def call(path, body=None, headers=None, method=None):
    hdrs = {"Content-Type": "application/json"}
    hdrs.update(headers or {})
    data = json.dumps(body).encode() if body is not None else None
    if method is None:
        method = "POST" if (body is not None or path.endswith("/confirm")
                            or path.endswith("/cancel")) else "GET"
    req = urllib.request.Request(BASE + path, data=data, headers=hdrs,
                                 method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def stocktake(batch, measured, order_id):
    return call(f"/batches/{batch}/stocktakes",
                {"measured_available_nl": measured},
                {"X-Stocktake-ID": order_id})


def show_batch(label):
    _, b = call("/batches/S1")
    print(f"   {label}: reserved={b['reserved_nl']} used={b['used_nl']} "
          f"loss={b['loss_nl']} available={b['available_nl']}")


print("1) register batch S1 and prepare one confirmed + one pending plan:")
status, _ = call("/batches", {"id": "S1", "concentration": "1000",
                              "initial_volume_nl": 1_000_000})
print("   register ->", status)
call("/inventory/plans", PLAN_BODY, {"X-Request-ID": "st-demo-A"})
call("/inventory/plans/st-demo-A/confirm")          # used
call("/inventory/plans", PLAN_BODY, {"X-Request-ID": "st-demo-B"})  # reserved
show_batch("after plans ")

print("2) deficit count: measured 997700 -> moves into cumulative loss:")
status, body = stocktake("S1", 997_700, "ST-0001")
print("   ->", status, body["kind"], "loss_delta:", body["loss_delta_nl"])
show_batch("after count ")

print("3) same order id + same content -> 200 replay (no double counting):")
status, body = stocktake("S1", 997_700, "ST-0001")
print("   ->", status, "kind:", body["kind"])
show_batch("after replay")

print("4) same order id + different content -> 409 stocktake_conflict:")
status, body = stocktake("S1", 997_000, "ST-0001")
print("   ->", status, body["error"])

print("5) surplus beyond the confirmed loss -> 409 surplus_exceeds_loss:")
status, body = stocktake("S1", 998_000, "ST-0002")
print("   ->", status, body["error"],
      "offsettable:", body["offsettable_loss_nl"])

print("6) surplus within the confirmed loss -> offsets it:")
status, body = stocktake("S1", 997_800, "ST-0003")
print("   ->", status, body["kind"], "loss_delta:", body["loss_delta_nl"])
show_batch("after offset")

print("7) rejected counts change nothing:")
for label, args in [
        ("negative volume", ("S1", -1, "ST-neg")),
        ("unknown batch", ("NOPE", 100, "ST-404"))]:
    status, body = stocktake(*args)
    print(f"   {label} -> {status} {body['error']}")
status, _ = call("/batches/S1/recall", {"reason": "QC positive"})
print("   recall S1 ->", status)
status, body = stocktake("S1", 997_800, "ST-rec")
print("   recalled batch ->", status, body["error"])
show_batch("final       ")

print("8) per-batch stocktake history:")
status, body = call("/batches/S1/stocktakes")
for rec in body["stocktakes"]:
    print(f"   {rec['stocktake_id']}: {rec['kind']} "
          f"measured={rec['measured_available_nl']} "
          f"loss_delta={rec['loss_delta_nl']}")
