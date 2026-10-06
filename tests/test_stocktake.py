"""Physical stocktake (inventory count) tests.

A stocktake reconciles one batch's book balance with the physically measured
unreserved volume (integer nL), keyed by batch id + a unique stocktake order
id (``X-Stocktake-ID``):

* a deficit moves from the *unreserved* available pool into the cumulative
  loss (``loss_nl``); reserved and used volumes never change;
* a surplus only offsets losses confirmed by earlier stocktakes -- it can
  never grow the initial registration, used volume or reservations;
* the balance change and the before/after snapshot are sealed atomically;
  same order id + same content replays the original record (200), same id +
  different content conflicts (409);
* recalled batches, unknown batches, negative/out-of-range volumes and
  surpluses beyond the offsettable loss are rejected without changing
  inventory;
* per-batch history is queryable.

Uses the same in-memory stand-in as test_inventory.py (its check-and-set
fakes are atomic under asyncio; the real SQL implementation uses one
transaction with the batch row locked FOR UPDATE).
"""
import asyncio
import json

import httpx
import pytest
from fastapi.testclient import TestClient

from tests.test_inventory import BATCH, PLAN, FakePool, FakeStore
import app.db as db
import app.main as main


class StocktakeFakeStore(FakeStore):
    """Adds the sealed stocktake ledger to the inventory fake."""

    def __init__(self):
        super().__init__()
        self.stocktakes = {}  # stocktake_id -> (content_hash, record)

    async def record_stocktake(self, conn, stocktake_id, batch_id,
                               measured_nl, content_hash):
        # atomic: no awaits between the checks and the updates
        existing = self.stocktakes.get(stocktake_id)
        if existing is not None:
            ehash, erec = existing
            if ehash == content_hash:
                return "replay", json.loads(json.dumps(erec))
            return "conflict", {"stocktake_id": stocktake_id,
                                "existing_batch_id": erec["batch_id"]}
        b = self.batches.get(batch_id)
        if b is None:
            return "unknown", None
        if b.get("recalled"):
            return "recalled", {
                "batch_id": batch_id,
                "recalled_reason": b.get("recalled_reason"),
                "recalled_at": b.get("recalled_at")}
        loss = b.get("loss_nl", 0)
        expected = (b["initial_volume_nl"] - b["reserved_nl"]
                    - b["used_nl"] - loss)
        diff = measured_nl - expected
        if diff > loss:
            return "surplus_exceeded", {
                "batch_id": batch_id,
                "measured_available_nl": measured_nl,
                "expected_available_nl": expected,
                "surplus_nl": diff,
                "offsettable_loss_nl": loss}
        new_loss = loss - diff
        b["loss_nl"] = new_loss
        record = {
            "stocktake_id": stocktake_id,
            "batch_id": batch_id,
            "measured_available_nl": measured_nl,
            "expected_available_nl": expected,
            "kind": ("balanced" if diff == 0
                     else "deficit" if diff < 0 else "surplus"),
            "loss_delta_nl": new_loss - loss,
            "before": {"initial_volume_nl": b["initial_volume_nl"],
                       "reserved_nl": b["reserved_nl"],
                       "used_nl": b["used_nl"],
                       "loss_nl": loss,
                       "available_nl": expected},
            "after": {"initial_volume_nl": b["initial_volume_nl"],
                      "reserved_nl": b["reserved_nl"],
                      "used_nl": b["used_nl"],
                      "loss_nl": new_loss,
                      "available_nl": (b["initial_volume_nl"]
                                       - b["reserved_nl"] - b["used_nl"]
                                       - new_loss)},
            "counted_at": "2026-01-01T00:00:00+00:00",
        }
        self.stocktakes[stocktake_id] = (content_hash, record)
        return "ok", json.loads(json.dumps(record))

    async def get_stocktake(self, conn, stocktake_id):
        hit = self.stocktakes.get(stocktake_id)
        return json.loads(json.dumps(hit[1])) if hit is not None else None

    async def list_stocktakes(self, conn, batch_id):
        if batch_id not in self.batches:
            return None
        return [json.loads(json.dumps(rec))
                for _h, rec in self.stocktakes.values()
                if rec["batch_id"] == batch_id]


@pytest.fixture
def store(monkeypatch):
    st = StocktakeFakeStore()

    async def fake_connect(dsn=None):
        return FakePool()

    pairs = [
        ("connect", fake_connect),
        ("get_frozen", st.get_frozen),
        ("freeze", st.freeze),
        ("create_batch", st.create_batch),
        ("get_batch", st.get_batch),
        ("get_batches", st.get_batches),
        ("list_batches", st.list_batches),
        ("get_reservation", st.get_reservation),
        ("reserve_and_freeze", st.reserve_and_freeze),
        ("transition_reservation", st.transition_reservation),
        ("register_step", st.register_step),
        ("complete_stepwise", st.complete_stepwise),
        ("abort_stepwise", st.abort_stepwise),
        ("recall_batch", st.recall_batch),
        ("get_recall", st.get_recall),
        ("record_stocktake", st.record_stocktake),
        ("get_stocktake", st.get_stocktake),
        ("list_stocktakes", st.list_stocktakes),
    ]
    for name, fn in pairs:
        monkeypatch.setattr(db, name, fn)
        monkeypatch.setattr(main, name, fn)
    main.app.state.db = FakePool()
    return st


@pytest.fixture
def client(store):
    with TestClient(main.app) as c:
        yield c


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _stocktake(client, batch_id, measured, stocktake_id, batch="S1"):
    return client.post(
        f"/batches/{batch_id}/stocktakes",
        json={"measured_available_nl": measured},
        headers={"X-Stocktake-ID": stocktake_id})


def _plan(client, rid, volume=600, mode="whole"):
    body = json.loads(json.dumps(PLAN))
    body["wells"][0]["final_volume_nl"] = volume
    body["execution_mode"] = mode
    return client.post("/inventory/plans", json=body,
                       headers={"X-Request-ID": rid})


def _batch(client, batch_id="S1"):
    return client.get(f"/batches/{batch_id}").json()


def _balances(client, batch_id="S1"):
    b = _batch(client, batch_id)
    return (b["initial_volume_nl"], b["reserved_nl"], b["used_nl"],
            b["loss_nl"], b["available_nl"])


# --------------------------------------------------------------------------- #
# Deficit / balanced / surplus adjudication
# --------------------------------------------------------------------------- #


def test_deficit_moves_unreserved_available_into_loss(client):
    client.post("/batches", json=BATCH)
    r = _stocktake(client, "S1", 800, "ST-1")
    assert r.status_code == 201
    body = r.json()
    assert body["stocktake_id"] == "ST-1"
    assert body["batch_id"] == "S1"
    assert body["kind"] == "deficit"
    assert body["measured_available_nl"] == 800
    assert body["expected_available_nl"] == 1000
    assert body["loss_delta_nl"] == 200
    assert body["before"] == {"initial_volume_nl": 1000, "reserved_nl": 0,
                              "used_nl": 0, "loss_nl": 0,
                              "available_nl": 1000}
    assert body["after"] == {"initial_volume_nl": 1000, "reserved_nl": 0,
                             "used_nl": 0, "loss_nl": 200,
                             "available_nl": 800}
    # only the unreserved available pool feeds the cumulative loss
    assert _balances(client) == (1000, 0, 0, 200, 800)


def test_deficit_to_zero_available(client):
    client.post("/batches", json=BATCH)
    r = _stocktake(client, "S1", 0, "ST-zero")
    assert r.status_code == 201
    assert r.json()["loss_delta_nl"] == 1000
    assert _balances(client) == (1000, 0, 0, 1000, 0)


def test_balanced_count_seals_record_without_movement(client):
    client.post("/batches", json=BATCH)
    r = _stocktake(client, "S1", 1000, "ST-bal")
    assert r.status_code == 201
    body = r.json()
    assert body["kind"] == "balanced"
    assert body["loss_delta_nl"] == 0
    assert _balances(client) == (1000, 0, 0, 0, 1000)
    # the count is still sealed and queryable
    assert client.get("/stocktakes/ST-bal").json()["kind"] == "balanced"


def test_surplus_offsets_previously_confirmed_loss(client):
    client.post("/batches", json=BATCH)
    assert _stocktake(client, "S1", 700, "ST-d").status_code == 201
    r = _stocktake(client, "S1", 900, "ST-s")
    assert r.status_code == 201
    body = r.json()
    assert body["kind"] == "surplus"
    assert body["loss_delta_nl"] == -200
    assert body["before"]["loss_nl"] == 300
    assert body["after"]["loss_nl"] == 100
    assert _balances(client) == (1000, 0, 0, 100, 900)


def test_surplus_may_exactly_offset_the_whole_loss(client):
    client.post("/batches", json=BATCH)
    assert _stocktake(client, "S1", 700, "ST-d").status_code == 201
    r = _stocktake(client, "S1", 1000, "ST-s")
    assert r.status_code == 201
    assert r.json()["after"]["loss_nl"] == 0
    assert _balances(client) == (1000, 0, 0, 0, 1000)


def test_surplus_beyond_offsettable_loss_rejected(client):
    client.post("/batches", json=BATCH)
    assert _stocktake(client, "S1", 700, "ST-d").status_code == 201
    r = _stocktake(client, "S1", 1001, "ST-over")
    assert r.status_code == 409
    body = r.json()
    assert body["error"] == "surplus_exceeds_loss"
    assert body["surplus_nl"] == 301
    assert body["offsettable_loss_nl"] == 300
    # failure changes nothing and seals no record
    assert _balances(client) == (1000, 0, 0, 300, 700)
    assert client.get("/stocktakes/ST-over").status_code == 404


def test_surplus_without_any_loss_rejected(client):
    client.post("/batches", json=BATCH)
    r = _stocktake(client, "S1", 1001, "ST-gain")
    assert r.status_code == 409
    assert r.json()["error"] == "surplus_exceeds_loss"
    assert r.json()["offsettable_loss_nl"] == 0
    assert _balances(client) == (1000, 0, 0, 0, 1000)


# --------------------------------------------------------------------------- #
# Interaction with reservations and used volumes
# --------------------------------------------------------------------------- #


def test_deficit_never_touches_reserved_or_used(client):
    client.post("/batches", json=BATCH)
    assert _plan(client, "st-p1").status_code == 201  # reserves 600
    assert _balances(client) == (1000, 600, 0, 0, 400)
    r = _stocktake(client, "S1", 150, "ST-res")
    assert r.status_code == 201
    assert r.json()["loss_delta_nl"] == 250
    # the deficit came out of the unreserved 400 only
    assert _balances(client) == (1000, 600, 0, 250, 150)
    # confirm moves reserved -> used exactly as before; loss is unaffected
    assert client.post("/inventory/plans/st-p1/confirm").status_code == 200
    assert _balances(client) == (1000, 0, 600, 250, 150)


def test_surplus_never_changes_initial_used_or_reserved(client):
    client.post("/batches", json=BATCH)
    assert _plan(client, "st-p2").status_code == 201
    assert client.post("/inventory/plans/st-p2/confirm").status_code == 200
    assert _plan(client, "st-p3", volume=300).status_code == 201
    assert _balances(client) == (1000, 300, 600, 0, 100)
    assert _stocktake(client, "S1", 50, "ST-d2").status_code == 201
    assert _balances(client) == (1000, 300, 600, 50, 50)
    r = _stocktake(client, "S1", 100, "ST-s2")
    assert r.status_code == 201
    assert r.json()["kind"] == "surplus"
    # initial registration, used volume and the pending reservation are
    # untouchable for a stocktake
    assert _balances(client) == (1000, 300, 600, 0, 100)
    # cancelling the pending reservation releases exactly its 300 back
    assert client.post("/inventory/plans/st-p3/cancel").status_code == 200
    assert _balances(client) == (1000, 0, 600, 0, 400)


def test_stocktake_with_stepwise_plan(client):
    client.post("/batches", json=BATCH)
    r = _plan(client, "st-sw", mode="stepwise")
    assert r.status_code == 201
    transfer = r.json()["plan"]["transfers"][0]
    # register the single stock-out step: reserved -> used
    r = client.post("/inventory/plans/st-sw/steps", json={
        "step": transfer["step"], "source": transfer["source"],
        "destination": transfer["destination"],
        "volume_nl": transfer["volume_nl"]})
    assert r.status_code == 201
    assert _balances(client) == (1000, 0, 600, 0, 400)
    assert _stocktake(client, "S1", 350, "ST-sw").status_code == 201
    assert _balances(client) == (1000, 0, 600, 50, 350)
    assert client.post("/inventory/plans/st-sw/complete").status_code == 201
    assert _balances(client) == (1000, 0, 600, 50, 350)


def test_stocktake_then_abort_releases_pending_reservation(client):
    client.post("/batches", json=BATCH)
    assert _plan(client, "st-ab", mode="stepwise").status_code == 201
    assert _balances(client) == (1000, 600, 0, 0, 400)
    assert _stocktake(client, "S1", 350, "ST-ab").status_code == 201
    assert _balances(client) == (1000, 600, 0, 50, 350)
    # abort releases the not-yet-taken reservation; the loss stays booked
    assert client.post("/inventory/plans/st-ab/abort").status_code == 201
    assert _balances(client) == (1000, 0, 0, 50, 950)


def test_loss_reduces_what_later_plans_can_reserve(client):
    client.post("/batches", json=BATCH)
    assert _stocktake(client, "S1", 400, "ST-cap").status_code == 201
    r = _plan(client, "st-over")  # needs 600, only 400 left
    assert r.status_code == 422
    assert r.json()["reason"] == "insufficient_inventory"
    assert _plan(client, "st-fits", volume=400).status_code == 201
    assert _balances(client) == (1000, 400, 0, 600, 0)


def test_offset_loss_makes_volume_plannable_again(client):
    client.post("/batches", json=BATCH)
    assert _stocktake(client, "S1", 400, "ST-d3").status_code == 201
    assert _plan(client, "st-nope").status_code == 422
    # a later count finds the missing stock back: the surplus offsets the
    # confirmed loss and the volume becomes reservable again
    assert _stocktake(client, "S1", 1000, "ST-s3").status_code == 201
    assert _balances(client) == (1000, 0, 0, 0, 1000)
    assert _plan(client, "st-yes").status_code == 201


# --------------------------------------------------------------------------- #
# Idempotency and conflicts
# --------------------------------------------------------------------------- #


def test_same_id_same_content_replays_without_double_counting(client):
    client.post("/batches", json=BATCH)
    r1 = _stocktake(client, "S1", 800, "ST-r")
    r2 = _stocktake(client, "S1", 800, "ST-r")
    assert r1.status_code == 201
    assert r2.status_code == 200
    assert r2.json() == r1.json()
    # the loss moved exactly once
    assert _balances(client) == (1000, 0, 0, 200, 800)
    history = client.get("/batches/S1/stocktakes").json()["stocktakes"]
    assert len(history) == 1


def test_same_id_different_content_conflicts(client):
    client.post("/batches", json=BATCH)
    client.post("/batches", json={"id": "S2", "concentration": "100",
                                  "initial_volume_nl": 5000})
    assert _stocktake(client, "S1", 800, "ST-c").status_code == 201
    r = _stocktake(client, "S1", 700, "ST-c")
    assert r.status_code == 409
    assert r.json()["error"] == "stocktake_conflict"
    assert r.json()["existing_batch_id"] == "S1"
    # the same order id against another batch is a conflict too
    r = _stocktake(client, "S2", 800, "ST-c")
    assert r.status_code == 409
    assert r.json()["error"] == "stocktake_conflict"
    # neither conflict changed any inventory
    assert _balances(client, "S1") == (1000, 0, 0, 200, 800)
    assert _balances(client, "S2") == (5000, 0, 0, 0, 5000)


def test_unknown_batch_rejected_and_order_id_stays_reusable(client):
    r = _stocktake(client, "NOPE", 100, "ST-u")
    assert r.status_code == 404
    assert r.json()["error"] == "unknown_batch"
    assert client.get("/stocktakes/ST-u").status_code == 404
    # nothing was sealed: once the batch exists the same order id works
    client.post("/batches", json=BATCH)
    assert _stocktake(client, "S1", 1000, "ST-u").status_code == 201


def test_recalled_batch_rejected(client):
    client.post("/batches", json=BATCH)
    client.post("/batches/S1/recall", json={"reason": "contaminated"})
    r = _stocktake(client, "S1", 800, "ST-rec")
    assert r.status_code == 409
    assert r.json()["error"] == "batch_recalled"
    # failure changes nothing and seals no record
    assert _balances(client) == (1000, 0, 0, 0, 1000)
    assert client.get("/stocktakes/ST-rec").status_code == 404


def test_stocktake_before_recall_stays_sealed(client):
    client.post("/batches", json=BATCH)
    assert _stocktake(client, "S1", 800, "ST-old").status_code == 201
    client.post("/batches/S1/recall", json={"reason": "contaminated"})
    # the sealed record and the booked loss survive the recall
    assert client.get("/stocktakes/ST-old").status_code == 200
    assert _balances(client) == (1000, 0, 0, 200, 800)
    history = client.get("/batches/S1/stocktakes").json()["stocktakes"]
    assert [h["stocktake_id"] for h in history] == ["ST-old"]


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #


def test_invalid_volumes_rejected_without_side_effects(client):
    client.post("/batches", json=BATCH)
    for bad in (-1, -1000, 2**63, 2**63 + 1, "abc", 1.5, None):
        r = _stocktake(client, "S1", bad, f"ST-bad-{bad}")
        assert r.status_code == 400, (bad, r.status_code)
        assert r.json()["error"] == "invalid_request"
    # missing / blank order id
    assert client.post("/batches/S1/stocktakes",
                       json={"measured_available_nl": 1}).status_code == 400
    assert client.post(
        "/batches/S1/stocktakes", json={"measured_available_nl": 1},
        headers={"X-Stocktake-ID": "  "}).status_code == 400
    # nothing changed, nothing sealed
    assert _balances(client) == (1000, 0, 0, 0, 1000)
    assert client.get("/batches/S1/stocktakes").json()["stocktakes"] == []


def test_boundary_volume_max_bigint_accepted_as_validation_only(client):
    # 2**63-1 passes range validation; the surplus check then rejects it
    # because it exceeds the offsettable loss (zero here).
    client.post("/batches", json=BATCH)
    r = _stocktake(client, "S1", 2**63 - 1, "ST-max")
    assert r.status_code == 409
    assert r.json()["error"] == "surplus_exceeds_loss"


# --------------------------------------------------------------------------- #
# History and single-record queries
# --------------------------------------------------------------------------- #


def test_history_is_per_batch_and_ordered(client):
    client.post("/batches", json=BATCH)
    client.post("/batches", json={"id": "S2", "concentration": "100",
                                  "initial_volume_nl": 5000})
    assert _stocktake(client, "S1", 900, "ST-h1").status_code == 201
    assert _stocktake(client, "S1", 800, "ST-h2").status_code == 201
    assert _stocktake(client, "S2", 4500, "ST-h3").status_code == 201
    r = client.get("/batches/S1/stocktakes")
    assert r.status_code == 200
    history = r.json()["stocktakes"]
    assert [h["stocktake_id"] for h in history] == ["ST-h1", "ST-h2"]
    # sealed snapshots chain: each count starts where the previous ended
    assert history[0]["after"] == history[1]["before"]
    other = client.get("/batches/S2/stocktakes").json()["stocktakes"]
    assert [h["stocktake_id"] for h in other] == ["ST-h3"]
    # single-record fetch and the unknown cases
    assert client.get("/stocktakes/ST-h2").json() == history[1]
    assert client.get("/stocktakes/nope").status_code == 404
    assert client.get("/batches/NOPE/stocktakes").status_code == 404


def test_batch_object_exposes_cumulative_loss(client):
    client.post("/batches", json=BATCH)
    assert _stocktake(client, "S1", 800, "ST-x").status_code == 201
    one = client.get("/batches/S1").json()
    assert one["loss_nl"] == 200
    assert one["available_nl"] == 800
    listed = client.get("/batches").json()["batches"]
    assert listed[0]["loss_nl"] == 200


# --------------------------------------------------------------------------- #
# Concurrency
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_concurrent_same_id_stocktakes_count_once(store, anyio_backend):
    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://test") as ac:
        await ac.post("/batches", json=BATCH)
        results = await asyncio.gather(*[
            ac.post("/batches/S1/stocktakes",
                    json={"measured_available_nl": 800},
                    headers={"X-Stocktake-ID": "ST-same"})
            for _ in range(5)
        ])
        codes = sorted(r.status_code for r in results)
        assert codes == [200, 200, 200, 200, 201]
        payloads = {json.dumps(r.json(), sort_keys=True) for r in results}
        assert len(payloads) == 1
        batch = (await ac.get("/batches/S1")).json()
        assert batch["loss_nl"] == 200  # applied exactly once
        history = (await ac.get("/batches/S1/stocktakes")).json()
        assert len(history["stocktakes"]) == 1


@pytest.mark.anyio
async def test_concurrent_stocktakes_chain_atomically(store, anyio_backend):
    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://test") as ac:
        await ac.post("/batches", json=BATCH)
        results = await asyncio.gather(*[
            ac.post("/batches/S1/stocktakes",
                    json={"measured_available_nl": m},
                    headers={"X-Stocktake-ID": f"ST-{m}"})
            for m in (900, 800, 700)
        ])
        assert all(r.status_code == 201 for r in results)
        history = (await ac.get("/batches/S1/stocktakes")).json()
        records = history["stocktakes"]
        assert len(records) == 3
        # every count was adjudicated on the locked state left by the
        # previous one: snapshots form an unbroken chain
        for prev, nxt in zip(records, records[1:]):
            assert prev["after"] == nxt["before"]
        batch = (await ac.get("/batches/S1")).json()
        last = records[-1]["after"]
        assert batch["loss_nl"] == last["loss_nl"]
        assert batch["available_nl"] == last["available_nl"]


@pytest.mark.anyio
async def test_concurrent_stocktake_and_plan_never_oversell(store,
                                                            anyio_backend):
    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://test") as ac:
        await ac.post("/batches", json=BATCH)
        plan_resp, count_resp = await asyncio.gather(
            ac.post("/inventory/plans", json=PLAN,
                    headers={"X-Request-ID": "st-race"}),
            ac.post("/batches/S1/stocktakes",
                    json={"measured_available_nl": 500},
                    headers={"X-Stocktake-ID": "ST-race"}),
        )
        batch = (await ac.get("/batches/S1")).json()
        if plan_resp.status_code == 201:
            # the plan reserved 600 first; the count then measures 500
            # against a book available of 400 -> surplus without any
            # offsettable loss -> rejected
            assert count_resp.status_code == 409
            assert count_resp.json()["error"] == "surplus_exceeds_loss"
            assert (batch["reserved_nl"], batch["loss_nl"],
                    batch["available_nl"]) == (600, 0, 400)
        else:
            # the count booked a 500 loss first; the plan then finds only
            # 500 available and is refused without reserving
            assert plan_resp.status_code == 422
            assert count_resp.status_code == 201
            assert (batch["reserved_nl"], batch["loss_nl"],
                    batch["available_nl"]) == (0, 500, 500)


@pytest.mark.anyio
async def test_concurrent_stocktake_and_recall_adjudicated(store,
                                                           anyio_backend):
    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://test") as ac:
        await ac.post("/batches", json=BATCH)
        count_resp, recall_resp = await asyncio.gather(
            ac.post("/batches/S1/stocktakes",
                    json={"measured_available_nl": 800},
                    headers={"X-Stocktake-ID": "ST-rc"}),
            ac.post("/batches/S1/recall", json={"reason": "contaminated"}),
        )
        assert recall_resp.status_code == 201
        batch = (await ac.get("/batches/S1")).json()
        if count_resp.status_code == 201:
            # the count committed before the recall took the batch lock
            assert (batch["loss_nl"], batch["available_nl"]) == (200, 800)
        else:
            # the recall committed first and forbids the count
            assert count_resp.status_code == 409
            assert count_resp.json()["error"] == "batch_recalled"
            assert (batch["loss_nl"], batch["available_nl"]) == (0, 1000)
