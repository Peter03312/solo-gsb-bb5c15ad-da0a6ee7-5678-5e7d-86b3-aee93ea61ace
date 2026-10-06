"""Physical stocktake (inventory count) tests.

A stocktake reconciles a batch's book inventory against a physically counted
available volume, identified by batch id + a unique stocktake id
(``X-Stocktake-ID`` header) + the counted integer nanoliters.

Rules under test:

* a shortage moves unreserved available volume into the cumulative confirmed
  loss (``lost_nl``); a surplus only reverses previously confirmed losses --
  it never raises the initial registration and never touches used volume or
  existing reservations;
* the inventory movement and the before/after snapshots are sealed
  atomically; same stocktake id + same content replays the original sealed
  record (200, balances untouched), same id + different content conflicts
  (409);
* recalled batches, unknown batches, negative counts and surpluses beyond
  the recoverable loss are explicitly rejected; a rejected stocktake changes
  no inventory and seals nothing (the id stays reusable);
* the per-batch stocktake history is queryable;
* concurrent stocktakes / plan creation / confirm serialize on the same
  locked inventory state.

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
    """Adds the sealed stocktake store to the inventory fake."""

    def __init__(self):
        super().__init__()
        self.stocktakes = {}  # stocktake_id -> {"content_hash", "doc"}

    @staticmethod
    def _snapshot(b):
        return {
            "initial_volume_nl": b["initial_volume_nl"],
            "reserved_nl": b["reserved_nl"],
            "used_nl": b["used_nl"],
            "lost_nl": b.get("lost_nl", 0),
            "available_nl": (b["initial_volume_nl"] - b["reserved_nl"]
                             - b["used_nl"] - b.get("lost_nl", 0)),
        }

    async def record_stocktake(self, conn, stocktake_id, batch_id,
                               counted_nl, content_hash):
        # atomic: no awaits between the checks and the updates
        existing = self.stocktakes.get(stocktake_id)
        if existing is not None:
            if existing["content_hash"] == content_hash:
                return "replay", json.loads(json.dumps(existing["doc"]))
            return "conflict", {
                "stocktake_id": stocktake_id,
                "existing_batch_id": existing["doc"]["batch_id"],
            }
        b = self.batches.get(batch_id)
        if b is None:
            return "unknown", None
        if b.get("recalled"):
            return "recalled", {
                "batch_id": batch_id,
                "recalled_reason": b.get("recalled_reason"),
                "recalled_at": b.get("recalled_at"),
            }
        before = self._snapshot(b)
        available = before["available_nl"]
        delta = counted_nl - available
        if delta > before["lost_nl"]:
            return "uncoverable", {
                "batch_id": batch_id,
                "counted_available_nl": counted_nl,
                "available_nl": available,
                "recoverable_loss_nl": before["lost_nl"],
                "max_counted_nl": available + before["lost_nl"],
            }
        b["lost_nl"] = before["lost_nl"] - delta
        after = self._snapshot(b)
        doc = {
            "stocktake_id": stocktake_id,
            "batch_id": batch_id,
            "counted_available_nl": counted_nl,
            "kind": ("loss" if delta < 0
                     else "gain" if delta > 0 else "balanced"),
            "delta_nl": delta,
            "loss_applied_nl": -delta if delta < 0 else 0,
            "gain_applied_nl": delta if delta > 0 else 0,
            "before": before,
            "after": after,
            "recorded_at": "2026-10-06T00:00:00+00:00",
        }
        self.stocktakes[stocktake_id] = {"content_hash": content_hash,
                                         "doc": doc}
        return "ok", json.loads(json.dumps(doc))

    async def get_stocktake(self, conn, stocktake_id):
        entry = self.stocktakes.get(stocktake_id)
        return json.loads(json.dumps(entry["doc"])) if entry else None

    async def list_stocktakes(self, conn, batch_id):
        if batch_id not in self.batches:
            return "missing"
        return [json.loads(json.dumps(e["doc"]))
                for e in self.stocktakes.values()
                if e["doc"]["batch_id"] == batch_id]


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


def _stocktake(client, bid="S1", counted=0, sid="ST-1"):
    return client.post(f"/batches/{bid}/stocktakes",
                       json={"counted_available_nl": counted},
                       headers={"X-Stocktake-ID": sid})


def _batch(client, bid="S1"):
    return client.get(f"/batches/{bid}").json()


# --------------------------------------------------------------------------- #
# Loss / gain / balanced bookkeeping
# --------------------------------------------------------------------------- #


def test_stocktake_loss_moves_available_into_cumulative_loss(client):
    client.post("/batches", json=BATCH)  # 1000 nL registered
    r = _stocktake(client, counted=400)
    assert r.status_code == 201, r.text
    doc = r.json()
    assert doc["stocktake_id"] == "ST-1"
    assert doc["batch_id"] == "S1"
    assert doc["counted_available_nl"] == 400
    assert doc["kind"] == "loss"
    assert doc["delta_nl"] == -600
    assert doc["loss_applied_nl"] == 600
    assert doc["gain_applied_nl"] == 0
    assert doc["before"] == {"initial_volume_nl": 1000, "reserved_nl": 0,
                             "used_nl": 0, "lost_nl": 0,
                             "available_nl": 1000}
    assert doc["after"] == {"initial_volume_nl": 1000, "reserved_nl": 0,
                            "used_nl": 0, "lost_nl": 600,
                            "available_nl": 400}
    assert doc["recorded_at"]
    b = _batch(client)
    assert (b["lost_nl"], b["available_nl"]) == (600, 400)
    # the loss never touched registration, used or reserved volumes
    assert (b["initial_volume_nl"], b["used_nl"], b["reserved_nl"]) == \
        (1000, 0, 0)


def test_stocktake_balanced_changes_nothing(client):
    client.post("/batches", json=BATCH)
    r = _stocktake(client, counted=1000)
    assert r.status_code == 201
    doc = r.json()
    assert doc["kind"] == "balanced"
    assert doc["delta_nl"] == 0
    assert doc["loss_applied_nl"] == 0
    assert doc["gain_applied_nl"] == 0
    assert doc["before"] == doc["after"]
    b = _batch(client)
    assert (b["lost_nl"], b["available_nl"]) == (0, 1000)


def test_stocktake_gain_reverses_prior_confirmed_losses(client):
    client.post("/batches", json=BATCH)
    assert _stocktake(client, counted=400, sid="ST-loss").status_code == 201
    # physical recount finds 700 nL: 300 of the 600 confirmed loss is
    # reversed back into the available pool
    r = _stocktake(client, counted=700, sid="ST-gain")
    assert r.status_code == 201, r.text
    doc = r.json()
    assert doc["kind"] == "gain"
    assert doc["delta_nl"] == 300
    assert doc["gain_applied_nl"] == 300
    assert doc["loss_applied_nl"] == 0
    assert doc["before"]["lost_nl"] == 600
    assert doc["after"]["lost_nl"] == 300
    b = _batch(client)
    assert (b["lost_nl"], b["available_nl"]) == (300, 700)


def test_stocktake_gain_may_recover_exactly_all_losses(client):
    client.post("/batches", json=BATCH)
    _stocktake(client, counted=400, sid="ST-loss")
    r = _stocktake(client, counted=1000, sid="ST-full")
    assert r.status_code == 201
    assert r.json()["after"]["lost_nl"] == 0
    b = _batch(client)
    assert (b["lost_nl"], b["available_nl"]) == (0, 1000)


def test_stocktake_surplus_beyond_recoverable_loss_rejected(client):
    client.post("/batches", json=BATCH)
    # no confirmed loss yet: any count above the book available is impossible
    r = _stocktake(client, counted=1001)
    assert r.status_code == 409
    body = r.json()
    assert body["error"] == "uncoverable_surplus"
    assert body["recoverable_loss_nl"] == 0
    assert body["max_counted_nl"] == 1000
    # failure changed nothing and sealed nothing
    b = _batch(client)
    assert (b["lost_nl"], b["available_nl"]) == (0, 1000)
    assert client.get("/stocktakes/ST-1").status_code == 404


def test_stocktake_surplus_capped_by_cumulative_loss(client):
    client.post("/batches", json=BATCH)
    _stocktake(client, counted=400, sid="ST-loss")  # lost 600
    r = _stocktake(client, counted=1001, sid="ST-over")
    assert r.status_code == 409
    body = r.json()
    assert body["error"] == "uncoverable_surplus"
    assert body["recoverable_loss_nl"] == 600
    assert body["max_counted_nl"] == 1000
    # counted == max is the largest acceptable recount
    assert _stocktake(client, counted=1000, sid="ST-ok").status_code == 201
    assert _batch(client)["lost_nl"] == 0


def test_stocktake_never_touches_reserved_or_used(client):
    client.post("/batches", json=BATCH)
    client.post("/inventory/plans", json=PLAN,
                headers={"X-Request-ID": "st-plan"})  # reserves 600
    r = _stocktake(client, counted=300)
    assert r.status_code == 201
    doc = r.json()
    # only the unreserved available (400) is reconciled; the loss comes
    # solely out of it
    assert doc["before"]["reserved_nl"] == 600
    assert doc["before"]["available_nl"] == 400
    assert doc["loss_applied_nl"] == 100
    b = _batch(client)
    assert (b["reserved_nl"], b["used_nl"], b["lost_nl"],
            b["available_nl"]) == (600, 0, 100, 300)
    # confirm still moves exactly the reservation into used
    client.post("/inventory/plans/st-plan/confirm")
    b = _batch(client)
    assert (b["reserved_nl"], b["used_nl"], b["lost_nl"],
            b["available_nl"]) == (0, 600, 100, 300)


def test_stocktake_loss_shrinks_plannable_inventory(client):
    client.post("/batches", json=BATCH)
    _stocktake(client, counted=300)  # 700 nL lost
    r = client.post("/inventory/plans", json=PLAN,
                    headers={"X-Request-ID": "st-tight"})
    assert r.status_code == 422
    assert r.json()["reason"] == "insufficient_inventory"
    # a plan within the counted remainder still fits
    small = json.loads(json.dumps(PLAN))
    small["wells"][0]["final_volume_nl"] = 300
    small["wells"][0]["final_concentration"] = "50"
    r = client.post("/inventory/plans", json=small,
                    headers={"X-Request-ID": "st-fits"})
    assert r.status_code == 201


# --------------------------------------------------------------------------- #
# Idempotency: replay / conflict
# --------------------------------------------------------------------------- #


def test_stocktake_replay_returns_original_without_double_count(client):
    client.post("/batches", json=BATCH)
    first = _stocktake(client, counted=400)
    assert first.status_code == 201
    again = _stocktake(client, counted=400)
    assert again.status_code == 200
    assert again.json() == first.json()
    # the loss was applied exactly once
    assert _batch(client)["lost_nl"] == 600
    assert len(client.get("/batches/S1/stocktakes").json()
               ["stocktakes"]) == 1


def test_stocktake_same_id_different_content_conflicts(client):
    client.post("/batches", json=BATCH)
    _stocktake(client, counted=400)
    r = _stocktake(client, counted=500)
    assert r.status_code == 409
    assert r.json()["error"] == "stocktake_conflict"
    # the conflicting attempt changed nothing
    assert _batch(client)["lost_nl"] == 600
    # the original still replays
    assert _stocktake(client, counted=400).status_code == 200


def test_stocktake_id_reused_on_another_batch_conflicts(client):
    client.post("/batches", json=BATCH)
    client.post("/batches", json={"id": "S2", "concentration": "50",
                                  "initial_volume_nl": 5000})
    _stocktake(client, counted=400)
    r = _stocktake(client, bid="S2", counted=4000)
    assert r.status_code == 409
    assert r.json()["error"] == "stocktake_conflict"
    assert r.json()["existing_batch_id"] == "S1"
    assert _batch(client, "S2")["lost_nl"] == 0


def test_failed_stocktake_consumes_no_id(client):
    client.post("/batches", json=BATCH)
    # rejected (uncoverable surplus): the id must stay reusable
    assert _stocktake(client, counted=2000, sid="ST-x").status_code == 409
    _stocktake(client, counted=400, sid="ST-loss")  # book a loss first
    r = _stocktake(client, counted=700, sid="ST-x")
    assert r.status_code == 201
    assert r.json()["kind"] == "gain"


# --------------------------------------------------------------------------- #
# Explicit rejections
# --------------------------------------------------------------------------- #


def test_stocktake_unknown_batch_404(client):
    r = _stocktake(client, bid="NOPE", counted=10)
    assert r.status_code == 404
    assert r.json()["error"] == "unknown_batch"
    assert client.get("/batches/NOPE/stocktakes").status_code == 404


def test_stocktake_recalled_batch_rejected(client):
    client.post("/batches", json=BATCH)
    client.post("/batches/S1/recall", json={"reason": "contaminated"})
    r = _stocktake(client, counted=400)
    assert r.status_code == 409
    assert r.json()["error"] == "batch_recalled"
    assert _batch(client)["lost_nl"] == 0


def test_stocktake_sealed_before_recall_still_replays(client):
    client.post("/batches", json=BATCH)
    first = _stocktake(client, counted=400)
    client.post("/batches/S1/recall", json={"reason": "contaminated"})
    # the sealed record is immutable: same id + same content replays it even
    # though the batch is now recalled
    again = _stocktake(client, counted=400)
    assert again.status_code == 200
    assert again.json() == first.json()
    # different content on the same id still conflicts
    assert _stocktake(client, counted=500).status_code == 409
    # a fresh stocktake on the recalled batch is rejected
    assert _stocktake(client, counted=400, sid="ST-new").status_code == 409


def test_stocktake_invalid_input_rejected(client):
    client.post("/batches", json=BATCH)
    # negative counted volume
    r = client.post("/batches/S1/stocktakes",
                    json={"counted_available_nl": -1},
                    headers={"X-Stocktake-ID": "ST-neg"})
    assert r.status_code == 400
    # missing header
    assert client.post("/batches/S1/stocktakes",
                       json={"counted_available_nl": 1}).status_code == 400
    # blank header
    assert _stocktake(client, counted=1, sid="   ").status_code == 400
    # reserved prefix / oversized id
    assert _stocktake(client, counted=1, sid="@st").status_code == 400
    assert _stocktake(client, counted=1, sid="x" * 65).status_code == 400
    # nothing was sealed, nothing changed
    assert _batch(client)["lost_nl"] == 0
    assert client.get("/batches/S1/stocktakes").json()["stocktakes"] == []


# --------------------------------------------------------------------------- #
# Queries
# --------------------------------------------------------------------------- #


def test_stocktake_history_per_batch(client):
    client.post("/batches", json=BATCH)
    client.post("/batches", json={"id": "S2", "concentration": "50",
                                  "initial_volume_nl": 5000})
    _stocktake(client, counted=900, sid="ST-a")
    _stocktake(client, counted=950, sid="ST-b")
    _stocktake(client, bid="S2", counted=4900, sid="ST-c")

    r = client.get("/batches/S1/stocktakes")
    assert r.status_code == 200
    docs = r.json()["stocktakes"]
    assert [d["stocktake_id"] for d in docs] == ["ST-a", "ST-b"]
    assert [d["kind"] for d in docs] == ["loss", "gain"]
    assert docs[0]["after"] == docs[1]["before"]
    # the other batch's history is independent
    docs2 = client.get("/batches/S2/stocktakes").json()["stocktakes"]
    assert [d["stocktake_id"] for d in docs2] == ["ST-c"]
    # a batch without stocktakes lists empty; unknown batch 404s
    client.post("/batches", json={"id": "S3", "concentration": "1",
                                  "initial_volume_nl": 10})
    assert client.get("/batches/S3/stocktakes").json()["stocktakes"] == []
    assert client.get("/batches/NOPE/stocktakes").status_code == 404


def test_get_stocktake_by_id(client):
    client.post("/batches", json=BATCH)
    sealed = _stocktake(client, counted=400).json()
    r = client.get("/stocktakes/ST-1")
    assert r.status_code == 200
    assert r.json() == sealed
    assert client.get("/stocktakes/NOPE").status_code == 404


def test_batch_views_expose_cumulative_loss(client):
    client.post("/batches", json=BATCH)
    assert _batch(client)["lost_nl"] == 0
    _stocktake(client, counted=400)
    one = _batch(client)
    assert (one["lost_nl"], one["available_nl"]) == (600, 400)
    listing = client.get("/batches").json()["batches"]
    assert listing[0]["lost_nl"] == 600
    assert listing[0]["available_nl"] == 400


# --------------------------------------------------------------------------- #
# Concurrency
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_concurrent_same_id_stocktakes_seal_once(store, anyio_backend):
    await store.create_batch(None, "S1", "100", 1000)
    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://test") as ac:
        results = await asyncio.gather(*[
            ac.post("/batches/S1/stocktakes",
                    json={"counted_available_nl": 400},
                    headers={"X-Stocktake-ID": "ST-race"})
            for _ in range(5)
        ])
    codes = sorted(r.status_code for r in results)
    assert codes == [200, 200, 200, 200, 201]
    payloads = {json.dumps(r.json(), sort_keys=True) for r in results}
    assert len(payloads) == 1
    b = store._pub(store.batches["S1"])
    assert (b["lost_nl"], b["available_nl"]) == (600, 400)
    assert len(store.stocktakes) == 1


@pytest.mark.anyio
async def test_concurrent_distinct_stocktakes_serialize(store, anyio_backend):
    await store.create_batch(None, "S1", "100", 1000)
    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://test") as ac:
        results = await asyncio.gather(*[
            ac.post("/batches/S1/stocktakes",
                    json={"counted_available_nl": 0},
                    headers={"X-Stocktake-ID": f"ST-{i}"})
            for i in range(5)
        ])
    assert all(r.status_code == 201 for r in results)
    b = store._pub(store.batches["S1"])
    # every count of 0 serialized: the first booked the whole 1000 nL loss,
    # the rest observed the emptied shelf and balanced out
    assert (b["lost_nl"], b["available_nl"]) == (1000, 0)
    applied = sorted(r.json()["loss_applied_nl"] for r in results)
    assert applied == [0, 0, 0, 0, 1000]
    # snapshots chain: each stocktake's before matches a predecessor's after
    docs = sorted(results,
                  key=lambda r: -r.json()["before"]["available_nl"])
    for prev, nxt in zip(docs, docs[1:]):
        assert prev.json()["after"] == nxt.json()["before"]


@pytest.mark.anyio
async def test_concurrent_stocktake_and_plan_creation(store, anyio_backend):
    await store.create_batch(None, "S1", "100", 1000)
    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://test") as ac:
        plan_res, st_res = await asyncio.gather(
            ac.post("/inventory/plans", json=PLAN,
                    headers={"X-Request-ID": "st-race-plan"}),
            ac.post("/batches/S1/stocktakes",
                    json={"counted_available_nl": 100},
                    headers={"X-Stocktake-ID": "ST-race-2"}),
        )
    assert st_res.status_code == 201
    b = store._pub(store.batches["S1"])
    if plan_res.status_code == 201:
        # plan reserved 600 first: the count reconciled the remaining 400
        assert (b["reserved_nl"], b["lost_nl"], b["available_nl"]) == \
            (600, 300, 100)
        assert st_res.json()["before"]["reserved_nl"] == 600
    else:
        # the loss (900) landed first: the plan no longer fits
        assert plan_res.status_code == 422
        assert (b["reserved_nl"], b["lost_nl"], b["available_nl"]) == \
            (0, 900, 100)
    # either way the ledger balances
    assert (b["reserved_nl"] + b["used_nl"] + b["lost_nl"]
            + b["available_nl"]) == b["initial_volume_nl"]


@pytest.mark.anyio
async def test_concurrent_stocktake_and_confirm(store, anyio_backend):
    await store.create_batch(None, "S1", "100", 1000)
    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://test") as ac:
        await ac.post("/inventory/plans", json=PLAN,
                      headers={"X-Request-ID": "st-race-3"})
        confirm_res, st_res = await asyncio.gather(
            ac.post("/inventory/plans/st-race-3/confirm"),
            ac.post("/batches/S1/stocktakes",
                    json={"counted_available_nl": 200},
                    headers={"X-Stocktake-ID": "ST-race-3"}),
        )
    assert confirm_res.status_code == 200
    assert st_res.status_code == 201
    b = store._pub(store.batches["S1"])
    # confirm (reserved -> used) and the loss (available -> lost) act on
    # disjoint buckets, so both linearizations land on the same state
    assert (b["reserved_nl"], b["used_nl"], b["lost_nl"],
            b["available_nl"]) == (0, 600, 200, 200)
