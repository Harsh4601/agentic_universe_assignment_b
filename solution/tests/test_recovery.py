"""Crash recovery: rebuild the database state each crash point leaves behind, restart, check.

Crash points (plan section 9):
  2  after reserve committed, before the POST was sent      -> provider never heard of it
  3  after the POST was sent, before the response was read  -> provider has it, still running
  4  same, and the call already ended while we were down    -> provider has the result
  5  provider unreachable during recovery                   -> keep holding the slot
"""
import asyncio
import dataclasses
from datetime import timedelta

import pytest

from conftest import THU, ist
from dispatcher.app import open_store, recover
from dispatcher.clock import FakeClock
from dispatcher.db import connect
from dispatcher.dispatcher import Dispatcher, DispatchSettings
from dispatcher.policy import Priority
from dispatcher.provider import LookupResult, LookupStatus
from dispatcher.store import StaleDatabaseError, Store
from fakes import FakeProvider

T0 = ist(THU, 17, 30)
REN, FB = "c_acme_renewals", "c_acme_feedback"
SHARED_PHONE = "+917695793186"   # ren_0018 and fb_0011
RETRY = timedelta(seconds=45)


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "state.db"


def crashed_with_reservation(db_path, cfg, lead=(REN, "ren_0018")):
    """Process 1 reserves a call and dies (connection dropped without any cleanup)."""
    s = Store(connect(db_path), cfg)
    s.init_run("run-1")
    s.seed_leads(T0)
    res = s.reserve(*lead, T0)
    s.conn.close()
    return res


def restart(db_path, cfg):
    """Process 2 starts on the same database."""
    s = open_store(db_path, cfg, "run-1")
    assert s.seed_leads(T0) == 0
    return s


def do_recover(store, provider, clock=None):
    asyncio.run(recover(store, provider, clock or FakeClock(T0 + timedelta(minutes=2)), RETRY))


def ended(res, status="no_answer"):
    return LookupResult(LookupStatus.FOUND, call={
        "call_sid": "CA1", "state": "ended", "result": {
            "request_id": res.request_id, "call_sid": "CA1", "campaign_id": res.campaign_id,
            "lead_id": res.lead_id, "status": status, "disposition": None,
            "ended_at": ist(THU, 17, 31).isoformat()}})


def test_crash_before_post_releases_slot_and_redials_same_request_id(db_path, cfg):  # point 2
    res = crashed_with_reservation(db_path, cfg)
    store, provider = restart(db_path, cfg), FakeProvider()
    do_recover(store, provider)
    assert store.call(res.request_id).state == "not_placed"
    assert store.lead(REN, "ren_0018").attempts == 0
    assert store.busy_phones() == set()
    lead = store.lead(REN, "ren_0018")
    assert lead.next_eligible_at == T0 + timedelta(minutes=2) + RETRY      # backs off before redialling

    again = store.reserve(REN, "ren_0018", lead.next_eligible_at)
    assert again.request_id == res.request_id   # same key: if the old POST did land, the provider dedupes
    store.conn.close()


def test_crash_after_post_keeps_call_in_flight_and_never_redials(db_path, cfg):  # point 3
    res = crashed_with_reservation(db_path, cfg)
    store, provider = restart(db_path, cfg), FakeProvider()
    provider.lookups[res.request_id] = LookupResult(LookupStatus.FOUND,
                                                    call={"call_sid": "CA1", "state": "active"})
    do_recover(store, provider)
    call = store.call(res.request_id)
    assert (call.state, call.accepted, call.call_sid) == ("active", True, "CA1")
    assert store.lead(REN, "ren_0018").attempts == 1

    d = Dispatcher(store, provider, FakeClock(T0 + timedelta(minutes=2)), cfg,
                   DispatchSettings(callback_url="http://cb"))

    async def go():
        d.dispatch_once()
        await d.drain()
    asyncio.run(go())
    dialled = {p["lead_id"] for p in provider.posts}
    assert "ren_0018" not in dialled and "fb_0011" not in dialled   # lead busy, shared phone busy (R3)
    store.conn.close()


def test_crash_after_post_and_call_ended_while_down(db_path, cfg):  # point 4
    res = crashed_with_reservation(db_path, cfg)
    store, provider = restart(db_path, cfg), FakeProvider()
    provider.lookups[res.request_id] = ended(res, status="busy")
    do_recover(store, provider)
    lead = store.lead(REN, "ren_0018")
    assert store.call(res.request_id).state == "ended"
    assert (lead.attempts, lead.priority, lead.last_status) == (1, Priority.RETRY, "busy")
    assert lead.next_eligible_at == ist(THU, 18, 31)                # 60 min after ended_at (R5)
    store.conn.close()


def test_provider_unreachable_during_recovery_keeps_slot_held(db_path, cfg):  # point 5
    res = crashed_with_reservation(db_path, cfg)
    store, provider = restart(db_path, cfg), FakeProvider()
    provider.lookups[res.request_id] = LookupResult(LookupStatus.ERROR, error="connection refused")
    do_recover(store, provider)
    assert store.call(res.request_id).state == "unknown"
    assert store.busy_phones() == {SHARED_PHONE}
    assert store.reserve(FB, "fb_0011", T0) is None
    store.conn.close()


def test_repeated_crashes_during_recovery_are_harmless(db_path, cfg):
    res = crashed_with_reservation(db_path, cfg)
    for _ in range(3):  # recovery itself dies before resolving anything
        s = restart(db_path, cfg)
        s.mark_orphans_unknown()
        s.conn.close()
    store, provider = restart(db_path, cfg), FakeProvider()
    provider.lookups[res.request_id] = ended(res)
    do_recover(store, provider)
    assert store.lead(REN, "ren_0018").attempts == 1
    store.conn.close()


def test_several_orphans_resolved_independently(db_path, cfg):
    s = Store(connect(db_path), cfg)
    s.init_run("run-1")
    s.seed_leads(T0)
    a = s.reserve(REN, "ren_0001", T0)
    b = s.reserve(REN, "ren_0002", T0)
    c = s.reserve(FB, "fb_0020", T0)
    s.conn.close()
    store, provider = restart(db_path, cfg), FakeProvider()
    provider.lookups[a.request_id] = LookupResult(LookupStatus.FOUND, call={"call_sid": "CAa", "state": "active"})
    provider.lookups[b.request_id] = ended(b)
    do_recover(store, provider)
    assert [store.call(r.request_id).state for r in (a, b, c)] == ["active", "ended", "not_placed"]
    assert store.in_flight_count("acme_insurance") == 1
    store.conn.close()


# --------------------------------------------------------------------------- database ownership
def test_database_from_earlier_provider_run_is_archived(db_path, cfg):
    crashed_with_reservation(db_path, cfg)
    store = open_store(db_path, cfg, "run-2")   # the mock was restarted
    assert store.seed_leads(T0) == 200            # fresh state
    assert store.calls_in_state("dialing") == []
    assert list(db_path.parent.glob("state.db.stale-*"))
    store.conn.close()


def test_database_for_other_fixtures_is_refused(db_path, cfg):
    crashed_with_reservation(db_path, cfg)
    with pytest.raises(StaleDatabaseError):
        open_store(db_path, dataclasses.replace(cfg, fingerprint="different"), "run-1")
