"""Reconciler: lost webhooks and unknown outcomes, with a fake clock and fake provider."""
import asyncio
from datetime import timedelta

import pytest

from conftest import THU, ist
from dispatcher.clock import FakeClock
from dispatcher.db import connect
from dispatcher.policy import LeadState, Priority
from dispatcher.provider import LookupResult, LookupStatus
from dispatcher.reconciler import Reconciler, ReconcileSettings
from dispatcher.store import ApplyStatus, Store
from fakes import FakeProvider

T0 = ist(THU, 17, 30)
REN = "c_acme_renewals"
SETTINGS = ReconcileSettings(stale_after=timedelta(minutes=8), repoll_every=timedelta(minutes=2))


@pytest.fixture
def env(tmp_path, cfg):
    store = Store(connect(tmp_path / "r.db"), cfg)
    store.init_run("run-1")
    store.seed_leads(T0)
    clock, provider = FakeClock(T0), FakeProvider()
    changes = []
    owned: set[str] = set()
    rec = Reconciler(store, provider, clock, SETTINGS, owned=lambda: owned, on_change=lambda: changes.append(1))
    yield store, provider, clock, rec, changes, owned
    store.conn.close()


def once(rec):
    asyncio.run(rec.reconcile_once())


def ended_view(res, sid, status="no_answer", disposition=None):
    result = {"event": "call.completed", "call_sid": sid, "request_id": res.request_id,
              "campaign_id": res.campaign_id, "lead_id": res.lead_id, "phone": res.phone,
              "status": status, "disposition": disposition, "callback_at": None,
              "started_at": T0.isoformat(), "ended_at": ist(THU, 17, 31).isoformat(), "duration_sec": 0}
    return LookupResult(LookupStatus.FOUND, call={"call_sid": sid, "request_id": res.request_id,
                                                  "state": "ended", "result": result})


def active_call(store, lead_id="ren_0001", sid="CA1"):
    res = store.reserve(REN, lead_id, T0)
    store.mark_accepted(res.request_id, sid)
    return res


# --------------------------------------------------------------------------- lost webhooks
def test_lost_webhook_is_recovered_by_polling(env):
    store, provider, clock, rec, changes, _ = env
    res = active_call(store)
    provider.calls_by_sid["CA1"] = ended_view(res, "CA1", status="busy")
    clock.set(T0 + timedelta(minutes=9))
    once(rec)
    lead = store.lead(REN, "ren_0001")
    assert store.call(res.request_id).state == "ended"
    assert (lead.attempts, lead.priority, lead.last_status) == (1, Priority.RETRY, "busy")
    assert rec.stats["lost_webhooks_recovered"] == 1 and changes == [1]
    assert store.in_flight_count("acme_insurance") == 0


def test_young_calls_are_not_polled(env):
    store, provider, clock, rec, _, _ = env
    active_call(store)
    clock.set(T0 + timedelta(minutes=5))
    once(rec)
    assert provider.get_call_count == 0


def test_running_call_is_repolled_only_after_interval(env):
    store, provider, clock, rec, _, _ = env
    res = active_call(store)
    provider.calls_by_sid["CA1"] = LookupResult(LookupStatus.FOUND, call={"call_sid": "CA1", "state": "active"})
    clock.set(T0 + timedelta(minutes=9))
    once(rec)
    clock.advance(minutes=1)
    once(rec)
    assert provider.get_call_count == 1           # not re-polled within 2 sim minutes
    clock.advance(minutes=2)
    once(rec)
    assert provider.get_call_count == 2
    assert store.call(res.request_id).state == "active"


def test_late_webhook_after_polling_is_a_duplicate(env):
    store, provider, clock, rec, _, _ = env
    res = active_call(store)
    view = ended_view(res, "CA1", status="completed", disposition="interested")
    provider.calls_by_sid["CA1"] = view
    clock.set(T0 + timedelta(minutes=9))
    once(rec)
    assert store.apply_result(view.call["result"], clock.now()) is ApplyStatus.DUPLICATE
    assert store.lead(REN, "ren_0001").state is LeadState.COMPLETED


def test_provider_error_while_polling_keeps_the_slot(env):
    store, provider, clock, rec, _, _ = env
    res = active_call(store)
    provider.calls_by_sid["CA1"] = LookupResult(LookupStatus.ERROR, error="down")
    clock.set(T0 + timedelta(minutes=9))
    once(rec)
    assert store.call(res.request_id).state == "active"
    assert store.in_flight_count("acme_insurance") == 1


def test_accepted_call_unknown_to_provider_keeps_the_slot(env):
    store, provider, clock, rec, _, _ = env
    res = active_call(store, sid="CAghost")         # calls_by_sid has nothing: NOT_FOUND
    clock.set(T0 + timedelta(minutes=9))
    once(rec)
    assert store.call(res.request_id).state == "active"


def test_active_call_without_sid_is_polled_by_request_id(env):
    store, provider, clock, rec, _, _ = env
    res = store.reserve(REN, "ren_0001", T0)
    store.mark_accepted(res.request_id, None)
    provider.lookups[res.request_id] = ended_view(res, "CA7")
    clock.set(T0 + timedelta(minutes=9))
    once(rec)
    assert store.call(res.request_id).state == "ended"


# --------------------------------------------------------------------------- unknown outcomes
def test_unknown_found_running_becomes_active(env):
    store, provider, clock, rec, changes, _ = env
    res = store.reserve(REN, "ren_0001", T0)
    store.mark_unknown(res.request_id)
    provider.lookups[res.request_id] = LookupResult(LookupStatus.FOUND,
                                                    call={"call_sid": "CA5", "state": "active"})
    once(rec)
    call = store.call(res.request_id)
    assert (call.state, call.call_sid, call.accepted) == ("active", "CA5", True)
    assert store.lead(REN, "ren_0001").attempts == 1
    assert changes == [1]


def test_unknown_not_found_is_released(env):
    store, provider, clock, rec, _, _ = env
    res = store.reserve(REN, "ren_0001", T0)
    store.mark_unknown(res.request_id)
    once(rec)
    assert store.call(res.request_id).state == "not_placed"
    assert store.lead(REN, "ren_0001").attempts == 0
    assert store.in_flight_count("acme_insurance") == 0


def test_unknown_stays_unknown_while_provider_is_unreachable(env):
    store, provider, clock, rec, changes, _ = env
    res = store.reserve(REN, "ren_0001", T0)
    store.mark_unknown(res.request_id)
    provider.lookups[res.request_id] = LookupResult(LookupStatus.ERROR, error="down")
    once(rec)
    assert store.call(res.request_id).state == "unknown"
    assert changes == []


def test_calls_owned_by_a_dial_task_are_left_alone(env):
    store, provider, clock, rec, _, owned = env
    res = store.reserve(REN, "ren_0001", T0)
    store.mark_unknown(res.request_id)
    owned.add(res.request_id)
    once(rec)
    assert store.call(res.request_id).state == "unknown"   # the dial task will resolve it
