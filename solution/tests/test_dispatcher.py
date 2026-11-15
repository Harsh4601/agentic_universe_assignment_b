"""Dispatcher behaviour with a fake clock and a fake provider (no network, no waiting)."""
import asyncio
from datetime import timedelta

import pytest

from conftest import FRI, THU, ist
from dispatcher.clock import FakeClock
from dispatcher.db import connect
from dispatcher.dispatcher import Dispatcher, DispatchSettings
from dispatcher.policy import LeadState, Priority
from dispatcher.provider import DialResult, DialStatus, LookupResult, LookupStatus
from dispatcher.store import Store
from fakes import FakeProvider

REN, FB, ZEN = "c_acme_renewals", "c_acme_feedback", "c_zen_winback"
SETTINGS = DispatchSettings(callback_url="http://cb/webhooks/telephony")


@pytest.fixture
def make(tmp_path, cfg):
    """make(start) -> (dispatcher, store, provider, clock). Leads are seeded at `start`."""
    stores = []

    def _make(start):
        store = Store(connect(tmp_path / "d.db"), cfg)
        store.init_run("run-1")
        store.seed_leads(start)
        stores.append(store)
        clock, provider = FakeClock(start), FakeProvider()
        return Dispatcher(store, provider, clock, cfg, SETTINGS), store, provider, clock
    yield _make
    for s in stores:
        s.conn.close()


def run(coro_fn):
    return asyncio.run(coro_fn())


def end_call(store, rid, now, status="no_answer", disposition=None, callback_at=None):
    call = store.call(rid)
    store.apply_result({"request_id": rid, "call_sid": call.call_sid, "campaign_id": call.campaign_id,
                        "lead_id": call.lead_id, "status": status, "disposition": disposition,
                        "callback_at": callback_at.isoformat() if callback_at else None,
                        "ended_at": now.isoformat()}, now)


def test_fills_every_tenant_up_to_its_limit(make):
    d, store, provider, _ = make(ist(THU, 17, 30))

    async def go():
        assert d.dispatch_once() == 7          # acme 4 + zen 3
        await d.drain()
        assert d.dispatch_once() == 0          # everyone at their limit
    run(go)
    assert store.in_flight_count("acme_insurance") == 4
    assert store.in_flight_count("zen_retail") == 3
    assert all(c.state == "active" for c in store.calls_in_state("active")) and len(provider.posts) == 7
    assert {p["callback_url"] for p in provider.posts} == {SETTINGS.callback_url}


def test_closed_campaign_is_deferred_to_its_next_opening(make):
    d, store, provider, _ = make(ist(THU, 17, 30))
    d.clock.set(ist(THU, 18, 10))  # zen closed at 18:00; acme still open

    async def go():
        d.dispatch_once()
        await d.drain()
    run(go)
    assert ZEN not in {p["campaign_id"] for p in provider.posts}
    assert len(provider.posts) == 4                                  # acme still fills its 4 slots
    assert store.lead(ZEN, "zen_0001").next_eligible_at == ist(FRI, 9, 30)


def test_no_new_calls_inside_the_end_margin(make):
    d, store, provider, _ = make(ist(THU, 17, 30))
    d.clock.set(ist(THU, 18, 59, 30))  # renewals closes 19:00, margin is 60s

    async def go():
        d.dispatch_once()
        await d.drain()
    run(go)
    assert {p["campaign_id"] for p in provider.posts} == {FB}
    assert store.lead(REN, "ren_0001").next_eligible_at == ist(FRI, 10, 0)


def test_due_callback_goes_before_fresh_leads(make):
    d, store, provider, clock = make(ist(THU, 17, 30))
    res = store.reserve(FB, "fb_0050", clock.now())
    store.mark_accepted(res.request_id, "CA1")
    end_call(store, res.request_id, ist(THU, 17, 35), status="completed", disposition="callback_requested",
             callback_at=ist(THU, 17, 50))
    clock.set(ist(THU, 17, 50))

    async def go():
        d.dispatch_once()
        await d.drain()
    run(go)
    assert provider.posts[0]["lead_id"] == "fb_0050"
    assert provider.posts[0]["request_id"] == f"{FB}:fb_0050:2"


def test_503_frees_the_slot_and_backs_off(make):
    d, store, provider, clock = make(ist(THU, 17, 30))
    rid = f"{FB}:fb_0001:1"
    provider.script(rid, DialResult(DialStatus.NOT_PLACED, 503))

    async def go():
        d.dispatch_once()
        await d.drain()
    run(go)
    assert store.call(rid).state == "not_placed"
    lead = store.lead(FB, "fb_0001")
    assert lead.attempts == 0
    assert lead.next_eligible_at == clock.now() + SETTINGS.not_placed_backoff
    assert store.in_flight_count("acme_insurance") == 3


def test_504_resolved_as_placed_and_already_ended(make):
    d, store, provider, clock = make(ist(THU, 17, 30))
    rid = f"{FB}:fb_0001:1"
    provider.script(rid, DialResult(DialStatus.UNKNOWN, 504))
    provider.lookups[rid] = LookupResult(LookupStatus.FOUND, call={
        "call_sid": "CA9", "state": "ended", "result": {
            "request_id": rid, "call_sid": "CA9", "campaign_id": FB, "lead_id": "fb_0001",
            "status": "busy", "disposition": None, "ended_at": ist(THU, 17, 31).isoformat()}})

    async def go():
        d.dispatch_once()
        await d.drain()
    run(go)
    call, lead = store.call(rid), store.lead(FB, "fb_0001")
    assert (call.state, call.accepted, call.call_sid) == ("ended", True, "CA9")
    assert (lead.attempts, lead.priority, lead.last_status) == (1, Priority.RETRY, "busy")


def test_504_resolved_as_not_placed(make):
    d, store, provider, _ = make(ist(THU, 17, 30))
    rid = f"{FB}:fb_0001:1"
    provider.script(rid, DialResult(DialStatus.UNKNOWN, 504))   # lookup defaults to NOT_FOUND

    async def go():
        d.dispatch_once()
        await d.drain()
    run(go)
    assert store.call(rid).state == "not_placed"
    assert store.lead(FB, "fb_0001").attempts == 0


def test_504_with_provider_unreachable_keeps_slot_held(make):
    d, store, provider, _ = make(ist(THU, 17, 30))
    rid = f"{FB}:fb_0001:1"
    provider.script(rid, DialResult(DialStatus.UNKNOWN, 504))
    provider.lookups[rid] = LookupResult(LookupStatus.ERROR, error="down")

    async def go():
        d.dispatch_once()
        await d.drain()
    run(go)
    assert store.call(rid).state == "unknown"
    assert store.in_flight_count("acme_insurance") == 4


def test_rule_rejection_backs_off_for_long(make):
    d, store, provider, clock = make(ist(THU, 17, 30))
    rid = f"{FB}:fb_0001:1"
    provider.script(rid, DialResult(DialStatus.REJECTED, 429, error="tenant_concurrency_exceeded"))

    async def go():
        d.dispatch_once()
        await d.drain()
    run(go)
    assert store.call(rid).state == "not_placed"
    assert store.lead(FB, "fb_0001").next_eligible_at == clock.now() + SETTINGS.rejected_backoff


def test_unexpected_error_leaves_call_unknown_for_reconciliation(make):
    d, store, provider, _ = make(ist(THU, 17, 30))
    provider.raise_on_post = RuntimeError("boom")

    async def go():
        d.dispatch_once()
        await d.drain()
    run(go)
    assert len(store.calls_in_state("unknown")) == 7


def test_pending_lead_on_dnd_phone_is_settled_not_left_hanging(make):
    d, store, provider, clock = make(ist(THU, 17, 30))
    store.conn.execute("INSERT INTO dnd_phones VALUES ('+917695793186', ?, 'ren_0018', 0)", (REN,))

    async def go():
        d.dispatch_once()
        await d.drain()
    run(go)
    assert store.lead(FB, "fb_0011").state is LeadState.DND_BLOCKED
    assert store.lead(REN, "ren_0018").state is LeadState.DND_BLOCKED


def test_run_until_complete_returns_when_all_final(make, cfg, tmp_path):
    d, store, provider, clock = make(ist(THU, 17, 30))
    store.conn.execute("UPDATE leads SET state = 'completed', priority = NULL, next_eligible_at = NULL")
    assert run(lambda: d.run_until_complete()) is True


def test_run_until_complete_honours_stop(make):
    d, store, provider, clock = make(ist(THU, 17, 30))
    assert run(lambda: d.run_until_complete(should_stop=lambda: True)) is False
