import dataclasses
import sqlite3
from datetime import timedelta

import pytest

from conftest import FRI, THU, ist
from dispatcher.db import connect, transaction
from dispatcher.policy import LeadState, Priority
from dispatcher.store import ApplyStatus, StaleDatabaseError, Store, request_id_for

T0 = ist(THU, 17, 30)
SHARED_PHONE = "+917695793186"           # fb_0011 (feedback) and ren_0018 (renewals)
REN, FB, ZEN = "c_acme_renewals", "c_acme_feedback", "c_zen_winback"


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "dispatcher.db"


@pytest.fixture
def store(db_path, cfg):
    s = Store(connect(db_path), cfg)
    s.init_run("run-1")
    s.seed_leads(T0)
    yield s
    s.conn.close()


def result(res, status="completed", disposition=None, ended_at=None, callback_at=None, sid=None, **extra):
    """A webhook-shaped result for a reservation."""
    body = {"event": "call.completed", "call_sid": sid or f"CA{res.request_id}", "request_id": res.request_id,
            "tenant_id": res.tenant_id, "campaign_id": res.campaign_id, "lead_id": res.lead_id,
            "phone": res.phone, "status": status, "disposition": disposition,
            "callback_at": callback_at.isoformat() if callback_at else None,
            "started_at": T0.isoformat(), "ended_at": (ended_at or ist(THU, 17, 40)).isoformat(),
            "duration_sec": 100}
    body.update(extra)
    return body


def dial_and_end(store, cid, lid, now=T0, **kw):
    """Reserve, accept and finish one attempt. Returns the reservation."""
    res = store.reserve(cid, lid, now)
    assert res is not None, f"could not reserve {cid}/{lid}"
    store.mark_accepted(res.request_id, f"CA{res.request_id}")
    assert store.apply_result(result(res, **kw), now) is ApplyStatus.APPLIED
    return res


# --------------------------------------------------------------------------- setup
def test_seed_inserts_all_leads_once(store):
    assert store.seed_leads(T0) == 0  # idempotent
    assert sum(sum(v.values()) for v in store.summary()["leads"].values()) == 200
    lead = store.lead(ZEN, "zen_0001")
    assert (lead.state, lead.priority, lead.next_eligible_at) == (LeadState.PENDING, Priority.FRESH, T0)


def test_seed_snaps_fresh_leads_to_window(db_path, cfg):
    s = Store(connect(db_path), cfg)
    s.init_run("run-1")
    s.seed_leads(ist(THU, 18, 10))  # zen closed at 18:00 on Thursday
    assert s.lead(ZEN, "zen_0001").next_eligible_at == ist(FRI, 9, 30)
    assert s.lead(REN, "ren_0001").next_eligible_at == ist(THU, 18, 10)


def test_database_is_bound_to_provider_run(db_path, store, cfg):
    again = Store(connect(db_path), cfg)
    again.init_run("run-1")  # same run: resume is fine
    with pytest.raises(StaleDatabaseError):
        again.init_run("run-2")
    with pytest.raises(StaleDatabaseError):
        Store(connect(db_path), dataclasses.replace(cfg, fingerprint="other")).init_run("run-1")


# --------------------------------------------------------------------------- reserve
def test_reserve_creates_deterministic_dialing_row(store):
    res = store.reserve(REN, "ren_0001", T0)
    assert res.request_id == request_id_for(REN, "ren_0001", 1) == "c_acme_renewals:ren_0001:1"
    call = store.call(res.request_id)
    assert (call.state, call.accepted, call.attempt_no) == ("dialing", False, 1)
    assert store.in_flight_count("acme_insurance") == 1
    assert store.busy_phones() == {res.phone}


def test_a_lead_has_at_most_one_call_in_flight(store):
    assert store.reserve(REN, "ren_0001", T0) is not None
    assert store.reserve(REN, "ren_0001", T0) is None


def test_tenant_limit_is_shared_across_campaigns(store):  # R2
    assert store.reserve(REN, "ren_0001", T0)
    assert store.reserve(REN, "ren_0002", T0)
    assert store.reserve(FB, "fb_0020", T0)
    assert store.reserve(FB, "fb_0021", T0)
    assert store.reserve(REN, "ren_0003", T0) is None      # acme is at 4
    assert store.reserve(FB, "fb_0022", T0) is None
    for i in (1, 2, 3):                                     # zen has its own 3
        assert store.reserve(ZEN, f"zen_000{i}", T0)
    assert store.reserve(ZEN, "zen_0004", T0) is None


def test_same_phone_is_never_in_two_calls(store):  # R3
    ren = store.reserve(REN, "ren_0018", T0)
    assert ren.phone == SHARED_PHONE
    assert store.reserve(FB, "fb_0011", T0) is None
    store.mark_accepted(ren.request_id, "CA1")
    store.apply_result(result(ren, status="no_answer"), T0)
    assert store.reserve(FB, "fb_0011", T0) is not None   # free again once the result is in


def test_database_index_blocks_second_inflight_call_per_phone(store):
    store.reserve(REN, "ren_0018", T0)
    with pytest.raises(sqlite3.IntegrityError):
        store.conn.execute(
            "INSERT INTO calls (request_id, campaign_id, lead_id, tenant_id, phone, attempt_no, state, reserved_at) "
            "VALUES ('x', ?, 'fb_0011', 'acme_insurance', ?, 1, 'active', 0)", (FB, SHARED_PHONE))


# --------------------------------------------------------------------------- not placed / unknown
def test_not_placed_frees_slot_and_reuses_request_id(store):  # R4: rejected calls don't count
    res = store.reserve(REN, "ren_0001", T0)
    retry_at = T0 + timedelta(seconds=45)
    assert store.mark_not_placed(res.request_id, retry_at)
    assert store.in_flight_count("acme_insurance") == 0
    lead = store.lead(REN, "ren_0001")
    assert lead.attempts == 0 and lead.next_eligible_at == retry_at
    again = store.reserve(REN, "ren_0001", retry_at)
    assert again.request_id == res.request_id
    assert store.call(res.request_id).state == "dialing"


def test_unknown_keeps_the_slot_held(store):
    res = store.reserve(REN, "ren_0001", T0)
    store.mark_unknown(res.request_id)
    assert store.call(res.request_id).state == "unknown"
    assert store.in_flight_count("acme_insurance") == 1
    assert store.busy_phones() == {res.phone}


def test_unknown_then_found_counts_one_attempt(store):
    res = store.reserve(REN, "ren_0001", T0)
    store.mark_unknown(res.request_id)
    store.mark_accepted(res.request_id, "CA1")
    assert store.call(res.request_id).state == "active"
    assert store.lead(REN, "ren_0001").attempts == 1
    assert not store.mark_not_placed(res.request_id, T0)   # can't un-place an accepted call


def test_orphaned_dialing_rows_become_unknown(store):
    res = store.reserve(REN, "ren_0001", T0)
    assert store.mark_orphans_unknown() == 1
    assert [c.request_id for c in store.calls_in_state("unknown")] == [res.request_id]


# --------------------------------------------------------------------------- results
def test_no_answer_schedules_retry_after_60_minutes(store):  # R5
    dial_and_end(store, REN, "ren_0001", status="no_answer", ended_at=ist(THU, 17, 40))
    lead = store.lead(REN, "ren_0001")
    assert (lead.state, lead.priority, lead.attempts) == (LeadState.PENDING, Priority.RETRY, 1)
    assert lead.next_eligible_at == ist(THU, 18, 40)
    assert (lead.last_status, lead.last_disposition) == ("no_answer", None)
    assert store.in_flight_count("acme_insurance") == 0


def test_duplicate_result_is_harmless(store):
    res = dial_and_end(store, REN, "ren_0001", status="no_answer")
    assert store.apply_result(result(res, status="no_answer"), T0) is ApplyStatus.DUPLICATE
    assert store.lead(REN, "ren_0001").attempts == 1


def test_webhook_before_201(store):
    res = store.reserve(REN, "ren_0001", T0)
    assert store.apply_result(result(res, status="busy"), T0) is ApplyStatus.APPLIED
    store.mark_accepted(res.request_id, f"CA{res.request_id}")  # the late 201
    assert store.call(res.request_id).state == "ended"
    assert store.lead(REN, "ren_0001").attempts == 1
    assert store.in_flight_count("acme_insurance") == 0


def test_result_for_unknown_or_mismatched_call(store):
    res = store.reserve(REN, "ren_0001", T0)
    assert store.apply_result(result(res, request_id="nope", sid="CAnope"), T0) is ApplyStatus.UNKNOWN_CALL
    assert store.apply_result(result(res, lead_id="ren_0002"), T0) is ApplyStatus.MISMATCH
    assert store.call(res.request_id).state == "dialing"


def test_result_found_by_call_sid_when_request_id_missing(store):
    res = store.reserve(REN, "ren_0001", T0)
    store.mark_accepted(res.request_id, "CAxyz")
    assert store.apply_result(result(res, sid="CAxyz", request_id=None, status="failed"), T0) is ApplyStatus.APPLIED


def test_malformed_result_raises(store):
    res = store.reserve(REN, "ren_0001", T0)
    with pytest.raises(ValueError):
        store.apply_result({"request_id": res.request_id, "status": "completed"}, T0)


def test_terminal_disposition_completes_and_lead_is_never_reserved_again(store):  # R7
    dial_and_end(store, REN, "ren_0001", disposition="not_interested")
    lead = store.lead(REN, "ren_0001")
    assert (lead.state, lead.priority, lead.next_eligible_at) == (LeadState.COMPLETED, None, None)
    assert store.reserve(REN, "ren_0001", T0) is None


def test_three_unanswered_attempts_exhaust_the_lead(store):  # R4
    for n in (1, 2, 3):
        res = dial_and_end(store, REN, "ren_0001", status="no_answer")
        assert res.request_id.endswith(f":{n}")
    lead = store.lead(REN, "ren_0001")
    assert (lead.state, lead.attempts) == (LeadState.EXHAUSTED, 3)
    assert store.reserve(REN, "ren_0001", T0) is None


def test_callback_is_scheduled_with_top_priority(store):  # R6
    dial_and_end(store, FB, "fb_0001", disposition="callback_requested", callback_at=ist(THU, 19, 30))
    lead = store.lead(FB, "fb_0001")
    assert (lead.priority, lead.next_eligible_at) == (Priority.CALLBACK, ist(THU, 19, 30))
    dial_and_end(store, FB, "fb_0002", status="failed", ended_at=ist(THU, 17, 40))  # retry at 17:55
    due = store.due_candidates("acme_insurance", ist(THU, 19, 30))
    assert [c.lead_id for c in due[:2]] == ["fb_0001", "fb_0002"]


# --------------------------------------------------------------------------- DND
def test_dnd_completes_lead_and_blocks_same_phone_elsewhere(store):  # R8
    dial_and_end(store, REN, "ren_0018", disposition="dnd")
    assert store.lead(REN, "ren_0018").state is LeadState.COMPLETED
    blocked = store.lead(FB, "fb_0011")
    assert (blocked.state, blocked.next_eligible_at) == (LeadState.DND_BLOCKED, None)
    assert store.dnd_phones() == {SHARED_PHONE}
    assert store.reserve(FB, "fb_0011", T0) is None


def test_dnd_does_not_override_exhausted_or_completed(store):  # R9 precedence
    for _ in range(3):
        dial_and_end(store, FB, "fb_0011", status="busy")
    assert store.lead(FB, "fb_0011").state is LeadState.EXHAUSTED
    dial_and_end(store, REN, "ren_0018", disposition="dnd")
    assert store.lead(FB, "fb_0011").state is LeadState.EXHAUSTED


def test_dnd_blocked_lead_with_attempts_keeps_its_history(store):
    dial_and_end(store, FB, "fb_0011", status="no_answer")
    dial_and_end(store, REN, "ren_0018", disposition="dnd")
    lead = store.lead(FB, "fb_0011")
    assert (lead.state, lead.attempts, lead.last_status) == (LeadState.DND_BLOCKED, 1, "no_answer")


def test_reserve_refuses_a_dnd_phone_and_blocks_the_lead(store):
    # Simulate a DND recorded without propagation (e.g. a lead loaded later): reserve must still refuse.
    store.conn.execute("INSERT INTO dnd_phones VALUES (?, ?, 'ren_0018', 0)", (SHARED_PHONE, REN))
    assert store.reserve(FB, "fb_0011", T0) is None
    assert store.lead(FB, "fb_0011").state is LeadState.DND_BLOCKED


# --------------------------------------------------------------------------- candidates
def test_due_candidates_skip_in_flight_and_future_leads(store):
    store.reserve(REN, "ren_0001", T0)
    dial_and_end(store, REN, "ren_0002", status="no_answer")  # due at 18:40
    ids = {c.lead_id for c in store.due_candidates("acme_insurance", T0)}
    assert "ren_0001" not in ids and "ren_0002" not in ids and "ren_0003" in ids
    assert len(ids) == 130 - 2


def test_next_wake(store):
    assert store.next_wake() == T0


# --------------------------------------------------------------------------- reconciliation helpers
def test_stale_active_calls(store):
    res = store.reserve(REN, "ren_0001", T0)
    store.mark_accepted(res.request_id, "CA1")
    older, every = timedelta(minutes=8), timedelta(minutes=2)
    assert store.stale_active_calls(T0 + timedelta(minutes=5), older, every) == []
    later = T0 + timedelta(minutes=9)
    assert [c.request_id for c in store.stale_active_calls(later, older, every)] == [res.request_id]
    store.mark_polled(res.request_id, later)
    assert store.stale_active_calls(later + timedelta(minutes=1), older, every) == []
    assert len(store.stale_active_calls(later + timedelta(minutes=3), older, every)) == 1


# --------------------------------------------------------------------------- durability
def test_state_survives_reopening_the_database(db_path, store, cfg):
    """Simulates kill -9 after reserve: the reservation is durable and still holds its slot."""
    res = store.reserve(REN, "ren_0018", T0)
    store.conn.close()
    reopened = Store(connect(db_path), cfg)
    reopened.init_run("run-1")
    assert reopened.seed_leads(T0) == 0
    assert reopened.call(res.request_id).state == "dialing"
    assert reopened.reserve(FB, "fb_0011", T0) is None     # phone still held
    reopened.conn.close()


def test_failed_transaction_leaves_nothing_behind(db_path, store):
    with pytest.raises(RuntimeError):
        with transaction(store.conn) as c:
            c.execute("UPDATE leads SET state = 'completed'")
            raise RuntimeError("crash mid-transaction")
    assert store.lead(REN, "ren_0001").state is LeadState.PENDING


# --------------------------------------------------------------------------- completion
def test_completion_and_final_rows(db_path, cfg):
    two = [l for l in cfg.leads if l.lead_id in ("ren_0001", "ren_0002")]
    s = Store(connect(db_path), dataclasses.replace(cfg, leads=tuple(two)))
    s.init_run("run-1")
    s.seed_leads(T0)
    assert not s.is_complete()
    dial_and_end(s, REN, "ren_0001", disposition="interested")
    res = s.reserve(REN, "ren_0002", T0)
    assert not s.is_complete()                              # a call is still in flight
    s.mark_accepted(res.request_id, "CA2")
    s.apply_result(result(res, status="no_answer"), T0)
    for _ in range(2):
        dial_and_end(s, REN, "ren_0002", status="no_answer")
    assert s.is_complete()
    assert s.final_rows() == [
        {"campaign_id": REN, "lead_id": "ren_0001", "final_state": "completed", "attempts": 1,
         "last_status": "completed", "last_disposition": "interested"},
        {"campaign_id": REN, "lead_id": "ren_0002", "final_state": "exhausted", "attempts": 3,
         "last_status": "no_answer", "last_disposition": ""},
    ]
