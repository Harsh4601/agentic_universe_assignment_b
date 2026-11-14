from datetime import timedelta

import pytest

from conftest import FRI, IST, MON, THU, ist
from dispatcher.policy import (
    CallOutcome, Decision, LeadState, Priority, RetryPolicy, decide_after_call, initial_decision,
    state_after_phone_dnd,
)

RETRY = RetryPolicy(no_answer=timedelta(minutes=60), busy=timedelta(minutes=60), failed=timedelta(minutes=15))


def decide(window, *, status="completed", disposition=None, ended_at=None, callback_at=None,
           attempts=1, max_attempts=3, phone_is_dnd=False) -> Decision:
    outcome = CallOutcome(status=status, disposition=disposition,
                          ended_at=ended_at or ist(THU, 17, 40), callback_at=callback_at)
    return decide_after_call(outcome, attempts=attempts, max_attempts=max_attempts,
                             phone_is_dnd=phone_is_dnd, window=window, tz=IST, retry=RETRY)


# --------------------------------------------------------------------------- R7 terminal
@pytest.mark.parametrize("disp", ["interested", "not_interested", "wrong_number", "dnd"])
@pytest.mark.parametrize("attempts", [1, 3])
def test_terminal_dispositions_complete_the_lead(renewals, disp, attempts):
    d = decide(renewals, disposition=disp, attempts=attempts)
    assert d.state is LeadState.COMPLETED
    assert d.next_eligible_at is None


def test_terminal_beats_dnd_elsewhere(renewals):
    # completed > dnd_blocked
    assert decide(renewals, disposition="interested", phone_is_dnd=True).state is LeadState.COMPLETED


# --------------------------------------------------------------------------- R4 attempts cap
@pytest.mark.parametrize("status, disp", [("no_answer", None), ("busy", None), ("failed", None),
                                          ("completed", "callback_requested")])
def test_last_attempt_without_terminal_is_exhausted(renewals, status, disp):
    d = decide(renewals, status=status, disposition=disp, attempts=3,
               callback_at=ist(THU, 18, 30) if disp else None)
    assert d.state is LeadState.EXHAUSTED


def test_exhausted_beats_dnd_blocked(renewals):
    assert decide(renewals, status="no_answer", attempts=3, phone_is_dnd=True).state is LeadState.EXHAUSTED


def test_dnd_elsewhere_blocks_a_lead_with_attempts_left(renewals):
    assert decide(renewals, status="no_answer", attempts=1, phone_is_dnd=True).state is LeadState.DND_BLOCKED


# --------------------------------------------------------------------------- R5 retry delays
@pytest.mark.parametrize("status, ended, expected", [
    ("no_answer", ist(THU, 17, 40), ist(THU, 18, 40)),
    ("busy", ist(THU, 17, 40), ist(THU, 18, 40)),
    ("failed", ist(THU, 17, 40), ist(THU, 17, 55)),
    ("no_answer", ist(THU, 18, 30), ist(FRI, 10, 0)),   # 19:30 is after close: next opening
    ("failed", ist(THU, 18, 50), ist(FRI, 10, 0)),      # 19:05 is after close
    ("failed", ist(THU, 18, 44, 59), ist(THU, 18, 59, 59)),  # still just inside
])
def test_retry_delay_measured_from_ended_at(renewals, status, ended, expected):
    d = decide(renewals, status=status, ended_at=ended)
    assert d.state is LeadState.PENDING
    assert d.priority is Priority.RETRY
    assert d.next_eligible_at == expected


def test_retry_for_zen_on_friday_evening_waits_for_monday(zen):
    d = decide(zen, status="no_answer", ended_at=ist(FRI, 17, 30))
    assert d.next_eligible_at == ist(MON, 9, 30)


def test_unknown_status_uses_longest_delay(renewals):
    d = decide(renewals, status="carrier_weirdness", ended_at=ist(THU, 17, 0))
    assert d.priority is Priority.RETRY
    assert d.next_eligible_at == ist(THU, 18, 0)


def test_retry_policy_delay_lookup():
    assert RETRY.delay_after("failed") == timedelta(minutes=15)
    assert RETRY.delay_after("no_answer") == timedelta(minutes=60)
    assert RETRY.delay_after("???") == timedelta(minutes=60)


# --------------------------------------------------------------------------- R6 callbacks
@pytest.mark.parametrize("campaign, callback_at, expected", [
    ("feedback", ist(THU, 19, 30), ist(THU, 19, 30)),   # inside window: exactly callback_at
    ("renewals", ist(THU, 20, 15), ist(FRI, 10, 0)),    # after close: next opening
    ("feedback", ist(FRI, 9, 0), ist(FRI, 11, 0)),      # before opening: same day's opening
    ("zen", ist(FRI, 19, 0), ist(MON, 9, 30)),          # over the weekend
])
def test_callback_scheduled_at_callback_time_or_next_opening(request, campaign, callback_at, expected):
    w = request.getfixturevalue(campaign)
    d = decide(w, disposition="callback_requested", callback_at=callback_at, ended_at=ist(THU, 17, 45))
    assert d.state is LeadState.PENDING
    assert d.priority is Priority.CALLBACK
    assert d.next_eligible_at == expected


def test_callback_without_time_is_treated_as_a_retry(renewals):
    d = decide(renewals, disposition="callback_requested", callback_at=None, ended_at=ist(THU, 17, 40))
    assert d.priority is Priority.RETRY
    assert d.next_eligible_at == ist(THU, 18, 40)


# --------------------------------------------------------------------------- fresh leads
@pytest.mark.parametrize("campaign, now, expected", [
    ("renewals", ist(THU, 17, 30), ist(THU, 17, 30)),
    ("zen", ist(THU, 17, 30), ist(THU, 17, 30)),
    ("zen", ist(THU, 18, 10), ist(FRI, 9, 30)),
])
def test_initial_decision(request, campaign, now, expected):
    d = initial_decision(now, request.getfixturevalue(campaign), IST)
    assert (d.state, d.priority, d.next_eligible_at) == (LeadState.PENDING, Priority.FRESH, expected)


# --------------------------------------------------------------------------- R8 DND propagation
@pytest.mark.parametrize("state, attempts, in_flight, expected", [
    (LeadState.PENDING, 0, False, LeadState.DND_BLOCKED),
    (LeadState.PENDING, 2, False, LeadState.DND_BLOCKED),
    (LeadState.PENDING, 3, False, LeadState.EXHAUSTED),     # exhausted > dnd_blocked
    (LeadState.PENDING, 1, True, LeadState.PENDING),        # settled when its call ends
    (LeadState.COMPLETED, 1, False, LeadState.COMPLETED),
    (LeadState.EXHAUSTED, 3, False, LeadState.EXHAUSTED),
    (LeadState.DND_BLOCKED, 1, False, LeadState.DND_BLOCKED),
])
def test_state_after_phone_dnd(state, attempts, in_flight, expected):
    assert state_after_phone_dnd(state, attempts=attempts, max_attempts=3, in_flight=in_flight) is expected


# --------------------------------------------------------------------------- payload parsing
def test_outcome_from_webhook_payload():
    o = CallOutcome.from_payload({
        "event": "call.completed", "status": "completed", "disposition": "callback_requested",
        "callback_at": "2026-11-12T20:15:00+05:30", "ended_at": "2026-11-12T18:42:37.401037+05:30",
    })
    assert o.status == "completed" and o.disposition == "callback_requested"
    assert o.callback_at == ist(THU, 20, 15)
    assert o.ended_at.tzinfo is not None


def test_outcome_from_payload_null_disposition():
    o = CallOutcome.from_payload({"status": "no_answer", "disposition": None, "callback_at": None,
                                  "ended_at": "2026-11-12T18:00:00+05:30"})
    assert o.disposition is None and o.callback_at is None


@pytest.mark.parametrize("payload", [
    {"status": "no_answer"},                                        # no ended_at
    {"ended_at": "2026-11-12T18:00:00+05:30"},                      # no status
    {"status": "no_answer", "ended_at": "2026-11-12T18:00:00"},     # naive timestamp
])
def test_outcome_from_payload_rejects_incomplete(payload):
    with pytest.raises(ValueError):
        CallOutcome.from_payload(payload)
