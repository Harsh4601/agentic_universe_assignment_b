"""Scheduling policy: what happens to a lead after each call, and who gets dialled next.

Pure functions only (no I/O, no clock). The store and dispatcher call into this module, so
every business rule about *timing* and *ordering* lives here and is unit-tested.

Rules implemented here (numbering from the assignment README):
  R4  attempts cap             -> decide_after_call
  R5  retry delays             -> decide_after_call / RetryPolicy
  R6  callbacks + priority     -> decide_after_call / select_for_dialing
  R7  terminal dispositions    -> decide_after_call
  R8  DND blocks other leads   -> state_after_phone_dnd
  R9  final-state precedence   -> decide_after_call / state_after_phone_dnd
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, tzinfo
from enum import IntEnum, StrEnum
from typing import Iterable, Mapping

from .windows import Window, next_open

TERMINAL_DISPOSITIONS = frozenset({"interested", "not_interested", "wrong_number", "dnd"})
DND_DISPOSITION = "dnd"
CALLBACK_DISPOSITION = "callback_requested"
RETRYABLE_STATUSES = frozenset({"no_answer", "busy", "failed"})


class LeadState(StrEnum):
    PENDING = "pending"
    COMPLETED = "completed"
    EXHAUSTED = "exhausted"
    DND_BLOCKED = "dnd_blocked"


FINAL_STATES = frozenset({LeadState.COMPLETED, LeadState.EXHAUSTED, LeadState.DND_BLOCKED})


class Priority(IntEnum):
    """Lower value is dialled first (R6: due callbacks, then retries, then fresh leads)."""
    CALLBACK = 0
    RETRY = 1
    FRESH = 2


@dataclass(frozen=True)
class RetryPolicy:
    no_answer: timedelta
    busy: timedelta
    failed: timedelta

    def delay_after(self, status: str) -> timedelta:
        """Wait before the next attempt after a call ending with ``status`` (R5).

        Unknown statuses get the longest configured delay: being late is safer than
        being early, and 'too early' is a violation.
        """
        known = {"no_answer": self.no_answer, "busy": self.busy, "failed": self.failed}
        return known.get(status, max(known.values()))


def parse_ts(value: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp from the provider. Naive timestamps are rejected."""
    if value in (None, ""):
        return None
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise ValueError(f"timestamp without timezone: {value!r}")
    return dt


@dataclass(frozen=True)
class CallOutcome:
    """The parts of an ended call's result that drive scheduling."""
    status: str
    disposition: str | None
    ended_at: datetime
    callback_at: datetime | None = None

    @classmethod
    def from_payload(cls, payload: Mapping) -> CallOutcome:
        """Build from a webhook body or the ``result`` of ``GET /v1/calls/...``."""
        status = payload.get("status")
        ended_at = parse_ts(payload.get("ended_at"))
        if not status or ended_at is None:
            raise ValueError("call result needs 'status' and 'ended_at'")
        return cls(
            status=status,
            disposition=payload.get("disposition") or None,
            ended_at=ended_at,
            callback_at=parse_ts(payload.get("callback_at")),
        )


@dataclass(frozen=True)
class Decision:
    """What a lead becomes. ``priority``/``next_eligible_at`` are set only when PENDING."""
    state: LeadState
    priority: Priority | None = None
    next_eligible_at: datetime | None = None
    reason: str = ""


def _schedule(priority: Priority, earliest: datetime, window: Window, tz: tzinfo, reason: str) -> Decision:
    when = next_open(earliest, window, tz)
    if when is None:  # config validation rejects never-open windows, so this is a bug
        raise ValueError("campaign window never opens; cannot schedule a call")
    return Decision(LeadState.PENDING, priority, when, reason)


def initial_decision(now: datetime, window: Window, tz: tzinfo) -> Decision:
    """Schedule a freshly loaded lead: first moment its campaign window is open."""
    return _schedule(Priority.FRESH, now, window, tz, "fresh lead")


def decide_after_call(
    outcome: CallOutcome,
    *,
    attempts: int,
    max_attempts: int,
    phone_is_dnd: bool,
    window: Window,
    tz: tzinfo,
    retry: RetryPolicy,
) -> Decision:
    """Next state of a lead whose call just ended.

    ``attempts`` counts accepted calls *including* the one that just ended. ``window`` is the
    campaign's effective window (already intersected with the regulatory window).
    The checks run in precedence order: completed > exhausted > dnd_blocked > pending.
    """
    disp = outcome.disposition
    if disp in TERMINAL_DISPOSITIONS:
        return Decision(LeadState.COMPLETED, reason=f"terminal disposition {disp}")
    if attempts >= max_attempts:
        return Decision(LeadState.EXHAUSTED, reason=f"{attempts}/{max_attempts} attempts used")
    if phone_is_dnd:
        return Decision(LeadState.DND_BLOCKED, reason="phone opted out (dnd) in another lead")
    if disp == CALLBACK_DISPOSITION and outcome.callback_at is not None:
        return _schedule(Priority.CALLBACK, outcome.callback_at, window, tz, "callback requested")
    # Retry: no_answer / busy / failed, a callback with no time given, or anything unexpected.
    delay = retry.delay_after(outcome.status)
    return _schedule(Priority.RETRY, outcome.ended_at + delay, window, tz,
                     f"retry after {outcome.status} (+{int(delay.total_seconds() // 60)}m)")


def state_after_phone_dnd(state: LeadState, *, attempts: int, max_attempts: int, in_flight: bool) -> LeadState:
    """New state of *another* lead whose phone just opted out (R8, with R9 precedence).

    Final leads keep their state. A lead with a call in flight is left pending and is settled
    when that call's result arrives (in practice R3 makes this impossible: two leads sharing a
    phone are never in calls at the same time).
    """
    if state in FINAL_STATES or in_flight:
        return state
    if attempts >= max_attempts:
        return LeadState.EXHAUSTED
    return LeadState.DND_BLOCKED


@dataclass(frozen=True)
class Candidate:
    """A pending lead competing for a dial slot."""
    campaign_id: str
    lead_id: str
    phone: str
    priority: Priority
    next_eligible_at: datetime

    @property
    def sort_key(self) -> tuple:
        # Class first (R6), then whoever has been waiting longest, then a stable tie-break.
        return (int(self.priority), self.next_eligible_at, self.campaign_id, self.lead_id)


def select_for_dialing(
    candidates: Iterable[Candidate],
    *,
    now: datetime,
    free_slots: int,
    busy_phones: set[str],
    blocked_phones: set[str],
    startable_campaigns: set[str],
) -> list[Candidate]:
    """Choose which leads of one tenant to dial right now, in priority order.

    A lead is skipped (not waited on) if it isn't due yet, its campaign can't start a call
    now, its phone opted out, or its phone already has a call in flight (R3). Two leads picked
    in the same pass never share a phone, since the same number can be in two campaigns.
    """
    picked: list[Candidate] = []
    taken = set(busy_phones)
    for cand in sorted(candidates, key=lambda c: c.sort_key):
        if len(picked) >= free_slots:
            break
        if cand.next_eligible_at > now:
            continue
        if cand.campaign_id not in startable_campaigns:
            continue
        if cand.phone in blocked_phones or cand.phone in taken:
            continue
        picked.append(cand)
        taken.add(cand.phone)
    return picked
