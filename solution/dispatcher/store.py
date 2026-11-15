"""All reads and writes of dispatcher state. Each public write method is one transaction.

The flow of one attempt, as seen by this module:

    reserve()                 lead pending -> call row 'dialing' (slot + phone now held)
      mark_accepted()         'dialing'/'unknown' -> 'active'      (201/200, or found by GET)
      mark_not_placed()       'dialing'/'unknown' -> 'not_placed'  (503, or 404 after a 504)
      mark_unknown()          'dialing' -> 'unknown'               (504, timeout: outcome unknown)
    apply_result()            any non-ended -> 'ended', lead gets its next state

apply_result() is the only place a call outcome changes a lead. It is idempotent: a second
delivery of the same result is reported as DUPLICATE and changes nothing. The slot and phone
are released in the same transaction that applies the result (including DND propagation),
so no other lead with that phone can be dialled before we know how the call ended.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Iterator, Mapping

from .config import Config
from .db import INFLIGHT_SQL, transaction
from .policy import (
    DND_DISPOSITION, FINAL_STATES, CallOutcome, Candidate, LeadState, Priority, decide_after_call,
    initial_decision, state_after_phone_dnd,
)

log = logging.getLogger(__name__)

# Higher wins when two decisions disagree about a lead (R9 precedence).
_STATE_RANK = {LeadState.PENDING: 0, LeadState.DND_BLOCKED: 1, LeadState.EXHAUSTED: 2, LeadState.COMPLETED: 3}


class StaleDatabaseError(Exception):
    """The database was built for a different provider run or different fixtures."""

    def __init__(self, key: str, message: str):
        super().__init__(message)
        self.key = key  # 'provider_run' or 'fixtures'


class ApplyStatus(StrEnum):
    APPLIED = "applied"
    DUPLICATE = "duplicate"          # this call's result was already applied
    UNKNOWN_CALL = "unknown_call"    # no call with this request_id / call_sid
    MISMATCH = "mismatch"            # payload's campaign/lead doesn't match our record


def request_id_for(campaign_id: str, lead_id: str, attempt_no: int) -> str:
    return f"{campaign_id}:{lead_id}:{attempt_no}"


@dataclass(frozen=True)
class Reservation:
    request_id: str
    campaign_id: str
    lead_id: str
    tenant_id: str
    phone: str
    attempt_no: int


@dataclass(frozen=True)
class CallRecord:
    request_id: str
    campaign_id: str
    lead_id: str
    tenant_id: str
    phone: str
    attempt_no: int
    state: str
    accepted: bool
    call_sid: str | None
    status: str | None
    disposition: str | None
    reserved_at: datetime


@dataclass(frozen=True)
class LeadRecord:
    campaign_id: str
    lead_id: str
    tenant_id: str
    phone: str
    state: LeadState
    priority: Priority | None
    next_eligible_at: datetime | None
    attempts: int
    last_status: str | None
    last_disposition: str | None


class Store:
    def __init__(self, conn: sqlite3.Connection, cfg: Config):
        self.conn = conn
        self.cfg = cfg
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ helpers
    @contextmanager
    def _txn(self) -> Iterator[sqlite3.Connection]:
        with self._lock, transaction(self.conn) as c:
            yield c

    @contextmanager
    def _read(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            yield self.conn

    @staticmethod
    def _ts(dt: datetime | None) -> float | None:
        return dt.timestamp() if dt is not None else None

    def _dt(self, ts: float | None) -> datetime | None:
        return datetime.fromtimestamp(ts, tz=self.cfg.tz) if ts is not None else None

    def _call_record(self, row: sqlite3.Row) -> CallRecord:
        return CallRecord(
            request_id=row["request_id"], campaign_id=row["campaign_id"], lead_id=row["lead_id"],
            tenant_id=row["tenant_id"], phone=row["phone"], attempt_no=row["attempt_no"],
            state=row["state"], accepted=bool(row["accepted"]), call_sid=row["call_sid"],
            status=row["status"], disposition=row["disposition"], reserved_at=self._dt(row["reserved_at"]),
        )

    def _lead_record(self, row: sqlite3.Row) -> LeadRecord:
        return LeadRecord(
            campaign_id=row["campaign_id"], lead_id=row["lead_id"], tenant_id=row["tenant_id"],
            phone=row["phone"], state=LeadState(row["state"]),
            priority=Priority(row["priority"]) if row["priority"] is not None else None,
            next_eligible_at=self._dt(row["next_eligible_at"]), attempts=row["attempts"],
            last_status=row["last_status"], last_disposition=row["last_disposition"],
        )

    @staticmethod
    def _inflight_for_lead(c: sqlite3.Connection, campaign_id: str, lead_id: str) -> bool:
        return c.execute(
            f"SELECT 1 FROM calls WHERE campaign_id = ? AND lead_id = ? AND state IN {INFLIGHT_SQL}",
            (campaign_id, lead_id)).fetchone() is not None

    @staticmethod
    def _refresh_attempts(c: sqlite3.Connection, campaign_id: str, lead_id: str) -> int:
        """Attempts are derived from accepted call rows, never incremented blindly."""
        n = c.execute("SELECT COUNT(*) FROM calls WHERE campaign_id = ? AND lead_id = ? AND accepted = 1",
                      (campaign_id, lead_id)).fetchone()[0]
        c.execute("UPDATE leads SET attempts = ? WHERE campaign_id = ? AND lead_id = ?", (n, campaign_id, lead_id))
        return n

    # ------------------------------------------------------------------ run setup
    def init_run(self, provider_run_id: str) -> None:
        """Bind this database to one provider run and one set of fixtures.

        Raises StaleDatabaseError if it was created for something else: resuming with stale
        attempt counts or request_ids against a restarted provider would break the rules.
        """
        with self._txn() as c:
            meta = {r["key"]: r["value"] for r in c.execute("SELECT key, value FROM meta")}
            for key, expected in (("provider_run", provider_run_id), ("fixtures", self.cfg.fingerprint)):
                have = meta.get(key)
                if have is None:
                    c.execute("INSERT INTO meta (key, value) VALUES (?, ?)", (key, expected))
                elif have != expected:
                    raise StaleDatabaseError(key, f"database belongs to {key}={have!r}, current is {expected!r}")

    def seed_leads(self, now: datetime) -> int:
        """Insert every lead from the fixtures. Idempotent: existing leads are left untouched."""
        inserted = 0
        with self._txn() as c:
            for lead in self.cfg.leads:
                camp = self.cfg.campaigns[lead.campaign_id]
                d = initial_decision(now, camp.window, self.cfg.tz)
                cur = c.execute(
                    "INSERT OR IGNORE INTO leads (campaign_id, lead_id, tenant_id, phone, state, priority, "
                    "next_eligible_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (lead.campaign_id, lead.lead_id, lead.tenant_id, lead.phone, d.state.value,
                     int(d.priority), self._ts(d.next_eligible_at), self._ts(now)))
                inserted += cur.rowcount
        return inserted

    # ------------------------------------------------------------------ reads for the dispatcher
    def due_candidates(self, tenant_id: str, now: datetime) -> list[Candidate]:
        """Pending leads of a tenant that are due and have no call in flight, best first."""
        with self._read() as c:
            rows = c.execute(
                f"""SELECT l.campaign_id, l.lead_id, l.phone, l.priority, l.next_eligible_at FROM leads l
                    WHERE l.tenant_id = ? AND l.state = 'pending' AND l.next_eligible_at <= ?
                      AND NOT EXISTS (SELECT 1 FROM calls k WHERE k.campaign_id = l.campaign_id
                                      AND k.lead_id = l.lead_id AND k.state IN {INFLIGHT_SQL})
                    ORDER BY l.priority, l.next_eligible_at, l.campaign_id, l.lead_id""",
                (tenant_id, self._ts(now))).fetchall()
        return [Candidate(r["campaign_id"], r["lead_id"], r["phone"], Priority(r["priority"]),
                          self._dt(r["next_eligible_at"])) for r in rows]

    def in_flight_count(self, tenant_id: str) -> int:
        with self._read() as c:
            return c.execute(f"SELECT COUNT(*) FROM calls WHERE tenant_id = ? AND state IN {INFLIGHT_SQL}",
                             (tenant_id,)).fetchone()[0]

    def busy_phones(self) -> set[str]:
        with self._read() as c:
            return {r[0] for r in c.execute(f"SELECT phone FROM calls WHERE state IN {INFLIGHT_SQL}")}

    def dnd_phones(self) -> set[str]:
        with self._read() as c:
            return {r[0] for r in c.execute("SELECT phone FROM dnd_phones")}

    def next_wake(self) -> datetime | None:
        """Earliest time any pending lead becomes due (lets the dispatcher sleep instead of spin)."""
        with self._read() as c:
            ts = c.execute("SELECT MIN(next_eligible_at) FROM leads WHERE state = 'pending'").fetchone()[0]
        return self._dt(ts)

    # ------------------------------------------------------------------ the dial lifecycle
    def reserve(self, campaign_id: str, lead_id: str, now: datetime) -> Reservation | None:
        """Atomically claim a tenant slot and the phone for this lead's next attempt.

        Re-checks every rule that depends on stored state (lead still pending, attempts left,
        phone not opted out, tenant under its limit, phone and lead not already in a call)
        inside one write transaction. Returns None, changing nothing, if any check fails.
        The caller has already checked the calling window for ``now``.
        """
        camp = self.cfg.campaigns[campaign_id]
        tenant = self.cfg.tenants[camp.tenant_id]
        with self._txn() as c:
            lead = c.execute("SELECT * FROM leads WHERE campaign_id = ? AND lead_id = ?",
                             (campaign_id, lead_id)).fetchone()
            if lead is None or lead["state"] != LeadState.PENDING:
                return None
            if lead["attempts"] >= camp.max_attempts:
                log.error("pending lead %s/%s already has %d attempts", campaign_id, lead_id, lead["attempts"])
                return None
            phone = lead["phone"]
            if c.execute("SELECT 1 FROM dnd_phones WHERE phone = ?", (phone,)).fetchone():
                self._block_for_dnd(c, campaign_id, lead_id, now)
                return None
            active = c.execute(f"SELECT COUNT(*) FROM calls WHERE tenant_id = ? AND state IN {INFLIGHT_SQL}",
                               (tenant.tenant_id,)).fetchone()[0]
            if active >= tenant.max_concurrent_calls:
                return None

            attempt_no = lead["attempts"] + 1
            rid = request_id_for(campaign_id, lead_id, attempt_no)
            existing = c.execute("SELECT state FROM calls WHERE request_id = ?", (rid,)).fetchone()
            try:
                if existing is None:
                    c.execute(
                        "INSERT INTO calls (request_id, campaign_id, lead_id, tenant_id, phone, attempt_no, "
                        "state, reserved_at, post_count) VALUES (?, ?, ?, ?, ?, ?, 'dialing', ?, 1)",
                        (rid, campaign_id, lead_id, tenant.tenant_id, phone, attempt_no, self._ts(now)))
                elif existing["state"] == "not_placed":
                    # Same attempt slot, same request_id: if an earlier POST did land after all,
                    # the provider dedupes this one instead of placing a second call.
                    c.execute("UPDATE calls SET state = 'dialing', reserved_at = ?, last_polled_at = NULL, "
                              "post_count = post_count + 1 WHERE request_id = ?", (self._ts(now), rid))
                else:
                    log.error("attempt slot %s is already %s; not reserving", rid, existing["state"])
                    return None
            except sqlite3.IntegrityError:
                return None  # phone or lead already has a call in flight (R3)
        return Reservation(rid, campaign_id, lead_id, tenant.tenant_id, phone, attempt_no)

    def mark_accepted(self, request_id: str, call_sid: str | None) -> bool:
        """Record proof the provider accepted the call (201, 200 replay, or found by GET).

        Never moves an ended call back to active: the webhook can arrive before the 201.
        """
        with self._txn() as c:
            row = c.execute("SELECT campaign_id, lead_id FROM calls WHERE request_id = ?", (request_id,)).fetchone()
            if row is None:
                return False
            c.execute("UPDATE calls SET accepted = 1, call_sid = COALESCE(call_sid, ?) WHERE request_id = ?",
                      (call_sid, request_id))
            try:
                c.execute("UPDATE calls SET state = 'active' WHERE request_id = ? "
                          "AND state IN ('dialing', 'unknown', 'not_placed')", (request_id,))
            except sqlite3.IntegrityError:
                # Accepted after we had declared it not placed and something else took the phone.
                # Should be impossible (we reuse request_ids), so shout but keep the attempt count.
                log.critical("call %s accepted but its phone/lead is already in another call", request_id)
            self._refresh_attempts(c, row["campaign_id"], row["lead_id"])
        return True

    def mark_unknown(self, request_id: str) -> None:
        """The POST's outcome is unknown (504, timeout, connection error). Keep the slot held."""
        with self._txn() as c:
            c.execute("UPDATE calls SET state = 'unknown' WHERE request_id = ? AND state = 'dialing'", (request_id,))

    def mark_not_placed(self, request_id: str, retry_at: datetime) -> bool:
        """The call definitely wasn't placed. Free the slot; the lead may be retried from ``retry_at``.

        No attempt is counted. Returns False if the call had already moved on (e.g. accepted).
        """
        with self._txn() as c:
            row = c.execute("SELECT campaign_id, lead_id FROM calls WHERE request_id = ? "
                            "AND state IN ('dialing', 'unknown')", (request_id,)).fetchone()
            if row is None:
                return False
            c.execute("UPDATE calls SET state = 'not_placed' WHERE request_id = ?", (request_id,))
            c.execute("UPDATE leads SET next_eligible_at = MAX(next_eligible_at, ?) "
                      "WHERE campaign_id = ? AND lead_id = ? AND state = 'pending'",
                      (self._ts(retry_at), row["campaign_id"], row["lead_id"]))
        return True

    def mark_orphans_unknown(self) -> int:
        """At startup: 'dialing' rows were left by a crashed process mid-dial; their outcome is unknown."""
        with self._txn() as c:
            return c.execute("UPDATE calls SET state = 'unknown' WHERE state = 'dialing'").rowcount

    def defer(self, campaign_id: str, lead_id: str, until: datetime) -> None:
        """Push a pending lead's next attempt later (e.g. its window is closing)."""
        with self._txn() as c:
            c.execute("UPDATE leads SET next_eligible_at = MAX(next_eligible_at, ?) "
                      "WHERE campaign_id = ? AND lead_id = ? AND state = 'pending'",
                      (self._ts(until), campaign_id, lead_id))

    def apply_result(self, payload: Mapping, now: datetime) -> ApplyStatus:
        """Apply an ended call's result (webhook body or polled ``result``). Idempotent.

        Raises ValueError if the payload is malformed (missing status/ended_at).
        """
        outcome = CallOutcome.from_payload(payload)
        rid, sid = payload.get("request_id"), payload.get("call_sid")
        with self._txn() as c:
            call = None
            if rid:
                call = c.execute("SELECT * FROM calls WHERE request_id = ?", (rid,)).fetchone()
            if call is None and sid:
                call = c.execute("SELECT * FROM calls WHERE call_sid = ?", (sid,)).fetchone()
            if call is None:
                return ApplyStatus.UNKNOWN_CALL
            if call["state"] == "ended":
                return ApplyStatus.DUPLICATE
            cid, lid = call["campaign_id"], call["lead_id"]
            if payload.get("campaign_id", cid) != cid or payload.get("lead_id", lid) != lid:
                log.error("result for %s names %s/%s but we dialled %s/%s", call["request_id"],
                          payload.get("campaign_id"), payload.get("lead_id"), cid, lid)
                return ApplyStatus.MISMATCH

            c.execute(
                "UPDATE calls SET state = 'ended', accepted = 1, call_sid = COALESCE(call_sid, ?), status = ?, "
                "disposition = ?, callback_at = ?, started_at = ?, ended_at = ?, duration_sec = ? "
                "WHERE request_id = ?",
                (sid, outcome.status, outcome.disposition, self._ts(outcome.callback_at),
                 self._ts(_parse_optional(payload.get("started_at"))), self._ts(outcome.ended_at),
                 payload.get("duration_sec"), call["request_id"]))
            attempts = self._refresh_attempts(c, cid, lid)

            phone = call["phone"]
            if outcome.disposition == DND_DISPOSITION:
                c.execute("INSERT OR IGNORE INTO dnd_phones (phone, campaign_id, lead_id, at) VALUES (?, ?, ?, ?)",
                          (phone, cid, lid, self._ts(outcome.ended_at)))
            phone_is_dnd = c.execute("SELECT 1 FROM dnd_phones WHERE phone = ?", (phone,)).fetchone() is not None

            camp = self.cfg.campaigns[cid]
            decision = decide_after_call(outcome, attempts=attempts, max_attempts=camp.max_attempts,
                                         phone_is_dnd=phone_is_dnd, window=camp.window, tz=self.cfg.tz,
                                         retry=self.cfg.retry)
            lead = c.execute("SELECT state FROM leads WHERE campaign_id = ? AND lead_id = ?", (cid, lid)).fetchone()
            current = LeadState(lead["state"])
            new_state = decision.state
            if current in FINAL_STATES and _STATE_RANK[current] > _STATE_RANK[new_state]:
                new_state = current  # never downgrade a final lead (defensive; R3 makes this unreachable)
            pending = new_state is LeadState.PENDING

            last = c.execute("SELECT status, disposition FROM calls WHERE campaign_id = ? AND lead_id = ? "
                             "AND state = 'ended' ORDER BY ended_at DESC, attempt_no DESC LIMIT 1",
                             (cid, lid)).fetchone()
            c.execute(
                "UPDATE leads SET state = ?, priority = ?, next_eligible_at = ?, callback_at = ?, "
                "last_status = ?, last_disposition = ?, updated_at = ? WHERE campaign_id = ? AND lead_id = ?",
                (new_state.value, int(decision.priority) if pending else None,
                 self._ts(decision.next_eligible_at) if pending else None,
                 self._ts(outcome.callback_at), last["status"], last["disposition"], self._ts(now), cid, lid))

            if outcome.disposition == DND_DISPOSITION:
                self._propagate_dnd(c, phone, now, exclude=(cid, lid))
        return ApplyStatus.APPLIED

    def settle_dnd_lead(self, campaign_id: str, lead_id: str, now: datetime) -> None:
        """Finalise a pending lead whose phone is on the DND list (dnd_blocked, or exhausted)."""
        with self._txn() as c:
            if c.execute("SELECT 1 FROM leads l JOIN dnd_phones d ON d.phone = l.phone "
                         "WHERE l.campaign_id = ? AND l.lead_id = ?", (campaign_id, lead_id)).fetchone():
                self._block_for_dnd(c, campaign_id, lead_id, now)

    def _propagate_dnd(self, c: sqlite3.Connection, phone: str, now: datetime, exclude: tuple[str, str]) -> None:
        """R8: every other unfinished lead with this phone stops being dialled."""
        for lead in c.execute("SELECT campaign_id, lead_id FROM leads WHERE phone = ? AND state = 'pending'",
                              (phone,)).fetchall():
            if (lead["campaign_id"], lead["lead_id"]) != exclude:
                self._block_for_dnd(c, lead["campaign_id"], lead["lead_id"], now)

    def _block_for_dnd(self, c: sqlite3.Connection, campaign_id: str, lead_id: str, now: datetime) -> None:
        lead = c.execute("SELECT state, attempts FROM leads WHERE campaign_id = ? AND lead_id = ?",
                         (campaign_id, lead_id)).fetchone()
        new = state_after_phone_dnd(LeadState(lead["state"]), attempts=lead["attempts"],
                                    max_attempts=self.cfg.campaigns[campaign_id].max_attempts,
                                    in_flight=self._inflight_for_lead(c, campaign_id, lead_id))
        if new is not LeadState(lead["state"]):
            c.execute("UPDATE leads SET state = ?, priority = NULL, next_eligible_at = NULL, updated_at = ? "
                      "WHERE campaign_id = ? AND lead_id = ?", (new.value, self._ts(now), campaign_id, lead_id))

    # ------------------------------------------------------------------ reconciliation support
    def calls_in_state(self, *states: str) -> list[CallRecord]:
        marks = ", ".join("?" for _ in states)
        with self._read() as c:
            rows = c.execute(f"SELECT * FROM calls WHERE state IN ({marks}) ORDER BY reserved_at", states).fetchall()
        return [self._call_record(r) for r in rows]

    def stale_active_calls(self, now: datetime, older_than: timedelta, repoll_every: timedelta) -> list[CallRecord]:
        """Active calls that should have ended by now and haven't been polled recently (lost webhooks)."""
        with self._read() as c:
            rows = c.execute(
                "SELECT * FROM calls WHERE state = 'active' AND reserved_at <= ? "
                "AND (last_polled_at IS NULL OR last_polled_at <= ?) ORDER BY reserved_at",
                (self._ts(now - older_than), self._ts(now - repoll_every))).fetchall()
        return [self._call_record(r) for r in rows]

    def mark_polled(self, request_id: str, now: datetime) -> None:
        with self._txn() as c:
            c.execute("UPDATE calls SET last_polled_at = ? WHERE request_id = ?", (self._ts(now), request_id))

    def record_webhook(self, *, now: datetime, raw_body: bytes, signature_ok: bool, outcome: str,
                       delivery_id: str | None = None, request_id: str | None = None,
                       call_sid: str | None = None) -> None:
        with self._txn() as c:
            c.execute("INSERT INTO webhook_events (received_at, delivery_id, request_id, call_sid, signature_ok, "
                      "outcome, raw_body) VALUES (?, ?, ?, ?, ?, ?, ?)",
                      (self._ts(now), delivery_id, request_id, call_sid, int(signature_ok), outcome,
                       raw_body.decode("utf-8", errors="replace")))

    # ------------------------------------------------------------------ completion and reporting
    def is_complete(self) -> bool:
        """True when every lead is final and no call is in flight (R9)."""
        with self._read() as c:
            pending = c.execute("SELECT COUNT(*) FROM leads WHERE state = 'pending'").fetchone()[0]
            inflight = c.execute(f"SELECT COUNT(*) FROM calls WHERE state IN {INFLIGHT_SQL}").fetchone()[0]
        return pending == 0 and inflight == 0

    def final_rows(self) -> list[dict]:
        """Rows for final_state.csv, sorted by campaign_id, lead_id."""
        with self._read() as c:
            rows = c.execute("SELECT campaign_id, lead_id, state, attempts, last_status, last_disposition "
                             "FROM leads ORDER BY campaign_id, lead_id").fetchall()
        return [{"campaign_id": r["campaign_id"], "lead_id": r["lead_id"], "final_state": r["state"],
                 "attempts": r["attempts"], "last_status": r["last_status"] or "",
                 "last_disposition": r["last_disposition"] or ""} for r in rows]

    def lead(self, campaign_id: str, lead_id: str) -> LeadRecord | None:
        with self._read() as c:
            row = c.execute("SELECT * FROM leads WHERE campaign_id = ? AND lead_id = ?",
                            (campaign_id, lead_id)).fetchone()
        return self._lead_record(row) if row else None

    def call(self, request_id: str) -> CallRecord | None:
        with self._read() as c:
            row = c.execute("SELECT * FROM calls WHERE request_id = ?", (request_id,)).fetchone()
        return self._call_record(row) if row else None

    def summary(self) -> dict:
        """Progress counts for logs and a /status endpoint."""
        with self._read() as c:
            by_campaign: dict[str, dict[str, int]] = {}
            for r in c.execute("SELECT campaign_id, state, COUNT(*) AS n FROM leads GROUP BY campaign_id, state"):
                by_campaign.setdefault(r["campaign_id"], {})[r["state"]] = r["n"]
            inflight = {r["tenant_id"]: r["n"] for r in c.execute(
                f"SELECT tenant_id, COUNT(*) AS n FROM calls WHERE state IN {INFLIGHT_SQL} GROUP BY tenant_id")}
            calls = {r["state"]: r["n"] for r in c.execute("SELECT state, COUNT(*) AS n FROM calls GROUP BY state")}
        return {"leads": by_campaign, "in_flight": inflight, "calls": calls}


def _parse_optional(value) -> datetime | None:
    try:
        return datetime.fromisoformat(value) if value else None
    except (TypeError, ValueError):
        return None
