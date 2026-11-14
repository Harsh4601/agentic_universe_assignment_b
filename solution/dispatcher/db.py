"""SQLite connection, schema and transactions.

Durability model: every state change is a committed transaction, and nothing important lives
only in memory. A `kill -9` therefore loses at most the transaction in progress, which SQLite
rolls back on the next open. WAL keeps readers and the single writer from blocking each other.

Two invariants are enforced by the database itself, as a last line of defence under the
application checks:
  * one in-flight call per phone (R3)         -> unique partial index one_inflight_per_phone
  * one in-flight call per lead               -> unique partial index one_inflight_per_lead
"In flight" means a call row in state dialing, unknown or active: from the moment we decide
to dial until we have applied the call's result.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

SCHEMA_VERSION = "1"
INFLIGHT_STATES = ("dialing", "unknown", "active")
INFLIGHT_SQL = "('dialing', 'unknown', 'active')"

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- One row per lead. Times are simulated instants stored as UTC epoch seconds.
CREATE TABLE IF NOT EXISTS leads (
    campaign_id      TEXT NOT NULL,
    lead_id          TEXT NOT NULL,
    tenant_id        TEXT NOT NULL,
    phone            TEXT NOT NULL,      -- exactly as in the CSV; sent to the provider verbatim
    state            TEXT NOT NULL CHECK (state IN ('pending', 'completed', 'exhausted', 'dnd_blocked')),
    priority         INTEGER,            -- 0 callback, 1 retry, 2 fresh; NULL once final
    next_eligible_at REAL,               -- earliest dial time, already snapped into the window; NULL once final
    callback_at      REAL,               -- callback time the customer asked for, if any
    attempts         INTEGER NOT NULL DEFAULT 0,  -- calls the provider accepted (derived from calls)
    last_status      TEXT,
    last_disposition TEXT,
    updated_at       REAL NOT NULL,
    PRIMARY KEY (campaign_id, lead_id)
);
CREATE INDEX IF NOT EXISTS leads_pick  ON leads (tenant_id, state, priority, next_eligible_at);
CREATE INDEX IF NOT EXISTS leads_phone ON leads (phone);

-- One row per attempt slot of a lead. request_id is deterministic ('campaign:lead:attempt_no')
-- and is written here BEFORE the POST, so after a crash we can always ask the provider about it.
CREATE TABLE IF NOT EXISTS calls (
    request_id     TEXT PRIMARY KEY,
    campaign_id    TEXT NOT NULL,
    lead_id        TEXT NOT NULL,
    tenant_id      TEXT NOT NULL,
    phone          TEXT NOT NULL,
    attempt_no     INTEGER NOT NULL,
    state          TEXT NOT NULL CHECK (state IN ('dialing', 'unknown', 'active', 'ended', 'not_placed')),
    accepted       INTEGER NOT NULL DEFAULT 0,   -- 1 once we have proof the provider accepted it
    call_sid       TEXT UNIQUE,
    status         TEXT,
    disposition    TEXT,
    callback_at    REAL,
    started_at     REAL,
    ended_at       REAL,
    duration_sec   INTEGER,
    reserved_at    REAL NOT NULL,                -- when we (last) decided to dial this slot
    last_polled_at REAL,
    post_count     INTEGER NOT NULL DEFAULT 0,   -- how many times this slot was reserved for a POST
    FOREIGN KEY (campaign_id, lead_id) REFERENCES leads (campaign_id, lead_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS one_inflight_per_phone ON calls (phone)
    WHERE state IN {INFLIGHT_SQL};
CREATE UNIQUE INDEX IF NOT EXISTS one_inflight_per_lead ON calls (campaign_id, lead_id)
    WHERE state IN {INFLIGHT_SQL};
CREATE INDEX IF NOT EXISTS calls_tenant_state ON calls (tenant_id, state);
CREATE INDEX IF NOT EXISTS calls_lead ON calls (campaign_id, lead_id);

-- Phones that opted out (R8). Global across campaigns and tenants.
CREATE TABLE IF NOT EXISTS dnd_phones (
    phone       TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL,
    lead_id     TEXT NOT NULL,
    at          REAL NOT NULL
);

-- Append-only audit log of every webhook delivery we received (including bad signatures).
CREATE TABLE IF NOT EXISTS webhook_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at  REAL NOT NULL,
    delivery_id  TEXT,
    request_id   TEXT,
    call_sid     TEXT,
    signature_ok INTEGER NOT NULL,
    outcome      TEXT NOT NULL,
    raw_body     TEXT NOT NULL
);
"""


class SchemaMismatchError(Exception):
    pass


def connect(path: str | Path) -> sqlite3.Connection:
    """Open (creating if needed) the dispatcher database and make sure the schema exists."""
    # isolation_level=None: we issue BEGIN/COMMIT ourselves (see transaction()).
    # check_same_thread=False: the Store serialises access with a lock; the test client for the
    # webhook app calls in from another thread.
    conn = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = FULL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA)
    row = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    if row is None:
        conn.execute("INSERT INTO meta (key, value) VALUES ('schema_version', ?)", (SCHEMA_VERSION,))
    elif row["value"] != SCHEMA_VERSION:
        conn.close()
        raise SchemaMismatchError(f"database schema v{row['value']}, code expects v{SCHEMA_VERSION}")
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """BEGIN IMMEDIATE ... COMMIT, rolling back on any exception.

    IMMEDIATE takes the write lock up front, so a read-check-write sequence inside the block
    (e.g. "count in-flight calls, then reserve one") can't interleave with another writer,
    even another process.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")
