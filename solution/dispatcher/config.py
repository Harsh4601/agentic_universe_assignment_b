"""Load and validate ``campaigns.json`` and the lead CSVs.

Bad configuration (unknown tenant, malformed window, ...) raises ConfigError: dialling with a
misread rule is worse than not starting. Bad *lead rows* are skipped and reported, because
one dirty row shouldn't stop a whole campaign.

Phones are kept exactly as written in the CSV. The provider compares raw strings, so
"normalising" a phone would make it reject the call as an unknown lead. Customer names are
PII we don't need, so they are never loaded.
"""
from __future__ import annotations

import csv
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .policy import RetryPolicy
from .windows import Window, intersect

log = logging.getLogger(__name__)

_IST_NAMES = {"Asia/Kolkata", "Asia/Calcutta"}


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class Tenant:
    tenant_id: str
    max_concurrent_calls: int


@dataclass(frozen=True)
class Campaign:
    campaign_id: str
    tenant_id: str
    window: Window          # effective window: campaign window intersected with the regulatory one
    campaign_window: Window  # as configured, kept for reporting
    max_attempts: int
    leads_file: str


@dataclass(frozen=True)
class Lead:
    campaign_id: str
    lead_id: str
    tenant_id: str
    phone: str


@dataclass(frozen=True)
class RejectedRow:
    file: str
    line: int
    reason: str


@dataclass(frozen=True)
class Config:
    sim_start: datetime
    tz: tzinfo
    regulatory_window: Window
    grace_seconds: int
    retry: RetryPolicy
    webhook_secret: str
    tenants: dict[str, Tenant]
    campaigns: dict[str, Campaign]
    leads: tuple[Lead, ...]
    rejected_rows: tuple[RejectedRow, ...]
    fingerprint: str  # sha256 of every input file, used to detect a DB built from other fixtures


def load_config(campaigns_path: str | Path) -> Config:
    path = Path(campaigns_path)
    try:
        raw_bytes = path.read_bytes()
        cfg = json.loads(raw_bytes)
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError(f"cannot read {path}: {exc}") from exc

    digest = hashlib.sha256(raw_bytes)
    try:
        tz = _load_tz(cfg.get("timezone", "Asia/Kolkata"))
        sim_start = datetime.fromisoformat(_require(cfg, "sim_start"))
        reg = _require(cfg, "regulatory_window")
        regulatory = Window.parse(reg["start"], reg["end"], reg.get("days"))
        rp = _require(cfg, "retry_policy")
        retry = RetryPolicy(
            no_answer=_minutes(rp, "no_answer_minutes"),
            busy=_minutes(rp, "busy_minutes"),
            failed=_minutes(rp, "failed_minutes"),
        )
        secret = _require(cfg, "webhook_secret")
        grace = int(cfg.get("window_grace_seconds", 0))
        tenants = _load_tenants(_require(cfg, "tenants"))
        campaigns = _load_campaigns(_require(cfg, "campaigns"), tenants, regulatory)
    except (KeyError, TypeError, ValueError) as exc:
        raise ConfigError(f"invalid {path.name}: {exc}") from exc

    if regulatory.is_empty:
        raise ConfigError("regulatory window is empty")
    if not isinstance(secret, str) or not secret:
        raise ConfigError("webhook_secret must be a non-empty string")
    if sim_start.tzinfo is None:
        raise ConfigError("sim_start must include a timezone offset")

    leads: list[Lead] = []
    rejected: list[RejectedRow] = []
    for camp in campaigns.values():
        lead_path = path.parent / camp.leads_file
        try:
            data = lead_path.read_bytes()
        except OSError as exc:
            raise ConfigError(f"cannot read leads file {lead_path}: {exc}") from exc
        digest.update(data)
        camp_leads, camp_rejected = _parse_leads(camp, data.decode("utf-8-sig"), camp.leads_file)
        leads.extend(camp_leads)
        rejected.extend(camp_rejected)

    for row in rejected:
        log.warning("skipping lead row %s:%d: %s", row.file, row.line, row.reason)

    return Config(
        sim_start=sim_start, tz=tz, regulatory_window=regulatory, grace_seconds=grace, retry=retry,
        webhook_secret=secret, tenants=tenants, campaigns=campaigns,
        leads=tuple(sorted(leads, key=lambda l: (l.campaign_id, l.lead_id))),
        rejected_rows=tuple(rejected), fingerprint=digest.hexdigest(),
    )


def _require(d: dict, key: str):
    if key not in d:
        raise KeyError(f"missing '{key}'")
    return d[key]


def _minutes(d: dict, key: str) -> timedelta:
    value = float(_require(d, key))
    if value < 0:
        raise ValueError(f"{key} must be >= 0")
    return timedelta(minutes=value)


def _positive_int(value, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{what} must be a positive integer, got {value!r}")
    return value


def _load_tz(name: str) -> tzinfo:
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        if name in _IST_NAMES:  # machine without tz data: IST is a fixed offset with no DST
            return timezone(timedelta(hours=5, minutes=30), "IST")
        raise ConfigError(f"unknown timezone {name!r}")


def _load_tenants(items: list[dict]) -> dict[str, Tenant]:
    tenants: dict[str, Tenant] = {}
    for t in items:
        tid = t["tenant_id"]
        if tid in tenants:
            raise ValueError(f"duplicate tenant_id {tid!r}")
        tenants[tid] = Tenant(tid, _positive_int(t["max_concurrent_calls"], f"{tid}.max_concurrent_calls"))
    return tenants


def _load_campaigns(items: list[dict], tenants: dict[str, Tenant], regulatory: Window) -> dict[str, Campaign]:
    campaigns: dict[str, Campaign] = {}
    for c in items:
        cid = c["campaign_id"]
        if cid in campaigns:
            raise ValueError(f"duplicate campaign_id {cid!r}")
        if c["tenant_id"] not in tenants:
            raise ValueError(f"campaign {cid!r} references unknown tenant {c['tenant_id']!r}")
        w = c["window"]
        own = Window.parse(w["start"], w["end"], w["days"])
        if own.start >= own.end:
            raise ValueError(f"campaign {cid!r}: window must start before it ends (no midnight crossing)")
        if not own.days:
            raise ValueError(f"campaign {cid!r}: window has no days")
        effective = intersect(own, regulatory)
        if effective.is_empty:
            raise ValueError(f"campaign {cid!r}: window never overlaps the regulatory window")
        campaigns[cid] = Campaign(
            campaign_id=cid, tenant_id=c["tenant_id"], window=effective, campaign_window=own,
            max_attempts=_positive_int(c["max_attempts"], f"{cid}.max_attempts"),
            leads_file=c["leads_file"],
        )
    return campaigns


def _parse_leads(camp: Campaign, text: str, filename: str) -> tuple[list[Lead], list[RejectedRow]]:
    reader = csv.DictReader(text.splitlines())
    missing = {"lead_id", "phone"} - set(reader.fieldnames or [])
    if missing:
        raise ConfigError(f"{filename}: missing column(s) {sorted(missing)}")

    by_id: dict[str, tuple[int, Lead]] = {}
    rejected: list[RejectedRow] = []
    for line_no, row in enumerate(reader, start=2):  # line 1 is the header
        lead_id = (row.get("lead_id") or "").strip()
        phone = row.get("phone") or ""
        if not lead_id:
            rejected.append(RejectedRow(filename, line_no, "empty lead_id"))
            continue
        if not phone.strip():
            rejected.append(RejectedRow(filename, line_no, f"lead {lead_id}: empty phone"))
            continue
        if lead_id in by_id:
            # The provider keeps the *last* row for a repeated lead_id, so we do too; otherwise
            # it would reject our calls as an unknown lead whenever the phones differ.
            first_line, _ = by_id[lead_id]
            rejected.append(RejectedRow(filename, first_line,
                                        f"lead {lead_id}: superseded by duplicate on line {line_no}"))
        by_id[lead_id] = (line_no, Lead(camp.campaign_id, lead_id, camp.tenant_id, phone))
    return [lead for _, lead in by_id.values()], rejected
