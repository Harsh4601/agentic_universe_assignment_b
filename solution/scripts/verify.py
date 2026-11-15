"""Check a run: print the provider's /admin/report and diff final_state.csv against /admin/leads.

    python solution/scripts/verify.py [--provider http://127.0.0.1:8099] [--csv final_state.csv]

Exit code 0 only if there are 0 violations and every row matches.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parents[2]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--provider", default="http://127.0.0.1:8099")
    ap.add_argument("--csv", type=Path, default=REPO_ROOT / "final_state.csv")
    a = ap.parse_args()

    report = httpx.get(f"{a.provider}/admin/report", timeout=10).json()
    expected = httpx.get(f"{a.provider}/admin/leads", timeout=10).json()
    summary = {k: report[k] for k in ("sim_now", "calls_total", "calls_active", "calls_by_status",
                                      "violation_count", "violations_by_type", "max_concurrency_seen",
                                      "callback_lateness_minutes", "webhooks")}
    print(json.dumps(summary, indent=2))
    for v in report.get("violations", [])[:20]:
        print("  VIOLATION", v)

    if not a.csv.exists():
        print(f"\n{a.csv} does not exist")
        return 1
    with open(a.csv, newline="") as f:
        ours = {(r["campaign_id"], r["lead_id"]): r for r in csv.DictReader(f)}

    diffs = []
    for e in expected:
        key = (e["campaign_id"], e["lead_id"])
        mine = ours.pop(key, None)
        if mine is None:
            diffs.append(f"{key}: missing from CSV (provider: {e['expected_state']})")
            continue
        want = {"final_state": e["expected_state"], "attempts": str(e["attempts"]),
                "last_status": e["last_status"] or "", "last_disposition": e["last_disposition"] or ""}
        got = {k: mine[k] for k in want}
        if got != want:
            diffs.append(f"{key}: ours {got} != provider {want}")
    diffs.extend(f"{key}: in CSV but unknown to provider" for key in ours)

    print(f"\nCSV vs /admin/leads: {len(expected) - len([d for d in diffs if 'unknown' not in d])}"
          f"/{len(expected)} rows match")
    for d in diffs[:50]:
        print("  DIFF", d)
    ok = report["violation_count"] == 0 and not diffs
    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
