"""Writing final_state.csv (the output contract)."""
from __future__ import annotations

import csv
import os
from pathlib import Path

FIELDS = ["campaign_id", "lead_id", "final_state", "attempts", "last_status", "last_disposition"]


def write_final_state(rows: list[dict], path: str | Path) -> Path:
    """Write rows sorted by campaign_id, lead_id. Atomic: readers never see a half-written file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        for row in sorted(rows, key=lambda r: (r["campaign_id"], r["lead_id"])):
            writer.writerow({k: row[k] for k in FIELDS})
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    return path
