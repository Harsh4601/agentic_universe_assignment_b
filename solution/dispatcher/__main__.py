"""Run the dispatcher:  python -m dispatcher [options]   (from the solution/ directory)"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from .app import AppSettings, run

REPO_ROOT = Path(__file__).resolve().parents[2]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m dispatcher", description="Durable campaign dispatcher")
    ap.add_argument("--fixtures", type=Path,
                    default=REPO_ROOT / "assignment-b-workflow" / "fixtures" / "campaigns.json")
    ap.add_argument("--db", type=Path, default=REPO_ROOT / "data" / "dispatcher.db",
                    help="SQLite state file (kept across restarts; that's what makes resume work)")
    ap.add_argument("--out", type=Path, default=REPO_ROOT / "final_state.csv")
    ap.add_argument("--provider", default="http://127.0.0.1:8099", help="provider base URL")
    ap.add_argument("--host", default="127.0.0.1", help="webhook receiver bind host")
    ap.add_argument("--port", type=int, default=8200, help="webhook receiver port")
    ap.add_argument("--callback-url", default=None, help="URL the provider posts results to "
                                                         "(default http://HOST:PORT/webhooks/telephony)")
    ap.add_argument("--tick", type=float, default=0.02, help="real seconds between scheduling passes")
    ap.add_argument("--log-level", default="INFO")
    a = ap.parse_args(argv)

    logging.basicConfig(level=a.log_level.upper(), stream=sys.stdout,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per HTTP request is too noisy
    settings = AppSettings(fixtures=a.fixtures, db_path=a.db, out_path=a.out, provider_url=a.provider,
                           host=a.host, port=a.port, callback_url=a.callback_url, tick_seconds=a.tick)
    try:
        return asyncio.run(run(settings))
    except KeyboardInterrupt:
        logging.getLogger("dispatcher").warning("interrupted; state is saved, run again to resume")
        return 130


if __name__ == "__main__":
    sys.exit(main())
