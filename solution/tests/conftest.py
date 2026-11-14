from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from dispatcher.config import load_config

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = REPO_ROOT / "assignment-b-workflow" / "fixtures" / "campaigns.json"
IST = timezone(timedelta(hours=5, minutes=30), "IST")

# The simulation starts Thu 12 Nov 2026. Handy day constants for readable tests.
THU, FRI, SAT, SUN, MON = 12, 13, 14, 15, 16


def ist(day: int, hour: int, minute: int = 0, second: int = 0) -> datetime:
    """An instant in November 2026, IST."""
    return datetime(2026, 11, day, hour, minute, second, tzinfo=IST)


@pytest.fixture(scope="session")
def cfg():
    return load_config(FIXTURES)


@pytest.fixture(scope="session")
def renewals(cfg):
    return cfg.campaigns["c_acme_renewals"].window  # 10:00-19:00 Mon-Sat


@pytest.fixture(scope="session")
def feedback(cfg):
    return cfg.campaigns["c_acme_feedback"].window  # 11:00-20:30 every day


@pytest.fixture(scope="session")
def zen(cfg):
    return cfg.campaigns["c_zen_winback"].window  # 09:30-18:00 Mon-Fri
