"""Simulated time. All business rules use the provider's clock, never the machine's.

The provider publishes ``sim_now = sim_epoch + (time.time() - real_epoch) * speed``. We run on
the same machine, so we compute the same formula locally instead of polling /v1/clock.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, tzinfo
from typing import Mapping


class SimClock:
    def __init__(self, sim_epoch: datetime, real_epoch: float, speed: float, tz: tzinfo):
        if speed <= 0:
            raise ValueError("clock speed must be positive")
        self.sim_epoch = sim_epoch
        self.real_epoch = real_epoch
        self.speed = speed
        self.tz = tz

    @classmethod
    def from_provider(cls, payload: Mapping, tz: tzinfo) -> SimClock:
        """Build from the body of ``GET /v1/clock``."""
        return cls(datetime.fromisoformat(payload["sim_epoch"]), float(payload["real_epoch"]),
                   float(payload["speed"]), tz)

    @property
    def run_id(self) -> str:
        """Identifies one provider process: a restarted mock gets a new real_epoch."""
        return f"{self.sim_epoch.isoformat()}|{self.real_epoch!r}"

    def now(self) -> datetime:
        return (self.sim_epoch + timedelta(seconds=(time.time() - self.real_epoch) * self.speed)).astimezone(self.tz)

    def real_seconds(self, sim: timedelta) -> float:
        """How long ``sim`` of simulated time takes in real seconds."""
        return sim.total_seconds() / self.speed


class FakeClock:
    """Manually driven clock for tests."""

    def __init__(self, start: datetime, speed: float = 600.0):
        self._now = start
        self.speed = speed
        self.tz = start.tzinfo
        self.run_id = "fake-run"

    def now(self) -> datetime:
        return self._now

    def set(self, t: datetime) -> None:
        self._now = t

    def advance(self, **kwargs) -> datetime:
        self._now += timedelta(**kwargs)
        return self._now

    def real_seconds(self, sim: timedelta) -> float:
        return sim.total_seconds() / self.speed
