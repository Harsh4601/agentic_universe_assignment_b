"""Calling-window maths.

Pure functions only: no I/O and no clock. Every function takes the instant it reasons about
as an argument, which is what makes these rules testable with a fake clock.

A window is a daily half-open interval ``[start, end)`` in a local timezone, on a set of
weekdays. Windows never cross midnight (``config`` rejects that), so each allowed day has
exactly one interval.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, tzinfo

DAY_NAMES = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")  # index == datetime.weekday()
ALL_DAYS = frozenset(range(7))

# A weekly window repeats every 7 days, so 8 days of look-ahead always finds the next opening
# (today might be just past today's opening).
_LOOKAHEAD_DAYS = 8


@dataclass(frozen=True)
class Window:
    start: time
    end: time
    days: frozenset[int] = ALL_DAYS  # weekday numbers, Monday == 0

    @property
    def is_empty(self) -> bool:
        """True if no instant is ever inside this window (e.g. an empty intersection)."""
        return self.start >= self.end or not self.days

    @classmethod
    def parse(cls, start: str, end: str, days: list[str] | None = None) -> Window:
        """Build from config strings: ``"10:00"``, ``"19:00"``, ``["mon", ...]`` (None = every day)."""
        if days is None:
            day_set = ALL_DAYS
        else:
            unknown = [d for d in days if d not in DAY_NAMES]
            if unknown:
                raise ValueError(f"unknown day name(s): {unknown}")
            day_set = frozenset(DAY_NAMES.index(d) for d in days)
        return cls(parse_hhmm(start), parse_hhmm(end), day_set)


def parse_hhmm(value: str) -> time:
    """Parse ``"HH:MM"`` (24h). Raises ValueError on anything else."""
    try:
        hh, mm = value.split(":")
        return time(int(hh), int(mm))
    except (ValueError, AttributeError) as exc:
        raise ValueError(f"invalid HH:MM time: {value!r}") from exc


def intersect(a: Window, b: Window) -> Window:
    """The window during which both ``a`` and ``b`` are open (may be empty)."""
    return Window(max(a.start, b.start), min(a.end, b.end), a.days & b.days)


def _bounds(day: date, w: Window, tz: tzinfo) -> tuple[datetime, datetime]:
    return (datetime.combine(day, w.start, tzinfo=tz), datetime.combine(day, w.end, tzinfo=tz))


def is_open(t: datetime, w: Window, tz: tzinfo) -> bool:
    """True if instant ``t`` lies inside ``w`` (start inclusive, end exclusive)."""
    if w.is_empty:
        return False
    local = t.astimezone(tz)
    if local.weekday() not in w.days:
        return False
    start, end = _bounds(local.date(), w, tz)
    return start <= local < end


def window_end(t: datetime, w: Window, tz: tzinfo) -> datetime | None:
    """Closing instant of the window interval containing ``t``, or None if ``t`` is outside."""
    if not is_open(t, w, tz):
        return None
    local = t.astimezone(tz)
    return _bounds(local.date(), w, tz)[1]


def next_open(t: datetime, w: Window, tz: tzinfo, margin: timedelta = timedelta(0)) -> datetime | None:
    """Earliest instant ``>= t`` at which a call may be *started*.

    "May be started" means inside ``w`` and strictly more than ``margin`` before that day's
    close. ``margin=0`` gives plain window semantics. Returns ``t`` itself (as a local
    datetime) when ``t`` already qualifies, and None if the window never opens (empty, or
    ``margin`` is at least as long as the window).
    """
    if w.is_empty:
        return None
    local = t.astimezone(tz)
    for offset in range(_LOOKAHEAD_DAYS):
        day = local.date() + timedelta(days=offset)
        if day.weekday() not in w.days:
            continue
        start, end = _bounds(day, w, tz)
        candidate = max(start, local)
        if candidate < end - margin:
            return candidate
    return None


def is_startable(t: datetime, w: Window, tz: tzinfo, margin: timedelta = timedelta(0)) -> bool:
    """True if a call may be started at ``t``: inside ``w`` and more than ``margin`` before close."""
    end = window_end(t, w, tz)
    return end is not None and t < end - margin
