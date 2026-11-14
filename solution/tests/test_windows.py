from datetime import datetime, time, timedelta, timezone

import pytest

from conftest import FRI, IST, MON, SAT, SUN, THU, ist
from dispatcher.windows import Window, intersect, is_open, is_startable, next_open, parse_hhmm, window_end

REGULATORY = Window.parse("09:00", "21:00")


# --------------------------------------------------------------------------- parsing
def test_parse_window():
    w = Window.parse("10:00", "19:00", ["mon", "sat"])
    assert (w.start, w.end, w.days) == (time(10), time(19), frozenset({0, 5}))


def test_parse_without_days_means_every_day():
    assert Window.parse("09:00", "21:00").days == frozenset(range(7))


@pytest.mark.parametrize("bad", ["25:00", "9", "ab:cd", "", None])
def test_parse_hhmm_rejects_garbage(bad):
    with pytest.raises(ValueError):
        parse_hhmm(bad)


def test_parse_rejects_unknown_day():
    with pytest.raises(ValueError):
        Window.parse("10:00", "19:00", ["mon", "funday"])


# --------------------------------------------------------------------------- intersection
def test_campaign_windows_are_intersected_with_regulatory(cfg):
    # All fixture windows sit inside 09:00-21:00, so the effective window is the campaign's own.
    for camp in cfg.campaigns.values():
        assert camp.window == camp.campaign_window


def test_intersection_clips_to_regulatory_hours():
    wide = Window.parse("08:00", "22:00", ["mon"])
    eff = intersect(wide, REGULATORY)
    assert (eff.start, eff.end, eff.days) == (time(9), time(21), frozenset({0}))


def test_disjoint_windows_intersect_to_empty():
    eff = intersect(Window.parse("06:00", "08:30"), REGULATORY)
    assert eff.is_empty
    assert not is_open(ist(THU, 7), eff, IST)
    assert next_open(ist(THU, 7), eff, IST) is None


# --------------------------------------------------------------------------- is_open boundaries
@pytest.mark.parametrize("t, expected", [
    (ist(THU, 17, 30), True),         # sim start
    (ist(THU, 10, 0), True),          # start is inclusive
    (ist(THU, 9, 59, 59), False),
    (ist(THU, 18, 59, 59), True),
    (ist(THU, 19, 0), False),         # end is exclusive
    (ist(SAT, 12, 0), True),
    (ist(SUN, 12, 0), False),         # renewals doesn't run on Sundays
])
def test_renewals_is_open(renewals, t, expected):
    assert is_open(t, renewals, IST) is expected


def test_timezone_is_applied_before_checking_day_and_hour(renewals, zen):
    # 13:00 UTC Thu == 18:30 IST Thu: open for renewals, closed for zen (closes 18:00).
    t = datetime(2026, 11, 12, 13, 0, tzinfo=timezone.utc)
    assert is_open(t, renewals, IST)
    assert not is_open(t, zen, IST)
    # 19:00 UTC Thu == 00:30 IST *Fri*: the next opening is Fri 10:00 IST, not Thu.
    late = datetime(2026, 11, 12, 19, 0, tzinfo=timezone.utc)
    assert next_open(late, renewals, IST) == ist(FRI, 10, 0)


# --------------------------------------------------------------------------- next_open
@pytest.mark.parametrize("campaign, t, expected", [
    ("zen", ist(THU, 17, 30), ist(THU, 17, 30)),       # already open: returns t itself
    ("zen", ist(THU, 18, 0), ist(FRI, 9, 30)),         # Thu window closed: next morning
    ("zen", ist(FRI, 18, 5), ist(MON, 9, 30)),         # skips Sat and Sun
    ("zen", ist(SAT, 10, 0), ist(MON, 9, 30)),
    ("renewals", ist(SAT, 19, 10), ist(MON, 10, 0)),   # skips Sunday
    ("renewals", ist(SUN, 12, 0), ist(MON, 10, 0)),
    ("renewals", ist(FRI, 7, 0), ist(FRI, 10, 0)),     # before today's start: today's start
    ("feedback", ist(THU, 20, 45), ist(FRI, 11, 0)),   # every day, so simply the next day
    ("feedback", ist(SUN, 10, 0), ist(SUN, 11, 0)),
    ("feedback", ist(THU, 20, 29, 59), ist(THU, 20, 29, 59)),
])
def test_next_open(request, campaign, t, expected):
    w = request.getfixturevalue(campaign)
    assert next_open(t, w, IST) == expected


@pytest.mark.parametrize("campaign", ["renewals", "feedback", "zen"])
def test_next_open_is_the_earliest_open_instant(request, campaign):
    """Property check over a week sampled every 7 minutes, for every fixture window."""
    w = request.getfixturevalue(campaign)
    t = ist(THU, 0, 3)
    while t < ist(THU + 7, 0, 0):
        n = next_open(t, w, IST)
        assert n is not None and n >= t
        assert is_open(n, w, IST)
        if is_open(t, w, IST):
            assert n == t
        else:
            # If we had to wait, n is a window opening: one second earlier is still closed.
            assert n.timetz().replace(tzinfo=None) == w.start
            assert not is_open(n - timedelta(seconds=1), w, IST)
        t += timedelta(minutes=7)


# --------------------------------------------------------------------------- end margin
def test_window_end(renewals):
    assert window_end(ist(THU, 17, 30), renewals, IST) == ist(THU, 19, 0)
    assert window_end(ist(THU, 19, 30), renewals, IST) is None


def test_is_startable_honours_margin_before_close(renewals):
    margin = timedelta(seconds=60)
    assert is_startable(ist(THU, 18, 58, 59), renewals, IST, margin)
    assert not is_startable(ist(THU, 18, 59, 0), renewals, IST, margin)   # exactly at end - margin
    assert not is_startable(ist(THU, 18, 59, 30), renewals, IST, margin)
    assert not is_startable(ist(THU, 19, 0), renewals, IST, margin)
    assert is_startable(ist(THU, 18, 59, 30), renewals, IST)              # no margin: still open


def test_next_open_with_margin_moves_to_next_day(renewals):
    margin = timedelta(seconds=60)
    assert next_open(ist(THU, 18, 59, 30), renewals, IST, margin) == ist(FRI, 10, 0)
    assert next_open(ist(THU, 18, 58), renewals, IST, margin) == ist(THU, 18, 58)


def test_margin_longer_than_window_never_opens():
    tiny = Window.parse("10:00", "10:01")
    assert next_open(ist(THU, 9, 0), tiny, IST, timedelta(minutes=5)) is None
