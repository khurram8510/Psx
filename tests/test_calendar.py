from datetime import date

from kmi30.config import MarketConfig
from kmi30.market_calendar import MarketCalendar

from .conftest import pkt


def test_weekday_sessions(calendar):
    assert calendar.is_open(pkt(2026, 9, 14, 10, 0))
    assert not calendar.is_open(pkt(2026, 9, 14, 9, 29))
    assert not calendar.is_open(pkt(2026, 9, 14, 15, 30))
    assert calendar.is_open(pkt(2026, 9, 14, 15, 33), with_grace=True)
    assert not calendar.is_open(pkt(2026, 9, 13, 11, 0))  # Sunday


def test_friday_break(calendar):
    assert calendar.is_open(pkt(2026, 9, 18, 11, 0))
    assert not calendar.is_open(pkt(2026, 9, 18, 13, 0))
    assert calendar.is_open(pkt(2026, 9, 18, 15, 0))


def test_next_open_skips_weekend_and_holiday():
    cal = MarketCalendar(MarketConfig(holidays=["2026-09-21"]))
    assert cal.next_open(pkt(2026, 9, 18, 17, 0)) == pkt(2026, 9, 22, 9, 30)
    assert not cal.is_trading_day(date(2026, 9, 21))


def test_warmup(calendar):
    assert calendar.in_warmup(pkt(2026, 9, 14, 9, 40), 15)
    assert not calendar.in_warmup(pkt(2026, 9, 14, 9, 46), 15)
    assert calendar.in_warmup(pkt(2026, 9, 18, 14, 35), 15)  # Friday afternoon re-open


def test_session_minutes(calendar):
    assert calendar.session_minutes(date(2026, 9, 14)) == 360
    assert calendar.session_minutes(date(2026, 9, 18)) == 285
