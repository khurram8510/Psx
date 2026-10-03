from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from kmi30.config import Settings
from kmi30.market_calendar import MarketCalendar

PKT = ZoneInfo("Asia/Karachi")


def pkt(y, mo, d, h, mi, s=0) -> datetime:
    return datetime(y, mo, d, h, mi, s, tzinfo=PKT)


@pytest.fixture
def settings() -> Settings:
    return Settings()


@pytest.fixture
def calendar(settings: Settings) -> MarketCalendar:
    return MarketCalendar(settings.market)
