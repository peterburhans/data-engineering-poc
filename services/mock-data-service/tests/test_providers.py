import random
from datetime import UTC, datetime
from decimal import Decimal

from data_contracts import US_STATE_CODES

from mock_data_service.main import backfill_window
from mock_data_service.providers.meter_readings import generate_reading
from mock_data_service.providers.pricing import generate_prices, price_for
from mock_data_service.raw_backfill import daily_ranges


def test_meter_reading_is_valid() -> None:
    reading = generate_reading(10, random.Random(42))
    assert reading.meter_id.startswith("meter-")
    assert reading.energy_kwh >= 0
    assert 0.85 <= reading.power_factor <= 1.0


def test_historical_meter_reading_is_deterministic() -> None:
    timestamp = datetime(2026, 3, 1, 12, tzinfo=UTC)
    first = generate_reading(10, random.Random(42), event_time=timestamp, meter_number=3)
    second = generate_reading(10, random.Random(42), event_time=timestamp, meter_number=3)
    assert first == second


def test_us_region_and_state_assignment() -> None:
    reading = generate_reading(10, event_time=datetime(2026, 1, 1, tzinfo=UTC), meter_number=1)
    assert reading.us_region == "Northeast"
    assert reading.state_code == "CT"


def test_regional_prices_cover_all_states() -> None:
    start = datetime(2026, 8, 3, 17, tzinfo=UTC)
    prices = list(generate_prices(start, start.replace(hour=18)))
    assert len(prices) == len(US_STATE_CODES)
    assert price_for(start, "CT") == Decimal("0.27")


def test_raw_backfill_splits_partial_days() -> None:
    start = datetime(2026, 1, 1, 12, tzinfo=UTC)
    end = datetime(2026, 1, 3, 6, tzinfo=UTC)
    assert list(daily_ranges(start, end)) == [
        (start, datetime(2026, 1, 2, tzinfo=UTC)),
        (datetime(2026, 1, 2, tzinfo=UTC), datetime(2026, 1, 3, tzinfo=UTC)),
        (datetime(2026, 1, 3, tzinfo=UTC), end),
    ]


def test_backfill_window_uses_completed_utc_day() -> None:
    start, end = backfill_window(7, datetime(2026, 8, 4, 17, 42, tzinfo=UTC))

    assert end == datetime(2026, 8, 4, tzinfo=UTC)
    assert start == datetime(2026, 7, 28, tzinfo=UTC)
