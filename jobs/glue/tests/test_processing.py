"""Failure-path tests for incremental and backfill processing modes."""

from types import SimpleNamespace

import pytest
from glue_lib import BackfillGrain, BackfillWindow, ProcessingMode


def runtime(mode: str, bookmark: str | None) -> SimpleNamespace:
    return SimpleNamespace(
        args={
            "processing_mode": mode,
            "backfill_start": "2026-01-15T00:00:00Z",
            "backfill_end": "2026-03-02T00:00:00Z",
            "backfill_grain": "month",
        },
        optional_argument=lambda _name: bookmark,
    )


def test_incremental_mode_accepts_enabled_bookmarks() -> None:
    assert (
        ProcessingMode.from_runtime(runtime("incremental", "job-bookmark-enable"))
        is ProcessingMode.INCREMENTAL
    )


def test_backfill_requires_disabled_bookmarks() -> None:
    with pytest.raises(ValueError, match="job-bookmark-disable"):
        ProcessingMode.from_runtime(runtime("backfill", "job-bookmark-enable"))


def test_backfill_accepts_disabled_bookmarks() -> None:
    assert (
        ProcessingMode.from_runtime(runtime("backfill", "job-bookmark-disable"))
        is ProcessingMode.BACKFILL
    )


def test_unknown_processing_mode_is_rejected() -> None:
    with pytest.raises(ValueError, match="must be one of"):
        ProcessingMode.from_runtime(runtime("repartition-everything", None))


def test_monthly_backfill_uses_partition_roots_and_exact_window() -> None:
    configured = runtime("backfill", "job-bookmark-disable")
    window = BackfillWindow.from_runtime(configured, ProcessingMode.BACKFILL)

    assert window is not None
    assert window.grain is BackfillGrain.MONTH
    assert window.source_paths("s3://raw/events/") == [
        "s3://raw/events/year=2026/month=01/",
        "s3://raw/events/year=2026/month=02/",
        "s3://raw/events/year=2026/month=03/",
    ]
    assert [(batch.start.day, batch.end.day) for batch in window.batches()] == [
        (15, 1),
        (1, 1),
        (1, 2),
    ]
    assert [(batch.start, batch.end) for batch in window.batch_windows()] == [
        (batch.start, batch.end) for batch in window.batches()
    ]


def test_backfill_rejects_invalid_window() -> None:
    configured = runtime("backfill", "job-bookmark-disable")
    configured.args["backfill_end"] = configured.args["backfill_start"]

    with pytest.raises(ValueError, match="must be before"):
        BackfillWindow.from_runtime(configured, ProcessingMode.BACKFILL)


def test_weekly_backfill_uses_monday_boundaries_and_daily_partition_paths() -> None:
    configured = runtime("backfill", "job-bookmark-disable")
    configured.args.update(
        backfill_start="2026-01-15T00:00:00Z",
        backfill_end="2026-01-27T00:00:00Z",
        backfill_grain="week",
    )
    window = BackfillWindow.from_runtime(configured, ProcessingMode.BACKFILL)

    assert window is not None
    assert [(batch.start.day, batch.end.day) for batch in window.batches()] == [
        (15, 19),
        (19, 26),
        (26, 27),
    ]
    assert window.batch_windows()[1].source_paths("s3://raw/events/") == [
        f"s3://raw/events/year=2026/month=01/day={day:02d}/" for day in range(19, 26)
    ]


def test_backfill_requires_day_aligned_boundaries() -> None:
    configured = runtime("backfill", "job-bookmark-disable")
    configured.args["backfill_start"] = "2026-01-15T06:00:00Z"

    with pytest.raises(ValueError, match="UTC midnight"):
        BackfillWindow.from_runtime(configured, ProcessingMode.BACKFILL)
