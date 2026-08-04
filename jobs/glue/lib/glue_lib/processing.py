"""Shared incremental and windowed backfill semantics for Glue stages."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from pyspark.sql import DataFrame

from glue_lib.runtime import GlueRuntime

BACKFILL_ARGUMENTS = ("backfill_start", "backfill_end", "backfill_grain")


class ProcessingMode(StrEnum):
    """Supported pipeline execution modes."""

    INCREMENTAL = "incremental"
    BACKFILL = "backfill"

    @classmethod
    def from_runtime(cls, runtime: GlueRuntime) -> "ProcessingMode":
        supplied = runtime.args["processing_mode"]
        try:
            mode = cls(supplied)
        except ValueError as error:
            supported = ", ".join(item.value for item in cls)
            raise ValueError(
                f"processing_mode must be one of [{supported}], not {supplied!r}"
            ) from error

        if mode is cls.BACKFILL:
            bookmark_option = runtime.optional_argument("job-bookmark-option")
            if bookmark_option != "job-bookmark-disable":
                raise ValueError(
                    "Backfill mode requires --job-bookmark-option "
                    "job-bookmark-disable so every source object is visible"
                )
        return mode


class BackfillGrain(StrEnum):
    """Physical batches supported by the partitioned lake."""

    DAY = "day"
    WEEK = "week"
    MONTH = "month"


@dataclass(frozen=True)
class BackfillBatch:
    """A half-open batch within a requested backfill window."""

    start: datetime
    end: datetime


@dataclass(frozen=True)
class BackfillWindow:
    """Validated half-open backfill window split into calendar batches."""

    start: datetime
    end: datetime
    grain: BackfillGrain

    @staticmethod
    def _timestamp(value: str, argument: str) -> datetime:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError(f"{argument} must be an ISO-8601 timestamp") from error
        if parsed.tzinfo is None:
            raise ValueError(f"{argument} must include a timezone")
        return parsed.astimezone(UTC)

    @classmethod
    def from_runtime(
        cls, runtime: GlueRuntime, mode: ProcessingMode
    ) -> "BackfillWindow | None":
        if mode is ProcessingMode.INCREMENTAL:
            return None
        start = cls._timestamp(runtime.args["backfill_start"], "backfill_start")
        end = cls._timestamp(runtime.args["backfill_end"], "backfill_end")
        if start >= end:
            raise ValueError("backfill_start must be before backfill_end")
        if any((start.hour, start.minute, start.second, start.microsecond)) or any(
            (end.hour, end.minute, end.second, end.microsecond)
        ):
            raise ValueError(
                "backfill_start and backfill_end must be UTC midnight boundaries"
            )
        try:
            grain = BackfillGrain(runtime.args["backfill_grain"])
        except ValueError as error:
            supported = ", ".join(item.value for item in BackfillGrain)
            raise ValueError(f"backfill_grain must be one of [{supported}]") from error
        return cls(start=start, end=end, grain=grain)

    def batches(self) -> tuple[BackfillBatch, ...]:
        batches: list[BackfillBatch] = []
        cursor = self.start
        while cursor < self.end:
            if self.grain is BackfillGrain.DAY:
                boundary = datetime(
                    cursor.year, cursor.month, cursor.day, tzinfo=UTC
                ) + timedelta(days=1)
            elif self.grain is BackfillGrain.WEEK:
                boundary = datetime(
                    cursor.year, cursor.month, cursor.day, tzinfo=UTC
                ) + timedelta(days=7 - cursor.weekday())
            else:
                year, month = cursor.year, cursor.month + 1
                if month == 13:
                    year, month = year + 1, 1
                boundary = datetime(year, month, 1, tzinfo=UTC)
            batch_end = min(boundary, self.end)
            batches.append(BackfillBatch(cursor, batch_end))
            cursor = batch_end
        return tuple(batches)

    def batch_windows(self) -> tuple["BackfillWindow", ...]:
        """Return independently processable windows at the configured grain."""

        return tuple(
            BackfillWindow(start=batch.start, end=batch.end, grain=self.grain)
            for batch in self.batches()
        )

    def source_paths(self, base_uri: str) -> list[str]:
        """Return unique partition roots for Spark to process as file batches."""

        paths: list[str] = []
        for batch in self.batches():
            if self.grain is BackfillGrain.MONTH:
                suffixes = [f"year={batch.start:%Y}/month={batch.start:%m}/"]
            elif self.grain is BackfillGrain.WEEK:
                suffixes = []
                cursor = batch.start
                while cursor < batch.end:
                    suffixes.append(
                        f"year={cursor:%Y}/month={cursor:%m}/day={cursor:%d}/"
                    )
                    cursor += timedelta(days=1)
            else:
                suffixes = [
                    f"year={batch.start:%Y}/month={batch.start:%m}/day={batch.start:%d}/"
                ]
            for suffix in suffixes:
                path = f"{base_uri.rstrip('/')}/{suffix}"
                if path not in paths:
                    paths.append(path)
        return paths

    def filter(self, frame: DataFrame, timestamp_column: str) -> DataFrame:
        """Enforce exact boundaries when the requested window cuts a partition."""

        from pyspark.sql import functions as F

        timestamp = F.col(timestamp_column).cast("timestamp")
        return frame.filter(
            (timestamp >= F.lit(self.start)) & (timestamp < F.lit(self.end))
        )


def write_partitioned_parquet(
    frame: DataFrame,
    destination: str,
    mode: ProcessingMode,
    partition_columns: tuple[str, ...],
) -> None:
    """Write Parquet without reshuffling an entire backfill window."""

    if mode is ProcessingMode.BACKFILL:
        if frame.isEmpty():
            return
        frame.sparkSession.conf.set(
            "spark.sql.sources.partitionOverwriteMode", "dynamic"
        )
        write_mode = "overwrite"
    else:
        write_mode = "append"

    (
        frame.write.mode(write_mode)
        .option("compression", "snappy")
        .partitionBy(*partition_columns)
        .parquet(destination)
    )
