"""Concurrent, idempotent historical event uploads to partitioned raw S3."""

import json
import logging
import random
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import UTC, datetime, time, timedelta

import boto3
from botocore.exceptions import ClientError

from mock_data_service.providers.meter_readings import generate_reading
from mock_data_service.providers.pricing import generate_prices
from mock_data_service.settings import Settings

LOGGER = logging.getLogger(__name__)


def daily_ranges(start: datetime, end: datetime) -> Iterable[tuple[datetime, datetime]]:
    cursor = datetime.combine(start.date(), time.min, tzinfo=UTC)
    while cursor < end:
        partition_start = max(cursor, start)
        partition_end = min(cursor + timedelta(days=1), end)
        if partition_start < partition_end:
            yield partition_start, partition_end
        cursor += timedelta(days=1)


class RawBackfillWriter:
    def __init__(self, settings: Settings, workers: int):
        if workers < 1:
            raise ValueError("workers must be positive")
        self.settings = settings
        self.workers = workers
        self.client = boto3.client(
            "s3",
            region_name=settings.aws_region,
            endpoint_url=settings.aws_endpoint_url,
        )

    def meter_readings(
        self,
        start: datetime,
        end: datetime,
        meter_count: int,
        interval_minutes: int,
        seed: int,
    ) -> int:
        interval = timedelta(minutes=interval_minutes)

        def generate(partition_start: datetime, partition_end: datetime):
            timestamp = partition_start
            while timestamp < partition_end:
                for meter_number in range(1, meter_count + 1):
                    yield generate_reading(
                        meter_count,
                        random.Random(f"{seed}:{meter_number}:{timestamp.isoformat()}"),
                        event_time=timestamp,
                        meter_number=meter_number,
                    )
                timestamp += interval

        return self._write("meter-readings", start, end, generate)

    def prices(self, start: datetime, end: datetime) -> int:
        return self._write("electricity-prices", start, end, generate_prices)

    def _write(
        self,
        prefix: str,
        start: datetime,
        end: datetime,
        event_factory: Callable[[datetime, datetime], Iterable[object]],
    ) -> int:
        self._require_raw_bucket()
        partitions = list(daily_ranges(start, end))
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = [
                executor.submit(
                    self._put,
                    prefix,
                    partition_start,
                    event_factory(partition_start, partition_end),
                )
                for partition_start, partition_end in partitions
            ]
            total = 0
            for completed, future in enumerate(as_completed(futures), start=1):
                total += future.result()
                if completed % 30 == 0 or completed == len(futures):
                    LOGGER.info("uploaded %d/%d %s partitions", completed, len(futures), prefix)
        return total

    def _require_raw_bucket(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.settings.raw_bucket)
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code", "unknown")
            raise RuntimeError(
                f"raw bucket {self.settings.raw_bucket!r} is unavailable ({code}); "
                "provision infrastructure before running a backfill"
            ) from error

    def _put(self, prefix: str, partition_start: datetime, events: Iterable[object]) -> int:
        rows = [json.dumps(asdict(event), separators=(",", ":")) for event in events]
        body = ("\n".join(rows) + "\n").encode()
        key = (
            f"{prefix}/year={partition_start:%Y}/month={partition_start:%m}/"
            f"day={partition_start:%d}/backfill.json"
        )
        self.client.put_object(
            Bucket=self.settings.raw_bucket,
            Key=key,
            Body=body,
            ContentType="application/x-ndjson",
            Metadata={"backfill": "true"},
        )
        return len(rows)
