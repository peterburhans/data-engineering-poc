"""Regional electricity price events published through Kinesis."""

import json
import logging
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import boto3
from data_contracts import US_CENSUS_REGIONS, US_STATE_CODES, region_for_state

from mock_data_service.settings import Settings

LOGGER = logging.getLogger(__name__)
PRICE_NAMESPACE = uuid.UUID("075c5f93-5961-40c2-922e-949f27cfce99")
MAX_KINESIS_BATCH_SIZE = 500
MAX_PUBLISH_ATTEMPTS = 5


@dataclass(frozen=True)
class PriceEvent:
    price_id: str
    schema_version: str
    us_region: str
    state_code: str
    currency_code: str
    effective_from: str
    price_per_kwh: float
    source_system: str

    def as_dict(self) -> dict:
        return asdict(self)


def price_for(timestamp: datetime, state_code: str) -> Decimal:
    region = region_for_state(state_code)
    regional = Decimal(str(US_CENSUS_REGIONS.index(region))) * Decimal("0.012")
    state_adjustment = Decimal(str(US_STATE_CODES.index(state_code) % 7)) * Decimal("0.002")
    peak = Decimal("0.09") if 16 <= timestamp.hour < 20 else Decimal(0)
    overnight = Decimal("0.05") if timestamp.hour < 7 else Decimal(0)
    return Decimal("0.18") + regional + state_adjustment + peak - overnight


def generate_prices(start: datetime, end: datetime):
    timestamp = start.replace(minute=0, second=0, microsecond=0, tzinfo=UTC)
    while timestamp < end:
        for state_code in US_STATE_CODES:
            yield PriceEvent(
                price_id=str(uuid.uuid5(PRICE_NAMESPACE, f"{state_code}:{timestamp.isoformat()}")),
                schema_version="1.0",
                us_region=region_for_state(state_code),
                state_code=state_code,
                currency_code="USD",
                effective_from=timestamp.isoformat(),
                price_per_kwh=float(price_for(timestamp, state_code)),
                source_system="mock-data-service",
            )
        timestamp += timedelta(hours=1)


class PricingProvider:
    name = "regional_electricity_prices"

    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = boto3.client(
            "kinesis",
            region_name=settings.aws_region,
            endpoint_url=settings.aws_endpoint_url,
        )

    def publish(self, start: datetime, end: datetime, batches_per_second: float = 0) -> int:
        published = 0
        batch = []
        for event in generate_prices(start, end):
            payload = json.dumps(event.as_dict(), separators=(",", ":")) + "\n"
            batch.append({"Data": payload.encode(), "PartitionKey": event.state_code})
            if len(batch) == MAX_KINESIS_BATCH_SIZE:
                published += self._publish_batch(batch)
                batch = []
                if batches_per_second > 0:
                    time.sleep(1 / batches_per_second)
        if batch:
            published += self._publish_batch(batch)
        return published

    def _publish_batch(self, records: list[dict]) -> int:
        pending = records
        for attempt in range(1, MAX_PUBLISH_ATTEMPTS + 1):
            response = self.client.put_records(
                StreamName=self.settings.pricing_stream_name, Records=pending
            )
            pending = [
                record
                for record, result in zip(pending, response["Records"], strict=True)
                if "ErrorCode" in result
            ]
            if not pending:
                return len(records)
            LOGGER.warning("retrying %d regional price records (attempt %d)", len(pending), attempt)
            time.sleep(0.25 * 2 ** (attempt - 1))
        raise RuntimeError(f"failed to publish {len(pending)} regional price records")

    def bootstrap(self) -> None:
        self.health()

    def run(self) -> None:
        while True:
            now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
            count = self.publish(now, now + timedelta(days=2))
            LOGGER.info("published %d current regional price events", count)
            time.sleep(self.settings.pricing_refresh_seconds)

    def health(self) -> None:
        self.client.describe_stream_summary(StreamName=self.settings.pricing_stream_name)
