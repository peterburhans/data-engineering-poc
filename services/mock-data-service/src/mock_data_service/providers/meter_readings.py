import json
import logging
import random
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from math import cos, pi

import boto3
from data_contracts import US_STATE_CODES, region_for_state

from mock_data_service.settings import Settings

LOGGER = logging.getLogger(__name__)
EVENT_NAMESPACE = uuid.UUID("15c924eb-85fc-46f6-8eb5-35c50dd26ab8")


@dataclass(frozen=True)
class MeterReading:
    event_id: str
    schema_version: str
    meter_id: str
    us_region: str
    state_code: str
    event_time: str
    energy_kwh: float
    voltage_v: float
    current_a: float
    power_factor: float

    def as_dict(self) -> dict[str, str | float]:
        return asdict(self)


def generate_reading(
    meter_count: int,
    rng: random.Random | None = None,
    *,
    event_time: datetime | None = None,
    meter_number: int | None = None,
) -> MeterReading:
    rng = rng or random.Random()
    timestamp = event_time or datetime.now(UTC)
    meter_number = meter_number or rng.randint(1, meter_count)
    meter_id = f"meter-{meter_number:06d}"
    state_code = US_STATE_CODES[(meter_number - 1) % len(US_STATE_CODES)]
    voltage = rng.gauss(230.0, 3.0)
    daily_load = 1.0 + 0.35 * cos((timestamp.hour - 19) * 2 * pi / 24)
    seasonal_load = 1.0 + 0.2 * cos((timestamp.timetuple().tm_yday - 15) * 2 * pi / 365)
    current = max(0.05, rng.lognormvariate(0.7, 0.35) * daily_load * seasonal_load)
    power_factor = rng.uniform(0.85, 1.0)
    event_key = f"{meter_id}:{timestamp.isoformat()}"
    return MeterReading(
        event_id=str(uuid.uuid5(EVENT_NAMESPACE, event_key)) if event_time else str(uuid.uuid4()),
        schema_version="1.0",
        meter_id=meter_id,
        us_region=region_for_state(state_code),
        state_code=state_code,
        event_time=timestamp.isoformat(),
        energy_kwh=round(voltage * current * power_factor / 1000 / 12, 5),
        voltage_v=round(voltage, 2),
        current_a=round(current, 3),
        power_factor=round(power_factor, 3),
    )


class MeterReadingProvider:
    name = "meter_readings"

    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = boto3.client(
            "kinesis",
            region_name=settings.aws_region,
            endpoint_url=settings.aws_endpoint_url,
        )

    def bootstrap(self) -> None:
        if self.settings.events_per_second <= 0:
            raise ValueError("EVENTS_PER_SECOND must be positive")
        if self.settings.meter_count <= 0:
            raise ValueError("METER_COUNT must be positive")
        self.health()

    def health(self) -> None:
        self.client.describe_stream_summary(StreamName=self.settings.stream_name)

    def run(self) -> None:
        delay = 1 / self.settings.events_per_second
        LOGGER.info(
            "publishing %s to %s at %.1f events/sec",
            self.name,
            self.settings.stream_name,
            self.settings.events_per_second,
        )
        while True:
            reading = generate_reading(self.settings.meter_count)
            payload = json.dumps(reading.as_dict(), separators=(",", ":")) + "\n"
            self.client.put_record(
                StreamName=self.settings.stream_name,
                Data=payload.encode(),
                PartitionKey=reading.meter_id,
            )
            time.sleep(delay)
