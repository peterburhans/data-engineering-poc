import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    stream_name: str
    pricing_stream_name: str
    aws_endpoint_url: str | None
    aws_region: str
    events_per_second: float
    meter_count: int
    pricing_refresh_seconds: int
    raw_bucket: str

    @classmethod
    def from_environment(cls) -> "Settings":
        return cls(
            stream_name=os.getenv("KINESIS_STREAM_NAME", "local_smart_meter_events_kds"),
            pricing_stream_name=os.getenv("PRICING_STREAM_NAME", "local_smart_meter_prices_kds"),
            aws_endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
            aws_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            events_per_second=float(os.getenv("EVENTS_PER_SECOND", "5")),
            meter_count=int(os.getenv("METER_COUNT", "100")),
            pricing_refresh_seconds=int(os.getenv("PRICING_REFRESH_SECONDS", "300")),
            raw_bucket=os.getenv("RAW_BUCKET", "local-smart-meter-raw-s3"),
        )
