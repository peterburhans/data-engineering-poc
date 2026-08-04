import argparse
import logging
import threading
from datetime import UTC, datetime, timedelta

import uvicorn
from fastapi import FastAPI

from mock_data_service.providers import build_providers
from mock_data_service.providers.pricing import generate_prices
from mock_data_service.raw_backfill import RawBackfillWriter
from mock_data_service.settings import Settings

LOGGER = logging.getLogger(__name__)
settings = Settings.from_environment()
providers = build_providers(settings)
app = FastAPI(title="Mock Data Service", version="1.0.0")


@app.get("/health")
def health() -> dict[str, object]:
    for provider in providers:
        provider.health()
    return {"status": "healthy", "providers": [provider.name for provider in providers]}


@app.get("/v1/prices/current")
def current_price() -> dict[str, list[dict]]:
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    return {"prices": [event.as_dict() for event in generate_prices(now, now + timedelta(hours=1))]}


def run_provider(provider: object) -> None:
    try:
        provider.run()  # type: ignore[attr-defined]
    except Exception:
        LOGGER.exception("mock provider stopped unexpectedly: %s", provider.name)  # type: ignore[attr-defined]
        raise


def backfill_window(days: int, now: datetime | None = None) -> tuple[datetime, datetime]:
    if days < 1:
        raise ValueError("--days must be a positive number")
    end = (
        (now or datetime.now(UTC))
        .astimezone(UTC)
        .replace(hour=0, minute=0, second=0, microsecond=0)
    )
    return end - timedelta(days=days), end


def backfill(days: int, workers: int) -> None:
    start, end = backfill_window(days)
    writer = RawBackfillWriter(settings, workers)
    LOGGER.info("writing raw backfill from %s up to but excluding %s", start, end)
    readings = writer.meter_readings(start, end, settings.meter_count, 60, 2026)
    prices = writer.prices(start, end)
    LOGGER.info("uploaded %s readings and %s prices", f"{readings:,}", f"{prices:,}")


def serve() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for provider in providers:
        provider.bootstrap()
    for provider in providers:
        threading.Thread(
            target=run_provider,
            args=(provider,),
            daemon=True,
            name=f"mock-{provider.name}",
        ).start()
    uvicorn.run(app, host="0.0.0.0", port=8080)


def main() -> None:
    parser = argparse.ArgumentParser(description="Mock smart-meter event service")
    commands = parser.add_subparsers(dest="command")
    backfill_parser = commands.add_parser(
        "backfill", help="Write historical events directly to partitioned raw S3."
    )
    backfill_parser.add_argument("--days", type=int, required=True, help="Number of days back.")
    backfill_parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.command == "backfill":
        backfill(args.days, args.workers)
        return
    serve()


if __name__ == "__main__":
    main()
