from mock_data_service.providers.base import MockProvider
from mock_data_service.providers.meter_readings import MeterReadingProvider
from mock_data_service.providers.pricing import PricingProvider
from mock_data_service.settings import Settings


def build_providers(settings: Settings) -> tuple[MockProvider, ...]:
    """Single extension point for enabling additional mock-data domains."""
    return MeterReadingProvider(settings), PricingProvider(settings)


__all__ = ["MockProvider", "PricingProvider", "build_providers"]
