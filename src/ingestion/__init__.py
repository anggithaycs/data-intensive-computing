"""Ingestion module providing generic and specialized dataset ingestors."""

from .air_quality_ingestion import AirQualityIngestor
from .base_ingestion import BaseDatasetIngestor
from .taxi_ingestion import TaxiTripsIngestor
from .weather_ingestion import WeatherIngestor
from .zone_ingestion import TaxiZoneIngestor

__all__ = [
    "AirQualityIngestor",
    "BaseDatasetIngestor",
    "TaxiTripsIngestor",
    "TaxiZoneIngestor",
    "WeatherIngestor",
]
