"""Tests for the ETL cleaning layer (etl/cleaning/).

Non-Spark tests (always run): source_name properties, FileNotFoundError on
missing Bronze files.

Spark tests (marked @requires_spark): type casts, null filters, deduplication,
GPS filtering, Parquet output. Skipped locally when PySpark / Java is absent.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from etl.cleaning import (
    BikeStationsCleaner,
    EventsCleaner,
    ParkingsCleaner,
    TransportCleaner,
    VenuesCleaner,
)
from etl.config import ETLConfig
from tests.etl.conftest import requires_spark

# ── Helpers ────────────────────────────────────────────────────────────────────


def _cfg(tmp_path: Path) -> ETLConfig:
    return ETLConfig(
        raw_data_path=tmp_path / "raw", processed_data_path=tmp_path / "silver"
    )


def _write_bronze(directory: Path, source: str, records: list[dict]) -> Path:
    """Write a minimal Bronze JSON file for *source* in *directory*."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{source}_2026-01-01.json"
    path.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")
    return path


# ── source_name (always run, no Spark) ────────────────────────────────────────


class TestSourceNames:
    def test_events_source_name(self):
        assert EventsCleaner().source_name == "events"

    def test_venues_source_name(self):
        assert VenuesCleaner().source_name == "venues"

    def test_parkings_source_name(self):
        assert ParkingsCleaner().source_name == "parkings"

    def test_bike_stations_source_name(self):
        assert BikeStationsCleaner().source_name == "bike_stations"

    def test_transport_source_name(self):
        assert TransportCleaner().source_name == "transport"


# ── read_bronze raises before touching Spark (always run) ─────────────────────


class TestReadBronzeNoFiles:
    """read_bronze() checks for files before calling spark.read — the
    FileNotFoundError is raised without any PySpark dependency."""

    def test_events_raises_when_no_bronze_file(self, tmp_path):
        cfg = _cfg(tmp_path)
        with pytest.raises(FileNotFoundError, match="events"):
            EventsCleaner(config=cfg).read_bronze(None)  # type: ignore[arg-type]

    def test_venues_raises_when_no_bronze_file(self, tmp_path):
        cfg = _cfg(tmp_path)
        with pytest.raises(FileNotFoundError, match="venues"):
            VenuesCleaner(config=cfg).read_bronze(None)  # type: ignore[arg-type]

    def test_parkings_raises_when_no_bronze_file(self, tmp_path):
        cfg = _cfg(tmp_path)
        with pytest.raises(FileNotFoundError, match="parkings"):
            ParkingsCleaner(config=cfg).read_bronze(None)  # type: ignore[arg-type]

    def test_bike_stations_raises_when_no_bronze_file(self, tmp_path):
        cfg = _cfg(tmp_path)
        with pytest.raises(FileNotFoundError, match="bike_stations"):
            BikeStationsCleaner(config=cfg).read_bronze(None)  # type: ignore[arg-type]

    def test_transport_raises_when_no_bronze_file(self, tmp_path):
        cfg = _cfg(tmp_path)
        with pytest.raises(FileNotFoundError, match="transport"):
            TransportCleaner(config=cfg).read_bronze(None)  # type: ignore[arg-type]


# ── EventsCleaner (Spark) ─────────────────────────────────────────────────────

_VALID_EVENT = {
    "id": "evt-001",
    "external_id": "oa-001",
    "title": "Hamlet",
    "category": "theatre",
    "venue_id": "v-001",
    "date_start": "2026-07-05",
    "date_end": "2026-07-05",
    "time_start": "20:30",
    "duration_minutes": 120,
    "price_min": 10.0,
    "price_max": 35.0,
    "remaining_seats": 50,
    "language": "fr",
    "pmr_accessible": True,
    "surtitled": False,
}


class TestEventsCleanerSpark:
    @requires_spark
    def test_clean_returns_expected_columns(self, spark_session, tmp_path):
        cfg = _cfg(tmp_path)
        _write_bronze(cfg.raw_data_path, "events", [_VALID_EVENT])
        df_raw = EventsCleaner(config=cfg).read_bronze(spark_session)
        df_clean = EventsCleaner(config=cfg).clean(df_raw)
        assert "id" in df_clean.columns
        assert "date_start" in df_clean.columns
        assert "price_min" in df_clean.columns

    @requires_spark
    def test_clean_drops_null_id(self, spark_session, tmp_path):
        cfg = _cfg(tmp_path)
        records = [_VALID_EVENT, {**_VALID_EVENT, "id": None}]
        _write_bronze(cfg.raw_data_path, "events", records)
        df = EventsCleaner(config=cfg).read_bronze(spark_session)
        df_clean = EventsCleaner(config=cfg).clean(df)
        assert df_clean.count() == 1

    @requires_spark
    def test_clean_deduplicates_on_id(self, spark_session, tmp_path):
        cfg = _cfg(tmp_path)
        records = [_VALID_EVENT, _VALID_EVENT]
        _write_bronze(cfg.raw_data_path, "events", records)
        df = EventsCleaner(config=cfg).read_bronze(spark_session)
        df_clean = EventsCleaner(config=cfg).clean(df)
        assert df_clean.count() == 1

    @requires_spark
    def test_clean_casts_date_start(self, spark_session, tmp_path):
        from pyspark.sql.types import DateType

        cfg = _cfg(tmp_path)
        _write_bronze(cfg.raw_data_path, "events", [_VALID_EVENT])
        df = EventsCleaner(config=cfg).read_bronze(spark_session)
        df_clean = EventsCleaner(config=cfg).clean(df)
        date_field = next(f for f in df_clean.schema.fields if f.name == "date_start")
        assert isinstance(date_field.dataType, DateType)

    @requires_spark
    def test_clean_fills_null_price_with_zero(self, spark_session, tmp_path):
        cfg = _cfg(tmp_path)
        record = {**_VALID_EVENT, "price_min": None, "price_max": None}
        _write_bronze(cfg.raw_data_path, "events", [record])
        df = EventsCleaner(config=cfg).read_bronze(spark_session)
        df_clean = EventsCleaner(config=cfg).clean(df)
        row = df_clean.first()
        assert row["price_min"] == 0.0
        assert row["price_max"] == 0.0

    @requires_spark
    def test_run_creates_parquet_directory(self, spark_session, tmp_path):
        cfg = _cfg(tmp_path)
        _write_bronze(cfg.raw_data_path, "events", [_VALID_EVENT])
        out = EventsCleaner(config=cfg).run(spark_session)
        assert out.exists()
        parquet_files = list(out.glob("*.parquet"))
        assert len(parquet_files) > 0


# ── VenuesCleaner (Spark) ─────────────────────────────────────────────────────

_VALID_VENUE = {
    "id": "v-001",
    "name": "Palais des Papes",
    "address": "Place du Palais",
    "city": "Avignon",
    "postal_code": "84000",
    "lat": 43.9515,
    "lon": 4.8056,
    "type": "theatre",
    "capacity": 2000,
    "pmr_accessible": True,
    "pmr_spots": 20,
    "phone": None,
    "website": None,
}


class TestVenuesCleanerSpark:
    @requires_spark
    def test_clean_keeps_valid_venue(self, spark_session, tmp_path):
        cfg = _cfg(tmp_path)
        _write_bronze(cfg.raw_data_path, "venues", [_VALID_VENUE])
        df = VenuesCleaner(config=cfg).read_bronze(spark_session)
        df_clean = VenuesCleaner(config=cfg).clean(df)
        assert df_clean.count() == 1

    @requires_spark
    def test_clean_drops_venue_outside_bbox(self, spark_session, tmp_path):
        cfg = _cfg(tmp_path)
        outside = {**_VALID_VENUE, "id": "v-out", "lat": 48.8566, "lon": 2.3522}
        _write_bronze(cfg.raw_data_path, "venues", [_VALID_VENUE, outside])
        df = VenuesCleaner(config=cfg).read_bronze(spark_session)
        df_clean = VenuesCleaner(config=cfg).clean(df)
        assert df_clean.count() == 1
        assert df_clean.first()["id"] == "v-001"

    @requires_spark
    def test_clean_defaults_city_when_null(self, spark_session, tmp_path):
        cfg = _cfg(tmp_path)
        record = {**_VALID_VENUE, "city": None}
        _write_bronze(cfg.raw_data_path, "venues", [record])
        df = VenuesCleaner(config=cfg).read_bronze(spark_session)
        df_clean = VenuesCleaner(config=cfg).clean(df)
        assert df_clean.first()["city"] == "Avignon"

    @requires_spark
    def test_clean_drops_null_id(self, spark_session, tmp_path):
        cfg = _cfg(tmp_path)
        records = [_VALID_VENUE, {**_VALID_VENUE, "id": None}]
        _write_bronze(cfg.raw_data_path, "venues", records)
        df = VenuesCleaner(config=cfg).read_bronze(spark_session)
        df_clean = VenuesCleaner(config=cfg).clean(df)
        assert df_clean.count() == 1


# ── ParkingsCleaner (Spark) ───────────────────────────────────────────────────

_VALID_PARKING = {
    "id": "p-001",
    "name": "Parking Palais",
    "lat": 43.9490,
    "lon": 4.8050,
    "capacity": 300,
    "hourly_rate": 1.5,
    "open_24h": True,
    "pmr_spots": 10,
    "type": "underground",
}


class TestParkingsCleanerSpark:
    @requires_spark
    def test_clean_keeps_valid_parking(self, spark_session, tmp_path):
        cfg = _cfg(tmp_path)
        _write_bronze(cfg.raw_data_path, "parkings", [_VALID_PARKING])
        df = ParkingsCleaner(config=cfg).read_bronze(spark_session)
        df_clean = ParkingsCleaner(config=cfg).clean(df)
        assert df_clean.count() == 1

    @requires_spark
    def test_clean_drops_outside_bbox(self, spark_session, tmp_path):
        cfg = _cfg(tmp_path)
        outside = {**_VALID_PARKING, "id": "p-out", "lat": 48.8566, "lon": 2.3522}
        _write_bronze(cfg.raw_data_path, "parkings", [_VALID_PARKING, outside])
        df = ParkingsCleaner(config=cfg).read_bronze(spark_session)
        df_clean = ParkingsCleaner(config=cfg).clean(df)
        assert df_clean.count() == 1

    @requires_spark
    def test_clean_fills_null_hourly_rate(self, spark_session, tmp_path):
        cfg = _cfg(tmp_path)
        record = {**_VALID_PARKING, "hourly_rate": None}
        _write_bronze(cfg.raw_data_path, "parkings", [record])
        df = ParkingsCleaner(config=cfg).read_bronze(spark_session)
        df_clean = ParkingsCleaner(config=cfg).clean(df)
        assert df_clean.first()["hourly_rate"] == 0.0


# ── BikeStationsCleaner (Spark) ───────────────────────────────────────────────

_VALID_STATION = {
    "id": "bs-001",
    "name": "République",
    "lat": 43.9480,
    "lon": 4.8060,
    "capacity": 20,
    "available_bikes": 5,
    "available_docks": 15,
    "network": "velopop",
    "pmr_accessible": False,
}


class TestBikeStationsCleanerSpark:
    @requires_spark
    def test_clean_keeps_valid_station(self, spark_session, tmp_path):
        cfg = _cfg(tmp_path)
        _write_bronze(cfg.raw_data_path, "bike_stations", [_VALID_STATION])
        df = BikeStationsCleaner(config=cfg).read_bronze(spark_session)
        df_clean = BikeStationsCleaner(config=cfg).clean(df)
        assert df_clean.count() == 1

    @requires_spark
    def test_clean_derives_available_docks_when_missing(self, spark_session, tmp_path):
        cfg = _cfg(tmp_path)
        record = {**_VALID_STATION, "available_docks": None}
        _write_bronze(cfg.raw_data_path, "bike_stations", [record])
        df = BikeStationsCleaner(config=cfg).read_bronze(spark_session)
        df_clean = BikeStationsCleaner(config=cfg).clean(df)
        row = df_clean.first()
        assert row["available_docks"] == row["capacity"] - row["available_bikes"]


# ── TransportCleaner (Spark) ──────────────────────────────────────────────────

_VALID_STOP = {
    "id": "bus_001",
    "name": "Gare Routière",
    "lat": 43.9462,
    "lon": 4.8059,
    "lines": ["1", "3"],
    "pmr_accessible": True,
    "shelter": True,
    "real_time_display": True,
}


class TestTransportCleanerSpark:
    @requires_spark
    def test_clean_keeps_valid_stop(self, spark_session, tmp_path):
        cfg = _cfg(tmp_path)
        _write_bronze(cfg.raw_data_path, "transport", [_VALID_STOP])
        df = TransportCleaner(config=cfg).read_bronze(spark_session)
        df_clean = TransportCleaner(config=cfg).clean(df)
        assert df_clean.count() == 1

    @requires_spark
    def test_clean_drops_stop_outside_bbox(self, spark_session, tmp_path):
        cfg = _cfg(tmp_path)
        outside = {**_VALID_STOP, "id": "bus_out", "lat": 43.50, "lon": 5.00}
        _write_bronze(cfg.raw_data_path, "transport", [_VALID_STOP, outside])
        df = TransportCleaner(config=cfg).read_bronze(spark_session)
        df_clean = TransportCleaner(config=cfg).clean(df)
        assert df_clean.count() == 1
