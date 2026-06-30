"""Tests for the ETL ingestion layer (etl/ingestion/).

All tests run offline: every ingester has use_fixtures_fallback=True by default,
so API failures (no key, no network) simply trigger the fixture fallback path.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from etl.config import ETLConfig
from etl.ingestion import (
    BikeStationsIngester,
    EventsIngester,
    ParkingsIngester,
    TransportIngester,
    VenuesIngester,
)

# ── Helpers ───────────────────────────────────────────────────────────────────


def _cfg_with_raw(tmp_path: Path) -> ETLConfig:
    """Return an ETLConfig that writes Bronze files into *tmp_path*."""
    return ETLConfig(raw_data_path=tmp_path)


# ── BaseIngester behaviour (tested via EventsIngester as a concrete proxy) ────


class TestBaseIngesterValidation:
    def test_validate_raises_on_empty_list(self):
        ingester = EventsIngester(config=ETLConfig(use_fixtures_fallback=False))
        with pytest.raises(ValueError, match="empty dataset"):
            ingester.validate_data([])

    def test_validate_raises_on_non_dict_items(self):
        ingester = EventsIngester(config=ETLConfig(use_fixtures_fallback=False))
        with pytest.raises(ValueError, match="non-dict"):
            ingester.validate_data(["string", 42])

    def test_validate_passthrough_for_valid_data(self):
        ingester = EventsIngester()
        data = [{"id": "1", "title": "Test"}]
        assert ingester.validate_data(data) is data

    def test_fetch_fallback_triggers_on_api_error(self, tmp_path):
        # disable_api by clearing any key; API call will raise → fallback fires
        cfg = ETLConfig(open_agenda_api_key="", use_fixtures_fallback=True)
        ingester = EventsIngester(config=cfg)
        data = ingester.fetch_data()
        assert len(data) > 0

    def test_fetch_raises_when_fallback_disabled(self):
        cfg = ETLConfig(open_agenda_api_key="", use_fixtures_fallback=False)
        ingester = EventsIngester(config=cfg)
        with pytest.raises(RuntimeError, match="OPEN_AGENDA_API_KEY"):
            ingester._fetch_from_api()


# ── EventsIngester ────────────────────────────────────────────────────────────


class TestEventsIngester:
    def test_fixture_returns_60_events(self):
        data = EventsIngester()._load_from_fixtures()
        assert len(data) == 60

    def test_fetch_data_falls_back_to_fixtures(self):
        data = EventsIngester().fetch_data()
        assert len(data) == 60

    def test_ingest_creates_json_file(self, tmp_path):
        path = EventsIngester(config=_cfg_with_raw(tmp_path)).ingest()
        assert path.exists()
        assert path.suffix == ".json"
        assert "events_" in path.name

    def test_ingest_file_contains_valid_json(self, tmp_path):
        path = EventsIngester(config=_cfg_with_raw(tmp_path)).ingest()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert len(data) == 60

    def test_source_name(self):
        assert EventsIngester().source_name == "events"


# ── VenuesIngester ────────────────────────────────────────────────────────────


class TestVenuesIngester:
    def test_fixture_returns_25_venues(self):
        data = VenuesIngester()._load_from_fixtures()
        assert len(data) == 25

    def test_fetch_data_falls_back_to_fixtures(self):
        data = VenuesIngester().fetch_data()
        assert len(data) == 25

    def test_ingest_creates_json_file(self, tmp_path):
        path = VenuesIngester(config=_cfg_with_raw(tmp_path)).ingest()
        assert path.exists()
        assert "venues_" in path.name

    def test_ingest_file_has_correct_count(self, tmp_path):
        path = VenuesIngester(config=_cfg_with_raw(tmp_path)).ingest()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert len(data) == 25

    def test_source_name(self):
        assert VenuesIngester().source_name == "venues"


# ── ParkingsIngester ──────────────────────────────────────────────────────────


class TestParkingsIngester:
    def test_fixture_returns_15_parkings(self):
        data = ParkingsIngester()._load_from_fixtures()
        assert len(data) == 15

    def test_fetch_data_falls_back_to_fixtures(self):
        data = ParkingsIngester().fetch_data()
        assert len(data) == 15

    def test_ingest_creates_json_file(self, tmp_path):
        path = ParkingsIngester(config=_cfg_with_raw(tmp_path)).ingest()
        assert path.exists()
        assert "parkings_" in path.name

    def test_source_name(self):
        assert ParkingsIngester().source_name == "parkings"


# ── BikeStationsIngester ──────────────────────────────────────────────────────


class TestBikeStationsIngester:
    def test_fixture_returns_12_stations(self):
        data = BikeStationsIngester()._load_from_fixtures()
        assert len(data) == 12

    def test_fetch_data_falls_back_to_fixtures(self):
        data = BikeStationsIngester().fetch_data()
        assert len(data) == 12

    def test_fetch_api_raises_without_key(self):
        cfg = ETLConfig(jcdecaux_api_key="", use_fixtures_fallback=False)
        ingester = BikeStationsIngester(config=cfg)
        with pytest.raises(RuntimeError, match="JCDECAUX_API_KEY"):
            ingester._fetch_from_api()

    def test_ingest_creates_json_file(self, tmp_path):
        path = BikeStationsIngester(config=_cfg_with_raw(tmp_path)).ingest()
        assert path.exists()
        assert "bike_stations_" in path.name

    def test_source_name(self):
        assert BikeStationsIngester().source_name == "bike_stations"


# ── TransportIngester ─────────────────────────────────────────────────────────


class TestTransportIngester:
    def test_fixture_returns_8_stops(self):
        data = TransportIngester()._load_from_fixtures()
        assert len(data) == 8

    def test_fetch_data_falls_back_to_fixtures(self):
        data = TransportIngester().fetch_data()
        assert len(data) == 8

    def test_ingest_creates_json_file(self, tmp_path):
        path = TransportIngester(config=_cfg_with_raw(tmp_path)).ingest()
        assert path.exists()
        assert "transport_" in path.name

    def test_source_name(self):
        assert TransportIngester().source_name == "transport"


# ── Integration: all ingesters write distinct Bronze files ────────────────────


class TestAllIngestersIntegration:
    def test_all_ingesters_write_to_same_directory(self, tmp_path):
        cfg = _cfg_with_raw(tmp_path)
        ingesters = [
            EventsIngester(config=cfg),
            VenuesIngester(config=cfg),
            ParkingsIngester(config=cfg),
            BikeStationsIngester(config=cfg),
            TransportIngester(config=cfg),
        ]
        paths = [ing.ingest() for ing in ingesters]
        # All 5 files exist and are distinct
        assert len({str(p) for p in paths}) == 5
        for path in paths:
            assert path.exists()

    def test_all_bronze_files_are_valid_json_lists(self, tmp_path):
        cfg = _cfg_with_raw(tmp_path)
        for cls in [
            EventsIngester,
            VenuesIngester,
            ParkingsIngester,
            BikeStationsIngester,
            TransportIngester,
        ]:
            path = cls(config=cfg).ingest()
            data = json.loads(path.read_text(encoding="utf-8"))
            assert isinstance(data, list), f"{cls.__name__} did not write a JSON list"
            assert len(data) > 0, f"{cls.__name__} wrote an empty list"

    def test_file_names_contain_today_date(self, tmp_path):
        from datetime import datetime

        today = datetime.now().strftime("%Y-%m-%d")
        cfg = _cfg_with_raw(tmp_path)
        path = EventsIngester(config=cfg).ingest()
        assert today in path.name
