"""Data-integrity tests for etl/fixtures/.

Validates counts, required fields, referential integrity (venue_id coherence),
GPS bounds, festival date range, and PMR consistency — without touching the
network, Spark, or any database.
"""

from __future__ import annotations

import re

import pytest

from etl.fixtures import (
    get_bike_stations,
    get_bus_stops,
    get_events,
    get_parkings,
    get_venues,
)

# ── Constants ──────────────────────────────────────────────────────────────────

FESTIVAL_START = "2026-07-05"
FESTIVAL_END = "2026-07-26"

# Avignon city bounding box (latitude, longitude)
LAT_MIN, LAT_MAX = 43.92, 43.97
LON_MIN, LON_MAX = 4.79, 4.83

VALID_CATEGORIES = {
    "Théâtre",
    "Danse",
    "Musique",
    "Cirque & Arts de la rue",
    "Performance",
}

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
TIME_RE = re.compile(r"^\d{2}:\d{2}$")

# ── Module-level fixtures (loaded once per test session) ───────────────────────


@pytest.fixture(scope="module")
def venues():
    return get_venues()


@pytest.fixture(scope="module")
def events():
    return get_events()


@pytest.fixture(scope="module")
def venue_ids():
    return {v["id"] for v in get_venues()}


@pytest.fixture(scope="module")
def parkings():
    return get_parkings()


@pytest.fixture(scope="module")
def bike_stations():
    return get_bike_stations()


@pytest.fixture(scope="module")
def bus_stops():
    return get_bus_stops()


# ── Venues ────────────────────────────────────────────────────────────────────


class TestVenues:
    def test_count(self, venues):
        assert len(venues) == 25

    def test_no_duplicate_ids(self, venues):
        ids = [v["id"] for v in venues]
        assert len(ids) == len(set(ids))

    def test_required_fields_present(self, venues):
        required = {
            "id",
            "name",
            "address",
            "city",
            "lat",
            "lon",
            "type",
            "capacity",
            "pmr_accessible",
        }
        for venue in venues:
            missing = required - venue.keys()
            assert not missing, f"{venue['id']} is missing: {missing}"

    def test_gps_coordinates_in_avignon_bounds(self, venues):
        for v in venues:
            assert LAT_MIN <= v["lat"] <= LAT_MAX, (
                f"{v['id']} lat={v['lat']} out of bounds"
            )
            assert LON_MIN <= v["lon"] <= LON_MAX, (
                f"{v['id']} lon={v['lon']} out of bounds"
            )

    def test_capacity_is_positive(self, venues):
        for v in venues:
            assert v["capacity"] > 0, f"{v['id']} has capacity={v['capacity']}"

    def test_pmr_accessible_is_bool(self, venues):
        for v in venues:
            assert isinstance(v["pmr_accessible"], bool), (
                f"{v['id']} pmr_accessible must be bool"
            )

    def test_pmr_spots_zero_when_not_accessible(self, venues):
        for v in venues:
            if not v["pmr_accessible"]:
                assert v["pmr_spots"] == 0, (
                    f"{v['id']} is not PMR but has pmr_spots > 0"
                )

    def test_all_cities_are_avignon(self, venues):
        for v in venues:
            assert v["city"] == "Avignon", f"{v['id']} city={v['city']}"

    def test_all_postal_codes_are_84000(self, venues):
        for v in venues:
            assert v["postal_code"] == "84000", (
                f"{v['id']} postal_code={v['postal_code']}"
            )


# ── Events ────────────────────────────────────────────────────────────────────


class TestEvents:
    def test_count(self, events):
        assert len(events) == 60

    def test_no_duplicate_ids(self, events):
        ids = [e["id"] for e in events]
        assert len(ids) == len(set(ids))

    def test_no_duplicate_external_ids(self, events):
        ext_ids = [e["external_id"] for e in events]
        assert len(ext_ids) == len(set(ext_ids))

    def test_required_fields_present(self, events):
        required = {
            "id",
            "external_id",
            "title",
            "category",
            "venue_id",
            "date_start",
            "date_end",
            "time_start",
            "duration_minutes",
            "price_min",
            "price_max",
            "remaining_seats",
            "pmr_accessible",
        }
        for event in events:
            missing = required - event.keys()
            assert not missing, f"{event['id']} is missing: {missing}"

    def test_all_venue_ids_exist(self, events, venue_ids):
        for event in events:
            assert event["venue_id"] in venue_ids, (
                f"{event['id']} references unknown venue_id={event['venue_id']}"
            )

    def test_categories_are_valid(self, events):
        for event in events:
            assert event["category"] in VALID_CATEGORIES, (
                f"{event['id']} has unknown category={event['category']}"
            )

    def test_dates_are_within_festival_period(self, events):
        for event in events:
            assert FESTIVAL_START <= event["date_start"] <= FESTIVAL_END, (
                f"{event['id']} date_start={event['date_start']} out of festival range"
            )
            assert FESTIVAL_START <= event["date_end"] <= FESTIVAL_END, (
                f"{event['id']} date_end={event['date_end']} out of festival range"
            )
            assert event["date_start"] <= event["date_end"], (
                f"{event['id']} date_start > date_end"
            )

    def test_date_format_is_iso(self, events):
        for event in events:
            assert DATE_RE.match(event["date_start"]), (
                f"{event['id']} bad date_start format"
            )
            assert DATE_RE.match(event["date_end"]), (
                f"{event['id']} bad date_end format"
            )

    def test_time_format_is_hhmm(self, events):
        for event in events:
            assert TIME_RE.match(event["time_start"]), (
                f"{event['id']} bad time_start={event['time_start']}"
            )

    def test_duration_is_positive(self, events):
        for event in events:
            assert event["duration_minutes"] > 0, f"{event['id']} duration <= 0"

    def test_price_max_gte_price_min(self, events):
        for event in events:
            assert event["price_max"] >= event["price_min"], (
                f"{event['id']} price_max < price_min"
            )

    def test_price_min_is_non_negative(self, events):
        for event in events:
            assert event["price_min"] >= 0.0, f"{event['id']} price_min < 0"

    def test_remaining_seats_is_non_negative(self, events):
        for event in events:
            assert event["remaining_seats"] >= 0, f"{event['id']} remaining_seats < 0"

    def test_pmr_accessible_is_bool(self, events):
        for event in events:
            assert isinstance(event["pmr_accessible"], bool)

    def test_category_distribution(self, events):
        counts: dict[str, int] = {}
        for e in events:
            counts[e["category"]] = counts.get(e["category"], 0) + 1
        assert counts.get("Théâtre", 0) == 30
        assert counts.get("Danse", 0) == 12
        assert counts.get("Musique", 0) == 8
        assert counts.get("Cirque & Arts de la rue", 0) == 5
        assert counts.get("Performance", 0) == 5

    def test_free_events_exist(self, events):
        free = [e for e in events if e["price_min"] == 0.0 and e["price_max"] == 0.0]
        assert len(free) >= 2, "Expected at least 2 free events"


# ── Parkings ──────────────────────────────────────────────────────────────────


class TestParkings:
    def test_count(self, parkings):
        assert len(parkings) == 15

    def test_no_duplicate_ids(self, parkings):
        ids = [p["id"] for p in parkings]
        assert len(ids) == len(set(ids))

    def test_required_fields_present(self, parkings):
        required = {
            "id",
            "name",
            "address",
            "lat",
            "lon",
            "capacity",
            "pmr_spots",
            "hourly_rate_eur",
            "open_24h",
        }
        for p in parkings:
            missing = required - p.keys()
            assert not missing, f"{p['id']} missing: {missing}"

    def test_gps_coordinates_near_avignon(self, parkings):
        for p in parkings:
            assert 43.92 <= p["lat"] <= 43.97, f"{p['id']} lat out of range"
            assert 4.79 <= p["lon"] <= 4.83, f"{p['id']} lon out of range"

    def test_capacity_is_positive(self, parkings):
        for p in parkings:
            assert p["capacity"] > 0

    def test_hourly_rate_is_non_negative(self, parkings):
        for p in parkings:
            assert p["hourly_rate_eur"] >= 0.0

    def test_open_24h_is_bool(self, parkings):
        for p in parkings:
            assert isinstance(p["open_24h"], bool)

    def test_free_parking_exists(self, parkings):
        free = [p for p in parkings if p["hourly_rate_eur"] == 0.0]
        assert len(free) >= 1, "Expected at least one free parking (P+R)"

    def test_high_capacity_pr_exists(self, parkings):
        large = [p for p in parkings if p["capacity"] >= 1000]
        assert len(large) >= 1, "Expected at least one P+R with capacity >= 1000"


# ── Bike Stations ─────────────────────────────────────────────────────────────


class TestBikeStations:
    def test_count(self, bike_stations):
        assert len(bike_stations) == 12

    def test_no_duplicate_ids(self, bike_stations):
        ids = [s["id"] for s in bike_stations]
        assert len(ids) == len(set(ids))

    def test_required_fields_present(self, bike_stations):
        required = {
            "id",
            "name",
            "lat",
            "lon",
            "capacity",
            "available_bikes",
            "available_docks",
        }
        for s in bike_stations:
            missing = required - s.keys()
            assert not missing, f"{s['id']} missing: {missing}"

    def test_gps_in_avignon_bounds(self, bike_stations):
        for s in bike_stations:
            assert LAT_MIN <= s["lat"] <= LAT_MAX, f"{s['id']} lat out of range"
            assert LON_MIN <= s["lon"] <= LON_MAX, f"{s['id']} lon out of range"

    def test_bikes_plus_docks_equals_capacity(self, bike_stations):
        for s in bike_stations:
            total = s["available_bikes"] + s["available_docks"]
            assert total == s["capacity"], (
                f"{s['id']}: bikes({s['available_bikes']}) + docks({s['available_docks']}) "
                f"= {total} != capacity({s['capacity']})"
            )

    def test_available_bikes_non_negative(self, bike_stations):
        for s in bike_stations:
            assert s["available_bikes"] >= 0

    def test_network_is_velopop(self, bike_stations):
        for s in bike_stations:
            assert s["network"] == "Vélopop"


# ── Bus Stops ─────────────────────────────────────────────────────────────────


class TestBusStops:
    def test_count(self, bus_stops):
        assert len(bus_stops) == 8

    def test_no_duplicate_ids(self, bus_stops):
        ids = [s["id"] for s in bus_stops]
        assert len(ids) == len(set(ids))

    def test_required_fields_present(self, bus_stops):
        required = {"id", "name", "lat", "lon", "lines", "pmr_accessible", "shelter"}
        for s in bus_stops:
            missing = required - s.keys()
            assert not missing, f"{s['id']} missing: {missing}"

    def test_lines_is_non_empty_list(self, bus_stops):
        for s in bus_stops:
            assert isinstance(s["lines"], list)
            assert len(s["lines"]) >= 1, f"{s['id']} has no bus lines"

    def test_gps_in_avignon_bounds(self, bus_stops):
        for s in bus_stops:
            assert LAT_MIN <= s["lat"] <= LAT_MAX, f"{s['id']} lat out of range"
            assert LON_MIN <= s["lon"] <= LON_MAX, f"{s['id']} lon out of range"


# ── Save functions (write to tmp_path and verify files) ────────────────────────


class TestSaveFunctions:
    def test_save_venues_creates_json_and_csv(self, tmp_path):
        from etl.fixtures import save_venues

        save_venues(tmp_path)
        assert (tmp_path / "venues_fixture.json").exists()
        assert (tmp_path / "venues_fixture.csv").exists()

    def test_save_events_creates_json_and_csv(self, tmp_path):
        from etl.fixtures import save_events

        save_events(tmp_path)
        assert (tmp_path / "events_fixture.json").exists()
        assert (tmp_path / "events_fixture.csv").exists()

    def test_save_mobility_creates_all_files(self, tmp_path):
        from etl.fixtures import save_mobility

        save_mobility(tmp_path)
        assert (tmp_path / "parkings_fixture.json").exists()
        assert (tmp_path / "parkings_fixture.csv").exists()
        assert (tmp_path / "bike_stations_fixture.json").exists()
        assert (tmp_path / "bike_stations_fixture.csv").exists()
        assert (tmp_path / "bus_stops_fixture.json").exists()
        assert (tmp_path / "bus_stops_fixture.csv").exists()

    def test_saved_json_is_valid_and_non_empty(self, tmp_path):
        import json as json_mod

        from etl.fixtures import save_events

        save_events(tmp_path)
        data = json_mod.loads(
            (tmp_path / "events_fixture.json").read_text(encoding="utf-8")
        )
        assert len(data) == 60

    def test_saved_venues_csv_has_correct_row_count(self, tmp_path):
        from etl.fixtures import save_venues

        save_venues(tmp_path)
        lines = (
            (tmp_path / "venues_fixture.csv").read_text(encoding="utf-8").splitlines()
        )
        # 1 header + 25 data rows
        assert len(lines) == 26
