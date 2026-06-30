"""Tests for the ETL transformation layer (etl/transformation/).

Non-Spark tests (always run): haversine_m(), GeoEnricher, GraphBuilder,
EventsEnricher.read_*_silver() FileNotFoundError.

Spark tests (marked @requires_spark): EventsEnricher.enrich() and run().
Skipped locally when PySpark / Java is absent.
"""

from __future__ import annotations

import json

import pytest

from etl.config import ETLConfig
from etl.transformation import (
    EventsEnricher,
    GeoEnricher,
    GraphBuilder,
    VenueProximity,
    haversine_m,
)
from tests.etl.conftest import requires_spark

# ── Fixtures ────────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def fixture_venues():
    from etl.fixtures.venues_fixture import get_venues

    return get_venues()


@pytest.fixture(scope="module")
def fixture_events():
    from etl.fixtures.events_fixture import get_events

    return get_events()


@pytest.fixture(scope="module")
def fixture_parkings():
    from etl.fixtures.mobility_fixture import get_parkings

    return get_parkings()


@pytest.fixture(scope="module")
def fixture_bike_stations():
    from etl.fixtures.mobility_fixture import get_bike_stations

    return get_bike_stations()


@pytest.fixture(scope="module")
def fixture_bus_stops():
    from etl.fixtures.mobility_fixture import get_bus_stops

    return get_bus_stops()


# ── haversine_m (always run) ───────────────────────────────────────────────────


class TestHaversineM:
    def test_same_point_is_zero(self):
        assert haversine_m(43.9515, 4.8056, 43.9515, 4.8056) == pytest.approx(
            0.0, abs=1e-6
        )

    def test_returns_positive_distance(self):
        d = haversine_m(43.9515, 4.8056, 43.9462, 4.8059)
        assert d > 0

    def test_is_symmetric(self):
        d1 = haversine_m(43.9515, 4.8056, 43.9462, 4.8059)
        d2 = haversine_m(43.9462, 4.8059, 43.9515, 4.8056)
        assert d1 == pytest.approx(d2, abs=1e-3)

    def test_palais_des_papes_to_gare_avignon(self):
        # Palais des Papes → Gare Avignon Centre: measured ~1136 m
        d = haversine_m(43.9515, 4.8056, 43.9416, 4.8091)
        assert 1_050 < d < 1_250

    def test_known_short_distance(self):
        # Two points ~110 m apart (lat offset ~0.001°)
        d = haversine_m(43.9500, 4.8060, 43.9510, 4.8060)
        assert 100 < d < 120


# ── GeoEnricher (always run) ───────────────────────────────────────────────────


class TestGeoEnricher:
    def test_returns_dict_keyed_by_venue_id(
        self, fixture_venues, fixture_parkings, fixture_bike_stations, fixture_bus_stops
    ):
        result = GeoEnricher().enrich_venues(
            fixture_venues, fixture_parkings, fixture_bike_stations, fixture_bus_stops
        )
        assert set(result.keys()) == {v["id"] for v in fixture_venues}

    def test_all_values_are_venue_proximity(
        self, fixture_venues, fixture_parkings, fixture_bike_stations, fixture_bus_stops
    ):
        result = GeoEnricher().enrich_venues(
            fixture_venues, fixture_parkings, fixture_bike_stations, fixture_bus_stops
        )
        assert all(isinstance(v, VenueProximity) for v in result.values())

    def test_some_venues_have_nearby_parkings(
        self, fixture_venues, fixture_parkings, fixture_bike_stations, fixture_bus_stops
    ):
        # With threshold 500 m, at least some Avignon venues should have a nearby parking
        result = GeoEnricher().enrich_venues(
            fixture_venues, fixture_parkings, fixture_bike_stations, fixture_bus_stops
        )
        total_edges = sum(len(p.nearby_parkings) for p in result.values())
        assert total_edges > 0

    def test_tiny_threshold_excludes_distant_pois(self):
        # Use controlled data: venue and parking are ~11 m apart (safe > 1 m threshold)
        venue = [{"id": "v1", "lat": 43.9500, "lon": 4.8060}]
        parking = [{"id": "p1", "lat": 43.9501, "lon": 4.8060}]  # ~11 m north
        cfg = ETLConfig(proximity_threshold_m=5)
        result = GeoEnricher(config=cfg).enrich_venues(venue, parking, [], [])
        assert len(result["v1"].nearby_parkings) == 0

    def test_huge_threshold_finds_all(
        self, fixture_venues, fixture_parkings, fixture_bike_stations, fixture_bus_stops
    ):
        cfg = ETLConfig(proximity_threshold_m=100_000)
        result = GeoEnricher(config=cfg).enrich_venues(
            fixture_venues, fixture_parkings, fixture_bike_stations, fixture_bus_stops
        )
        for prox in result.values():
            assert len(prox.nearby_parkings) == len(fixture_parkings)
            assert len(prox.nearby_bike_stations) == len(fixture_bike_stations)
            assert len(prox.nearby_bus_stops) == len(fixture_bus_stops)

    def test_specific_proximity(self):
        # Venue and parking 200 m apart — both within 500 m threshold, neither within 100 m
        venue = [{"id": "v1", "lat": 43.9500, "lon": 4.8060}]
        parking = [{"id": "p1", "lat": 43.9518, "lon": 4.8060}]  # ~200 m north
        result_500 = GeoEnricher(
            config=ETLConfig(proximity_threshold_m=500)
        ).enrich_venues(venue, parking, [], [])
        result_100 = GeoEnricher(
            config=ETLConfig(proximity_threshold_m=100)
        ).enrich_venues(venue, parking, [], [])
        assert "p1" in result_500["v1"].nearby_parkings
        assert "p1" not in result_100["v1"].nearby_parkings


# ── GraphBuilder (always run) ──────────────────────────────────────────────────


class TestGraphBuilderNodes:
    def test_build_nodes_returns_all_five_types(
        self,
        fixture_events,
        fixture_venues,
        fixture_parkings,
        fixture_bike_stations,
        fixture_bus_stops,
    ):
        nodes = GraphBuilder().build_nodes(
            fixture_events,
            fixture_venues,
            fixture_parkings,
            fixture_bike_stations,
            fixture_bus_stops,
        )
        assert set(nodes.keys()) == {
            "events",
            "venues",
            "parkings",
            "bike_stations",
            "bus_stops",
        }

    def test_node_counts_match_inputs(
        self,
        fixture_events,
        fixture_venues,
        fixture_parkings,
        fixture_bike_stations,
        fixture_bus_stops,
    ):
        nodes = GraphBuilder().build_nodes(
            fixture_events,
            fixture_venues,
            fixture_parkings,
            fixture_bike_stations,
            fixture_bus_stops,
        )
        assert len(nodes["events"]) == 60
        assert len(nodes["venues"]) == 25
        assert len(nodes["parkings"]) == 15
        assert len(nodes["bike_stations"]) == 12
        assert len(nodes["bus_stops"]) == 8

    def test_event_node_has_required_fields(
        self,
        fixture_events,
        fixture_venues,
        fixture_parkings,
        fixture_bike_stations,
        fixture_bus_stops,
    ):
        nodes = GraphBuilder().build_nodes(
            fixture_events,
            fixture_venues,
            fixture_parkings,
            fixture_bike_stations,
            fixture_bus_stops,
        )
        event_node = nodes["events"][0]
        assert "id" in event_node
        assert event_node["label"] == "Event"
        assert "title" in event_node

    def test_venue_node_has_gps(
        self,
        fixture_events,
        fixture_venues,
        fixture_parkings,
        fixture_bike_stations,
        fixture_bus_stops,
    ):
        nodes = GraphBuilder().build_nodes(
            fixture_events,
            fixture_venues,
            fixture_parkings,
            fixture_bike_stations,
            fixture_bus_stops,
        )
        for v in nodes["venues"]:
            assert v["label"] == "Venue"
            assert "lat" in v
            assert "lon" in v


class TestGraphBuilderEdges:
    def test_build_edges_returns_all_four_types(
        self,
        fixture_events,
        fixture_venues,
        fixture_parkings,
        fixture_bike_stations,
        fixture_bus_stops,
    ):
        edges = GraphBuilder().build_edges(
            fixture_events,
            fixture_venues,
            fixture_parkings,
            fixture_bike_stations,
            fixture_bus_stops,
        )
        assert set(edges.keys()) == {
            "event_at_venue",
            "venue_near_parking",
            "venue_near_bike",
            "venue_near_bus",
        }

    def test_all_events_have_venue_edge(
        self,
        fixture_events,
        fixture_venues,
        fixture_parkings,
        fixture_bike_stations,
        fixture_bus_stops,
    ):
        edges = GraphBuilder().build_edges(
            fixture_events,
            fixture_venues,
            fixture_parkings,
            fixture_bike_stations,
            fixture_bus_stops,
        )
        assert len(edges["event_at_venue"]) == 60

    def test_event_edge_has_correct_type(
        self,
        fixture_events,
        fixture_venues,
        fixture_parkings,
        fixture_bike_stations,
        fixture_bus_stops,
    ):
        edges = GraphBuilder().build_edges(
            fixture_events,
            fixture_venues,
            fixture_parkings,
            fixture_bike_stations,
            fixture_bus_stops,
        )
        for edge in edges["event_at_venue"]:
            assert edge["type"] == "EVENT_AT_VENUE"
            assert "from" in edge
            assert "to" in edge

    def test_proximity_edges_exist(
        self,
        fixture_events,
        fixture_venues,
        fixture_parkings,
        fixture_bike_stations,
        fixture_bus_stops,
    ):
        edges = GraphBuilder().build_edges(
            fixture_events,
            fixture_venues,
            fixture_parkings,
            fixture_bike_stations,
            fixture_bus_stops,
        )
        assert len(edges["venue_near_parking"]) > 0


class TestGraphBuilderSave:
    def test_save_creates_nine_json_files(
        self,
        tmp_path,
        fixture_events,
        fixture_venues,
        fixture_parkings,
        fixture_bike_stations,
        fixture_bus_stops,
    ):
        cfg = ETLConfig(curated_data_path=tmp_path / "curated")
        gb = GraphBuilder(config=cfg)
        graph_path = gb.run(
            fixture_events,
            fixture_venues,
            fixture_parkings,
            fixture_bike_stations,
            fixture_bus_stops,
        )
        json_files = list(graph_path.glob("*.json"))
        assert len(json_files) == 9  # 5 node files + 4 edge files

    def test_nodes_events_file_is_valid_json_list(
        self,
        tmp_path,
        fixture_events,
        fixture_venues,
        fixture_parkings,
        fixture_bike_stations,
        fixture_bus_stops,
    ):
        cfg = ETLConfig(curated_data_path=tmp_path / "curated")
        graph_path = GraphBuilder(config=cfg).run(
            fixture_events,
            fixture_venues,
            fixture_parkings,
            fixture_bike_stations,
            fixture_bus_stops,
        )
        data = json.loads(
            (graph_path / "nodes_events.json").read_text(encoding="utf-8")
        )
        assert isinstance(data, list)
        assert len(data) == 60

    def test_edges_event_at_venue_file_is_valid(
        self,
        tmp_path,
        fixture_events,
        fixture_venues,
        fixture_parkings,
        fixture_bike_stations,
        fixture_bus_stops,
    ):
        cfg = ETLConfig(curated_data_path=tmp_path / "curated")
        graph_path = GraphBuilder(config=cfg).run(
            fixture_events,
            fixture_venues,
            fixture_parkings,
            fixture_bike_stations,
            fixture_bus_stops,
        )
        data = json.loads(
            (graph_path / "edges_event_at_venue.json").read_text(encoding="utf-8")
        )
        assert isinstance(data, list)
        assert len(data) == 60


# ── EventsEnricher (no Spark — FileNotFoundError tests) ────────────────────────


class TestEventsEnricherNoSpark:
    def test_read_events_silver_raises_when_missing(self, tmp_path):
        cfg = ETLConfig(processed_data_path=tmp_path / "silver")
        with pytest.raises(FileNotFoundError, match="Events Silver"):
            EventsEnricher(config=cfg).read_events_silver(None)  # type: ignore[arg-type]

    def test_read_venues_silver_raises_when_missing(self, tmp_path):
        cfg = ETLConfig(processed_data_path=tmp_path / "silver")
        with pytest.raises(FileNotFoundError, match="Venues Silver"):
            EventsEnricher(config=cfg).read_venues_silver(None)  # type: ignore[arg-type]


# ── EventsEnricher (Spark) ─────────────────────────────────────────────────────


class TestEventsEnricherSpark:
    @requires_spark
    def test_enrich_adds_venue_columns(self, spark_session, tmp_path):
        import json as _json

        cfg = ETLConfig(
            processed_data_path=tmp_path / "silver",
            curated_data_path=tmp_path / "curated",
        )
        silver = tmp_path / "silver"

        events_data = [
            {
                "id": "e1",
                "external_id": "oa-1",
                "title": "Hamlet",
                "category": "theatre",
                "venue_id": "v1",
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
        ]
        venues_data = [
            {
                "id": "v1",
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
        ]

        (silver / "events").mkdir(parents=True)
        (silver / "venues").mkdir(parents=True)
        (silver / "events" / "data.json").write_text(
            _json.dumps(events_data), encoding="utf-8"
        )
        (silver / "venues" / "data.json").write_text(
            _json.dumps(venues_data), encoding="utf-8"
        )

        events_df = spark_session.read.option("multiLine", "true").json(
            str(silver / "events" / "data.json")
        )
        venues_df = spark_session.read.option("multiLine", "true").json(
            str(silver / "venues" / "data.json")
        )

        enriched_df = EventsEnricher(config=cfg).enrich(events_df, venues_df)

        assert "venue_name" in enriched_df.columns
        assert "venue_lat" in enriched_df.columns
        assert "venue_lon" in enriched_df.columns
        row = enriched_df.first()
        assert row["venue_name"] == "Palais des Papes"

    @requires_spark
    def test_enrich_keeps_all_events(self, spark_session, tmp_path):
        import json as _json

        cfg = ETLConfig(
            processed_data_path=tmp_path / "silver",
            curated_data_path=tmp_path / "curated",
        )
        silver = tmp_path / "silver"

        events_data = [
            {
                "id": f"e{i}",
                "external_id": f"oa-{i}",
                "title": f"Show {i}",
                "category": "theatre",
                "venue_id": "v1",
                "date_start": "2026-07-05",
                "date_end": "2026-07-05",
                "time_start": "20:30",
                "duration_minutes": 90,
                "price_min": 0.0,
                "price_max": 20.0,
                "remaining_seats": 100,
                "language": "fr",
                "pmr_accessible": False,
                "surtitled": False,
            }
            for i in range(5)
        ]
        venues_data = [
            {
                "id": "v1",
                "name": "Scène",
                "address": "1 Rue Test",
                "city": "Avignon",
                "postal_code": "84000",
                "lat": 43.9515,
                "lon": 4.8056,
                "type": "theatre",
                "capacity": 500,
                "pmr_accessible": False,
                "pmr_spots": 0,
                "phone": None,
                "website": None,
            }
        ]

        (silver / "events").mkdir(parents=True)
        (silver / "venues").mkdir(parents=True)
        (silver / "events" / "data.json").write_text(
            _json.dumps(events_data), encoding="utf-8"
        )
        (silver / "venues" / "data.json").write_text(
            _json.dumps(venues_data), encoding="utf-8"
        )

        events_df = spark_session.read.option("multiLine", "true").json(
            str(silver / "events" / "data.json")
        )
        venues_df = spark_session.read.option("multiLine", "true").json(
            str(silver / "venues" / "data.json")
        )

        enriched_df = EventsEnricher(config=cfg).enrich(events_df, venues_df)
        assert enriched_df.count() == 5
