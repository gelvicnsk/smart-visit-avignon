"""Tests for the ETL loading layer (etl/loading/).

Strategy:
  - Pure-Python tests: static helpers, empty-data guards, context manager protocol.
  - Mock tests: verify the correct pymongo / neo4j calls are made without a
    live database (unittest.mock.patch replaces the DB clients at import time).
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from etl.loading import MongoDBLoader, Neo4jLoader
from etl.loading.neo4j_loader import _build_edge_batch, _build_node_batch

# ── BaseLoader (tested via MongoDBLoader) ──────────────────────────────────────


class TestBaseLoader:
    def test_context_manager_calls_close(self):
        loader = MongoDBLoader()
        mock_client = MagicMock()
        loader._client = mock_client
        with loader:
            pass
        mock_client.close.assert_called_once()

    def test_context_manager_calls_close_on_exception(self):
        loader = MongoDBLoader()
        mock_client = MagicMock()
        loader._client = mock_client
        try:
            with loader:
                raise ValueError("deliberate")
        except ValueError:
            pass
        mock_client.close.assert_called_once()

    def test_enter_returns_self(self):
        loader = MongoDBLoader()
        assert loader.__enter__() is loader


# ── MongoDBLoader — pure Python ────────────────────────────────────────────────


class TestMongoDBLoaderPure:
    def test_initializes_without_connecting(self):
        loader = MongoDBLoader()
        assert loader._client is None

    def test_upsert_empty_list_returns_zero(self):
        loader = MongoDBLoader()
        result = loader.upsert("events", [])
        assert result == 0

    def test_build_operations_length_matches_data(self):
        data = [{"id": "e1"}, {"id": "e2"}, {"id": "e3"}]
        ops = MongoDBLoader._build_operations(data)
        assert len(ops) == 3

    def test_build_operations_uses_id_as_filter(self):
        data = [{"id": "e1", "title": "Hamlet"}]
        ops = MongoDBLoader._build_operations(data)
        # ReplaceOne exposes the filter via _filter
        assert ops[0]._filter == {"id": "e1"}

    def test_build_operations_sets_upsert_true(self):
        data = [{"id": "e1"}]
        ops = MongoDBLoader._build_operations(data)
        assert ops[0]._filter == {"id": "e1"}
        assert ops[0]._upsert is True


# ── MongoDBLoader — mock tests ─────────────────────────────────────────────────


class TestMongoDBLoaderMock:
    def _make_mock_collection(self, upserted=1, modified=0):
        mock_result = MagicMock()
        mock_result.upserted_count = upserted
        mock_result.modified_count = modified
        mock_collection = MagicMock()
        mock_collection.bulk_write.return_value = mock_result
        return mock_collection

    def _patched_loader(self, mock_client, mock_collection):
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client.return_value.__getitem__ = MagicMock(return_value=mock_db)

    def test_upsert_calls_bulk_write(self):
        with patch("etl.loading.mongo_loader.MongoClient") as mock_client:
            mock_collection = self._make_mock_collection(upserted=2)
            self._patched_loader(mock_client, mock_collection)

            loader = MongoDBLoader()
            loader.upsert("events", [{"id": "e1"}, {"id": "e2"}])

            mock_collection.bulk_write.assert_called_once()

    def test_upsert_returns_upserted_plus_modified(self):
        with patch("etl.loading.mongo_loader.MongoClient") as mock_client:
            mock_collection = self._make_mock_collection(upserted=1, modified=1)
            self._patched_loader(mock_client, mock_collection)

            loader = MongoDBLoader()
            result = loader.upsert("events", [{"id": "e1"}, {"id": "e2"}])

            assert result == 2

    def test_run_calls_upsert_for_all_five_collections(self):
        with patch("etl.loading.mongo_loader.MongoClient") as mock_client:
            mock_collection = self._make_mock_collection()
            self._patched_loader(mock_client, mock_collection)

            loader = MongoDBLoader()
            loader.run(
                events=[{"id": "e1"}],
                venues=[{"id": "v1"}],
                parkings=[{"id": "p1"}],
                bike_stations=[{"id": "b1"}],
                bus_stops=[{"id": "s1"}],
            )

            assert mock_collection.bulk_write.call_count == 5

    def test_close_closes_client(self):
        loader = MongoDBLoader()
        mock_client = MagicMock()
        loader._client = mock_client
        loader.close()
        mock_client.close.assert_called_once()
        assert loader._client is None

    def test_close_is_idempotent_when_no_client(self):
        loader = MongoDBLoader()
        loader.close()  # Should not raise


# ── Neo4jLoader — pure Python ──────────────────────────────────────────────────


class TestNeo4jLoaderPure:
    def test_initializes_without_connecting(self):
        loader = Neo4jLoader()
        assert loader._driver is None

    def test_load_nodes_empty_returns_zero(self):
        loader = Neo4jLoader()
        result = loader.load_nodes("Event", [])
        assert result == 0

    def test_load_edges_empty_returns_zero(self):
        loader = Neo4jLoader()
        result = loader.load_edges("EVENT_AT_VENUE", [])
        assert result == 0

    def test_build_node_batch_extracts_id_and_props(self):
        nodes = [{"id": "v1", "label": "Venue", "name": "Palais", "capacity": 2000}]
        batch = _build_node_batch(nodes)
        assert batch[0]["id"] == "v1"
        assert "name" in batch[0]["props"]
        assert "capacity" in batch[0]["props"]
        assert "label" not in batch[0]["props"]
        assert "id" not in batch[0]["props"]

    def test_build_node_batch_removes_none_values(self):
        nodes = [{"id": "v1", "label": "Venue", "phone": None, "name": "Test"}]
        batch = _build_node_batch(nodes)
        assert "phone" not in batch[0]["props"]
        assert "name" in batch[0]["props"]

    def test_build_edge_batch_maps_from_to(self):
        edges = [{"from": "e1", "to": "v1", "type": "EVENT_AT_VENUE"}]
        batch = _build_edge_batch(edges)
        assert batch[0]["from_id"] == "e1"
        assert batch[0]["to_id"] == "v1"

    def test_build_edge_batch_length_matches_input(self):
        edges = [
            {"from": "e1", "to": "v1", "type": "EVENT_AT_VENUE"},
            {"from": "e2", "to": "v1", "type": "EVENT_AT_VENUE"},
        ]
        batch = _build_edge_batch(edges)
        assert len(batch) == 2


# ── Neo4jLoader — mock tests ───────────────────────────────────────────────────


def _make_neo4j_mock():
    """Return (mock_GraphDatabase, mock_session) ready to patch."""
    mock_session = MagicMock()
    mock_driver = MagicMock()
    mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)
    mock_gdb = MagicMock()
    mock_gdb.driver.return_value = mock_driver
    return mock_gdb, mock_session


class TestNeo4jLoaderMock:
    def test_load_nodes_calls_session_run(self):
        mock_gdb, mock_session = _make_neo4j_mock()
        with patch("etl.loading.neo4j_loader.GraphDatabase", mock_gdb):
            loader = Neo4jLoader()
            count = loader.load_nodes(
                "Venue", [{"id": "v1", "label": "Venue", "name": "X"}]
            )
            mock_session.run.assert_called_once()
            assert count == 1

    def test_load_nodes_returns_correct_count(self):
        mock_gdb, _ = _make_neo4j_mock()
        with patch("etl.loading.neo4j_loader.GraphDatabase", mock_gdb):
            loader = Neo4jLoader()
            nodes = [{"id": f"v{i}", "label": "Venue"} for i in range(5)]
            assert loader.load_nodes("Venue", nodes) == 5

    def test_load_edges_calls_session_run(self):
        mock_gdb, mock_session = _make_neo4j_mock()
        with patch("etl.loading.neo4j_loader.GraphDatabase", mock_gdb):
            loader = Neo4jLoader()
            edges = [{"from": "e1", "to": "v1", "type": "EVENT_AT_VENUE"}]
            count = loader.load_edges("EVENT_AT_VENUE", edges)
            mock_session.run.assert_called_once()
            assert count == 1

    def test_run_reads_graph_files_and_loads_all(self, tmp_path):
        mock_gdb, mock_session = _make_neo4j_mock()
        (tmp_path / "nodes_events.json").write_text(
            json.dumps([{"id": "e1", "label": "Event", "title": "Test"}]),
            encoding="utf-8",
        )
        (tmp_path / "nodes_venues.json").write_text(
            json.dumps([{"id": "v1", "label": "Venue", "name": "Palais"}]),
            encoding="utf-8",
        )
        (tmp_path / "edges_event_at_venue.json").write_text(
            json.dumps([{"from": "e1", "to": "v1", "type": "EVENT_AT_VENUE"}]),
            encoding="utf-8",
        )

        with patch("etl.loading.neo4j_loader.GraphDatabase", mock_gdb):
            loader = Neo4jLoader()
            total_nodes, total_edges = loader.run(tmp_path)

        assert total_nodes == 2
        assert total_edges == 1
        assert mock_session.run.call_count == 3  # 2 node queries + 1 edge query

    def test_run_skips_empty_edge_files(self, tmp_path):
        mock_gdb, _mock_session = _make_neo4j_mock()
        (tmp_path / "nodes_venues.json").write_text(
            json.dumps([{"id": "v1", "label": "Venue"}]), encoding="utf-8"
        )
        (tmp_path / "edges_venue_near_parking.json").write_text(
            json.dumps([]), encoding="utf-8"
        )

        with patch("etl.loading.neo4j_loader.GraphDatabase", mock_gdb):
            loader = Neo4jLoader()
            total_nodes, total_edges = loader.run(tmp_path)

        assert total_nodes == 1
        assert total_edges == 0

    def test_close_closes_driver(self):
        loader = Neo4jLoader()
        mock_driver = MagicMock()
        loader._driver = mock_driver
        loader.close()
        mock_driver.close.assert_called_once()
        assert loader._driver is None

    def test_close_is_idempotent_when_no_driver(self):
        loader = Neo4jLoader()
        loader.close()  # Should not raise
