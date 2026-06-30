"""Neo4j loader — imports graph nodes and edges from curated JSON files.

Node labels:   Event, Venue, Parking, BikeStation, BusStop
Relationships: EVENT_AT_VENUE, VENUE_NEAR_PARKING,
               VENUE_NEAR_BIKE_STATION, VENUE_NEAR_BUS_STOP

Uses UNWIND for efficient batch Cypher execution.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from neo4j import GraphDatabase

from etl.config import ETLConfig
from etl.loading.base_loader import BaseLoader

# Maps GraphBuilder node file name stems to Neo4j labels
_LABEL_MAP: dict[str, str] = {
    "events": "Event",
    "venues": "Venue",
    "parkings": "Parking",
    "bike_stations": "BikeStation",
    "bus_stops": "BusStop",
}


def _merge_nodes_query(label: str) -> str:
    return (
        f"UNWIND $batch AS item MERGE (n:{label} {{id: item.id}}) SET n += item.props"
    )


def _merge_edges_query(edge_type: str) -> str:
    return (
        f"UNWIND $batch AS item "
        f"MATCH (a {{id: item.from_id}}) "
        f"MATCH (b {{id: item.to_id}}) "
        f"MERGE (a)-[r:{edge_type}]->(b)"
    )


def _build_node_batch(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert raw node dicts to {id, props} UNWIND batch items."""
    return [
        {
            "id": node["id"],
            "props": {
                k: v
                for k, v in node.items()
                if k not in ("id", "label") and v is not None
            },
        }
        for node in nodes
    ]


def _build_edge_batch(edges: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert raw edge dicts to {from_id, to_id} UNWIND batch items."""
    return [{"from_id": e["from"], "to_id": e["to"]} for e in edges]


class Neo4jLoader(BaseLoader):
    """Loads graph nodes and edges into Neo4j via the Bolt driver.

    Connection is lazy — established on the first call to
    :meth:`load_nodes` or :meth:`load_edges`.

    Usage::

        with Neo4jLoader() as loader:
            loader.run(graph_path)
    """

    def __init__(self, config: ETLConfig | None = None) -> None:
        super().__init__(config)
        self._driver = None

    # ── Connection management ───────────────────────────────────────────────────

    @property
    def driver(self):
        if self._driver is None:
            self._driver = GraphDatabase.driver(
                self.config.neo4j_uri,
                auth=(self.config.neo4j_user, self.config.neo4j_password),
            )
        return self._driver

    def close(self) -> None:
        if self._driver is not None:
            self._driver.close()
            self._driver = None

    # ── Public API ──────────────────────────────────────────────────────────────

    def load_nodes(self, label: str, nodes: list[dict[str, Any]]) -> int:
        """MERGE *nodes* into Neo4j with the given *label*.

        Returns:
            Number of nodes processed.
        """
        if not nodes:
            return 0
        batch = _build_node_batch(nodes)
        with self.driver.session() as session:
            session.run(_merge_nodes_query(label), batch=batch)
        self.logger.info("neo4j.loaded_nodes", label=label, count=len(nodes))
        return len(nodes)

    def load_edges(self, edge_type: str, edges: list[dict[str, Any]]) -> int:
        """MERGE *edges* as *edge_type* relationships in Neo4j.

        Returns:
            Number of edges processed.
        """
        if not edges:
            return 0
        batch = _build_edge_batch(edges)
        with self.driver.session() as session:
            session.run(_merge_edges_query(edge_type), batch=batch)
        self.logger.info("neo4j.loaded_edges", type=edge_type, count=len(edges))
        return len(edges)

    def run(self, graph_path: Path | str) -> tuple[int, int]:
        """Load all node and edge JSON files from *graph_path* into Neo4j.

        Nodes are loaded before edges so MATCH in edge queries always finds them.

        Returns:
            Tuple of (total_nodes_loaded, total_edges_loaded).
        """
        graph_path = Path(graph_path)
        self.logger.info("neo4j_loader.start", graph_path=str(graph_path))
        total_nodes = 0
        total_edges = 0

        # Load nodes first
        for node_file in sorted(graph_path.glob("nodes_*.json")):
            key = node_file.stem.removeprefix("nodes_")
            label = _LABEL_MAP.get(key, key.title())
            data: list[dict[str, Any]] = json.loads(
                node_file.read_text(encoding="utf-8")
            )
            total_nodes += self.load_nodes(label, data)

        # Load edges after nodes
        for edge_file in sorted(graph_path.glob("edges_*.json")):
            data = json.loads(edge_file.read_text(encoding="utf-8"))
            # Read edge_type from the first record's 'type' field
            edge_type = data[0]["type"] if data else edge_file.stem.upper()
            total_edges += self.load_edges(edge_type, data)

        self.logger.info(
            "neo4j_loader.complete",
            total_nodes=total_nodes,
            total_edges=total_edges,
        )
        return total_nodes, total_edges
