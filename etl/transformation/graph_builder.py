"""Build Neo4j-ready node and edge JSON files from curated data.

Pure-Python implementation — no PySpark dependency.

Output layout (all files under data/curated/graph/):
  nodes_events.json          — Event nodes
  nodes_venues.json          — Venue nodes
  nodes_parkings.json        — Parking nodes
  nodes_bike_stations.json   — BikeStation nodes
  nodes_bus_stops.json       — BusStop nodes
  edges_event_at_venue.json      — EVENT_AT_VENUE relationships
  edges_venue_near_parking.json  — VENUE_NEAR_PARKING relationships
  edges_venue_near_bike.json     — VENUE_NEAR_BIKE_STATION relationships
  edges_venue_near_bus.json      — VENUE_NEAR_BUS_STOP relationships
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from etl.config import ETLConfig, etl_config
from etl.transformation.geo_enricher import GeoEnricher
from etl.utils.logger import get_etl_logger


class GraphBuilder:
    """Build Neo4j node/edge JSON from venue and mobility fixture or curated data.

    Usage::

        from etl.fixtures import get_events, get_venues, get_parkings, ...
        gb = GraphBuilder()
        graph_path = gb.run(events, venues, parkings, bike_stations, bus_stops)
    """

    def __init__(self, config: ETLConfig | None = None) -> None:
        self.config = config or etl_config
        self.logger = get_etl_logger(self.__class__.__name__)
        self._geo = GeoEnricher(config=config)

    # ── Node builders ──────────────────────────────────────────────────────────

    @staticmethod
    def _event_node(e: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": e["id"],
            "label": "Event",
            "title": e.get("title"),
            "category": e.get("category"),
            "date_start": e.get("date_start"),
            "date_end": e.get("date_end"),
            "price_min": e.get("price_min"),
            "price_max": e.get("price_max"),
            "pmr_accessible": e.get("pmr_accessible"),
        }

    @staticmethod
    def _venue_node(v: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": v["id"],
            "label": "Venue",
            "name": v.get("name"),
            "address": v.get("address"),
            "lat": v.get("lat"),
            "lon": v.get("lon"),
            "capacity": v.get("capacity"),
            "pmr_accessible": v.get("pmr_accessible"),
        }

    @staticmethod
    def _parking_node(p: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": p["id"],
            "label": "Parking",
            "name": p.get("name"),
            "lat": p.get("lat"),
            "lon": p.get("lon"),
            "capacity": p.get("capacity"),
            "hourly_rate": p.get("hourly_rate"),
            "open_24h": p.get("open_24h"),
        }

    @staticmethod
    def _bike_node(b: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": b["id"],
            "label": "BikeStation",
            "name": b.get("name"),
            "lat": b.get("lat"),
            "lon": b.get("lon"),
            "available_bikes": b.get("available_bikes"),
            "capacity": b.get("capacity"),
        }

    @staticmethod
    def _bus_node(s: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": s["id"],
            "label": "BusStop",
            "name": s.get("name"),
            "lat": s.get("lat"),
            "lon": s.get("lon"),
            "lines": s.get("lines", []),
            "pmr_accessible": s.get("pmr_accessible"),
        }

    # ── Public pipeline ────────────────────────────────────────────────────────

    def build_nodes(
        self,
        events: list[dict[str, Any]],
        venues: list[dict[str, Any]],
        parkings: list[dict[str, Any]],
        bike_stations: list[dict[str, Any]],
        bus_stops: list[dict[str, Any]],
    ) -> dict[str, list[dict[str, Any]]]:
        return {
            "events": [self._event_node(e) for e in events],
            "venues": [self._venue_node(v) for v in venues],
            "parkings": [self._parking_node(p) for p in parkings],
            "bike_stations": [self._bike_node(b) for b in bike_stations],
            "bus_stops": [self._bus_node(s) for s in bus_stops],
        }

    def build_edges(
        self,
        events: list[dict[str, Any]],
        venues: list[dict[str, Any]],
        parkings: list[dict[str, Any]],
        bike_stations: list[dict[str, Any]],
        bus_stops: list[dict[str, Any]],
    ) -> dict[str, list[dict[str, Any]]]:
        proximity = self._geo.enrich_venues(venues, parkings, bike_stations, bus_stops)

        event_at_venue = [
            {"from": e["id"], "to": e["venue_id"], "type": "EVENT_AT_VENUE"}
            for e in events
            if e.get("venue_id")
        ]

        venue_near_parking: list[dict[str, Any]] = []
        venue_near_bike: list[dict[str, Any]] = []
        venue_near_bus: list[dict[str, Any]] = []

        for vid, prox in proximity.items():
            for pid in prox.nearby_parkings:
                venue_near_parking.append(
                    {"from": vid, "to": pid, "type": "VENUE_NEAR_PARKING"}
                )
            for bid in prox.nearby_bike_stations:
                venue_near_bike.append(
                    {"from": vid, "to": bid, "type": "VENUE_NEAR_BIKE_STATION"}
                )
            for sid in prox.nearby_bus_stops:
                venue_near_bus.append(
                    {"from": vid, "to": sid, "type": "VENUE_NEAR_BUS_STOP"}
                )

        return {
            "event_at_venue": event_at_venue,
            "venue_near_parking": venue_near_parking,
            "venue_near_bike": venue_near_bike,
            "venue_near_bus": venue_near_bus,
        }

    def save(
        self,
        nodes: dict[str, list[dict[str, Any]]],
        edges: dict[str, list[dict[str, Any]]],
    ) -> Path:
        """Persist nodes and edges as JSON files under the graph curated path."""
        graph_path = self.config.graph_data_path
        graph_path.mkdir(parents=True, exist_ok=True)

        for name, data in nodes.items():
            (graph_path / f"nodes_{name}.json").write_text(
                json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
            )

        for name, data in edges.items():
            (graph_path / f"edges_{name}.json").write_text(
                json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
            )

        self.logger.info(
            "graph_builder.saved",
            graph_path=str(graph_path),
            node_files=len(nodes),
            edge_files=len(edges),
        )
        return graph_path

    def run(
        self,
        events: list[dict[str, Any]],
        venues: list[dict[str, Any]],
        parkings: list[dict[str, Any]],
        bike_stations: list[dict[str, Any]],
        bus_stops: list[dict[str, Any]],
    ) -> Path:
        """Build and save the full graph (nodes + edges)."""
        self.logger.info(
            "graph_builder.start",
            events=len(events),
            venues=len(venues),
        )
        nodes = self.build_nodes(events, venues, parkings, bike_stations, bus_stops)
        edges = self.build_edges(events, venues, parkings, bike_stations, bus_stops)
        graph_path = self.save(nodes, edges)
        self.logger.info(
            "graph_builder.complete",
            events=len(nodes["events"]),
            venues=len(nodes["venues"]),
            event_at_venue_edges=len(edges["event_at_venue"]),
            venue_near_parking_edges=len(edges["venue_near_parking"]),
        )
        return graph_path
