"""Geographic proximity enrichment for venue data.

Pure-Python implementation — no PySpark dependency.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

from etl.config import ETLConfig, etl_config
from etl.utils.logger import get_etl_logger

_EARTH_RADIUS_M = 6_371_000.0


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return the great-circle distance in metres between two GPS coordinates."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return _EARTH_RADIUS_M * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


@dataclass
class VenueProximity:
    """Proximity snapshot for a single venue."""

    venue_id: str
    nearby_parkings: list[str] = field(default_factory=list)
    nearby_bike_stations: list[str] = field(default_factory=list)
    nearby_bus_stops: list[str] = field(default_factory=list)


class GeoEnricher:
    """Compute proximity relationships between venues and mobility POIs.

    Uses the Haversine formula; compares every venue against every mobility
    POI and keeps those within ``config.proximity_threshold_m`` metres.
    """

    def __init__(self, config: ETLConfig | None = None) -> None:
        self.config = config or etl_config
        self.logger = get_etl_logger(self.__class__.__name__)

    # ── Public API ─────────────────────────────────────────────────────────────

    def enrich_venues(
        self,
        venues: list[dict[str, Any]],
        parkings: list[dict[str, Any]],
        bike_stations: list[dict[str, Any]],
        bus_stops: list[dict[str, Any]],
    ) -> dict[str, VenueProximity]:
        """Return a mapping from venue_id → VenueProximity.

        Each VenueProximity lists the IDs of mobility POIs within the
        configured proximity threshold.
        """
        threshold = self.config.proximity_threshold_m
        result: dict[str, VenueProximity] = {}

        for venue in venues:
            vid = venue["id"]
            vlat = float(venue["lat"])
            vlon = float(venue["lon"])

            prox = VenueProximity(
                venue_id=vid,
                nearby_parkings=self._nearby_ids(vlat, vlon, parkings, threshold),
                nearby_bike_stations=self._nearby_ids(
                    vlat, vlon, bike_stations, threshold
                ),
                nearby_bus_stops=self._nearby_ids(vlat, vlon, bus_stops, threshold),
            )
            result[vid] = prox

        self.logger.info(
            "geo_enricher.complete",
            venues=len(result),
            threshold_m=threshold,
        )
        return result

    # ── Internal helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _nearby_ids(
        vlat: float,
        vlon: float,
        pois: list[dict[str, Any]],
        threshold_m: int,
    ) -> list[str]:
        return [
            poi["id"]
            for poi in pois
            if haversine_m(vlat, vlon, float(poi["lat"]), float(poi["lon"]))
            <= threshold_m
        ]
