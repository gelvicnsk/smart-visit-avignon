"""Ingester for Avignon Vélopop bike stations via the JCDecaux API.

The Vélopop network (Grand Avignon) is operated by JCDecaux. The public API
requires a free API key from https://developer.jcdecaux.com.

Set JCDECAUX_API_KEY in .env — without it the ingester falls back to fixtures.
"""

from __future__ import annotations

from typing import Any

import requests

from etl.ingestion.base_ingester import BaseIngester

_JCDECAUX_URL = "https://api.jcdecaux.com/vls/v1/stations"
_CONTRACT = "avignon"


def _normalise_station(station: dict[str, Any], index: int) -> dict[str, Any]:
    """Convert a JCDecaux station record to the bike-station Bronze schema."""
    pos = station.get("position") or {}
    number = station.get("number", index + 1)
    capacity = station.get("bike_stands", 0)
    available_bikes = station.get("available_bikes", 0)

    return {
        "id": f"velo_{number:02d}",
        "name": station.get("name", ""),
        "lat": pos.get("lat"),
        "lon": pos.get("lng"),
        "capacity": capacity,
        "available_bikes": available_bikes,
        "available_docks": capacity - available_bikes,
        "pmr_accessible": False,
        "network": "Vélopop",
    }


class BikeStationsIngester(BaseIngester):
    """Fetches Vélopop bike-station availability from the JCDecaux public API."""

    @property
    def source_name(self) -> str:
        return "bike_stations"

    def _fetch_from_api(self) -> list[dict[str, Any]]:
        if not self.config.jcdecaux_api_key:
            raise RuntimeError(
                "JCDECAUX_API_KEY not set — set it in .env to use the live API"
            )

        response = requests.get(
            _JCDECAUX_URL,
            params={
                "contract": _CONTRACT,
                "apiKey": self.config.jcdecaux_api_key,
            },
            timeout=self.config.api_timeout_seconds,
        )
        response.raise_for_status()
        stations: list[dict[str, Any]] = response.json()
        return [_normalise_station(s, i) for i, s in enumerate(stations)]

    def _load_from_fixtures(self) -> list[dict[str, Any]]:
        from etl.fixtures.mobility_fixture import get_bike_stations

        return get_bike_stations()
