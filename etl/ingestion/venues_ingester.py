"""Ingester for Festival venues via the Overpass API (OpenStreetMap).

The Overpass query retrieves theatres and arts centres inside the Avignon
bounding box. No API key required — Overpass is a public service.
"""

from __future__ import annotations

from typing import Any

import requests

from etl.ingestion.base_ingester import BaseIngester

# Avignon city bounding box: south, west, north, east
_BBOX = "43.92,4.79,43.96,4.83"

_OVERPASS_QUERY = f"""
[out:json][timeout:30];
(
  node["amenity"="theatre"]({_BBOX});
  way["amenity"="theatre"]({_BBOX});
  node["amenity"="arts_centre"]({_BBOX});
  way["amenity"="arts_centre"]({_BBOX});
  node["amenity"="cinema"]({_BBOX});
  way["building"="civic"]({_BBOX});
);
out center;
"""


def _normalise_element(element: dict[str, Any], index: int) -> dict[str, Any] | None:
    """Convert a single Overpass element to the venue Bronze schema."""
    tags = element.get("tags", {})
    name = tags.get("name", "").strip()
    if not name:
        return None  # skip unnamed elements

    lat = element.get("lat") or element.get("center", {}).get("lat")
    lon = element.get("lon") or element.get("center", {}).get("lon")
    if lat is None or lon is None:
        return None

    wheelchair = tags.get("wheelchair", "")
    return {
        "id": f"osm_{element['id']}",
        "name": name,
        "address": tags.get("addr:street", ""),
        "city": tags.get("addr:city", "Avignon"),
        "postal_code": tags.get("addr:postcode", "84000"),
        "lat": float(lat),
        "lon": float(lon),
        "type": tags.get("amenity", "theatre"),
        "capacity": int(tags.get("capacity") or 0),
        "pmr_accessible": wheelchair in ("yes", "limited"),
        "pmr_spots": 0,
        "phone": tags.get("phone") or tags.get("contact:phone"),
        "website": tags.get("website") or tags.get("contact:website"),
    }


class VenuesIngester(BaseIngester):
    """Fetches performance venues from OpenStreetMap via the Overpass API."""

    @property
    def source_name(self) -> str:
        return "venues"

    def _fetch_from_api(self) -> list[dict[str, Any]]:
        response = requests.post(
            self.config.overpass_api_url,
            data={"data": _OVERPASS_QUERY},
            timeout=self.config.api_timeout_seconds,
        )
        response.raise_for_status()

        elements: list[dict[str, Any]] = response.json().get("elements", [])
        venues = [
            normalised
            for i, el in enumerate(elements)
            if (normalised := _normalise_element(el, i)) is not None
        ]
        return venues

    def _load_from_fixtures(self) -> list[dict[str, Any]]:
        from etl.fixtures.venues_fixture import get_venues

        return get_venues()
