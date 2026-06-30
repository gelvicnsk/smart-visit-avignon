"""Ingester for TCRA bus stops via transport.data.gouv.fr (GTFS).

Downloads the TCRA GTFS archive, extracts stops.txt, and filters stops
within the Avignon festival bounding box. No API key required.
"""

from __future__ import annotations

import csv
import io
import zipfile
from typing import Any

import requests

from etl.ingestion.base_ingester import BaseIngester

# Avignon bounding box for stop filtering
_LAT_MIN, _LAT_MAX = 43.92, 43.96
_LON_MIN, _LON_MAX = 4.79, 4.83

# Catalog entry for the TCRA GTFS dataset on transport.data.gouv.fr
_CATALOG_URL = "https://transport.data.gouv.fr/api/datasets"
_GTFS_SEARCH_QUERY = "tcra avignon"


def _parse_stops_from_zip(content: bytes) -> list[dict[str, Any]]:
    """Extract and normalise bus stops from a GTFS ZIP archive."""
    stops: list[dict[str, Any]] = []
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        if "stops.txt" not in zf.namelist():
            raise RuntimeError("stops.txt not found in GTFS archive")

        with zf.open("stops.txt") as raw:
            reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig"))
            for row in reader:
                try:
                    lat = float(row.get("stop_lat", 0) or 0)
                    lon = float(row.get("stop_lon", 0) or 0)
                except ValueError:
                    continue

                # Keep only stops inside the festival bounding box
                if not (_LAT_MIN <= lat <= _LAT_MAX and _LON_MIN <= lon <= _LON_MAX):
                    continue

                stops.append(
                    {
                        "id": f"bus_{row.get('stop_id', '')}",
                        "name": row.get("stop_name", "").strip(),
                        "lat": lat,
                        "lon": lon,
                        "lines": [],
                        "pmr_accessible": row.get("wheelchair_boarding") == "1",
                        "shelter": False,
                        "real_time_display": False,
                    }
                )
    return stops


class TransportIngester(BaseIngester):
    """Downloads TCRA GTFS data and extracts bus stops near festival venues."""

    @property
    def source_name(self) -> str:
        return "transport"

    def _fetch_from_api(self) -> list[dict[str, Any]]:
        # Step 1: search the catalog for the TCRA GTFS dataset
        catalog_response = requests.get(
            _CATALOG_URL,
            params={"q": _GTFS_SEARCH_QUERY},
            timeout=self.config.api_timeout_seconds,
        )
        catalog_response.raise_for_status()
        datasets: list[dict[str, Any]] = catalog_response.json()

        if not datasets:
            raise RuntimeError(
                f"No dataset found for query '{_GTFS_SEARCH_QUERY}' "
                "on transport.data.gouv.fr"
            )

        # Step 2: find the first GTFS resource URL in the top result
        gtfs_url: str | None = None
        for resource in datasets[0].get("resources", []):
            fmt = (resource.get("format") or "").lower()
            if fmt == "gtfs":
                gtfs_url = resource.get("original_url") or resource.get("url")
                break

        if not gtfs_url:
            raise RuntimeError("No GTFS resource URL found in the dataset")

        # Step 3: download the ZIP archive (may be large; allow extra time)
        gtfs_response = requests.get(
            gtfs_url,
            timeout=max(self.config.api_timeout_seconds, 60),
        )
        gtfs_response.raise_for_status()

        return _parse_stops_from_zip(gtfs_response.content)

    def _load_from_fixtures(self) -> list[dict[str, Any]]:
        from etl.fixtures.mobility_fixture import get_bus_stops

        return get_bus_stops()
