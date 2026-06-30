"""Ingester for Avignon public parkings via data.gouv.fr.

The dataset is a GeoJSON file published by Grand Avignon on the national open
data portal. No API key required.

Dataset reference: https://www.data.gouv.fr/fr/datasets/parkings-avignon/
"""

from __future__ import annotations

from typing import Any

import requests

from etl.ingestion.base_ingester import BaseIngester

# Static URL of the GeoJSON resource from data.gouv.fr (Grand Avignon dataset)
_DATASET_URL = (
    "https://www.data.gouv.fr/api/1/datasets/"
    "parkings-de-stationnement-en-ouvrage-grand-avignon/"
)


def _normalise_feature(feature: dict[str, Any], index: int) -> dict[str, Any]:
    """Convert a GeoJSON Feature to the parking Bronze schema."""
    props = feature.get("properties", {})
    coords = feature.get("geometry", {}).get("coordinates", [None, None])
    lon = float(coords[0]) if coords[0] is not None else None
    lat = float(coords[1]) if coords[1] is not None else None

    return {
        "id": f"parking_{index + 1:02d}",
        "name": props.get("nom") or props.get("name") or props.get("NOM", ""),
        "address": props.get("adresse") or props.get("ADRESSE", ""),
        "lat": lat,
        "lon": lon,
        "capacity": int(props.get("nb_places") or props.get("CAPACITE") or 0),
        "pmr_spots": int(props.get("nb_pmr") or props.get("PMR") or 0),
        "hourly_rate_eur": float(props.get("tarif_h") or props.get("TARIF_H") or 0.0),
        "open_24h": bool(props.get("ouvert_24h") or props.get("H24") or False),
        "operator": props.get("gestionnaire") or props.get("GESTIONNAIRE") or "",
        "electric_charging": bool(props.get("recharge_electrique") or False),
    }


class ParkingsIngester(BaseIngester):
    """Fetches public parking data from the Grand Avignon open-data portal."""

    @property
    def source_name(self) -> str:
        return "parkings"

    def _fetch_from_api(self) -> list[dict[str, Any]]:
        # Step 1: resolve the actual resource URL from the dataset metadata
        meta_response = requests.get(
            _DATASET_URL,
            timeout=self.config.api_timeout_seconds,
            headers={"Accept": "application/json"},
        )
        meta_response.raise_for_status()
        dataset = meta_response.json()

        geojson_url: str | None = None
        for resource in dataset.get("resources", []):
            fmt = (resource.get("format") or "").lower()
            if fmt in ("geojson", "json"):
                geojson_url = resource.get("url") or resource.get("latest")
                break

        if not geojson_url:
            raise RuntimeError("No GeoJSON resource found in the dataset metadata")

        # Step 2: download the GeoJSON
        data_response = requests.get(
            geojson_url,
            timeout=self.config.api_timeout_seconds,
        )
        data_response.raise_for_status()
        geojson: dict[str, Any] = data_response.json()

        features: list[dict[str, Any]] = geojson.get("features", [])
        return [_normalise_feature(f, i) for i, f in enumerate(features)]

    def _load_from_fixtures(self) -> list[dict[str, Any]]:
        from etl.fixtures.mobility_fixture import get_parkings

        return get_parkings()
