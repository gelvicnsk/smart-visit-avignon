"""Mobility fixtures: 15 parkings, 12 bike stations, 8 bus stops.

All coordinates are accurate for Avignon (inside the city walls and close environs).
Schemas match the Bronze layer format produced by real open-data sources.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

# ── 15 Parkings ───────────────────────────────────────────────────────────────

PARKINGS: list[dict[str, Any]] = [
    {
        "id": "parking_01",
        "name": "Parking des Italiens",
        "address": "Rue des Italiens, 84000 Avignon",
        "lat": 43.9493,
        "lon": 4.8035,
        "capacity": 600,
        "pmr_spots": 12,
        "hourly_rate_eur": 1.50,
        "open_24h": True,
        "operator": "Q-Park",
        "electric_charging": True,
    },
    {
        "id": "parking_02",
        "name": "Parking du Palais",
        "address": "Place du Palais des Papes, 84000 Avignon",
        "lat": 43.9517,
        "lon": 4.8072,
        "capacity": 400,
        "pmr_spots": 10,
        "hourly_rate_eur": 2.00,
        "open_24h": True,
        "operator": "Effia",
        "electric_charging": False,
    },
    {
        "id": "parking_03",
        "name": "Parking de la Balance",
        "address": "Rue de la Balance, 84000 Avignon",
        "lat": 43.9501,
        "lon": 4.8053,
        "capacity": 350,
        "pmr_spots": 8,
        "hourly_rate_eur": 1.80,
        "open_24h": False,
        "operator": "Indigo",
        "electric_charging": True,
    },
    {
        "id": "parking_04",
        "name": "Parking Hôtel de Ville",
        "address": "Place de l'Horloge, 84000 Avignon",
        "lat": 43.9490,
        "lon": 4.8048,
        "capacity": 200,
        "pmr_spots": 6,
        "hourly_rate_eur": 2.20,
        "open_24h": False,
        "operator": "Ville d'Avignon",
        "electric_charging": False,
    },
    {
        "id": "parking_05",
        "name": "Parking des Rochers",
        "address": "Rue du Rempart du Rhône, 84000 Avignon",
        "lat": 43.9528,
        "lon": 4.8019,
        "capacity": 250,
        "pmr_spots": 8,
        "hourly_rate_eur": 1.20,
        "open_24h": True,
        "operator": "SAEMES",
        "electric_charging": True,
    },
    {
        "id": "parking_06",
        "name": "Parking Gare TGV Avignon Centre",
        "address": "Boulevard Saint-Roch, 84000 Avignon",
        "lat": 43.9329,
        "lon": 4.8068,
        "capacity": 500,
        "pmr_spots": 15,
        "hourly_rate_eur": 1.00,
        "open_24h": True,
        "operator": "SNCF Gares & Connexions",
        "electric_charging": True,
    },
    {
        "id": "parking_07",
        "name": "Parking Chamfleury",
        "address": "Rue du Chamfleury, 84000 Avignon",
        "lat": 43.9382,
        "lon": 4.8095,
        "capacity": 700,
        "pmr_spots": 20,
        "hourly_rate_eur": 0.80,
        "open_24h": True,
        "operator": "Indigo",
        "electric_charging": True,
    },
    {
        "id": "parking_08",
        "name": "Parking de la Banasterie",
        "address": "Rue de la Banasterie, 84000 Avignon",
        "lat": 43.9503,
        "lon": 4.8085,
        "capacity": 150,
        "pmr_spots": 4,
        "hourly_rate_eur": 1.60,
        "open_24h": False,
        "operator": "Ville d'Avignon",
        "electric_charging": False,
    },
    {
        "id": "parking_09",
        "name": "Parking du Pigonnet",
        "address": "Chemin du Pigonnet, 84000 Avignon",
        "lat": 43.9421,
        "lon": 4.8023,
        "capacity": 300,
        "pmr_spots": 8,
        "hourly_rate_eur": 1.00,
        "open_24h": False,
        "operator": "Effia",
        "electric_charging": False,
    },
    {
        "id": "parking_10",
        "name": "Parking Porte Magnanen",
        "address": "Boulevard du Roi René, 84000 Avignon",
        "lat": 43.9449,
        "lon": 4.8097,
        "capacity": 180,
        "pmr_spots": 5,
        "hourly_rate_eur": 1.20,
        "open_24h": False,
        "operator": "Ville d'Avignon",
        "electric_charging": False,
    },
    {
        "id": "parking_11",
        "name": "Parking Molière",
        "address": "Rue Molière, 84000 Avignon",
        "lat": 43.9477,
        "lon": 4.8064,
        "capacity": 200,
        "pmr_spots": 6,
        "hourly_rate_eur": 1.40,
        "open_24h": False,
        "operator": "Indigo",
        "electric_charging": True,
    },
    {
        "id": "parking_12",
        "name": "Parking des Carmes",
        "address": "Place des Carmes, 84000 Avignon",
        "lat": 43.9487,
        "lon": 4.8075,
        "capacity": 250,
        "pmr_spots": 8,
        "hourly_rate_eur": 1.60,
        "open_24h": False,
        "operator": "Q-Park",
        "electric_charging": False,
    },
    {
        "id": "parking_13",
        "name": "Parking de la République",
        "address": "Place de la République, 84000 Avignon",
        "lat": 43.9476,
        "lon": 4.8045,
        "capacity": 300,
        "pmr_spots": 8,
        "hourly_rate_eur": 1.80,
        "open_24h": True,
        "operator": "SAEMES",
        "electric_charging": True,
    },
    {
        "id": "parking_14",
        "name": "Parking du Théâtre Municipal",
        "address": "Rue du Portail Matheron, 84000 Avignon",
        "lat": 43.9494,
        "lon": 4.8037,
        "capacity": 180,
        "pmr_spots": 5,
        "hourly_rate_eur": 1.60,
        "open_24h": False,
        "operator": "Ville d'Avignon",
        "electric_charging": False,
    },
    {
        "id": "parking_15",
        "name": "Parking Piot — P+R Festival",
        "address": "Route de Lyon, 84000 Avignon",
        "lat": 43.9601,
        "lon": 4.8101,
        "capacity": 1200,
        "pmr_spots": 30,
        "hourly_rate_eur": 0.0,
        "open_24h": True,
        "operator": "Grand Avignon",
        "electric_charging": True,
    },
]

# ── 12 Bike stations ───────────────────────────────────────────────────────────

BIKE_STATIONS: list[dict[str, Any]] = [
    {
        "id": "velo_01",
        "name": "Horloge / Hôtel de Ville",
        "lat": 43.9491,
        "lon": 4.8053,
        "capacity": 20,
        "available_bikes": 9,
        "available_docks": 11,
        "pmr_accessible": True,
        "network": "Vélopop",
    },
    {
        "id": "velo_02",
        "name": "Palais des Papes",
        "lat": 43.9508,
        "lon": 4.8066,
        "capacity": 15,
        "available_bikes": 6,
        "available_docks": 9,
        "pmr_accessible": True,
        "network": "Vélopop",
    },
    {
        "id": "velo_03",
        "name": "Gare Avignon Centre",
        "lat": 43.9329,
        "lon": 4.8068,
        "capacity": 25,
        "available_bikes": 14,
        "available_docks": 11,
        "pmr_accessible": True,
        "network": "Vélopop",
    },
    {
        "id": "velo_04",
        "name": "Les Halles — Place Pie",
        "lat": 43.9489,
        "lon": 4.8052,
        "capacity": 20,
        "available_bikes": 3,
        "available_docks": 17,
        "pmr_accessible": True,
        "network": "Vélopop",
    },
    {
        "id": "velo_05",
        "name": "Place de la République",
        "lat": 43.9476,
        "lon": 4.8045,
        "capacity": 15,
        "available_bikes": 8,
        "available_docks": 7,
        "pmr_accessible": True,
        "network": "Vélopop",
    },
    {
        "id": "velo_06",
        "name": "Place des Carmes",
        "lat": 43.9487,
        "lon": 4.8075,
        "capacity": 10,
        "available_bikes": 5,
        "available_docks": 5,
        "pmr_accessible": False,
        "network": "Vélopop",
    },
    {
        "id": "velo_07",
        "name": "Rocher des Doms",
        "lat": 43.9525,
        "lon": 4.8056,
        "capacity": 12,
        "available_bikes": 2,
        "available_docks": 10,
        "pmr_accessible": False,
        "network": "Vélopop",
    },
    {
        "id": "velo_08",
        "name": "Carreterie / Saint-Lazare",
        "lat": 43.9450,
        "lon": 4.8060,
        "capacity": 15,
        "available_bikes": 11,
        "available_docks": 4,
        "pmr_accessible": True,
        "network": "Vélopop",
    },
    {
        "id": "velo_09",
        "name": "Rue de la Banasterie",
        "lat": 43.9503,
        "lon": 4.8085,
        "capacity": 10,
        "available_bikes": 4,
        "available_docks": 6,
        "pmr_accessible": False,
        "network": "Vélopop",
    },
    {
        "id": "velo_10",
        "name": "Promenade de l'Oulle",
        "lat": 43.9532,
        "lon": 4.8031,
        "capacity": 15,
        "available_bikes": 7,
        "available_docks": 8,
        "pmr_accessible": True,
        "network": "Vélopop",
    },
    {
        "id": "velo_11",
        "name": "Chamfleury / La FabricA",
        "lat": 43.9389,
        "lon": 4.8073,
        "capacity": 20,
        "available_bikes": 10,
        "available_docks": 10,
        "pmr_accessible": True,
        "network": "Vélopop",
    },
    {
        "id": "velo_12",
        "name": "Piotière / Route de Lyon",
        "lat": 43.9558,
        "lon": 4.8078,
        "capacity": 12,
        "available_bikes": 5,
        "available_docks": 7,
        "pmr_accessible": True,
        "network": "Vélopop",
    },
]

# ── 8 Bus stops (TCRA réseau) ──────────────────────────────────────────────────

BUS_STOPS: list[dict[str, Any]] = [
    {
        "id": "bus_01",
        "name": "Palais des Papes",
        "lat": 43.9510,
        "lon": 4.8068,
        "lines": ["1", "11"],
        "pmr_accessible": True,
        "shelter": True,
        "real_time_display": True,
    },
    {
        "id": "bus_02",
        "name": "Horloge",
        "lat": 43.9493,
        "lon": 4.8054,
        "lines": ["1", "2", "4"],
        "pmr_accessible": True,
        "shelter": True,
        "real_time_display": True,
    },
    {
        "id": "bus_03",
        "name": "République",
        "lat": 43.9478,
        "lon": 4.8046,
        "lines": ["2", "3", "5"],
        "pmr_accessible": True,
        "shelter": True,
        "real_time_display": True,
    },
    {
        "id": "bus_04",
        "name": "Gare Avignon Centre",
        "lat": 43.9332,
        "lon": 4.8070,
        "lines": ["1", "2", "3", "4", "5", "6", "11"],
        "pmr_accessible": True,
        "shelter": True,
        "real_time_display": True,
    },
    {
        "id": "bus_05",
        "name": "Place Pie / Halles",
        "lat": 43.9492,
        "lon": 4.8056,
        "lines": ["3", "6"],
        "pmr_accessible": True,
        "shelter": False,
        "real_time_display": True,
    },
    {
        "id": "bus_06",
        "name": "Carmes",
        "lat": 43.9489,
        "lon": 4.8077,
        "lines": ["4", "6"],
        "pmr_accessible": False,
        "shelter": True,
        "real_time_display": False,
    },
    {
        "id": "bus_07",
        "name": "Chamfleury",
        "lat": 43.9385,
        "lon": 4.8091,
        "lines": ["5"],
        "pmr_accessible": True,
        "shelter": True,
        "real_time_display": True,
    },
    {
        "id": "bus_08",
        "name": "Piot — P+R",
        "lat": 43.9599,
        "lon": 4.8098,
        "lines": ["11"],
        "pmr_accessible": True,
        "shelter": True,
        "real_time_display": True,
    },
]


# ── Public accessors ───────────────────────────────────────────────────────────


def get_parkings() -> list[dict[str, Any]]:
    """Return the 15 Avignon parking fixtures."""
    return PARKINGS


def get_bike_stations() -> list[dict[str, Any]]:
    """Return the 12 Vélopop bike station fixtures."""
    return BIKE_STATIONS


def get_bus_stops() -> list[dict[str, Any]]:
    """Return the 8 TCRA bus stop fixtures."""
    return BUS_STOPS


def save_mobility(output_dir: Path) -> None:
    """Write all mobility fixtures to JSON and CSV in *output_dir*."""
    output_dir.mkdir(parents=True, exist_ok=True)

    for name, dataset in (
        ("parkings_fixture", PARKINGS),
        ("bike_stations_fixture", BIKE_STATIONS),
        ("bus_stops_fixture", BUS_STOPS),
    ):
        json_path = output_dir / f"{name}.json"
        json_path.write_text(
            json.dumps(dataset, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        csv_path = output_dir / f"{name}.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(dataset[0].keys()))
            writer.writeheader()
            writer.writerows(dataset)
