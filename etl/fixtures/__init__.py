"""Synthetic fixture data for the Festival d'Avignon ETL pipeline.

Used as a fallback when external APIs (Open Agenda, Overpass, data.gouv.fr)
are unreachable, and as the primary data source in offline tests.
"""

from etl.fixtures.events_fixture import get_events, save_events
from etl.fixtures.mobility_fixture import (
    get_bike_stations,
    get_bus_stops,
    get_parkings,
    save_mobility,
)
from etl.fixtures.venues_fixture import get_venues, save_venues

__all__ = [
    "get_bike_stations",
    "get_bus_stops",
    "get_events",
    "get_parkings",
    "get_venues",
    "save_events",
    "save_mobility",
    "save_venues",
]
