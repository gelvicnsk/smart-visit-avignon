"""Ingester for Festival d'Avignon shows via the Open Agenda v2 API.

API docs: https://openagenda.com/docs
Requires OPEN_AGENDA_API_KEY in .env (or fixture fallback is used automatically).
"""

from __future__ import annotations

from typing import Any

import requests

from etl.ingestion.base_ingester import BaseIngester


class EventsIngester(BaseIngester):
    """Fetches all Festival d'Avignon events from the Open Agenda v2 API.

    Paginates automatically using the ``after`` cursor until all events are
    retrieved. Falls back to fixture data when the API key is absent or the
    request fails.
    """

    @property
    def source_name(self) -> str:
        return "events"

    def _fetch_from_api(self) -> list[dict[str, Any]]:
        if not self.config.open_agenda_api_key:
            raise RuntimeError(
                "OPEN_AGENDA_API_KEY not set — set it in .env to use the live API"
            )

        url = (
            f"{self.config.open_agenda_api_url}"
            f"/agendas/{self.config.open_agenda_festival_uid}/events"
        )
        events: list[dict[str, Any]] = []
        after: str | None = None

        while True:
            params: dict[str, Any] = {
                "key": self.config.open_agenda_api_key,
                "size": 100,
            }
            if after:
                params["after"] = after

            response = requests.get(
                url,
                params=params,
                timeout=self.config.api_timeout_seconds,
            )
            response.raise_for_status()
            payload: dict[str, Any] = response.json()

            batch: list[dict[str, Any]] = payload.get("events", [])
            events.extend(batch)

            after = payload.get("after")
            total = payload.get("total", 0)
            if not after or len(events) >= total:
                break

        return events

    def _load_from_fixtures(self) -> list[dict[str, Any]]:
        from etl.fixtures.events_fixture import get_events

        return get_events()
