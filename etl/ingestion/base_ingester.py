"""Abstract base class for all ETL ingesters.

Each ingester implements two methods:
  - _fetch_from_api()   : download data from the external source
  - _load_from_fixtures() : return fixture data as offline fallback

The public interface is:
  - fetch_data()  : calls _fetch_from_api(); on any exception, if
                    use_fixtures_fallback is True, calls _load_from_fixtures()
  - validate_data() : verifies the dataset is a non-empty list of dicts
  - save_raw()    : serialises to JSON in the Bronze layer (data/raw/)
  - ingest()      : fetch → validate → save in one call
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any

from etl.config import ETLConfig, etl_config
from etl.utils.logger import get_etl_logger


class BaseIngester(ABC):
    def __init__(self, config: ETLConfig | None = None) -> None:
        self.config = config or etl_config
        self.logger = get_etl_logger(self.__class__.__name__)

    # ── Abstract interface ────────────────────────────────────────

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Short label used in output file names, e.g. 'events', 'venues'."""

    @abstractmethod
    def _fetch_from_api(self) -> list[dict[str, Any]]:
        """Download raw data from the external API. Raises on any failure."""

    @abstractmethod
    def _load_from_fixtures(self) -> list[dict[str, Any]]:
        """Return in-memory fixture data as an offline fallback."""

    # ── Public pipeline steps ─────────────────────────────────────

    def fetch_data(self) -> list[dict[str, Any]]:
        """Try the API; fall back to fixtures when configured to do so."""
        try:
            data = self._fetch_from_api()
            self.logger.info(
                "ingestion.api_success",
                source=self.source_name,
                count=len(data),
            )
            return data
        except Exception as exc:
            if self.config.use_fixtures_fallback:
                self.logger.warning(
                    "ingestion.api_failed_using_fixtures",
                    source=self.source_name,
                    error=str(exc),
                )
                return self._load_from_fixtures()
            raise

    def validate_data(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Verify the dataset is a non-empty list of dicts.

        Raises:
            ValueError: if *data* is empty or contains non-dict items.
        """
        if not data:
            raise ValueError(
                f"{self.source_name}: empty dataset returned — "
                "check the API response or fixture"
            )
        if not all(isinstance(item, dict) for item in data):
            raise ValueError(f"{self.source_name}: dataset contains non-dict items")
        return data

    def save_raw(self, data: list[dict[str, Any]]) -> Path:
        """Persist *data* as JSON in the Bronze layer (data/raw/).

        Output file: ``data/raw/{source_name}_YYYY-MM-DD.json``

        Returns:
            The path of the written file.
        """
        self.config.raw_data_path.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now().strftime("%Y-%m-%d")
        output_path = self.config.raw_data_path / f"{self.source_name}_{date_str}.json"
        output_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self.logger.info(
            "ingestion.saved",
            source=self.source_name,
            path=str(output_path),
            records=len(data),
        )
        return output_path

    def ingest(self) -> Path:
        """Run the complete ingestion pipeline: fetch → validate → save.

        Returns:
            The path of the written Bronze file.
        """
        self.logger.info("ingestion.start", source=self.source_name)
        data = self.fetch_data()
        data = self.validate_data(data)
        path = self.save_raw(data)
        self.logger.info("ingestion.complete", source=self.source_name, path=str(path))
        return path
