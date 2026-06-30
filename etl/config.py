"""ETL pipeline configuration.

Reads from the same .env file as the API so all services share a single
source of truth. Override any value via environment variable.
"""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# Project root — two levels up from this file (etl/config.py → project root)
PROJECT_ROOT = Path(__file__).parent.parent


class ETLConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── Data lake paths (Bronze / Silver / Gold) ──────────────────
    raw_data_path: Path = PROJECT_ROOT / "data" / "raw"
    processed_data_path: Path = PROJECT_ROOT / "data" / "processed"
    curated_data_path: Path = PROJECT_ROOT / "data" / "curated"

    # ── MongoDB ───────────────────────────────────────────────────
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "smartvisit"

    # ── Neo4j ─────────────────────────────────────────────────────
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "password123"

    # ── Apache Spark ──────────────────────────────────────────────
    # Empty string → local[*] mode (dev/test). Set to cluster hostname in Docker.
    spark_master_host: str = ""
    spark_master_port: int = 7077
    spark_log_level: str = "WARN"
    spark_shuffle_partitions: int = 8

    # ── External APIs ─────────────────────────────────────────────
    open_agenda_api_url: str = "https://api.openagenda.com/v2"
    # Festival d'Avignon public UID on Open Agenda
    open_agenda_festival_uid: str = "19611607"
    # Set in .env — empty string triggers automatic fixture fallback
    open_agenda_api_key: str = ""
    overpass_api_url: str = "https://overpass-api.de/api/interpreter"
    osrm_api_url: str = "http://router.project-osrm.org/route/v1/driving/"
    # JCDecaux API key for Vélopop bike stations — empty → fixture fallback
    jcdecaux_api_key: str = ""

    # ── Ingestion behaviour ───────────────────────────────────────
    api_timeout_seconds: int = 30
    api_max_retries: int = 3
    # Automatically fall back to fixture data when an API is unreachable
    use_fixtures_fallback: bool = True

    # ── Graph enrichment ─────────────────────────────────────────
    # Maximum walking distance (metres) to draw a PROCHE_DE relation between venues
    proximity_threshold_m: int = 500
    # Average walking speed in m/min for time estimation
    walking_speed_m_per_min: float = 80.0

    # ── Properties ───────────────────────────────────────────────

    @property
    def spark_master_url(self) -> str:
        """Return the Spark master URL (cluster or local mode)."""
        if self.spark_master_host:
            return f"spark://{self.spark_master_host}:{self.spark_master_port}"
        return "local[*]"

    @property
    def graph_data_path(self) -> Path:
        """Subdirectory of Gold layer containing Neo4j-ready relationship files."""
        return self.curated_data_path / "graph"

    # ── Helpers ──────────────────────────────────────────────────

    def ensure_directories(self) -> None:
        """Create the Bronze / Silver / Gold directories if they do not exist."""
        for path in (
            self.raw_data_path,
            self.processed_data_path,
            self.curated_data_path,
            self.graph_data_path,
        ):
            path.mkdir(parents=True, exist_ok=True)


# Module-level singleton — import this everywhere instead of instantiating locally
etl_config = ETLConfig()
