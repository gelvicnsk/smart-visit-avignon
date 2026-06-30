from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central configuration loaded from environment variables / .env file.

    All values have safe defaults so the app starts without a .env file
    (useful in tests and CI). Override in production via environment variables.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # Ignore unknown env vars (Docker, CI, etc.)
    )

    # ── Application ───────────────────────────────────────────────
    app_name: str = "Smart Visit Avignon"
    app_version: str = "1.0.0"
    app_env: str = "development"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_reload: bool = False
    secret_key: str = "change-me-in-production"

    # ── MongoDB ───────────────────────────────────────────────────
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "smartvisit"

    # ── Neo4j ─────────────────────────────────────────────────────
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "password123"

    # ── Redis ─────────────────────────────────────────────────────
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0

    # ── Spark ─────────────────────────────────────────────────────
    spark_master_host: str = "spark-master"
    spark_master_port: int = 7077

    # ── OSRM ──────────────────────────────────────────────────────
    osrm_api_url: str = "http://router.project-osrm.org/route/v1/driving/"

    @property
    def is_development(self) -> bool:
        return self.app_env == "development"

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"


settings = Settings()
