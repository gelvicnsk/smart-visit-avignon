"""Silver → Gold enrichment for events: join with venues to add geo/PMR context."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from etl.config import ETLConfig, etl_config
from etl.utils.logger import get_etl_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class EventsEnricher:
    """Join events Silver with venues Silver to produce curated enriched events.

    Reads:
      ``data/processed/events/``   (Parquet, output of EventsCleaner)
      ``data/processed/venues/``   (Parquet, output of VenuesCleaner)

    Writes:
      ``data/curated/events_enriched/``  (Parquet, snappy)

    Added columns: venue_name, venue_address, venue_lat, venue_lon,
                   venue_pmr_accessible, venue_capacity.
    """

    def __init__(self, config: ETLConfig | None = None) -> None:
        self.config = config or etl_config
        self.logger = get_etl_logger(self.__class__.__name__)

    # ── Read helpers ────────────────────────────────────────────────────────────

    def read_events_silver(self, spark: SparkSession) -> DataFrame:
        path = self.config.processed_data_path / "events"
        if not path.exists():
            raise FileNotFoundError(
                f"Events Silver not found at {path}. Run EventsCleaner first."
            )
        return spark.read.parquet(str(path))

    def read_venues_silver(self, spark: SparkSession) -> DataFrame:
        path = self.config.processed_data_path / "venues"
        if not path.exists():
            raise FileNotFoundError(
                f"Venues Silver not found at {path}. Run VenuesCleaner first."
            )
        return spark.read.parquet(str(path))

    # ── Transformation ──────────────────────────────────────────────────────────

    def enrich(self, events_df: DataFrame, venues_df: DataFrame) -> DataFrame:
        """Left-join events with venues to add venue context columns."""
        from pyspark.sql import functions as F

        venue_slim = venues_df.select(
            F.col("id").alias("_v_id"),
            F.col("name").alias("venue_name"),
            F.col("address").alias("venue_address"),
            F.col("lat").alias("venue_lat"),
            F.col("lon").alias("venue_lon"),
            F.col("pmr_accessible").alias("venue_pmr_accessible"),
            F.col("capacity").alias("venue_capacity"),
        )

        return events_df.join(
            venue_slim, events_df["venue_id"] == venue_slim["_v_id"], how="left"
        ).drop("_v_id")

    # ── Write / run ─────────────────────────────────────────────────────────────

    def write_curated(self, df: DataFrame) -> Path:
        """Write the enriched events DataFrame to the Gold layer."""
        output_path = self.config.curated_data_path / "events_enriched"
        self.config.curated_data_path.mkdir(parents=True, exist_ok=True)
        count = df.count()
        df.write.mode("overwrite").option("compression", "snappy").parquet(
            str(output_path)
        )
        self.logger.info(
            "events_enricher.wrote_curated",
            path=str(output_path),
            records=count,
        )
        return output_path

    def run(self, spark: SparkSession) -> Path:
        """Execute the full Silver → Gold enrichment pipeline."""
        self.logger.info("events_enricher.start")
        events_df = self.read_events_silver(spark)
        venues_df = self.read_venues_silver(spark)
        enriched_df = self.enrich(events_df, venues_df)
        output_path = self.write_curated(enriched_df)
        self.logger.info("events_enricher.complete", output=str(output_path))
        return output_path
