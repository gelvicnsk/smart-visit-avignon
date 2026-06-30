"""Bronze → Silver cleaner for Festival d'Avignon events."""

from __future__ import annotations

from typing import TYPE_CHECKING

from etl.cleaning.base_cleaner import BaseCleaner

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# Silver columns in final order
_SILVER_COLUMNS = [
    "id",
    "external_id",
    "title",
    "category",
    "venue_id",
    "date_start",
    "date_end",
    "time_start",
    "duration_minutes",
    "price_min",
    "price_max",
    "remaining_seats",
    "language",
    "pmr_accessible",
    "surtitled",
]


class EventsCleaner(BaseCleaner):
    """Cleans raw events JSON (Bronze) to the Silver Parquet schema.

    Transformations:
      - Cast date_start / date_end → DateType
      - Cast price_min / price_max → DoubleType
      - Cast duration_minutes / remaining_seats → IntegerType
      - Strip whitespace from title and category
      - Drop rows with null id, empty title, null date_start, null venue_id
      - Fill null prices and seats with 0
      - Deduplicate on id
      - Select and order Silver columns
    """

    @property
    def source_name(self) -> str:
        return "events"

    def clean(self, df: DataFrame) -> DataFrame:
        from pyspark.sql import functions as F
        from pyspark.sql.types import DoubleType, IntegerType

        return (
            df
            # ── Type casts ────────────────────────────────────
            .withColumn("date_start", F.to_date(F.col("date_start"), "yyyy-MM-dd"))
            .withColumn("date_end", F.to_date(F.col("date_end"), "yyyy-MM-dd"))
            .withColumn("price_min", F.col("price_min").cast(DoubleType()))
            .withColumn("price_max", F.col("price_max").cast(DoubleType()))
            .withColumn(
                "duration_minutes", F.col("duration_minutes").cast(IntegerType())
            )
            .withColumn("remaining_seats", F.col("remaining_seats").cast(IntegerType()))
            # ── Text normalisation ────────────────────────────
            .withColumn("title", F.trim(F.col("title")))
            .withColumn("category", F.trim(F.col("category")))
            # ── Required-field filters ────────────────────────
            .filter(F.col("id").isNotNull())
            .filter(F.length(F.trim(F.col("title"))) > 0)
            .filter(F.col("date_start").isNotNull())
            .filter(F.col("venue_id").isNotNull())
            # ── Fill optional nulls ───────────────────────────
            .fillna({"price_min": 0.0, "price_max": 0.0, "remaining_seats": 0})
            # ── Deduplication ─────────────────────────────────
            .dropDuplicates(["id"])
            # ── Final Silver projection ───────────────────────
            .select(*_SILVER_COLUMNS)
        )
