"""Bronze → Silver cleaner for festival venues."""

from __future__ import annotations

from typing import TYPE_CHECKING

from etl.cleaning.base_cleaner import BaseCleaner

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# Avignon festival bounding box
_LAT_MIN, _LAT_MAX = 43.92, 43.96
_LON_MIN, _LON_MAX = 4.79, 4.83

_SILVER_COLUMNS = [
    "id",
    "name",
    "address",
    "city",
    "postal_code",
    "lat",
    "lon",
    "type",
    "capacity",
    "pmr_accessible",
    "pmr_spots",
    "phone",
    "website",
]


class VenuesCleaner(BaseCleaner):
    """Cleans raw venues JSON (Bronze) to the Silver Parquet schema.

    Transformations:
      - Cast lat / lon → DoubleType
      - Cast capacity / pmr_spots → IntegerType
      - Drop rows outside the Avignon bounding box (lat 43.92-43.96, lon 4.79-4.83)
      - Drop rows with null id or empty name
      - Default city → 'Avignon', postal_code → '84000' when null
      - Fill null capacity / pmr_spots with 0
      - Deduplicate on id
      - Select and order Silver columns
    """

    @property
    def source_name(self) -> str:
        return "venues"

    def clean(self, df: DataFrame) -> DataFrame:
        from pyspark.sql import functions as F
        from pyspark.sql.types import DoubleType, IntegerType

        return (
            df
            # ── Type casts ────────────────────────────────────
            .withColumn("lat", F.col("lat").cast(DoubleType()))
            .withColumn("lon", F.col("lon").cast(DoubleType()))
            .withColumn("capacity", F.col("capacity").cast(IntegerType()))
            .withColumn("pmr_spots", F.col("pmr_spots").cast(IntegerType()))
            # ── GPS validity (drops out-of-bound venues) ──────
            .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
            .filter(
                (F.col("lat") >= _LAT_MIN)
                & (F.col("lat") <= _LAT_MAX)
                & (F.col("lon") >= _LON_MIN)
                & (F.col("lon") <= _LON_MAX)
            )
            # ── Required-field filters ────────────────────────
            .filter(F.col("id").isNotNull())
            .filter(F.length(F.trim(F.col("name"))) > 0)
            # ── Text normalisation ────────────────────────────
            .withColumn("name", F.trim(F.col("name")))
            # ── Default values for optional fields ────────────
            .withColumn(
                "city",
                F.when(F.col("city").isNull(), F.lit("Avignon")).otherwise(
                    F.col("city")
                ),
            )
            .withColumn(
                "postal_code",
                F.when(F.col("postal_code").isNull(), F.lit("84000")).otherwise(
                    F.col("postal_code")
                ),
            )
            .fillna({"capacity": 0, "pmr_spots": 0})
            # ── Deduplication ─────────────────────────────────
            .dropDuplicates(["id"])
            # ── Final Silver projection ───────────────────────
            .select(*_SILVER_COLUMNS)
        )
