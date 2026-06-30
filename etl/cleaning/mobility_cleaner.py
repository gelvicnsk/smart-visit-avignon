"""Bronze → Silver cleaners for mobility data: parkings, bike stations, bus stops."""

from __future__ import annotations

from typing import TYPE_CHECKING

from etl.cleaning.base_cleaner import BaseCleaner

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

_LAT_MIN, _LAT_MAX = 43.92, 43.96
_LON_MIN, _LON_MAX = 4.79, 4.83


class ParkingsCleaner(BaseCleaner):
    """Cleans raw parkings JSON (Bronze) to the Silver Parquet schema.

    Transformations:
      - Cast lat / lon → DoubleType, capacity → IntegerType, hourly_rate → DoubleType
      - Drop rows outside the Avignon bounding box
      - Drop rows with null id or empty name
      - Fill null capacity with 0, null hourly_rate with 0.0
      - Deduplicate on id
    """

    @property
    def source_name(self) -> str:
        return "parkings"

    def clean(self, df: DataFrame) -> DataFrame:
        from pyspark.sql import functions as F
        from pyspark.sql.types import DoubleType, IntegerType

        return (
            df.withColumn("lat", F.col("lat").cast(DoubleType()))
            .withColumn("lon", F.col("lon").cast(DoubleType()))
            .withColumn("capacity", F.col("capacity").cast(IntegerType()))
            .withColumn("hourly_rate", F.col("hourly_rate").cast(DoubleType()))
            .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
            .filter(
                (F.col("lat") >= _LAT_MIN)
                & (F.col("lat") <= _LAT_MAX)
                & (F.col("lon") >= _LON_MIN)
                & (F.col("lon") <= _LON_MAX)
            )
            .filter(F.col("id").isNotNull())
            .filter(F.length(F.trim(F.col("name"))) > 0)
            .withColumn("name", F.trim(F.col("name")))
            .fillna({"capacity": 0, "hourly_rate": 0.0})
            .dropDuplicates(["id"])
            .select(
                "id",
                "name",
                "lat",
                "lon",
                "capacity",
                "hourly_rate",
                "open_24h",
                "pmr_spots",
                "type",
            )
        )


class BikeStationsCleaner(BaseCleaner):
    """Cleans raw bike stations JSON (Bronze) to the Silver Parquet schema.

    Transformations:
      - Cast lat / lon → DoubleType, capacity / available_bikes → IntegerType
      - Derive available_docks = capacity - available_bikes (when missing)
      - Drop rows outside the Avignon bounding box
      - Drop rows with null id or empty name
      - Deduplicate on id
    """

    @property
    def source_name(self) -> str:
        return "bike_stations"

    def clean(self, df: DataFrame) -> DataFrame:
        from pyspark.sql import functions as F
        from pyspark.sql.types import DoubleType, IntegerType

        return (
            df.withColumn("lat", F.col("lat").cast(DoubleType()))
            .withColumn("lon", F.col("lon").cast(DoubleType()))
            .withColumn("capacity", F.col("capacity").cast(IntegerType()))
            .withColumn("available_bikes", F.col("available_bikes").cast(IntegerType()))
            .withColumn(
                "available_docks",
                F.when(
                    F.col("available_docks").isNull(),
                    F.col("capacity") - F.col("available_bikes"),
                ).otherwise(F.col("available_docks").cast(IntegerType())),
            )
            .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
            .filter(
                (F.col("lat") >= _LAT_MIN)
                & (F.col("lat") <= _LAT_MAX)
                & (F.col("lon") >= _LON_MIN)
                & (F.col("lon") <= _LON_MAX)
            )
            .filter(F.col("id").isNotNull())
            .filter(F.length(F.trim(F.col("name"))) > 0)
            .withColumn("name", F.trim(F.col("name")))
            .dropDuplicates(["id"])
            .select(
                "id",
                "name",
                "lat",
                "lon",
                "capacity",
                "available_bikes",
                "available_docks",
                "network",
                "pmr_accessible",
            )
        )


class TransportCleaner(BaseCleaner):
    """Cleans raw bus stops JSON (Bronze) to the Silver Parquet schema.

    Transformations:
      - Cast lat / lon → DoubleType
      - Drop rows outside the Avignon bounding box
      - Drop rows with null id or empty name
      - Deduplicate on id
    """

    @property
    def source_name(self) -> str:
        return "transport"

    def clean(self, df: DataFrame) -> DataFrame:
        from pyspark.sql import functions as F
        from pyspark.sql.types import DoubleType

        return (
            df.withColumn("lat", F.col("lat").cast(DoubleType()))
            .withColumn("lon", F.col("lon").cast(DoubleType()))
            .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
            .filter(
                (F.col("lat") >= _LAT_MIN)
                & (F.col("lat") <= _LAT_MAX)
                & (F.col("lon") >= _LON_MIN)
                & (F.col("lon") <= _LON_MAX)
            )
            .filter(F.col("id").isNotNull())
            .filter(F.length(F.trim(F.col("name"))) > 0)
            .withColumn("name", F.trim(F.col("name")))
            .dropDuplicates(["id"])
            .select(
                "id",
                "name",
                "lat",
                "lon",
                "lines",
                "pmr_accessible",
                "shelter",
                "real_time_display",
            )
        )
