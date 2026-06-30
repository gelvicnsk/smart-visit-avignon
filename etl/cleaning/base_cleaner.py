"""Abstract base class for all ETL Bronze → Silver cleaners."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING

from etl.config import ETLConfig, etl_config
from etl.utils.logger import get_etl_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class BaseCleaner(ABC):
    """Transforms raw Bronze JSON into cleaned Silver Parquet via Apache Spark.

    Subclass protocol:
      1. Set ``source_name`` to match the ingester prefix (e.g. "events").
      2. Implement ``clean(df)`` — receive a raw DataFrame, return a cleaned one.

    The inherited ``run(spark)`` handles the full pipeline:
    ``read_bronze → clean → write_silver``.
    """

    def __init__(self, config: ETLConfig | None = None) -> None:
        self.config = config or etl_config
        self.logger = get_etl_logger(self.__class__.__name__)

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Bronze file prefix, e.g. 'events', 'venues', 'parkings'."""

    @abstractmethod
    def clean(self, df: DataFrame) -> DataFrame:
        """Apply all cleaning transformations to the raw Bronze DataFrame."""

    # ── Pipeline steps ─────────────────────────────────────────────────────────

    def read_bronze(self, spark: SparkSession) -> DataFrame:
        """Read the most recent Bronze JSON file for this source.

        Raises:
            FileNotFoundError: if no Bronze file exists yet for ``source_name``.
        """
        bronze_files = sorted(
            self.config.raw_data_path.glob(f"{self.source_name}_*.json")
        )
        if not bronze_files:
            raise FileNotFoundError(
                f"No Bronze file found for '{self.source_name}' "
                f"in {self.config.raw_data_path}. "
                "Run the ingestion step first."
            )
        latest = bronze_files[-1]
        self.logger.info(
            "cleaner.reading_bronze",
            source=self.source_name,
            file=latest.name,
        )
        return spark.read.option("multiLine", "true").json(str(latest))

    def write_silver(self, df: DataFrame) -> Path:
        """Write the cleaned DataFrame as snappy-compressed Parquet.

        Output: ``data/processed/{source_name}/``

        Returns:
            Path to the Parquet directory.
        """
        output_path = self.config.processed_data_path / self.source_name
        self.config.processed_data_path.mkdir(parents=True, exist_ok=True)
        count = df.count()
        df.write.mode("overwrite").option("compression", "snappy").parquet(
            str(output_path)
        )
        self.logger.info(
            "cleaner.wrote_silver",
            source=self.source_name,
            path=str(output_path),
            records=count,
        )
        return output_path

    def run(self, spark: SparkSession) -> Path:
        """Execute the full Bronze → Silver cleaning pipeline."""
        self.logger.info("cleaner.start", source=self.source_name)
        df_raw = self.read_bronze(spark)
        df_clean = self.clean(df_raw)
        output_path = self.write_silver(df_clean)
        self.logger.info(
            "cleaner.complete",
            source=self.source_name,
            output=str(output_path),
        )
        return output_path
