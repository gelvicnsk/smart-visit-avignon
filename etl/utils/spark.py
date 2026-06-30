"""SparkSession factory for Smart Visit ETL jobs.

Supports two runtime modes:
  • local[*]   — development and CI (default when SPARK_MASTER_HOST is unset)
  • spark://…  — Docker cluster (when SPARK_MASTER_HOST is set)

PySpark is imported lazily so this module can be imported safely on machines
where Java is not installed (e.g. lightweight CI runners that only run config
or logger tests).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog

from etl.config import etl_config

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = structlog.get_logger(__name__)


def get_spark(job_name: str = "smartvisit_etl", *, force_local: bool = False) -> Any:
    """Return a SparkSession, creating one if it does not already exist.

    Args:
        job_name:    Suffix appended to the application name shown in Spark UI.
        force_local: Ignore SPARK_MASTER_HOST and always use local[*].
                     Useful in unit tests to avoid connecting to a cluster.

    Returns:
        An active SparkSession.

    Raises:
        RuntimeError: If PySpark (and therefore Java) is not available.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(
            "PySpark is not installed or Java is not available. "
            "Install PySpark: pip install pyspark==3.5.1"
        ) from exc

    master_url = "local[*]" if force_local else etl_config.spark_master_url

    logger.info("spark.creating_session", master=master_url, job=job_name)

    session: SparkSession = (
        SparkSession.builder.master(master_url)
        .appName(f"smartvisit.{job_name}")
        .config(
            "spark.sql.shuffle.partitions",
            str(etl_config.spark_shuffle_partitions),
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel(etl_config.spark_log_level)
    logger.info("spark.session_ready", master=master_url, version=session.version)

    return session


def stop_spark(session: Any) -> None:
    """Stop the given SparkSession and release all resources."""
    session.stop()
    logger.info("spark.session_stopped")
