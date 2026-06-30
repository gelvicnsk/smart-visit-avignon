"""Tests for etl/utils/spark.py.

All tests in this module are skipped when Java is not available on the host.
In CI without a JVM, these tests skip cleanly — this is expected behaviour.
"""

from tests.etl.conftest import requires_spark


def test_spark_module_importable_without_java():
    """etl.utils.spark must be importable even when Java is not installed."""
    import etl.utils.spark  # noqa: F401 — import-only test


@requires_spark
def test_get_spark_returns_spark_session(spark_session):
    from pyspark.sql import SparkSession

    assert isinstance(spark_session, SparkSession)


@requires_spark
def test_spark_master_is_local(spark_session):
    assert spark_session.sparkContext.master == "local[*]"


@requires_spark
def test_spark_can_create_and_count_dataframe(spark_session):
    df = spark_session.createDataFrame(
        [(1, "Macbeth"), (2, "Hamlet"), (3, "Othello")],
        ["id", "title"],
    )
    assert df.count() == 3


@requires_spark
def test_spark_shuffle_partitions_applied(spark_session):
    partitions = spark_session.conf.get("spark.sql.shuffle.partitions")
    # Must match the ETLConfig default (8) — proves .config() was applied
    from etl.config import etl_config

    assert partitions == str(etl_config.spark_shuffle_partitions)
