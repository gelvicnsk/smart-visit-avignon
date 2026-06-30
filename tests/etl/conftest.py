"""Shared fixtures and marks for ETL tests."""

import importlib.util
import subprocess

import pytest


def _spark_available() -> bool:
    """Return True if both the pyspark package and a JVM are reachable."""
    # 1. pyspark must be importable from the active environment
    if importlib.util.find_spec("pyspark") is None:
        return False

    # 2. A JVM must be available (pyspark delegates all execution to Java)
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


_SPARK_AVAILABLE = _spark_available()

# Apply this mark to any test that starts a SparkSession.
requires_spark = pytest.mark.skipif(
    not _SPARK_AVAILABLE,
    reason="PySpark or Java not available — Spark tests skipped",
)


@pytest.fixture(scope="module")
def spark_session():
    """Module-scoped SparkSession in local[*] mode.

    Skips the entire module if PySpark or Java is not available.
    The session is stopped once after all tests in the module have run.
    """
    if not _SPARK_AVAILABLE:
        pytest.skip("PySpark or Java not available — Spark tests skipped")

    from etl.utils.spark import get_spark, stop_spark

    session = get_spark("test_session", force_local=True)
    yield session
    stop_spark(session)
