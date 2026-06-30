"""Unit tests for etl/utils/logger.py."""

import logging

from etl.utils.logger import configure_etl_logging, get_etl_logger


def test_configure_does_not_raise():
    configure_etl_logging()


def test_configure_accepts_custom_log_level():
    configure_etl_logging(level=logging.WARNING)


def test_get_etl_logger_returns_object():
    logger = get_etl_logger("test_job")
    assert logger is not None


def test_logger_info_does_not_raise():
    configure_etl_logging()
    log = get_etl_logger("test_job")
    # Should emit a log line without raising
    log.info("test.message", step="unit_test", count=42)


def test_logger_warning_does_not_raise():
    configure_etl_logging()
    log = get_etl_logger("test_job")
    log.warning("test.warning", reason="intentional")


def test_different_jobs_get_independent_loggers():
    log_a = get_etl_logger("job_a")
    log_b = get_etl_logger("job_b")
    # Bound loggers are separate objects (different job context)
    assert log_a is not log_b
