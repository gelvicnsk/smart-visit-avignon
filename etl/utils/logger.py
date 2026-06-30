"""Structured logging for ETL jobs.

ETL jobs run as separate processes (via spark-submit or CLI), so they configure
structlog independently from the FastAPI application.
"""

import logging
from typing import Any

import structlog


def configure_etl_logging(level: int = logging.INFO) -> None:
    """Configure structlog for ETL jobs.

    Call once at the top of each job's entry point (jobs/*.py).
    Safe to call multiple times — repeated calls update the configuration.
    """
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        cache_logger_on_first_use=False,
    )


def get_etl_logger(job_name: str) -> Any:
    """Return a logger pre-bound with the job name.

    Usage:
        log = get_etl_logger("events_cleaner")
        log.info("cleaning.start", records=1200)
    """
    return structlog.get_logger().bind(job=job_name)
