"""Abstract base class for all ETL loaders."""

from __future__ import annotations

from abc import ABC, abstractmethod

from etl.config import ETLConfig, etl_config
from etl.utils.logger import get_etl_logger


class BaseLoader(ABC):
    """Common interface for loaders that write curated data to a database.

    Provides a context manager so connections are always closed:

        with MongoDBLoader() as loader:
            loader.run(...)
    """

    def __init__(self, config: ETLConfig | None = None) -> None:
        self.config = config or etl_config
        self.logger = get_etl_logger(self.__class__.__name__)

    @abstractmethod
    def close(self) -> None:
        """Close any open database connections."""

    def __enter__(self) -> BaseLoader:
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> bool:
        self.close()
        return False
