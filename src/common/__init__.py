"""Common platform utilities and session factory."""

from .logger import get_logger
from .spark_session import get_spark_session, stop_spark_session

__all__ = [
    "get_logger",
    "get_spark_session",
    "stop_spark_session",
]
