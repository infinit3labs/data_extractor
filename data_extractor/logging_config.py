"""
Enterprise-grade logging configuration with structured logging support.
Provides JSON logging, multiple handlers, and configurable log levels.
"""

import json
import logging
import logging.config
import os
import sys
from datetime import datetime
from typing import Any, Dict, Optional


class StructuredFormatter(logging.Formatter):
    """
    Custom formatter that outputs structured JSON logs for better parsing and analysis.
    """

    def __init__(self, include_extra: bool = True):
        super().__init__()
        self.include_extra = include_extra

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON."""
        log_data: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "thread": record.threadName,
            "process": record.process,
        }

        # Add exception information if present
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": (
                    self.formatException(record.exc_info) if record.exc_info else None
                ),
            }

        # Add extra fields if configured
        if self.include_extra:
            extra_fields = {
                key: value
                for key, value in record.__dict__.items()
                if key
                not in {
                    "name",
                    "msg",
                    "args",
                    "levelname",
                    "levelno",
                    "pathname",
                    "filename",
                    "module",
                    "lineno",
                    "funcName",
                    "created",
                    "msecs",
                    "relativeCreated",
                    "thread",
                    "threadName",
                    "processName",
                    "process",
                    "getMessage",
                    "exc_info",
                    "exc_text",
                    "stack_info",
                    "message",
                }
            }
            if extra_fields:
                log_data["extra"] = extra_fields

        return json.dumps(log_data, default=str, separators=(",", ":"))


class ThreadSafeFileHandler(logging.FileHandler):
    """
    Thread-safe file handler that ensures proper log file rotation and handling.
    """

    def __init__(self, filename: str, mode: str = "a", encoding: Optional[str] = None):
        # Ensure log directory exists
        log_dir = os.path.dirname(filename)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        super().__init__(filename, mode, encoding)


def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    structured_logging: bool = False,
    include_console: bool = True,
    logger_name: Optional[str] = None,
) -> logging.Logger:
    """
    Set up enterprise-grade logging configuration.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file (optional)
        structured_logging: Whether to use structured JSON logging
        include_console: Whether to include console output
        logger_name: Name of the logger (defaults to root logger)

    Returns:
        Configured logger instance
    """

    # Validate log level
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    # Create logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(numeric_level)

    # Clear existing handlers
    logger.handlers.clear()

    # Configure formatters
    if structured_logging:
        formatter = StructuredFormatter()
        console_formatter = StructuredFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(threadName)s - %(levelname)s - %(message)s"
        )
        console_formatter = logging.Formatter(
            "%(asctime)s - %(threadName)s - %(levelname)s - %(message)s"
        )

    # Add console handler if requested
    if include_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(numeric_level)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    # Add file handler if log file specified
    if log_file:
        file_handler = ThreadSafeFileHandler(log_file)
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Add error handler for critical errors
    if log_file:
        error_log_file = log_file.replace(".log", "_errors.log")
        error_handler = ThreadSafeFileHandler(error_log_file)
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        logger.addHandler(error_handler)

    return logger


def get_databricks_logger(name: str = "data_extractor") -> logging.Logger:
    """
    Get a logger configured for Databricks environment.

    Args:
        name: Logger name

    Returns:
        Configured logger for Databricks
    """
    return setup_logging(
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        structured_logging=True,
        include_console=True,
        logger_name=name,
    )


def get_production_logger(
    name: str = "data_extractor", log_dir: str = "/var/log/data_extractor"
) -> logging.Logger:
    """
    Get a logger configured for production environment.

    Args:
        name: Logger name
        log_dir: Directory for log files

    Returns:
        Configured logger for production
    """
    log_file = os.path.join(log_dir, f"{name}.log")

    return setup_logging(
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        log_file=log_file,
        structured_logging=True,
        include_console=False,
        logger_name=name,
    )


def configure_logging_from_config(config: Dict[str, Any]) -> None:
    """
    Configure logging from a dictionary configuration.

    Args:
        config: Logging configuration dictionary
    """
    logging.config.dictConfig(config)


# Default logging configuration for different environments
DEVELOPMENT_LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s - %(name)s - %(threadName)s - %(levelname)s - %(message)s"
        },
        "detailed": {
            "format": "%(asctime)s - %(name)s - %(threadName)s - %(levelname)s - %(module)s - %(funcName)s:%(lineno)d - %(message)s"
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "standard",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "class": "logging.FileHandler",
            "level": "DEBUG",
            "formatter": "detailed",
            "filename": "data_extractor_dev.log",
            "mode": "a",
        },
    },
    "root": {"level": "INFO", "handlers": ["console", "file"]},
}

PRODUCTION_LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"json": {"()": "data_extractor.logging_config.StructuredFormatter"}},
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "WARNING",
            "formatter": "json",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "()": "data_extractor.logging_config.ThreadSafeFileHandler",
            "level": "INFO",
            "formatter": "json",
            "filename": "/var/log/data_extractor/app.log",
        },
        "error_file": {
            "()": "data_extractor.logging_config.ThreadSafeFileHandler",
            "level": "ERROR",
            "formatter": "json",
            "filename": "/var/log/data_extractor/errors.log",
        },
    },
    "root": {"level": "INFO", "handlers": ["console", "file", "error_file"]},
}
