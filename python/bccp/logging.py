import json
import logging
import sys


class JSONFormatter(logging.Formatter):
    """Simple JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "component": getattr(record, "component", record.name.split(".")[-1]),
            "message": record.getMessage(),
        }

        # Add extra fields if present
        if hasattr(record, "extra_fields"):
            log_data.update(record.extra_fields)

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


def setup_logger(
    component: str, level: str = "INFO", use_json: bool = True
) -> logging.Logger:
    """Set up a logger for a component."""
    logger = logging.getLogger(f"bccp.{component}")
    logger.setLevel(getattr(logging, level.upper()))

    # Clear existing handlers
    logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)

    if use_json:
        formatter: logging.Formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s"
        )

    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False

    return logger


def log_with_fields(logger: logging.Logger, level: str, message: str, **fields):
    """Log a message with additional structured fields."""
    record = logger.makeRecord(
        logger.name, getattr(logging, level.upper()), "", 0, message, (), None
    )
    record.extra_fields = fields
    logger.handle(record)
