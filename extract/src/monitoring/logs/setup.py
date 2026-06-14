import logging
from monitoring.logs.logger import configure_logging

logger = logging.getLogger(__name__)


def resolve_log_level(level_str: str) -> int:
    """Resolve Log Level String to Log Level Int"""
    level_mapping = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }
    key: str = level_str.lower().strip()
    if key not in level_mapping:
        raise ValueError(f"Unknown log level: {level_str!r}")
    return level_mapping[key]


def level_name(log_level: int) -> str:
    """Get Log Level Name from Log Level Int"""
    level_names = {
        logging.DEBUG: "DEBUG",
        logging.INFO: "INFO",
        logging.WARNING: "WARNING",
        logging.ERROR: "ERROR",
        logging.CRITICAL: "CRITICAL",
    }
    result = level_names.get(log_level)
    if result is None:
        raise ValueError(f"Unknown log level (int): {log_level}")

    return result


def apply_log_level(log_level: int) -> None:
    """Apply Log Level to Logger and Print Log Level Name"""
    log_name: str = level_name(log_level)

    configure_logging(log_level)
    logger.info("Set Logging Level Name %s ", log_name)
