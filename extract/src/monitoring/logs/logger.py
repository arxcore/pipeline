import logging
import sys


def configure_logging(
    level: int = logging.INFO,
) -> None:
    """
    Setup centralized logging.
    Only StreamHandler (stdout); no FileHandler (scale later).
    """

    # Reset handlers to avoid duplicates when module is imported repeatedly
    root_logger = logging.getLogger()
    if root_logger.handlers:
        root_logger.handlers.clear()

    root_logger.setLevel(level)

    console_handler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - %(filename)s  - %(lineno)s | %(message)s",  # %(name)s - (funcName)s
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(formatter)

    root_logger.addHandler(console_handler)
