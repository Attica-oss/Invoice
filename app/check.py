"""Check application module."""

from app.logger import logger


def check_logistics_records() -> None:
    """Check logistics records for consistency."""
    logger.info("Checking logistics records")
    print("Checking logistics records...")
