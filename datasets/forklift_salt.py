""""Forklift records for salt operations"""

from __future__ import annotations
import polars as pl

from utils import duration_to_hhmm
from datasets import load_salt




def forklift_salt(salt: pl.LazyFrame | None = None) -> pl.LazyFrame:
    """Forklift report for salt operations, durations formatted as HH:MM.

    Reuses the Duration buckets computed in :func:`load_salt` instead of
    re-deriving them, so report and billing always agree.
    """
    lf = salt if salt is not None else load_salt()

    result = lf.filter(
        # ne_missing keeps rows with a null operation_type — plain ``ne``
        # evaluated to null and silently dropped them.
        pl.col("operation_type").ne_missing("Loading @ Zone 14")
    ).select(
        pl.col("day_name").alias("day"),
        "date",
        "vessel",
        "customer",
        "start_time",
        "end_time",
        pl.format("Salt Loading ({} Tons)", pl.col("tonnage").cast(pl.Int64)).alias(
            "purpose"
        ),
        "total_duration",
        pl.col("normal").alias("normal_hour_services"),
        pl.col("overtime_150").alias("overtime_for_normal_services"),
        pl.col("overtime_200").alias("overtime_for_extended_services"),
    )

    return duration_to_hhmm(
        result,
        [
            "total_duration",
            "normal_hour_services",
            "overtime_for_normal_services",
            "overtime_for_extended_services",
        ],
    )

FORKLIFT_DATASET = forklift_salt()
