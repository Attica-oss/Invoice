""" "Forklift records for scow_handling"""

from __future__ import annotations

import polars as pl

from datasets.miscellaneous import miscellaneous_lf


def forklift_scow_handling_lf() -> pl.LazyFrame:
    return (
        miscellaneous_lf()
        .filter(pl.col("operation_type").str.contains("Bin Dispatch"))
        .select(
            pl.col("day_name"),
            pl.col("date"),
            pl.col("operation_type"),
            (pl.col("bins_in") + pl.col("bins_out").abs()).alias("number_of_scows"),
        )
    )
