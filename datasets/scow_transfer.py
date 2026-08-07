"""Scow Transfer dataset"""

from __future__ import annotations

import polars as pl
from functools import lru_cache

from scan_google_sheet import scan_google_sheet
from utils.config import TRANSPORT_SHEET_ID,SCOW_TRANSFER_SHEET
from utils import CURRENT_YEAR
# from datasets import unit_price


@lru_cache(maxsize=1)
def scow_transfer() -> pl.LazyFrame:
    # price = unit_price("Forklift Scow Handling")

    # is_special = pl.col("day_name").is_in(SPECIAL_DAYS)



    return (
        scan_google_sheet(
            sheet_id=TRANSPORT_SHEET_ID, sheet_name=SCOW_TRANSFER_SHEET
        )
        .filter(pl.col("date").dt.year().eq(CURRENT_YEAR))
        .select(
            pl.col("date").days.add_day_name(), #
            pl.col("date"),
            pl.col("container_number"),
            pl.col("customer"),
            pl.col("movement_type"),
            pl.col("driver"),
            pl.col("from"),
            pl.col("time_out"),
            pl.col("destination"),
            pl.col("time_in"),
            pl.col("status"),
            pl.col("remarks"),
            pl.col("num_of_scows")
        )
    )
