"""Invoicing Status DataFrame"""

import polars as pl

from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import INVOICING_ID, REPORT_STATUS_SHEET_NAME


def get_invoice_status_df() -> pl.LazyFrame:
    return load_gsheet_data(INVOICING_ID, REPORT_STATUS_SHEET_NAME)


def clean_invoice_sto_status(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.filter(pl.col("report_type") == "STO").select(
        pl.col("month"),
        pl.col("invoice_number"),
        pl.col("report_type"),
        pl.col("sub_type"),
        pl.col("vessel/client"),
        pl.col("customer"),
        pl.col("start_date"),
        pl.col("end_date"),
        pl.col("status"),
    )


def sto_invoice_status() -> pl.LazyFrame:
    return get_invoice_status_df().pipe(clean_invoice_sto_status)


def clean_invoice_full_oss_status(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.filter(pl.col("sub_type") == "FULL OSS").select(
        pl.col("month"),
        pl.col("invoice_number"),
        pl.col("report_type"),
        pl.col("sub_type"),
        pl.col("vessel/client"),
        pl.col("customer"),
        pl.col("start_date"),
        pl.col("end_date"),
        pl.col("status"),
    )


def full_oss_invoice_status() -> pl.LazyFrame:
    return get_invoice_status_df().pipe(clean_invoice_full_oss_status)


def clean_invoice_basic_oss_status(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.filter(pl.col("sub_type") == "BASIC OSS").select(
        pl.col("month"),
        pl.col("invoice_number"),
        pl.col("report_type"),
        pl.col("sub_type"),
        pl.col("vessel/client"),
        pl.col("customer"),
        pl.col("start_date"),
        pl.col("end_date"),
        pl.col("status"),
    )


def basic_oss_invoice_status() -> pl.LazyFrame:
    return get_invoice_status_df().pipe(clean_invoice_basic_oss_status)


def clean_invoice_cccs_oss_status(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.filter(pl.col("sub_type") == "CCCS OSS").select(
        pl.col("month"),
        pl.col("invoice_number"),
        pl.col("report_type"),
        pl.col("sub_type"),
        pl.col("vessel/client"),
        pl.col("customer"),
        pl.col("start_date"),
        pl.col("end_date"),
        pl.col("status"),
    )


def cccs_oss_invoice_status() -> pl.LazyFrame:
    return get_invoice_status_df().pipe(clean_invoice_cccs_oss_status)


def clean_all_invoice_status() -> pl.LazyFrame:
    return get_invoice_status_df().select(
        pl.col("month"),
        pl.col("invoice_number"),
        pl.col("report_type"),
        pl.col("sub_type"),
        pl.col("vessel/client"),
        pl.col("customer"),
        pl.col("start_date"),
        pl.col("end_date"),
        pl.col("status"),
        pl.col("invoice_amount"),
    )
