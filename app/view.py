""""View Logistics Datasets to add in Google Sheets"""
from __future__ import annotations
import polars as pl
from data_source.make_dataset import load_excel 
from data_source.excel_file_path import ExcelFiles



def forklift_logistics_dataset() -> pl.DataFrame:
    """
    Loads and returns a forklift logistics dataset from an Excel file.

    Args:
        file_path (ExcelFiles): The path to the Excel file and sheet name.

    Returns:
        pl.DataFrame: A DataFrame containing the forklift logistics data.
    """
    forklift_data = load_excel(ExcelFiles.FORKLIFT_USAGE).select(
        pl.col("Date of Service").alias("date"),
        pl.col("Driver").str.to_titlecase().alias("driver"),
        pl.col("Forklift No.").alias("forklift_number"),
        pl.col("Time Out").alias("start_time"),
        pl.col("Time In").alias("end_time"),
        pl.col("Vessel/Client").alias("customer"),
        pl.col("Purpose").alias("remarks"),
    )
    return forklift_data


def view_data(dataset: pl.LazyFrame) -> pl.DataFrame:
    """
    Loads and returns data from an Excel file.

    Args:
        dataset (pl.LazyFrame): The dataset to be viewed.

    Returns:
        pl.DataFrame: A DataFrame containing the data.
    """
    data = dataset.collect()
    return data