"""Make the dataset (LazyFrame) from a google sheet id and sheet names"""

import logging


import polars as pl
from polars_result import Result
from read_google_sheet import read_google_sheet

from data_source.excel_file_path import ExcelFiles


# Configure logging
logging.basicConfig(level=logging.ERROR)

# Excel file path type
# type ExcelFiles = tuple[Path,str]


def load_gsheet_data(sheet_id: str, sheet_name: str) -> Result[pl.LazyFrame, Exception]:
    """
    Loads a Google Sheet as a Polars LazyFrame.

    Args:
        sheet_id (str): The ID of the Google Sheet.
        sheet_name (str): The name of the sheet to load.

    Returns:
        Result: A LazyFrame containing the google sheet data, or an Exception if an error occurred.
    Raises:
        Exception: If there is an error loading the Google Sheet.
    """

    return  read_google_sheet(sheet_id=sheet_id, sheet_name=sheet_name)


def load_excel(file_path: ExcelFiles) -> pl.LazyFrame:
    """
    Loads an Excel file as a Polars LazyFrame.

    Args:
        file_path (ExcelFiles): The path to the Excel file.
    Returns:

        pl.LazyFrame: A LazyFrame containing the Excel data.
    """
    file, sheet = file_path.value
    return pl.read_excel(file, sheet_name=sheet).lazy()
