"""Export DataFrame to Excel sheet with default formatting."""

from pathlib import Path
from typing import Any,cast

import polars as pl
import polars.selectors as cs
import xlsxwriter
from xlsxwriter.worksheet import Worksheet

from .clean_table_name import clean_table_name
from .summary_page import _apply_print_layout, write_summary_sheet

DATAFRAME_SHEET_DEFAULTS = {
    "header_color": "#4472C4",
    "header_font_color": "#FFFFFF",
    "table_style": "None",
    "banded_rows": True,
    "max_column_width": 40,
    "header_row_height": 24,
    "sheet_zoom": 90,
    "dtype_formats": {
        pl.Date: "yyyy-mm-dd",
        pl.Datetime: "yyyy-mm-dd hh:mm",
        pl.Float32: "#,##0.00;[Red]-#,##0.00",
        pl.Float64: "#,##0.00;[Red]-#,##0.00",
        pl.Int32: "#,##0;[Red]-#,##0",
        pl.Int64: "#,##0;[Red]-#,##0",
    },
}


def _unique_sheet_name(name: str, used_names: set[str]) -> str:
    """
    Truncate to Excel's 31-char limit and de-duplicate against already
    used names (case-insensitive, as Excel treats sheet names).
    """
    base = name[:31]
    candidate = base
    counter = 2

    while candidate.lower() in used_names:
        suffix = f" ({counter})"
        candidate = base[: 31 - len(suffix)] + suffix
        counter += 1

    used_names.add(candidate.lower())
    return candidate


def _autofit_columns(
    worksheet: Worksheet,
    df: pl.DataFrame,
    max_width: int,
) -> None:
    """Size each column to its longest value, capped at max_width."""

    for column_index, column_name in enumerate(df.columns):
        longest_value = (
            df[column_name]
            .cast(pl.String)
            .fill_null("")
            .str.len_chars()
            .max()
            )

        width = max(len(column_name), cast(int, longest_value) if longest_value is not None else 0)

        worksheet.set_column(
            column_index,
            column_index,
            min(width + 2, max_width),
        )

def write_dataframe_sheet(
        workbook: xlsxwriter.Workbook,
        worksheet: Worksheet,
        df: pl.DataFrame,
        config: dict[str, Any],
        table_name: str,
    ) -> None:
        """
        Write a polars DataFrame as a styled Excel table.

        Config keys (all optional)
        --------------------------
        header_color, header_font_color : str hex colors for the header row
        table_style : str               Excel table style name ("None" = manual)
        banded_rows : bool
        column_totals : list[str]       defaults to every numeric column
        dtype_formats : dict            polars dtype -> Excel number format
        max_column_width : int
        header_row_height : int
        sheet_zoom : int
        Plus the print keys accepted by _apply_print_layout.
        """

        settings = DATAFRAME_SHEET_DEFAULTS | config

        numeric_columns = df.select(cs.numeric()).columns

        df.write_excel(
            workbook=workbook,
            worksheet=worksheet,
            position="A1",
            table_name=table_name,
            table_style={
                "style": settings["table_style"],
                "banded_rows": settings["banded_rows"],
                "first_column": False,
                "last_column": False,
            },
            header_format={
                "bg_color": settings["header_color"],
                "font_color": settings["header_font_color"],
                "bold": True,
                "align": "center",
                "valign": "vcenter",
                "text_wrap": True,
            },
            column_totals=settings.get("column_totals", numeric_columns),
            dtype_formats=settings["dtype_formats"],
            autofit=True,
            autofilter=True,
            freeze_panes=(1, 0),
            hide_gridlines=True,
            sheet_zoom=settings["sheet_zoom"],
        )

        _autofit_columns(worksheet, df, settings["max_column_width"])
        worksheet.set_row(0, settings["header_row_height"])

        _apply_print_layout(
            worksheet,
            config,
            last_row=df.height + 1,  # header + rows + totals row
            last_col=df.width - 1,
            landscape_default=True,
            set_print_area=False,
        )


def export_dataframes(
    dataframes: dict[str, dict[str, Any]],
    output_path: str | Path,
) -> Path:
    """
    Export a collection of sheets to a single xlsx file.

    Each entry in `dataframes` maps a sheet name to a config dict. A config
    with `"type": "summary"` is rendered by write_summary_sheet; anything
    else must provide a `"df"` key and is rendered by write_dataframe_sheet.
    """

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    used_sheet_names: set[str] = set()

    with xlsxwriter.Workbook(output_path) as workbook:
        for index, (sheet_name, config) in enumerate(
            dataframes.items(),
            start=1,
        ):
            safe_name = _unique_sheet_name(sheet_name, used_sheet_names)
            worksheet = workbook.add_worksheet(safe_name)

            if config.get("type") == "summary":
                write_summary_sheet(
                    workbook=workbook,
                    worksheet=worksheet,
                    config=config,
                )
                continue

            if "df" not in config:
                raise ValueError(
                    f"Sheet '{sheet_name}' has no 'df' key and is not a summary sheet."
                )

            write_dataframe_sheet(
                workbook=workbook,
                worksheet=worksheet,
                df=config["df"],
                config=config,
                table_name=clean_table_name(sheet_name, index),
            )

    return output_path
