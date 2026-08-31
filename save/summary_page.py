
from typing import Any

import polars as pl
import xlsxwriter
from xlsxwriter.worksheet import Worksheet

# =====================================================
# Summary sheet writer (reusable)
# =====================================================

SUMMARY_DEFAULT_LAYOUT = {
    # zero-based column indices
    "label_col": 1,        # B — labels / section names
    "value_col": 2,        # C — values / service names
    "price_col": 3,        # D — $ symbol; unit price lands in D+1, total in D+3
    "column_widths": {
        "A:A": 4, "B:B": 30, "C:C": 26,
        "D:D": 4, "E:E": 11, "F:F": 4, "G:G": 13,
    },
}

SUMMARY_DEFAULT_TEXT = {
    "date_range_label": "Date Range:",
    "client_label": "Client:",
    "services_heading": "Services",
    "unit_price_heading": "Unit Price ($)",
    "total_heading": "Total ($)",
    "currency_symbol": "$",
    "empty_display": "-",
}





def build_summary_formats(
    workbook: xlsxwriter.Workbook,
) -> dict[str, Any]:
    """Create all formats used by the summary sheet, once per workbook call."""

    navy = "#000080"
    dark_blue = "#002060"
    header_fill = "#17365D"
    currency_num_format = "#,##0.00;[Red]-#,##0.00"


    base = {"valign": "vcenter", "font_size": 11}


    return {
            "header_label": workbook.add_format(
                base | {"align": "right", "bold": True}
            ),
            "header_value": workbook.add_format(base | {"align": "center"}),
            "client_box": workbook.add_format(
                base
                | {
                    "align": "center",
                    "bold": True,
                    "font_color": "#FFFFFF",
                    "bg_color": header_fill,
                    "text_wrap": True,
                    "border": 1,
                }
            ),
            "label": workbook.add_format(base | {"align": "right"}),
            "label_italic": workbook.add_format(
                base | {"align": "right", "italic": True}
            ),
            "value": workbook.add_format(
                base | {"align": "right", "bold": True, "font_color": navy}
            ),
            "unit": workbook.add_format(base | {"align": "left", "italic": True}),
            "services_heading": workbook.add_format(
                base | {"align": "center", "font_color": navy}
            ),
            "section": workbook.add_format(base | {"align": "left", "bold": True}),
            "service_name": workbook.add_format(
                base | {"align": "right", "italic": True}
            ),
            "price_header": workbook.add_format(
                base
                | {
                    "align": "center",
                    "bold": True,
                    "font_color": "#FFFFFF",
                    "bg_color": header_fill,
                    "border": 1,
                }
            ),
            "currency_symbol": workbook.add_format(
                base | {"align": "center", "left": 1, "top": 1, "bottom": 1}
            ),
            "currency_value": workbook.add_format(
                base
                | {
                    "align": "right",
                    "num_format": currency_num_format,
                    "font_color": dark_blue,
                    "top": 1,
                    "right": 1,
                    "bottom": 1,
                }
            ),
            "grand_total_symbol": workbook.add_format(
                {
                    "align": "center",
                    "bold": True,
                    "top": 1,
                    "bottom": 6,
                    "font_size": 11,
                }
            ),
            "grand_total_value": workbook.add_format(
                {
                    "align": "right",
                    "bold": True,
                    "num_format": currency_num_format,
                    "font_color": dark_blue,
                    "top": 1,
                    "bottom": 6,
                    "font_size": 11,
                }
            ),
        }


def _write_currency_pair(
    worksheet: Worksheet,
    fmt: dict[str, Any],
    text: dict[str, str],
    row: int,
    symbol_col: int,
    amount: float | None,
    *,
    blank: bool = False,
) -> None:
    """
    Write a bordered [$ | amount] cell pair.

    blank=True writes empty bordered cells (used for section spacer rows).
    None or zero amounts display the configured empty marker.
    """
    symbol = "" if blank else text["currency_symbol"]
    worksheet.write(row, symbol_col, symbol, fmt["currency_symbol"])

    if blank:
        worksheet.write(row, symbol_col + 1, "", fmt["currency_value"])
    elif amount is None or amount == 0:
        worksheet.write(
            row, symbol_col + 1, text["empty_display"], fmt["currency_value"]
        )
    else:
        worksheet.write_number(
            row, symbol_col + 1, float(amount), fmt["currency_value"]
        )


def _write_page_header(
    worksheet: Worksheet,
    fmt: dict[str, Any],
    text: dict[str, str],
    layout: dict[str, Any],
    config: dict[str, Any],
    start_row: int,
) -> int:
    """Write the Date Range / Client block. Returns the next free row."""

    label_col = layout["label_col"]
    value_col = layout["value_col"]
    row = start_row

    worksheet.write(row, label_col, text["date_range_label"], fmt["header_label"])
    worksheet.write(row, value_col, config.get("date_range", ""), fmt["header_value"])
    row += 1

    worksheet.set_row(row, config.get("client_row_height", 30))
    worksheet.write(row, label_col, text["client_label"], fmt["header_label"])
    worksheet.write(row, value_col, config.get("client", ""), fmt["client_box"])

    return row + 3  # gap before the next block


def _write_metric_rows(
    workbook: xlsxwriter.Workbook,
    worksheet: Worksheet,
    fmt: dict[str, Any],
    text: dict[str, str],
    layout: dict[str, Any],
    summary_rows: list[dict[str, Any] | None],
    start_row: int,
) -> int:
    """Write the label / value / unit metric block. Returns the next free row."""

    label_col = layout["label_col"]
    value_col = layout["value_col"]
    row = start_row

    for item in summary_rows:
        if item is None:
            row += 1
            continue

        value = item.get("value")
        dash_if_zero = item.get("dash_if_zero", True)

        label_format = (
            fmt["label_italic"] if item.get("italic", False) else fmt["label"]
        )
        worksheet.write(row, label_col, item["label"], label_format)

        if value is None or (dash_if_zero and value == 0):
            worksheet.write(row, value_col, text["empty_display"], fmt["value"])
        elif isinstance(value, (int, float)):
            value_format = fmt["value"]
            if number_format := item.get("number_format"):
                value_format = workbook.add_format(
                    {
                        "align": "right",
                        "valign": "vcenter",
                        "bold": True,
                        "font_color": "#000080",
                        "font_size": 11,
                        "num_format": number_format,
                    }
                )
            worksheet.write_number(row, value_col, float(value), value_format)
        else:
            worksheet.write(row, value_col, value, fmt["value"])

        worksheet.write(row, value_col + 1, item.get("unit", ""), fmt["unit"])
        row += 1

    return row


def _write_service_table(
    worksheet: Worksheet,
    fmt: dict[str, Any],
    text: dict[str, str],
    layout: dict[str, Any],
    service_df: pl.DataFrame,
    start_row: int,
) -> int:
    """
    Write the section / service / unit price / total table with a grand
    total. Returns the grand total row.
    """

    required_columns = {"section", "service", "unit_price", "total"}
    missing_columns = required_columns.difference(service_df.columns)
    if missing_columns:
        raise ValueError(
            f"service_df is missing columns: {sorted(missing_columns)}"
        )

    label_col = layout["label_col"]
    value_col = layout["value_col"]
    price_col = layout["price_col"]
    total_col = price_col + 2

    # ----- header row -----
    row = start_row
    worksheet.write(row, label_col, text["services_heading"], fmt["services_heading"])
    worksheet.merge_range(
        row, price_col, row, price_col + 1,
        text["unit_price_heading"], fmt["price_header"],
    )
    worksheet.merge_range(
        row, total_col, row, total_col + 1,
        text["total_heading"], fmt["price_header"],
    )
    row += 1

    # ----- body -----
    previous_section: str | None = None

    for record in service_df.iter_rows(named=True):
        section = record.get("section")

        if previous_section is not None and section != previous_section:
            # Spacer row: keep the price box borders continuous.
            _write_currency_pair(
                worksheet, fmt, text, row, price_col, None, blank=True
            )
            _write_currency_pair(
                worksheet, fmt, text, row, total_col, None, blank=True
            )
            row += 1

        section_display = section if section != previous_section else ""
        previous_section = section

        worksheet.write(row, label_col, section_display, fmt["section"])
        worksheet.write(
            row, value_col, record.get("service") or "", fmt["service_name"]
        )
        _write_currency_pair(
            worksheet, fmt, text, row, price_col, record.get("unit_price")
        )
        _write_currency_pair(
            worksheet, fmt, text, row, total_col, record.get("total")
        )
        row += 1

    # ----- grand total -----
    row += 1
    grand_total = service_df.select(pl.col("total").sum()).item() or 0

    worksheet.write(row, total_col, text["currency_symbol"], fmt["grand_total_symbol"])
    worksheet.write_number(
        row, total_col + 1, float(grand_total), fmt["grand_total_value"]
    )

    return row


def _apply_print_layout(
    worksheet: Worksheet,
    config: dict[str, Any],
    last_row: int,
    last_col: int,
    *,
    landscape_default: bool = False,
    set_print_area: bool = True,
) -> None:
    """Apply print area, orientation, paper, headers, and margins."""

    if set_print_area:
        worksheet.print_area(0, 0, last_row + 1, last_col)

    if config.get("landscape", landscape_default):
        worksheet.set_landscape()
    else:
        worksheet.set_portrait()

    worksheet.set_paper(config.get("paper_size", 9))
    worksheet.fit_to_pages(
        config.get("pages_wide", 1),
        config.get("pages_tall", 1),
    )

    worksheet.set_header(
        config.get("print_header", "&C&B&A"),
        {"margin": config.get("header_margin", 0.3)},
    )
    worksheet.set_footer(
        config.get("print_footer", "&CPage &P of &N"),
        {"margin": config.get("footer_margin", 0.3)},
    )

    if config.get("center_horizontally", True):
        worksheet.center_horizontally()

    worksheet.set_margins(
        left=config.get("margin_left", 0.25),
        right=config.get("margin_right", 0.25),
        top=config.get("margin_top", 0.6),
        bottom=config.get("margin_bottom", 0.6),
    )



def write_summary_sheet(
    workbook: xlsxwriter.Workbook,
    worksheet: Worksheet,
    config: dict[str, Any],
) -> None:
    """
    Write an invoice-style summary worksheet.

    Config keys
    -----------
    date_range : str            e.g. "April '26"
    client : str                e.g. "MAERSKLINE - ISLAND CATCH"
    summary_rows : list         dicts with label / value / unit / italic /
                                number_format / dash_if_zero; None = blank row
    service_df : pl.DataFrame   columns: section, service, unit_price, total
    start_row : int             first written row (default 1)
    summary_service_gap : int   blank rows between blocks (default 2)
    layout : dict               overrides for SUMMARY_DEFAULT_LAYOUT
    text : dict                 overrides for SUMMARY_DEFAULT_TEXT
    Plus print keys: landscape, paper_size, pages_wide, pages_tall,
    print_header, print_footer, header_margin, footer_margin,
    center_horizontally, margin_left/right/top/bottom, sheet_zoom,
    client_row_height.
    """

    layout = SUMMARY_DEFAULT_LAYOUT | config.get("layout", {})
    text = SUMMARY_DEFAULT_TEXT | config.get("text", {})
    fmt = build_summary_formats(workbook)

    worksheet.hide_gridlines(2)
    worksheet.set_zoom(config.get("sheet_zoom", 90))

    for column_range, width in layout["column_widths"].items():
        worksheet.set_column(column_range, width)

    row = _write_page_header(
        worksheet, fmt, text, layout, config, config.get("start_row", 1)
    )

    row = _write_metric_rows(
        workbook, worksheet, fmt, text, layout,
        config.get("summary_rows", []), row,
    )

    row += config.get("summary_service_gap", 2)

    service_df: pl.DataFrame | None = config.get("service_df")
    if service_df is not None and not service_df.is_empty():
        row = _write_service_table(worksheet, fmt, text, layout, service_df, row)

    last_col = layout["price_col"] + 3  # total value column
    _apply_print_layout(worksheet, config, row, last_col)
