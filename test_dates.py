import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")

with app.setup:
    import polars as pl
    import marimo as mo
    from type_casting.dates import date_string_to_date,stop_over_date_range,Month


@app.cell
def _():
    from datetime import date
    from dataclasses import dataclass

    @dataclass
    class Year(date):
        value:int
    

    return Year, date


@app.cell
def _(Year):
    Year(value=1)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## stop_over_date_range

    To depreciate this function as it can be done in another way
    """)
    return


@app.cell
def _(date_str_to_date):
    stop_over_date_range(
        start_date=date_str_to_date(date_string="02/01/2026"),
        end_date=date_str_to_date(date_string="05/01/2026"),
    )
    return


@app.cell
def _():
    Month.from_name(name="MAY")
    return


@app.cell
def _():
    Month.from_number(number=2)
    return


@app.cell
def _():
    date_string_to_date(date_string="01/05/2026")
    return


@app.cell
def _(date):


    from typing import Sequence

    _DATE_FORMATS: Sequence[str] = (
        "%Y-%m-%d",   # 2024-01-15  (ISO)
        "%d/%m/%Y",   # 15/01/2024
        "%m/%d/%Y",   # 01/15/2024
        "%d-%m-%Y",   # 15-01-2024
        "%d.%m.%Y",   # 15.01.2024
        "%B %d, %Y",  # January 15, 2024
        "%b %d, %Y",  # Jan 15, 2024
        "%d %B %Y",   # 15 January 2024
        "%d %b %Y",   # 15 Jan 2024
        "%Y%m%d",     # 20240115  (compact)
    )


    def date_str_to_date(date_string: str, formats: Sequence[str] = _DATE_FORMATS) -> date:
        """
        Converts a date string to a date object, trying multiple formats.

        Args:
            date_string: The date string to parse.
            formats: Ordered sequence of strptime format strings to attempt.

        Returns:
            A date object if any format matches.

        Raises:
            ValueError: If no format matches the input string.
        """
        stripped = date_string.strip()

        for fmt in formats:
            try:
                return date.strptime(stripped, fmt)
            except ValueError:
                continue

        tried = ", ".join(formats)
        raise ValueError(
            f"Unable to parse date string {date_string!r}. Tried formats: {tried}"
        )

    return (date_str_to_date,)


@app.cell
def _(date_str_to_date):
    date_str_to_date(date_string="12/01/2026")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
