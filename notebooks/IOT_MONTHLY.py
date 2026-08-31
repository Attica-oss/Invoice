import marimo

__generated_with = "0.23.14"
app = marimo.App(width="columns")

with app.setup:
    from datetime import date

    import marimo as mo
    import polars as pl

    from datasets.stuffing import coa


@app.cell
def _():

    _today = date.today()
    _years = sorted({_today.year, _today.year + 1})
    month_options = {
        date(_y, _m, 1).strftime("%b %Y"): f"{_y}-{_m:02d}-01"
        for _y in _years
        for _m in range(1, 13)
    }

    month_selector = mo.ui.dropdown(
        options=month_options,
        label="Select invoice month",
        value=_today.replace(day=1).strftime("%b %Y"),
    )
    return (month_selector,)


@app.function
def format_datestr_to_month_year(date_str: str) -> str:
    """Format the datestring to month 'year"""
    date_converted = date.fromisoformat(date_str)
    month = date_converted.strftime(format="%B")
    year = date_converted.strftime(format="%y")
    return month + " '" + year


@app.cell
def _(month_selector):
    title = mo.md("# 🐟 IOT Monthly Report")

    filter_bar = mo.hstack(
        [month_selector],
        justify="start",
        gap=2,
    )

    meta = mo.hstack(
        [
            mo.stat(
                label="Month",
                value=format_datestr_to_month_year(month_selector.value),
                caption="Operation date",
            ),
            # mo.stat(
            #     label="Invoice Value",
            #     value=metrics_df.item(),
            #     caption="Total Price",
            # ),
        ],
        justify="start",
        gap=1,
    )

    # actions = mo.hstack([copy_button, save_button], justify="start", gap=1)

    mo.vstack(
        [
            title,
            mo.md("---"),
            filter_bar,
            meta,
            mo.md("---"),
            # summary_table,
            # mo.md("---"),
            # actions,
        ]
    )


@app.cell(hide_code=True)
def _(month_selector):
    scow_empty_transfer = mo.sql(
        f"""
        FROM
            empty_scows
        SELECT
            day_name,
            date,
            movement_type,
            customer AS client,
            num_of_scows,
            overtime,
            unit_price,
            total_price
        WHERE
            date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}')
        """
    )
    return (scow_empty_transfer,)


@app.cell(hide_code=True)
def _(month_selector):
    container_transfer = mo.sql(
        f"""
        FROM transfer
        WHERE (remarks = 'IOT' AND line = 'IOT') AND date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}')
        """
    )
    return (container_transfer,)


@app.cell
def _():
    container_plugin = coa()
    return (container_plugin,)


@app.cell(hide_code=True)
def _(container_plugin, month_selector):
    _df = mo.sql(
        f"""
        FROM
            container_plugin
        WHERE
            customer = 'IOT' AND shipping_line = 'IOT' AND date_plugged <= LAST_DAY(DATE '{month_selector.value}')
            AND (
                date_out IS NULL
                OR date_out >= DATE '{month_selector.value}'
            )
        """
    )


@app.cell
def _(container_transfer, scow_empty_transfer):
    # Empty Scow Transfer

    number_of_scows = scow_empty_transfer.select(pl.col("num_of_scows").sum()).item()
    scows_price = scow_empty_transfer.select(pl.col("total_price").cast(pl.Decimal(scale=2)).sum()).item()

    # Transfer // Haulage

    number_of_transfer = container_transfer.select(pl.col("container_number").count()).item()
    shifting_price = container_transfer.select(pl.col("shifting_price").cast(pl.Decimal(scale=2)).sum()).item()
    haulage_price = container_transfer.select(pl.col("haulage_price").cast(pl.Decimal(scale=2)).sum()).item()
    return (haulage_price,)


@app.cell
def _(haulage_price):
    haulage_price


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
