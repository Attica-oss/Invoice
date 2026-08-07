import marimo

__generated_with = "0.23.14"
app = marimo.App(width="columns")

with app.setup:
    import polars as pl
    from datetime import date

    from dataframe.transport import transfer
    from dataframe.emr import shifting
    from dataframe.bin_dispatch import empty_scows

    from datasets.container_ops_activity import coa
    from datasets.container_ops_activity import pallet
    from dataframe.miscellaneous import cross_stuffing
    from datasets.truck_to_cold_store import cccs_record
    from dataframe.transport import forklift


@app.cell(hide_code=True)
def _(mo):
    _df = mo.sql(
        f"""
        FROM forklift
        WHERE customer = 'CCCS'

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    _df = mo.sql(
        f"""
        FROM shifting
        """
    )
    return


@app.cell
def _(mo):
    select_report = mo.ui.dropdown(
        label="Select Line", options=["IOT"], value="IOT"
    )

    month_options = {
        "Jan 2026": "2026-01-01",
        "Feb 2026": "2026-02-01",
        "Mar 2026": "2026-03-01",
        "Apr 2026": "2026-04-01",
        "May 2026": "2026-05-01",
        "Jun 2026": "2026-06-01",
        "Jul 2026": "2026-07-01",
        "Aug 2026": "2026-08-01"
    }


    month_selector = mo.ui.dropdown(
        options=month_options,
        label="Select invoice month",
        value=date.today().replace(day=1).strftime(format="%b %Y"),
    )


    mo.vstack([select_report, month_selector])
    return month_selector, select_report


@app.cell
def _():
    stdu_path = r"C:\Users\gmounac\Dropbox\Container and Transport\Transport Section\Container Movements\STDU Transfer.xlsx"
    return (stdu_path,)


@app.cell
def _(stdu_path):
    stdu_ldf = pl.read_excel(stdu_path,schema_overrides={"Time out":pl.Time,"Time in":pl.Time}).filter(pl.col("Date").dt.year().eq(2026).and_(pl.col("Date").dt.month().eq(4)))
    return (stdu_ldf,)


@app.cell
def _(mo, month_selector, select_report):
    transfer_df = mo.sql(
        f"""
        FROM transfer
        WHERE remarks = '{select_report.value}' AND (date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}'))
        """
    )
    return (transfer_df,)


@app.cell(hide_code=True)
def _(mo, transfer_df):
    _df = mo.sql(
        f"""
        FROM transfer_df
        SELECT COUNT(*) AS number_of_units,SUM(shifting_price) AS shifting_price
        """
    )
    return


@app.cell(hide_code=True)
def _(mo, stdu_ldf):
    _df = mo.sql(
        f"""
        FROM stdu_ldf
            SELECT date,SUM("No of Scows")::INT AS scows
    
        WHERE "Status" = 'Empty'
        GROUP BY ALL
        ORDER BY ALL
        """
    )
    return


@app.cell
def _():
    plugin_ldf = coa()
    return (plugin_ldf,)


@app.cell(hide_code=True)
def _(mo, month_selector, plugin_ldf, select_report):
    _df = mo.sql(
        f"""
        FROM plugin_ldf
        WHERE customer = '{select_report.value}' AND (date_plugged BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}'))
        """
    )
    return


@app.cell
def _(mo, month_selector, select_report):
    _df = mo.sql(
        f"""
        FROM cross_stuffing
        WHERE invoiced = '{select_report.value}' AND  (date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}'))
        """
    )
    return


@app.cell
def _():
    truck_to_cccs = cccs_record()
    return (truck_to_cccs,)


@app.cell(hide_code=True)
def _(mo, month_selector, select_report, truck_to_cccs):
    _df = mo.sql(
        f"""
        FROM truck_to_cccs
        WHERE destination LIKE '%{select_report.value}%' AND  (date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}'))
        """
    )
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
