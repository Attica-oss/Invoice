import marimo

__generated_with = "0.24.0"
app = marimo.App(width="columns")

with app.setup:

    from datetime import datetime

    import polars as pl
    import marimo as mo

    from datasets.container_ops_activity import coa
    from datasets.net_list import net_list
    from dataframe.operations import _berth as berth


@app.cell
def _():
    container_stuffing = coa()
    net = net_list()
    berth_dues = berth()
    return berth_dues, container_stuffing, net


@app.cell
def _(berth_dues):
    _df = mo.sql(
        f"""
        FROM berth_dues
        """
    )
    return


@app.cell
def _():
    vessel_selection = mo.ui.text(placeholder="Vessel")
    start_date_selection = mo.ui.text(placeholder="Start Date")
    end_date_selection = mo.ui.text(placeholder="End Date")


    mo.vstack([vessel_selection,start_date_selection,end_date_selection])
    return end_date_selection, start_date_selection, vessel_selection


@app.cell
def _(end_date_selection, start_date_selection):
    end_date = datetime.strptime(end_date_selection.value, '%d/%m/%Y').strftime('%Y-%m-%d')
    start_date = datetime.strptime(start_date_selection.value, '%d/%m/%Y').strftime('%Y-%m-%d')
    return end_date, start_date


@app.cell(hide_code=True)
def _(end_date, net, start_date, vessel_selection):
    _df = mo.sql(
        f"""
        WITH n AS (FROM net
            SELECT 
            	date::DATE + start_time::TIME AS datetime_start,
            	service,
            	date::DATE + end_time::TIME AS datetime_end
        WHERE
            vessel = '{vessel_selection.value}'
            AND date BETWEEN '{start_date}' AND '{end_date}')

        FROM  n
        SELECT 
        	MIN(datetime_start) AS started,
        	service,
        	MAX(datetime_end) AS ended 
        GROUP BY ALL
        ORDER BY started
        """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Container Stuffing
    """)
    return


@app.cell(hide_code=True)
def _(container_stuffing, end_date, start_date, vessel_selection):
    _df = mo.sql(
        f"""
        FROM
            container_stuffing
        SELECT
            MIN(date_plugged) AS start_date,
            vessel_client,
            MAX(date_plugged) AS end_date,
            customer,
            operation_type,
            count(*) AS number
        WHERE
            vessel_client = '{vessel_selection.value}'
            AND date_plugged BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY ALL
        """
    )
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Berth Dues
    """)
    return


@app.cell
def _(berth_dues, end_date, start_date, vessel_selection):
    _df = mo.sql(
        f"""
        FROM berth_dues
            SELECT "VESSEL NAME",
            "DATE IN",
            "TIME IN",
            "DATE OUT",
            "TIME OUT"

        WHERE "VESSEL NAME" = '{vessel_selection.value}' AND "DATE IN" BETWEEN '{start_date}' AND '{end_date}'
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
