import marimo

__generated_with = "0.19.2"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    import polars as pl
    import marimo as mo
    from dataframe.netlist import netList
    from utils.google_sheet import GoogleSheetsLoader


@app.cell
def _():
    gs = GoogleSheetsLoader()
    return (gs,)


@app.cell
def _(gs):
    report = gs.load_sheet(config_name="validation",parse_dates=True).data
    return (report,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## STO Reports
    """)
    return


@app.cell
def _(report):
    sto_dataf = mo.sql(
        f"""
        --- STO reports for December 25'
        SELECT
            report_name AS vessel,
            sub_type AS id,
            customer,
            starting_date,
            STRPTIME(ending_date, '%d/%m/%Y')::DATE AS ending_date
        FROM
            report
        WHERE
            report_type = 'sto'
            AND month LIKE '%December%'
        ORDER BY starting_date ASC
        """
    )
    return (sto_dataf,)


@app.cell
def _(netlist):
    _df = mo.sql(
        f"""
        SELECT * FROM netList
        """
    )
    return


@app.cell
def _(netlist, sto_dataf):
    _df = mo.sql(
        f"""
        WITH
            sto AS (
                SELECT
                    *
                FROM
                    sto_dataf
            ),
            net AS (
                SELECT
                    *
                FROM
                    netList
            ),
        joined AS (
    

        SELECT
            s.id,
            n.date,
            n.vessel,
            n.service,
            n.total_tonnage,
            n.invoice_value

        FROM
            sto s
            JOIN net n ON n.date BETWEEN s.starting_date AND s.ending_date
            AND n.vessel = s.vessel
        WHERE YEAR(n.date) = 2025
        )

        SELECT id,vessel,ROUND(SUM(total_tonnage),3) AS tonnage, ROUND(SUM(invoice_value),3) AS amount FROM joined GROUP BY id,vessel

        """
    )
    return


@app.cell
def _():
    from dataframe.stuffing import coa
    return (coa,)


@app.cell
def _(coa):
    electricity = coa
    return (electricity,)


@app.cell
def _(electricity, sto_dataf):
    _df = mo.sql(
        f"""
        WITH
            sto AS (
                SELECT
                    *
                FROM
                    sto_dataf
            ),
            elect AS (
                SELECT
                    *
                FROM
                    electricity
            )
        SELECT
            s.id,
            e.vessel_client AS vessel,
            e.container_number,
            e.date_out,
            e.plugin_price,
            e.monitoring_price,
            e.electricity_unit_price,
            e.total
    
    
        FROM
            sto s
            JOIN elect e ON e.date_plugged BETWEEN s.starting_date AND s.ending_date
            AND s.vessel = e.vessel_client
        WHERE e.customer NOT IN ('MAERSKLINE','IOT')
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
