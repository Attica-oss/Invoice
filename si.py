import marimo

__generated_with = "0.19.2"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells

    import marimo as mo
    import polars as pl
    from utils.google_sheet import GoogleSheetsLoader


@app.cell
def _():
    gs = GoogleSheetsLoader()
    return (gs,)


@app.cell
def _(gs):
    report = gs.load_sheet(config_name="validation").data
    return (report,)


@app.cell
def _(report):
    _df = mo.sql(
        f"""
        SELECT
            sub_type,
            report_name,
            customer,
            starting_date,
            ending_date,
            remarks
        FROM
            report
        WHERE
            report_type = 'si'
            AND month LIKE '%December%'
        """
    )
    return


@app.cell
def _():
    from dataframe.transport import forklift
    return (forklift,)


@app.cell
def _(forklift):
    _df = mo.sql(
        f"""
        WITH raw AS (SELECT
            *
        FROM
            forklift
        WHERE
            date BETWEEN '2025-12-01' AND '2025-12-31'
            AND invoiced_in IN ('ECHEBASTAR', 'HARTSWATER LIMITED'))

        SELECT invoiced_in,CEIL(SUM(normal_hours) / 60) * 30 AS price FROM raw GROUP BY invoiced_in
        """
    )
    return


@app.cell
def _():
    from dataframe.shore_handling import forklift_salt,salt
    return forklift_salt, salt


@app.cell
def _(salt):
    _df = mo.sql(
        f"""
        SELECT customer,ROUND(SUM(price),3) AS price FROM salt WHERE
            date BETWEEN '2025-12-01' AND '2025-12-31'
            AND customer IN ('ECHEBASTAR', 'HARTSWATER LIMITED') GROUP BY customer
        """
    )
    return


@app.cell
def _(forklift_salt):
    fs =forklift_salt()
    return (fs,)


@app.cell
def _(fs):
    _df = mo.sql(
        f"""
        SELECT * FROM fs WHERE
            date BETWEEN '2025-12-01' AND '2025-12-31'
        """
    )
    return


@app.cell
def _():
    from dataframe.stuffing import coa
    return (coa,)


@app.cell
def _(coa):
    _df = mo.sql(
        f"""
        SELECT
            *,
            CASE 
            	WHEN (date_out > '2025-12-31' OR date_out IS NULL) THEN '2025-12-31' ELSE 
            date_out END AS end_date,
            DATEDIFF('days',date_plugged,end_date) AS days
        FROM
            coa
        WHERE
            (customer = 'IOT IMPORT')
            AND (date_plugged < '2025-12-31')
            AND (
                date_out IS NULL
                OR date_out > '2025-12-01'
            )
        """
    )
    return


@app.cell
def _():
    38.76 + 30 +35
    return


@app.cell
def _():
    6168.304 + (35*4)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
