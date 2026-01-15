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
        SELECT sub_type,report_name,customer,starting_date,ending_date,remarks FROM report WHERE report_type = 'si' AND month LIKE '%December%'
        """
    )
    return


@app.cell
def _():
    from dataframe.miscellaneous import cross_stuffing
    return (cross_stuffing,)


@app.cell
def _(cross_stuffing):
    _df = mo.sql(
        f"""
        SELECT
            *
        FROM
            cross_stuffing
        WHERE
            date BETWEEN '2025-12-01' AND '2025-12-31' AND invoiced <> 'IPHS'
        """
    )
    return


@app.cell
def _():
    from dataframe.transport import transfer
    from dataframe.emr import washing
    return transfer, washing


@app.cell
def _(transfer):
    _df = mo.sql(
        f"""
        SELECT * FROM transfer WHERE container_number IN ('SUDU8195865','MNBU0302365','MNBU9043434','MNBU0655228') AND movement_type <> 'Delivery'
        """
    )
    return


@app.cell
def _(washing):
    _df = mo.sql(
        f"""
        SELECT * FROM washing --WHERE container_number IN ('SUDU8195865','MNBU0302365','MNBU9043434','MNBU0655228')
        """
    )
    return


@app.cell
def _():
    (90 * 4) + (35 * 4) + (530.007+ 533.43 +526.973 +522.894)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
