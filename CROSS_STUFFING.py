import marimo

__generated_with = "0.23.14"
app = marimo.App(width="columns")

with app.setup:
    import polars as pl
    import marimo as mo
    from dataframe.miscellaneous import cross_stuffing
    from dataframe.emr import washing
    from dataframe.stuffing import coa
    from dataframe.transport import transfer


@app.cell
def _():
    xstuff_df = mo.sql(
        f"""
        FROM cross_stuffing
        WHERE MONTH(date)  = 5 AND invoiced = 'IOT IMPORT'
        """
    )
    return (xstuff_df,)


@app.cell
def _():
    34*1.5
    return


@app.cell
def _(xstuff_df):
    _df = mo.sql(
        f"""
        FROM xstuff_df SELECT STRING_AGG(origin,',')
        """
    )
    return


@app.cell(hide_code=True)
def _(xstuff_df):
    _df = mo.sql(
        f"""
        FROM washing
        WHERE container_number IN (FROM xstuff_df SELECT DISTINCT origin  )
        """
    )
    return


@app.cell
def _():
    from scan_google_sheet import scan_google_sheet

    return (scan_google_sheet,)


@app.cell
def _(scan_google_sheet):
    washing_ldf = scan_google_sheet(url="https://docs.google.com/spreadsheets/d/1L9qkq9WlIa2j5DcvoLvxkqYogRg76S-e8OxAIyLruAE/edit?gid=656582295#gid=656582295",sheet_name="ContainerCleaning")
    return (washing_ldf,)


@app.cell(hide_code=True)
def _(washing_ldf):
    _df = mo.sql(
        f"""
        fROM washing_ldf 
        --WHERE "Invoice To" = 'MAERSKLINE' AND MONTH(date) = 4 AND MONTH(Timestamp) <> 4
        """
    )
    return


@app.cell(hide_code=True)
def _(washing_ldf):
    _df = mo.sql(
        f"""
        FROM washing_ldf
        WHERE container_number IN ('MNBU0486473','MNBU0439893','TTNU8257966','TTNU8380625','MNBU4642682','CGMU5151276','MNBU0437756') AND date BETWEEN '2026-04-29' AND '2026-05-02'
        """
    )
    return


@app.cell
def _():
    coa.filter(pl.col("vessel_client").eq("MAERSKLINE")).collect( optimizations=pl.QueryOptFlags.none())
    return


@app.cell(hide_code=True)
def _():
    _df = mo.sql(
        f"""
        FROM coa
        WHERE vessel_client = 'MAERSKLINE'
        """
    )
    return


@app.cell(hide_code=True)
def _():
    _df = mo.sql(
        f"""
        FROM transfer
        WHERE container_number IN ('MNBU0157170','MNBU3076430','MNBU4642682')
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
