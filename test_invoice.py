import marimo

__generated_with = "0.18.3"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    import marimo as mo
    import polars as pl


@app.cell
def _():
    from type_casting.dates import Days
    return


@app.cell
def _():
    from dataframe.emr import shifting
    return (shifting,)


@app.cell
def _(shifting):
    shifting.collect()
    return


@app.cell
def _():
    from utils.google_sheet import GoogleSheetsLoader



    gs = GoogleSheetsLoader()
    return (gs,)


@app.cell
def _(gs):
    def emr_dataframe():
        return gs.load_sheet("container_cleaning").data
    return (emr_dataframe,)


@app.cell
def _(emr_dataframe):
    washing = emr_dataframe().filter(pl.col("date").dt.year().ge(2025)).select(pl.col("date").days.add_day_name(),pl.col("container_number"),pl.col("invoice_to"),pl.col("service_remarks"))
    return (washing,)


@app.cell
def _(washing):
    washing.collect()
    return


@app.cell
def _():
    _df = mo.sql(
        f"""
        WITH price AS SELECT
            date,
            container_number,
            invoice_to,
            service_remarks
        FROM
            washing
        WHERE
            YEAR(date) >= 2025
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
