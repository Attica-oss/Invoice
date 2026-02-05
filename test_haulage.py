import marimo

__generated_with = "0.19.7"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    import polars as pl
    from dataframe.transport import transfer,shore_crane
    from dataframe.stuffing import coa


@app.cell
def _():
    _df = mo.sql(
        f"""
        FROM coa WHERE container_number = 'TLLU1218288'
        """
    )
    return


@app.cell
def _():
    _df = mo.sql(
        f"""
        FROM shore_crane WHERE customer = 'ELAI ALAI'
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
