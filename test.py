import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")

with app.setup:
    import polars as pl
    import marimo as mo

    from dataframe.bin_dispatch import full_scows,bin_dispatch
    from dataframe.transport import scow_transfer


@app.cell(hide_code=True)
def _():
    _df = mo.sql(
        f"""
        FROM bin_dispatch
        """
    )
    return


@app.cell(hide_code=True)
def _():
    _df = mo.sql(
        f"""
        FROM full_scows
        """
    )
    return


@app.cell(hide_code=True)
def _():
    _df = mo.sql(
        f"""
        FROM scow_transfer
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
