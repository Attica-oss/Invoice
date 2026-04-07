import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")

with app.setup:
    import polars as pl
    from dataframe.netlist import netList


@app.cell
def _():
    netList
    return


@app.cell(hide_code=True)
def _(mo, netlist):
    _df = mo.sql(
        f"""
        FROM netList
        """
    )
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
