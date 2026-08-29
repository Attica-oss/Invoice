import marimo

__generated_with = "0.23.14"
app = marimo.App(width="medium")

with app.setup:
    from datasets import shore_crane,coa
    import polars as pl
    import marimo as mo


@app.cell
def _():
    coa()
    return


@app.cell
def _():
    shore_crane_lf = shore_crane.shore_crane()
    return (shore_crane_lf,)


@app.cell(hide_code=True)
def _(shore_crane_lf):
    _df = mo.sql(
        f"""
        FROM shore_crane_lf
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
