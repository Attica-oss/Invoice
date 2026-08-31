import marimo

__generated_with = "0.23.14"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo

    from datasets import coa, shore_crane


@app.cell
def _():
    coa()


@app.cell
def _():
    shore_crane_lf = shore_crane.shore_crane()
    return (shore_crane_lf,)


@app.cell(hide_code=True)
def _(shore_crane_lf):
    _df = mo.sql(
        """
        FROM shore_crane_lf
        """
    )


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
