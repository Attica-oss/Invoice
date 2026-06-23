import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    import polars as pl


@app.cell
def _():
    from dataframe import salt

    return (salt,)


@app.cell(hide_code=True)
def _(salt):
    _df = mo.sql(
        f"""
        FROM salt
        """
    )
    return


if __name__ == "__main__":
    app.run()
