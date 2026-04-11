import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")

with app.setup:
    import polars as pl
    import marimo as mo


@app.cell
def _():
    coa_path = r"C:\Users\gmounac\Dropbox\Container and Transport\Container Section\Container Operations Activity\Container Operation Activity.xlsx"

    df = pl.read_excel(coa_path)
    return (df,)


@app.cell(hide_code=True)
def _(df):
    _df = mo.sql(
        f"""
        FROM df
        WHERE "Container Ref. No." IN ('CGMU5110298','TCLU1158030','CGMU5567788','TEMU9260067','CGMU5525627','TCLU1372171')
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
