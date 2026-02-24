import marimo

__generated_with = "0.19.9"
app = marimo.App(width="medium")

with app.setup:
    from dataframe.shore_handling import salt,forklift_salt
    from dataframe.transport import forklift
    from data_source.excel_file_path import ExcelFiles
    from data_source.make_dataset import load_excel


@app.cell
def _():
    f_salt = forklift_salt()
    return (f_salt,)


@app.cell(hide_code=True)
def _(mo):
    _df = mo.sql(
        f"""
        FROM salt WHERE vessel = 'PLAYA DE RIS' AND date = '2025-11-07'
        """
    )
    return


@app.cell(hide_code=True)
def _(f_salt, mo):
    _df = mo.sql(
        f"""
        FROM f_salt WHERE vessel = 'PLAYA DE RIS' AND date = '2025-11-07'
        """
    )
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
