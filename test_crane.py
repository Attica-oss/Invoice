import marimo

__generated_with = "0.19.7"
app = marimo.App(width="medium")

with app.setup:
    import polars as pl
    import marimo as mo
    from dataframe.transport import shore_crane
    from data_source.excel_file_path import ExcelFiles
    from data_source.make_dataset import load_excel


@app.cell
def _():
    _df = mo.sql(
        f"""
        FROM shore_crane
        SELECT * --EXCLUDE(customer,location), customer ||' // '||location AS customer,
        where year(DATE) >= 2025
        """
    )
    return


@app.cell
def _():
    berth = load_excel(file_path=ExcelFiles.BERTH_DUES)
    return (berth,)


@app.cell
def _(berth):
    _df = mo.sql(
        f"""
        FROM berth WHERE "DATE IN">= '2025-10-01'
        """
    )
    return


@app.cell
def _():
    stuffing = load_excel(file_path=ExcelFiles.CONTAINER_OPERATIONS)
    return (stuffing,)


@app.cell
def _(stuffing):
    _df = mo.sql(
        f"""
        FROM stuffing WHERE "Container Ref. No." = 'SZLU9828128'
        """
    )
    return


@app.cell
def _(stuffing):
    _df = mo.sql(
        f"""
        FROM stuffing WHERE "Date affected" BETWEEN '2025-10-21' AND '2025-10-26' AND Vessel LIKE '%Franche Terre%'
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
