import marimo

__generated_with = "0.23.14"
app = marimo.App(width="columns")

with app.setup:
    import polars as pl
    from dataframe import full_scows
    from data_source.all_dataframe import miscellaneous
    from scan_google_sheet import scan_google_sheet


@app.cell
def _():
    stuffing_ldf = scan_google_sheet(url="https://docs.google.com/spreadsheets/d/1L0HlevB-asshOgXMmIQ14bysq56lUPp0lYCrHPB0iGg/edit?gid=76354553#gid=76354553",sheet_name="containerOperations")
    return (stuffing_ldf,)


@app.cell(hide_code=True)
def _(mo, sapmer_ldf, stuffing_ldf):
    _df = mo.sql(
        f"""
        WITH stuff AS (FROM stuffing_ldf
        SELECT vessel_client,date_plugged,container_number,set_point
            WHERE date_plugged BETWEEN '2026-07-22' AND '2026-07-31' AND customer = 'SAPMER'),sapmer AS (FROM sapmer_ldf SELECT 
            "Vessel-Trip","Container num","Set Point Temp")

        FROM stuff S LEFT JOIN sapmer M ON M."Container num" = S.container_number
        """
    )
    return


@app.cell
def _():
    sapmer_ldf = pl.read_excel(source=r"C:\Users\gmounac\Desktop\LOADING LIST (25x40) CMA CGM NACALA ETA 01.08.xlsx")
    return (sapmer_ldf,)


@app.cell
def _(sapmer_ldf):
    sapmer_ldf
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    scow_path = r"C:\Users\gmounac\Dropbox\Container and Transport\Transport Section\Container Movements\STDU Transfer.xlsx"
    return (scow_path,)


@app.cell
def _(scow_path):
    pl.read_excel(scow_path).filter(pl.col("Date").dt.year().eq(2026).and_(pl.col("Date").dt.month().eq(7)))
    return


@app.cell
def _():
    misc_ldf = miscellaneous()
    return (misc_ldf,)


@app.cell(hide_code=True)
def _(misc_ldf, mo):
    _df = mo.sql(
        f"""
        FROM misc_ldf
        WHERE operation_type LIKE '%Bin Dispatch%' AND MONTH(date) = 7
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    _df = mo.sql(
        f"""
        From full_scows
        WHERE MONTH(date) = 7
        """
    )
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
