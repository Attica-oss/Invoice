import marimo

__generated_with = "0.19.7"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    from dataframe.transport import forklift,transfer
    from dataframe.emr import shifting
    from dataframe.miscellaneous import by_catch,cccs_stuffing
    from dataframe.stuffing import coa
    from dataframe.shore_handling import salt
    from data_source.excel_file_path import ExcelFiles
    from data_source.make_dataset import load_excel


@app.cell
def _():
    ops_activity = load_excel(ExcelFiles.CONTAINER_OPERATIONS)


    return (ops_activity,)


@app.cell
def _(ops_activity):
    _df = mo.sql(
        f"""
        WITH log_ AS (FROM
            ops_activity
        SELECT Vessel,"Date affected" AS date_affected,"Container Ref. No." AS container_number,SP,"Type" AS stuffing_type,"Status" AS stuffing_status,ROUND(Tonnage * 0.001,3) AS tonnage,"Date Out" AS date_out
        WHERE
            "Date affected" BETWEEN '2025-10-12' AND '2025-10-17'
            AND Vessel LIKE '%Dolomieu%'),inv_ AS ( FROM coa WHERE date_plugged BETWEEN '2025-10-12' AND '2025-10-17' AND vessel_client = 'DOLOMIEU' )

    

        FROM log_ l SELECT *

        """
    )
    return


@app.cell
def _():
    _df = mo.sql(
        f"""
        FROM transfer WHERE date BETWEEN '2025-09-01' AND '2025-10-31' AND container_number = 'SUDU8180510'
        """
    )
    return


@app.cell
def _():
    _df = mo.sql(
        f"""
        FROM salt WHERE date BETWEEN '2025-10-01' AND '2025-10-31'
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
