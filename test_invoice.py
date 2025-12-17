# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "duckdb==1.4.3",
#     "fastexcel==0.18.0",
#     "polars==1.36.1",
#     "pyarrow==22.0.0",
#     "python-dateutil==2.9.0.post0",
#     "requests==2.32.5",
#     "sqlglot==28.3.0",
# ]
# ///

import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    import marimo as mo
    import polars as pl


@app.cell
def _():
    from type_casting.dates import Days
    return


@app.cell
def _():
    from dataframe.netlist import netList
    return (netList,)


@app.cell
def _(netList):
    net_dataf = netList.with_columns(pl.col("date").days.add_day_name())
    return (net_dataf,)


@app.cell
def _(net_dataf):
    clean = mo.sql(
        f"""
        SELECT
            *,
            CASE
                WHEN day_name IN ('PH', 'Sun')
                AND overtime = 'overtime 150%' THEN 'normal hours'
                ELSE overtime
            END AS filter_overtime
        FROM
            net_dataf
        WHERE
            filter_overtime <> 'normal hours'
        """
    )
    return (clean,)


@app.cell
def _(net_dataf):
    list_x = mo.sql(
        f"""
        WITH
            overtime_only AS (
                SELECT
                    *,
                    CASE
                        WHEN day_name IN ('PH', 'Sun')
                        AND overtime = 'overtime 150%' THEN 'normal hours'
                        ELSE overtime
                    END AS filter_overtime
                FROM
                    net_dataf
                WHERE
                    filter_overtime <> 'normal hours'
            ),
            net_list_data AS (
                SELECT
                    day_name,
                    date,
                    vessel,
                    MIN(start_time) AS start_time,
                    overtime,
                    storage_type,
                    MAX(end_time) AS end_time,
                    ROUND(SUM(total_tonnage), 3) AS total_tonnage
                FROM
                    overtime_only
                GROUP BY
                    day_name,
                    date,
                    vessel,
                    overtime,
                    storage_type
                ORDER BY
                    date
            ),
            data_to_check AS (
                SELECT
                    "Day" AS day_name,
                    "Date" AS date,
                    UPPER("Vessel") AS vessel,
                    Tonnage AS total_tonnage,
                    Hours AS num_of_hours,
                    "Num of Stevedores" AS num_of_stevedores
                FROM
                    READ_CSV("./additional_stevedores.csv")
                ORDER BY
                    "Date"
            )
        SELECT
            *
        FROM
            net_list_data n
            FULL OUTER JOIN data_to_check d ON n.date = d.date
            AND n.vessel = d.vessel
            --AND n.total_tonnage = d.total_tonnage
        WHERE
            d.day_name IS NULL
        ORDER BY d.date
        """
    )
    return (list_x,)


@app.cell
def _(clean, list_x):
    test = mo.sql(
        f"""
        SELECT
            *
        FROM
            clean n
            JOIN list_x l ON n.date = l.date
            AND n.vessel = l.vessel
        ORDER BY n.date
        """
    )
    return (test,)


@app.cell
def _(test):
    _df = mo.sql(
        f"""
        SELECT * FROM test WHERE date = '2025-11-13' AND vessel = 'ATERPE ALAI'
        """
    )
    return


@app.cell
def _():
    sum([
    5.9,
    10.53,
    6.83,
    11.55,
    2.06])
    return


@app.cell
def _():
    # Dry 

    # -- IOT

    72.581
    # -- SAPMER

    # Brine



    93.282 - 72.581
    return


@app.cell
def _():
    mo.md(r"""
    # 🥇 Unloading Report
    * EGALABUR - 02/12 - to check why BONITO instead of MELVA

    # Shore Crane
    * We shall not invoice Dolomieu for crane rental for the unloading of cargo in November. Peter will invoice SAPMER directly.



    * To check if there were 2 shore cranes for HAWWA on 04/12 - 05/12.

        *  ::lucide:a-arrow-down:: on 06/12 there is only a crane at the back; with salt loading also,
        *  to check if crane was used for salt
        *  ![](C:\Users\gmounac\Invoice\public\image.png)
    * To check if there were 2 shore cranes for BERNICA unloading to CCCS as from 06/12 to ; and if for IPHS.

    # ❄️ Cold Store

    * To check if there was overtime for IOT bin dispatch on 04/12.

        * ::lucide:a-arrow-down::Please prepare to work in overtime for IOT Full Scow transfer. Last trip to leave IPHS by 1800hrs.

    * Why is it labelled as CCCS JAMARR for the unloading of HAWWA, and who is to invoice RAWANQ or JMARR or MAERSK for this service?

    *  This net to check the start time.

      --- 9,292	Dry	07/12/2025 00:20:00	CCCS (SAPMER)	Bernica

    ## Bycatch Transfer

        🚚 To check the bycatch transfer forms, as WhatsApp is limited.

    * To check if Playa de Ris to OB was done using IPHS truck on 03/12.

    * To check if Hawwa's OB (3.208) was done using IPHS truck on 06/12.

    * [12:23, 04/12/2025] Rupert Suzette: IPHS Tipping truck to collect by-catch from Hawwa and deliver to CCCS on behalf of OB

    * [15:30, 02/12/2025] Lewis MARIE: Tipping truck no7...no2 assign with bycath for OB on playa de Azkorri.

    * [13:52, 05/12/2025] Lewis MARIE: 1hour pass truck no6...no7  they stuck with bycath for OB on hopper no2 not unloading yet.

    ## From container loading.

    04/12 - Cap Sainte Marie to OB (0.304 tons) there is a comment "From container loading."
    """)
    return


@app.cell
def _():
    cccs_reports = mo.ui.file_browser()
    cccs_reports
    return (cccs_reports,)


@app.cell
def _(cccs_reports):
    cccs_reports.path(index=0)
    return


@app.cell
def _(cccs_reports):
    pl.read_excel(cccs_reports.path(index=0), sheet_name="JMARR")
    return


@app.cell
def _():
    45.682 + 39.513 + 12.496 + 24.724
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
