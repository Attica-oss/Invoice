import marimo

__generated_with = "0.19.4"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    import polars as pl
    from utils.google_sheet import GoogleSheetsLoader


@app.cell
def _():
    gs = GoogleSheetsLoader()
    return (gs,)


@app.cell
def _(gs):
    cccs_stuffing = gs.load_sheet(config_name="CCCS_Container_Stuffing").data
    electricity = gs.load_sheet(config_name="Electricity").data
    electricity_log = gs.load_sheet(config_name="Container_Plugin_log").data
    return cccs_stuffing, electricity, electricity_log


@app.cell
def _(cccs_stuffing):
    cccs_stuffing_df = mo.sql(
        f"""
        SELECT date,container_number,CASE WHEN customer = 'INPESCA' THEN 'INPESCA S.A' ELSE customer END AS customer FROM cccs_stuffing WHERE year(date)>=2025
        """,
        output=False
    )
    return (cccs_stuffing_df,)


@app.cell
def _(electricity):
    inv_electricity_df = mo.sql(
        f"""
        FROM electricity
        SELECT vessel_client AS customer,date_plugged AS date,time_plugged,container_number,operation_type
        WHERE operation_type LIKE '%CCCS%' AND YEAR(date)>=2025
        """,
        output=False
    )
    return (inv_electricity_df,)


@app.cell
def _(electricity_log):
    log_electricity_df = mo.sql(
        f"""
        FROM electricity_log
        SELECT "Timestamp",Date,Time,"Container Number" AS container_number
        """
    )
    return (log_electricity_df,)


@app.cell
def _(cccs_stuffing_df, inv_electricity_df, log_electricity_df):
    by_catch_plugin = mo.sql(
        f"""
        WITH
            stuffing AS (
                FROM
                    cccs_stuffing_df
            ),
            elec_inv AS (
                FROM
                    inv_electricity_df
            ),
            elec_log AS (
            FROM log_electricity_df
            )
        FROM
            stuffing s
            LEFT JOIN elec_inv i ON s.date = i.date
            AND s.container_number = i.container_number
            AND s.customer = i.customer
            LEFT JOIN elec_log l ON s.container_number = l.container_number AND l.date = i.date
        SELECT
            s.date AS stuffing_date,
            l.date AS plugged_date,
            l.time AS plugged_time,
            s.container_number,
            i.customer,
            i.time_plugged,
            i.operation_type
        """,
        output=False
    )
    return (by_catch_plugin,)


@app.cell
def _(by_catch_plugin):
    missing_plugin = mo.sql(
        f"""
        FROM
            by_catch_plugin

        SELECT
            * EXCLUDE(operation_type,time_plugged) ,
            CASE
                WHEN operation_type LIKE '%Direct%' THEN 'Direct Transfer'
                ELSE 'Stuffing and Plugged?'
            END AS remarks
        ,

        WHERE
            plugged_date IS NULL
        """
    )
    return (missing_plugin,)


@app.cell
def _(missing_plugin):
    _df = mo.sql(
        f"""
        FROM missing_plugin
        """
    )
    return


@app.cell
def _():
    mo.md(r"""
    ### Stuffed by Vessels
    """)
    return


@app.cell
def _(gs):
    stuffing_vessel = gs.load_sheet(config_name="Operations_Activity").data
    return (stuffing_vessel,)


@app.cell
def _(stuffing_vessel):
    c = mo.sql(
        f"""
        FROM
            stuffing_vessel
        SELECT
            Date,
            Vessel,
            "Container (Destination)" AS destination,
            Species
        WHERE
            Vessel IN ('Egalabur', 'Artza', 'Playa De Ris')
            AND Date > '2025-12-01'
        GROUP BY
            Date,
            Vessel,
            destination,
            Species
        HAVING
            Species IN ('BON - Bonito', 'BY - Bycatch')
            AND destination NOT LIKE '%Quay%'
        """
    )
    return (c,)


@app.cell
def _(c):
    _df = mo.sql(
        f"""
        FROM c

        SELECT MIN(Date) AS start_date,Vessel,destination,FIRST(Species)
        GROUP BY destination,vessel
        ORDER BY start_date
        """
    )
    return


@app.cell
def _():
    overtime = pl.read_excel(r"C:\Users\gmounac\Dropbox\! OPERATION SUPPORTING DOCUMENTATION\2025\2025 IPHS operation activity.xlsx")
    return (overtime,)


@app.cell
def _(overtime):
    ot_dataf = mo.sql(
        f"""
        FROM overtime
        SELECT DATE AS date,"VESSEL NAME" AS vessel,"TOTAL TONNAGE" AS tonnage
        WHERE DAY = 'OT'
        """
    )
    return (ot_dataf,)


@app.cell
def _(ot_dataf, stuffing_vessel):
    _df = mo.sql(
        f"""
        WITH
            ot AS (
                FROM
                    ot_dataf
            ),
            stuffing AS (
                FROM
                    stuffing_vessel
                SELECT
                    SUM(CAST(
                        REPLACE("Scale Reading (-Fish Net)", ',', '.') AS DOUBLE
                    )) AS tonnage,
            		Date,
            Vessel,
            MIN(Time) AS start_time,
            -- "Container (Destination)" AS container
    	
            WHERE Storage = 'Dry' AND STARTS_WITH("Container (Destination)",'MSFU') AND overtime <> 'normal hours'
            GROUP BY Date,Vessel
            )
        FROM
    
            ot o
            LEFT JOIN stuffing s ON o.date = s.date
            AND o.vessel = UPPER(s.vessel)
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
