import marimo

__generated_with = "0.23.14"
app = marimo.App(width="columns")

with app.setup:
    import polars as pl
    import marimo as mo

    from pathlib import Path

    from dataframe import netList
    from datasets import net_list

    from scan_google_sheet import scan_google_sheet


@app.cell
def _():
    file_path = (
        Path.home()
        / "Dropbox"
        / "! OPERATION SUPPORTING DOCUMENTATION"
        / "2026"
        / "2026 IPHS operation activity.xlsx"
    )


    ops_activity_url:str="https://docs.google.com/spreadsheets/d/1PvTkl6DYZdhtaiNshz0qwtSPxC8S1OOeu905NmhFKNs/edit?gid=1049250522#gid=1049250522"
    return file_path, ops_activity_url


@app.cell
def _(file_path):
    ops_activity_df = pl.read_excel(source=file_path,table_name="handling_activity")
    return (ops_activity_df,)


@app.function
def clean_operations_activity_details(df:pl.DataFrame):
    """Clean Ops Activity"""
    return df.select(
        pl.col("DAY").alias("day_name"),
        pl.col("DATE").alias("date"),
        pl.col("VESSEL NAME").alias("vessel"),
        # pl.col("BRINE (SAUMURE)").cast(pl.Decimal(scale=2)).alias("brine_tonnage"),
        # pl.col("DRY (Below -30°C)").cast(pl.Decimal(scale=2)).alias("dry_tonnage"),
        pl.col("TOTAL TONNAGE").cast(pl.Decimal(scale=2)).alias("total_tonnage"),
        pl.col("Container Brine").fill_null(0).cast(pl.Decimal(scale=2)).alias("container_brine"),
        pl.col("Container Dry").fill_null(0).cast(pl.Decimal(scale=2)).alias("container_dry"),
        pl.col("Transhipment Brine").fill_null(0).cast(pl.Decimal(scale=2)).alias("transhipment_brine"),
        pl.col("Transhipment Dry").fill_null(0).cast(pl.Decimal(scale=2)).alias("transhipment_dry"),
        pl.col("Simple Unloading Brine").fill_null(0).cast(pl.Decimal(scale=2)).alias("simple_unloading_brine"),
        pl.col("Simple Unloading Dry").fill_null(0).cast(pl.Decimal(scale=2)).alias("simple_unloading_dry"),
        pl.col("Unloading to CCCS Brine").fill_null(0).cast(pl.Decimal(scale=2)).alias("unloading_to_cccs_brine"),
        pl.col("Unloading to CCCS Dry").fill_null(0).cast(pl.Decimal(scale=2)).alias("unloading_to_cccs_dry"),
        # pl.col("Comments").alias("comments")
    )


@app.cell
def _(ops_activity_df):
    detailed_ops = ops_activity_df.pipe(clean_operations_activity_details)
    return (detailed_ops,)


@app.cell
def _(ops_activity_df):
    ops_activity_df.pipe(clean_operations_activity)
    return


@app.function
def clean_operations_activity(df:pl.DataFrame):
    """Clean Ops Activity"""
    return df.select(
        pl.col("DAY").alias("day_name"),
        pl.col("DATE").alias("date"),
        pl.col("VESSEL NAME").alias("vessel"),
        pl.col("BRINE (SAUMURE)").cast(pl.Decimal(scale=2)).alias("brine_tonnage"),
        pl.col("DRY (Below -30°C)").cast(pl.Decimal(scale=2)).alias("dry_tonnage"),
        pl.col("TOTAL TONNAGE").cast(pl.Decimal(scale=2)).alias("total_tonnage"),
        pl.col("Well-to-Well Transfer").fill_null(0).cast(pl.Decimal(scale=2)).alias("well_to_well_transfer"),
        pl.col("Comments").alias("comments")
    )


@app.cell
def _(ops_activity_url: str):
    overtime_stuffing_df = scan_google_sheet(url=ops_activity_url,sheet_name="OvertimeStuffing").filter(pl.col("Date").dt.year().eq(2026))
    return (overtime_stuffing_df,)


@app.cell(hide_code=True)
def _(overtime_stuffing_df):
    ot_stuffing_df = mo.sql(
        f"""
        WITH
            after_midnight AS (
                FROM
                    overtime_stuffing_df
                SELECT
                    Day AS day_name,
                    EndDate AS start_date,
                    Vessel AS vessel,
                    '00:00'::TIME AS start_time,
                    EndDate AS end_date,
                    EndTime::TIME AS end_time,
                    Side AS side_working
                WHERE
                    EndDate <> Date
            ),
            without_crossing_midnight AS (
                FROM
                    overtime_stuffing_df
                SELECT
                    Day AS day_name,
                    Date AS start_date,
                    Vessel AS vessel,
                    StartTime::TIME AS start_time,
                    EndDate AS end_date,
                    EndTime::TIME AS end_time,
                    Side AS side_working
                WHERE
                    Date = EndDate
            ),
            before_midnight AS (
                FROM
                    overtime_stuffing_df
                SELECT
                    Day AS day_name,
                    Date AS start_date,
                    Vessel AS vessel,
                    StartTime::TIME AS start_time,
                    Date AS end_date,
                    '23:59'::TIME AS end_time,
                    Side AS side_working
                WHERE
                    Date <> EndDate
            )
        FROM
            after_midnight
        UNION ALL
        FROM
            without_crossing_midnight
        UNION ALL
        FROM
            before_midnight
        ORDER BY start_date,start_time
        """,
        output=False
    )
    return (ot_stuffing_df,)


@app.cell
def _():
    # netList = net_list()
    return


@app.cell(hide_code=True)
def _(netlist, ot_stuffing_df):
    net_list_df = mo.sql(
        f"""
        FROM
            netList n
            LEFT JOIN ot_stuffing_df o ON n.date = o.start_date
            AND n.vessel = o.vessel
            AND n.start_time BETWEEN o.start_time AND o.end_time
            AND n.storage_type = 'Dry'
        SELECT
            COALESCE(o.day_name,n.day_name) AS day_name,
        	n.* EXCLUDE(n.day_name)

        ORDER BY n.date,n.start_time
        """
    )
    return (net_list_df,)


@app.cell
def _(net_list_df):
    pivoted_net_list_df = mo.sql(
        f"""
        WITH base AS (
            SELECT
                day_name,
                date,
                vessel,
                SUM(total_tonnage)::DECIMAL AS total_tonnage,
                CASE
                    WHEN service IN ('Full OSS', 'Basic OSS', 'Container Stuffing')
                        THEN 'container_' || LOWER(storage_type)
                    WHEN service = 'Unload to Quay'
                        THEN 'simple_unloading_' || LOWER(storage_type)
                    WHEN service = 'Transhipment'
                        THEN 'transhipment_' || LOWER(storage_type)
                    WHEN service = 'Unload to CCCS'
                        THEN 'unloading_to_cccs_' || LOWER(storage_type)
                    ELSE 'invalid_' || LOWER(storage_type)
                END AS service_storage
            FROM net_list_df
            GROUP BY ALL
        )

        PIVOT base
        ON service_storage
        USING COALESCE(SUM(total_tonnage),0)
        GROUP BY day_name, date, vessel
        ORDER BY date;
        """
    )
    return (pivoted_net_list_df,)


@app.cell(hide_code=True)
def _(detailed_ops, pivoted_net_list_df):
    _df = mo.sql(
        f"""
        WITH
            exl AS (
                FROM
                    detailed_ops
                SELECT
                    day_name,
                    date,
                    vessel,
                    container_brine,
                    container_dry
            ),
            plr AS (
                FROM
                    pivoted_net_list_df
                SELECT
                    day_name,
                    date,
                    vessel,
                    container_brine,
                    container_dry
            )
        SELECT
            e.*,
            p.container_brine AS plr_cnt_brine,
            p.container_dry AS plr_cnt_dry,
            e.container_brine - p.container_brine AS brine_diff,
            e.container_dry - p.container_dry AS dry_diff
        FROM
            exl e
            LEFT JOIN plr p ON e.day_name = p.day_name
            AND e.date = p.date
            AND e.vessel = p.vessel
        WHERE
            brine_diff IS NOT NULL
            AND brine_diff <> 0
            AND MONTH(e.date) >= 5
        ORDER BY e.date
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
