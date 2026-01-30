import marimo

__generated_with = "0.19.6"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    import polars as pl
    from data_source.excel_file_path import ExcelFiles
    from data_source.make_dataset import load_excel
    from data_source.make_dataset import load_gsheet_data
    from data_source.sheet_ids import (
        OPS_SHEET_ID,
        net_list_sheet,
        raw_sheet,
        MISC_SHEET_ID,
        ALL_CCCS_DATA_SHEET,
        STUFFING_SHEET_ID,
        plugin_sheet,
    MASTER_ID,price_sheet
    )
    from type_casting.dates import public_holiday
    from type_casting.containers import iot_soc
    from type_casting.customers import cargo


@app.cell
def _():
    berth = load_excel(ExcelFiles.BERTH_DUES)
    return (berth,)


@app.cell
def _(berth):
    _df = mo.sql(
        f"""
        FROM berth WHERE Month = 12
        """
    )
    return


@app.cell
def _():
    # Datasets

    raw = load_gsheet_data(OPS_SHEET_ID, raw_sheet)
    cccs = load_gsheet_data(MISC_SHEET_ID, ALL_CCCS_DATA_SHEET)
    stuffing = load_gsheet_data(STUFFING_SHEET_ID, plugin_sheet)
    price = load_gsheet_data(MASTER_ID,price_sheet)
    return cccs, price, raw, stuffing


@app.cell
def _():
    ph = public_holiday().to_frame()
    iot_soc_df = pl.DataFrame({"destination":iot_soc})
    cargo_df = pl.DataFrame({"destination":cargo})
    return cargo_df, iot_soc_df, ph


@app.cell
def _(price):
    price_dataf = mo.sql(
        f"""
        FROM
            price
        SELECT
            Service AS service,
            Price AS price,
            StartingDate AS effective_date
        WHERE
            Class = 'Net List'
        """,
        output=False
    )
    return (price_dataf,)


@app.cell
def _(stuffing):
    stuffing_type_dataf = mo.sql(
        f"""
        FROM
            stuffing
        SELECT DISTINCT
            vessel_client,
            customer,
            date_plugged,
            container_number,
            -- operation_type,
            CASE
                WHEN operation_type LIKE '%Full%' THEN 'Full OSS'
                WHEN operation_type LIKE '%Basic%' THEN 'Basic OSS'
                ELSE 'Container Stuffing'
            END AS stuffing
        WHERE
            NOT regexp_matches(operation_type, '(CCCS|Cross)')
            AND regexp_matches(operation_type, '(Full|Basic|Stuffing)')
        ORDER BY
            date_plugged
        """,
        output=False
    )
    return (stuffing_type_dataf,)


@app.cell
def _(ph, raw):
    raw_df = mo.sql(
        f"""
        WITH
            raw_data AS (
                FROM
                    raw
                SELECT
                    Species,
                    CAST(
                        REPLACE("Scale Reading(-Fish Net) (Cal)", ',', '') AS INTEGER
                    ) * 0.001 AS tonnage,
                    Storage AS storage_type,
                    CAST("Date/Time" AS DATE) AS date,
                    CAST("Date/Time" AS TIME) AS time,
                    "Container (Destination)" AS destination,
                    UPPER(Vessel) AS vessel,
                    "Side Working" AS side_working
            ),
            ph_dates AS (
                FROM
                    ph
                SELECT
                    v0 as date,
                    'PH' AS value_
            ),
            add_day_name AS (
                FROM
                    raw_data r
                    LEFT JOIN ph_dates p ON p.date = r.date
                SELECT
                    * EXCLUDE (p.date, p.value_),
                    CASE
                        WHEN value_ IS NULL THEN STRFTIME(r.date, '%a')
                        ELSE value_
                    END AS day_name
            ),
            add_overtime AS (
                FROM
                    add_day_name
                SELECT
                    *,
                    CASE
                        WHEN time >= TIME '00:00:00'
                        AND time < TIME '07:00:00' THEN 'overtime 150%'
                        WHEN (day_name IN ('Sun', 'PH'))
                        AND time >= TIME '16:00:00' THEN 'overtime 200%'
                        WHEN (
                            (
                                day_name IN ('Sun', 'PH')
                                AND time < TIME '16:00:00'
                            )
                            OR time > TIME '17:00:00'
                        ) THEN 'overtime 150%'
                        ELSE 'normal hours'
                    END AS overtime
            )
        FROM
            add_overtime
        ORDER BY
            date,
            vessel,
        time
        """,
        output=False
    )
    return (raw_df,)


@app.cell
def _(cccs, raw_df):
    adjusted_cold_store_dataf = mo.sql(
        f"""
        --- Cold Store Adjusted Records
        WITH
            filtered_cccs AS (
                FROM
                    raw_df
                SELECT
                    *,
                    SUM(tonnage) OVER (
                        PARTITION BY
                            date,
                            vessel,
                            destination,
                            overtime,
                            storage_type
                    ) AS tons,
                    CASE
                        WHEN (
                            day_name IN ('Sun', 'PH')
                            AND overtime = 'overtime 150%'
                        )
                        OR (overtime = 'normal hours') THEN 'normal'
                        WHEN overtime IN ('overtime 150%', 'overtime 200%') THEN 'overtime'
                        ELSE 'error'
                    END AS tonnage_select
                WHERE
                    destination LIKE '%CCCS%'
                ORDER BY
                    date,
                    time
            ),
            cccs_record AS (
                FROM
                    cccs
                SELECT
                    day AS day_name,
                    date,
                    'CCCS (' || REPLACE(REPLACE(customer, ' S.A', ''), ' SA', '') || ')' AS destination,
                    vessel,
                    storage_type,
                    total_tonnage,
                    COALESCE(CAST(NULLIF(overtime_tonnage, '') AS DOUBLE), 0) AS overtime_tonnage
                WHERE
                    YEAR(date) >= 2025
                    AND operation_type IN (
                        'Sorting from Unloading',
                        'Unsorted from Unloading'
                    )
                    AND customer NOT IN ('AMIRANTE', 'OCEAN BASKET', 'ISLAND CATCH')
            ),
            grouped_cccs_record AS (
                FROM
                    cccs_record
                SELECT
                    date,
                    destination,
                    vessel,
                    storage_type,
                    ROUND(SUM(total_tonnage), 3) AS total_tonnage,
                    ROUND(SUM(overtime_tonnage), 3) AS overtime_tonnage
                GROUP BY
                    date,
                    destination,
                    vessel,
                    storage_type
                ORDER BY
                    date
            ),
            add_adjusted_tonnage AS (
                SELECT
                    f.*,
                    g.total_tonnage,
                    g.overtime_tonnage,
                    (g.total_tonnage - g.overtime_tonnage) AS normal_tonnage,
                    CASE
                        WHEN tonnage_select = 'normal' THEN DIVIDE(normal_tonnage, tons)
                        ELSE DIVIDE(g.overtime_tonnage, tons)
                    END AS perc_diff,
                    f.tonnage * perc_diff AS adjusted_tonnage
                FROM
                    filtered_cccs f
                    LEFT JOIN grouped_cccs_record g ON g.date = f.date
                    AND f.destination = g.destination
                    AND f.vessel = g.vessel
                    AND g.storage_type = f.storage_type
            )
        FROM
            add_adjusted_tonnage
        SELECT
            day_name,
            date,
            overtime,
            vessel,
            MIN(time) AS start_time,
            destination,
            storage_type,
            MAX(time) AS end_time,
            ROUND(SUM(adjusted_tonnage), 3) AS total_tonnage
        GROUP BY
            day_name,
            date,
            overtime,
            vessel,
            destination,
            storage_type

        ORDER BY date,vessel,start_time
        """,
        output=False
    )
    return (adjusted_cold_store_dataf,)


@app.cell
def _(
    adjusted_cold_store_dataf,
    cargo_df,
    iot_soc_df,
    price_dataf,
    raw_df,
    stuffing_type_dataf,
):
    net_list_dataf = mo.sql(
        f"""
        --- Net list Combined with the Cold Store data
        WITH
            net_list AS (
                FROM
                    raw_df
                SELECT
                    day_name,
                    date,
                    overtime,
                    vessel,
                    MIN(time) AS start_time,
                    destination,
                    storage_type,
                    MAX(time) AS end_time,
                    ROUND(SUM(tonnage), 3) AS total_tonnage
                WHERE
                    destination NOT LIKE '%CCCS%'
                GROUP BY
                    day_name,
                    date,
                    overtime,
                    vessel,
                    destination,
                    storage_type
                UNION ALL
                FROM
                    adjusted_cold_store_dataf
                ORDER BY
                    date,
                    vessel,
                    start_time
            ),
            stuffing AS (
                FROM
                    stuffing_type_dataf
            ),
            by_catch_stuffed_by_vessel AS (
                FROM
                    stuffing_type_dataf
                WHERE
                    vessel_client IN ('AMIRANTE', 'ISLAND CATCH', 'OCEAN BASKET')
            ),
            add_service AS (
                SELECT
                    l.*,
                    -- r.* EXCLUDE(r.stuffing),
                    CASE
                        WHEN r.stuffing IS NULL THEN rx.stuffing
                        ELSE r.stuffing
                    END AS stuffing_,
                    rx.vessel_client AS remarks,
                    CASE
                        WHEN l.destination LIKE '%IOT%'
                        OR (
                            l.destination LIKE '%DARDANEL%'
                            AND l.date < '2025-09-01'
                        )
                        OR (
                            l.destination LIKE '%Asian Marine Reefer%'
                            AND l.date BETWEEN '2025-11-03' AND '2025-11-19'
                        )
                        OR (destination LIKE '%Unload to Quay%')
                        OR (
                            destination IN (
                                FROM
                                    iot_soc_df
                            )
                            AND r.customer = 'IOT'
                        ) THEN 'Unload to Quay'
                        WHEN UPPER(destination) IN (
                            FROM
                                cargo_df
                        ) THEN 'Transhipment'
                        WHEN destination LIKE '%CCCS%' THEN 'Unload to CCCS'
                        ELSE stuffing_
                    END AS service
                FROM
                    net_list l
                    ASOF LEFT JOIN stuffing r ON l.destination = r.container_number
                    AND l.vessel = UPPER(r.vessel_client)
                    AND l.date <= r.date_plugged
                    LEFT JOIN by_catch_stuffed_by_vessel rx ON l.destination = rx.container_number
                    AND l.date = rx.date_plugged
                ORDER BY
                    l.date,
                    l.vessel
            ),
            price AS (
                FROM
                    price_dataf
            ),
            add_combined_service AS (
                SELECT
                    * EXCLUDE (stuffing_),
                    service || ' - ' || storage_type AS service_and_storage
                FROM
                    add_service
            )
        SELECT
            n.day_name,
            n.date,
            n.vessel,
            n.start_time,
            n.destination,
            n.overtime,
            n.storage_type,
            n.end_time,
            n.total_tonnage,
            n.service,
            CASE
                WHEN overtime = 'normal hours' THEN p.price * 1.0
                WHEN overtime = 'overtime 150%' THEN p.price * 1.5
                WHEN overtime = 'overtime 200%' THEN p.price * 2.0
                ELSE 0
            END AS unit_price,
            ROUND(total_tonnage * unit_price, 3) AS total_price,
            n.remarks
        FROM
            add_combined_service n
            ASOF LEFT JOIN price p ON p.effective_date <= n.date
            AND p.service = n.service_and_storage
        ORDER BY 
        n.date,n.vessel,n.start_time
        """,
        output=False
    )
    return (net_list_dataf,)


@app.cell
def _():
    import duckdb

    DATABASE_URL = "netlist.db"
    engine = duckdb.connect(DATABASE_URL, read_only=False)
    return (engine,)


@app.cell
def _(engine):
    _df = mo.sql(
        f"""
        CREATE OR REPLACE TABLE  net_list (
            day_name       VARCHAR,
            date           DATE,
            vessel         VARCHAR,
            start_time     TIME,
            destination    VARCHAR,
            overtime       VARCHAR,
            storage_type   VARCHAR,
            end_time       TIME,
            total_tonnage  DOUBLE,
            service        VARCHAR,
            unit_price     DOUBLE,
            total_price    DOUBLE,
            remarks        VARCHAR
        );
        """,
        engine=engine
    )
    return


@app.cell
def _(engine, net_list, net_list_dataf):
    _df = mo.sql(
        f"""
        INSERT INTO net_list (
            day_name, date, vessel, start_time, destination, overtime,
            storage_type, end_time, total_tonnage, service,
            unit_price, total_price, remarks
        )
        SELECT
            day_name, date, vessel, start_time, destination, overtime,
            storage_type, end_time, total_tonnage, service,
            unit_price, total_price, remarks
        FROM net_list_dataf;
        """,
        engine=engine
    )
    return


@app.cell
def _(net_list_dataf):
    type(net_list_dataf)
    return


@app.cell
def _(net_list_dataf):
    _df = mo.sql(
        f"""
        SELECT MAX(date) FROM net_list_dataf
        """
    )
    return


@app.cell
def _(net_list_dataf):
    _df = mo.sql(
        f"""
        COPY (
            SELECT *
            FROM net_list_dataf
        ) TO 'C:/Users/gmounac/invoice/csv/netlist.csv'
        WITH (HEADER, DELIMITER ',');
        """
    )
    return


@app.cell
def _():
    mo.ui.button()
    return


@app.cell
def _(engine, net_list):
    _df = mo.sql(
        f"""
        SELECT * FROM net_list
        """,
        engine=engine
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
