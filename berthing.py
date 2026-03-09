import marimo

__generated_with = "0.20.4"
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
        MASTER_ID,
        price_sheet,
    INVOICING,INVOICE_STATUS
    )
    from type_casting.dates import public_holiday
    from type_casting.containers import iot_soc
    from type_casting.customers import cargo


@app.cell
def _():
    raw_stuffing = load_gsheet_data(sheet_id="1PvTkl6DYZdhtaiNshz0qwtSPxC8S1OOeu905NmhFKNs",sheet_name="RawData").and_then(lambda x:x.filter(pl.col("Date").dt.year().eq(2026))).collect()
    return (raw_stuffing,)


@app.cell(hide_code=True)
def _(raw_stuffing):
    _df = mo.sql(
        f"""
        WITH
            iot_dol AS (
                FROM
                    raw_stuffing
                SELECT
                    "Container (Destination)",
                    Vessel,
                    Date,
                    Time,
                    "Side Working"
                WHERE
                    vessel = 'Dolomieu'
                    AND "Container (Destination)" IN (
                        'SUDU8064187',
                        'SUDU8076059',
                        'SUDU8082046',
                        'SUDU8170548',
                        'SUDU8183062',
                        'SUDU8178045',
                        'CNIU2234183',
                        'SUDU8179720',
                        'SUDU8081034',
                        'SUDU8087048',
                        'MNBU0318280'
                    )
            )
        FROM
            iot_dol
        SELECT "Container (Destination)",Date,MIN(Time) AS start_time,MAX(Time) AS end_time
        GROUP BY "Container (Destination)",Date
        """
    )
    return


@app.cell
def _():
    report_status = load_gsheet_data(sheet_id=INVOICING,sheet_name="report_status").unwrap()
    return (report_status,)


@app.cell(hide_code=True)
def _(report_status):
    _df = mo.sql(
        f"""
        FROM
            report_status
        SELECT
            invoice_number,
            report_type || ' -- ' || sub_type AS report_type,
            "vessel/client",
            start_date,
            end_date,
            status,
            invoice_amount
        ORDER BY invoice_number DESC
        """
    )
    return


@app.cell
def _():
    invoice_df = load_gsheet_data(sheet_id=INVOICING,sheet_name=INVOICE_STATUS).unwrap()
    return (invoice_df,)


@app.cell
def _():
    berth = load_excel(ExcelFiles.BERTH_DUES_2026)
    return (berth,)


@app.cell
def _():
    berth2 = load_excel(ExcelFiles.BERTH_DUES)
    return (berth2,)


@app.cell(hide_code=True)
def _(berth2):
    _df = mo.sql(
        f"""
        FROM berth2 WHERE MONTH("DATE IN") = 12
        """
    )
    return


@app.cell
def _(berth):
    berth.collect()
    return


@app.cell
def _():
    iphs_truck = load_gsheet_data(sheet_id="1VbfiiWsp8yxs6KSR1CXpw1S_35tYlWV8UjjWah9Afpw",sheet_name="IPHSTruck").unwrap()
    return (iphs_truck,)


@app.cell
def _():
    from datetime import date

    bin_dispatch = (
        load_gsheet_data(
            sheet_id="1VbfiiWsp8yxs6KSR1CXpw1S_35tYlWV8UjjWah9Afpw",
            sheet_name="CCCSReport",
        )
        .and_then(
            lambda x: x.filter(
                pl.col("operation_type")
                .is_in(["Bin Dispatch to IOT", "Bin Dispatch from IOT"])
                .and_(
                    pl.col("date").is_between(date(2026, 2, 1), date(2026, 2, 28))
                )
            ).select(
                pl.col("day"),
                pl.col("date"),
                pl.col("vessel"),
                pl.col("operation_type"),
                pl.col("total_tonnage").abs(),
                pl.col("overtime_tonnage")
                .str.replace("", "0")
                .cast(pl.Float64)
                .round(3),
            )
        )
        .collect()
    )
    return (bin_dispatch,)


@app.cell(hide_code=True)
def _(bin_dispatch):
    _df = mo.sql(
        f"""
        FROM bin_dispatch
        SELECT *,CASE WHEN "day" IN ('SUN','PH') THEN (CAST(total_tonnage - overtime_tonnage AS DECIMAL) * 3.5*1.5) + CAST(overtime_tonnage * 3.5 *2.0 AS DECIMAL) ELSE (CAST(total_tonnage - overtime_tonnage AS DECIMAL) * 3.5) + CAST(overtime_tonnage * 3.5 *1.5 AS DECIMAL) END AS price
        """
    )
    return


@app.cell
def _():
    1461.50375 - (250*3.5*1.5)
    return


@app.cell(hide_code=True)
def _(iphs_truck):
    _df = mo.sql(
        f"""
        FROM
            iphs_truck
        SELECT *,CASE WHEN "day" IN ('SUN','PH') THEN (CAST(total_tonnage - overtime_tonnage AS DECIMAL) * 3.5*1.5) + CAST(overtime_tonnage * 3.5 *2.0 AS DECIMAL) ELSE (CAST(total_tonnage - overtime_tonnage AS DECIMAL) * 3.5) + CAST(overtime_tonnage * 3.5 *1.5 AS DECIMAL) END AS price
        WHERE
            YEAR(date) = 2026
            AND MONTH(date) = 2
            AND customer LIKE '%SAPMER%' AND vessel = 'DOLOMIEU'
        """
    )
    return


@app.cell
def _():
    9.29600 - 7.02
    return


@app.cell(hide_code=True)
def _(berth):
    _df = mo.sql(
        f"""
        FROM
            berth
        SELECT
            "VESSEL NAME" AS vessel,
            "DATE IN" AS date_in,
            CAST("TIME IN" AS TIME) AS time_in,
            "DATE OUT" AS date_out,
            CAST("TIME OUT" AS TIME) AS time_out,
            "DURATION IN PORT" AS duration_in_port,
            "INVOICING ENTITY" AS customer,
            "VESSEL TYPE" AS vessel_type,
            "INVOICE VALUE" AS total_amount,
            "COMMENTS" AS remarks
        WHERE time_in  = '00:00'::TIME
        """
    )
    return


@app.cell
def _():
    from dataframe.miscellaneous import cccs_container_stuffing,truck_to_cccs

    return (truck_to_cccs,)


@app.cell(hide_code=True)
def _(truck_to_cccs):
    _df = mo.sql(
        f"""
        FROM truck_to_cccs WHERE customer = 'DARDANEL'
        """
    )
    return


@app.cell(hide_code=True)
def _(cccs_stuffing):
    _df = mo.sql(
        f"""
        FROM
            cccs_stuffing
        SELECT
            -- *
            date,
            ROUND(CAST(SUM(total_tonnage) AS DECIMAL),3) AS tons,
            ROUND(CAST(SUM(total_tonnage) * 3.0 AS DECIMAL),2) AS price
            -- CAST(SUM(total_tonnage) AS DECIMAL) * 3.0
        WHERE
            YEAR(date) = 2026
            AND MONTH(date) = 2
            AND customer = 'OCEAN BASKET'
        GROUP BY date
        ORDER BY date
        """
    )
    return


@app.cell
def _():
    100.254 * 5.0 *1.5
    return


@app.cell
def _():
    from dataframe.transport import forklift

    return (forklift,)


@app.cell
def _():
    forklift_log = load_excel(file_path=ExcelFiles.FORKLIFT_USAGE)

    shifting_log = load_excel(file_path=ExcelFiles.CONTAINER_SHIFTING)
    return forklift_log, shifting_log


@app.cell(hide_code=True)
def _(shifting_log):
    _df = mo.sql(
        f"""
        FROM shifting_log
        """
    )
    return


@app.cell(hide_code=True)
def _(forklift_log):
    _df = mo.sql(
        f"""
        SELECT
            "Date of Service",
            Driver,
            "Forklift No.",
            CAST("Time Out" AS TIME) AS "Time Out",
            CAST("Time In" AS TIME) AS "Time In",
            CAST("Duration" AS TIME) AS "Duration",
            "Vessel/Client",
            "Purpose"
        FROM
            forklift_log
        WHERE
            YEAR("Date of Service") = 2026
            AND "Vessel/Client" = 'EGALABUR'
        ORDER BY "Date of Service","Time Out"
        """
    )
    return


@app.cell
def _(berth, invoice_df):
    _df = mo.sql(
        f"""
        WITH
            berthing AS (
                FROM
                    berth
                SELECT
                    "VESSEL NAME" AS vessel,
                    "DATE IN" AS date_in,
                    CAST("TIME IN" AS TIME) AS time_in,
                    "DATE OUT" AS date_out,
                    CAST("TIME OUT" AS TIME) AS time_out,
                    "INVOICING ENTITY" AS customer,
                    "VESSEL TYPE" AS vessel_type,
                    "COMMENTS" AS remarks
                WHERE
                    Month >= 11

                    AND vessel_type = 'FISHING VESSEL'
            ),
            sto_number AS (
                FROM
                    invoice_df
                SELECT
                    "month",
                    sub_type,
                    report_name,
                    customer,
                    starting_date,
                    ending_date
                WHERE
                    report_type = 'sto'
                    AND starting_date BETWEEN '2025-12-01' AND '2025-12-31'
            --AND report_status <> 'Closed'
            )

        SELECT
            s.sub_type,
            b.* EXCLUDE(vessel_type),
            s.*
        FROM
            berthing b
            LEFT JOIN sto_number s ON b.vessel = s.report_name
            AND b.date_in = s.starting_date
        ORDER BY date_in,time_in
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
                    'CCCS (' || REPLACE(REPLACE(REPLACE(customer, ' S.A.', ''), ' SA', ''),' S.A','') || ')' AS destination,
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
             SELECT CAST(r.container_number AS VARCHAR)
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
            -- 2545377
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


@app.cell(hide_code=True)
def _(net_list_dataf):
    _df = mo.sql(
        f"""
        FROM net_list_dataf WHERE vessel = 'BERNICA' AND date = '2025-12-11'
        """
    )
    return


@app.cell
def _():
    80.404 + 18.504 + 69.128 + 42.743 +19.456
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
def _():
    ops = load_excel(ExcelFiles.OPERATIONS_ACTIVITY)
    ops_2026 = load_excel(ExcelFiles.OPERATIONS_ACTIVITY_2026)
    return ops, ops_2026


@app.cell(hide_code=True)
def _(ops):
    _df = mo.sql(
        f"""
        FROM
            ops
        SELECT
            DATE,
            "VESSEL NAME" AS vessel,
            CAST("TOTAL TONNAGE" AS DECIMAL)AS tonnage
        WHERE
            DATE IS NOT NULL
            AND DATE > '2025-10-01'
        ORDER BY DATE
        """
    )
    return


@app.cell(hide_code=True)
def _(net_list_dataf):
    _df = mo.sql(
        f"""
        FROM net_list_dataf WHERE date = '2025-12-09' AND vessel = 'BERNICA'
        """
    )
    return


@app.cell
def _():
    17.45 + 26.895
    return


@app.cell
def _():
    27.686 - 27.736
    return


@app.cell
def _(engine, net_list_dataf, ops_2026):
    _df = mo.sql(
        f"""
        WITH
            o AS (
                FROM
                    ops_2026
                SELECT
                    DATE,
                    "VESSEL NAME" AS vessel,
                    SUM(CAST("TOTAL TONNAGE" AS DECIMAL)) AS tonnage
                WHERE
                    DATE IS NOT NULL
                    -- AND DATE > '2025-10-01'
                GROUP BY
                    DATE,
                    vessel
                ORDER BY
                    DATE
            ),
            n AS (
                SELECT
                    date,
                    CAST(SUM(total_tonnage) AS DECIMAL) AS tonnage,
                    vessel
                FROM
                    net_list_dataf
                WHERE
                    date > '2025-12-31'
                GROUP BY
                    date,
                    vessel
                ORDER BY
                    date
            )



        FROM o LEFT JOIN n ON n.date = o.date AND n.vessel = o.vessel
        SELECT *, (o.tonnage - n.tonnage) AS difference
        WHERE n.date IS NOT NULL AND difference <> 0
        """,
        engine=engine
    )
    return


@app.cell
def _():
    from dataframe.netlist import netList

    return


@app.cell(hide_code=True)
def _(net_list_dataf):
    _df = mo.sql(
        f"""
        SELECT 
            date,
            CAST(SUM(total_tonnage) AS DECIMAL) AS tonnage

        FROM
            net_list_dataf
        WHERE
            vessel = 'PLAYA DE AZKORRI'
            AND MONTH(date) = 12
        GROUP BY 
        	date
        ORDER BY 
        	date
        """
    )
    return


@app.cell(hide_code=True)
def _(net_list_dataf):
    _df = mo.sql(
        f"""
        FROM net_list_dataf WHERE YEAR(date) = 2026 AND vessel = 'BERNICA'
        """
    )
    return


@app.cell
def _():
    23.370 + 17.97 + 14.23
    return


@app.cell
def _():
    from dataframe.shore_handling import salt,forklift_salt
    # from dataframe.transport import forklift
    return forklift_salt, salt


@app.cell(hide_code=True)
def _(forklift):
    _df = mo.sql(
        f"""
        FROM forklift WHERE customer LIKE '%INTERTUNA%'
        """
    )
    return


@app.cell(hide_code=True)
def _(salt):
    _df = mo.sql(
        f"""
        FROM salt WHERE customer LIKE '%ATUNSA NV%' AND date >= '2025-12-01'
        """
    )
    return


@app.cell
def _():
    76 + 44.869565217391305 + 80 + 80
    return


@app.cell
def _():
    return


@app.cell
def _(forklift_salt):
    f_salt = forklift_salt()
    return (f_salt,)


@app.cell(hide_code=True)
def _(f_salt):
    _df = mo.sql(
        f"""
        FROM f_salt
        """
    )
    return


@app.cell
def _():
    from dataframe.transport import shore_crane,transfer

    return shore_crane, transfer


@app.cell
def _(transfer):
    _df = mo.sql(
        f"""
        FROM
            transfer
        WHERE
            container_number IN (
                'MNBU3735646',

            )
            AND date BETWEEN '2025-12-01' AND '2025-12-31'
        AND status = 'Full'
        """
    )
    return


@app.cell(hide_code=True)
def _(shore_crane):
    _df = mo.sql(
        f"""
        FROM shore_crane WHERE date BETWEEN '2025-12-01' AND '2025-12-31'
        """
    )
    return


@app.cell
def _():
    sheet_id = "1L9qkq9WlIa2j5DcvoLvxkqYogRg76S-e8OxAIyLruAE"
    pre_trip = "ContainerPTI"
    gate_in_s = "ContainerGateIn"
    gate_out_s = "ContainerGateOut"
    plug_in = "ContainerPlugIn"
    washing = "ContainerCleaning"
    return gate_in_s, pre_trip, sheet_id, washing


@app.cell
def _(gate_in_s, pre_trip, sheet_id, washing):
    washing_log = load_gsheet_data(sheet_id=sheet_id,sheet_name=washing).unwrap()
    gate_in_log = load_gsheet_data(sheet_id=sheet_id,sheet_name=gate_in_s).unwrap()
    pti_log = load_gsheet_data(sheet_id=sheet_id,sheet_name=pre_trip).unwrap()
    return gate_in_log, pti_log, washing_log


@app.cell(hide_code=True)
def _(gate_in_log, pti_log):
    _df = mo.sql(
        f"""
        WITH
            pti AS (
                FROM
                    pti_log
                SELECT
                    "Date Plugin",
                    "Container Number",
                    "Set Point",
                    "Unit Manufacturer"
                WHERE
                    "Date Plugin" BETWEEN '2026-01-01' AND '2026-01-31'
                    AND "Shipping Line" = 'CMA CGM'
            ),
            gate AS (
                FROM
                    gate_in_log
            )

        SELECT 
            	p.*,
            	g."Date",
            	g."Type",
            	g."Unit Manufacturer"
        FROM
            pti p

            LEFT JOIN gate g ON p."Date Plugin" >= g."Date"
            AND p."Container Number" = g."Container Number"
        """
    )
    return


@app.cell
def _():
    2768774
    return


@app.cell(hide_code=True)
def _(washing_log):
    _df = mo.sql(
        f"""
        FROM washing_log WHERE date BETWEEN '2026-01-01' AND '2026-01-31'
        """
    )
    return


@app.cell
def _():
    transfer_log = load_excel(file_path=ExcelFiles.CONTAINER_TRANSFER)
    return (transfer_log,)


@app.cell(hide_code=True)
def _(transfer_log):
    _df = mo.sql(
        f"""
        FROM transfer_log WHERE date BETWEEN '2026-01-01' AND '2026-01-31' AND "Line/Client" = 'IOT'
        """
    )
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    from dataframe.miscellaneous import cross_stuffing

    return (cross_stuffing,)


@app.cell(hide_code=True)
def _(cross_stuffing):
    _df = mo.sql(
        f"""
        FROM cross_stuffing WHERE YEAR(date) = 2026 AND MONTH(date) = 2 and invoiced = 'IOT'
        """
    )
    return


@app.cell
def _():
    (7.481) * 3.5
    return


@app.cell
def _():
    from dataframe.stuffing import coa 

    return (coa,)


@app.cell(hide_code=True)
def _(coa):
    _df = mo.sql(
        f"""
        SELECT
            *
        FROM
            coa
        WHERE
            date_plugged BETWEEN '2025-12-01' AND '2026-12-31'
            AND container_number = 'SUDU8108177'
        	-- AND operation_type = 'Basic OSS'
        """
    )
    return


@app.cell
def _():
    (17*70) *3
    return


@app.cell(hide_code=True)
def _(net_list_dataf):
    _df = mo.sql(
        f"""
        FROM net_list_dataf WHERE remarks = 'AMIRANTE'
        """
    )
    return


@app.cell
def _():
    stuffing_log = load_excel(file_path=ExcelFiles.CONTAINER_OPERATIONS)
    return


@app.cell(hide_code=True)
def _(stuffing):
    _df = mo.sql(
        f"""
        FROM stuffing WHERE container_number = 'TTNU8056832' --WHERE "Date affected" BETWEEN '2025-12-01' AND '2025-12-31' AND "Shipping Line" = 'IOT'
        """
    )
    return


@app.cell
def _():
    from dataframe.miscellaneous import cccs_stuffing

    return (cccs_stuffing,)


@app.cell(hide_code=True)
def _(cccs_stuffing):
    _df = mo.sql(
        f"""
        FROM cccs_stuffing WHERE "date" BETWEEN '2026-02-01' AND '2026-02-28'
        """
    )
    return


@app.cell
def _():
    (19.371)*(3.5+3.0)
    return


@app.cell
def _():
    stuffing_cccs  = pl.read_excel(source=r"A:\INVOICING!!\2025\ShippingLines\MAERSK\12 - December\OSS\16 - S.I. Maersk OSS AMIRANTE - CCCS - December '25\AMIRANTE CCCS - December '25.xlsx",sheet_name="CCCS Container Stuffing",schema_overrides={"Tonnage":pl.Float64,"Overtime Tonnage":pl.Float64})
    return (stuffing_cccs,)


@app.cell(hide_code=True)
def _(stuffing_cccs):
    _df = mo.sql(
        f"""
        WITH
            base AS (
                SELECT
                    "Day",
                    "Date affected" AS date_affected,
                    "Container Ref. No." AS container_ref,
                    "Stuffing Method" AS stuffing_method,
                    Price,
                    COALESCE("Tonnage", 0) AS tonnage,
                    COALESCE("Overtime Tonnage", 0) AS overtime_tonnage,
                    total_price
                FROM
                    stuffing_cccs
                WHERE
                    date_affected IS NOT NULL
            ),
            result AS (
                SELECT
                    "Day",
                    date_affected,
                    container_ref,
                    stuffing_method,
                    Price,
                    tonnage_type,
                    tonnage_value AS tonnage,
                FROM
                    base
                    CROSS JOIN LATERAL (
                        VALUES
                            ('Normal', tonnage - overtime_tonnage),
                            ('Overtime', overtime_tonnage)
                    ) v (tonnage_type, tonnage_value)
                    -- optional: drop zero lines (common for overtime=0)
                WHERE
                    tonnage_value <> 0
                ORDER BY
                    date_affected,
                    container_ref,
                    tonnage_type
            ),
            add_ot_text AS (
                SELECT
                    *,
                    CASE
                        WHEN Day IN ('Sun', 'PH')
                        AND tonnage_type = 'Overtime' THEN 'Overtime 200%'
                        WHEN (
                            Day IN ('Sun', 'PH')
                            AND tonnage_type = 'Normal'
                        )
                        OR (
                            Day NOT IN ('Sun', 'PH')
                            AND tonnage_type = 'Overtime'
                        ) THEN 'Overtime 150%'
                        WHEN (
                            Day NOT IN ('Sun', 'PH')
                            AND tonnage_type = 'Normal'
                        ) THEN 'Normal Hours'
                        ELSE 'Error'
                    END AS overtime,
                FROM
                    result
            ),add_total_price AS (


        SELECT
            *,
            CASE
                WHEN overtime = 'Normal Hours' THEN CAST(Price * tonnage * 1.0 AS DECIMAL)
                WHEN overtime = 'Overtime 150%' THEN CAST(Price * tonnage * 1.5 AS DECIMAL)
                WHEN overtime = 'Overtime 200%' THEN CAST(Price * tonnage * 2.0 AS DECIMAL)
                ELSE 0.0::DECIMAL
            END AS total_price
        FROM
            add_ot_text)

        SELECT
          stuffing_method,
          overtime,
          CAST(SUM(tonnage) AS DECIMAL(18,3))      AS tonnage,
          CAST(SUM(total_price) AS DECIMAL(18,2))  AS total_usd
        FROM add_total_price
        GROUP BY stuffing_method, overtime
        ORDER BY
          stuffing_method,
          CASE overtime
            WHEN 'Normal Tonnage' THEN 1
            WHEN 'Overtime 150%'  THEN 2
            WHEN 'Overtime 200%'  THEN 3
            ELSE 9
          END;
        """
    )
    return


@app.cell
def _():
    scow_tranfer = load_excel(ExcelFiles.SCOW_TRANSFER)
    return (scow_tranfer,)


@app.cell(hide_code=True)
def _(scow_tranfer):
    _df = mo.sql(
        f"""
        FROM scow_tranfer WHERE YEAR(Date) = 2026 AND MONTH(Date) = 2
        """
    )
    return


@app.cell
def _():
    salt_log = load_excel(file_path=ExcelFiles.SALT_HANDLING)
    return (salt_log,)


@app.cell
def _(salt_log):
    salt_log.filter(pl.col("Tue 17/01").dt.year().eq(2026)).collect()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
