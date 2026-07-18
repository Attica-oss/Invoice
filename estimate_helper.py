import marimo

__generated_with = "0.23.1"
app = marimo.App(width="columns")

with app.setup:
    from datetime import date
    from calendar import monthrange
    import polars as pl
    import marimo as mo
    from dataframe import coa,by_catch,transfer,cccs_stuffing,shore_crane


@app.function
def eomonth(dt: str) -> date:
    converted_date = date.fromisoformat(dt)
    last_day = monthrange(converted_date.year, converted_date.month)[1]
    return converted_date.replace(day=last_day)


@app.cell
def _():
    select_report = mo.ui.dropdown(
        label="Select Line", options=["AMIRANTE", "ISLAND CATCH", "OCEAN BASKET"], value="AMIRANTE"
    )

    month_options = {
        "Jan 2026": "2026-01-01",
        "Feb 2026": "2026-02-01",
        "Mar 2026": "2026-03-01",
        "Apr 2026": "2026-04-01",
        "May 2026": "2026-05-01",
        "Jun 2026": "2026-06-01",
        "Jul 2026": "2026-07-01",
        "Aug 2026": "2026-08-01"
    }


    month_selector = mo.ui.dropdown(
        options=month_options,
        label="Select invoice month",
        value=date.today().replace(day=1).strftime(format="%b %Y"),
    )


    mo.vstack([select_report, month_selector])
    return month_selector, select_report


@app.cell(hide_code=True)
def _(month_selector, select_report):
    truck_to_cccs_df = mo.sql(
        f"""
        WITH main_data AS (FROM
            by_catch
        SELECT
            day_name,
            date,
            vessel,
            CASE
                WHEN operation_type <> 'Transfer of By-catch' THEN 'Using ' || customer || '\''s Truck'
                ELSE operation_type
            END AS operation_type,
            CASE
                WHEN unit_price <> 7.5 THEN unit_price
                ELSE 0
            END AS entrance_fee,
            CASE
                WHEN unit_price = 7.5 THEN unit_price
                ELSE 0
            END AS unloading_fee,
            total_tonnage,
            CASE
                WHEN day_name NOT IN ('PH', 'Sun') THEN (total_tonnage - overtime_tonnage)::DECIMAL
                ELSE 0
            END AS normal_hour_tonnage,
            CASE
                WHEN day_name IN ('PH', 'Sun') THEN (total_tonnage - overtime_tonnage)::DECIMAL
                WHEN day_name NOT IN ('Sun', 'PH') THEN overtime_tonnage::DECIMAL
                ELSE 0
            END AS overtime_150_tonnage,
            CASE
                WHEN day_name IN ('PH', 'Sun') THEN overtime_tonnage::DECIMAL
                ELSE 0
            END AS overtime_200_tonnage,
            -- overtime_tonnage,
            -- unit_price,
            total_price::DECIMAL AS total_price
        WHERE
            customer = '{select_report.value}'
            AND (
                date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}')
            ))

        FROM main_data
        SELECT 
        day_name,
        date,
        operation_type,
        entrance_fee,
            unloading_fee,
            SUM(total_tonnage)::DECIMAL AS total_tonnage,
                SUM(normal_hour_tonnage)::DECIMAL AS normal_hour_tonnage,
                SUM(overtime_150_tonnage)::DECIMAL AS overtime_150_tonnage,
                SUM(overtime_200_tonnage)::DECIMAL AS overtime_200_tonnage,
            SUM(total_price)::DECIMAL AS total_price,
        STRING_AGG(vessel,', ') AS vessels
        GROUP BY ALL
        ORDER BY date
        """,
        output=False
    )
    return (truck_to_cccs_df,)


@app.cell(hide_code=True)
def _(month_selector, select_report):
    haulage_df = mo.sql(
        f"""
        FROM
            transfer
        SELECT
            day_name,
            container_number,
            date,
            movement_type,
            destination,
            status,
            time_out,
            CASE
                WHEN driver LIKE '%IPHS%' THEN 'IPHS'
                ELSE driver
            END AS driver,
            "type",
            "size",
            remarks,
            haulage_price AS price_per_move
        WHERE
            remarks = '{select_report.value}'
            AND (
                date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}')
            )
        """,
        output=False
    )
    return (haulage_df,)


@app.cell(hide_code=True)
def _(month_selector, select_report):
    electricity_df = mo.sql(
        f"""
        WITH
            main_data AS (
                SELECT
                    *,
                    GREATEST(date_plugged, DATE '{month_selector.value}') AS date_start,
                    LEAST(
                        COALESCE(date_out, LAST_DAY(DATE '{month_selector.value}')),
                        LAST_DAY(DATE '{month_selector.value}')
                    ) AS date_stop
                FROM
                    coa
                WHERE
                     (operation_type LIKE '%CCCS%' OR operation_type LIKE '%Exchange%')
                    AND customer = '{select_report.value}'
                    AND date_plugged <= LAST_DAY(DATE '{month_selector.value}')
                    AND (
                        date_out >= DATE '{month_selector.value}'
                        OR date_out IS NULL
                    )
            ),
            added_metrics AS (
                FROM
                    main_data
                SELECT
                    customer,
                    date_plugged,
                    time_plugged,
                    container_number,
                    plugged_status,
                    CASE
                        WHEN (
                            date_out > LAST_DAY(DATE '{month_selector.value}')
                            OR date_out IS NULL
                        ) THEN NULL
                        ELSE date_out
                    END AS date_out,
                    CASE
                        WHEN plugged_status = 'Partial'
                        AND "location" = 'For Completion' THEN DATEDIFF('days', date_start, date_stop)
                        ELSE DATEDIFF('days', date_start, date_stop) + 1
                    END AS number_of_days_on_plug,
                    set_point,
                    tonnage,
                    CASE
                        WHEN date_plugged < '{month_selector.value}' THEN 0
                        ELSE plugin_price
                    END AS plugin_price,
                    CASE
                        WHEN (
                            date_out > LAST_DAY(DATE '{month_selector.value}')
                            OR date_out IS NULL
                        ) THEN 0
                        ELSE monitoring_price
                    END AS monitoring_price,
                    electricity_unit_price
            ),add_storage_price AS (
        FROM
            added_metrics
        SELECT * EXCLUDE (electricity_unit_price),
        electricity_unit_price * number_of_days_on_plug AS storage_price)


        FROM add_storage_price
        SELECT *, storage_price + monitoring_price + plugin_price AS total_price
        """,
        output=False
    )
    return (electricity_df,)


@app.cell(hide_code=True)
def _(month_selector, select_report):
    shore_crane_df = mo.sql(
        f"""
        FROM shore_crane
            SELECT * EXCLUDE('invoiced_to',operation_type)
        WHERE invoiced_to = '{select_report.value}' AND operation_type = 'CCCS Container Stuffing'  AND (
                date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}')
            )
        """,
        output=False
    )
    return (shore_crane_df,)


@app.cell(hide_code=True)
def _(month_selector, select_report):
    container_stuffing_df = mo.sql(
        f"""
        FROM
            cccs_stuffing
        SELECT
            day_name,
            date,
            container_number,
            customer,
            service,
            total_tonnage,
            overtime_tonnage,
            unit_price,
            total_price::DECIMAL AS total_price
        WHERE
            invoiced = '{select_report.value}'
            AND (
                date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}')
            )
        """,
        output=False
    )
    return (container_stuffing_df,)


@app.cell(hide_code=True)
def _(
    container_stuffing_df,
    electricity_df,
    haulage_df,
    shore_crane_df,
    truck_to_cccs_df,
):
    summary_df = mo.sql(
        f"""
        WITH
            tipping_normal AS (
                FROM
                    truck_to_cccs_df
                SELECT
                    COALESCE(SUM(normal_hour_tonnage), 0) AS QTY,
                    'TIPPING TRUCK' AS Description,
                    'STD' AS Variant
                WHERE
                    unloading_fee <> 0
                GROUP BY ALL
            ),
            tipping_ot_1 AS (
                FROM
                    truck_to_cccs_df
                SELECT
                    COALESCE(SUM(overtime_150_tonnage), 0) AS QTY,
                    'TIPPING TRUCK' AS Description,
                    '150%' AS Variant
                WHERE
                    unloading_fee <> 0
                GROUP BY ALL
            ),tipping_ot_2 AS (
                FROM
                    truck_to_cccs_df
                SELECT
                    COALESCE(SUM(overtime_200_tonnage), 0) AS QTY,
                    'TIPPING TRUCK' AS Description,
                    '200%' AS Variant
                WHERE
                    unloading_fee <> 0
                GROUP BY ALL
            ),
            ---BY CATCH
           by_catch_normal AS (
                FROM
                    truck_to_cccs_df
                SELECT
                    COALESCE(SUM(normal_hour_tonnage), 0) AS QTY,
                    'BY CATCH' AS Description,
                    'STD' AS Variant
                WHERE
                    unloading_fee = 0
                GROUP BY ALL
            ),by_catch_ot_1 AS (
                FROM
                    truck_to_cccs_df
                SELECT
                    COALESCE(SUM(overtime_150_tonnage), 0) AS QTY,
                    'BY CATCH' AS Description,
                    '150%' AS Variant
                WHERE
                    unloading_fee = 0
                GROUP BY ALL
            ),by_catch_ot_2 AS (
                FROM
                    truck_to_cccs_df
                SELECT
                    COALESCE(SUM(overtime_200_tonnage), 0) AS QTY,
                    'BY CATCH' AS Description,
                    '200%' AS Variant
                WHERE
                    unloading_fee = 0
                GROUP BY ALL
            ),
            ---- Haulage
           normal_haulage AS (FROM
            haulage_df
        SELECT
            COUNT(*) AS QTY,
            'TRANSFER FEU' AS Description,
            'STD' AS Variant
        WHERE
            day_name NOT IN ('Sun', 'PH')
            AND time_out::TIME <= '17:00'::TIME),
        ot_1_haulage AS (
            FROM haulage_df
            SELECT 
            	COUNT(*) AS QTY,
            'TRANSFER FEU' AS Description,
            '150%' AS Variant
        WHERE
            (day_name NOT IN ('Sun', 'PH')
            AND time_out::TIME > '17:00'::TIME) OR (day_name IN ('Sun', 'PH')
            AND time_out::TIME <= '16:00'::TIME)
        ),ot_2_haulage AS (
            FROM haulage_df
            SELECT 
            	COUNT(*) AS QTY,
            'TRANSFER FEU' AS Description,
            '200%' AS Variant
        WHERE
           day_name IN ('Sun', 'PH')
            AND time_out::TIME > '16:00'::TIME),
            ---ELECTRICITY

         plugin AS (
                FROM
                    electricity_df
                SELECT
                    COUNT(*) AS QTY,
                    'PLUGIN' AS Description,
                    'STD' AS Variant
                WHERE
                    plugin_price <> 0
            ),
            monitoring AS (
                FROM
                    electricity_df
                SELECT
                    COUNT(*) AS QTY,
                    'MONITORING' AS Description,
                    'STD' AS Variant
                WHERE
                    monitoring_price <> 0
            ),
            electricity_25 AS (
                FROM
                    electricity_df
                SELECT
                    COALESCE(SUM(number_of_days_on_plug), 0) AS QTY,
                    'ELECTRICITY - 25°' AS Description,
                    'STD' AS Variant
                WHERE
                    set_point = -25
            ),
            electricity_35 AS (
                FROM
                    electricity_df
                SELECT
                    COALESCE(SUM(number_of_days_on_plug), 0) AS QTY,
                    'ELECTRICITY - 35°' AS Description,
                    'STD' AS Variant
                WHERE
                    set_point = -35
            ),
            electricity_60 AS (
                FROM
                    electricity_df
                SELECT
                    COALESCE(SUM(number_of_days_on_plug), 0) AS QTY,
                    'ELECTRICITY - 60°' AS Description,
                    'STD' AS Variant
                WHERE
                    set_point = -60
            ),
            -- Stuffing
            explicit_tonnage AS (
                FROM
                    container_stuffing_df
                SELECT
                    *,
                    CASE
                        WHEN day_name NOT IN ('Sun', 'PH') THEN (total_tonnage - overtime_tonnage)::DECIMAL
                        ELSE 0
                    END AS normal_hour_tonnage,
                    CASE
                        WHEN day_name NOT IN ('Sun', 'PH') THEN overtime_tonnage::DECIMAL
                        WHEN day_name IN ('Sun', 'PH') THEN (total_tonnage - overtime_tonnage)::DECIMAL
                        ELSE 0
                    END AS ot_1_tonnage,
                    CASE
                        WHEN day_name IN ('Sun', 'PH') THEN overtime_tonnage::DECIMAL
                        ELSE 0
                    END AS ot_2_tonnage
            ),
            normal_static AS (
                FROM
                    explicit_tonnage
                SELECT
                    COALESCE(SUM(normal_hour_tonnage),0) AS QTY,
                    'STUFFING - STATIC LOADER' AS Description,
                    'STD' AS Variant
                WHERE
                    service = 'Static Loader'
            ),
            ot_1_static AS (
                FROM
                    explicit_tonnage
                SELECT
                    COALESCE(SUM(ot_1_tonnage),0) AS QTY,
                    'STUFFING - STATIC LOADER' AS Description,
                    '150%' AS Variant
                WHERE
                    service = 'Static Loader'
            ),
            ot_2_static AS (
                FROM
                    explicit_tonnage
                SELECT
                    COALESCE(SUM(ot_2_tonnage),0) AS QTY,
                    'STUFFING - STATIC LOADER' AS Description,
                    '200%' AS Variant
                WHERE
                    service = 'Static Loader'
            )

            --- Shore Crane and Fish Loader
            ,
            normal_shore AS (
                FROM
                    explicit_tonnage
                SELECT
                    COALESCE(SUM(normal_hour_tonnage),0) AS QTY,
                    'SHORE CRANE + FISHLOADER' AS Description,
                    'STD' AS Variant
                WHERE
                    service = 'Shore Crane & Fishloader'
            ),
            ot_1_shore AS (
                FROM
                    explicit_tonnage
                SELECT
                    COALESCE(SUM(ot_1_tonnage),0) AS QTY,
                    'SHORE CRANE + FISHLOADER' AS Description,
                    '150%' AS Variant
                WHERE
                    service = 'Shore Crane & Fishloader'
            ),
            ot_2_shore AS (
                FROM
                    explicit_tonnage
                SELECT
                    COALESCE(SUM(ot_2_tonnage),0) AS QTY,
                    'SHORE CRANE + FISHLOADER' AS Description,
                    '200%' AS Variant
                WHERE
                    service = 'Shore Crane & Fishloader'
            )
            --- Hand
            ,normal_hand AS (
                FROM
                    explicit_tonnage
                SELECT
                    COALESCE(SUM(normal_hour_tonnage),0) AS QTY,
                    'STUFFING - BY HAND' AS Description,
                    'STD' AS Variant
                WHERE
                    service = 'Container Stuffing by Hand'
            ),
            ot_1_hand AS (
                FROM
                    explicit_tonnage
                SELECT
                    COALESCE(SUM(ot_1_tonnage),0) AS QTY,
                    'STUFFING - BY HAND' AS Description,
                    '150%' AS Variant
                WHERE
                    service = 'Container Stuffing by Hand'
            ),
            ot_2_hand AS (
                FROM
                    explicit_tonnage
                SELECT
                    COALESCE(SUM(ot_2_tonnage),0) AS QTY,
                    'STUFFING - BY HAND' AS Description,
                    '200%' AS Variant
                WHERE
                    service = 'Container Stuffing by Hand'
            ),
            -- SHore Crane
            main AS (
                FROM
                    shore_crane_df
                SELECT
                    *,
                    CASE
                        WHEN day_name NOT IN ('Sun', 'PH') THEN "hours" - overtime_hours
                        ELSE 0
                    END AS normal_hours,
                    CASE
                        WHEN day_name NOT IN ('Sun', 'PH') THEN overtime_hours
                        WHEN day_name IN ('Sun', 'PH') THEN "hours" - overtime_hours
                        ELSE 0
                    END AS ot_1_hours,
                    CASE
                        WHEN day_name IN ('Sun', 'PH') THEN overtime_hours
                        ELSE 0
                    END AS ot_2_hours
            ),
              normal AS (
                FROM
                    main
                SELECT
                    COALESCE(SUM(normal_hours),0) AS QTY,
                    'SHORE CRANE RENTAL' AS Description,
                    'STD' AS Variant
            ),
            ot_1 AS (
                FROM
                    main
                SELECT
                    COALESCE(SUM(ot_1_hours),0) AS QTY,
                    'SHORE CRANE RENTAL' AS Description,
                    'OT1' AS Variant
            ),
            ot_2 AS (
                FROM
                    main
                SELECT
                    COALESCE(SUM(ot_2_hours),0) AS QTY,
                    'SHORE CRANE RENTAL' AS Description,
                    'OT2' AS Variant
            )









        FROM
            tipping_normal
        UNION ALL
        FROM
            tipping_ot_1
        UNION ALL
        FROM 
        tipping_ot_2
        UNION ALL
        FROM by_catch_normal
        UNION ALL
        FROM by_catch_ot_1
        UNION ALL
        FROM by_catch_ot_2
        UNION ALL
        FROM normal_haulage
        UNION ALL
        FROM ot_1_haulage
        UNION ALL
        FROM ot_2_haulage
            UNION ALL
        FROM
            plugin
        UNION ALL
        FROM
            monitoring
        UNION ALL
        FROM
            electricity_25
        UNION ALL
        FROM
            electricity_35
        UNION ALL
        FROM
            electricity_60
        UNION ALL
        FROM
            normal_static
        UNION ALL
        FROM
            ot_1_static
        UNION ALL
        FROM
            ot_2_static
        UNION ALL
        FROM
            normal_shore
        UNION ALL
        FROM
            ot_1_shore
        UNION ALL
        FROM
            ot_2_shore
        UNION ALL
        FROM
            normal_hand
        UNION ALL
        FROM
            ot_1_hand
        UNION ALL
        FROM
            ot_2_hand
        UNION ALL
        FROM
            normal
        UNION ALL
        FROM
            ot_1
        UNION ALL
        FROM ot_2
        """,
        output=False
    )
    return (summary_df,)


@app.cell
def _():
    price_df = pl.read_excel(r"C:\Users\gmounac\Downloads\price.xlsx")
    return (price_df,)


@app.cell(hide_code=True)
def _(price_df, summary_df):
    pricing_df = mo.sql(
        f"""
        WITH bc AS (FROM
            price_df
        WHERE
            Description IN (
                'TIPPING TRUCK',
                'BY CATCH',
                'STUFFING - STATIC LOADER',
                'SHORE CRANE + FISHLOADER',
                'STUFFING - BY HAND',
                'PLUGIN',
                'MONITORING',
                'ELECTRICITY - 25°',
                'ELECTRICITY - 35°',
                'ELECTRICITY - 60°',
                'SHORE CRANE RENTAL',
                'TRANSFER FEU'
            )),summaries AS (FROM summary_df)


        SELECT 
            b.Type,
            b."No.",
            b.Description,
            b.Variant,
            s.QTY,
            b."Unit Price"
        FROM bc b LEFT JOIN summaries s ON s.Description = b.Description AND s.Variant = b.Variant
        """,
        output=False
    )
    return (pricing_df,)


@app.cell(hide_code=True)
def _(pricing_df):
    _df = mo.sql(
        f"""
        FROM pricing_df
            SELECT * EXCLUDE("Unit Price")
        WHERE QTY > 0
        """
    )
    return


@app.cell(hide_code=True)
def _(pricing_df):
    _df = mo.sql(
        f"""
        With a AS (FROM pricing_df

        SELECT Description,Variant,"Unit Price",QTY,("Unit Price" * QTY)::DECIMAL AS total_price
        WHERE QTY > 0)

        FROM a 
         SELECT SUM(total_price)::DECIMAL AS total_price
        """
    )
    return


@app.cell
def _(month_selector):
    eomonth(month_selector.value).isoformat()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
