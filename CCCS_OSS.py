import marimo

__generated_with = "0.23.1"
app = marimo.App(width="columns")

with app.setup:
    from datetime import date
    import re
    from calendar import monthrange
    import polars as pl
    from typing import Any
    import polars.selectors as cs
    import xlsxwriter
    from pathlib import Path
    import marimo as mo
    from dataframe import cccs_stuffing, coa, transfer, shore_crane


@app.function
def eomonth(dt: str) -> date:
    converted_date = date.fromisoformat(dt)
    last_day = monthrange(converted_date.year, converted_date.month)[1]
    return converted_date.replace(day=last_day)


@app.cell
def _():
    select_report = mo.ui.dropdown(
        label="Select Line",
        options=[
            "AMIRANTE",
            "CFTO",
            "OCEAN BASKET",
            "JMARR",
            "SAPMER",
            "ISLAND CATCH",
            "ATUNSA",
            "INPESCA",
            "ECHEBASTAR",
            "IOT",
        ],
        value="AMIRANTE",
    )

    month_options = {
        "Jan 2026": "2026-01-01",
        "Feb 2026": "2026-02-01",
        "Mar 2026": "2026-03-01",
        "Apr 2026": "2026-04-01",
        "May 2026": "2026-05-01",
        "Jun 2026": "2026-06-01",
        "Jul 2026": "2026-07-01",
        "Aug 2026": "2026-08-01",
        "Sep 2026": "2026-09-01",
        "Oct 2026": "2026-10-01",
        "Nov 2026": "2026-11-01",
        "Dec 2026": "2026-12-01",
    }


    month_selector = mo.ui.dropdown(
        options=month_options,
        label="Select invoice month",
        value=date.today().replace(day=1).strftime(format="%b %Y"),
    )
    return month_selector, select_report


@app.cell
def _(
    electricity_summary_df,
    haulage_summary_df,
    shore_crane_summary_df,
    stuffing_summary_df,
):
    haulage_collection = (
        haulage_summary_df.filter(pl.col("remarks") == "Haulage Collection")
        .select(pl.col("number"))
        .item()
    )

    haulage_delivery = (
        haulage_summary_df.filter(pl.col("remarks") == "Haulage Delivery")
        .select(pl.col("number"))
        .item()
    )

    total_moves = haulage_summary_df.select(pl.col("number").sum()).item()

    shore_crane_hours = shore_crane_summary_df.item()

    number_stuffed = stuffing_summary_df.select(
        pl.col("number_stuffed").cast(pl.Int32)
    ).item()

    days_plugged = electricity_summary_df.select(
        pl.col("days_plugged").cast(pl.Int32)
    ).item()

    total_tonnage = stuffing_summary_df.select(pl.col("tonnage")).item()
    return (
        days_plugged,
        haulage_collection,
        haulage_delivery,
        number_stuffed,
        shore_crane_hours,
        total_moves,
        total_tonnage,
    )


@app.cell
def _(
    days_plugged,
    haulage_collection,
    haulage_delivery,
    number_stuffed,
    shore_crane_hours,
    total_moves,
    total_tonnage,
):
    summary_table = mo.Html(
        f"""
        <table style="
            margin: 0 auto;
            border-collapse: collapse;
            font-size: 16px;
            line-height: 1.45;
        ">
            <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    Haulage Collection
                </td>
                <td style="text-align:right; min-width:55px; font-weight:bold;">
                    {haulage_collection if haulage_collection else "-"}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    {"Move" if haulage_collection == 1 else "Moves"}
                </td>
            </tr>

            <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    Haulage Delivery
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {haulage_delivery}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    {"Move" if haulage_delivery == 1 else "Moves"}
                </td>
            </tr>

            <tr>
                <td style="text-align:right; padding-right:28px;">
                    Total Moves
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {total_moves}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    {"Move" if total_moves == 1 else "Moves"}
                </td>
            </tr>

            <tr>
                <td colspan="3" style="height:18px;"></td>
            </tr>

            <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    Hours of Service
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {shore_crane_hours}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Hours
                </td>
            </tr>

            <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    Total No of Container Stuffed
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {number_stuffed}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Units
                </td>
            </tr>

            <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    No of Days / Storage (Live)
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {days_plugged}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Days
                </td>
            </tr>

            <tr>
                <td colspan="3" style="height:18px;"></td>
            </tr>

            <tr>
                <td style="text-align:right; padding-right:28px;">
                    Total Tonnage (CCCS to Container)
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {total_tonnage:,.3f}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Tons
                </td>
            </tr>
        </table>
        """
    )
    return (summary_table,)


@app.cell
def _(service_breakdown_df):
    grand_total = service_breakdown_df.select(pl.col("total").sum()).item()
    return


@app.cell
def _(metrics_df, month_selector, select_report, summary_table):
    # --- Header / stat cards ---
    title = mo.md(f"# 🚢 CCCS / OSS Report  for — {select_report.value.title()}")

    filter_bar = mo.hstack(
        [month_selector, select_report],
        justify="start",
        gap=2,
    )


    meta = mo.hstack(
        [
            mo.stat(
                label="Month",
                value=format_datestr_to_month_year(month_selector.value),
                caption="Operation date",
            ),
            mo.stat(
                label="Invoice Value",
                value=metrics_df.item(),
                caption="Total Price",
            ),
        ],
        justify="start",
        gap=1,
    )


    mo.vstack(
        [
            title,
            mo.md("---"),
            filter_bar,
            meta,
            mo.md("---"),
            summary_table,
        ]
    )
    return


@app.function
def format_datestr_to_month_year(date_str: str) -> str:
    """Format the datestring to month 'year"""
    month = date.fromisoformat(date_str).strftime(format="%B")
    year = date.fromisoformat(date_str).strftime(format="%y")
    return month + " '" + year


@app.cell(hide_code=True)
def _(pricing_df):
    metrics_df = mo.sql(
        f"""
        With a AS (FROM pricing_df

        SELECT Description,Variant,"Unit Price",QTY,ROUND("Unit Price" * QTY,2) AS total_price
        WHERE QTY > 0)

        FROM a 
         SELECT COALESCE(ROUND(SUM(total_price)::FLOAT8,2),0) AS total_price
        """,
        output=False,
    )
    return (metrics_df,)


@app.cell(hide_code=True)
def _(month_selector, select_report):
    stuffing_df = mo.sql(
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
            invoiced = 'MAERSKLINE' AND 
            customer = '{select_report.value}'
            AND (
                date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}')
            )
        """,
        output=False,
    )
    return (stuffing_df,)


@app.cell(hide_code=True)
def _(stuffing_df):
    stuffing_summary_df = mo.sql(
        f"""
        FROM stuffing_df
        SELECT SUM(total_tonnage) AS tonnage,COUNT(DISTINCT container_number) AS number_stuffed
        GROUP BY ALL
        """,
        output=False,
    )
    return (stuffing_summary_df,)


@app.cell
def _(electricity_df):
    electricity_summary_df = mo.sql(
        f"""
        FROM electricity_df
        SELECT SUM(number_of_days_on_plug) AS days_plugged,COUNT (DISTINCT container_number) AS number_stuffed
        """,
        output=False,
    )
    return (electricity_summary_df,)


@app.cell(hide_code=True)
def _(shore_crane_df):
    shore_crane_summary_df = mo.sql(
        f"""
        FROM shore_crane_df
        SELECT COALESCE(SUM("hours"),0) AS total_hours
        """,
        output=False,
    )
    return (shore_crane_summary_df,)


@app.cell(hide_code=True)
def _(haulage_df):
    haulage_summary_df = mo.sql(
        f"""
        WITH
            del AS (
                FROM
                    haulage_df
                SELECT
                    'Haulage Delivery' AS remarks,
                    COUNT(movement_type) AS number,
            	'Moves' AS moves
                WHERE
                    movement_type = 'Delivery'
            ),
            col AS (
                FROM
                    haulage_df
                SELECT
                    'Haulage Collection' AS remarks,
                    COUNT(movement_type) AS number,
                	'Moves' AS moves
                WHERE
                    movement_type = 'Collection'
            )
        FROM
            col
        UNION ALL
        FROM
            del
        """,
        output=False,
    )
    return (haulage_summary_df,)


@app.cell(hide_code=True)
def _(month_selector, select_report):
    shore_crane_df = mo.sql(
        f"""
        FROM
            shore_crane
        SELECT
            * EXCLUDE ('invoiced_to', operation_type)
        WHERE
            invoiced_to = 'MAERSKLINE'
            AND customer = '{select_report.value}'
            AND operation_type = 'CCCS Container Stuffing'
            AND (
                date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}')
            )
        """,
        output=False,
    )
    return (shore_crane_df,)


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
                    (
                        operation_type LIKE '%CCCS%'
                        OR operation_type LIKE '%Exchange%'
                    )
                    AND customer = 'MAERSKLINE'
                    AND vessel_client LIKE '%{select_report.value}%'
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
                    vessel_client,
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
            ),
            add_storage_price AS (
                FROM
                    added_metrics
                SELECT
                    * EXCLUDE (electricity_unit_price),
                    electricity_unit_price * number_of_days_on_plug AS storage_price
            )
        FROM
            add_storage_price
        SELECT
            vessel_client,
           date_plugged,
            time_plugged,
            container_number,
            plugged_status,
            date_out,
            number_of_days_on_plug,
            set_point,
            tonnage,
            plugin_price,
            monitoring_price,
            storage_price,
            storage_price + monitoring_price + plugin_price AS total_price
        """,
        output=False,
    )
    return (electricity_df,)


@app.cell(hide_code=True)
def _(electricity_df, month_selector, select_report):
    haulage_df = mo.sql(
        f"""
        WITH e AS (
           WITH RECURSIVE

        seeds AS (
            SELECT
                ROW_NUMBER() OVER (
                    ORDER BY
                        container_number,
                        date_plugged,
                        time_plugged,
                        vessel_client
                ) AS seed_id,

                container_number,
                vessel_client AS filtered_vessel_client,
                date_plugged AS filtered_date_plugged,
                time_plugged AS filtered_time_plugged

            FROM electricity_df
        ),

        container_history AS (
            /*
            Start from each row in the filtered dataframe.
            */
            SELECT
                seeds.seed_id,
                seeds.filtered_vessel_client,
                coa.*,
                0 AS chain_level

            FROM seeds

            INNER JOIN coa
                ON coa.container_number = seeds.container_number
                AND coa.date_plugged = seeds.filtered_date_plugged
                AND coa.time_plugged = seeds.filtered_time_plugged

            UNION ALL

            /*
            Move backwards through the container records.
            */
            SELECT
                current_record.seed_id,
                current_record.filtered_vessel_client,
                previous_record.*,
                current_record.chain_level + 1 AS chain_level

            FROM container_history AS current_record

            INNER JOIN coa AS previous_record
                ON previous_record.container_number =
                    current_record.container_number
                AND previous_record.date_out =
                    current_record.date_plugged

            WHERE NOT (
                previous_record.date_plugged =
                    current_record.date_plugged
                AND 
                    previous_record.time_plugged = 
                    current_record.time_plugged

            )
        ),

        first_chain_record AS (
            /*
            Find the earliest record reached for each seed.
            */
            SELECT
                seed_id,
                vessel_client AS first_vessel_client,
                filtered_vessel_client

            FROM container_history

            QUALIFY
                ROW_NUMBER() OVER (
                    PARTITION BY seed_id
                    ORDER BY
                        chain_level DESC,
                        date_plugged ASC,
                        time_plugged ASC
                ) = 1
        ),

        valid_chains AS (
            /*
            Keep the chain only when its first vessel matches
            the vessel in the filtered dataframe.
            */
            SELECT
                seed_id

            FROM first_chain_record

            WHERE
                first_vessel_client = filtered_vessel_client
        ),

        last_chain_record AS (
            /*
            Return the last occurrence from each valid chain.
            */
            SELECT
                history.*

            FROM container_history AS history

            SEMI JOIN valid_chains AS valid
                ON history.seed_id = valid.seed_id

            QUALIFY
                ROW_NUMBER() OVER (
                    PARTITION BY history.seed_id
                    ORDER BY
                        history.date_plugged DESC,
                        history.time_plugged DESC,
                        history.chain_level ASC
                ) = 1
        )

        SELECT
            -- * EXCLUDE (
            --     seed_id,
            --     chain_level,
            --     filtered_vessel_client
            -- )
            container_number,
            date_out

        FROM last_chain_record

        /*
        Multiple filtered rows may resolve to the same final container row.
        Keep one result per container.
        */
        QUALIFY
            ROW_NUMBER() OVER (
                PARTITION BY container_number
                ORDER BY
                    date_plugged DESC,
                    time_plugged DESC
            ) = 1

        ORDER BY
            date_plugged,
            time_plugged,
            container_number
        ),
        t AS (
            FROM transfer
            SELECT day_name,
            container_number,
            date,
            movement_type,
            destination,
            status,
            time_out,
            CASE WHEN driver LIKE '%IPHS%' THEN 'IPHS' ELSE driver END AS driver,
            "type",
            size,
            '{select_report.value}' AS customer,
            haulage_price
            WHERE status = 'Full' AND movement_type = 'Delivery'
        )

        FROM t
        SEMI JOIN e
            ON trim(t.container_number) = trim(e.container_number)
        SELECT
            t.*
        WHERE (
                date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}')
            )
        """,
        output=False,
    )
    return (haulage_df,)


@app.cell(hide_code=True)
def _(electricity_df, haulage_df, shore_crane_df, stuffing_df):
    summary_df = mo.sql(
        f"""
        WITH explicit_tonnage AS (
                FROM
                    stuffing_df
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

            --- Static Loader Services
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
            --- Stuffing by Hand Services
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
            --- Shore Crane
            shore_crane_main AS (
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
              normal_crane AS (
                FROM
                    shore_crane_main
                SELECT
                    COALESCE(SUM(normal_hours),0) AS QTY,
                    'SHORE CRANE RENTAL' AS Description,
                    'STD' AS Variant
            ),
            ot_1_crane AS (
                FROM
                    shore_crane_main
                SELECT
                    COALESCE(SUM(ot_1_hours),0) AS QTY,
                    'SHORE CRANE RENTAL' AS Description,
                    'OT1' AS Variant
            ),
            ot_2_crane AS (
                FROM
                    shore_crane_main
                SELECT
                    COALESCE(SUM(ot_2_hours),0) AS QTY,
                    'SHORE CRANE RENTAL' AS Description,
                    'OT2' AS Variant
            ),
            --- Electricity

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
            --- Haulage

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
            AND time_out::TIME > '16:00'::TIME)


        -- Stuffing
        FROM normal_static
        UNION  ALL
        FROM ot_1_static
        UNION ALL
        FROM ot_2_static
        UNION ALL
        FROM normal_shore
        UNION ALL
        FROM ot_1_shore
        UNION ALL
        FROM ot_2_shore
        UNION ALL
        FROM normal_hand
        UNION ALL
        FROM ot_1_hand
        UNION ALL
        FROM ot_2_hand
        UNION ALL
        -- Shore Crane
        FROM normal_crane
        UNION ALL
        FROM ot_1_crane
        UNION ALL
        FROM ot_2_crane
        -- Electricity

        UNION ALL
        FROM plugin
        UNION ALL
        FROM monitoring

        UNION ALL
        FROM electricity_25
        UNION ALL
        FROM electricity_35
        UNION ALL
        FROM electricity_60
        --- Haulage
           UNION ALL
        FROM normal_haulage
        UNION ALL
        FROM ot_1_haulage
        UNION ALL
        FROM ot_2_haulage
        """,
        output=False,
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
            'TRANSFER FEU',
                'STUFFING - STATIC LOADER',
                'SHORE CRANE + FISHLOADER',
            'STUFFING - BY HAND',
            'SHORE CRANE RENTAL',
            'PLUGIN',
            'MONITORING',
            'ELECTRICITY - 25°',
            'ELECTRICITY - 35°',
            'ELECTRICITY - 60°'
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
        output=False,
    )
    return (pricing_df,)


@app.cell(hide_code=True)
def _(pricing_df):
    _1_service_breakdown_df = mo.sql(
        f"""
        FROM pricing_df
        SELECT *,(QTY * "Unit Price") AS Total
        WHERE QTY >0
        """,
        output=False,
    )
    return


@app.cell
def _():
    copy_button = mo.ui.run_button(label="📋 Copy BC data to clipboard")
    copy_button
    return (copy_button,)


@app.cell
def _(bc_df, copy_button):
    mo.stop(not copy_button.value)
    bc_df.write_clipboard()  # tab-separated, pastes cleanly into BC / Excel
    mo.md("✅ Copied to clipboard")
    return


@app.cell(hide_code=True)
def _(pricing_df):
    bc_df = mo.sql(
        f"""
        FROM pricing_df
            SELECT * EXCLUDE("Unit Price")
        WHERE QTY > 0
        """
    )
    return (bc_df,)


@app.cell(hide_code=True)
def _(pricing_df):
    service_breakdown_df = mo.sql(
        f"""
        WITH mapped AS (
                    FROM pricing_df
                    SELECT
                        CASE Description
                            WHEN 'TRANSFER FEU'             THEN 'Haulage'
                            WHEN 'ELECTRICITY - 25°'        THEN 'Live Storage'
                            WHEN 'ELECTRICITY - 35°'        THEN 'Live Storage'
                            WHEN 'ELECTRICITY - 60°'        THEN 'Live Storage'
                            WHEN 'PLUGIN'                   THEN 'Plugin'
                            WHEN 'MONITORING'               THEN 'Monitoring'
                            ELSE 'CCCS Unloading to Container'
                        END AS section,
                        CASE Description
                            WHEN 'TRANSFER FEU'             THEN '40'''
                            WHEN 'ELECTRICITY - 25°'        THEN '-25'
                            WHEN 'ELECTRICITY - 35°'        THEN '-35'
                            WHEN 'ELECTRICITY - 60°'        THEN '-60'
                            WHEN 'PLUGIN'                   THEN ''
                            WHEN 'MONITORING'               THEN ''
                            WHEN 'STUFFING - STATIC LOADER' THEN 'Stuffing - Static Loader'
                            WHEN 'SHORE CRANE + FISHLOADER' THEN 'Stuffing - Shore Crane'
                            WHEN 'STUFFING - BY HAND'       THEN 'Stuffing - By Hand'
                            WHEN 'SHORE CRANE RENTAL'       THEN 'Crane Rental'
                        END AS service,
                        CASE Description
                            WHEN 'TRANSFER FEU'             THEN 1
                            WHEN 'ELECTRICITY - 25°'        THEN 2
                            WHEN 'ELECTRICITY - 35°'        THEN 3
                            WHEN 'ELECTRICITY - 60°'        THEN 4
                            WHEN 'PLUGIN'                   THEN 5
                            WHEN 'MONITORING'               THEN 6
                            WHEN 'STUFFING - STATIC LOADER' THEN 7
                            WHEN 'SHORE CRANE + FISHLOADER' THEN 8
                            WHEN 'STUFFING - BY HAND'       THEN 9
                            WHEN 'SHORE CRANE RENTAL'       THEN 10
                        END AS sort_order,
                        Variant,
                        COALESCE(QTY, 0) AS QTY,
                        "Unit Price",
                        COALESCE(QTY, 0) * "Unit Price" AS line_total
                )

                FROM mapped
                SELECT
                    section,
                    service,
                    sort_order,
                    -- base rate shown on the invoice; OT variants only affect the total
                    MAX(CASE WHEN Variant = 'STD' THEN "Unit Price" END) AS unit_price,
                    SUM(QTY) AS quantity,
                    ROUND(SUM(line_total), 2) AS total
                GROUP BY section, service, sort_order
                ORDER BY sort_order
        """,
        output=False,
    )
    return (service_breakdown_df,)


@app.cell
def _(electricity_df, haulage_df, month_selector):
    processed_electricity_df = mo.sql(
        f"""
        WITH
            e AS (
                FROM
                    electricity_df
            ),
            f AS (
                FROM
                    haulage_df
            )
        SELECT
           e.date_plugged::DATE + e.time_plugged::TIME AS date_plugged,
        e.container_number,
        e.plugged_status,
        e.date_out,
        f.time_out AS exit_time,
        e.number_of_days_on_plug AS days_on_plug,
        CAST(e.set_point AS VARCHAR) AS set_point,
        CASE WHEN MONTH(e.date_plugged) <> MONTH('{month_selector.value}'::DATE) THEN 0 ELSE e.tonnage END AS tonnage,
        e.plugin_price,
        e.monitoring_price,
        e.storage_price,
        e.total_price,
        CASE WHEN e.date_out IS NULL THEN 'Still On Plug' ELSE '' END AS remarks
        FROM
            e
            LEFT JOIN f ON e.container_number = f.container_number
        ORDER BY date_plugged
        """,
        output=False,
    )
    return (processed_electricity_df,)


@app.cell
def _(
    electricity_summary_df,
    haulage_collection,
    haulage_delivery,
    haulage_df,
    month_selector,
    processed_electricity_df,
    select_report,
    service_breakdown_df,
    shore_crane_df,
    shore_crane_summary_df,
    stuffing_df,
    stuffing_summary_df,
    total_moves,
):
    reports = {
        "Summary": {
            "type": "summary",
            "date_range": f"{format_datestr_to_month_year(month_selector.value)}",
            "client": f"MAERSKLINE - {select_report.value}",
            "summary_rows": [
                {
                    "label": "Haulage Collection",
                    "value": haulage_collection,
                    "unit": "Move" if haulage_collection == 1 else "Moves",
                    "italic": True,
                },
                {
                    "label": "Haulage Delivery",
                    "value": haulage_delivery,
                    "unit": "Move" if haulage_delivery == 1 else "Moves",
                    "italic": True,
                },
                {
                    "label": "Total Moves",
                    "value": total_moves,
                    "unit": "Move" if total_moves == 1 else "Moves",
                },
                None,  # blank row
                {
                    "label": "Hours of Service",
                    "value": shore_crane_summary_df.item(),
                    "unit": "Hours",
                    "italic": True,
                },
                {
                    "label": "Total No of Container Stuffed",
                    "value": (
                        stuffing_summary_df.select(
                            pl.col("number_stuffed").cast(pl.Int32)
                        ).item()
                    ),
                    "unit": "Units",
                    "italic": True,
                },
                {
                    "label": "No of Days / Storage (Live)",
                    "value": (
                        electricity_summary_df.select(
                            pl.col("days_plugged").cast(pl.Int32)
                        ).item()
                    ),
                    "unit": "Days",
                    "italic": True,
                },
                None,
                {
                    "label": "Total Tonnage (CCCS to Container)",
                    "value": (
                        stuffing_summary_df.select(pl.col("tonnage")).item()
                    ),
                    "unit": "Tons",
                    "number_format": "#,##0.000",
                },
            ],
            # Expected columns:
            # section, service, quantity, unit_price, total
            "service_df": service_breakdown_df,
            "landscape": False,
            "pages_wide": 1,
            "pages_tall": 1,
        },
        "CCCS Container Stuffing": {
            "df": stuffing_df,
            "header_color": "#f35f97",
        },
        "Shore Crane": {
            "df": shore_crane_df,
            "header_color": "#1f4e78",
        },
        "Live Storage": {
            "df": processed_electricity_df,
            "header_color": "#006113",
        },
        "Transfer": {
            "df": haulage_df,
            "header_color": "#222b35",
        },
    }
    return (reports,)


@app.function
def clean_table_name(name: str, index: int) -> str:
    """
    Create a valid, workbook-unique Excel table name.

    Excel table names:
    - cannot contain spaces
    - should start with a letter or underscore
    - must be unique within the workbook
    """
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", name)

    if not cleaned or cleaned[0].isdigit():
        cleaned = f"Table_{cleaned}"

    return f"{cleaned}_{index}"


@app.cell
def _():
    def write_summary_sheet(
        workbook: xlsxwriter.Workbook,
        worksheet: xlsxwriter.worksheet.Worksheet,
        config: dict[str, Any],
    ) -> None:
        summary_rows = config.get("summary_rows", [])
        service_df: pl.DataFrame | None = config.get("service_df")

        # -------------------------------------------------
        # Formats
        # -------------------------------------------------

        header_label_format = workbook.add_format(
            {"align": "right", "valign": "vcenter", "bold": True, "font_size": 11}
        )

        header_value_format = workbook.add_format(
            {"align": "center", "valign": "vcenter", "font_size": 11}
        )

        client_box_format = workbook.add_format(
            {
                "align": "center",
                "valign": "vcenter",
                "bold": True,
                "font_color": "#FFFFFF",
                "bg_color": "#17365D",
                "text_wrap": True,
                "border": 1,
                "font_size": 11,
            }
        )

        label_format = workbook.add_format(
            {"align": "right", "valign": "vcenter", "font_size": 11}
        )

        italic_label_format = workbook.add_format(
            {
                "align": "right",
                "valign": "vcenter",
                "italic": True,
                "font_size": 11,
            }
        )

        value_format = workbook.add_format(
            {
                "align": "right",
                "valign": "vcenter",
                "bold": True,
                "font_color": "#000080",
                "font_size": 11,
            }
        )

        unit_format = workbook.add_format(
            {"align": "left", "valign": "vcenter", "italic": True, "font_size": 11}
        )

        services_heading_format = workbook.add_format(
            {
                "align": "center",
                "valign": "vcenter",
                "font_color": "#000080",
                "font_size": 11,
            }
        )

        section_format = workbook.add_format(
            {"align": "left", "valign": "vcenter", "bold": True, "font_size": 11}
        )

        service_name_format = workbook.add_format(
            {
                "align": "right",
                "valign": "vcenter",
                "italic": True,
                "font_size": 11,
            }
        )

        price_header_format = workbook.add_format(
            {
                "align": "center",
                "valign": "vcenter",
                "bold": True,
                "font_color": "#FFFFFF",
                "bg_color": "#17365D",
                "border": 1,
                "font_size": 11,
            }
        )

        currency_symbol_format = workbook.add_format(
            {
                "align": "center",
                "valign": "vcenter",
                "left": 1,
                "top": 1,
                "bottom": 1,
                "font_size": 11,
            }
        )

        currency_value_format = workbook.add_format(
            {
                "align": "right",
                "valign": "vcenter",
                "num_format": "#,##0.00",
                "font_color": "#002060",
                "top": 1,
                "right": 1,
                "bottom": 1,
                "font_size": 11,
            }
        )

        grand_total_symbol_format = workbook.add_format(
            {
                "align": "center",
                "bold": True,
                "top": 1,
                "bottom": 6,
                "font_size": 11,
            }
        )

        grand_total_value_format = workbook.add_format(
            {
                "align": "right",
                "bold": True,
                "num_format": "#,##0.00",
                "font_color": "#002060",
                "top": 1,
                "bottom": 6,
                "font_size": 11,
            }
        )

        # -------------------------------------------------
        # Worksheet layout
        # -------------------------------------------------

        worksheet.hide_gridlines(2)
        worksheet.set_zoom(config.get("sheet_zoom", 90))

        worksheet.set_column("A:A", 4)
        worksheet.set_column("B:B", 30)  # summary labels / section names
        worksheet.set_column("C:C", 26)  # summary values / service names
        worksheet.set_column("D:D", 4)  # $ (unit price)
        worksheet.set_column("E:E", 11)  # unit price
        worksheet.set_column("F:F", 4)  # $ (total)
        worksheet.set_column("G:G", 13)  # total

        # -------------------------------------------------
        # Date range / client header
        # -------------------------------------------------

        current_row = 1

        worksheet.write(current_row, 1, "Date Range:", header_label_format)
        worksheet.write(
            current_row, 2, config.get("date_range", ""), header_value_format
        )
        current_row += 1

        worksheet.set_row(current_row, 30)
        worksheet.write(current_row, 1, "Client:", header_label_format)
        worksheet.write(
            current_row, 2, config.get("client", ""), client_box_format
        )
        current_row += 3  # gap before the summary block

        # -------------------------------------------------
        # Summary block
        # -------------------------------------------------

        for item in summary_rows:
            if item is None:
                current_row += 1
                continue

            label = item["label"]
            value = item.get("value")
            unit = item.get("unit", "")

            selected_label_format = (
                italic_label_format if item.get("italic", False) else label_format
            )

            worksheet.write(current_row, 1, label, selected_label_format)

            if value is None or value == 0:
                worksheet.write(current_row, 2, "-", value_format)
            elif isinstance(value, (int, float)):
                custom_value_format = value_format
                if number_format := item.get("number_format"):
                    custom_value_format = workbook.add_format(
                        {
                            "align": "right",
                            "valign": "vcenter",
                            "bold": True,
                            "font_color": "#000080",
                            "font_size": 11,
                            "num_format": number_format,
                        }
                    )
                worksheet.write_number(
                    current_row, 2, float(value), custom_value_format
                )
            else:
                worksheet.write(current_row, 2, value, value_format)

            worksheet.write(current_row, 3, unit, unit_format)
            current_row += 1

        current_row += config.get("summary_service_gap", 2)

        # -------------------------------------------------
        # Service breakdown table
        # -------------------------------------------------

        if service_df is not None and not service_df.is_empty():
            required_columns = {"section", "service", "unit_price", "total"}
            missing_columns = required_columns.difference(service_df.columns)
            if missing_columns:
                raise ValueError(
                    f"service_df is missing columns: {sorted(missing_columns)}"
                )

            header_row = current_row

            worksheet.write(header_row, 1, "Services", services_heading_format)
            worksheet.merge_range(
                header_row, 3, header_row, 4, "Unit Price ($)", price_header_format
            )
            worksheet.merge_range(
                header_row, 5, header_row, 6, "Total ($)", price_header_format
            )

            current_row += 1
            previous_section: str | None = None

            for record in service_df.iter_rows(named=True):
                section = record.get("section")
                service = record.get("service")
                unit_price = record.get("unit_price")
                total = record.get("total")

                # spacer between sections
                if previous_section is not None and section != previous_section:
                    current_row += 1

                section_display = section if section != previous_section else ""
                previous_section = section

                worksheet.write(current_row, 1, section_display, section_format)
                worksheet.write(current_row, 2, service or "", service_name_format)

                worksheet.write(current_row, 3, "$", currency_symbol_format)
                if unit_price is None:
                    worksheet.write(current_row, 4, "-", currency_value_format)
                else:
                    worksheet.write_number(
                        current_row, 4, float(unit_price), currency_value_format
                    )

                worksheet.write(current_row, 5, "$", currency_symbol_format)
                if total is None or total == 0:
                    worksheet.write(current_row, 6, "-", currency_value_format)
                else:
                    worksheet.write_number(
                        current_row, 6, float(total), currency_value_format
                    )

                current_row += 1

            current_row += 1

            grand_total = service_df.select(pl.col("total").sum()).item() or 0

            worksheet.write(current_row, 5, "$", grand_total_symbol_format)
            worksheet.write_number(
                current_row, 6, float(grand_total), grand_total_value_format
            )

            worksheet.print_area(0, 0, current_row + 1, 6)

        # ----- printed page layout (unchanged from your version) -----
        if config.get("landscape", False):
            worksheet.set_landscape()
        else:
            worksheet.set_portrait()
        worksheet.set_paper(config.get("paper_size", 9))
        worksheet.fit_to_pages(
            config.get("pages_wide", 1), config.get("pages_tall", 1)
        )
        worksheet.set_header(
            config.get("print_header", "&C&B&A"),
            {"margin": config.get("header_margin", 0.3)},
        )
        worksheet.set_footer(
            config.get("print_footer", "&CPage &P of &N"),
            {"margin": config.get("footer_margin", 0.3)},
        )
        if config.get("center_horizontally", True):
            worksheet.center_horizontally()
        worksheet.set_margins(
            left=config.get("margin_left", 0.25),
            right=config.get("margin_right", 0.25),
            top=config.get("margin_top", 0.6),
            bottom=config.get("margin_bottom", 0.6),
        )

        # -------------------------------------------------
        # Printed page layout
        # -------------------------------------------------

        if config.get("landscape", False):
            worksheet.set_landscape()
        else:
            worksheet.set_portrait()

        worksheet.set_paper(config.get("paper_size", 9))

        worksheet.fit_to_pages(
            config.get("pages_wide", 1),
            config.get("pages_tall", 1),
        )

        worksheet.set_header(
            config.get("print_header", "&C&B&A"),
            {
                "margin": config.get("header_margin", 0.3),
            },
        )

        worksheet.set_footer(
            config.get("print_footer", "&CPage &P of &N"),
            {
                "margin": config.get("footer_margin", 0.3),
            },
        )

        if config.get("center_horizontally", True):
            worksheet.center_horizontally()

        worksheet.set_margins(
            left=config.get("margin_left", 0.25),
            right=config.get("margin_right", 0.25),
            top=config.get("margin_top", 0.6),
            bottom=config.get("margin_bottom", 0.6),
        )


    def export_dataframes(
        dataframes: dict[str, dict[str, Any]],
        output_path: str | Path,
    ) -> Path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with xlsxwriter.Workbook(output_path) as workbook:
            for index, (sheet_name, config) in enumerate(
                dataframes.items(),
                start=1,
            ):
                safe_sheet_name = sheet_name[:31]
                worksheet = workbook.add_worksheet(safe_sheet_name)

                # -----------------------------------------
                # Custom summary worksheet
                # -----------------------------------------

                if config.get("type") == "summary":
                    write_summary_sheet(
                        workbook=workbook,
                        worksheet=worksheet,
                        config=config,
                    )
                    continue

                # -----------------------------------------
                # Normal dataframe worksheet
                # -----------------------------------------

                df: pl.DataFrame = config["df"]

                numeric_columns = df.select(cs.numeric()).columns

                header_color = config.get(
                    "header_color",
                    "#4472C4",
                )

                df.write_excel(
                    workbook=workbook,
                    worksheet=worksheet,
                    position="A1",
                    table_name=clean_table_name(
                        sheet_name,
                        index,
                    ),
                    table_style={
                        "style": config.get(
                            "table_style",
                            "None",
                        ),
                        "banded_rows": config.get(
                            "banded_rows",
                            True,
                        ),
                        "first_column": False,
                        "last_column": False,
                    },
                    header_format={
                        "bg_color": header_color,
                        "font_color": config.get(
                            "header_font_color",
                            "#FFFFFF",
                        ),
                        "bold": True,
                        "align": "center",
                        "valign": "vcenter",
                        "text_wrap": True,
                    },
                    column_totals=config.get(
                        "column_totals",
                        numeric_columns,
                    ),
                    dtype_formats={
                        pl.Date: "yyyy-dd-mm",
                        pl.Datetime: "yyyy-dd-mm hh:mm",
                        pl.Float32: ("#,##0.00;[Red]-#,##0.00"),
                        pl.Float64: ("#,##0.00;[Red]-#,##0.00"),
                        pl.Int32: "#,##0;[Red]-#,##0",
                        pl.Int64: "#,##0;[Red]-#,##0",
                    },
                    autofit=True,
                    autofilter=True,
                    freeze_panes=(1, 0),
                    hide_gridlines=True,
                    sheet_zoom=90,
                )

                # Limit excessively wide columns.
                for column_index, column_name in enumerate(df.columns):
                    maximum_length = max(
                        len(column_name),
                        (
                            df[column_name]
                            .cast(pl.String)
                            .fill_null("")
                            .str.len_chars()
                            .max()
                            or 0
                        ),
                    )

                    worksheet.set_column(
                        column_index,
                        column_index,
                        min(maximum_length + 2, 40),
                    )

                worksheet.set_row(0, 24)

                # ---------------------------------
                # Printed page layout configuration
                # ---------------------------------

                if config.get("landscape", True):
                    worksheet.set_landscape()
                else:
                    worksheet.set_portrait()

                worksheet.set_paper(config.get("paper_size", 9))

                worksheet.fit_to_pages(
                    config.get("pages_wide", 1),
                    config.get("pages_tall", 1),
                )

                worksheet.set_header(
                    config.get(
                        "print_header",
                        "&C&B&A",
                    ),
                    {
                        "margin": config.get(
                            "header_margin",
                            0.3,
                        ),
                    },
                )

                worksheet.set_footer(
                    config.get(
                        "print_footer",
                        "&CPage &P of &N",
                    ),
                    {
                        "margin": config.get(
                            "footer_margin",
                            0.3,
                        ),
                    },
                )

                if config.get(
                    "center_horizontally",
                    True,
                ):
                    worksheet.center_horizontally()

                worksheet.set_margins(
                    left=config.get("margin_left", 0.25),
                    right=config.get("margin_right", 0.25),
                    top=config.get("margin_top", 0.6),
                    bottom=config.get("margin_bottom", 0.6),
                )

        return output_path

    return (export_dataframes,)


@app.cell
def _():
    save_button = mo.ui.run_button(label="💾 Save XLSX report")
    save_button
    return (save_button,)


@app.cell
def _(export_dataframes, month_selector, reports, save_button, select_report):
    mo.stop(
        not save_button.value, mo.md("*Click the button to generate the file.*")
    )

    output_file = export_dataframes(
        dataframes=reports,
        output_path=f"output/{select_report.value} CCCS - {format_datestr_to_month_year(month_selector.value)}.xlsx",
    )
    mo.md(f"✅ Saved to `{output_file}`")
    return


@app.cell
def _():
    # output_file = export_dataframes(
    #     dataframes=reports,
    #     output_path=f"output/{select_report.value + ' CCCS - ' + format_datestr_to_month_year(month_selector.value)}.xlsx",
    # )
    return


if __name__ == "__main__":
    app.run()
