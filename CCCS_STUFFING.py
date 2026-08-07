import marimo

__generated_with = "0.23.14"
app = marimo.App(width="columns")

with app.setup:
    from concurrent.futures import ThreadPoolExecutor
    from datetime import date
    from calendar import monthrange
    import polars as pl
    import marimo as mo

    from data import bc_items_lf
    from save import export_dataframes
    from datasets import cccs_stuffing, coa, transfer, shore_crane

    with ThreadPoolExecutor(max_workers=5) as _pool:
        _f_stuffing = _pool.submit(lambda: cccs_stuffing().collect())
        _f_elec = _pool.submit(lambda: coa().collect())
        _f_transfer = _pool.submit(lambda: transfer.transfer().collect())
        _f_crane = _pool.submit(lambda: shore_crane.shore_crane().collect())
        _f_price = _pool.submit(lambda: bc_items_lf.collect())

    def _fix_time(df: pl.DataFrame) -> pl.DataFrame:
        # DuckDB Arrow filter pushdown doesn't support TIME_NS; cast to strings.
        # SQL callers can still do col::TIME for comparisons.
        time_cols = [c for c, d in zip(df.columns, df.dtypes) if d == pl.Time]
        return (
            df.with_columns(pl.col(c).cast(pl.Utf8) for c in time_cols)
            if time_cols
            else df
        )

    CCCS_STUFFING_DATASET = _f_stuffing.result()
    ELECTRICITY_DATASET = _fix_time(_f_elec.result())
    TRANSFER_DATASET = _fix_time(_f_transfer.result())
    SHORE_CRANE_DATASET = _fix_time(_f_crane.result())
    price_df = _f_price.result()

    def _build_container_origin(df: pl.DataFrame) -> pl.DataFrame:
        records = df.select(
            "container_number", "date_plugged", "time_plugged", "vessel_client"
        )
        predecessors = (
            df.filter(pl.col("date_out") != pl.col("date_plugged"))
            .select("container_number", pl.col("date_out").alias("date_plugged"))
            .drop_nulls()
            .unique()
        )
        chain_starts = (
            records.join(predecessors, on=["container_number", "date_plugged"], how="anti")
            .sort("container_number", "date_plugged")
            .select(
                "container_number",
                "date_plugged",
                pl.col("vessel_client").alias("original_vessel"),
            )
        )
        return (
            records.sort("container_number", "date_plugged")
            .join_asof(
                chain_starts,
                by="container_number",
                on="date_plugged",
                strategy="backward",
            )
            .select(
                "container_number", "date_plugged", "time_plugged", "original_vessel"
            )
        )

    CONTAINER_ORIGIN = _build_container_origin(ELECTRICITY_DATASET)


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

    _today = date.today()
    _years = sorted({_today.year, _today.year + 1})
    month_options = {
        date(_y, _m, 1).strftime("%b %Y"): f"{_y}-{_m:02d}-01"
        for _y in _years
        for _m in range(1, 13)
    }

    month_selector = mo.ui.dropdown(
        options=month_options,
        label="Select invoice month",
        value=_today.replace(day=1).strftime("%b %Y"),
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
def _(
    copy_button,
    metrics_df,
    month_selector,
    save_button,
    select_report,
    summary_table,
):
    title = mo.md(f"# 🚢 CCCS - Stuffing Report — {select_report.value.title()}")

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

    actions = mo.hstack([copy_button, save_button], justify="start", gap=1)

    mo.vstack(
        [
            title,
            mo.md("---"),
            filter_bar,
            meta,
            mo.md("---"),
            summary_table,
            mo.md("---"),
            actions,
        ]
    )
    return


@app.cell(hide_code=True)
def _(
    electricity_df,
    haulage_df,
    month_selector,
    select_report,
    shore_crane_df,
    stuffing_df,
):
    _has_data = (
        len(electricity_df)
        + len(stuffing_df)
        + len(shore_crane_df)
        + len(haulage_df)
        > 0
    )
    mo.callout(
        mo.md(
            f"**No records found** for **{select_report.value}** "
            f"in **{format_datestr_to_month_year(month_selector.value)}**. "
            "Check the vessel name or select a different month."
        ),
        kind="warn",
    ) if not _has_data else None
    return


@app.function
def format_datestr_to_month_year(date_str: str) -> str:
    """Format the datestring to month 'year"""
    date_converted = date.fromisoformat(date_str)
    month = date_converted.strftime(format="%B")
    year = date_converted.strftime(format="%y")
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
        output=False
    )
    return (metrics_df,)


@app.cell(hide_code=True)
def _(cccs_stuffing_dataset, month_selector, select_report):
    with mo.status.spinner("Loading stuffing data…"):
        stuffing_df = mo.sql(
            f"""
            FROM CCCS_STUFFING_DATASET
            SELECT day_name, date, container_number, customer, service,
                   total_tonnage, overtime_tonnage, unit_price,
                   total_price::DECIMAL AS total_price
            WHERE invoiced <> 'MAERSKLINE'
              AND customer = '{select_report.value}'
              AND date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}')
            """,
            output=True,
        )
    return (stuffing_df,)


@app.cell(hide_code=True)
def _(stuffing_df):
    stuffing_summary_df = mo.sql(
        f"""
        FROM stuffing_df
        SELECT COALESCE(SUM(total_tonnage), 0.0) AS tonnage,
               COUNT(DISTINCT container_number) AS number_stuffed,
        """
    )
    return (stuffing_summary_df,)


@app.cell
def _(electricity_df):
    electricity_summary_df = mo.sql(
        f"""
        FROM electricity_df
        SELECT SUM(number_of_days_on_plug) AS days_plugged,COUNT (DISTINCT container_number) AS number_stuffed
        """
    )
    return (electricity_summary_df,)


@app.cell(hide_code=True)
def _(shore_crane_df):
    shore_crane_summary_df = mo.sql(
        f"""
        FROM shore_crane_df
        SELECT COALESCE(SUM("hours"),0) AS total_hours
        """,
        output=False
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
        output=False
    )
    return (haulage_summary_df,)


@app.cell(hide_code=True)
def _(month_selector, select_report, shore_crane_dataset):
    shore_crane_df = mo.sql(
        f"""
        FROM SHORE_CRANE_DATASET
                SELECT * EXCLUDE (invoiced_to, operation_type)
                WHERE invoiced_to <> 'MAERSKLINE'
                  AND customer LIKE '%{select_report.value}%'
                  AND operation_type = 'CCCS Container Stuffing'
                  AND date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}')
        """
    )
    return (shore_crane_df,)


@app.cell(hide_code=True)
def _(electricity_dataset, month_selector, select_report):
    with mo.status.spinner("Loading live storage data…"):
        electricity_df = mo.sql(
            f"""
            WITH main_data AS (
                SELECT *,
                    GREATEST(date_plugged, DATE '{month_selector.value}') AS date_start,
                    LEAST(
                        COALESCE(date_out, LAST_DAY(DATE '{month_selector.value}')),
                        LAST_DAY(DATE '{month_selector.value}')
                    ) AS date_stop
                FROM ELECTRICITY_DATASET
                WHERE (operation_type LIKE '%CCCS%' OR operation_type LIKE '%Exchange%')
                  AND customer <> 'MAERSKLINE'
                  AND vessel_client LIKE '%{select_report.value}%'
                  AND date_plugged <= LAST_DAY(DATE '{month_selector.value}')
                  AND (date_out >= DATE '{month_selector.value}' OR date_out IS NULL)
            ),
            added_metrics AS (
                FROM main_data SELECT
                    vessel_client, date_plugged, time_plugged, container_number, plugged_status,
                    CASE WHEN (date_out > LAST_DAY(DATE '{month_selector.value}') OR date_out IS NULL)
                         THEN NULL ELSE date_out END AS date_out,
                    CASE WHEN operation_type LIKE '%Direct%' THEN 0 WHEN plugged_status = 'Partial' AND "location" = 'For Completion'
                         THEN DATEDIFF('days', date_start, date_stop)
                         ELSE DATEDIFF('days', date_start, date_stop) + 1
                    END AS number_of_days_on_plug,
                    set_point, tonnage,
                    CASE WHEN date_plugged < '{month_selector.value}' THEN 0 ELSE plugin_price END AS plugin_price,
                    CASE WHEN (date_out > LAST_DAY(DATE '{month_selector.value}') OR date_out IS NULL)
                         THEN 0 ELSE monitoring_price END AS monitoring_price,
                    electricity_unit_price
            ),
            add_storage_price AS (
                FROM added_metrics SELECT * EXCLUDE (electricity_unit_price),
                    electricity_unit_price * number_of_days_on_plug AS storage_price
            )
            FROM add_storage_price SELECT
                vessel_client, date_plugged, time_plugged, container_number, plugged_status,
                date_out, number_of_days_on_plug, set_point, tonnage,
                plugin_price, monitoring_price, storage_price,
                storage_price + monitoring_price + plugin_price AS total_price
            """,
            output=True,
        )
    return (electricity_df,)


@app.cell(hide_code=True)
def _(
    container_origin,
    electricity_df,
    month_selector,
    select_report,
    transfer_dataset,
):
    haulage_df = mo.sql(
        f"""
        WITH valid_containers AS (
                    SELECT DISTINCT e.container_number
                    FROM electricity_df e
                    INNER JOIN CONTAINER_ORIGIN co
                        ON trim(e.container_number) = trim(co.container_number)
                        AND e.date_plugged  = co.date_plugged
                        AND e.time_plugged  = co.time_plugged
                    WHERE co.original_vessel LIKE '%{select_report.value}%'
                )
                FROM TRANSFER_DATASET t
                SEMI JOIN valid_containers v ON trim(t.container_number) = trim(v.container_number)
                SELECT
                    t.day_name,
                    t.container_number,
                    t.date,
                    t.movement_type,
                    t.destination,
                    t.status,
                    t.time_out,
                    CASE WHEN t.driver LIKE '%IPHS%' THEN 'IPHS' ELSE t.driver END AS driver,
                    t.type,
                    t.size,
                    '{select_report.value}' AS customer,
                    t.haulage_price
                WHERE t.status = 'Full'
                  AND t.movement_type = 'Delivery'
                  AND t.date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}')
        """
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
        output=False
    )
    return (summary_df,)


@app.cell(hide_code=True)
def _(summary_df):
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
            b."Unit Price"::DECIMAL AS "Unit Price"
        FROM bc b LEFT JOIN summaries s ON s.Description = b.Description AND s.Variant = b.Variant
        """,
        output=False
    )
    return (pricing_df,)


@app.cell
def _():
    copy_button = mo.ui.run_button(label="📋 Copy BC data to clipboard")
    save_button = mo.ui.run_button(label="💾 Save XLSX report")
    return copy_button, save_button


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
    _df = mo.sql(
        f"""
        FROM pricing_df
            SELECT *, "Unit Price" * QTY
        WHERE QTY > 0
        """
    )
    return


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
        output=False
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
        output=False
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


@app.cell
def _(month_selector, reports, save_button, select_report):
    mo.stop(
        not save_button.value,
        mo.md("*Click the button to generate the file.*"),
    )

    output_file = export_dataframes(
        dataframes=reports,
        output_path=f"output/{select_report.value} CCCS - {format_datestr_to_month_year(month_selector.value)}.xlsx",
    )
    mo.md(f"✅ Saved to `{output_file}`")
    return


if __name__ == "__main__":
    app.run()
