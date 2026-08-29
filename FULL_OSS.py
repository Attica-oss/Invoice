import marimo

__generated_with = "0.24.0"
app = marimo.App(width="full")

with app.setup:
    from concurrent.futures import ThreadPoolExecutor
    from datetime import date, timedelta
    from calendar import monthrange
    import polars as pl
    import marimo as mo

    from typing import Any

    from data import bc_items_lf
    from save import export_dataframes
    from datasets import (
        # genesis_raw,
        coa,
        # load_salt,
        # forklift_salt,
    oss_stuffing,
        net_list,
        shore_crane,
        transfer,
    )
    from scan_google_sheet import scan_google_sheet
    # from dataframe import forklift, hatch_to_hatch

    ACTIVITY_XLSX = (
        r"P:\Verification & Invoicing\Validation Report"
        r"\2026 IPHS operation activity.xlsx"
    )
    VALIDATION_XLSX = (
        r"P:\Verification & Invoicing\Validation Report\Validation Report.xlsx"
    )

    with ThreadPoolExecutor(max_workers=10) as _pool:
        # _f_tare = _pool.submit(lambda: genesis_raw().collect())
        _f_stuffing = _pool.submit(lambda: coa().collect())
        # _f_salt = _pool.submit(lambda: load_salt().collect())
        # _f_forklift_salt = _pool.submit(lambda: forklift_salt().collect())
        # _f_forklift = _pool.submit(lambda: forklift.collect())
        # _f_extra_men = _pool.submit(lambda: extramen.collect())
        # _f_additional_overtime = _pool.submit(lambda: additional.collect())
        # _f_well_to_well = _pool.submit(lambda: hatch_to_hatch.collect())
        _f_full_oss = _pool.submit(lambda: oss_stuffing.oss().collect())
        _f_net_list = _pool.submit(lambda: net_list().collect())
        _f_shore_crane = _pool.submit(
            lambda: shore_crane.shore_crane().collect()
        )
        _f_transfer = _pool.submit(lambda: transfer.transfer().collect())
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

    # TARE_DATASET = _fix_time(_f_tare.result())
    STUFFING_DATASET = _fix_time(_f_stuffing.result())
    # SALT_DATASET = _fix_time(_f_salt.result())
    # FORKLIFT_SALT_DATASET = _fix_time(_f_forklift_salt.result())
    # FORKLIFT_DATASET = _fix_time(_f_forklift.result())
    # EXTRA_MEN_DATASET = _f_extra_men.result()
    # ADDITIONAL_OVERTIME_DATASET = _f_additional_overtime.result()
    # WELL_TO_WELL_DATASET = _f_well_to_well.result()
    FULL_OSS_DATASET = _fix_time(_f_full_oss.result())
    NET_LIST_DATASET = _fix_time(_f_net_list.result())
    SHORE_CRANE_DATASET = _fix_time(_f_shore_crane.result())
    TRANSFER_DATASET = _fix_time(_f_transfer.result())
    price_df = _f_price.result()


@app.function
def format_datestr_to_month_year(date_str: str) -> str:
    """Format an ISO date string to "Month 'YY"."""
    date_converted = date.fromisoformat(date_str)
    return f"{date_converted:%B} '{date_converted:%y}"


@app.function
def get_first_value(
    df: pl.DataFrame,
    column: str,
    default: Any = None,
) -> Any:
    """First value of a column, or a default when the frame is empty."""
    if df.is_empty() or column not in df.columns:
        return default
    return df.get_column(column).item(0)


@app.cell
def _():
    select_report = mo.ui.dropdown(
        label="Select Line",
        options=[
            "ACILA",
            "ADAMAS",
            "AFFINIS",
            "ALAKRANA",
            "ALAKRANTXU",
            "ALBACAN",
            "ALBACORA CUATRO",
            "ALBACORA UNO",
            "ALBATUN DOS",
            "ALBATUN TRES",
            "ALPHA GOLD No. 66",
            "ALTARRI",
            "ARCHANDA",
            "ARTZA",
            "ARWA",
            "ASIAN MARINE REEFER",
            "ATERPE ALAI",
            "AURIGA",
            "AVEL VAD",
            "AVUNDA REEFER",
            "BERNICA",
            "BETI AURRERA",
            "BLUE OCEAN",
            "BOYANG CAPELLA",
            "CAP SAINT VINCENT",
            "CAP SAINTE MARIE",
            "CAPE CORAL",
            "DOLOMIEU",
            "DONIENE",
            "DRACO",
            "EGALABUR",
            "ELAI ALAI",
            "EUSKADI ALAI",
            "FRANCHE TERRE",
            "FU KUO No. 8",
            "GALERNA DOS",
            "GALERNA LAU",
            "GALERNA TRES",
            "GEVRED",
            "GLENAN",
            "GOLDEN FULL No. 168",
            "GREEN AUSTEVOLL",
            "GREEN BODO",
            "GREENSEA BERMEO",
            "GUAYATUNA UNO",
            "HAIZEA BOST",
            "HAIZEA HIRU",
            "HAIZEA LAU",
            "HAIZEA SEI",
            "HAWWA",
            "HSIANG PERNG No. 212",
            "INDO PRINCE",
            "INTERTUNA TRES",
            "ITSAS TXORI",
            "IZAR ARGIA",
            "HAI YANG 26",
            "IZARO",
            "IZURDIA",
            "JAI ALAI",
            "JANVIER LOUIS RAPHAEL",
            "JO WEN",
            "KERSAINT",
            "LAYLA",
            "MAN AN",
            "MERCURY",
            "NF DAFA No. 8",
            "NF EASTERN STAR",
            "NF INDIAN TUNA No. 9",
            "NF SEA GLORY No. 16",
            "NF TUNA PEAK",
            "NF TUNA PEAK No. 1",
            "NO. 121 DONGWON",
            "NOUR",
            "ORANGE SEA",
            "ORANGE SPIRIT",
            "ORANGE STREAM",
            "PACIFIC STAR",
            "PENDRUC",
            "PLAYA DE ANZORAS",
            "PLAYA DE ARITZATXU",
            "PLAYA DE AZKORRI",
            "PLAYA DE LAIDA",
            "PLAYA DE RIS",
            "SALGIR",
            "SIERRA LARA",
            "SIERRA LAUREL",
            "SIERRA LEYRE",
            "SIERRA LYRE",
            "SIN HUA FONG",
            "TANG SHAN 122",
            "TORRE ITALIA",
            "TREVIGNON",
            "TXORI ARGI",
            "TXORI AUNDI",
            "TXORI BAT",
            "TXORI BERRI",
            "TXORI BI",
            "TXORI GORRI",
            "TXORI TOKI",
            "TXORI ZURI",
            "VASCO",
            "WOENFULL No. 668",
            "YI FENG",
            "YI FENG 3",
            "ZAYN",
        ],
        value="PACIFIC STAR",
        searchable=True,
    )
    return (select_report,)


@app.cell
def _(report_status_df, select_report):
    _options = (
        report_status_df.filter(pl.col("vessel/client").eq(select_report.value))
        .select(pl.col("start_date").dt.date().dt.strftime(format="%Y-%m-%d"))
        .unique()
        .sort("start_date")
            .collect()
        .to_series()
        .to_list()
    )

    start_date = mo.ui.dropdown(
        options=_options,
        value=_options[0] if _options else None,
        label="Start Date",
    )
    return (start_date,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
 
    """)
    return


@app.cell
def _(
    copy_button,
    customer,
    document_date,
    metrics_df,
    period_end,
    period_start,
    posting_date,
    save_button,
    select_report,
    start_date,
    sto_number,
    summary_table,
):
    title = mo.md(f"# ::lucide:blocks:: OSS Report")

    filter_bar = mo.hstack(
        [
            select_report,
            start_date,
        ],
        justify="start",
        gap=2,
    )

    meta = mo.hstack(
        [
            mo.stat(
                label="Invoice Value",
                value=f"{metrics_df.item():,.2f}",
                caption="Total price (USD)",
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
            customer,
            meta,
            mo.md("---"),
            document_date,
            posting_date,
            period_start,
            period_end,
            sto_number,
            summary_table,
            mo.md("---"),
            actions,
        ]
    )
    return


@app.cell
def _():
    report_status_df = scan_google_sheet(
        url="https://docs.google.com/spreadsheets/d/1xNh4SiP_xw8Ck1baLhFazBsmMLIbOHjF4tg_M89efvI/edit?gid=1899664910#gid=1899664910",
        sheet_name="report_status",
    )
    return (report_status_df,)


@app.cell(hide_code=True)
def _(report_status_df, select_report, start_date):
    report_data_df = mo.sql(
        f"""
        WITH

            report AS (
                FROM
                    report_status_df
                SELECT
                    "month" AS report_month,
                    sub_type AS sto_number,
                    "vessel/client" AS vessel,
                    customer,
                    start_date,
                    end_date
                WHERE
                    report_type = 'OSS'
                    AND "vessel/client" = '{select_report.value}'
                    AND start_date = '{start_date.value}'
            )


        FROM
            report r
        """
    )
    return (report_data_df,)


@app.cell
def _(report_data_df):
    end_date = get_first_value(report_data_df, "end_date", default="")
    return (end_date,)


@app.cell
def _(report_data_df):
    document_date = mo.ui.text(
        label="Document Date:",
        value=str(
            get_first_value(report_data_df, "end_date", default="")
        ),
    )

    posting_date = mo.ui.text(
        label="Posting Date:",
        value=get_first_value(
            report_data_df,
            "end_date",
            default="",
        ),
    )

    period_start = mo.ui.text(
        label="Period Start:",
        value=str(get_first_value(report_data_df, "start_date", default=""))
    )

    period_end = mo.ui.text(
        label="Period End:",
        value=str(get_first_value(report_data_df, "end_date", default="")),
    )

    sto_number = mo.ui.text(
        label="StopOver No.",
        value=str(get_first_value(report_data_df, "sto_number", default="")),
    )

    customer = mo.ui.text(
        label="Customer Name",
        value=str(get_first_value(report_data_df, "customer", default="")),
    )
    return (
        customer,
        document_date,
        period_end,
        period_start,
        posting_date,
        sto_number,
    )


@app.cell
def _():
    copy_button = mo.ui.run_button(label="📋 Copy BC data to clipboard")
    save_button = mo.ui.run_button(label="💾 Save XLSX report")
    return copy_button, save_button


@app.cell(hide_code=True)
def _(days_on_electricity, no_of_plugin):
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
                    Plugin
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {no_of_plugin}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Plugins
                </td>
            </tr>
            <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    Electricity
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {days_on_electricity}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Days
                </td>
            </tr>
            <tr>
                <td colspan="3" style="height:18px;"></td>
            </tr>
        </table>
        """
    )
    return (summary_table,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Datasets
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Stuffing ::lucide:container::
    """)
    return


@app.cell(hide_code=True)
def _(end_date, full_oss_dataset, select_report, start_date):
    stuffing_old_df = mo.sql(
        f"""
        FROM FULL_OSS_DATASET
        WHERE vessel = '{select_report.value}' AND date BETWEEN '{start_date.value}' AND '{end_date}'
        ORDER BY date
        """
    )
    return (stuffing_old_df,)


@app.cell(hide_code=True)
def _(stuffing_old_df):
    _df = mo.sql(
        f"""
        FROM stuffing_old_df
        SELECT storage_type,overtime,SUM(total_tonnage)::DECIMAL AS tonnage,SUM(invoice_value)::DECIMAL AS invoice_value
        GROUP BY ALL
        """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Electricity ::lucide:plug-zap::
    """)
    return


@app.cell(hide_code=True)
def _(end_date, select_report, start_date, stuffing_dataset):
    stuffing_i_df = mo.sql(
        f"""
        FROM
            STUFFING_DATASET
        SELECT
            vessel_client AS vessel,
            date_plugged::DATE + time_plugged::TIME AS datetime_plugged_in,
            container_number,shipping_line,
            plugged_status,
            tonnage,
            set_point,
            date_out,
            CASE
                WHEN operation_type NOT LIKE '%OSS%' THEN 0
                ELSE days_on_plug
            END AS days_on_plug,
            CASE
                WHEN operation_type NOT LIKE '%OSS%' THEN 0
                ELSE total
            END AS total_price,
            CASE
                WHEN customer = 'IOT' THEN 'On the account of IOT'
                ELSE ''
            END AS remarks
        WHERE
            vessel_client = '{select_report.value}'
            AND date_plugged BETWEEN '{start_date.value}' AND '{end_date}'
        ORDER BY datetime_plugged_in
        """,
        output=False
    )
    return


@app.cell(hide_code=True)
def _(end_date, select_report, start_date, stuffing_dataset):
    stuffing_df = mo.sql(
        f"""
        FROM STUFFING_DATASET
            SELECT date_plugged::DATE + time_plugged::TIME AS datetime_plugged_in,
            container_number,
            plugged_status,
            set_point,
            tonnage,
            date_out,
            days_on_plug,
            plugin_price,
            electricity_unit_price AS storage_price,
            monitoring_price,
            total_electricity,
            total AS total_price,
            CASE WHEN location = 'LML' THEN '' ELSE location END  AS remarks

        WHERE
            customer = 'MAERSKLINE' AND
            vessel_client = '{select_report.value}'
            AND date_plugged BETWEEN '{start_date.value}' AND '{end_date}'
        """
    )
    return (stuffing_df,)


@app.cell
def _(stuffing_df):
    # STO summary figures shown in the UI and on the Summary sheet.
    no_of_plugin = stuffing_df.filter(pl.col("remarks").is_null()).height
    days_on_electricity = (
        stuffing_df.filter(pl.col("remarks").is_null())
        .get_column("days_on_plug")
        .sum()
        or 0
    )
    return days_on_electricity, no_of_plugin


@app.cell(hide_code=True)
def _(stuffing_df):
    plugin_summary_df = mo.sql(
        f"""
        WITH
            plugin AS (
                FROM
                    stuffing_df
                SELECT
                    'PLUGIN' AS service,
                    COUNT_IF(plugin_price = 25) AS Number,
                    SUM(plugin_price) AS price
            ),
            monitoring AS (
                FROM
                    stuffing_df
                SELECT
                    'MONITORING' AS service,
                    COUNT_IF(monitoring_price = 30) AS Number,
                    SUM(monitoring_price) AS price
            ),
            electricity_25 AS (
                FROM
                    stuffing_df
                SELECT
                    'ELECTRICITY -25°' AS service,
                    COALESCE(SUM(days_on_plug), 0) AS Number,
                    COALESCE(SUM(total_electricity), 0) AS price
                WHERE
                    set_point = -25
            ),
            electricity_35 AS (
                FROM
                    stuffing_df
                SELECT
                    'ELECTRICITY -35°' AS service,
                    COALESCE(SUM(days_on_plug), 0) AS Number,
                    COALESCE(SUM(total_electricity), 0) AS price
                WHERE
                    set_point = -35
            ),
            electricity_60 AS (
                FROM
                    stuffing_df
                SELECT
                    'ELECTRICITY -60°' AS service,
                    COALESCE(SUM(days_on_plug), 0) AS Number,
                    COALESCE(SUM(total_electricity), 0) AS price
                WHERE
                    set_point = -60
            )
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
        """
    )
    return (plugin_summary_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Net List
    """)
    return


@app.cell(hide_code=True)
def _(end_date, full_oss_dataset, select_report, start_date):
    net_list_df = mo.sql(
        f"""
        FROM FULL_OSS_DATASET
        WHERE
            vessel = '{select_report.value}'
            AND date BETWEEN '{start_date.value}' AND '{end_date}'
        """
    )
    return (net_list_df,)


@app.cell(hide_code=True)
def _(net_list_df):
    summary_net_list_df = mo.sql(
        f"""
        FROM net_list_df
        SELECT service,storage_type,overtime,SUM(total_tonnage)::DECIMAL AS total_tonnage,SUM(invoice_value)::DECIMAL AS total_price
        GROUP BY ALL
        ORDER BY service,overtime
        """
    )
    return (summary_net_list_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Shore Crane ::lucide:git-pull-request-create::
    """)
    return


@app.cell(hide_code=True)
def _(end_date, select_report, shore_crane_dataset, start_date):
    shore_crane_df = mo.sql(
        f"""
        FROM SHORE_CRANE_DATASET
        WHERE
            customer = '{select_report.value}' AND invoiced_to = 'MAERSKLINE'
            AND date BETWEEN '{start_date.value}' AND '{end_date}'
        """
    )
    return (shore_crane_df,)


@app.cell(hide_code=True)
def _(shore_crane_df):
    summary_shore_crane_df = mo.sql(
        f"""
        WITH
            ot_2 AS (
                FROM
                    shore_crane_df
                SELECT
                    'Overtime 200%' AS overtime,
                    SUM(overtime_hours) AS value
                WHERE
                    day_name IN ('Sun', 'PH')
            ),
            ot_1_sun AS (
                FROM
                    shore_crane_df
                SELECT
                    'Overtime 150%' AS overtime,
                    SUM(hours - overtime_hours) AS value
                WHERE
                    day_name IN ('Sun', 'PH')
            ),
            ot_1 AS (
                FROM
                    shore_crane_df
                SELECT
                    'Overtime 150%' AS overtime,
                    SUM(overtime_hours) AS value
                WHERE
                    day_name NOT IN ('Sun', 'PH')
            ),
            normal AS (
                FROM
                    shore_crane_df
                SELECT
                    'Normal Hours' AS overtime,
                    SUM(hours - overtime_hours) AS value
                WHERE
                    day_name NOT IN ('Sun', 'PH')
            ),
            -- Append the data
            grouped AS (
                FROM
                    normal
                UNION ALL
                FROM
                    ot_1
                UNION ALL
                FROM
                    ot_1_sun
                UNION ALL
                FROM
                    ot_2
            ),
            main AS (
                FROM
                    grouped
                SELECT
                    overtime,
                    SUM(value) AS value
                GROUP BY ALL
            ),
            sort_order AS (
                FROM
                    main
                SELECT
                    overtime,
                    CASE
                        WHEN overtime = 'Normal Hours' THEN 1
                        WHEN overtime = 'Overtime 150%' THEN 2
                        WHEN overtime = 'Overtime 200%' THEN 3
                        ELSE 0
                    END AS sort
            ),
            unit_price AS (
                FROM
                    main
                SELECT
                    overtime,
                    CASE
                        WHEN overtime = 'Normal Hours' THEN 135 * 1.0
                        WHEN overtime = 'Overtime 150%' THEN 135 * 1.6
                        WHEN overtime = 'Overtime 200%' THEN 135 * 2.1
                        ELSE 0
                    END AS unit_price
            )
        SELECT
            m.*,
            p.unit_price * m.value AS total_price
        FROM
            main m
            LEFT JOIN sort_order s ON s.overtime = m.overtime
            LEFT JOIN unit_price p ON p.overtime = m.overtime
        ORDER BY
            s.sort
        """
    )
    return (summary_shore_crane_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Transfer
    """)
    return


@app.cell
def _():
    affectation_df = pl.read_excel(
        source=VALIDATION_XLSX,
        table_name="Affectation",
    ).pipe(clean_affectation)
    return (affectation_df,)


@app.function
def clean_affectation(df: pl.DataFrame) -> pl.DataFrame:
    """Clean the affectation dataset"""
    return df.select(
        pl.col("Date affected").alias("date"),
        pl.col("Container Ref. No.").alias("container_number"),
        pl.col("Assigned to").str.to_uppercase().alias("vessel"),
        pl.col("Date Gate Out").alias("exit_date"),
    )


@app.cell(hide_code=True)
def _(affectation_df, end_date, select_report, start_date):
    to_filter_transfer_df = mo.sql(
        f"""
        FROM affectation_df
        WHERE vessel = '{select_report.value}' AND date BETWEEN '{start_date.value}' AND '{end_date}'
        """,
        output=False
    )
    return (to_filter_transfer_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Container still on plug
    """)
    return


@app.cell
def _(to_filter_transfer_df):
    container_on_plug = to_filter_transfer_df.filter(
        pl.col("exit_date").is_null()
    )
    container_on_plug
    return


@app.cell(hide_code=True)
def _(to_filter_transfer_df, transfer_dataset):
    transfer_df = mo.sql(
        f"""
        WITH
            t AS (
                FROM
                    TRANSFER_DATASET
                WHERE
                    status = 'Full'
            )
        SELECT
            t.day_name,
            t.container_number,
            t.date,
            t.movement_type,
            t.destination,
            t.status,
            t.time_out::TIME AS time_out,
            CASE
                WHEN driver LIKE '%IPHS%' THEN 'IPHS'
                ELSE driver
            END AS driver,
            t."type",
            t.size,
            f.vessel,
            t.haulage_price AS price_per_move,
            t.remarks
        FROM
            t
            LEFT JOIN to_filter_transfer_df f ON f.exit_date = t.date
            AND f.container_number = t.container_number
        WHERE
            f.exit_date IS NOT NULL
           AND t.remarks = 'MAERSKLINE'
        """
    )
    return (transfer_df,)


@app.cell(hide_code=True)
def _(transfer_df):
    summary_transfer_df = mo.sql(
        f"""
        WITH
            main AS (
                FROM
                    transfer_df
                SELECT
                    COUNT(*) AS total_service,
                    CASE
                        WHEN day_name IN ('Sun', 'PH')
                        AND time_out::TIME > '16:00'::TIME THEN 'Overtime 200%'
                        WHEN (
                            day_name IN ('Sun', 'PH')
                            OR time_out::TIME > '17:00'::TIME
                        )
                        OR (
                            time_out::TIME BETWEEN '00:00'::TIME AND '08:00'::TIME
                        ) THEN 'Overtime 150%'
                        ELSE 'Normal Hours'
                    END AS overtime
                GROUP BY ALL
            ),
            sort_order AS (
                SELECT
                    *
                FROM
                    (
                        VALUES
                            ('Normal Hours', 1),
                            ('Overtime 150%', 2),
                            ('Overtime 200%', 3)
                    ) AS t (overtime, sort)
            ),
            unit_price AS (
                SELECT
                    *
                FROM
                    (
                        VALUES
                            ('Normal Hours', 90),
                            ('Overtime 150%', 90 * 1.5),
                            ('Overtime 200%', 90 * 2.0)
                    ) AS t (overtime, unit_price)
            )
        SELECT
            m.overtime,
            COALESCE(o.total_service, 0) AS total_service,
            p.unit_price * COALESCE(o.total_service, 0) AS total_price
        FROM
            sort_order m
            FULL OUTER JOIN main o ON o.overtime = m.overtime
            LEFT JOIN unit_price p ON p.overtime = m.overtime
        ORDER BY
            m.sort
        """
    )
    return (summary_transfer_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Summaries Df (Business Central line items)
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    -- Stuffing
    """)
    return


@app.cell(hide_code=True)
def _(plugin_summary_df):
    stuffing_bc = mo.sql(
        f"""
        WITH
            plugin AS (
                FROM
                    plugin_summary_df
                SELECT
                   Number AS QTY,
                    service AS Description,
                    'STD' AS Variant
                WHERE
                    service = 'PLUGIN'
            ),
            monitoring AS (
               FROM
                    plugin_summary_df
                SELECT
                   Number AS QTY,
                    service AS Description,
                    'STD' AS Variant
                WHERE
                    service = 'MONITORING'
            ),
            standard AS (
                       FROM
                    plugin_summary_df
                SELECT
                   Number AS QTY,
                    'ELECTRICITY - 25°' AS Description,
                    'STD' AS Variant
                WHERE
                    service = 'ELECTRICITY -25°'
            ),
            magnum AS (
                FROM
                    plugin_summary_df
                SELECT
                   Number AS QTY,
                    'ELECTRICITY - 35°' AS Description,
                    'STD' AS Variant
                WHERE
                    service = 'ELECTRICITY -35°'
            ),
            s_freezer AS (
               FROM
                    plugin_summary_df
                SELECT
                   Number AS QTY,
                    'ELECTRICITY - 60°' AS Description,
                    'STD' AS Variant
                WHERE
                    service = 'ELECTRICITY -60°'
            )
        FROM
            plugin
        UNION ALL
        FROM
            monitoring
        UNION ALL
        FROM
            standard
        UNION ALL
        FROM magnum
        UNION ALL
        FROM s_freezer
        """
    )
    return (stuffing_bc,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    --- Net List
    """)
    return


@app.cell(hide_code=True)
def _(summary_net_list_df):
    net_list_bc = mo.sql(
        f"""
        WITH
            service_map AS (
                SELECT
                    *
                FROM
                    (
                        VALUES
                            ('Stuffing', 'Brine', 'BASIC OSS - BRINE'),
                            ('Stuffing', 'Dry', 'BASIC OSS - DRY'),
                            ('Container Stuffing - Brine', 'Brine', 'FULL OSS - BRINE'),
                            ('Container Stuffing - Dry', 'Dry', 'FULL OSS - DRY')
                    ) AS t (service, storage_type, Description)
            ),
            variant_map AS (
                SELECT
                    *
                FROM
                    (
                        VALUES
                            ('normal hours', 'STD'),
                            ('overtime 150%', '150%'),
                            ('overtime 200%', '200%')
                    ) AS t (overtime, Variant)
            )
        SELECT
            s.total_tonnage AS QTY,
            m.Description,
            v.Variant
        FROM
            summary_net_list_df s
            JOIN service_map m ON m.service = s.service
            AND m.storage_type = s.storage_type
            JOIN variant_map v ON v.overtime = s.overtime
        """
    )
    return (net_list_bc,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    --- Shore Crane
    """)
    return


@app.cell(hide_code=True)
def _(summary_shore_crane_df):
    shore_crane_bc = mo.sql(
        f"""
        WITH variant_map AS (
                    SELECT * FROM (
                        VALUES
                            ('Normal Hours', 'STD'),
                            ('Overtime 150%', 'OT1'),
                            ('Overtime 200%', 'OT2')
                    ) AS t (overtime, Variant)
                )
                SELECT
                    s.value AS QTY,
                    'SHORE CRANE RENTAL' AS Description,
                    v.Variant
                FROM summary_shore_crane_df s
                JOIN variant_map v ON v.overtime = s.overtime
        """,
        output=False
    )
    return (shore_crane_bc,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    --- Transfer
    """)
    return


@app.cell(hide_code=True)
def _(summary_transfer_df):
    transfer_bc = mo.sql(
        f"""
        WITH variant_map AS (
                    SELECT * FROM (
                        VALUES
                            ('Normal Hours', 'STD'),
                            ('Overtime 150%', '150%'),
                            ('Overtime 200%', '200%')
                    ) AS t (overtime, Variant)
                )
                SELECT
                    s.total_service AS QTY,
                    'TRANSFER FEU' AS Description,
                    v.Variant
                FROM summary_transfer_df s
                JOIN variant_map v ON v.overtime = s.overtime
        """,
        output=False
    )
    return (transfer_bc,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Combined Summaries
    """)
    return


@app.cell
def _(net_list_bc, shore_crane_bc, stuffing_bc, transfer_bc):
    combined_services_bc = pl.concat(
        [
            _df.select(
                pl.col("QTY").cast(pl.Decimal(scale=3)),
                pl.col("Description").cast(pl.Utf8),
                pl.col("Variant").cast(pl.Utf8),
            )
            for _df in [


                stuffing_bc,


                net_list_bc,
                shore_crane_bc,
                transfer_bc,
            ]
        ]
    )
    combined_services_bc
    return (combined_services_bc,)


@app.cell(hide_code=True)
def _(combined_services_bc):
    pricing_df = mo.sql(
        f"""
        WITH bc AS (FROM
            price_df
            SELECT 
            "Type",
            "No.",
            "Description",
            "Variant",
           "Unit Price"

        WHERE
            Description IN (

            'TRANSFER FEU',
            'SHORE CRANE RENTAL',
            'BASIC OSS - BRINE',
            'BASIC OSS - DRY',
            'FULL OSS - BRINE',
            'FULL OSS - DRY',
            'PLUGIN',
            'MONITORING',
            'ELECTRICITY - 25°',
        	'ELECTRICITY - 35°',
        	'ELECTRICITY - 60°',




            ) ),summaries AS (FROM combined_services_bc WHERE QTY <>0)


        SELECT 
            b.Type,
            b."No.",
            b.Description,
            b.Variant,
            s.QTY::DECIMAL AS QTY,
            CASE WHEN b.Description = 'BERTH DUES (FISHING VESSELS)' THEN 1000 ELSE b."Unit Price"::DECIMAL END AS "Unit Price"
        FROM bc b LEFT JOIN summaries s ON s.Description = b.Description AND s.Variant = b.Variant
        WHERE s.QTY IS NOT NULL AND s.QTY >0
        """
    )
    return (pricing_df,)


@app.cell(hide_code=True)
def _(pricing_df):
    metrics_df = mo.sql(
        f"""
        WITH a AS (FROM pricing_df
                SELECT Description, Variant, "Unit Price", QTY,
                    ROUND("Unit Price" * QTY, 2) AS total_price
                WHERE QTY > 0)

                FROM a
                SELECT COALESCE(ROUND(SUM(total_price)::FLOAT8, 2), 0) AS total_price
        """
    )
    return (metrics_df,)


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
        FROM pricing_df
                SELECT
                    'STO Services' AS section,
                    Description ||
                        CASE WHEN Variant <> 'STD' THEN ' (' || Variant || ')' ELSE '' END
                        AS service,
                    ROW_NUMBER() OVER (ORDER BY Description, Variant) AS sort_order,
                    "Unit Price" AS unit_price,
                    QTY AS quantity,
                    ROUND(QTY * "Unit Price", 2) AS total
                WHERE QTY > 0
                ORDER BY sort_order
        """
    )
    return (service_breakdown_df,)


@app.cell
def _(
    additional_overtime_df,
    days_on_electricity,
    end_date,
    extra_men_df,
    final_berth_df,
    final_forklift_df,
    net_list_df,
    no_of_plugin,
    salt_df,
    select_report,
    service_breakdown_df,
    shore_crane_df,
    start_date,
    stuffing_df,
    tare_df,
    transfer_df,
    well_to_well_df,
):
    _detail_sheets = {
        "Berth Dues": final_berth_df,
        "Tare Weight": tare_df,
        "Stuffing": stuffing_df,
        "Salt": salt_df,
        "Forklift": final_forklift_df,
        "Extra Men": extra_men_df,
        "Additional Overtime": additional_overtime_df,
        "Well to Well": well_to_well_df,
        "Net List": net_list_df,
        "Shore Crane": shore_crane_df,
        "Transfer": transfer_df,
    }

    reports = {
        "Summary": {
            "type": "summary",
            "month": f"{start_date.value} to {end_date}",
            "client": f"{select_report.value}",
            "summary_rows": [
                None,  # blank row
                {
                    "label": "Plugin:",
                    "value": no_of_plugin,
                    "unit": "Plugins",
                    "italic": True,
                },
                {
                    "label": "Electricity:",
                    "value": days_on_electricity,
                    "unit": "Days",
                    "italic": True,
                },
                None,
            ],
            # Expected columns:
            # section, service, quantity, unit_price, total
            "service_df": service_breakdown_df,
            "landscape": False,
            "pages_wide": 1,
            "pages_tall": 1,
        },
        # Only export detail sheets that actually have rows.
        **{
            _name: {"df": _df, "header_color": "#1f6fb2"}
            for _name, _df in _detail_sheets.items()
            if len(_df) > 0
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
        output_path=f"output/{select_report.value} STO - {format_datestr_to_month_year(month_selector.value)}.xlsx",
    )
    mo.md(f"✅ Saved to `{output_file}`")
    return


if __name__ == "__main__":
    app.run()
