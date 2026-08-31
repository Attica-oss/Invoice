import marimo

__generated_with = "0.23.14"
app = marimo.App(width="columns")

with app.setup:
    from concurrent.futures import ThreadPoolExecutor
    from datetime import date, timedelta
    from typing import Any

    import marimo as mo
    import polars as pl
    from scan_google_sheet import scan_google_sheet

    from data import bc_items_lf
    from dataframe import forklift, hatch_to_hatch
    from datasets import (
        coa,
        forklift_salt,
        genesis_raw,
        load_salt,
        net_list,
        shore_crane,
        transfer,
    )
    from save import export_dataframes

    ACTIVITY_XLSX = r"C:\Users\gmounac\Dropbox\! OPERATION SUPPORTING DOCUMENTATION\2026\2026 IPHS operation activity.xlsx"
    VALIDATION_XLSX = (
        r"P:\Verification & Invoicing\Validation Report\Validation Report.xlsx"
    )

    with ThreadPoolExecutor(max_workers=10) as _pool:
        _f_tare = _pool.submit(lambda: genesis_raw().collect())
        _f_stuffing = _pool.submit(lambda: coa().collect())
        _f_salt = _pool.submit(lambda: load_salt().collect())
        _f_forklift_salt = _pool.submit(lambda: forklift_salt().collect())
        _f_forklift = _pool.submit(lambda: forklift.collect())
        # _f_extra_men = _pool.submit(lambda: extramen.collect())
        # _f_additional_overtime = _pool.submit(lambda: additional.collect())
        _f_well_to_well = _pool.submit(lambda: hatch_to_hatch.collect())
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

    TARE_DATASET = _fix_time(_f_tare.result())
    STUFFING_DATASET = _fix_time(_f_stuffing.result())
    SALT_DATASET = _fix_time(_f_salt.result())
    FORKLIFT_SALT_DATASET = _fix_time(_f_forklift_salt.result())
    FORKLIFT_DATASET = _fix_time(_f_forklift.result())
    # EXTRA_MEN_DATASET = _f_extra_men.result()
    # ADDITIONAL_OVERTIME_DATASET = _f_additional_overtime.result()
    WELL_TO_WELL_DATASET = _f_well_to_well.result()
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
    well_inv_df = scan_google_sheet(
        url="https://docs.google.com/spreadsheets/d/1PvTkl6DYZdhtaiNshz0qwtSPxC8S1OOeu905NmhFKNs/edit?gid=1301483182#gid=1301483182",
        sheet_name="WelltoWell",
    )
    return (well_inv_df,)


@app.cell(hide_code=True)
def _():
    berth_ldf = pl.read_excel(
        source=ACTIVITY_XLSX,
        table_name="berth_dues",
        schema_overrides={"TIME IN": pl.Time, "TIME OUT": pl.Time},
    )

    additional_ldf = pl.read_excel(
        source=ACTIVITY_XLSX,
        sheet_name="Additional Stevedores",
        schema_overrides={"End Time": pl.Time},
    )

    extra_men_ldf = pl.read_excel(
        source=ACTIVITY_XLSX,
        table_name="handling_activity",
    )
    return additional_ldf, berth_ldf, extra_men_ldf


@app.cell(hide_code=True)
def _(extra_men_ldf):
    EXTRA_MEN_DATASET_NEW = mo.sql(
        """
        WITH main AS (FROM
            extra_men_ldf
        SELECT
            "DAY" AS day_name,
            "DATE" AS date,
            "VESSEL NAME" AS vessel,
            "TOTAL TONNAGE"::DECIMAL AS total_tonnage,
            "Extra Men" AS extra_men,
        CASE WHEN "DAY" IN ('Sun','PH') THEN 1.2 ELSE 0.8 END AS price,
            "Comments" AS remarks
        WHERE "Extra Men" >0)


        FROM main 
        SELECT * EXCLUDE remarks,
        price * total_tonnage * extra_men AS total_price,
        remarks
        """,
        output=False
    )


@app.cell(hide_code=True)
def _(additional_ldf):
    ADDITIONAL_OVERTIME_DATASET_NEW = mo.sql(
        """
        FROM additional_ldf
        SELECT 
        Day AS day_name,
        Date::DATE + "End Time"::TIME AS date,
        Vessel AS vessel,
        "Num of Stevedores" AS number_of_stevedores,
        "Hours" AS number_of_hours,
        "Tonnage" AS overtime_tonnage,
        8 AS unit_price,
        ("Num of Stevedores"::INT * "Hours"::INT) * 8 AS total_price,
        Remarks
        WHERE "Num of Stevedores" <> 'To check'
        """,
        output=False
    )


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
            "BALBAYA",
            "BELLE ISLE",
            "BELLE RIVE",
            "BELOUVE",
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
            "IGS SANKALP",
            "INDO PRINCE",
            "INS IKSHAK",
            "INS KESARI",
            "INS TARKASH",
            "INS TIR",
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
            "TALENDUIC",
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


@app.cell(hide_code=True)
def _(berth_ldf):
    berth_df = mo.sql(
        """
        FROM berth_ldf
        SELECT "VESSEL NAME" AS vessel,
        "DATE IN"::DATE + "TIME IN"::TIME AS date_in,
        "DATE OUT"::DATE + "TIME OUT"::TIME AS date_out,
        "DURATION IN PORT" AS duration_in_port,
        "VALUE ($)" AS "value",
        COALESCE("DISCOUNT",0) AS discount,
        "INVOICE VALUE" AS invoice_value,
        "INVOICING ENTITY" AS invoicing_entity,
        "VESSEL TYPE" AS vessel_type,
        "COMMENTS" AS remarks
        """,
        output=False
    )
    return (berth_df,)


@app.cell
def _(berth_df, select_report):
    _options = (
        berth_df.filter(pl.col("vessel").eq(select_report.value))
        .select(pl.col("date_in").dt.date().dt.strftime(format="%Y-%m-%d"))
        .unique()
        .sort("date_in")
        .to_series()
        .to_list()
    )

    start_date = mo.ui.dropdown(
        options=_options,
        value=_options[0] if _options else None,
        label="Start Date",
    )
    return (start_date,)


@app.cell
def _(berth_df, select_report, start_date):
    mo.stop(
        start_date.value is None,
        mo.callout(
            mo.md(
                f"**No berth record found for {select_report.value}.** "
                "Select a different vessel."
            ),
            kind="warn",
        ),
    )

    end_date = (
        berth_df.filter(
            pl.col("vessel")
            .eq(select_report.value)
            .and_(
                pl.col("date_in")
                .dt.date()
                .eq(date.fromisoformat(start_date.value))
            )
        )
        .select(pl.col("date_out").dt.date().max())
        .item()
    )

    mo.stop(
        end_date is None,
        mo.callout(
            mo.md(
                f"**Missing DATE OUT** for {select_report.value} on "
                f"{start_date.value}. Complete the berth-dues sheet first."
            ),
            kind="warn",
        ),
    )
    return (end_date,)


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
    title = mo.md("# ⛴️ STO Report")

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


@app.cell
def _(bc_df, copy_button):
    mo.stop(not copy_button.value)
    bc_df.write_clipboard()  # tab-separated, pastes cleanly into BC / Excel
    mo.md("✅ Copied to clipboard")


@app.cell
def _():
    report_status_df = scan_google_sheet(
        url="https://docs.google.com/spreadsheets/d/1xNh4SiP_xw8Ck1baLhFazBsmMLIbOHjF4tg_M89efvI/edit?gid=1899664910#gid=1899664910",
        sheet_name="report_status",
    )
    return (report_status_df,)


@app.cell(hide_code=True)
def _(berth_ldf, report_status_df, select_report, start_date):
    report_data_df = mo.sql(
        f"""
        WITH
            main AS (
                FROM
                    berth_ldf
                SELECT
                    "VESSEL NAME" AS vessel,
                    "DATE IN"::DATE + "TIME IN"::TIME AS start_date,
                    "DATE OUT"::DATE + "TIME OUT"::TIME AS end_date,
                WHERE
                    "VESSEL NAME" = '{select_report.value}'
                    AND "DATE IN" = '{start_date.value}'
            ),
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
        SELECT 
            r.report_month,


            r.customer,
            r.end_date AS document_date,
            r.end_date AS posting_date,
            m.start_date AS period_start,
            m.end_date AS period_end,
            r.sto_number,
            r.vessel

        FROM
            report r
            LEFT JOIN main m ON m.vessel = r.vessel
            AND m.start_date::DATE = r.start_date
            AND m.end_date::DATE = r.end_date
        """,
        output=False
    )
    return (report_data_df,)


@app.cell
def _(report_data_df):
    document_date = mo.ui.text(
        label="Document Date:",
        value=str(
            get_first_value(report_data_df, "document_date", default="")
        ),
    )

    posting_date = mo.ui.text(
        label="Posting Date:",
        value=get_first_value(
            report_data_df,
            "posting_date",
            default="",
        ),
    )

    period_start = mo.ui.text(
        label="Period Start:",
        value=str(get_first_value(report_data_df, "period_start", default="")),
    )

    period_end = mo.ui.text(
        label="Period End:",
        value=str(get_first_value(report_data_df, "period_end", default="")),
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
def _(
    forklift_hours,
    no_of_plugin,
    number_of_containers,
    salt_operations,
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
                    Tonnage
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {total_tonnage}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Tons
                </td>
            </tr>
             <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    Total Container Stuffed
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {number_of_containers}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Containers
                </td>
            </tr>
            <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    Forklift Services:
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {forklift_hours}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Hours
                </td>
                </tr>
            <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    Salt Operations:
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {salt_operations}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Hours
                </td>
            </tr>
            <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    Forklift Services:
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {forklift_hours}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Hours
                </td>
            </tr>
            <tr>
                <td colspan="3" style="height:18px;"></td>
            </tr>
        </table>
        """
    )
    return (summary_table,)


@app.cell
def _(final_berth_df, select_report, start_date):
    mo.callout(
        mo.md(
            f"**No berth record found** for **{select_report.value}** "
            f"starting **{start_date.value}**. "
            "Check the vessel name or select a different start date."
        ),
        kind="warn",
    ) if final_berth_df.is_empty() else None


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Datasets
    """)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Berth Dues :⚓
    """)


@app.cell(hide_code=True)
def _(berth_df, select_report, start_date):
    final_berth_df = mo.sql(
        f"""
        FROM
            berth_df 
        WHERE
            vessel = '{select_report.value}'
            AND date_in::DATE = '{start_date.value}'
        """
    )
    return (final_berth_df,)


@app.cell
def _(final_berth_df):
    discount_berthing_figure: int = final_berth_df.select(
        pl.col("discount").sum()
    ).item()
    value_berthing_figure: int = final_berth_df.select(
        pl.col("value").sum()
    ).item()
    return discount_berthing_figure, value_berthing_figure


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Tare Weight ::lucide:weight::
    """)


@app.cell(hide_code=True)
def _(end_date, select_report, start_date, tare_dataset):
    tare_df = mo.sql(
        f"""
        WITH
            main AS (
                FROM
                    TARE_DATASET
                SELECT
                    date AS date,
                    UPPER(vessel) AS vessel,
                    1 AS rental,
                    COUNT(DISTINCT "Side Working") AS number_of_side_working,
                    STRING_AGG(DISTINCT "Side Working", ',') AS side_working,
                    (50) AS rental_price,
                    (50) AS calibration_price,
                GROUP BY ALL
                HAVING
                    vessel = '{select_report.value}'
                    AND date BETWEEN '{start_date.value}' AND '{end_date}'
                ORDER BY
                    date
            )
        FROM
            main
        SELECT
            date,
            rental AS rental_of_weight,
            side_working,
            rental_price,
            (calibration_price * number_of_side_working) AS calibration_price,
        rental_price +  (calibration_price * number_of_side_working) AS total_price
        """
    )
    return (tare_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Calibration Summary
    """)


@app.cell(hide_code=True)
def _(end_date, select_report, start_date, tare_dataset, unpivoted):
    tare_summary_df = mo.sql(
        f"""
        WITH
            main AS (
                FROM
                    TARE_DATASET
                SELECT
                    date AS date,
                    UPPER(vessel) AS vessel,
                    1 AS rental,
                    COUNT(DISTINCT "Side Working") AS number_of_side_working,
                    STRING_AGG(DISTINCT "Side Working", ',') AS side_working,
                    (50) AS rental_price,
                    (50) AS calibration_price,
                GROUP BY ALL
                HAVING
                    vessel = '{select_report.value}'
                    AND date BETWEEN '{start_date.value}' AND '{end_date}'
                ORDER BY
                    date
            ),


        summary AS (
            SELECT
                SUM(rental) AS rental,
                SUM(number_of_side_working) AS calibration
            FROM main
        ),

        unpivoted AS (
            UNPIVOT summary
            ON rental, calibration
            INTO
                NAME service
                VALUE quantity
        )

        SELECT
            service,
            quantity,
            -- 50 AS unit_price,
            quantity * 50 AS total_price
        FROM unpivoted;
        """
    )
    return (tare_summary_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Stuffing ::lucide:container::
    ----
    """)


@app.cell(hide_code=True)
def _(end_date, select_report, start_date, stuffing_dataset):
    stuffing_df = mo.sql(
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
                WHEN shipping_line = 'IOT' THEN 0
                ELSE days_on_plug
            END AS days_on_plug,
            CASE
                WHEN shipping_line = 'IOT' THEN 0
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
        """
    )
    return (stuffing_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    * Stuffing Metrics for summary
    """)


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
    number_of_containers = stuffing_df.select(
        pl.col("container_number").unique_counts().sum()
    ).item()
    return days_on_electricity, no_of_plugin, number_of_containers


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Salt
    """)


@app.cell(hide_code=True)
def _(end_date, salt_dataset, select_report, start_date):
    salt_df = mo.sql(
        f"""
        FROM SALT_DATASET
        WHERE customer NOT IN ('HARTSWATER LIMITED','ECHEBASTAR') AND vessel = '{select_report.value}'  AND date BETWEEN '{start_date.value}' AND '{end_date}'
        """
    )
    return (salt_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Summary of Salt Ops
    """)


@app.cell(hide_code=True)
def _(salt_df, unpivoted):
    salt_summary_df = mo.sql(
        """
        WITH
            summaries AS (
                FROM
                    salt_df
                SELECT
                    SUM(normal_tonnage)::DECIMAL AS "Normal Hours",
                    SUM(overtime_150_tonnage)::DECIMAL AS "Overtime 150%",
                    SUM(overtime_200_tonnage)::DECIMAL AS "Overtime 200%"
            ),
            unpivoted AS (
                UNPIVOT summaries ON "Normal Hours",
                "Overtime 150%",
                "Overtime 200%" INTO NAME service VALUE quantity
            )
        FROM
            unpivoted
        SELECT
            *,
            CASE
                WHEN service = 'Normal Hours' THEN 10 * 1.0 * quantity
                WHEN service = 'Overtime 150%' THEN 10 * 1.5 * quantity
                WHEN service = 'Overtime 200%' THEN 10 * 2.0 * quantity
            END AS total_price
        """
    )
    return (salt_summary_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    * Salt metrics for summary
    """)


@app.cell
def _(salt_summary_df):
    salt_operations = salt_summary_df.select(pl.col("quantity").sum()).item()
    return (salt_operations,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Forklift ::lucide:forklift::
    """)


@app.cell
def _(end_date, select_report, start_date):
    pl.read_excel(
        source=r"C:\Users\gmounac\Dropbox\Container and Transport\Transport Section\Forklift Usage\Forklift Record.xlsx",
        schema_overrides={
            "Time Out": pl.Time,
            "Time In": pl.Time,
            "Duration": pl.Time,
        },
    ).filter(
        pl.col("Invoiced in:").is_in(["ECHEBASTAR", "HARTSWATER"]).not_()
    ).filter(
        pl.col("Vessel/Client")
        .eq(select_report.value)
        .and_(
            pl.col("Date of Service").is_between(
                date.fromisoformat(start_date.value),
                end_date + timedelta(days=5),
            )
        )
    ).sort(["Date of Service", "Time Out"])


@app.cell(hide_code=True)
def _(end_date, forklift_salt_dataset, select_report, start_date):
    f_salt_df = mo.sql(
        f"""
        FROM FORKLIFT_SALT_DATASET
        WHERE customer NOT IN ('HARTSWATER LIMITED','ECHEBASTAR') AND vessel = '{select_report.value}'  AND date BETWEEN '{start_date.value}' AND '{end_date}'
        """
    )
    return (f_salt_df,)


@app.cell(hide_code=True)
def _(end_date, forklift_dataset, select_report, start_date):
    forklift_df = mo.sql(
        f"""
        FROM FORKLIFT_DATASET
        WHERE invoiced_in NOT IN ('HARTSWATER LIMITED','ECHEBASTAR','SAPMER') AND customer = '{select_report.value}'  AND date BETWEEN '{start_date.value}' AND '{end_date}'::DATE+ INTERVAL 5 DAY
        """
    )
    return (forklift_df,)


@app.cell(hide_code=True)
def _(forklift_df, unpivoted):
    forklift_summary_df = mo.sql(
        """
        WITH
            summaries AS (FROM forklift_df
        SELECT 
        CEIL(SUM(normal_hours) / 60 ) AS "Normal Hours",
        CEIL(SUM(overtime_150) / 60 ) AS "Overtime 150%",
        CEIL(SUM(overtime_200) / 60 ) AS "Overtime 200%"),

         unpivoted AS (
                UNPIVOT summaries ON "Normal Hours",
                "Overtime 150%",
                "Overtime 200%" INTO NAME service VALUE quantity
            )



        FROM
            unpivoted
        SELECT
            *,
            CASE
                WHEN service = 'Normal Hours' THEN 30 * 1.0 * quantity
                WHEN service = 'Overtime 150%' THEN 30* 1.5 * quantity
                WHEN service = 'Overtime 200%' THEN 30* 2.0 * quantity
            END AS total_price
        """
    )
    return (forklift_summary_df,)


@app.cell(hide_code=True)
def _(f_salt_df, unpivoted):
    forklift_salt_summary_df = mo.sql(
        """
        WITH
            summaries AS (
                SELECT
                    CEIL(SUM(EPOCH(normal_hour_services::TIME)) / 3600) AS "Normal Hours",
                    CEIL(
                        SUM(EPOCH(overtime_for_normal_services::TIME)) / 3600
                    ) AS "Overtime 150%",
                    CEIL(
                        SUM(EPOCH(overtime_for_extended_services::TIME)) / 3600
                    ) AS "Overtime 200%"
                FROM
                    f_salt_df
            ),
            unpivoted AS (
                UNPIVOT summaries ON "Normal Hours",
                "Overtime 150%",
                "Overtime 200%" INTO NAME service VALUE quantity
            )
        FROM
            unpivoted
        SELECT
            *,
            CASE
                WHEN service = 'Normal Hours' THEN 30 * 1.0 * quantity
                WHEN service = 'Overtime 150%' THEN 30 * 1.5 * quantity
                WHEN service = 'Overtime 200%' THEN 30 * 2.0 * quantity
            END AS total_price
        """
    )
    return (forklift_salt_summary_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    * Forklift metrics for summary
    """)


@app.cell
def _(forklift_salt_summary_df, forklift_summary_df):
    forklift_hours = int(
        pl.concat([forklift_summary_df, forklift_salt_summary_df])
        .select(pl.col("quantity").sum())
        .item()
    )
    return (forklift_hours,)


@app.cell(hide_code=True)
def _(f_salt_df, forklift_df):
    final_forklift_df = mo.sql(
        """
        WITH main AS (FROM
            forklift_df
        SELECT
            day AS day_name,
            date,
            customer AS vessel,
            start_time,
            end_time,
            duration AS hours_of_service,
            service_type AS purpose),salt AS (
            FROM f_salt_df
        SELECT day AS day_name,
        date,
        vessel,
        start_time,
        end_time,
        total_duration AS hours_of_service,
        purpose
            )


        FROM main 
        UNION ALL
        FROM salt
        ORDER BY date,start_time
        """
    )
    return (final_forklift_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Handling Activity
    ---

    #### Extra Men
    """)


@app.cell(hide_code=True)
def _(end_date, extra_men_dataset_new, select_report, start_date):
    extra_men_df = mo.sql(
        f"""
        FROM
            EXTRA_MEN_DATASET_NEW
        WHERE
            vessel = '{select_report.value}'
            AND date BETWEEN '{start_date.value}' AND '{end_date}'
        """
    )
    return (extra_men_df,)


@app.cell(hide_code=True)
def _(extra_men_df):
    summary_extra_men_df = mo.sql(
        """
        WITH normal AS (FROM extra_men_df
            SELECT 'Normal Hour' AS description,SUM(total_tonnage * extra_men::INT)::DECIMAL AS value,MIN(price) AS unit_price
        WHERE price = 0.8),overtime AS (FROM extra_men_df
            SELECT 'Overtime Hour' AS description,SUM(total_tonnage * extra_men::INT)::DECIMAL AS value,MIN(price) AS unit_price
        WHERE price = 1.2)


        ,grouped AS (FROM normal
        UNION ALL
        FROM overtime)


            FROM grouped
        SELECT *,(value * unit_price)::DECIMAL AS total_price
        """
    )
    return (summary_extra_men_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Additional Overtime
    """)


@app.cell(hide_code=True)
def _(additional_overtime_dataset_new, end_date, select_report, start_date):
    additional_overtime_df = mo.sql(
        f"""
        FROM ADDITIONAL_OVERTIME_DATASET_NEW
        WHERE
            vessel = '{select_report.value}'
            AND date::DATE BETWEEN '{start_date.value}' AND '{end_date}'
        ORDER BY date
        """
    )
    return (additional_overtime_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Well to Well
    """)


@app.cell(hide_code=True)
def _(end_date, select_report, start_date, well_inv_df):
    well_to_well_df = mo.sql(
        f"""
        WITH main AS (FROM well_inv_df
            SELECT "Day" AS day_name,
            "Date" AS date,
            "Vessel" AS vessel,
            "Tonnage" AS tonnage,
            CASE WHEN "Day" IN ('Sun','PH') THEN 17*1.5 ELSE 17 END AS unit_price
        WHERE
            vessel = '{select_report.value}'
            AND date BETWEEN '{start_date.value}' AND '{end_date}')

        FROM main
        SELECT *, tonnage * unit_price AS total_price
        """
    )
    return (well_to_well_df,)


@app.cell(hide_code=True)
def _(well_to_well_df):
    summary_well_to_well_df = mo.sql(
        """
        WITH main AS (FROM
            well_to_well_df
        SELECT
            CASE
                WHEN day_name IN ('Sun', 'PH') THEN 'Overtime'
                ELSE 'Normal'
            END AS overtime,
            SUM(tonnage) AS total_tonnage
        GROUP BY ALL),sort AS (SELECT *
            FROM (
                VALUES
                    ('Normal', 1),
                    ('Overtime', 2),
            ) AS t(overtime, sort))

        SELECT s.overtime,COALESCE(m.total_tonnage,0) AS total_tonnage
        FROM sort s LEFT JOIN main m ON m.overtime = s.overtime
        ORDER BY s.sort
        """
    )
    return (summary_well_to_well_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Net List
    """)


@app.cell(hide_code=True)
def _(end_date, net_list_dataset, select_report, start_date):
    net_list_df = mo.sql(
        f"""
        WITH adjusted AS (
            SELECT
                * EXCLUDE(unit_price, remarks),

                CASE
                    WHEN service = 'Full OSS' AND overtime = 'normal hours'
                        THEN 38.75::DECIMAL
                    WHEN service = 'Full OSS' AND overtime = 'overtime 150%'
                        THEN 58.125::DECIMAL
                    WHEN service = 'Full OSS' AND overtime = 'overtime 200%'
                        THEN 77.5::DECIMAL
                    ELSE unit_price
                END AS unit_price,

                remarks

            FROM NET_LIST_DATASET

            WHERE
                vessel = '{select_report.value}'
                AND date BETWEEN '{start_date.value}' AND '{end_date}'
        )

        SELECT
            * EXCLUDE(invoice_value),
            total_tonnage::DECIMAL * unit_price::DECIMAL AS invoice_value
        FROM adjusted
        """
    )
    return (net_list_df,)


@app.cell(hide_code=True)
def _(net_list_df):
    summary_net_list_df = mo.sql(
        """
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
    * Net list metrics for summary
    """)


@app.cell
def _(summary_net_list_df):
    total_tonnage = summary_net_list_df.select(
        pl.col("total_tonnage").sum()
    ).item()
    return (total_tonnage,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Shore Crane ::lucide:git-pull-request-create::
    """)


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
        """
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


@app.cell
def _(to_filter_transfer_df):
    container_on_plug = to_filter_transfer_df.filter(
        pl.col("exit_date").is_null()
    )
    container_on_plug


@app.cell(hide_code=True)
def _(to_filter_transfer_df, transfer_dataset):
    transfer_df = mo.sql(
        """
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
           AND t.remarks NOT IN ('IOT')
        """
    )
    return (transfer_df,)


@app.cell(hide_code=True)
def _(transfer_df):
    summary_transfer_df = mo.sql(
        """
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


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    --- Berth Dues
    """)


@app.cell(hide_code=True)
def _(final_berth_df):
    berth_bc = mo.sql(
        """
        WITH type_map AS (
                        SELECT * FROM (
                            VALUES
                                ('FISHING VESSEL', 'BERTH DUES (FISHING VESSELS)'),
                                ('CARGO REEFER', 'BERTH DUES (CARGO)'),
                                ('LONGLINER', 'BERTH DUES (LONGLINER)'),
                                ('SUPPLY BOAT', 'BERTH DUES (SUPPLY VESSELS)'),
                                ('MILITARY VESSEL', 'BERTH DUES (MILITARY)')
                        ) AS t (vessel_type, Description)
                    ),
                    -- one charge line per vessel type
                    charges AS (
                        SELECT
                            COALESCE(SUM(f.duration_in_port), 0) AS QTY,
                            t.Description,
                            'STD' AS Variant
                        FROM final_berth_df f
                        JOIN type_map t ON t.vessel_type = f.vessel_type
                        GROUP BY t.Description
                    ),
                    -- one discount line (QTY = -1) per vessel type with a discount
                    discounts AS (
                        SELECT
                            -1 AS QTY,
                            t.Description,
                            'STD' AS Variant
                        FROM final_berth_df f
                        JOIN type_map t ON t.vessel_type = f.vessel_type
                        WHERE f.discount < 0
                        GROUP BY t.Description
                    )
                FROM charges
                UNION ALL
                FROM discounts
        """
    )
    return (berth_bc,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    -- Tare Weight
    """)


@app.cell(hide_code=True)
def _(tare_summary_df):
    tare_bc = mo.sql(
        """
        WITH map AS (
                    SELECT * FROM (
                        VALUES
                            ('rental', 'TARE- RENTAL'),
                            ('calibration', 'CALIBRATION')
                    ) AS t (service, Description)
                )
                SELECT
                    s.quantity AS QTY,
                    m.Description,
                    'STD' AS Variant
                FROM tare_summary_df s
                JOIN map m ON m.service = s.service
        """,
        output=False
    )
    return (tare_bc,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    -- Stuffing
    """)


@app.cell(hide_code=True)
def _(stuffing_df):
    stuffing_bc = mo.sql(
        """
        WITH
            plugin AS (
                FROM
                    stuffing_df
                SELECT
                    COUNT(*) AS QTY,
                    'PLUGIN' AS Description,
                    'STD' AS Variant
                WHERE
                    remarks= ''
            ),
            monitoring AS (
                FROM
                    stuffing_df
                SELECT DISTINCT
                    COUNT(*) AS QTY,
                    'MONITORING' AS Description,
                    'STD' AS Variant
                WHERE
                    remarks= ''
                    AND plugged_status <> 'Partial'
            ),
            standard AS (
                FROM
                    stuffing_df
                SELECT
                    COALESCE(SUM(days_on_plug), 0) AS QTY,
                    'ELECTRICITY - 25°' AS Description,
                    'STD' AS Variant
                WHERE
                    set_point = -25
                    AND remarks = ''
            ),
            magnum AS (
                FROM
                    stuffing_df
                SELECT
                    COALESCE(SUM(days_on_plug), 0) AS QTY,
                    'ELECTRICITY - 35°' AS Description,
                    'STD' AS Variant
                WHERE
                    set_point = -35
                    AND remarks = ''
            ),
            s_freezer AS (
                FROM
                    stuffing_df
                SELECT
                    COALESCE(SUM(days_on_plug), 0) AS QTY,
                    'ELECTRICITY - 60°' AS Description,
                    'STD' AS Variant
                WHERE
                    set_point = -60
                    AND remarks= ''
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
        """,
        output=False
    )
    return (stuffing_bc,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    -- Salt Operation
    """)


@app.cell(hide_code=True)
def _(salt_summary_df):
    salt_bc = mo.sql(
        """
        WITH variant_map AS (
                    SELECT * FROM (
                        VALUES
                            ('Normal Hours', 'STD'),
                            ('Overtime 150%', '150%'),
                            ('Overtime 200%', '200%')
                    ) AS t (service, Variant)
                )
                SELECT
                    s.quantity AS QTY,
                    'SALT LOADING' AS Description,
                    v.Variant
                FROM salt_summary_df s
                JOIN variant_map v ON v.service = s.service
        """,
        output=False
    )
    return (salt_bc,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    --- Forklift
    """)


@app.cell(hide_code=True)
def _(forklift_salt_summary_df, forklift_summary_df):
    forklift_bc = mo.sql(
        """
        WITH
            all_data AS (
                FROM
                    forklift_salt_summary_df
                UNION ALL
                FROM
                    forklift_summary_df
            ),
            grouped AS (
                FROM
                    all_data
                SELECT
                    service,
                    SUM(quantity) AS quantity
                GROUP BY ALL
            ),
        --- 
         normal AS (
                FROM
                    grouped
                SELECT
                    quantity AS QTY,
                    'FORKLIFT RENTAL' AS Description,
                    'STD' AS Variant
                WHERE
                    service = 'Normal Hours'
            ),
            overtime_150 AS (
                FROM
                    grouped
                SELECT
                    quantity AS QTY,
                    'FORKLIFT RENTAL' AS Description,
                    '150%' AS Variant
                WHERE
                    service = 'Overtime 150%'
            ),
            overtime_200 AS (
                FROM
                    grouped
                SELECT
                    quantity AS QTY,
                    'FORKLIFT RENTAL' AS Description,
                    '200%' AS Variant
                WHERE
                    service = 'Overtime 200%'
            )
        FROM
            normal
        UNION ALL
        FROM
            overtime_150
        UNION ALL
        FROM
            overtime_200
        """,
        output=False
    )
    return (forklift_bc,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    -- Extra Men
    """)


@app.cell(hide_code=True)
def _(summary_extra_men_df):
    extra_men_bc = mo.sql(
        """
        WITH normal AS (FROM summary_extra_men_df
            SELECT COALESCE(SUM(value),0) AS QTY,
            'EXTRA MEN' AS Description,
            'STD' AS Variant
        WHERE description = 'Normal Hour'),overtime AS (FROM summary_extra_men_df
            SELECT COALESCE(SUM(value),0) AS QTY,
            'EXTRA MEN' AS Description,
            '150%' AS Variant
        WHERE description = 'Overtime Hour')


        FROM normal 
        UNION ALL
        FROM overtime
        """,
        output=False
    )
    return (extra_men_bc,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    -- Additional Overtime
    """)


@app.cell(hide_code=True)
def _(additional_overtime_df):
    additional_overtime_bc = mo.sql(
        """
        FROM additional_overtime_df
        SELECT COALESCE(SUM(number_of_stevedores::INT * number_of_hours),0) AS QTY,
        'ADDITIONAL OVERTIME' AS Description,
        'STD' AS Variant
        """,
        output=False
    )
    return (additional_overtime_bc,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    --- Well to Well
    """)


@app.cell(hide_code=True)
def _(summary_well_to_well_df):
    well_to_well_bc = mo.sql(
        """
        WITH variant_map AS (
                    SELECT * FROM (
                        VALUES
                            ('Normal', 'STD'),
                            ('Overtime', '150%')
                    ) AS t (overtime, Variant)
                )
                SELECT
                    s.total_tonnage AS QTY,
                    'WELL TO WELL' AS Description,
                    v.Variant
                FROM summary_well_to_well_df s
                JOIN variant_map v ON v.overtime = s.overtime
        """,
        output=False
    )
    return (well_to_well_bc,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    --- Net List
    """)


@app.cell(hide_code=True)
def _(summary_net_list_df):
    net_list_bc = mo.sql(
        """
        WITH
                    service_map AS (
                        SELECT * FROM (
                            VALUES
                                ('Unload to Quay', 'Brine', 'UNLOAD TO QUAY - BRINE'),
                                ('Unload to Quay', 'Dry', 'UNLOAD TO QUAY - DRY'),
                                ('Unload to CCCS', 'Brine', 'UNLOAD TO COLD STORE - BRINE'),
                                ('Unload to CCCS', 'Dry', 'UNLOAD TO COLD STORE - DRY'),
                                ('Transhipment', 'Brine', 'TRANSHIPMENT - BRINE'),
                                ('Transhipment', 'Dry', 'TRANSHIPMENT - DRY'),
                                ('Basic OSS', 'Brine', 'STO -BASIC OSS - BRINE'),
                                ('Basic OSS', 'Dry', 'STO -BASIC OSS - DRY'),
                                ('Container Stuffing', 'Brine', 'CONTAINER STUFFING - BRINE'),
                                ('Container Stuffing', 'Dry', 'CONTAINER STUFFING - DRY'),
                                ('Full OSS', 'Brine', 'CONTAINER STUFFING - BRINE'),
                                ('Full OSS', 'Dry', 'CONTAINER STUFFING - DRY')
                        ) AS t (service, storage_type, Description)
                    ),
                    variant_map AS (
                        SELECT * FROM (
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
                FROM summary_net_list_df s
                JOIN service_map m
                    ON m.service = s.service
                    AND m.storage_type = s.storage_type
                JOIN variant_map v ON v.overtime = s.overtime
        """,
        output=False
    )
    return (net_list_bc,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    --- Shore Crane
    """)


@app.cell(hide_code=True)
def _(summary_shore_crane_df):
    shore_crane_bc = mo.sql(
        """
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


@app.cell(hide_code=True)
def _(summary_transfer_df):
    transfer_bc = mo.sql(
        """
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


@app.cell
def _(
    additional_overtime_bc,
    berth_bc,
    extra_men_bc,
    forklift_bc,
    net_list_bc,
    salt_bc,
    shore_crane_bc,
    stuffing_bc,
    tare_bc,
    transfer_bc,
    well_to_well_bc,
):
    combined_services_bc = pl.concat(
        [
            _df.select(
                pl.col("QTY").cast(pl.Decimal(scale=3)),
                pl.col("Description").cast(pl.Utf8),
                pl.col("Variant").cast(pl.Utf8),
            )
            for _df in [
                berth_bc,
                tare_bc,
                stuffing_bc,
                salt_bc,
                forklift_bc,
                extra_men_bc,
                additional_overtime_bc,
                well_to_well_bc,
                net_list_bc,
                shore_crane_bc,
                transfer_bc,
            ]
        ]
    )
    combined_services_bc
    return (combined_services_bc,)


@app.cell(hide_code=True)
def _(combined_services_bc, customer):
    pricing_df = mo.sql(
        f"""
        WITH
            bc AS (
                SELECT
                    "Type",
                    "No.",
                    "Description",
                    "Variant",
                    "Unit Price",
                    list_position(
                        [
                            'BERTH DUES (FISHING VESSELS)',
                            'BERTH DUES (LONGLINER)',
                            'BERTH DUES (CARGO)',
                            'BERTH DUES (SUPPLY VESSELS)',
                            'BERTH DUES (MILITARY)',
                            'TARE- RENTAL',
                            'CALIBRATION',
                            'PLUGIN',
                            'MONITORING',
                            'ELECTRICITY - 25°',
                            'ELECTRICITY - 35°',
                            'ELECTRICITY - 60°',
                            'SALT LOADING',
                            'FORKLIFT RENTAL',
                            'EXTRA MEN',
                            'ADDITIONAL OVERTIME',
                            'WELL TO WELL',
                            'UNLOAD TO COLD STORE - DRY',
                            'UNLOAD TO COLD STORE - BRINE',
                            'TRANSHIPMENT - DRY',
                            'TRANSHIPMENT - BRINE',
                            'STO -BASIC OSS - DRY',
                            'STO -BASIC OSS - BRINE',
                            'UNLOAD TO QUAY - DRY',
                            'UNLOAD TO QUAY - BRINE',
                            'CONTAINER STUFFING - DRY',
                            'CONTAINER STUFFING - BRINE',
                            'SHORE CRANE RENTAL',
                            'TRANSFER FEU'
                        ],
                        "Description"
                    ) AS sort_order
                FROM
                    price_df
                WHERE
                    "Description" IN (
                        'BERTH DUES (FISHING VESSELS)',
                        'BERTH DUES (LONGLINER)',
                        'BERTH DUES (CARGO)',
                        'BERTH DUES (SUPPLY VESSELS)',
                        'BERTH DUES (MILITARY)',
                        'TARE- RENTAL',
                        'CALIBRATION',
                        'PLUGIN',
                        'MONITORING',
                        'ELECTRICITY - 25°',
                        'ELECTRICITY - 35°',
                        'ELECTRICITY - 60°',
                        'SALT LOADING',
                        'FORKLIFT RENTAL',
                        'EXTRA MEN',
                        'ADDITIONAL OVERTIME',
                        'WELL TO WELL',
                        'UNLOAD TO COLD STORE - DRY',
                        'UNLOAD TO COLD STORE - BRINE',
                        'TRANSHIPMENT - DRY',
                        'TRANSHIPMENT - BRINE',
                        'STO -BASIC OSS - DRY',
                        'STO -BASIC OSS - BRINE',
                        'UNLOAD TO QUAY - DRY',
                        'UNLOAD TO QUAY - BRINE',
                        'CONTAINER STUFFING - DRY',
                        'CONTAINER STUFFING - BRINE',
                        'SHORE CRANE RENTAL',
                        'TRANSFER FEU'
                    )
            ),

            summaries AS (
                FROM
                    combined_services_bc
                SELECT
                    *
                WHERE
                    QTY <> 0
            )

        SELECT
            b."Type",
            b."No.",
            b."Description",
            b."Variant",
            s.QTY::DECIMAL AS QTY,
            CASE
                WHEN b."Description" = 'BERTH DUES (FISHING VESSELS)'
                    AND '{customer.value}' IN (
                        'ECHEBASTAR',
                        'HARTSWATER LIMITED'
                    )
                THEN 1250
                ELSE b."Unit Price"::DECIMAL
            END AS "Unit Price"
        FROM
            bc AS b
        LEFT JOIN summaries AS s
            ON s."Description" = b."Description"
            AND s."Variant" = b."Variant"
        WHERE
            s.QTY IS NOT NULL
        ORDER BY
            b.sort_order,
            b."Variant";
        """
    )
    return (pricing_df,)


@app.cell(hide_code=True)
def _(service_breakdown_df):
    metrics_df = mo.sql(
        """
        WITH
            a AS (
                FROM
                    service_breakdown_df
                SELECT
                    Description,
                    Variant,
                    "unit_price",
                    quantity AS QTY,
                    total AS total_price
            )
        FROM
            a
        SELECT SUM(total_price) AS total_price
        """
    )
    return (metrics_df,)


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(pricing_df):
    bc_df = mo.sql(
        """
        FROM pricing_df
            SELECT * EXCLUDE("Unit Price")
        """
    )
    return (bc_df,)


@app.cell(hide_code=True)
def _(discount_berthing_figure: int, pricing_df, value_berthing_figure: int):
    service_breakdown_df = mo.sql(
        f"""
        FROM pricing_df
                SELECT
                    'STO Services' AS section,
            Description,
            Variant,
                    Description ||
                        CASE WHEN Variant <> 'STD' THEN ' (' || Variant || ')' ELSE '' END
                        AS service,
                    ROW_NUMBER() OVER (ORDER BY Description, Variant) AS sort_order,
                    "Unit Price" AS unit_price,
                    QTY AS quantity,
                    CASE WHEN service LIKE '%BERTH%' AND QTY > 0 THEN '{value_berthing_figure}' WHEN service LIKE '%BERTH%' AND QTY < 0 THEN '{discount_berthing_figure}' ELSE ROUND(QTY * "Unit Price", 2) END AS total

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


if __name__ == "__main__":
    app.run()
