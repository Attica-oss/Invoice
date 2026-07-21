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
    from datasets import cccs_stuffing, iot_soc

    with ThreadPoolExecutor(max_workers=5) as _pool:
        # _f_cccs_stuffing = _pool.submit(lambda: cccs_stuffing().collect())
        _f_stuffing = _pool.submit(lambda: iot_soc.iot_stuffing().collect())
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

    # CCCS_STUFFING_DATASET = _f_cccs_stuffing.result()
    STUFFING_DATASET = _fix_time(_f_stuffing.result())
    price_df = _f_price.result()

    # def _build_container_origin(df: pl.DataFrame) -> pl.DataFrame:
    #     records = df.select(
    #         "container_number", "date_plugged", "time_plugged", "vessel_client"
    #     )
    #     predecessors = (
    #         df.filter(pl.col("date_out") != pl.col("date_plugged"))
    #         .select("container_number", pl.col("date_out").alias("date_plugged"))
    #         .drop_nulls()
    #         .unique()
    #     )
    #     chain_starts = (
    #         records.join(predecessors, on=["container_number", "date_plugged"], how="anti")
    #         .sort("container_number", "date_plugged")
    #         .select(
    #             "container_number",
    #             "date_plugged",
    #             pl.col("vessel_client").alias("original_vessel"),
    #         )
    #     )
    #     return (
    #         records.sort("container_number", "date_plugged")
    #         .join_asof(
    #             chain_starts,
    #             by="container_number",
    #             on="date_plugged",
    #             strategy="backward",
    #         )
    #         .select(
    #             "container_number", "date_plugged", "time_plugged", "original_vessel"
    #         )
    #     )

    # CONTAINER_ORIGIN = _build_container_origin(ELECTRICITY_DATASET)


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
            "IOT",
        ],
        value="IOT",
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
def _(stuffing_summary_df):
    stuffing_summary_df
    return


@app.cell
def _(stuffing_summary_df):
    normal_tonnage = stuffing_summary_df.filter(pl.col("overtime").eq("normal hours")).select(pl.col("tonnage")).sum().item()
    overtime_150_tonnage = stuffing_summary_df.filter(pl.col("overtime").eq("overtime 150%")).select(pl.col("tonnage")).sum().item()
    overtime_200_tonnage = stuffing_summary_df.filter(pl.col("overtime").eq("overtime 200%")).select(pl.col("tonnage")).sum().item()
    total_tonnage = stuffing_summary_df.select(pl.col("tonnage")).sum().item()
    return (
        normal_tonnage,
        overtime_150_tonnage,
        overtime_200_tonnage,
        total_tonnage,
    )


@app.cell
def _():
    return


@app.cell
def _(
    normal_tonnage,
    overtime_150_tonnage,
    overtime_200_tonnage,
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
                    Normal Hours
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {normal_tonnage}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Tons
                </td>
            </tr>

            <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    Overtime 150%
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {overtime_150_tonnage}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Tons
                </td>
            </tr>

                    <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    Overtime 200%
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {overtime_200_tonnage}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Tons
                </td>
            </tr>

            <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    Total Tonnage
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {total_tonnage}
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


@app.cell
def _(
    copy_button,
    metrics_df,
    month_selector,
    save_button,
    select_report,
    summary_table,
):
    title = mo.md(f"# 🐟 IOT Stuffing Report")

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
def _(month_selector, select_report, stuffing_df):
    _has_data = (
   
        + len(stuffing_df)
   
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
        """
    )
    return (metrics_df,)


@app.cell(hide_code=True)
def _(stuffing_df):
    stuffing_summary_df = mo.sql(
        f"""
        WITH
            main AS (
                FROM
                    stuffing_df
                SELECT
                    overtime,
                    COALESCE(SUM(total_tonnage), 0.0)::DECIMAL AS tonnage,
                GROUP BY ALL
            ),
            sort_order AS (
                FROM
                    stuffing_df
                SELECT DISTINCT
                    overtime,
                    CASE
                        WHEN overtime = 'normal hours' THEN 1
                        WHEN overtime = 'overtime 150%' THEN 2
                        WHEN overtime = 'overtime 200%' THEN 3
                    END AS sort
            )
        SELECT
            m.overtime,
            tonnage
        FROM
            sort_order s
            LEFT JOIN main m ON m.overtime = s.overtime
        ORDER BY
            sort
        """
    )
    return (stuffing_summary_df,)


@app.cell(hide_code=True)
def _(month_selector, stuffing_dataset):
    stuffing_df = mo.sql(
        f"""
        FROM
            STUFFING_DATASET
        SELECT 
            *

        WHERE
            date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}')
        """
    )
    return (stuffing_df,)


@app.cell(hide_code=True)
def _(stuffing_df):
    summary_df = mo.sql(
        f"""
        WITH
   
            normal_stuffing AS (
                FROM
                    stuffing_df
                SELECT
                    COALESCE(SUM(total_tonnage), 0) AS QTY,
                    'STUFFING - FISHLOADER' AS Description,
                    'STD' AS Variant
                WHERE
                    overtime = 'normal hours'
            ), ot_1_stuffing AS (
                FROM
                    stuffing_df
                SELECT
                    COALESCE(SUM(total_tonnage), 0) AS QTY,
                    'STUFFING - FISHLOADER' AS Description,
                    '150%' AS Variant
                WHERE
                    overtime = 'overtime 150%'
            ), ot_2_stuffing AS (
                FROM
                    stuffing_df
                SELECT
                    COALESCE(SUM(total_tonnage), 0) AS QTY,
                    'STUFFING - FISHLOADER' AS Description,
                    '200%' AS Variant
                WHERE
                    overtime = 'overtime 200%'
            )


    

        FROM normal_stuffing
        UNION ALL
        FROM ot_1_stuffing
        UNION ALL
        FROM ot_2_stuffing
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
            'STUFFING - FISHLOADER',
            )),summaries AS (FROM summary_df)


        SELECT 
            b.Type,
            b."No.",
            b.Description,
            b.Variant,
            s.QTY::DECIMAL AS QTY,
            b."Unit Price"::DECIMAL AS "Unit Price"
        FROM bc b LEFT JOIN summaries s ON s.Description = b.Description AND s.Variant = b.Variant
        """
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
            SELECT *, ("Unit Price" * QTY)::DECIMAL AS total_price
        WHERE QTY > 0
        """
    )
    return


@app.cell(hide_code=True)
def _(pricing_df):
    service_breakdown_df = mo.sql(
        f"""
        WITH
            mapped AS (
                FROM
                    pricing_df
                SELECT
                    CASE
                        WHEN Description = 'STUFFING - FISHLOADER' THEN 'Vessel Unloading Operations'
                    END AS section,
                    CASE
                        WHEN Description = 'STUFFING - FISHLOADER'
                        AND Variant = 'STD' THEN 'Normal Hours'
                        WHEN Description = 'STUFFING - FISHLOADER'
                        AND Variant = '150%' THEN 'Overtime 150%'
                        WHEN Description = 'STUFFING - FISHLOADER'
                        AND Variant = '200%' THEN 'Overtime 200%'
                    END AS service,
                    CASE
                        WHEN Description = 'STUFFING - FISHLOADER'
                        AND Variant = 'STD' THEN 1
                        WHEN Description = 'STUFFING - FISHLOADER'
                        AND Variant = '150%' THEN 2
                        WHEN Description = 'STUFFING - FISHLOADER'
                        AND Variant = '200%' THEN 3
                    END AS sort_order,
                    Variant,
                    COALESCE(QTY, 0)::DECIMAL AS QTY,
                    "Unit Price",
                    (COALESCE(QTY, 0) * "Unit Price")::DECIMAL AS line_total
            )
        FROM
            mapped
        SELECT
            section,
            service,
            sort_order,
            -- base rate shown on the invoice; OT variants only affect the total
            "Unit Price" AS unit_price,
            SUM(QTY) AS quantity,
            ROUND(SUM(line_total), 2) AS total
        GROUP BY
            section,
            service,
            sort_order,
            unit_price
            HAVING section IS NOT NULL
        ORDER BY
            sort_order
        """
    )
    return (service_breakdown_df,)


@app.cell
def _(
    month_selector,
    normal_tonnage,
    overtime_150_tonnage,
    overtime_200_tonnage,
    select_report,
    service_breakdown_df,
    stuffing_df,
    total_tonnage,
):
    reports = {
        "Summary": {
            "type": "summary",
            "date_range": f"{format_datestr_to_month_year(month_selector.value)}",
            "client": f"{select_report.value}",
            "summary_rows": [
           
                None,  # blank row
                {
                    "label": "Normal Hours",
                    "value": normal_tonnage,
                    "unit": "Tons",
                    "italic": True,
                },
                 {
                    "label": "Overtime 150%",
                    "value": overtime_150_tonnage,
                    "unit": "Tons",
                    "italic": True,
                },
                        {
                    "label": "Overtime 200%",
                    "value": overtime_200_tonnage,
                    "unit": "Tons",
                    "italic": True,
                },
            
                None,
                {
                    "label": "Total Tonnage",
                    "value": (
                        total_tonnage
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
        "Stuffing": {
            "df": stuffing_df,
            "header_color": "#f35f97",
        }
    
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
