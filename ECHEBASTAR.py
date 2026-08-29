import marimo

__generated_with = "0.23.14"
app = marimo.App(width="columns")

with app.setup:
    from datetime import date
    from calendar import monthrange
    import polars as pl
    import marimo as mo
    from dataframe import forklift,forklift_for_salt,salt


@app.function
def eomonth(dt: str) -> date:
    converted_date = date.fromisoformat(dt)
    last_day = monthrange(converted_date.year, converted_date.month)[1]
    return converted_date.replace(day=last_day)


@app.cell
def _():
    select_report = mo.ui.dropdown(
        label="Select Line", options=["ECHEBASTAR","HARTSWATER LIMITED"], value="ECHEBASTAR"
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
def _():
    mo.md(r"""
    ## Salt Operations
    """)
    return


@app.cell(hide_code=True)
def _(month_selector, select_report):
    salt_df = mo.sql(
        f"""
        FROM salt
        WHERE customer = '{select_report.value}' AND (date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}'))
        """
    )
    return (salt_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Forklift
    """)
    return


@app.cell(hide_code=True)
def _(month_selector, select_report):
    forklift_salt_df = mo.sql(
        f"""
        WITH main AS (FROM
            forklift_for_salt
        SELECT
            * EXCLUDE (
                normal_hour_services,
                overtime_for_normal_services,
                overtime_for_extended_services
            ),
            CASE
                WHEN minute(normal_hour_services::TIME) = 0
                AND second(normal_hour_services::TIME) = 0
                AND microsecond(normal_hour_services::TIME) = 0 THEN HOUR(normal_hour_services::TIME)
                ELSE (hour(normal_hour_services::TIME) + 1) % 24
            END AS normal_hour,
            --- ot 1
            CASE
                WHEN minute(overtime_for_normal_services::TIME) = 0
                AND second(overtime_for_normal_services::TIME) = 0
                AND microsecond(overtime_for_normal_services::TIME) = 0 THEN HOUR(overtime_for_normal_services::TIME)
                ELSE (hour(overtime_for_normal_services::TIME) + 1) % 24
            END AS overtime_150_hour,
            --- ot 2
            CASE
                WHEN minute(overtime_for_extended_services::TIME) = 0
                AND second(overtime_for_extended_services::TIME) = 0
                AND microsecond(overtime_for_extended_services::TIME) = 0 THEN HOUR(overtime_for_extended_services::TIME)
                ELSE (hour(overtime_for_extended_services::TIME) + 1) % 24
            END AS overtime_200_hour
        WHERE
            customer = '{select_report.value}'
            AND (
                date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}')
            ))

        FROM main
        SELECT *,
        (overtime_200_hour * 60 ) + (overtime_150_hour * 45 ) + (normal_hour * 30) AS total_price
        """
    )
    return (forklift_salt_df,)


@app.cell(hide_code=True)
def _(month_selector, select_report):
    forklift_services_df = mo.sql(
        f"""
        FROM forklift
        WHERE
            invoiced_in = '{select_report.value}'
            AND (
                date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}')
            )
        """
    )
    return (forklift_services_df,)


@app.cell(hide_code=True)
def _(forklift_services_df, unpiv):
    forklift_summary_df = mo.sql(
        f"""
        WITH totals AS (
            FROM forklift_services_df
            SELECT
                COALESCE(CEILING(SUM(normal_hours) / 60),0) AS normal_hours,
                COALESCE(CEILING(SUM(overtime_150) / 60),0) AS overtime_150_hours,
                COALESCE(CEILING(SUM(overtime_200) / 60),0) AS overtime_200_hours
        ),unpiv AS (

        UNPIVOT totals
        ON
            normal_hours,
            overtime_150_hours,
            overtime_200_hours
        INTO
            NAME hour_type
            VALUE total_hours)

        FROM unpiv
        SELECT *, CASE WHEN hour_type = 'normal_hours' THEN total_hours * 30 WHEN hour_type = 'overtime_150_hours' THEN total_hours *45 WHEN hour_type = 'overtime_200_hours' THEN total_hours * 60 ELSE 0 END AS total_price
        """
    )
    return (forklift_summary_df,)


@app.cell
def _(forklift_salt_df, forklift_summary_df):
    all_forklift_df = mo.sql(
        f"""
        WITH
            f_normal AS (
                FROM
                    forklift_salt_df
                SELECT
                    SUM(normal_hour)::INT AS QTY,
                    'FORKLIFT RENTAL' AS Description,
                    'STD' AS Variant
            ),
            f_ot_1 AS (
                FROM
                    forklift_salt_df
                SELECT
                    SUM(overtime_150_hour)::INT AS QTY,
                    'FORKLIFT RENTAL' AS Description,
                    '150%' AS Variant
            ),
            f_ot_2 AS (
                FROM
                    forklift_salt_df
                SELECT
                    SUM(overtime_200_hour)::INT AS QTY,
                    'FORKLIFT RENTAL' AS Description,
                    '200%' AS Variant
            ),
            --Other forklift services
            fs_normal AS (
                FROM
                    forklift_summary_df
                SELECT
                    total_hours::INT AS QTY,
                    'FORKLIFT RENTAL' AS Description,
                    'STD' AS Variant
                WHERE
                    hour_type = 'normal_hours'
            ),
            fs_ot_1 AS (
                FROM
                    forklift_summary_df
                SELECT
                    total_hours::INT AS QTY,
                    'FORKLIFT RENTAL' AS Description,
                    '150%' AS Variant
                WHERE
                    hour_type = 'overtime_150_hours'
            ),
            fs_ot_2 AS (
                FROM
                    forklift_summary_df
                SELECT
                    total_hours::INT AS QTY,
                    'FORKLIFT RENTAL' AS Description,
                    '200%' AS Variant
                WHERE
                    hour_type = 'overtime_200_hours'
            ),
            --- Append all tables
            grouped_data AS (
        FROM
            f_normal
        UNION ALL
        FROM
            f_ot_1
        UNION ALL
        FROM
            f_ot_2
        UNION ALL
        FROM
            fs_normal
        UNION ALL

        FROM fs_ot_1
        UNION ALL

        FROM fs_ot_2)


        FROM grouped_data
        SELECT SUM(QTY)::INT AS QTY,
        Description, 
        Variant 
        GROUP BY ALL
        """
    )
    return (all_forklift_df,)


@app.cell(hide_code=True)
def _(all_forklift_df, salt_df):
    summary_df = mo.sql(
        f"""
        WITH
            salt_normal AS (
                FROM
                    salt_df
                SELECT
                    COALESCE(SUM(normal), 0) AS QTY,
                    'SALT LOADING' AS Description,
                    'STD' AS Variant
                GROUP BY ALL
            ),salt_ot_1 AS (
                FROM
                    salt_df
                SELECT
                    COALESCE(SUM(overtime_150), 0) AS QTY,
                    'SALT LOADING' AS Description,
                    '150%' AS Variant
                GROUP BY ALL
            ),salt_ot_2 AS (
                FROM
                    salt_df
                SELECT
                    COALESCE(SUM(overtime_200), 0) AS QTY,
                    'SALT LOADING' AS Description,
                    '200%' AS Variant
                GROUP BY ALL
            ),forklift_group AS (
            FROM all_forklift_df
            )


        FROM
            salt_normal
        UNION ALL
        FROM
            salt_ot_1
        UNION ALL
        FROM 
        salt_ot_2
        UNION ALL
        FROM forklift_group
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
                'FORKLIFT RENTAL',
                'SALT LOADING',
                -- 'SALT LOADING EXTERNAL',
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
        """
    )
    return


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
         SELECT COALESCE(SUM(total_price)::DECIMAL,0) AS total_price
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
