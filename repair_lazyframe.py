import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")

with app.setup:
    import polars as pl
    import marimo as mo
    from dataframe.transport import forklift

    from data_source.make_dataset import load_gsheet_data


@app.cell
def _():
    report_status = load_gsheet_data(
        sheet_id="1xNh4SiP_xw8Ck1baLhFazBsmMLIbOHjF4tg_M89efvI",
        sheet_name="report_status",
    ).and_then(
        lambda x: x.filter(pl.col("report_type").ne(pl.lit("OSS"))).select(
            pl.col("vessel/client").alias("customer"),
            pl.col("start_date"),
            pl.col("end_date").str.to_date(format="%d/%m/%Y", strict=False)

        ).with_row_index("row_nr").unique(["start_date","customer"])
    )
    return (report_status,)


@app.cell
def _(report_status):
    report_status.collect()
    return


@app.cell
def _():
    forklift.collect()
    return


@app.cell
def _():
    _df = mo.sql(
        f"""
        WITH
          flift AS (FROM forklift),
          service AS (FROM report_status),

          result AS (
            SELECT
              s.row_nr,
              COALESCE(f.customer, s.customer) AS customer,
              f.invoiced_in,

              -- keep raw totals (minutes)
              COALESCE(SUM(f.overtime_150), 0) AS min_150,
              COALESCE(SUM(f.overtime_200), 0) AS min_200,
              COALESCE(SUM(f.normal_hours), 0) AS min_normal,

              s.start_date,
              s.end_date
            FROM flift f
            FULL OUTER JOIN service s
              ON f.customer = s.customer
             AND f.date BETWEEN s.start_date AND s.end_date
            GROUP BY ALL
          )

        SELECT
          *,
          -- HH:MM display (from minutes; no rounding)
          LPAD(CAST(FLOOR(min_150 / 60) AS VARCHAR), 2, '0') || ':' ||
          LPAD(CAST((min_150 % 60) AS VARCHAR), 2, '0') AS overtime_150_hhmm,

          LPAD(CAST(FLOOR(min_200 / 60) AS VARCHAR), 2, '0') || ':' ||
          LPAD(CAST((min_200 % 60) AS VARCHAR), 2, '0') AS overtime_200_hhmm,

          LPAD(CAST(FLOOR(min_normal / 60) AS VARCHAR), 2, '0') || ':' ||
          LPAD(CAST((min_normal % 60) AS VARCHAR), 2, '0') AS normal_hhmm,

          -- billed hours (rounded up once)
          CEIL(min_150 / 60.0) AS billed_hrs_150,
          CEIL(min_200 / 60.0) AS billed_hrs_200,
          CEIL(min_normal / 60.0) AS billed_hrs_normal,

          -- example pricing (adjust rates as needed)
          CEIL(min_150 / 60.0) * 30 * 1.5 AS amount_150,
          CEIL(min_200 / 60.0) * 30 * 2.0 AS amount_200,
          CEIL(min_normal / 60.0) * 30       AS amount_normal

        FROM result
        WHERE row_nr IS NOT NULL
          AND customer IS NOT NULL
        ORDER BY start_date;
        """
    )
    return


@app.cell(hide_code=True)
def _():
    _df = mo.sql(
        f"""
        FROM transfer
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
