import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")

with app.setup:
    import polars as pl
    import marimo as mo

    from data_source.make_dataset import load_gsheet_data


@app.cell
def _():
    logistics_sheet_id = '1L9qkq9WlIa2j5DcvoLvxkqYogRg76S-e8OxAIyLruAE'


    gate_in_raw = load_gsheet_data(sheet_id=logistics_sheet_id,sheet_name="ContainerGateIn")
    gate_out_raw = load_gsheet_data(sheet_id=logistics_sheet_id,sheet_name='ContainerGateOut')
    plugin_raw = load_gsheet_data(sheet_id=logistics_sheet_id,sheet_name='ContainerPlugIn')
    pti_raw = load_gsheet_data(sheet_id=logistics_sheet_id,sheet_name='ContainerPTI')
    return gate_in_raw, gate_out_raw


@app.cell
def _():
    maersk_list = ["MNBU3898896","MNBU0224494","MNBU0031957","SUDU8002420"]

    maersk_df = pl.DataFrame({
        "container_number": maersk_list
    })
    return (maersk_df,)


@app.cell
def _(maersk_df):
    maersk_df
    return


@app.cell(hide_code=True)
def _(gate_in_raw, gate_out_raw, maersk_df):
    _df = mo.sql(
        f"""
        WITH
            gate_in AS (
                SELECT
                    g.Date,
                    g.Time,
                    g."Container Number"
                FROM
                    gate_in_raw g
                    RIGHT JOIN maersk_df m ON g."Container Number" = m.container_number
            ),
            gate_out AS (
                SELECT
                    g.Date,
                    g."Time Out",
                    g."Container Number",
                    g.Status
                FROM
                    gate_out_raw g
                    RIGHT JOIN maersk_df m ON g."Container Number" = m.container_number
            ),
            view_data AS (
                SELECT
                    i.Date AS date_in,
                    i.Time AS time_in,
                    i."Container Number" AS container_number,
                    o.Date AS date_out,
                    o."Time Out" AS time_out
                FROM
                    gate_in i
                    LEFT JOIN gate_out o ON i."Container Number" = o."Container Number"
            )
        FROM
            view_data
        SELECT
            *,
            DATE_DIFF('Days', date_in, date_out) + 1 AS total_duration_in_port,
        	DATE_DIFF('Days',date_in,'2025-12-31')+1 AS december,
            DATE_DIFF('Days','2026-01-01'::Date,'2026-01-31')+1 AS january,
        	DATE_DIFF('Days','2026-02-01',date_out)+1 AS february,
        """
    )
    return


@app.cell
def _():
    pl.read_excel(source=r"A:\INVOICING!!\2026\Shipping Line\Maersk\Maersk Depot - January'26.xlsx",sheet_name="Storage")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
