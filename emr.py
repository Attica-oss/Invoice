import marimo as mo

__generated_with = "0.18.4"
app = mo.App(width="full")

with app.setup:
    # Initialization code that runs before all other cells
    import polars as pl
    from utils.google_sheet import load_google_sheet
   


@app.cell
def _():
    pti_log = load_google_sheet(
        config_name="Container_PTI_log", parse_dates=True
    ).data

    gate_in = load_google_sheet(config_name="Container_Gate_In").data
    return gate_in, pti_log


@app.cell
def _(gate_in, pti_log):
    pti_against_entry = mo.sql(
        f"""
        WITH
            gate_in_of_empty AS (
                SELECT
                    Date AS gate_in_date,
                    Time AS gate_in_time,
                    "Container Number" AS container_number,
                    "Shipping Line" AS shipping_line,
                    Type AS container_type,
                    "Unit Manufacturer" AS unit_manufacturer,
                    Status AS entry_status,
                    "PTI Status" AS pti_status
                FROM
                    gate_in
                WHERE
                    Date BETWEEN '2025-01-01' AND '2025-12-31'
                    AND container_type <> 'Dry'
            ),
            pti AS (
                SELECT
                    "Date Plugin" AS plugged_date,
                    "Time Plugin" AS plugged_time,
                    "Container Number" AS container_number,
                    "Set Point" AS set_point,
                    CASE
                        WHEN "Set Point" = -60 THEN 'S Freezer'
                        WHEN "Set Point" = -35 THEN 'Magnum'
                        ELSE 'Standard'
                    END AS type_based_on_set_point,
                    "Unit Manufacturer" AS unit_manufacturer,
                    "Shipping Line" AS shipping_line,
                    "Sticker" AS sticker,
                    "Status" AS status,
            "Generator",
                    "Date Plugin" + "Time Plugin" AS datetime_start,
                    CASE
                        WHEN "Unplugged Date" = 'On Plug' THEN NULL
                        ELSE strptime("Unplugged Date", '%d/%m/%Y %H:%M')
                    END AS unplugged_date,
                    ROUND(EPOCH(unplugged_date - datetime_start) / 3600, 2) AS hours
                FROM
                    pti_log
                WHERE
                    "Date Plugin" BETWEEN '2025-12-01' AND '2025-12-31'
            )
        SELECT
            *
        FROM
            pti p
            JOIN gate_in_of_empty g ON g.container_number = p.container_number
            AND g.gate_in_date <= p.plugged_date
        ORDER BY p.plugged_date
        """,
        output=False,
    )
    return (pti_against_entry,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## PTI Queries and Issues - December '25
    """)
    return


@app.cell
def _():
    mo.md(r"""
    **To check the *Set Point* for this unit.**
    """)
    return


@app.cell(hide_code=True)
def _(pti_against_entry):
    _df = mo.sql(
        f"""
        SELECT plugged_date,plugged_time,container_number,set_point FROM pti_against_entry WHERE type_based_on_set_point <> container_type
        """
    )
    return


@app.cell
def _():
    mo.md(r"""
    **Unit manufacturer difference in PTI and Gate in record.**
    """)
    return


@app.cell(hide_code=True)
def _(pti_against_entry):
    _df = mo.sql(
        f"""
        SELECT
            MAX(plugged_date) AS plugged_date,
            container_number,
            unit_manufacturer,
            MAX(gate_in_date) AS gate_in_date,
            unit_manufacturer_1 AS unit_manufacturer_on_gate_in
        FROM
            pti_against_entry
        WHERE
            unit_manufacturer <> unit_manufacturer_1
        GROUP BY container_number,unit_manufacturer,unit_manufacturer_1
        """
    )
    return


@app.cell
def _():
    mo.md(r"""
    **Shipping Line : "Not Found"**
    """)
    return


@app.cell
def _(pti_against_entry):
    _df = mo.sql(
        f"""
        SELECT plugged_date,plugged_time,container_number,set_point,shipping_line FROM pti_against_entry WHERE shipping_line <> shipping_line_1
        """
    )
    return


@app.cell(hide_code=True)
def _(pti_against_entry):
    _df = mo.sql(
        f"""
        SELECT
            plugged_date,
            plugged_time,
            container_number,
            gate_in_date,
            gate_in_time,
            gate_in_time < plugged_time AS check_invalid_logic
        FROM
            pti_against_entry
        WHERE
            plugged_date = gate_in_date
            AND NOT (check_invalid_logic)
        """,
        output=False,
    )
    return


@app.cell
def _():
    mo.md(r"""
    **PTI service above 30 hours.**
    """)
    return


@app.cell
def _(pti_against_entry):
    _df = mo.sql(
        f"""
        SELECT datetime_start,container_number,set_point,shipping_line,unplugged_date,hours FROM pti_against_entry WHERE hours > 30
        """
    )
    return


@app.cell
def _():
    mo.md(r"""
    **PTI service with negative hours**
    """)
    return


@app.cell
def _(pti_against_entry):
    _df = mo.sql(
        f"""
        SELECT  datetime_start,container_number,set_point,shipping_line,unplugged_date,hours FROM pti_against_entry WHERE hours < 0
        """
    )
    return


@app.cell
def _(pti_against_entry):
    duplications = mo.sql(
        f"""
        SELECT
            *
        FROM
            pti_against_entry
        WHERE
            container_number IN (
                SELECT
                    container_number
                FROM
                    pti_against_entry
                GROUP BY
                    container_number
                HAVING
                    COUNT(*) > 1
            )
        ORDER BY container_number,plugged_date
        """,
        output=False,
    )
    return (duplications,)


@app.cell
def _():
    mo.md(r"""
    **Re-positioning of PASSED unit?**
    """)
    return


@app.cell
def _(pti_against_entry):
    _df = mo.sql(
        f"""
        SELECT plugged_date,plugged_time,container_number,set_point,shipping_line,unplugged_date,status FROM pti_against_entry WHERE container_number IN ('MNBU0228273');
        """
    )
    return


@app.cell
def _():
    mo.md(r"""
    **Duplication?**
    """)
    return


@app.cell
def _(pti_against_entry):
    _df = mo.sql(
        f"""
        SELECT plugged_date,plugged_time,container_number,set_point,shipping_line,unplugged_date,status FROM pti_against_entry WHERE container_number IN ('SUDU8195865');
        """
    )
    return


@app.cell
def _(pti_against_entry):
    _df = mo.sql(
        f"""
        SELECT plugged_date,plugged_time,container_number,set_point,shipping_line,unplugged_date,status FROM pti_against_entry WHERE container_number IN ('MNBU3789600');
        """
    )
    return


@app.cell
def _(duplications):
    _df = mo.sql(
        f"""
        SELECT
            datetime_start,
            container_number,
            unplugged_date,
            shipping_line,
            status
        FROM
            duplications
        """,
        output=False,
    )
    return


@app.cell
def _():
    mo.md(r"""
    **Based on the WhatsApp message dated 03/12/2025, we shall remove the following records if there are no objections.**
    """)
    return


@app.cell
def _():
    mo.image(src="public\image.png", width="250")
    return


@app.cell
def _(pti_against_entry):
    _df = mo.sql(
        f"""
        SELECT
           plugged_date,plugged_time,container_number,set_point,shipping_line,unplugged_date,status
        FROM
            pti_against_entry
        WHERE
            container_number IN (
                'MNBU9138503',
                'MNBU4302065',
                'MNBU3590820',
                'MNBU3917609'
            ) AND plugged_date < '2025-12-03'
        """
    )
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
