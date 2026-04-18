import marimo

__generated_with = "0.23.1"
app = marimo.App(width="columns", app_title="Process Invoice")

with app.setup:
    from datetime import date, timedelta
    from enum import StrEnum

    import marimo as mo
    import polars as pl
    from scan_google_sheet import scan_google_sheet


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Casting Types
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Container Number Validator
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### STDU container.
    Only one in service currently.
    """)
    return


@app.cell
def _():
    class STDUContainer(PolarsEnum):
        """Cast the two STDU containers"""

        STDU6536343 = "STDU6536343"
        STDU6536338 = "STDU6536338"

    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Container Validation Text Box Widget 🇬🇸
    """)
    return


@app.cell
def _():
    import re

    import anywidget
    import traitlets

    class ContainerValidationWidget(anywidget.AnyWidget):
        _esm = """
        function render({ model, el }) {
          const container = document.createElement("div");
          container.style.cssText = `
            font-family: monospace;
            padding: 14px 16px;
            border: 2px solid #555;
            border-radius: 6px;
            display: inline-block;
            transition: border-color 0.2s;
          `;

          const label = document.createElement("label");
          label.textContent = "Container Number";
          label.style.cssText = "display: block; font-weight: bold; margin-bottom: 6px; font-size: 13px;";

          const input = document.createElement("input");
          input.type = "text";
          input.placeholder = "e.g. MSCU1234565";
          input.style.cssText = `
            width: 200px;
            padding: 7px 10px;
            font-size: 14px;
            text-transform: uppercase;
            border: none;
            outline: none;
            font-family: monospace;
            letter-spacing: 1px;
            background: transparent;
            color: inherit;
          `;

          const validationMsg = document.createElement("div");
          validationMsg.style.cssText = "margin-top: 8px; font-size: 13px; font-weight: bold;";

          const details = document.createElement("div");
          details.style.cssText = `
            margin-top: 8px;
            font-size: 12px;
            opacity: 0.7;
            border-top: 1px solid #555;
            padding-top: 6px;
          `;

          function updateValidation() {
            const isValid = model.get("is_valid");
            const msg = model.get("validation_message");
            const owner = model.get("owner_code");
            const equip = model.get("equipment_category");
            const serial = model.get("serial_number");
            const check = model.get("check_digit");

            validationMsg.textContent = msg;

            if (!msg) {
              validationMsg.style.color = "";
              container.style.borderColor = "#555";
            } else if (isValid) {
              validationMsg.style.color = "#4caf50";
              container.style.borderColor = "#4caf50";
            } else {
              validationMsg.style.color = "#f44336";
              container.style.borderColor = "#f44336";
            }

            if (owner) {
              details.innerHTML = `
                <b>Owner:</b> ${owner} &nbsp;|&nbsp;
                <b>Category:</b> ${equip} &nbsp;|&nbsp;
                <b>Serial:</b> ${serial} &nbsp;|&nbsp;
                <b>Check Digit:</b> ${check}
              `;
            } else {
              details.innerHTML = "";
            }
          }

          input.addEventListener("input", (e) => {
            const value = e.target.value.toUpperCase();
            e.target.value = value;
            model.set("container_number", value);
            model.save_changes();
          });

          model.on("change:is_valid", updateValidation);
          model.on("change:validation_message", updateValidation);

          container.appendChild(label);
          container.appendChild(input);
          container.appendChild(validationMsg);
          container.appendChild(details);
          el.appendChild(container);

          updateValidation();
        }
        export default { render };
        """

        container_number = traitlets.Unicode("").tag(sync=True)
        is_valid = traitlets.Bool(False).tag(sync=True)
        validation_message = traitlets.Unicode("").tag(sync=True)
        owner_code = traitlets.Unicode("").tag(sync=True)
        equipment_category = traitlets.Unicode("").tag(sync=True)
        serial_number = traitlets.Unicode("").tag(sync=True)
        check_digit = traitlets.Unicode("").tag(sync=True)

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.observe(self._validate_container, names=["container_number"])

        def _validate_container(self, change):
            container = change["new"].strip().upper()

            if not container:
                self.is_valid = False
                self.validation_message = ""
                self.owner_code = ""
                self.equipment_category = ""
                self.serial_number = ""
                self.check_digit = ""
                return

            pattern = r"^([A-Z]{3})([UJZ])(\d{6})(\d)$"
            match = re.match(pattern, container)

            if not match:
                self.is_valid = False
                self.validation_message = (
                    "Invalid format. Expected: 3 owner letters + "
                    "equipment category (U/J/Z) + 6 digits + 1 check digit"
                )
                return

            owner, equipment, serial, check = match.groups()
            calculated_check = self._calculate_check_digit(owner + equipment + serial)

            self.owner_code = owner
            self.equipment_category = self._get_equipment_name(equipment)
            self.serial_number = serial
            self.check_digit = check

            if str(calculated_check) != check:
                self.is_valid = False
                self.validation_message = f"Invalid check digit. Expected {calculated_check}, got {check}"
            else:
                self.is_valid = True
                self.validation_message = "✓ Valid container number"

        def _calculate_check_digit(self, code: str) -> int:
            letter_values = {
                "A": 10, "B": 12, "C": 13, "D": 14, "E": 15, "F": 16, "G": 17,
                "H": 18, "I": 19, "J": 20, "K": 21, "L": 23, "M": 24, "N": 25,
                "O": 26, "P": 27, "Q": 28, "R": 29, "S": 30, "T": 31, "U": 32,
                "V": 34, "W": 35, "X": 36, "Y": 37, "Z": 38,
            }
            total = sum(
                (letter_values[c] if c.isalpha() else int(c)) * (2**i)
                for i, c in enumerate(code)
            )
            return (total % 11) % 10

        def _get_equipment_name(self, code: str) -> str:
            categories = {
                "U": "Freight Container",
                "J": "Detachable Freight Container Equipment",
                "Z": "Trailer and Chassis",
            }
            return f"{code} ({categories.get(code, 'Unknown')})"

    return (ContainerValidationWidget,)


@app.cell
def _(ContainerValidationWidget):
    myc = ContainerValidationWidget()
    myc
    return (myc,)


@app.cell
def _(myc):
    myc.is_valid
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Helper function
        * to clean transfer data, and keep only unique containers numbers as from 2025
    """)
    return


@app.cell
def _(TransferMovementType):
    def keep_only_collection_in_transfer(lf: pl.LazyFrame) -> pl.LazyFrame:
        return (
            lf.filter(
                pl.col("movement_type")
                .eq(TransferMovementType.COLLECTION)
                .and_(pl.col("date").dt.year().ge(2025))
            )
            .select(pl.col("container_number"), pl.col("line"))
            .unique()
        )

    return (keep_only_collection_in_transfer,)


@app.cell
def _(keep_only_collection_in_transfer):
    all_containers = scan_google_sheet(
        sheet_id="1O8K26c7CqLSdLr-f2gvZliDpBn9ArxXvj9tEJy-ElUg",
        sheet_name="Transfer",
    ).pipe(keep_only_collection_in_transfer)
    return (all_containers,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### IOT SOC
    """)
    return


@app.cell(hide_code=True)
def _(all_containers):
    iot_soc_list = mo.sql(
        f"""
        FROM all_containers
            SELECT container_number
        WHERE line = 'IOT'
        """,
        output=False
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Other Reefers
    """)
    return


@app.cell(hide_code=True)
def _(all_containers):
    _df = mo.sql(
        f"""
        FROM all_containers
        WHERE line IN ('CMA CGM','MAERSK')
        """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ---
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Polars Enum

    - Great for type casting
    """)
    return


@app.class_definition
class PolarsEnum(StrEnum):
    """StrEnum base with Polars helpers."""

    @classmethod
    def list_all(cls) -> list[str]:
        """All allowed string values (in enum order)."""
        return [m.value for m in cls]

    @classmethod
    def enum_dtype(cls) -> pl.Enum:
        """Polars Enum dtype for casting/validation."""
        return pl.Enum(cls.list_all())

    @classmethod
    def has_value(cls, value: str) -> bool:
        """Fast membership check against allowed values."""
        return value in cls._value2member_map_

    @classmethod
    def parse(cls, value: str) -> PolarsEnum:
        """
        Parse an exact value into the enum.
        Raises ValueError if invalid.
        """
        try:
            return cls(value)
        except Exception as e:
            raise ValueError(
                f"Invalid {cls.__name__}: {value!r}. Allowed: {cls.list_all()}"
            ) from e

    @classmethod
    def normalize(cls, value: str) -> str:
        """
        Normalize user input to the canonical enum value (case/whitespace tolerant).
        Returns the canonical string value (not the enum member).
        """
        if value is None:
            raise ValueError(f"{cls.__name__} cannot be None")

        v = value.strip()
        # exact match first
        if cls.has_value(v):
            return v

        # case-insensitive match
        lower_map = {m.value.lower(): m.value for m in cls}
        key = v.lower()
        if key in lower_map:
            return lower_map[key]

        raise ValueError(f"Invalid {cls.__name__}: {value!r}. Allowed: {cls.list_all()}")

    @classmethod
    def lit(cls, value: str) -> pl.Expr:
        """Polars literal cast to this enum dtype."""
        return pl.lit(cls.normalize(value)).cast(cls.enum_dtype())

    @classmethod
    def cast_col(cls, col: str | pl.Expr) -> pl.Expr:
        """Cast a column/expression to this enum dtype."""
        expr = pl.col(col) if isinstance(col, str) else col
        return expr.cast(cls.enum_dtype())


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Service Type Enum

    > To aid in fitering services that are grouped under a common name
    """)
    return


@app.class_definition
class ServiceGroupType(PolarsEnum):
    """Kind of Services by Group"""

    PTI = "PTI"
    NET_LIST = "Net List"
    CROSS_STUFFING = "Cross Stuffing"
    SALT = "Salt"
    STEVEDORING = "Stevedoring"
    BY_CATCH = "By Catch"
    DEPOT = "Depot"
    BIN_DISPATCH = "Bin Dispatch"
    CCCS = "CCCS"
    COLD_STORE_STUFFING = "Cold Store Stuffing"
    ELECTRICITY = "Electricity"
    TRANSFER = "Transfer"
    BERTH_DUES = "Berth Dues"


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Movement Type Enum
    """)
    return


@app.cell
def _():
    class TransferMovementType(PolarsEnum):
        """Kind of transfers"""

        DELIVERY = "Delivery"
        COLLECTION = "Collection"
        SHIFTING = "Shifting"

    class ColdStoreMovementType(PolarsEnum):
        """Cold Store Movement NOTE INTERVAL is not valid"""

        IN = "IN"
        OUT = "OUT"
        INTERNAL = "INTERNAL"

    return (TransferMovementType,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Vessel Type Enum
    """)
    return


@app.class_definition
class VesselType(PolarsEnum):
    """Vessel Type"""

    PURSEINER = "PURSEINER"
    CARGO_VESSEL = "CARGO VESSEL"
    SUPPLY_VESSEL = "SUPPLY VESSEL"
    LONGLINER = "LONGLINER"
    MILITARY_VESSEL = "MILITARY VESSEL"


@app.cell
def _():
    return


@app.cell(column=1, hide_code=True)
def _():
    mo.md(r"""
    ## Price DataFrame
    """)
    return


@app.cell
def _():
    MASTER_VALIDATION_URL = "https://docs.google.com/spreadsheets/d/1ai-zQMtbPUx0LeQeLmXcpPgKvL5cyvwDfSJqRzxfUQg/edit?gid=0#gid=0"
    return (MASTER_VALIDATION_URL,)


@app.cell
def _(MASTER_VALIDATION_URL):
    price_raw_dataf = scan_google_sheet(url=MASTER_VALIDATION_URL, sheet_name="Price")
    return (price_raw_dataf,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Process the LazyFrame with SQL
    """)
    return


@app.cell
def _(price_raw_dataf):
    price_df = mo.sql(
        f"""
        FROM
            price_raw_dataf
        SELECT
            Service AS service,
            "Class" AS service_type,
            Price AS unit_price,
            "StartingDate" AS effective_date
        WHERE
            "EndingDate" = ''
        """,
        output=False
    )
    return (price_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Helper function to filter price list
    """)
    return


@app.function
def filter_price_by_group(
    df: pl.DataFrame | pl.LazyFrame,
    service_group: ServiceGroupType,
):
    return df.filter(pl.col("service_type").eq(service_group))


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Example
    """)
    return


@app.cell
def _(price_df):
    price_df.pipe(filter_price_by_group, ServiceGroupType.TRANSFER)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### For only one service

    📓 To prepare a function for this
    """)
    return


@app.cell
def _(price_df):
    price_df.filter(pl.col("service").eq("Forklift"))
    return


@app.cell
def _():
    return


@app.cell(column=2, hide_code=True)
def _():
    mo.md(r"""
    # Customers
    """)
    return


@app.cell
def _(MASTER_VALIDATION_URL):
    client_raw_dataf = scan_google_sheet(url=MASTER_VALIDATION_URL, sheet_name="Client")
    return (client_raw_dataf,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Vessel list ⛴️
    """)
    return


@app.cell
def _():
    select_vessel = mo.ui.text(label="Search Vessel: ")
    select_vessel
    return (select_vessel,)


@app.cell
def _():
    iotc_vessel = pl.read_excel(source="./rav_search_2026-04-17T16_05_23.033Z.xlsx")
    return (iotc_vessel,)


@app.cell(hide_code=True)
def _(iotc_vessel, select_vessel):
    _df = mo.sql(
        f"""
        FROM iotc_vessel WHERE "Name" LIKE '%{select_vessel.value}%'
        """
    )
    return


@app.cell(hide_code=True)
def _(client_raw_dataf):
    vessel_df = mo.sql(
        f"""
        FROM
            client_raw_dataf
        SELECT
            "Vessel/Client" AS vessel,
            "Customer" AS ship_owner
        WHERE
            "Type" IN (
                '{VesselType.PURSEINER}',
                '{VesselType.CARGO_VESSEL}',
                '{VesselType.SUPPLY_VESSEL}',
                '{VesselType.LONGLINER}'
            )
        """
    )
    return


@app.cell
def _():
    return


@app.cell(column=3, hide_code=True)
def _():
    mo.md(r"""
    ## Public Holiday DataFrame
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Config ⚗️
    """)
    return


@app.cell
def _():
    # from __future__ import annotations

    # from datetime import date
    from dataclasses import dataclass

    @dataclass(frozen=True, slots=True)
    class ContinuousHoliday:
        start_year: int
        month: int
        day: int
        name: str

        def applies(self, year: int) -> bool:
            return year >= self.start_year

        def to_date(self, year: int) -> date:
            return date(year, self.month, self.day)

    NEW_CONTINUOUS_HOLIDAYS = [
        ContinuousHoliday(start_year=2026, month=2, day=1, name="Abolition of Slavery"),
    ]

    ONE_TIME_HOLIDAYS_BY_YEAR: dict[int, set[date]] = {
        2025: {
            date(2025, 10, 11),
            date(2025, 10, 13),
            date(2025, 10, 27),
        }
    }

    FIXED_HOLIDAYS = [
        (1, 1),
        (1, 2),
        (5, 1),
        (6, 18),
        (6, 29),
        (8, 15),
        (11, 1),
        (12, 8),
        (12, 25),
    ]
    return (
        FIXED_HOLIDAYS,
        NEW_CONTINUOUS_HOLIDAYS,
        ONE_TIME_HOLIDAYS_BY_YEAR,
        dataclass,
    )


@app.cell
def _(dataclass):
    # from __future__ import annotations

    # from dataclasses import dataclass
    # from datetime import date, timedelta
    # import polars as pl

    # from holiday_config import (
    #     FIXED_HOLIDAYS,
    #     NEW_CONTINUOUS_HOLIDAYS,
    #     ONE_TIME_HOLIDAYS_BY_YEAR,
    # )

    @dataclass(slots=True)
    class PublicHolidayCalendar:
        fixed_holidays: list[tuple[int, int]]
        continuous_holidays: list[dict]
        one_time_holidays_by_year: dict[int, set[date]]

        def get_fixed_holidays(self, year: int) -> set[date]:
            return {date(year, month, day) for month, day in self.fixed_holidays}

        def get_continuous_holidays(self, year: int) -> set[date]:
            holidays: set[date] = set()

            for holiday in self.continuous_holidays:
                if holiday.applies(year):
                    holidays.add(holiday.to_date(year))

            return holidays

        def get_one_time_holidays(self, year: int) -> set[date]:
            return self.one_time_holidays_by_year.get(year, set())

        def get_easter_related_holidays(self, year: int) -> set[date]:
            easter = self._calculate_easter_sunday(year)

            return {
                easter,  # Easter Sunday
                easter - timedelta(days=2),  # Good Friday
                easter - timedelta(days=1),  # Holy Saturday
                easter + timedelta(days=1),  # Easter Monday
                easter + timedelta(days=60),  # Corpus Christi
            }

        def get_religious_holidays(self, year: int) -> set[date]:
            return self.get_easter_related_holidays(year)

        def get_holidays(self, year: int) -> set[date]:
            holidays: set[date] = set()

            holidays |= self.get_fixed_holidays(year)
            holidays |= self.get_continuous_holidays(year)
            holidays |= self.get_one_time_holidays(year)
            holidays |= self.get_religious_holidays(year)

            holidays |= self._get_monday_after_sunday_holidays(holidays)

            return holidays

        def to_lazyframe(self, year: int) -> pl.LazyFrame:
            holidays = sorted(self.get_holidays(year))
            return pl.LazyFrame({"date": holidays}).with_columns(pl.lit("PH").alias("day_name"))

        @staticmethod
        def _get_monday_after_sunday_holidays(holidays: set[date]) -> set[date]:
            return {holiday + timedelta(days=1) for holiday in holidays if holiday.weekday() == 6}

        @staticmethod
        def _calculate_easter_sunday(year: int) -> date:
            a = year % 19
            b = year // 100
            c = year % 100
            d = (19 * a + b - b // 4 - ((b - (b + 8) // 25 + 1) // 3) + 15) % 30
            e = (32 + 2 * (b % 4) + 2 * (c // 4) - d - (c % 4)) % 7
            f = d + e - 7 * ((a + 11 * d + 22 * e) // 451) + 114
            month = f // 31
            day = f % 31 + 1
            return date(year, month, day)

    return (PublicHolidayCalendar,)


@app.cell
def _(
    FIXED_HOLIDAYS,
    NEW_CONTINUOUS_HOLIDAYS,
    ONE_TIME_HOLIDAYS_BY_YEAR: dict[int, set[date]],
    PublicHolidayCalendar,
):
    holiday_calendar = PublicHolidayCalendar(
        fixed_holidays=FIXED_HOLIDAYS,
        continuous_holidays=NEW_CONTINUOUS_HOLIDAYS,
        one_time_holidays_by_year=ONE_TIME_HOLIDAYS_BY_YEAR,
    )

    public_holidays_lf = holiday_calendar.to_lazyframe(2026)
    return (public_holidays_lf,)


@app.cell
def _():
    return


@app.cell
def _(public_holidays_lf):
    public_holiday_dates = public_holidays_lf
    return (public_holiday_dates,)


@app.cell(column=4, hide_code=True)
def _():
    mo.md(r"""
    # Shore Crane Rental Data
    """)
    return


@app.cell
def _():
    shore_crane_raw = scan_google_sheet(
        sheet_id="1O8K26c7CqLSdLr-f2gvZliDpBn9ArxXvj9tEJy-ElUg",
        sheet_name="ShoreCrane",
    )
    return (shore_crane_raw,)


@app.cell(hide_code=True)
def _(price_df, public_holiday_dates, shore_crane_raw):
    shore_crane_raw_df = mo.sql(
        f"""
        WITH unit_price AS (
            SELECT service, unit_price
            FROM price_df
            WHERE service = 'Shore Crane'
        ),

        ph_dates AS (
            SELECT
                CAST(date AS DATE) AS ph_date,
                day_name
            FROM public_holiday_dates
        ),

        raw AS (
            SELECT
                CAST("date" AS DATE) AS service_date,
               TRY_CAST(NULLIF(TRIM(CAST(start_time AS VARCHAR)), '') AS TIME)AS start_time,
               TRY_CAST(NULLIF(TRIM(CAST(end_time AS VARCHAR)), '') AS TIME) AS end_time,
                "hours",
                "overtime_hours",
                customer,
                location,
                operation_type,
                remarks,
                invoiced_to,
                'Shore Crane' AS service
            FROM shore_crane_raw
            WHERE YEAR("date") = 2026
        ),

        enriched AS (
            SELECT
                r.*,
                CASE
                    WHEN p.ph_date IS NOT NULL THEN p.day_name
                    ELSE STRFTIME(r.service_date, '%a')
                END AS day_name,
                CASE
                    WHEN remarks IS NULL OR remarks = '' THEN operation_type
                    ELSE operation_type || ' - ' || remarks
                END AS remarks_final
            FROM raw r
            LEFT JOIN ph_dates p
                ON r.service_date = p.ph_date
        ),

        validated AS (
            SELECT
                e.*,
                start_time IS NULL AS missing_start_time,
                end_time IS NULL AS missing_end_time,
                start_time IS NOT NULL
                    AND end_time IS NOT NULL
                    AND end_time < start_time AS end_before_start_time
            FROM enriched e
        ),

        durations AS (
            SELECT
                v.*,
                CASE
                    WHEN missing_start_time OR missing_end_time OR end_before_start_time THEN NULL
                    WHEN "hours" IS NULL THEN NULL
                    ELSE CEIL(EXTRACT(EPOCH FROM "hours") / 3600.0)
                END AS total_hours,
                CASE
                    WHEN missing_start_time OR missing_end_time OR end_before_start_time THEN NULL
                    WHEN "overtime_hours" IS NULL THEN NULL
                    ELSE CEIL(EXTRACT(EPOCH FROM "overtime_hours") / 3600.0)
                END AS overtime_hours_rounded
            FROM validated v
        ),

        normalized AS (
            SELECT
                d.*,
                CASE
                    WHEN total_hours IS NULL OR overtime_hours_rounded IS NULL THEN NULL
                    WHEN overtime_hours_rounded > total_hours THEN NULL
                    ELSE total_hours - overtime_hours_rounded
                END AS normal_hours,
                CASE
                    WHEN total_hours IS NOT NULL
                     AND overtime_hours_rounded IS NOT NULL
                     AND overtime_hours_rounded > total_hours
                    THEN TRUE
                    ELSE FALSE
                END AS overtime_exceeds_total
            FROM durations d
        ),

        priced AS (
            SELECT
                n.service_date,
                n.day_name,
                n.start_time,
                n.end_time,
                n.total_hours,
                n.overtime_hours_rounded AS overtime_hours,
                n.normal_hours,
                n.customer,
                n.location,
                n.remarks_final AS remarks,
                n.invoiced_to,
                u.unit_price AS base_unit_price,

                CASE
                    WHEN n.day_name IN ('Sun', 'PH')
                     AND n.service_date >= DATE '2026-03-01'
                        THEN ROUND(u.unit_price * 1.6, 3)
                    WHEN n.day_name IN ('Sun', 'PH')
                        THEN ROUND(u.unit_price * 1.5, 3)
                    ELSE ROUND(u.unit_price, 3)
                END AS unit_price,

                n.missing_start_time,
                n.missing_end_time,
                n.end_before_start_time,
                n.overtime_exceeds_total,

                CASE
                    WHEN n.missing_start_time THEN 'Missing start_time'
                    WHEN n.missing_end_time THEN 'Missing end_time'
                    WHEN n.end_before_start_time THEN 'end_time before start_time'
                    WHEN n.overtime_exceeds_total THEN 'overtime_hours exceeds total_hours'
                    ELSE 'OK'
                END AS validation_status
            FROM normalized n
            LEFT JOIN unit_price u
                ON u.service = n.service
        )

        FROM priced;
        """,
        output=False
    )
    return (shore_crane_raw_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Invalid Record to Clean Up
    """)
    return


@app.cell(hide_code=True)
def _(shore_crane_raw_df):
    _df = mo.sql(
        f"""
        FROM shore_crane_raw_df
        WHERE validation_status <> 'OK'
        """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    Valid Record to Process
    """)
    return


@app.cell(hide_code=True)
def _(shore_crane_raw_df):
    final_shore_crane_df = mo.sql(
        f"""
        SELECT
            day_name,
            service_date,
            start_time,
            end_time,
            total_hours,
            overtime_hours,
            customer,
            location,
            remarks,
            unit_price,
            CASE
                -- WHEN validation_status <> 'OK' THEN NULL

                WHEN day_name IN ('Sun', 'PH')
                 AND service_date >= DATE '2026-03-01'
                    THEN ROUND(
                        (base_unit_price * 1.6 * normal_hours) +
                        (base_unit_price * 2.1 * overtime_hours),
                        3
                    )

                WHEN day_name NOT IN ('Sun', 'PH')
                 AND service_date >= DATE '2026-03-01'
                    THEN ROUND(
                        (base_unit_price * 1.0 * normal_hours) +
                        (base_unit_price * 1.6 * overtime_hours),
                        3
                    )

                WHEN day_name IN ('Sun', 'PH')
                    THEN ROUND(
                        (base_unit_price * 1.5 * normal_hours) +
                        (base_unit_price * 2.0 * overtime_hours),
                        3
                    )

                ELSE ROUND(
                    (base_unit_price * 1.0 * normal_hours) +
                    (base_unit_price * 1.5 * overtime_hours),
                    3
                )
            END AS total_price,
            invoiced_to
        FROM shore_crane_raw_df
            WHERE validation_status = 'OK'
        ORDER BY service_date, start_time;
        """,
        output=False
    )
    return (final_shore_crane_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Sample of the data (LIMIT 5;)
    """)
    return


@app.cell(hide_code=True)
def _(final_shore_crane_df):
    _df = mo.sql(
        f"""
        FROM final_shore_crane_df
        LIMIT 5
        """
    )
    return


@app.cell(column=5, hide_code=True)
def _():
    mo.md(r"""
    # Openpyxl Styling for Excel
    """)
    return


@app.cell
def _():
    from openpyxl import load_workbook
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter
    from openpyxl.worksheet.table import TableStyleInfo

    def format_shore_crane_report(
        xlsx_path: str,
        sheet_name: str,
        vessel_name: str,
        start_date: date,
        end_date: date,
        service_name: str,
        table_name: str | None = None,
        header_color: str = "1F4E78",
    ) -> None:
        wb = load_workbook(xlsx_path)
        ws = wb[sheet_name]

        # ------------------------------------------------------------------
        # Page / worksheet layout
        # ------------------------------------------------------------------
        ws.page_setup.orientation = ws.ORIENTATION_LANDSCAPE
        ws.page_setup.paperSize = ws.PAPERSIZE_A4
        ws.sheet_view.showGridLines = False
        ws.sheet_view.view = "normal"

        # Fit to page width
        ws.page_setup.fitToWidth = 1
        ws.page_setup.fitToHeight = 0

        # Margins
        ws.page_margins.left = 0.25
        ws.page_margins.right = 0.25
        ws.page_margins.top = 0.5
        ws.page_margins.bottom = 0.5
        ws.page_margins.header = 0.2
        ws.page_margins.footer = 0.2

        # ------------------------------------------------------------------
        # Header / footer
        # ------------------------------------------------------------------
        header_text = f"{vessel_name} | {start_date:%d-%b-%Y} to {end_date:%d-%b-%Y}"

        # Centered header, centered footer
        ws.oddHeader.center.text = header_text
        ws.oddHeader.center.font = "Calibri,Bold"
        ws.oddHeader.center.size = 12

        ws.oddFooter.center.text = service_name
        ws.oddFooter.center.font = "Calibri"
        ws.oddFooter.center.size = 10

        # Optional page number on right footer
        ws.oddFooter.right.text = "Page &[Page] of &[Pages]"

        # Repeat header row when printing
        ws.print_title_rows = "1:1"

        # ------------------------------------------------------------------
        # Freeze panes
        # ------------------------------------------------------------------
        ws.freeze_panes = "A2"

        # ------------------------------------------------------------------
        # Determine used range
        # ------------------------------------------------------------------
        max_row = ws.max_row
        max_col = ws.max_column

        # ------------------------------------------------------------------
        # Table styling
        # ------------------------------------------------------------------
        # If a table already exists (e.g. from Polars), update its style.
        # Otherwise, leave it alone or create one separately if needed.
        if ws.tables:
            # Use the first table unless a table_name is provided
            if table_name and table_name in ws.tables:
                tab = ws.tables[table_name]
            else:
                first_key = next(iter(ws.tables))
                tab = ws.tables[first_key]

            # Built-in Excel table style only
            tab.tableStyleInfo = TableStyleInfo(
                name="TableStyleMedium2",
                showFirstColumn=False,
                showLastColumn=False,
                showRowStripes=True,
                showColumnStripes=False,
            )

        # ------------------------------------------------------------------
        # Header row formatting
        # ------------------------------------------------------------------
        header_fill = PatternFill("solid", fgColor=header_color)
        header_font = Font(color="FFFFFF", bold=True)
        header_alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = header_alignment

        ws.row_dimensions[1].height = 24

        # ------------------------------------------------------------------
        # Number formats
        # ------------------------------------------------------------------
        header_map = {ws.cell(row=1, column=c).value: c for c in range(1, max_col + 1)}

        currency_cols = ["Price / Hour ($)", "Total Price ($)"]
        int_cols = ["Hours", "Overtime Hours"]
        date_cols = ["Date"]
        time_cols = ["Start Time", "End Time"]

        for name in currency_cols:
            col_idx = header_map.get(name)
            if col_idx:
                for r in range(2, max_row + 1):
                    ws.cell(r, col_idx).number_format = "$#,##0.000"

        for name in int_cols:
            col_idx = header_map.get(name)
            if col_idx:
                for r in range(2, max_row + 1):
                    ws.cell(r, col_idx).number_format = "0"

        for name in date_cols:
            col_idx = header_map.get(name)
            if col_idx:
                for r in range(2, max_row + 1):
                    ws.cell(r, col_idx).number_format = "dd-mmm-yyyy"

        for name in time_cols:
            col_idx = header_map.get(name)
            if col_idx:
                for r in range(2, max_row + 1):
                    ws.cell(r, col_idx).number_format = "hh:mm"

        # ------------------------------------------------------------------
        # Alignment
        # ------------------------------------------------------------------
        center_headers = {
            "Day Name",
            "Date",
            "Start Time",
            "End Time",
            "Hours",
            "Overtime Hours",
        }
        right_headers = {"Price / Hour ($)", "Total Price ($)"}

        for col_name, col_idx in header_map.items():
            if col_name in center_headers:
                align = Alignment(horizontal="center", vertical="center")
            elif col_name in right_headers:
                align = Alignment(horizontal="right", vertical="center")
            else:
                align = Alignment(horizontal="left", vertical="center")

            for r in range(2, max_row + 1):
                ws.cell(r, col_idx).alignment = align

        # ------------------------------------------------------------------
        # Borders
        # ------------------------------------------------------------------
        thin_gray = Side(style="thin", color="D9D9D9")
        bottom_border = Border(bottom=thin_gray)

        for r in range(2, max_row + 1):
            for c in range(1, max_col + 1):
                ws.cell(r, c).border = bottom_border

        # ------------------------------------------------------------------
        # Auto width
        # ------------------------------------------------------------------
        width_overrides = {
            "Day Name": 11,
            "Date": 13,
            "Start Time": 12,
            "End Time": 12,
            "Hours": 8,
            "Overtime Hours": 15,
            "Client": 22,
            "Price / Hour ($)": 16,
            "Operation": 28,
            "Total Price ($)": 16,
        }

        for c in range(1, max_col + 1):
            col_letter = get_column_letter(c)
            header = ws.cell(1, c).value
            if header in width_overrides:
                ws.column_dimensions[col_letter].width = width_overrides[header]
            else:
                max_len = 0
                for r in range(1, max_row + 1):
                    value = ws.cell(r, c).value
                    if value is not None:
                        max_len = max(max_len, len(str(value)))
                ws.column_dimensions[col_letter].width = min(max(max_len + 2, 10), 30)

        # ------------------------------------------------------------------
        # Hide unused rows/columns to mimic "hide empty cells"
        # ------------------------------------------------------------------
        # Excel does not have a true "hide empty cells" mode for worksheets,
        # so we hide everything after the used range.
        for r in range(max_row + 1, 1048577):
            ws.row_dimensions[r].hidden = True
            if r > max_row + 200:
                break  # keep file size/work reasonable

        for c in range(max_col + 1, min(max_col + 51, 16385)):
            ws.column_dimensions[get_column_letter(c)].hidden = True

        # ------------------------------------------------------------------
        # Print area
        # ------------------------------------------------------------------
        ws.print_area = f"A1:{get_column_letter(max_col)}{max_row}"

        wb.save(xlsx_path)

    return (format_shore_crane_report,)


@app.cell
def _(end_date, final_shore_crane_df, start_date, vessel_name):
    summary_df = final_shore_crane_df.filter(
        (pl.col("service_date") >= start_date)
        & (pl.col("service_date") <= end_date)
        & (pl.col("customer") == vessel_name)
        & (pl.col("invoiced_to") != "MAERSKLINE")
    )
    return (summary_df,)


@app.cell(hide_code=True)
def _(summary_df):
    _df = mo.sql(
        f"""
        FROM summary_df
        """
    )
    return


@app.cell(hide_code=True)
def _(summary_df):
    _df = mo.sql(
        f"""
        WITH bucketed AS (
            SELECT
                service_date,
                day_name,
                total_hours,
                overtime_hours,
                unit_price,
                total_price,

                CASE
                    WHEN day_name NOT IN ('Sun', 'PH')
                        THEN total_hours - overtime_hours
                    ELSE 0
                END AS normal_hour_hours,

                CASE
                    WHEN day_name IN ('Sun', 'PH')
                        THEN total_hours - overtime_hours
                    WHEN day_name NOT IN ('Sun', 'PH') AND overtime_hours > 0
                        THEN overtime_hours
                    ELSE 0
                END AS overtime_150_hours,

                CASE
                    WHEN day_name IN ('Sun', 'PH') AND overtime_hours > 0
                        THEN overtime_hours
                    ELSE 0
                END AS overtime_200_hours
            FROM summary_df
        ),
        unpivoted AS (
            SELECT
                'normal_hour' AS bucket,
                normal_hour_hours AS hours,
                normal_hour_hours * unit_price AS price
            FROM bucketed

            UNION ALL

            SELECT
                'overtime_150' AS bucket,
                overtime_150_hours AS hours,
                overtime_150_hours * unit_price AS price
            FROM bucketed

            UNION ALL

            SELECT
                'overtime_200' AS bucket,
                overtime_200_hours AS hours,
                CASE
                    WHEN service_date >= DATE '2026-03-01'
                        THEN overtime_200_hours * unit_price * (2.1 / 1.6)
                    ELSE overtime_200_hours * unit_price * (2.0 / 1.5)
                END AS price
            FROM bucketed
        ),
        summary AS (
            SELECT
                bucket,
                SUM(hours) AS hours,
                ROUND(SUM(price), 3) AS price
            FROM unpivoted
            GROUP BY bucket
        )
        SELECT * FROM summary

        UNION ALL

        SELECT
            'total' AS bucket,
            SUM(hours) AS hours,
            ROUND(SUM(price), 3) AS price
        FROM summary;
        """
    )
    return


@app.cell
def _(
    end_date,
    final_shore_crane_df,
    format_shore_crane_report,
    start_date,
    vessel_name,
):
    final_df = (
        final_shore_crane_df.filter(
            (pl.col("service_date") >= start_date)
            & (pl.col("service_date") <= end_date)
            & (pl.col("customer") == vessel_name)
            & (pl.col("invoiced_to") != "MAERSKLINE")
        )
        .rename(
            {
                "day_name": "Day Name",
                "service_date": "Date",
                "start_time": "Start Time",
                "end_time": "End Time",
                "total_hours": "Hours",
                "overtime_hours": "Overtime Hours",
                "customer": "Client",
                "unit_price": "Price / Hour ($)",
                "remarks": "Operation",
                "total_price": "Total Price ($)",
            }
        )
        .drop(["invoiced_to"])
        .select(
            [
                "Day Name",
                "Date",
                "Start Time",
                "End Time",
                "Hours",
                "Overtime Hours",
                "Client",
                "Price / Hour ($)",
                "Operation",
                "Total Price ($)",
            ]
        )
    )

    final_df.write_excel(
        "shore_crane_report.xlsx",
        worksheet="Shore Crane",
        table_style="Table Style Medium 2",
        dtype_formats={
            pl.Date: "yyyy-mm-dd",
            pl.Time: "hh:mm",
        },
        column_formats={
            "Price / Hour ($)": "$#,##0.000",
            "Total Price ($)": "$#,##0.000",
            "Hours": "0",
            "Overtime Hours": "0",
        },
        column_totals={"Hours": "sum", "Total Price ($)": "sum"},
    )

    format_shore_crane_report(
        xlsx_path="shore_crane_report.xlsx",
        sheet_name="Shore Crane",
        vessel_name=vessel_name,
        start_date=start_date,
        end_date=end_date,
        service_name="Shore Crane",
        header_color="1F4E78",
    )
    return


@app.cell
def _():
    return


@app.cell(column=6, hide_code=True)
def _():
    mo.md(r"""
    ## Data to process to Excel
    """)
    return


@app.cell
def _():
    start_date = date(year=2026, month=2, day=23)
    end_date = date(2026, 2, 26)
    vessel_name = "TORRE ITALIA"
    return end_date, start_date, vessel_name


if __name__ == "__main__":
    app.run()
