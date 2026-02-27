"""Customer Validations"""

from __future__ import annotations
from dataclasses import dataclass

import polars as pl
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import MASTER_ID, client_sheet


@dataclass(frozen=True)
class CustomerCatalog:
    """ "Customer Catalog loaded from the master validation sheet, with pre-computed lookups for efficiency."""

    df: pl.DataFrame
    by_type: dict[str, list[str]]
    customer_enum: pl.Enum

    @staticmethod
    def _clean_str(expr: pl.Expr) -> pl.Expr:
        """Normalize once so you don't have " IOT", "iot", "Iot", etc"""
        return expr.cast(pl.Utf8).str.strip_chars().str.to_uppercase()

    @classmethod
    def from_gsheet(cls) -> "CustomerCatalog":
        """Collect once. (If load_gsheet_data already returns a LazyFrame wrapped in Result)"""
        lf = load_gsheet_data(MASTER_ID, client_sheet).unwrap()

        df = lf.select(
            cls._clean_str(pl.col("Type")).alias("Type"),
            cls._clean_str(pl.col("Vessel/Client")).alias("Vessel/Client"),
            cls._clean_str(pl.col("Customer")).alias("Customer"),
        ).collect()

        # Build Enum categories once (drop null/empty just in case)
        categories = (
            df.get_column("Vessel/Client")
            .drop_nulls()
            .filter(pl.col("Vessel/Client") != "")
            .unique()
            .sort()
            .to_list()
        )
        customer_enum = pl.Enum(
            categories
        )  # Polars Enum dtype :contentReference[oaicite:2]{index=2}

        # Build a {Type: [Vessel/Client...]} mapping in one go
        by_type_df = df.group_by("Type").agg(pl.col("Vessel/Client").unique().sort())

        by_type: dict[str, list[str]] = {
            row["Type"]: row["Vessel/Client"] for row in by_type_df.to_dicts()
        }

        return cls(df=df, by_type=by_type, customer_enum=customer_enum)

    def customers(self, customer_type: str) -> list[str]:
        key = customer_type.strip().upper()
        return self.by_type.get(key, [])

    def ship_owner(self) -> list[str]:
        # Your original logic filtered Type == THONIER then selected Customer
        # If that's intentional, keep it; otherwise maybe Type == "SHIP OWNER"?
        return (
            self.df.filter(pl.col("Type") == "THONIER")
            .select("Customer")
            .unique()
            .get_column("Customer")
            .drop_nulls()
            .to_list()
        )


# Create ONE catalog instance (module-level)
CATALOG = CustomerCatalog.from_gsheet()


# Public helpers (keep your old API if other code imports these)
def enum_customer() -> pl.Enum:
    return CATALOG.customer_enum


def customers(customer_type: str) -> list[str]:
    return CATALOG.customers(customer_type)


def ship_owner() -> list[str]:
    return CATALOG.ship_owner()


purseiner = customers("THONIER")
longliner = customers("LONGLINER")
cargo = customers("CARGO")
supply_vessel = customers("SUPPLY VESSEL")
military_vessel = customers("MILITARY VESSEL")
tug_boat = customers("TUG BOAT")

vessels: list[str] = (
    purseiner + longliner + cargo + tug_boat + supply_vessel + military_vessel
)
enum_vessel: pl.Enum = pl.Enum(vessels)

factory = customers("FACTORY")
ship_owner_operator = customers("SHIP OWNER")

agent = customers("AGENT")
bycatch = customers("BYCATCH")
various = customers("VARIOUS")

hauler = customers("HAULAGE")

# IOT is both a Factory and a Shipping Line per se.
shipping_line = customers("SHIPPING LINE") + ["IOT"]

# Client which handles shorecost when transporting fish in IPHS truck
# We should move this somewhere else
client_shore_cost = ["DARDANEL", "IOT"]

# List of Customers shipping

shipper = (
    agent
    + ship_owner_operator
    + shipping_line
    + bycatch
    + [
        "IOT EXPORT",
        "IOT IMPORT",
        "CCCS",
        "IPHS",
        "JMARR",
        # "ALBACORA SA",
        # "INPESCA SA",
    ]
)
