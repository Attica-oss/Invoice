"""Cast columns to numbers"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

import polars as pl


class Numbers(Enum):
    FLOAT64 = "float64"
    INT64 = "int64"
    DECIMAL = "decimal"
    INT8 = "int8"
    INT16 = "int16"
    INT32 = "int32"
    # CURRENCY = "currency"


@dataclass(frozen=True)
class CastToNumbers:
    column_name: str

    def _column(self) -> pl.Expr:
        return pl.col(self.column_name)

    def _clean_column(self) -> pl.Expr:
        return self._column().cast(pl.String).str.strip_chars().replace("", None)

    def to_currency(self, precision: int = 18, scale: int = 2) -> pl.Expr:
        value = self.to_numbers(Numbers.DECIMAL, precision=precision, scale=scale)

        return pl.concat_str(
            pl.lit("$"),
            value.cast(pl.String),
        ).alias(self.column_name)

    def to_numbers(
        self,
        to: Numbers,
        precision: int | None = None,
        scale: int | None = None,
        is_strict: bool = True,
    ) -> pl.Expr:

        column = self._clean_column()
        match to:
            case Numbers.FLOAT64:
                return column.fill_null("0.0").cast(pl.Float64, strict=is_strict)
            case Numbers.INT64:
                return column.fill_null("0").cast(pl.Int64, strict=is_strict)
            case Numbers.DECIMAL:
                precision = 18 if precision is None else precision
                scale = 2 if scale is None else scale
                return column.fill_null(Decimal("0.0")).cast(
                    pl.Decimal(precision=precision, scale=scale), strict=is_strict
                )
            case Numbers.INT8:
                return column.fill_null("0").cast(pl.Int8, strict=is_strict)
            case Numbers.INT16:
                return column.fill_null("0").cast(pl.Int16, strict=is_strict)
            case Numbers.INT32:
                return column.fill_null("0").cast(pl.Int32, strict=is_strict)
