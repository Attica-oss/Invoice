
""""Polars Enum helper class for type-safe string enums with Polars integration."""

from __future__ import annotations

from enum import StrEnum
import polars as pl


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
            raise ValueError(f"Invalid {cls.__name__}: {value!r}. Allowed: {cls.list_all()}") from e

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