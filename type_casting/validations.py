"""Validations"""

import polars as pl

from .polars_enum import PolarsEnum

# Paller types


class PalletType(PolarsEnum):
    """Pallet Type"""

    PALLET: str = "Pallet"
    LINER_PALLET: str = "Liner & Pallet"
    LINER: str = "Liner"


class ServiceType(PolarsEnum):
    """Cold Store Services"""

    BIN_DISPATCH_TO_IOT: str = "Bin Dispatch to IOT"
    BIN_DISPATCH_FROM_IOT: str = "Bin Dispatch from IOT"
    UNLOADING_SORTED: str = "Sorting from Unloading"
    UNLOADING_UNSORTED: str = "Unsorted from Unloading"
    DISPATCH_TO_CARGO: str = "Dispatch to Cargo Vessel"
    DISPATCH_FROM_CARGO: str = "Dispatch from Cargo Vessel"
    UNSTUFFING_TO_COLD_STORE: str = "Unstuffing to CCCS"
    COLD_STORE_STUFFING: str = "Container loading from CCCS"
    VESSEL_DISPATCH_SERVICE: str = "From CCCS to Vessel"

    def bin_dispatch(self) -> list[str]:
        return [self.BIN_DISPATCH_TO_IOT, self.BIN_DISPATCH_FROM_IOT]


class ColdStoreFishOriginType(PolarsEnum):
    """Origin of the fish in the cold store"""

    IOT_FACTORY: str = "IOT FACTORY"
    VESSEL: str = "VESSEL"
    COLD_STORE: str = "CCCS COLDSTORE"
    CONTAINER: str = "CONTAINER"
    CARGO: str = "CARGO"


# Transportation Validation Type
# --------------------------------
class ShoreCraneLocationType(PolarsEnum):
    """Location of the shore crane"""

    REAR: str = "IOT FACTORY"
    FRONT: str = "VESSEL"
    COLD_STORE: str = "CCCS"
    NORTH: str = "NORTH"
    SOUTH: str = "SOUTH"
    VIA_SKIFF: str = "VIA SKIFF"
    OTHER: str = "N/A"


class MovementType(PolarsEnum):
    """Classification of Movement"""

    DELIVERY: str = "Delivery"
    COLLECTION: str = "Collection"
    SHIFTING: str = "Shifting"  # Not used
    EXTERNAL: str = "External"
    IN: str = "IN"
    OUT: str = "OUT"
    INTERNAL: str = "INTERNAL"

    @classmethod
    def cold_store_perspective(cls) -> pl.Enum:
        """Returns the movement type from the perspective of the cold store"""
        return pl.Enum([cls.IN, cls.OUT], dtype=pl.Utf8)


class ContainerStatusType(PolarsEnum):
    """Check if Full or Empty"""

    FULL: str = "Full"
    EMPTY: str = "Empty"


class FishStorageType(PolarsEnum):
    """Brine or Dry"""

    BRINE: str = "Brine"
    DRY: str = "Dry"


class PluggedStatusType(PolarsEnum):
    """Check if the unit is Full, Partial of has been Completed"""

    PARTIAL: str = "Partial"
    COMPLETED: str = "Completed"
    FULL: str = "Full"


class SetPointType(PolarsEnum):
    """Classifies the main 3 set point"""

    STANDARD: int = -25
    MAGNUM: int = -35
    S_FREEZER: int = -60
