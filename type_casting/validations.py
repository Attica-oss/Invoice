"""Validations"""

from dataclasses import dataclass


from .polars_enum import PolarsEnum

# Paller types


class PalletType(PolarsEnum):
    """Pallet Type"""

    PALLET: str = "Pallet"
    LINER_PALLET: str = "Liner & Pallet"
    LINER: str = "Liner"


class ServiceType(PolarsEnum):
    """Cold Store Services"""

    BIN_DISPATCH_SERVICE: list[str] = ["Bin Dispatch to IOT", "Bin Dispatch from IOT"]
    UNLOADING_SERVICE: list[str] = ["Sorting from Unloading", "Unsorted from Unloading"]
    CARGO_DISPATCH_SERVICE: list[str] = [
        "Dispatch to Cargo Vessel",
        "Dispatch from Cargo Vessel",
    ]
    UNSTUFFING_TO_COLD_STORE: list[str] = ["Unstuffing to CCCS"]
    COLD_STORE_STUFFING: list[str] = ["Container loading from CCCS"]
    VESSEL_DISPATCH_SERVICE: list[str] = ["From CCCS to Vessel"]


class ColdStoreFishOriginType(PolarsEnum):
    """Origin of the fish in the cold store"""

    IOT_FACTORY: str = "IOT FACTORY"
    VESSEL: str = "VESSEL"
    COLD_STORE: str = "CCCS COLDSTORE"
    CONTAINER: str = "CONTAINER"
    CARGO: str = "CARGO"


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
    def cold_store_perspective(cls) -> list[str]:
        """Returns the movement type from the perspective of the cold store"""
        return [cls.IN, cls.OUT]


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
