"""Overtime multipliers for different types of overtime."""


class Overtime:
    """Overtime multipliers for different types of overtime."""

    normal_hours: float = 1.0
    overtime_150: float = 1.5
    overtime_200: float = 2.0

    def __str__(self) -> str:
        return f"""Overtime(normal={self.normal_hours},
        overtime_150={self.overtime_150},
        overtime_200={self.overtime_200})"""

    def textual_representation(self) -> dict[str, str]:
        """Returns a dictionary with textual representation of overtime types."""
        return {
            self.normal_hours: "normal hours",
            self.overtime_150: "overtime 150%",
            self.overtime_200: "overtime 200%",
        }
