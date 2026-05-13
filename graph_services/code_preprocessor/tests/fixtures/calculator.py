"""Simple calculator module for E2E pipeline fixture."""


class Calculator:
    """Basic arithmetic calculator."""

    def add(self, a: float, b: float) -> float:
        """Return the sum of a and b."""
        return a + b

    def subtract(self, a: float, b: float) -> float:
        """Return a minus b."""
        return a - b

    def multiply(self, a: float, b: float) -> float:
        """Return a multiplied by b."""
        return a * b

    def divide(self, a: float, b: float) -> float:
        """Return a divided by b. Raises ZeroDivisionError if b is 0."""
        if b == 0:
            raise ZeroDivisionError("Cannot divide by zero")
        return a / b


def format_result(value: float, precision: int = 2) -> str:
    """Format a numeric result to the given decimal precision."""
    return f"{value:.{precision}f}"
