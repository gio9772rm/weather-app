"""Pure display helpers shared by the weather dashboard and its tests."""

from __future__ import annotations

import math
from typing import Any


def compass_direction(value: Any) -> str:
    """Return an Italian eight-point compass label and normalized degrees."""
    try:
        degrees = float(value)
    except (TypeError, ValueError):
        return "—"
    if not math.isfinite(degrees):
        return "—"
    degrees %= 360
    labels = ("N", "NE", "E", "SE", "S", "SO", "O", "NO")
    label = labels[int((degrees + 22.5) // 45) % len(labels)]
    return f"{label} · {degrees:.0f}°"
