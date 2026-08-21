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


def weather_cell_style(value: Any, metric: str) -> str:
    """Return an accessible semantic cell colour for a weather metric."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if not math.isfinite(number):
        return ""

    green = "background-color:#bbf7d0;color:#14532d;font-weight:600"
    yellow = "background-color:#fef3c7;color:#78350f;font-weight:600"
    orange = "background-color:#fed7aa;color:#7c2d12;font-weight:700"
    red = "background-color:#fecaca;color:#7f1d1d;font-weight:700"
    blue = "background-color:#bfdbfe;color:#1e3a8a;font-weight:600"
    strong_blue = "background-color:#60a5fa;color:#0b1f3a;font-weight:700"
    grey = "background-color:#cbd5e1;color:#1e293b;font-weight:600"

    if metric == "temperature":
        if number <= 0 or number >= 35:
            return red
        if number <= 5 or number >= 32:
            return orange
        if 15 <= number <= 27:
            return green
        return yellow
    if metric == "humidity":
        if number < 25 or number > 85:
            return red
        if number < 35 or number > 78:
            return orange
        if 40 <= number <= 70:
            return green
        return yellow
    if metric == "pressure":
        if number < 990 or number > 1040:
            return red
        if number < 1000 or number > 1030:
            return orange
        if 1008 <= number <= 1025:
            return green
        return yellow
    if metric in {"wind", "gust"}:
        warning = 35 if metric == "wind" else 45
        danger = 50 if metric == "wind" else 65
        if number >= danger:
            return red
        if number >= warning:
            return orange
        if number < 20:
            return green
        return yellow
    if metric == "rain":
        if number >= 15:
            return red
        if number >= 5:
            return orange
        return blue if number > 0 else ""
    if metric == "rain_probability":
        if number >= 75:
            return strong_blue
        if number >= 40:
            return blue
        return green
    if metric == "clouds":
        if number >= 85:
            return grey
        if number >= 60:
            return yellow
        return green
    if metric == "confidence":
        if number < 45:
            return red
        if number < 65:
            return yellow
        return green
    if metric == "sqm":
        if number >= 21.3:
            return green
        if number >= 20.2:
            return yellow
        if number >= 19.0:
            return orange
        return red
    if metric == "bortle":
        if number <= 3:
            return green
        if number <= 5:
            return yellow
        if number <= 7:
            return orange
        return red
    return ""
