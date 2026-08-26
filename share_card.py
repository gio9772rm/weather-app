"""Small downloadable PNG summary for the daily weather experience."""

from __future__ import annotations

import io
import textwrap
from dataclasses import dataclass
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


@dataclass(frozen=True)
class ShareCardSummary:
    location: str
    date_label: str
    condition: str
    temperature: str
    rain: str
    wind: str
    confidence: str
    briefing: str
    air: str = ""


def _font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = (
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf")
        if bold
        else Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
        Path("/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf")
        if bold
        else Path("/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf"),
    )
    for path in candidates:
        if path.exists():
            return ImageFont.truetype(str(path), size=size)
    return ImageFont.load_default()


def render_share_card(summary: ShareCardSummary) -> bytes:
    """Render an accessible, provider-neutral 1200×630 PNG card."""
    width, height = 1200, 630
    image = Image.new("RGB", (width, height), "#0d4f8b")
    pixels = image.load()
    for y in range(height):
        blend = y / max(1, height - 1)
        for x in range(width):
            horizontal = x / max(1, width - 1)
            r = int(13 + 6 * blend + 5 * horizontal)
            g = int(79 + 63 * horizontal + 30 * blend)
            b = int(139 + 18 * horizontal + 8 * blend)
            pixels[x, y] = (r, g, b)

    draw = ImageDraw.Draw(image, "RGBA")
    draw.rounded_rectangle((48, 44, 1152, 586), radius=34, fill=(4, 18, 40, 112))
    draw.rounded_rectangle((70, 375, 1130, 552), radius=24, fill=(255, 255, 255, 34))
    white = "#f8fafc"
    soft = "#dbeafe"
    draw.text((80, 72), "METEO V4 · RIEPILOGO GIORNALIERO", font=_font(22, True), fill=soft)
    draw.text((80, 112), summary.location, font=_font(52, True), fill=white)
    draw.text((82, 178), summary.date_label, font=_font(26), fill=soft)
    draw.text((80, 238), summary.temperature, font=_font(70, True), fill=white)
    draw.text((390, 252), summary.condition, font=_font(34, True), fill=white)

    metrics = (
        ("PIOGGIA", summary.rain),
        ("VENTO", summary.wind),
        ("FIDUCIA", summary.confidence),
        ("ARIA", summary.air or "dato non disponibile"),
    )
    metric_width = 255
    for index, (label, value) in enumerate(metrics):
        x = 85 + index * metric_width
        draw.text((x, 400), label, font=_font(18, True), fill=soft)
        for line_index, line in enumerate(textwrap.wrap(value, width=18)[:2]):
            draw.text(
                (x, 431 + line_index * 27),
                line,
                font=_font(22, line_index == 0),
                fill=white,
            )
    briefing = " ".join(summary.briefing.split())
    draw.text((82, 567), briefing[:110], font=_font(18), fill=soft)

    output = io.BytesIO()
    image.save(output, format="PNG", optimize=True)
    return output.getvalue()
