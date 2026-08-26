from __future__ import annotations

import io

from PIL import Image

from share_card import ShareCardSummary, render_share_card


def test_share_card_is_a_valid_social_png():
    content = render_share_card(
        ShareCardSummary(
            location="Stazione meteo Roma",
            date_label="Mercoledì 26 agosto",
            condition="Sereno",
            temperature="23° / 31°",
            rain="0 mm · 10%",
            wind="18 km/h",
            confidence="82%",
            briefing="Condizioni regolari nelle prossime 24 ore.",
            air="AQI 22",
        )
    )
    image = Image.open(io.BytesIO(content))
    assert image.format == "PNG"
    assert image.size == (1200, 630)
