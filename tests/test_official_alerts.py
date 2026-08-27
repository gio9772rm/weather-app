from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from official_alerts import (
    _PageParser,
    archive_official_alerts,
    latest_pdf_link,
    parse_dpc_summary,
)


def test_dpc_summary_and_latest_regional_pdf_are_normalised(sqlite_engine):
    dpc = _PageParser()
    dpc.feed(
        "<h3>Bollettino di Criticità del 27 agosto 2026 ore 14:22</h3>"
        "<p>Per la giornata di oggi: NESSUNA ALLERTA.</p>"
        "<h3>Bollettino di Vigilanza Meteorologica Nazionale</h3>"
    )
    row = parse_dpc_summary(dpc, pd.Timestamp("2026-08-27T13:00:00Z"))
    assert row is not None
    assert row["severity"] == "green"
    assert row["source"] == "dpc_nazionale"

    lazio = _PageParser()
    lazio.feed(
        '<a href="/files/bollettino_26_08_2026.pdf">Bollettino 26/08/2026</a>'
        '<a href="/files/bollettino_27_08_2026.pdf">Bollettino 27/08/2026</a>'
    )
    latest = latest_pdf_link(lazio, "https://www.regione.lazio.it")
    assert latest is not None
    assert latest.url.endswith("bollettino_27_08_2026.pdf")

    archive_official_alerts(pd.DataFrame([row]), sqlite_engine)
    with sqlite_engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT COUNT(*) FROM official_alerts")
            ).scalar_one()
            == 1
        )
