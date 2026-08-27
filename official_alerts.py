"""Official DPC and Regione Lazio bulletins shown inside the dashboard."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time
from html.parser import HTMLParser
from time import perf_counter
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import Settings, settings
from db import ensure_schema, get_engine
from forecast_providers import build_session
from source_health import record_source_disabled, record_source_result

DPC_ALERTS_URL = "https://rischi.protezionecivile.gov.it/it/meteo-idro/allertamento/"
LAZIO_BASE = "https://www.regione.lazio.it"
LAZIO_CRITICALITY_URL = (
    LAZIO_BASE + "/protezione-civile/centro-funzionale-regionale/"
    "bollettini-criticita-idrogeologica-idraulica"
)
LAZIO_ALERTING_URL = (
    LAZIO_BASE
    + "/protezione-civile/centro-funzionale-regionale/bollettini-allertamenti"
)
ALERT_COLUMNS = [
    "source",
    "alert_id",
    "issued_at",
    "starts_at",
    "ends_at",
    "severity",
    "title",
    "description",
    "area",
    "source_url",
    "fetched_at",
]


class OfficialAlertsError(RuntimeError):
    """Institutional bulletin pages are temporarily unavailable."""


class _PageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.text_parts: list[str] = []
        self.links: list[tuple[str, str]] = []
        self._href: str | None = None
        self._link_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "a":
            self._href = dict(attrs).get("href")
            self._link_text = []

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a" and self._href:
            self.links.append((self._href, " ".join(self._link_text)))
            self._href = None
            self._link_text = []

    def handle_data(self, data: str) -> None:
        cleaned = " ".join(data.split())
        if not cleaned:
            return
        self.text_parts.append(cleaned)
        if self._href:
            self._link_text.append(cleaned)

    @property
    def text(self) -> str:
        return " ".join(self.text_parts)


@dataclass(frozen=True)
class BulletinLink:
    published: date
    url: str
    label: str


def _get_page(session: requests.Session, url: str) -> _PageParser:
    try:
        response = session.get(url, timeout=(8, 35))
    except requests.RequestException as exc:
        raise OfficialAlertsError(
            "bollettini ufficiali: fonte non raggiungibile"
        ) from exc
    if not response.ok:
        raise OfficialAlertsError(
            f"bollettini ufficiali: risposta HTTP {response.status_code}"
        )
    parser = _PageParser()
    # Both institutional sites publish UTF-8 pages, while one occasionally
    # advertises a legacy charset and would otherwise turn "Criticità" into
    # mojibake, breaking the structured summary parser.
    parser.feed(response.content.decode("utf-8", errors="replace"))
    return parser


def _date_from_text(value: str) -> date | None:
    match = re.search(r"(?<!\d)(\d{1,2})[-_/\.](\d{1,2})[-_/\.](20\d{2})(?!\d)", value)
    if not match:
        return None
    try:
        return date(int(match.group(3)), int(match.group(2)), int(match.group(1)))
    except ValueError:
        return None


def latest_pdf_link(parser: _PageParser, base_url: str) -> BulletinLink | None:
    candidates: list[BulletinLink] = []
    for href, label in parser.links:
        if ".pdf" not in href.lower():
            continue
        published = _date_from_text(href) or _date_from_text(label)
        if published is None:
            continue
        candidates.append(
            BulletinLink(published, urljoin(base_url, href), label.strip())
        )
    return (
        max(candidates, key=lambda item: (item.published, item.url))
        if candidates
        else None
    )


def _local_issued(published: date, hour: int = 14) -> pd.Timestamp:
    local = datetime.combine(published, time(hour=hour), ZoneInfo("Europe/Rome"))
    return pd.Timestamp(local).tz_convert("UTC")


def _severity(text_value: str) -> str:
    normalised = " ".join(text_value.upper().split())
    if "ALLERTA ROSSA" in normalised or "CRITICITÀ ROSSA" in normalised:
        return "red"
    if "ALLERTA ARANCIONE" in normalised or "CRITICITÀ ARANCIONE" in normalised:
        return "orange"
    if "ALLERTA GIALLA" in normalised or "CRITICITÀ GIALLA" in normalised:
        return "yellow"
    if (
        "NESSUNA ALLERTA" in normalised
        or "ASSENZA DI FENOMENI SIGNIFICATIVI" in normalised
    ):
        return "green"
    return "information"


def parse_dpc_summary(
    parser: _PageParser, fetched_at: pd.Timestamp
) -> dict[str, Any] | None:
    pattern = re.compile(
        r"Bollettino di Criticità del\s+(\d{1,2})\s+([A-Za-zàèéìòù]+)\s+(20\d{2})\s+ore\s+(\d{1,2}:\d{2})(.*?)(?=Bollettino di Vigilanza|Anteprima mappa|Link utili)",
        flags=re.IGNORECASE,
    )
    match = pattern.search(parser.text)
    if not match:
        return None
    months = {
        "gennaio": 1,
        "febbraio": 2,
        "marzo": 3,
        "aprile": 4,
        "maggio": 5,
        "giugno": 6,
        "luglio": 7,
        "agosto": 8,
        "settembre": 9,
        "ottobre": 10,
        "novembre": 11,
        "dicembre": 12,
    }
    month = months.get(match.group(2).casefold())
    if month is None:
        return None
    published = date(int(match.group(3)), month, int(match.group(1)))
    hours, minutes = (int(item) for item in match.group(4).split(":"))
    local = datetime.combine(published, time(hours, minutes), ZoneInfo("Europe/Rome"))
    description = " ".join(match.group(5).split()).strip(" :-")
    return {
        "source": "dpc_nazionale",
        "alert_id": f"criticita-{published:%Y%m%d}-{hours:02d}{minutes:02d}",
        "issued_at": pd.Timestamp(local).tz_convert("UTC"),
        "starts_at": pd.Timestamp(local).tz_convert("UTC"),
        "ends_at": pd.Timestamp(local).tz_convert("UTC") + pd.Timedelta(hours=34),
        "severity": _severity(description),
        "title": f"Bollettino nazionale di criticità · {published:%d/%m/%Y}",
        "description": description or "Consulta il bollettino ufficiale DPC.",
        "area": "Italia",
        "source_url": DPC_ALERTS_URL,
        "fetched_at": fetched_at,
    }


def fetch_official_alerts(
    cfg: Settings = settings, session: requests.Session | None = None
) -> pd.DataFrame:
    """Fetch official summaries and document links without scraping PDF content."""
    del cfg  # reserved for future regional selection
    own_session = session is None
    session = session or build_session(retries=2)
    fetched_at = pd.Timestamp.now(tz="UTC")
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    try:
        try:
            dpc = parse_dpc_summary(_get_page(session, DPC_ALERTS_URL), fetched_at)
            if dpc:
                rows.append(dpc)
        except OfficialAlertsError as exc:
            errors.append(str(exc))

        try:
            criticality = latest_pdf_link(
                _get_page(session, LAZIO_CRITICALITY_URL), LAZIO_BASE
            )
            if criticality:
                issued = _local_issued(criticality.published)
                rows.append(
                    {
                        "source": "regione_lazio",
                        "alert_id": f"criticita-{criticality.published:%Y%m%d}",
                        "issued_at": issued,
                        "starts_at": issued,
                        "ends_at": issued + pd.Timedelta(hours=34),
                        "severity": "information",
                        "title": f"Bollettino di criticità Lazio · {criticality.published:%d/%m/%Y}",
                        "description": "Documento quotidiano del Centro Funzionale Regionale.",
                        "area": "Lazio · tutte le zone di allerta",
                        "source_url": criticality.url,
                        "fetched_at": fetched_at,
                    }
                )
        except OfficialAlertsError as exc:
            errors.append(str(exc))

        try:
            alerting = latest_pdf_link(
                _get_page(session, LAZIO_ALERTING_URL), LAZIO_BASE
            )
            if alerting:
                issued = _local_issued(alerting.published)
                rows.append(
                    {
                        "source": "regione_lazio",
                        "alert_id": f"allertamento-{alerting.published:%Y%m%d}",
                        "issued_at": issued,
                        "starts_at": issued,
                        "ends_at": None,
                        "severity": "official_notice",
                        "title": f"Allertamento regionale Lazio · {alerting.published:%d/%m/%Y}",
                        "description": "Ultimo documento di allertamento pubblicato dalla Regione Lazio; aprilo per zone, rischi e validità.",
                        "area": "Lazio",
                        "source_url": alerting.url,
                        "fetched_at": fetched_at,
                    }
                )
        except OfficialAlertsError as exc:
            errors.append(str(exc))
    finally:
        if own_session:
            session.close()
    if not rows:
        raise OfficialAlertsError(
            errors[0] if errors else "bollettini ufficiali non disponibili"
        )
    return pd.DataFrame(rows, columns=ALERT_COLUMNS)


def _iso(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return None
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def archive_official_alerts(frame: pd.DataFrame, engine: Engine | None = None) -> int:
    if frame.empty:
        return 0
    ensure_schema()
    engine = engine or get_engine()
    records: list[dict[str, Any]] = []
    for row in frame.reindex(columns=ALERT_COLUMNS).to_dict("records"):
        payload = dict(row)
        for column in ("issued_at", "starts_at", "ends_at", "fetched_at"):
            payload[column] = _iso(payload.get(column))
        records.append(payload)
    placeholders = ",".join(f":{column}" for column in ALERT_COLUMNS)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO official_alerts ("
                + ",".join(ALERT_COLUMNS)
                + ") VALUES ("
                + placeholders
                + ") ON CONFLICT (source,alert_id,issued_at) DO UPDATE SET "
                "starts_at=excluded.starts_at,ends_at=excluded.ends_at,"
                "severity=excluded.severity,title=excluded.title,"
                "description=excluded.description,area=excluded.area,"
                "source_url=excluded.source_url,fetched_at=excluded.fetched_at"
            ),
            records,
        )
    return len(records)


def refresh_official_alerts(
    cfg: Settings = settings, engine: Engine | None = None
) -> tuple[pd.DataFrame, str | None]:
    if not cfg.feature_official_alerts_enabled:
        record_source_disabled("official_alerts", engine=engine)
        return pd.DataFrame(columns=ALERT_COLUMNS), None
    started = perf_counter()
    try:
        frame = fetch_official_alerts(cfg)
        archive_official_alerts(frame, engine)
    except (OfficialAlertsError, ValueError) as exc:
        record_source_result(
            "official_alerts",
            success=False,
            latency_ms=(perf_counter() - started) * 1000,
            error=exc,
            engine=engine,
        )
        return pd.DataFrame(columns=ALERT_COLUMNS), str(exc)
    record_source_result(
        "official_alerts",
        success=True,
        rows_received=len(frame),
        last_observation_at=frame["issued_at"].max(),
        latency_ms=(perf_counter() - started) * 1000,
        engine=engine,
    )
    return frame, None
