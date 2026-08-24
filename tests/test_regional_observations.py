from __future__ import annotations

from dataclasses import replace
from typing import Any

import pandas as pd

from config import Settings
from regional_observations import (
    SOURCE_ARSIAL,
    SOURCE_CFR,
    fetch_arsial_observations,
    fetch_cfr_observations,
    parse_arsial_tables,
    parse_cfr_frames,
)


def _settings() -> Settings:
    return replace(
        Settings.from_env(),
        latitude=41.9067235,
        longitude=12.3899144,
        arsial_observations_enabled=True,
        arsial_station_name="ROMA Lanciani-SEDE ARSIAL",
        cfr_observations_enabled=False,
    )


class FakeResponse:
    def __init__(
        self,
        body: str,
        *,
        url: str,
        status: int = 200,
        content_type: str = "text/html",
        payload: Any = None,
    ) -> None:
        self.text = body
        self.content = body.encode("utf-8")
        self.url = url
        self.status_code = status
        self.ok = 200 <= status < 300
        self.headers = {"content-type": content_type}
        self._payload = payload

    def json(self) -> Any:
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


class FakeArsialSession:
    def __init__(self, cfg: Settings) -> None:
        self.cfg = cfg
        self.closed = False
        self.calls: list[str] = []

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        del kwargs
        self.calls.append(url)
        if url == self.cfg.arsial_dashboard_url:
            return FakeResponse(
                '<html><script>window.data={"slice_id":77}</script></html>',
                url=url,
            )
        if url.endswith("/api/v1/dashboard/7/charts"):
            return FakeResponse(
                "{}",
                url=url,
                content_type="application/json",
                payload={"result": [{"id": 77, "slice_name": "Dati orari"}]},
            )
        if url.endswith("/api/v1/dashboard/7"):
            return FakeResponse("{}", url=url, status=404)
        if url.endswith("/api/v1/chart/77"):
            return FakeResponse(
                "{}",
                url=url,
                content_type="application/json",
                payload={"result": {"params": '{"slice_id":77}'}},
            )
        if url.endswith("/superset/explore_json/"):
            return FakeResponse(
                "Stazione;Data;Ora;Temperatura med (°C);Umidità aria med (%)\n"
                "ROMA Lanciani-SEDE ARSIAL;2026-08-24;20;26,4;61\n",
                url=url,
                content_type="text/csv",
            )
        if url == self.cfg.arsial_station_registry_url:
            return FakeResponse(
                "Cod staz;Nome stazione;ALTITUDINE;lat;lon\n"
                "501;ROMA Lanciani-SEDE ARSIAL;52;41,92;12,52\n",
                url=url,
                content_type="text/csv",
            )
        raise AssertionError(f"unexpected URL {url}")

    def close(self) -> None:
        self.closed = True


class FailOnRequestSession:
    def get(self, url: str, **kwargs: Any) -> None:
        del url, kwargs
        raise AssertionError("the dormant CFR connector performed a request")


def test_arsial_wide_tables_are_normalised_and_merged():
    table = pd.DataFrame(
        [
            {
                "Stazione": "ROMA Lanciani-SEDE ARSIAL",
                "Data": "2026-08-24",
                "Ora": "20",
                "Precipitazione (mm)": "0,4",
                "Pressione atm ridotta med (hPa)": "1014,8",
                "Temperatura med (°C)": "26,4",
                "Temperatura max (°C)": "30,1",
                "Umidità aria med (%)": "61",
                "Velocità vento med (m/s)": "2,5",
                "Raffica max (m/s)": "4,0",
                "Direzione vento med (°)": "225",
            }
        ]
    )
    registry = pd.DataFrame(
        [
            {
                "Cod staz": 501,
                "Nome stazione": "ROMA Lanciani-SEDE ARSIAL",
                "ALTITUDINE": 52,
                "lat": 41.92,
                "lon": 12.52,
            }
        ]
    )

    frame = parse_arsial_tables(
        [table],
        _settings(),
        registry,
        pd.Timestamp("2026-08-24T20:30:00Z"),
    )

    assert len(frame) == 1
    row = frame.iloc[0]
    assert row["source"] == SOURCE_ARSIAL
    assert row["station_id"] == "ARSIAL-501"
    assert row["time"] == pd.Timestamp("2026-08-24T20:00:00Z")
    assert row["temp_c"] == 26.4
    assert row["pressure_hpa"] == 1014.8
    assert row["wind_kmh"] == 9.0
    assert row["wind_gust_kmh"] == 14.4
    assert row["wind_dir"] == 225
    assert row["rain_mm"] == 0.4
    assert row["precip_observed"] == 1
    assert row["dewpoint_c"] is not None
    assert 5 < row["distance_km"] < 20


def test_arsial_long_rows_coalesce_at_the_same_timestamp():
    table = pd.DataFrame(
        [
            {
                "Stazione": "ROMA Lanciani-SEDE ARSIAL",
                "Data rilevazione": "2026-08-24T19:00:00Z",
                "Grandezza": "Temperatura aria",
                "Valore": "25,2",
                "Unità di misura": "°C",
            },
            {
                "Stazione": "ROMA Lanciani-SEDE ARSIAL",
                "Data rilevazione": "2026-08-24T19:00:00Z",
                "Grandezza": "Umidità aria",
                "Valore": "64",
                "Unità di misura": "%",
            },
        ]
    )

    frame = parse_arsial_tables([table], _settings())

    assert len(frame) == 1
    assert frame.iloc[0]["temp_c"] == 25.2
    assert frame.iloc[0]["humidity"] == 64
    assert frame.iloc[0]["dewpoint_c"] is not None


def test_arsial_hourly_dashboard_stays_in_utc_across_seasons():
    table = pd.DataFrame(
        [
            {
                "Stazione": "ROMA Lanciani-SEDE ARSIAL",
                "Data": "2026-01-15",
                "Ora": "12",
                "Temperatura med (°C)": 10,
            },
            {
                "Stazione": "ROMA Lanciani-SEDE ARSIAL",
                "Data": "2026-07-15",
                "Ora": "12",
                "Temperatura med (°C)": 30,
            },
        ]
    )

    frame = parse_arsial_tables([table], _settings())

    assert frame.iloc[0]["time"] == pd.Timestamp("2026-01-15T12:00:00Z")
    assert frame.iloc[1]["time"] == pd.Timestamp("2026-07-15T12:00:00Z")


def test_arsial_fetch_discovers_public_superset_csv():
    cfg = _settings()
    session = FakeArsialSession(cfg)

    frame = fetch_arsial_observations(cfg, session)

    assert len(frame) == 1
    assert frame.iloc[0]["station_id"] == "ARSIAL-501"
    assert any(url.endswith("/superset/explore_json/") for url in session.calls)
    assert session.closed is False


def test_cfr_connector_is_completely_dormant_by_default():
    frame = fetch_cfr_observations(_settings(), FailOnRequestSession())

    assert frame.empty


def test_cfr_future_json_or_csv_contract_is_already_supported():
    cfg = replace(
        _settings(),
        cfr_observations_enabled=True,
        cfr_station_ids=("37081",),
    )
    table = pd.DataFrame(
        [
            {
                "station_id": "37081",
                "Stazione": "Roma Ovest",
                "timestamp": "2026-08-24T21:15:00+02:00",
                "latitudine": 41.91,
                "longitudine": 12.41,
                "Temperatura": 25.8,
                "Humidity": 66,
                "Pressure hPa": 1013.2,
                "Wind speed km/h": 7.4,
                "Precipitazione mm": 0,
            }
        ]
    )

    frame = parse_cfr_frames([table], cfg)

    assert len(frame) == 1
    row = frame.iloc[0]
    assert row["source"] == SOURCE_CFR
    assert row["station_id"] == "37081"
    assert row["time"] == pd.Timestamp("2026-08-24T19:15:00Z")
    assert row["temp_c"] == 25.8
    assert row["humidity"] == 66
    assert row["pressure_hpa"] == 1013.2
    assert row["wind_kmh"] == 7.4
    assert row["rain_mm"] == 0
