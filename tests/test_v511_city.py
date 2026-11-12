import pandas as pd
from starlette.applications import Starlette
from starlette.testclient import TestClient

import v5_city
from city_weather import CityForecast, CityLocation
from v5_api import public_routes
from v53_profiles import validate_profile

TOKYO = CityLocation(
    "Tokyo", "Giappone", "", 35.69, 139.69, "Asia/Tokyo", geonames_id=1850147
)


def forecast(location, **_):
    at = pd.Timestamp("2026-11-12T12:00Z").tz_convert(location.timezone)
    current = {"time": at, "temp_c": 14, "description": "Nuvoloso"}
    return CityForecast(
        location.timezone,
        current,
        pd.DataFrame([current]),
        pd.DataFrame([{"time": at.normalize(), "temp_min_c": 9, "temp_max_c": 14}]),
        at,
    )


def test_public_id_resolves_after_restart_and_does_not_require_a_station(monkeypatch):
    def geocode(geo_id, **_):
        assert geo_id == TOKYO.geonames_id
        return TOKYO

    monkeypatch.setattr(v5_city, "city_by_geonames_id", geocode)
    monkeypatch.setattr(v5_city, "fetch_city_forecast", forecast)
    monkeypatch.setattr(v5_city, "_cached", lambda _, fn, ttl: fn())
    monkeypatch.setattr(v5_city, "_LOCATIONS", {})
    monkeypatch.setattr(v5_city, "_remember", lambda _: None)
    with TestClient(Starlette(routes=public_routes())) as client:
        response = client.get("/api/v5/tools/city?geo_id=1850147")
        assert response.status_code == 200
        data = response.json()
        assert data["label"] == "Tokyo, Giappone" and data["timezone"] == "Asia/Tokyo"
        assert data["current"]["time"].endswith("+09:00")
        assert not {"latitude", "longitude", "station"} & data.keys()
        assert client.get("/api/v5/tools/city?geo_id=0").status_code == 400


def test_nearby_uses_coarse_position_and_is_not_a_saved_city(monkeypatch):
    coordinates = []

    def fetch(location, **kwargs):
        coordinates.append((location.latitude, location.longitude))
        return forecast(location, **kwargs)

    monkeypatch.setattr(v5_city, "nearby_public_city", lambda *_: None)
    monkeypatch.setattr(v5_city, "fetch_city_forecast", fetch)
    monkeypatch.setattr(v5_city, "_cached", lambda _, fn, ttl: fn())
    with TestClient(Starlette(routes=public_routes())) as client:
        path = "/api/v5/tools/nearby-city"
        body = {
            "latitude": 34.69 + 17 / 10000,
            "longitude": 135.51 - 28 / 10000,
            "timezone": "Asia/Tokyo",
        }
        assert client.post(path, json=body).status_code == 403
        response = client.post(path, json=body, headers={"Origin": "http://testserver"})
        assert response.status_code == 200
        assert coordinates == [(34.69, 135.51)]
        assert (
            response.json()["approximate"] is True and response.json()["geo_id"] is None
        )
        assert all(str(body[k]) not in response.text for k in ("latitude", "longitude"))
        assert (
            client.post(
                path,
                json={**body, "latitude": 190},
                headers={"Origin": "http://testserver"},
            ).status_code
            == 400
        )


def test_nearby_uses_local_index_and_sends_only_public_city_centre(monkeypatch):
    from v5_city_index import nearby_public_city, public_city

    assert nearby_public_city(48.86, 2.35).geonames_id == 2988507
    assert nearby_public_city(-27, -129) is None
    calls = []

    def fetch(location, **kwargs):
        calls.append(location)
        return forecast(location, **kwargs)

    monkeypatch.setattr(v5_city, "fetch_city_forecast", fetch)
    monkeypatch.setattr(v5_city, "_cached", lambda _, fn, ttl: fn())
    monkeypatch.setattr(v5_city, "_LOCATIONS", {})
    monkeypatch.setattr(v5_city, "_remember", lambda _: None)
    payload = v5_city.nearby_city(
        {"latitude": 35.7, "longitude": 139.71, "timezone": "Asia/Tokyo"}
    )
    assert payload["geo_id"] == TOKYO.geonames_id and not payload["approximate"]
    assert calls == [public_city(TOKYO.geonames_id)]


def test_profile_syncs_city_identity_and_legacy_favorites_without_coordinates():
    city = {"geo_id": TOKYO.geonames_id, "name": TOKYO.name, "label": TOKYO.label}
    old = {"label": "Londra, Regno Unito", "query": "Londra"}
    extra = {**city, "latitude": 35.69, "longitude": 139.69}
    result = validate_profile(
        {"preferences": {"home_city": extra}, "cities": [extra, old]}
    )
    assert result["preferences"]["home_city"] == city
    assert result["cities"] == [city, old]


def test_old_diagnostics_links_open_pro_with_station_and_theme():
    with TestClient(Starlette(routes=public_routes())) as client:
        for page in ("quality", "models"):
            response = client.get(
                f"/?page={page}&station=comacchio-secondary&theme=dark",
                follow_redirects=False,
            )
            assert response.status_code == 307
            assert (
                response.headers["location"]
                == "/pro/?tab=precision&station=comacchio-secondary&theme=dark"
            )
