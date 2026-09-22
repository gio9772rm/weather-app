from __future__ import annotations

import base64
import io
import json
import zipfile

import pandas as pd
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from sqlalchemy import text
from starlette.applications import Starlette
from starlette.testclient import TestClient

from config import Settings
from v5_api import _cache, public_routes
from v5_push import (
    notification_candidates,
    push_keys,
    quiet_now,
    save_subscription,
    send_pending,
    validate_rules,
    validate_subscription,
)


@pytest.fixture
def client(sqlite_engine):
    _cache.clear()
    with TestClient(Starlette(routes=public_routes())) as client:
        yield client
    _cache.clear()


def subscription():
    public = (
        ec.generate_private_key(ec.SECP256R1())
        .public_key()
        .public_bytes(
            serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint
        )
    )
    return {
        "endpoint": "https://fcm.googleapis.com/fcm/send/test-device-only",
        "keys": {
            "p256dh": base64.urlsafe_b64encode(public).decode().rstrip("="),
            "auth": base64.urlsafe_b64encode(bytes(range(16))).decode().rstrip("="),
        },
    }


def test_api_snapshot_stays_fixed_for_ten_minutes(client, monkeypatch):
    import v5_api

    calls, clock = [], [10.0]
    identifier = Settings.from_env().station_id
    monkeypatch.setattr(v5_api.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        v5_api,
        "read_snapshot",
        lambda station: calls.append(station) or {"generation": len(calls)},
    )
    first = client.get(f"/api/v5/snapshot/{identifier}")
    clock[0] += 599
    cached = client.get(
        f"/api/v5/snapshot/{identifier}",
        headers={"If-None-Match": first.headers["etag"]},
    )
    assert cached.status_code == 304
    assert len(calls) == 1
    clock[0] += 1
    refreshed = client.get(f"/api/v5/snapshot/{identifier}")
    assert refreshed.json() == {"generation": 2}
    assert "max-age=600" in refreshed.headers["cache-control"]
    assert client.get("/api/v5/snapshot/missing").status_code == 404


def test_legacy_bookmarks_and_sw_scope(client):
    response = client.get("/?tab=system&admin=test-only", follow_redirects=False)
    assert response.headers["location"] == "/pro/?tab=system&admin=test-only"
    assert response.headers["referrer-policy"] == "no-referrer"
    assert client.get("/sw.js").headers["service-worker-allowed"] == "/"
    assert "Meteo Pro · V5" in client.get("/").text


def test_import_requires_auth_origin_and_same_preview_file(client, monkeypatch):
    import station_daily

    identifier = Settings.from_env().station_id
    url = f"/api/v5/import/{identifier}"
    monkeypatch.setenv("ADMIN_ACCESS_TOKEN", "test-admin")
    assert client.post(url, content=b"bad").status_code == 401
    headers = {"Authorization": "Bearer test-admin", "Origin": "https://wrong.example"}
    assert client.post(url, headers=headers, content=b"bad").status_code == 403
    headers["Origin"] = "http://testserver"
    assert client.post(url, headers=headers, content=b"bad").status_code == 400
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w") as archive:
        archive.writestr("fixture.xml", "workbook fixture")
    parsed = pd.DataFrame({"local_date": ["2026-09-01"], "temp_mean_c": [20]})
    writes = []
    monkeypatch.setattr(
        station_daily, "parse_ecowitt_daily_export", lambda source: parsed
    )
    monkeypatch.setattr(
        station_daily,
        "upsert_daily_summaries",
        lambda frame, station: writes.append(station) or len(frame),
    )
    preview = client.post(url, headers=headers, content=data.getvalue()).json()
    assert preview["written"] == 0 and not writes
    assert (
        client.post(
            url + "?confirm=changed", headers=headers, content=data.getvalue()
        ).status_code
        == 400
    )
    confirmed = client.post(
        url + "?confirm=" + preview["sha256"], headers=headers, content=data.getvalue()
    )
    assert confirmed.json()["written"] == 1 and writes == [identifier]
    assert confirmed.headers["cache-control"] == "no-store"


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://fcm.googleapis.com/fcm/send/a",
        "https://127.0.0.1/push",
        "https://fcm.googleapis.com.evil.example/push",
        "https://evil.example/",
        "https://fcm.googleapis.com:444/a",
        "https://user:pass@fcm.googleapis.com/a",
    ],
)
def test_push_rejects_arbitrary_destinations(endpoint):
    value = subscription()
    value["endpoint"] = endpoint
    with pytest.raises(ValueError):
        validate_subscription(value)


def test_push_api_requires_management_token_and_never_exposes_private_key(client):
    keys = push_keys(create=True)
    public = client.get("/api/v5/push/key")
    assert public.json() == {"public_key": keys["public_key"]}
    assert keys["private_key"] not in public.text
    value = {
        "station_id": Settings.from_env().station_id,
        "subscription": subscription(),
        "rules": {"rain": True},
    }
    path = "/api/v5/push/subscription"
    assert client.post(path, json=value).status_code == 403
    headers = {"Origin": "http://testserver"}
    created = client.post(path, json=value, headers=headers)
    assert created.status_code == 200
    saved = created.json()
    assert client.post(path, json=value, headers=headers).status_code == 403
    headers["Authorization"] = "Bearer " + saved["token"]
    assert (
        client.post(path, json=value, headers=headers).json()["token"] == saved["token"]
    )
    assert client.request(
        "DELETE", path, json={"id": saved["id"]}, headers=headers
    ).json() == {"deleted": True}


def test_push_quiet_hours_cross_midnight_and_dst():
    rules = validate_rules({"quiet_start": "23:00", "quiet_end": "08:00"})
    assert quiet_now(rules, pd.Timestamp("2026-10-25T00:30Z"), "Europe/Rome")
    assert quiet_now(rules, pd.Timestamp("2026-10-25T01:30Z"), "Europe/Rome")
    assert not quiet_now(rules, pd.Timestamp("2026-10-25T07:00Z"), "Europe/Rome")


def test_push_deduplicates_and_uses_only_fresh_station_snapshot(sqlite_engine):
    now = pd.Timestamp("2026-09-22T12:00Z")
    station_id = Settings.from_env().station_id
    rules = validate_rules({"rain": True, "quiet_start": "00:00", "quiet_end": "00:00"})
    value = {"station_id": station_id, "subscription": subscription(), "rules": rules}
    save_subscription(value)
    payload = {
        "generated_at": now.isoformat(),
        "station": {"id": station_id, "name": "Test", "timezone": "Europe/Rome"},
        "forecast": [
            {"valid_time": "2026-09-22T13:00Z", "rain_mm": 2, "precip_probability": 90}
        ],
    }
    with sqlite_engine.begin() as con:
        con.execute(
            text("INSERT INTO public_snapshots VALUES(:id,:at,:payload)"),
            {"id": station_id, "at": now.isoformat(), "payload": json.dumps(payload)},
        )
    delivered = []

    def sender(**kwargs):
        delivered.append(json.loads(kwargs["data"]))

    assert send_pending(now=now, sender=sender) == 1
    assert send_pending(now=now + pd.Timedelta(minutes=10), sender=sender) == 0
    assert delivered[0]["station"] == station_id
    assert not notification_candidates(payload, rules, now + pd.Timedelta(hours=2))


def test_vapid_and_payload_encrypt_with_real_library_without_network(sqlite_engine):
    from pywebpush import webpush
    from requests import Response

    class NoNetwork:
        def post(self, url, **kwargs):
            assert url.startswith("https://fcm.googleapis.com/")
            assert kwargs["data"] and kwargs["headers"]["authorization"].startswith(
                "vapid "
            )
            response = Response()
            response.status_code = 201
            response._content = b""
            return response

    keys = push_keys(create=True)
    response = webpush(
        subscription_info=subscription(),
        data='{"body":"test"}',
        vapid_private_key=keys["private_key"],
        vapid_claims={"sub": "https://weather.example"},
        requests_session=NoNetwork(),
    )
    assert response.status_code == 201
