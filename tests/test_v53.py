from __future__ import annotations

import copy
import json
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pytest
from sqlalchemy import text
from starlette.applications import Starlette
from starlette.testclient import TestClient

from v5_api import public_routes
from v53_alerts import session_changes
from v53_comparison import paired_comparison, published_history
from v53_profiles import (
    ConflictError,
    RateLimitError,
    authenticate,
    rate_limit,
    validate_profile,
    write_profile,
)
from v53_schedule import optimize_schedule, schedule_options
from v53_sky import sky_rows
from v53_windows import member_paths, validate_windows, window_probabilities


def comparison_data(days=10):
    times = pd.date_range("2026-08-01T00:00Z", periods=days * 24, freq="h")
    frames = []
    for name, error in (("a", 1), ("b", 2)):
        frames.append(
            pd.DataFrame(
                {
                    "valid_time": times,
                    "issued_at": times - pd.Timedelta(hours=24),
                    "acquired_at": times - pd.Timedelta(hours=24),
                    "provider": "test",
                    "model": name,
                    "basis": "live",
                    "temp_c": 20 + error,
                    "interval_hours": 1,
                }
            )
        )
    return (
        pd.concat(frames, ignore_index=True),
        pd.DataFrame({"time": times, "temp_c": 20}),
        times[-1] + pd.Timedelta(hours=1),
    )


def test_paired_errors_use_identical_hours_and_acquisition_cutoffs():
    f, obs, now = comparison_data()
    f.loc[f.model.eq("b") & f.valid_time.lt(obs.time.iloc[24]), "temp_c"] = None
    f.loc[f.model.eq("a") & f.valid_time.eq(obs.time.iloc[24]), "acquired_at"] += (
        pd.Timedelta(minutes=1)
    )
    results = paired_comparison(f, obs, now)
    row = next(r for r in results if r["window_days"] == 30 and r["lead_hours"] == 24)
    assert row["n"] == len(obs) - 25
    assert row["mae_left"] == 1 and row["mae_right"] == 2
    assert row["delta_ci95"] == [-1, -1]
    assert row["evidence"] == "left"
    assert not any(r["lead_hours"] == 72 for r in results)


def test_paired_small_sample_has_no_significance_or_cross_basis_pairs():
    f, obs, now = comparison_data(1)
    rows = paired_comparison(f, obs, now)
    assert rows and all(
        not r["adequate"] and r["evidence"] == "uncertain" for r in rows
    )
    f.loc[f.model.eq("b"), "basis"] = "previous_run"
    assert paired_comparison(f, obs, now) == []


def test_paired_native_six_hour_rain_is_never_compared_to_hourly_gauge():
    f, _, now = comparison_data()
    f["rain_mm"] = 2
    f.loc[f.model.eq("b"), "interval_hours"] = 6
    times = pd.date_range(now - pd.Timedelta(days=10), now, freq="5min")
    results = paired_comparison(f, pd.DataFrame({"time": times, "rain_mm": 0}), now)
    assert results == []


def paths():
    times = pd.date_range("2026-09-01T18:00Z", periods=7, freq="h")
    return {
        "hourly_native": True,
        "model": "icon_seamless",
        "acquired_at": (times[0] - pd.Timedelta(minutes=1)).isoformat(),
        "times": [t.isoformat() for t in times],
        "members": {
            str(i): {
                "rain": [0.0] * 7,
                "temp": [15.0] * 7,
                "rh": [60.0] * 7,
                "wind": [4.0] * 7,
                "gust": [7.0] * 7,
                "clouds": [10.0] * 7,
            }
            for i in range(20)
        },
    }


def test_continuous_probability_uses_members_not_hourly_products():
    p = paths()
    for i, m in enumerate(p["members"].values()):
        m["rain"][1 if i < 10 else 2] = 0.2
    first = window_probabilities(p)[0]
    assert first["probability"] == 0
    assert first["members"] == 20
    assert first["start"] == pd.Timestamp(p["times"][0])
    assert first["end"] == pd.Timestamp(p["times"][2])


def test_missing_members_are_not_dry_and_gaps_are_not_bridged():
    p = paths()
    for m in list(p["members"].values())[:5]:
        m["rain"][1] = None
    first = window_probabilities(p)[0]
    assert first["members"] == 15 and first["probability"] is None
    p["times"][1] = pd.Timestamp(p["times"][1]) + pd.Timedelta(minutes=30)
    rows = window_probabilities(p)
    assert not any(r["start"] == pd.Timestamp(p["times"][0]) for r in rows)
    p["hourly_native"] = False
    assert window_probabilities(p) == []


def test_photo_window_requires_same_member_at_every_endpoint():
    p = paths()
    p["members"]["0"]["clouds"][0] = 90
    p["members"]["1"]["rh"][3] = 100
    p["members"]["2"]["wind"][2] = None
    row = next(r for r in window_probabilities(p) if r["kind"] == "photo")
    assert row["members"] == 19 and row["successful_members"] == 17
    assert row["probability"] == pytest.approx(17 / 19 * 100)


def test_member_parser_preserves_identity_and_rejects_native_coarse_model():
    p = paths()
    payload = {
        "hourly": {
            "time": p["times"],
            "precipitation_member02": [2] * 7,
            "precipitation_member01": [1] * 7,
            "cloud_cover_member01": [10] * 7,
            "cloud_cover_member02": [20] * 7,
        }
    }
    parsed = member_paths(payload, "icon_seamless", pd.Timestamp(p["acquired_at"]))
    assert parsed["members"]["1"]["rain"][0] == 1
    assert parsed["members"]["1"]["clouds"][0] == 10
    assert not member_paths(payload, "weathernext2", pd.Timestamp(p["acquired_at"]))[
        "hourly_native"
    ]


def test_prospective_window_validation_requires_full_observed_hours(
    sqlite_engine, monkeypatch
):
    now = pd.Timestamp("2026-09-02T08:00Z")
    start = now - pd.Timedelta(hours=4)
    product = {
        "rows": [
            {
                "kind": "dry",
                "start": start.isoformat(),
                "end": (start + pd.Timedelta(hours=2)).isoformat(),
                "probability": 80,
            }
        ]
    }
    with sqlite_engine.begin() as con:
        con.execute(
            text("INSERT INTO window_predictions VALUES('one','cycle',:at,:payload)"),
            {
                "at": (start - pd.Timedelta(hours=7)).isoformat(),
                "payload": json.dumps(product),
            },
        )
    obs = pd.DataFrame(
        {
            "time": pd.date_range(
                start + pd.Timedelta(minutes=5), periods=24, freq="5min"
            ),
            "rain_mm": 0,
        }
    )
    monkeypatch.setattr("data_access.load_station", lambda *_: obs)
    result = validate_windows("one", now)
    assert result["n"] == 1 and result["brier"] == pytest.approx(0.04)
    assert validate_windows("other", now)["n"] == 0
    obs.loc[5, "rain_mm"] = None
    assert validate_windows("one", now)["n"] == 0


def test_cams_missing_or_stale_does_not_invent_transparency():
    now = pd.Timestamp("2026-09-02T18:00Z")
    forecast = pd.DataFrame(
        {
            "valid_time": [now],
            "clouds": [0],
            "cape_j_kg": [100],
            "wind_300hpa_kmh": [40],
        }
    )
    air = {
        "fetched_at": now,
        "hourly": [{"time": now, "aerosol_optical_depth": 0.5, "dust": 150}],
    }
    row = sky_rows(forecast, air, now)["rows"][0]
    assert row["transparency_estimate"] == 61
    assert row["aerosol_extinction_mag_airmass"] == pytest.approx(0.543)
    assert row["sky_brightness"] is None
    air["fetched_at"] = now - pd.Timedelta(hours=13)
    assert sky_rows(forecast, air, now)["rows"][0]["transparency_estimate"] is None
    assert sky_rows(forecast, None, now)["rows"][0]["aod_550nm"] is None


def test_scheduler_respects_obstacles_windows_and_overheads():
    start = pd.Timestamp("2026-09-02T18:00Z")
    end = start + pd.Timedelta(hours=4)
    sessions = [
        {
            "target": "a",
            "windows": [{"start": start, "end": start + pd.Timedelta(hours=2)}],
            "limiting_factor": "Luna",
        },
        {
            "target": "b",
            "windows": [{"start": start + pd.Timedelta(hours=2), "end": end}],
            "limiting_factor": "Altezza",
        },
    ]
    tracks = pd.concat(
        [
            pd.DataFrame(
                {
                    "target": t,
                    "valid_time": pd.date_range(start, end, freq="15min"),
                    "planner_score": 80,
                }
            )
            for t in ("a", "b")
        ]
    )
    options = schedule_options(
        {
            "targets": ["a", "b"],
            "setup_minutes": 20,
            "switch_minutes": 10,
            "minimum_minutes": 45,
        }
    )
    result = optimize_schedule(sessions, tracks, start, end, options)
    assert {b["target"] for b in result["blocks"]} == {"a", "b"}
    assert result["blocks"][0]["prepare_start"] >= start
    for prev, nxt in zip(result["blocks"], result["blocks"][1:]):
        assert prev["end"] <= nxt["prepare_start"]
    assert result["net_hours"] == pytest.approx(
        sum(b["minutes"] for b in result["blocks"]) / 60
    )
    assert result["net_hours"] < 4 and result["overhead_minutes"] >= 30


@pytest.mark.parametrize(
    "values",
    [
        {"switch_minutes": -1},
        {"minimum_minutes": float("nan")},
        {"compare_nights": 500},
        {"priorities": {"unknown": 4}},
    ],
)
def test_scheduler_rejects_unbounded_options(values):
    with pytest.raises(ValueError):
        schedule_options(values)


def client():
    return TestClient(Starlette(routes=public_routes()), base_url="https://testserver")


def register(c, name="personal-one"):
    response = c.post(
        "/api/v5/personal/register",
        json={"username": name, "password": "test-password-unique-53"},
        headers={"origin": "https://testserver"},
    )
    assert response.status_code == 200, response.text
    return response.json()


def write(c, session, profile, revision=None):
    return c.post(
        "/api/v5/personal/profile",
        json={
            "account_id": session["account"]["id"],
            "revision": session["revision"] if revision is None else revision,
            "profile": profile,
        },
        headers={"origin": "https://testserver"},
    )


def test_accounts_secure_cookies_private_reads_and_no_raw_credentials(sqlite_engine):
    c = client()
    assert c.get("/api/v5/personal/profile").status_code == 401
    r = c.post(
        "/api/v5/personal/register",
        json={"username": "sample", "password": "test-password-unique-53"},
        headers={"origin": "https://testserver"},
    )
    assert r.status_code == 200
    cookie = r.headers["set-cookie"]
    assert all(v in cookie for v in ("HttpOnly", "Secure", "SameSite=strict", "Path=/"))
    assert "no-store" in r.headers["cache-control"]
    with sqlite_engine.connect() as con:
        row = con.execute(
            text("SELECT password_hash,recovery_hash FROM personal_accounts")
        ).one()
        assert "test-password" not in row[0] and row[0].startswith(
            "pbkdf2-sha256:600000:"
        )
        assert row[1] != r.json()["recovery"]
        session_hash = con.execute(
            text("SELECT token_hash FROM personal_sessions")
        ).scalar()
        assert session_hash not in cookie
    assert "password_hash" not in c.get("/api/v5/personal/profile").text


def test_private_ownership_and_compare_and_swap(sqlite_engine):
    a, b = client(), client()
    one, two = register(a), register(b, "personal-two")
    profile = {"cities": [{"label": "Roma", "query": "Roma"}]}
    assert write(a, one, profile).status_code == 200
    assert b.get("/api/v5/personal/profile").json()["profile"] == {}
    assert write(a, one, {"cities": []}).status_code == 409
    assert write(a, two, profile).status_code == 401
    assert (
        a.get("/api/v5/personal/profile").json()["profile"]["cities"]
        == profile["cities"]
    )


def test_concurrent_profile_writes_preserve_one_revision(sqlite_engine):
    c = client()
    s = register(c)

    def update(n):
        try:
            return write_profile(
                s["account"],
                {
                    "account_id": s["account"]["id"],
                    "revision": 0,
                    "profile": {"cities": [{"label": str(n)}]},
                },
            )["revision"]
        except ConflictError:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(update, [1, 2]))
    assert sorted(map(str, results)) == ["1", "conflict"]


def test_csrf_origin_json_and_profile_allowlist(sqlite_engine):
    c = client()
    s = register(c)
    for headers in (
        {},
        {"origin": "https://testserver.evil"},
        {"origin": "https://testserver/path"},
    ):
        assert (
            c.post("/api/v5/personal/profile", json={}, headers=headers).status_code
            == 403
        )
    assert write(c, s, {"admin_token": "do-not-store"}).status_code == 400
    assert (
        c.post(
            "/api/v5/personal/logout",
            content="{}",
            headers={"origin": "https://testserver"},
        ).status_code
        == 400
    )


def test_recovery_rotates_code_and_revokes_all_old_sessions(sqlite_engine):
    a, b = client(), client()
    s = register(a)
    result = b.post(
        "/api/v5/personal/recover",
        json={
            "username": s["account"]["username"],
            "password": "a-new-long-password-53",
            "recovery": s["recovery"],
        },
        headers={"origin": "https://testserver"},
    )
    assert result.status_code == 200 and result.json()["recovery"] != s["recovery"]
    assert a.get("/api/v5/personal/profile").status_code == 401
    assert b.get("/api/v5/personal/profile").status_code == 200
    retry = a.post(
        "/api/v5/personal/recover",
        json={
            "username": s["account"]["username"],
            "password": "another-long-password-53",
            "recovery": s["recovery"],
        },
        headers={"origin": "https://testserver"},
    )
    assert retry.status_code == 401


def test_logout_all_and_delete_account(sqlite_engine):
    c = client()
    register(c)
    assert (
        c.post(
            "/api/v5/personal/delete",
            json={"password": "wrong-long-password"},
            headers={"origin": "https://testserver"},
        ).status_code
        == 401
    )
    assert (
        c.post(
            "/api/v5/personal/delete",
            json={"password": "test-password-unique-53"},
            headers={"origin": "https://testserver"},
        ).status_code
        == 200
    )
    assert c.get("/api/v5/personal/profile").status_code == 401
    with sqlite_engine.connect() as con:
        assert con.execute(text("SELECT COUNT(*) FROM personal_accounts")).scalar() == 0
        assert con.execute(text("SELECT COUNT(*) FROM personal_profiles")).scalar() == 0


def test_persistent_auth_throttling_and_session_expiry(sqlite_engine):
    for _ in range(2):
        rate_limit("test", 2)
    with pytest.raises(RateLimitError):
        rate_limit("test", 2)
    c = client()
    register(c)
    token = c.cookies.get("__Host-MeteoSession")
    with sqlite_engine.begin() as con:
        con.execute(
            text("UPDATE personal_sessions SET expires_at='2000-01-01T00:00:00+00:00'")
        )
    with pytest.raises(PermissionError):
        authenticate(token)


def test_saved_session_thresholds_ignore_tiny_changes():
    old = {
        "targets": {"M31": 3},
        "net_hours": 3,
        "alternative_hours": 2,
        "alternative_start": "2026-09-03T20:00Z",
    }
    new = copy.deepcopy(old)
    rules = {"minutes": 30, "percent": 25}
    new["targets"]["M31"] = 2.8
    assert session_changes(old, new, rules) == []
    new["targets"]["M31"] = 2
    assert "M31" in session_changes(old, new, rules)[0]
    new["alternative_hours"] = 4
    assert len(session_changes(old, new, rules)) == 2


def test_published_history_is_causal_and_station_scoped(sqlite_engine):
    now = pd.Timestamp("2026-09-02T10:00Z")
    target = now - pd.Timedelta(hours=1)
    with sqlite_engine.begin() as con:
        for station, acquired, temp in (
            ("one", now - pd.Timedelta(hours=3), 20),
            ("one", now, 99),
            ("other", now - pd.Timedelta(hours=3), 80),
        ):
            con.execute(
                text(
                    "INSERT INTO location_model_runs VALUES(:id,'website','published',:at,:at,'published',:payload)"
                ),
                {
                    "id": station,
                    "at": acquired.isoformat(),
                    "payload": json.dumps(
                        [{"valid_time": target.isoformat(), "temp_c": temp}]
                    ),
                },
            )
    result = published_history("one", now)
    assert len(result) == 1 and result[0]["temp_c"] == 20


def test_profile_limits_keep_device_secrets_out():
    p = validate_profile(
        {
            "preferences": {
                "theme": "dark",
                "offline": True,
                "admin_token": "secret",
                "station": "roma-primary",
            }
        }
    )
    assert p["preferences"] == {"theme": "dark", "station": "roma-primary"}
    with pytest.raises(ValueError):
        validate_profile({"plans": [{}] * 13})


def test_streamed_comparison_retains_same_cutoff_and_scope(sqlite_engine, monkeypatch):
    from v53_comparison import load_common_runs

    monkeypatch.setenv("STATION_ID", "one")
    now = pd.Timestamp("2026-09-02T12:00Z")
    valid = now - pd.Timedelta(hours=1)
    with sqlite_engine.begin() as con:
        for station, lead, temp in (
            ("one", 25, 20),
            ("one", 23, 99),
            ("other", 24, 80),
        ):
            issued = valid - pd.Timedelta(hours=lead)
            con.execute(
                text(
                    "INSERT INTO location_model_runs VALUES(:id,'test','model',:at,:at,'live',:payload)"
                ),
                {
                    "id": station,
                    "at": issued.isoformat(),
                    "payload": json.dumps(
                        [{"valid_time": valid.isoformat(), "temp_c": temp}]
                    ),
                },
            )
    frame = load_common_runs("one", now)
    assert len(frame) == 1 and frame.temp_c.iloc[0] == 20


def test_logout_detaches_only_proven_private_push_device(sqlite_engine):
    from v53_profiles import digest

    c = client()
    s = register(c)
    with sqlite_engine.begin() as con:
        for device in ("this-device", "another-device"):
            con.execute(
                text(
                    "INSERT INTO push_subscriptions VALUES(:id,:token,'{}','one','{}','2026-09-01T00:00Z')"
                ),
                {"id": device, "token": digest(device + "-token")},
            )
            con.execute(
                text("INSERT INTO personal_devices VALUES(:device,:account)"),
                {"device": device, "account": s["account"]["id"]},
            )
    r = c.post(
        "/api/v5/personal/logout",
        json={"device": {"id": "this-device", "token": "this-device-token"}},
        headers={"origin": "https://testserver"},
    )
    assert r.status_code == 200
    with sqlite_engine.connect() as con:
        assert con.execute(
            text("SELECT subscription_id FROM personal_devices")
        ).scalars().all() == ["another-device"]


def test_private_session_alerts_require_owner_recent_event_and_quiet_hours(
    sqlite_engine,
):
    from v53_alerts import device_candidates

    now = pd.Timestamp("2026-09-02T20:00Z")
    state = {
        "start": (now + pd.Timedelta(hours=12)).isoformat(),
        "rules": {"quiet_start": "23:00", "quiet_end": "08:00"},
        "timezone": "Europe/Rome",
        "pending": {
            "created_at": now.isoformat(),
            "body": "M31 ridotta",
            "kind": "session:a",
        },
    }
    with sqlite_engine.begin() as con:
        con.execute(text("INSERT INTO personal_devices VALUES('own','account')"))
        con.execute(text("INSERT INTO personal_devices VALUES('other','another')"))
        con.execute(
            text("INSERT INTO personal_alerts VALUES('account','plan',:payload,:at)"),
            {"payload": json.dumps(state), "at": now.isoformat()},
        )
    assert len(device_candidates("own", now)) == 1
    assert device_candidates("other", now) == []
    assert device_candidates("own", now + pd.Timedelta(hours=1)) == []
    assert device_candidates("own", now + pd.Timedelta(hours=2)) == []
