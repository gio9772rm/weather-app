"""Opt-in web push, evaluated only inside the existing ten-minute cron.

Endpoints, encryption keys and management tokens are private. No contact list,
server secrets or notification bodies enter the public weather API or logs.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import secrets
from urllib.parse import urlsplit

import pandas as pd
from sqlalchemy import text

from db import ensure_schema, get_engine


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _decode(value: str) -> bytes:
    if not isinstance(value, str) or not re.fullmatch(
        r"[A-Za-z0-9_-]{16,128}={0,2}", value
    ):
        raise ValueError("Chiave push non valida")
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def push_keys(*, create=False) -> dict | None:
    ensure_schema()
    with get_engine().begin() as con:
        row = (
            con.execute(
                text("SELECT private_key,public_key FROM push_config WHERE id=1")
            )
            .mappings()
            .first()
        )
        if not row and create:
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.primitives.asymmetric import ec

            key = ec.generate_private_key(ec.SECP256R1())
            values = {
                "private": _b64(
                    key.private_bytes(
                        serialization.Encoding.DER,
                        serialization.PrivateFormat.PKCS8,
                        serialization.NoEncryption(),
                    )
                ),
                "public": _b64(
                    key.public_key().public_bytes(
                        serialization.Encoding.X962,
                        serialization.PublicFormat.UncompressedPoint,
                    )
                ),
            }
            con.execute(
                text(
                    "INSERT INTO push_config(id,private_key,public_key) VALUES(1,:private,:public) ON CONFLICT(id) DO NOTHING"
                ),
                values,
            )
            row = (
                con.execute(
                    text("SELECT private_key,public_key FROM push_config WHERE id=1")
                )
                .mappings()
                .first()
            )
        return dict(row) if row else None


def validate_subscription(value: dict) -> dict:
    if not isinstance(value, dict):
        raise TypeError("Sottoscrizione non valida")
    endpoint = value.get("endpoint", "")
    if not isinstance(endpoint, str) or not 30 <= len(endpoint) <= 2048:
        raise ValueError("Recapito push non valido")
    url = urlsplit(endpoint)
    host = url.hostname or ""
    # Browser push services only. Arbitrary destinations and redirects are
    # rejected to prevent the public subscription endpoint becoming an SSRF.
    allowed = host in {
        "fcm.googleapis.com",
        "updates.push.services.mozilla.com",
        "web.push.apple.com",
    } or host.endswith(".notify.windows.com")
    if (
        not allowed
        or url.scheme != "https"
        or url.port not in (None, 443)
        or url.username
        or url.password
        or url.fragment
    ):
        raise ValueError("Servizio push non supportato")
    keys = value.get("keys", {})
    if not isinstance(keys, dict):
        raise TypeError("Chiavi push non valide")
    public, auth = _decode(keys.get("p256dh")), _decode(keys.get("auth"))
    if len(public) != 65 or public[0] != 4 or len(auth) != 16:
        raise ValueError("Chiavi push non valide")
    from cryptography.hazmat.primitives.asymmetric import ec

    ec.EllipticCurvePublicKey.from_encoded_point(ec.SECP256R1(), public)
    return {"endpoint": endpoint, "keys": {"p256dh": _b64(public), "auth": _b64(auth)}}


def validate_rules(value: dict) -> dict:
    if not isinstance(value, dict):
        raise TypeError("Preferenze non valide")
    result = {
        kind: value.get(kind) is True
        for kind in ("rain", "wind", "astronomy", "station")
    }
    for key, low, high, default in (("pop", 30, 100, 60), ("gust", 20, 150, 40)):
        number = float(value.get(key, default))
        if not low <= number <= high:
            raise ValueError("Soglia non valida")
        result[key] = number
    for key, default in (("quiet_start", "23:00"), ("quiet_end", "08:00")):
        hour = value.get(key, default)
        if not isinstance(hour, str) or not re.fullmatch(
            r"(?:[01]\d|2[0-3]):[0-5]\d", hour
        ):
            raise ValueError("Fascia silenziosa non valida")
        result[key] = hour
    profile = value.get("profile", "deep_sky")
    if profile not in {"deep_sky", "visual", "planetary"}:
        raise ValueError("Profilo non valido")
    result["profile"] = profile
    return result


def save_subscription(value: dict, token: str = "") -> dict:
    from v5_data import public_stations

    station = value.get("station_id")
    if station not in {row["id"] for row in public_stations()}:
        raise ValueError("Località non disponibile")
    subscription = validate_subscription(value.get("subscription"))
    rules = validate_rules(value.get("rules", {}))
    identifier = hashlib.sha256(subscription["endpoint"].encode()).hexdigest()
    new_token = secrets.token_urlsafe(32)
    with get_engine().begin() as con:
        row = con.execute(
            text("SELECT token_hash FROM push_subscriptions WHERE id=:id"),
            {"id": identifier},
        ).scalar()
        if row and (
            not token
            or not hmac.compare_digest(row, hashlib.sha256(token.encode()).hexdigest())
        ):
            raise PermissionError(
                "Rimuovi la sottoscrizione dal browser e attivala di nuovo"
            )
        if (
            not row
            and con.execute(text("SELECT COUNT(*) FROM push_subscriptions")).scalar()
            >= 200
        ):
            raise ValueError("Numero massimo di dispositivi raggiunto")
        con.execute(
            text(
                "INSERT INTO push_subscriptions(id,token_hash,subscription,station_id,rules,updated_at) VALUES(:id,:token,:sub,:station,:rules,:at) ON CONFLICT(id) DO UPDATE SET subscription=excluded.subscription,station_id=excluded.station_id,rules=excluded.rules,updated_at=excluded.updated_at"
            ),
            {
                "id": identifier,
                "token": hashlib.sha256(new_token.encode()).hexdigest(),
                "sub": json.dumps(subscription),
                "station": station,
                "rules": json.dumps(rules),
                "at": pd.Timestamp.now(tz="UTC").isoformat(),
            },
        )
    return {"id": identifier, "token": token if row else new_token}


def delete_subscription(identifier: str, token: str) -> None:
    with get_engine().begin() as con:
        stored = con.execute(
            text("SELECT token_hash FROM push_subscriptions WHERE id=:id"),
            {"id": identifier},
        ).scalar()
        if stored and (
            not token
            or not hmac.compare_digest(
                stored, hashlib.sha256(token.encode()).hexdigest()
            )
        ):
            raise PermissionError("Autorizzazione richiesta")
        con.execute(
            text("DELETE FROM push_deliveries WHERE subscription_id=:id"),
            {"id": identifier},
        )
        con.execute(
            text("DELETE FROM push_subscriptions WHERE id=:id"), {"id": identifier}
        )


def quiet_now(rules: dict, now: pd.Timestamp, timezone: str) -> bool:
    local = now.tz_convert(timezone).strftime("%H:%M")
    start, end = rules["quiet_start"], rules["quiet_end"]
    return (
        start <= local < end
        if start < end
        else (local >= start or local < end)
        if start > end
        else False
    )


def notification_candidates(
    snapshot: dict, rules: dict, now: pd.Timestamp
) -> list[dict]:
    generated = pd.to_datetime(snapshot.get("generated_at"), utc=True, errors="coerce")
    if pd.isna(generated) or not pd.Timedelta(0) <= now - generated <= pd.Timedelta(
        minutes=30
    ):
        return []
    rows = [
        r
        for r in snapshot.get("forecast", [])
        if now
        <= pd.to_datetime(r["valid_time"], utc=True)
        <= now + pd.Timedelta(hours=6)
    ]
    station = snapshot["station"]
    if quiet_now(rules, now, station["timezone"]):
        return []
    result = []

    def add(kind, event, message, page):
        result.append(
            {
                "kind": kind,
                "event_key": event,
                "title": "Meteo Pro · " + station["name"],
                "body": message,
                "station": station["id"],
                "page": page,
            }
        )

    def number(row, key):
        try:
            return float(row.get(key))
        except (ValueError, TypeError):
            return float("nan")

    for kind, key, threshold in (
        ("rain", "precip_probability", rules["pop"]),
        ("wind", "wind_gust_kmh", rules["gust"]),
    ):
        match = next(
            (
                r
                for r in rows
                if number(r, key) >= threshold
                and (kind != "rain" or number(r, "rain_mm") > 0)
            ),
            None,
        )
        if rules[kind] and match:
            local = (
                pd.Timestamp(match["valid_time"])
                .tz_convert(station["timezone"])
                .strftime("%H:%M")
            )
            message = (
                f"Pioggia prevista dalle {local}: probabilità {number(match, key):.0f}%."
                if kind == "rain"
                else f"Raffiche previste alle {local}: {number(match, key):.0f} km/h."
            )
            add(kind, str(match["valid_time"]), message, "forecast")
    if rules["astronomy"]:
        windows = (
            snapshot.get("astronomy", {}).get(rules["profile"], {}).get("nights", [])
        )
        for night in windows:
            start = pd.to_datetime(night.get("best_start"), utc=True, errors="coerce")
            if (
                pd.notna(start)
                and now <= start <= now + pd.Timedelta(hours=12)
                and night.get("continuous_hours", 0) >= 1.5
            ):
                add(
                    "astronomy",
                    str(night["date"]),
                    f"Finestra favorevole dalle {start.tz_convert(station['timezone']):%H:%M}: {night['continuous_hours']:.1f} ore continue previste.",
                    "astronomy",
                )
                break
    observed = pd.to_datetime(snapshot.get("observed_at"), utc=True, errors="coerce")
    if (
        rules["station"]
        and pd.notna(observed)
        and now - observed >= pd.Timedelta(minutes=60)
    ):
        add(
            "station",
            str(snapshot["observed_at"]),
            "La stazione non trasmette misure aggiornate da almeno un’ora.",
            "stations",
        )
    return result


def send_pending(*, now=None, sender=None) -> int:
    """Reserve before delivery; duplicates are bounded even after a cron crash."""
    import requests
    from pywebpush import WebPushException, webpush

    now = pd.Timestamp(now if now is not None else pd.Timestamp.now(tz="UTC"))
    keys = push_keys(create=True)
    cutoff = (now - pd.Timedelta(days=90)).isoformat()
    with get_engine().begin() as con:
        con.execute(
            text("DELETE FROM push_subscriptions WHERE updated_at<:cutoff"),
            {"cutoff": cutoff},
        )
        con.execute(
            text(
                "DELETE FROM push_deliveries WHERE subscription_id NOT IN (SELECT id FROM push_subscriptions)"
            )
        )
        subscriptions = (
            con.execute(
                text(
                    "SELECT * FROM push_subscriptions ORDER BY updated_at DESC LIMIT 200"
                )
            )
            .mappings()
            .all()
        )
        snapshots = {
            row[0]: json.loads(row[1])
            for row in con.execute(
                text("SELECT station_id,payload FROM public_snapshots")
            )
        }
    sent, attempts = 0, 0

    class NoRedirectSession(requests.Session):
        def post(self, url, **kwargs):
            kwargs["allow_redirects"] = False
            return super().post(url, **kwargs)

    with NoRedirectSession() as session:
        for sub in subscriptions:
            snapshot = snapshots.get(sub["station_id"])
            if not snapshot:
                continue
            for candidate in notification_candidates(
                snapshot, json.loads(sub["rules"]), now
            ):
                if attempts >= 30:
                    return sent
                params = {
                    "id": sub["id"],
                    "kind": candidate["kind"],
                    "event": candidate["event_key"],
                    "at": now.isoformat(),
                    "cutoff": (now - pd.Timedelta(hours=4)).isoformat(),
                }
                with get_engine().begin() as con:
                    claimed = con.execute(
                        text(
                            "INSERT INTO push_deliveries(subscription_id,kind,event_key,attempted_at,status) VALUES(:id,:kind,:event,:at,'attempted') ON CONFLICT(subscription_id,kind) DO UPDATE SET event_key=excluded.event_key,attempted_at=excluded.attempted_at,status='attempted' WHERE push_deliveries.attempted_at<:cutoff AND push_deliveries.event_key<>:event"
                        ),
                        params,
                    ).rowcount
                if not claimed:
                    continue
                attempts += 1
                try:
                    (sender or webpush)(
                        subscription_info=validate_subscription(
                            json.loads(sub["subscription"])
                        ),
                        data=json.dumps(
                            {k: v for k, v in candidate.items() if k != "event_key"}
                        ),
                        vapid_private_key=keys["private_key"],
                        vapid_claims={
                            "sub": "https://weather-app-v3-w2jd.onrender.com"
                        },
                        ttl=600,
                        timeout=8,
                        requests_session=session,
                    )
                except WebPushException as exc:
                    if exc.response is not None and exc.response.status_code in (
                        404,
                        410,
                    ):
                        with get_engine().begin() as con:
                            con.execute(
                                text("DELETE FROM push_subscriptions WHERE id=:id"),
                                {"id": sub["id"]},
                            )
                    continue
                except (ValueError, requests.RequestException):
                    continue
                with get_engine().begin() as con:
                    con.execute(
                        text(
                            "UPDATE push_deliveries SET status='sent' WHERE subscription_id=:id AND kind=:kind"
                        ),
                        params,
                    )
                sent += 1
    return sent
