"""Private accounts, opaque sessions and versioned cross-device profiles."""

import hashlib
import hmac
import json
import re
import secrets
import threading

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from db import ensure_schema, get_engine

COOKIE = "__Host-MeteoSession"
HEADERS = {
    "Cache-Control": "no-store",
    "X-Content-Type-Options": "nosniff",
    "Vary": "Cookie",
    "Referrer-Policy": "no-referrer",
}
HASH_LOCK = threading.BoundedSemaphore(2)
SYNC_KEYS = {
    "preferences",
    "cities",
    "planner",
    "activity-rules",
    "activity-choice",
    "journal",
    "plans",
    "equipment",
    "observing",
    "identity",
}
ID = re.compile(r"^[A-Za-z0-9_-]{1,80}$")


class ConflictError(Exception):
    pass


class RateLimitError(Exception):
    pass


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def password_hash(password, salt=None):
    if not isinstance(password, str) or not 12 <= len(password) <= 128:
        raise ValueError("Password da 12 a 128 caratteri")
    salt = salt or secrets.token_hex(16)
    with HASH_LOCK:
        result = hashlib.pbkdf2_hmac(
            "sha256", password.encode(), bytes.fromhex(salt), 600000
        ).hex()
    return "pbkdf2-sha256:600000:" + salt + ":" + result


def check_password(password, stored):
    salt = stored.split(":")[2] if stored else "0" * 32
    candidate = password_hash(password, salt)
    return bool(stored) and hmac.compare_digest(candidate, stored)


def rate_limit(bucket, maximum, minutes=15):
    now = pd.Timestamp.now(tz="UTC")
    params = {
        "key": digest(bucket),
        "now": now.isoformat(),
        "end": (now + pd.Timedelta(minutes=minutes)).isoformat(),
    }
    with get_engine().begin() as con:
        con.execute(
            text("DELETE FROM personal_auth_limits WHERE expires_at<:now"), params
        )
        count = con.execute(
            text(
                "INSERT INTO personal_auth_limits(bucket,attempts,expires_at) VALUES(:key,1,:end) ON CONFLICT(bucket) DO UPDATE SET attempts=personal_auth_limits.attempts+1 RETURNING attempts"
            ),
            params,
        ).scalar()
    if count > maximum:
        raise RateLimitError("Troppi tentativi; riprova tra 15 minuti")


def issue_session(account_id, con, method="password"):
    token = secrets.token_urlsafe(32)
    now = pd.Timestamp.now(tz="UTC")
    con.execute(
        text("DELETE FROM personal_sessions WHERE expires_at<:now"),
        {"now": now.isoformat()},
    )
    # A bounded number of active devices; oldest sessions expire first.
    old = (
        con.execute(
            text(
                "SELECT token_hash FROM personal_sessions WHERE account_id=:id ORDER BY expires_at DESC"
            ),
            {"id": account_id},
        )
        .scalars()
        .all()
    )
    for key in old[9:]:
        con.execute(
            text("DELETE FROM personal_sessions WHERE token_hash=:key"), {"key": key}
        )
    con.execute(
        text(
            "INSERT INTO personal_sessions(token_hash,account_id,auth_method,expires_at) VALUES(:token,:id,:method,:end)"
        ),
        {
            "token": digest(token),
            "id": account_id,
            "method": method,
            "end": (now + pd.Timedelta(days=30)).isoformat(),
        },
    )
    return token


def authenticate(token):
    if not isinstance(token, str) or not re.fullmatch(r"[A-Za-z0-9_-]{43}", token):
        raise PermissionError("Accedi al profilo personale")
    with get_engine().connect() as con:
        row = (
            con.execute(
                text(
                    "SELECT a.id,a.username,s.auth_method FROM personal_accounts a JOIN personal_sessions s ON s.account_id=a.id WHERE s.token_hash=:token AND s.expires_at>:now"
                ),
                {"token": digest(token), "now": pd.Timestamp.now(tz="UTC").isoformat()},
            )
            .mappings()
            .first()
        )
    if not row:
        raise PermissionError("Sessione scaduta: accedi di nuovo")
    return dict(row)


def account_login(action, values, address):
    username = values.get("username", "")
    if not isinstance(username, str) or not re.fullmatch(
        r"[A-Za-z0-9_.-]{3,40}", username
    ):
        raise ValueError("Nome da 3 a 40 caratteri: lettere, numeri, punto, trattino")
    username = username.lower()
    rate_limit("auth:global", 120)
    rate_limit("auth:ip:" + address, 40)
    rate_limit("auth:user:" + username, 10)
    password = values.get("password", "")
    recovery = None
    if action == "register":
        rate_limit("register:global", 10, minutes=60)
        hashed = password_hash(password)
        recovery = secrets.token_urlsafe(32)
        account_id = secrets.token_hex(16)
        try:
            with get_engine().begin() as con:
                if (
                    con.execute(text("SELECT COUNT(*) FROM personal_accounts")).scalar()
                    >= 200
                ):
                    raise ValueError("Registrazioni temporaneamente sospese")
                con.execute(
                    text(
                        "INSERT INTO personal_accounts(id,username,password_hash,recovery_hash,created_at) VALUES(:id,:name,:password,:recovery,:at)"
                    ),
                    {
                        "id": account_id,
                        "name": username,
                        "password": hashed,
                        "recovery": digest(recovery),
                        "at": pd.Timestamp.now(tz="UTC").isoformat(),
                    },
                )
                con.execute(
                    text(
                        "INSERT INTO personal_profiles(account_id,revision,payload,updated_at) VALUES(:id,0,'{}',:at)"
                    ),
                    {"id": account_id, "at": pd.Timestamp.now(tz="UTC").isoformat()},
                )
                token = issue_session(account_id, con)
        except IntegrityError as exc:
            raise ValueError(
                "Nome non disponibile: scegli un altro nome o accedi"
            ) from exc
    else:
        with get_engine().connect() as con:
            row = (
                con.execute(
                    text("SELECT * FROM personal_accounts WHERE username=:name"),
                    {"name": username},
                )
                .mappings()
                .first()
            )
        if action == "recover":
            code = values.get("recovery", "")
            if (
                not isinstance(code, str)
                or len(code) > 128
                or not row
                or not hmac.compare_digest(digest(code), row["recovery_hash"])
            ):
                raise PermissionError("Credenziali non valide")
            hashed = password_hash(password)
            recovery = secrets.token_urlsafe(32)
        elif not check_password(password, row["password_hash"] if row else None):
            raise PermissionError("Credenziali non valide")
        account_id = row["id"]
        with get_engine().begin() as con:
            if action == "recover":
                changed = con.execute(
                    text(
                        "UPDATE personal_accounts SET password_hash=:password,recovery_hash=:recovery WHERE id=:id AND recovery_hash=:old"
                    ),
                    {
                        "id": account_id,
                        "password": hashed,
                        "recovery": digest(recovery),
                        "old": row["recovery_hash"],
                    },
                ).rowcount
                if not changed:
                    raise PermissionError("Codice già utilizzato")
                con.execute(
                    text("DELETE FROM personal_sessions WHERE account_id=:id"),
                    {"id": account_id},
                )
                con.execute(
                    text("DELETE FROM personal_devices WHERE account_id=:id"),
                    {"id": account_id},
                )
            token = issue_session(account_id, con)
    return {"id": account_id, "username": username, "recovery": recovery}, token


def plan_config(value):
    from astronomy_planner import equipment_profile
    from sky_catalog import plan_targets
    from v5_data import public_stations
    from v5_horizons import horizon_mask
    from v5_imaging_budget import budget_options
    from v5_observatories import site_spec, timezone_name
    from v5_sessions import session_limits
    from v53_schedule import schedule_options

    if not isinstance(value, dict):
        raise TypeError("Piano non valido")
    targets = [target.name for target in plan_targets(value.get("targets", []))]
    if value.get("station_id") not in {s["id"] for s in public_stations()}:
        raise ValueError("Località non valida")
    result = {k: value[k] for k in ("station_id", "start", "end")}
    result["targets"] = targets
    if value.get("site") is not None:
        result["site"] = site_spec(value["site"])
        result["site_timezone"] = timezone_name(
            value.get("site_timezone") or result["site"].get("timezone")
        )
    for key in ("start", "end"):
        if (
            not isinstance(result[key], str)
            or len(result[key]) > 40
            or pd.isna(pd.to_datetime(result[key], errors="coerce"))
        ):
            raise ValueError("Data del piano non valida")
    result["profile"] = value.get("profile", "deep_sky")
    result["limits"] = session_limits(result["profile"], value.get("limits"))
    for key, default, maximum in (
        ("altitude", 25, 85),
        ("moon", 30, 180),
        ("rotation", 0, 360),
    ):
        v = float(value.get(key, default))
        if not 0 <= v <= maximum:
            raise ValueError("Soglia non valida")
        result[key] = v
    result["horizon"] = {
        f"{az:g}": alt for az, alt in horizon_mask(value.get("horizon", {})).items()
    }
    if value.get("equipment"):
        from dataclasses import asdict

        result["equipment"] = asdict(equipment_profile(**value["equipment"]))
    result.update(schedule_options({**value, "targets": targets}))
    if "budget" in value and result["profile"] == "deep_sky":
        result["budget"] = budget_options(value["budget"])
    return result


def validate_observing(value):
    from sky_catalog import resolve_target
    from v5_horizons import saved_horizons
    from v5_observatories import site_spec
    from v5_projects import saved_projects

    if not isinstance(value, dict) or set(value) - {
        "site",
        "sites",
        "targets",
        "rig",
        "horizons",
        "projects",
    }:
        raise ValueError("Preferenze Cielo non valide")
    sites, targets, rig = (
        value.get("sites", []),
        value.get("targets", []),
        value.get("rig", ""),
    )
    if (
        not isinstance(sites, list)
        or len(sites) > 12
        or not isinstance(targets, list)
        or len(targets) > 30
        or not isinstance(rig, str)
        or len(rig) > 100
    ):
        raise ValueError("Troppi preferiti o strumento non valido")
    return {
        "site": site_spec(value["site"]) if value.get("site") else None,
        "sites": [site_spec(site) for site in sites],
        "targets": list(
            dict.fromkeys(resolve_target(target).name for target in targets)
        ),
        "rig": rig,
        **(
            {"horizons": saved_horizons(value["horizons"])}
            if "horizons" in value
            else {}
        ),
        **(
            {"projects": saved_projects(value["projects"])}
            if "projects" in value
            else {}
        ),
    }


def validate_profile(value):
    from v5_city import saved_city

    if not isinstance(value, dict) or set(value) - SYNC_KEYS:
        raise ValueError("Profilo non valido")
    # JSON only, bounded payload; never persist provider/admin/push tokens.
    encoded = json.dumps(value, allow_nan=False)
    if len(encoded.encode()) > 2400000:
        raise ValueError("Profilo troppo grande; esporta e riduci il diario")
    result = {}
    if "identity" in value:
        from v53_identity import identity_spec

        result["identity"] = identity_spec(value["identity"])
    if "observing" in value:
        result["observing"] = validate_observing(value["observing"])
    prefs = value.get("preferences", {})
    if not isinstance(prefs, dict):
        raise TypeError("Preferenze non valide")
    cards = {"hours", "chart", "astronomy", "change", "history", "air"}
    for key in ("cards", "hidden"):
        if key in prefs and (
            not isinstance(prefs[key], list)
            or len(prefs[key]) > 6
            or any(c not in cards for c in prefs[key])
        ):
            raise ValueError("Riquadri non validi")
    if prefs.get("profile", "deep_sky") not in {
        "deep_sky",
        "visual",
        "planetary",
    } or prefs.get("theme", "light") not in {"light", "dark"}:
        raise ValueError("Preferenze non valide")
    result["preferences"] = {
        k: v
        for k, v in prefs.items()
        if k in {"cards", "hidden", "profile", "expert", "theme", "station"}
    }
    if "station" in result["preferences"] and not ID.fullmatch(
        str(result["preferences"]["station"])
    ):
        raise ValueError("Località iniziale non valida")
    if prefs.get("home_city") is not None:
        result["preferences"]["home_city"] = saved_city(prefs["home_city"])
    for name in ("cities", "equipment", "journal", "plans"):
        values = value.get(name, [])
        if (
            not isinstance(values, list)
            or len(values)
            > {"cities": 12, "equipment": 10, "journal": 500, "plans": 12}[name]
        ):
            raise ValueError("Troppi elementi nel profilo")
        result[name] = []
        for item in values:
            if not isinstance(item, dict):
                raise TypeError("Elemento non valido")
            if name == "cities":
                result[name].append(
                    saved_city(item)
                    if item.get("geo_id") is not None
                    else {k: str(item.get(k, ""))[:120] for k in ("label", "query")}
                )
            elif name == "equipment":
                from dataclasses import asdict

                from astronomy_planner import equipment_profile

                result[name].append(asdict(equipment_profile(**item)))
            elif name == "journal":
                if (
                    not ID.fullmatch(str(item.get("id", "")))
                    or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(item.get("date", "")))
                    or item.get("quality") not in (None, 1, 2, 3, 4, 5)
                ):
                    raise ValueError("Voce del diario non valida")
                cleaned = {
                    k: str(item.get(k, ""))[:limit]
                    for k, limit in (
                        ("id", 80),
                        ("date", 10),
                        ("target", 120),
                        ("notes", 4000),
                        ("station_id", 80),
                        ("station_name", 120),
                    )
                }
                cleaned["quality"] = item.get("quality")
                f = item.get("forecast")
                cleaned["forecast"] = (
                    {
                        k: f.get(k)
                        for k in ("score", "clouds", "good_hours", "captured_at")
                    }
                    if isinstance(f, dict)
                    else None
                )
                result[name].append(cleaned)
            else:
                from v5_push import validate_rules

                if not ID.fullmatch(str(item.get("id", ""))):
                    raise ValueError("Identificativo del piano non valido")
                alert = item.get("alerts", {})
                minutes, percent = (
                    float(alert.get("minutes", 30)),
                    float(alert.get("percent", 25)),
                )
                if not 15 <= minutes <= 180 or not 10 <= percent <= 100:
                    raise ValueError("Soglia avviso non valida")
                quiet = validate_rules(alert)
                config = plan_config(item.get("config"))
                result[name].append(
                    {
                        "id": item["id"],
                        "name": str(item.get("name", "Sessione"))[:120],
                        "config": config,
                        "alerts": {
                            "enabled": alert.get("enabled") is True
                            and not config.get("site"),
                            "minutes": minutes,
                            "percent": percent,
                            "quiet_start": quiet["quiet_start"],
                            "quiet_end": quiet["quiet_end"],
                        },
                    }
                )
    if sum(p["alerts"]["enabled"] for p in result["plans"]) > 3:
        raise ValueError("Massimo tre piani con avvisi attivi")
    for name in ("planner", "activity-rules", "activity-choice"):
        if name in value:
            if len(json.dumps(value[name])) > (32000 if name == "planner" else 16000):
                raise ValueError("Impostazioni troppo grandi")
            if name == "planner":
                result[name] = {
                    **plan_config(value[name]),
                    "horizonText": str(value[name].get("horizonText", ""))[:1000],
                }
            elif name == "activity-choice":
                if value[name] not in {"walk", "bike", "outdoor"}:
                    raise ValueError("Attività non valida")
                result[name] = value[name]
            else:
                if not isinstance(value[name], dict):
                    raise TypeError("Soglie non valide")
                result[name] = value[name]
    return result


def read_profile(account):
    with get_engine().connect() as con:
        row = (
            con.execute(
                text(
                    "SELECT revision,payload,updated_at FROM personal_profiles WHERE account_id=:id"
                ),
                {"id": account["id"]},
            )
            .mappings()
            .one()
        )
        alerts = [
            json.loads(r)
            for r in con.execute(
                text(
                    "SELECT payload FROM personal_alerts WHERE account_id=:id ORDER BY evaluated_at DESC"
                ),
                {"id": account["id"]},
            ).scalars()
        ]
        link = con.execute(
            text("SELECT email FROM personal_google_links WHERE account_id=:id"),
            {"id": account["id"]},
        ).first()
    return {
        "account": {
            **account,
            "auth_method": account.get("auth_method", "password"),
            "google": {"email": link[0]} if link is not None else None,
        },
        "revision": row["revision"],
        "profile": json.loads(row["payload"]),
        "updated_at": row["updated_at"],
        "alerts": alerts,
    }


def write_profile(account, values):
    if values.get("account_id") != account["id"]:
        raise PermissionError(
            "L’account è cambiato in un’altra scheda: accedi di nuovo"
        )
    rate_limit("write:" + account["id"], 120)
    revision = values.get("revision")
    if type(revision) is not int or revision < 0:
        raise ValueError("Revisione richiesta")
    profile = validate_profile(values.get("profile"))
    with get_engine().begin() as con:
        changed = con.execute(
            text(
                "UPDATE personal_profiles SET revision=revision+1,payload=:payload,updated_at=:at WHERE account_id=:id AND revision=:revision"
            ),
            {
                "id": account["id"],
                "revision": revision,
                "payload": json.dumps(profile, allow_nan=False),
                "at": pd.Timestamp.now(tz="UTC").isoformat(),
            },
        ).rowcount
        if changed != 1:
            raise ConflictError(
                "Il profilo è cambiato su un altro dispositivo. Scegli quale versione conservare."
            )
        # Removed/changed plans cannot continue producing alerts from an old state.
        states = con.execute(
            text("SELECT plan_id,payload FROM personal_alerts WHERE account_id=:id"),
            {"id": account["id"]},
        ).all()
        plans = {p["id"]: p for p in profile.get("plans", [])}
        for identifier, payload in states:
            p = plans.get(identifier)
            if (
                not p
                or not p["alerts"]["enabled"]
                or digest(json.dumps(p, sort_keys=True))
                != json.loads(payload).get("plan_hash")
            ):
                con.execute(
                    text(
                        "DELETE FROM personal_alerts WHERE account_id=:id AND plan_id=:plan"
                    ),
                    {"id": account["id"], "plan": identifier},
                )
    return read_profile(account)


def account_action(action, values, account, token):
    identifier = account["id"]
    if action == "profile":
        return write_profile(account, values)
    with get_engine().begin() as con:
        if action == "logout":
            # Detach only the device whose existing push-management token is
            # presented. A guessed subscription ID must not revoke another device.
            device = values.get("device", {})
            if isinstance(device, dict) and isinstance(device.get("token"), str):
                stored = con.execute(
                    text("SELECT token_hash FROM push_subscriptions WHERE id=:id"),
                    {"id": str(device.get("id", ""))},
                ).scalar()
                if stored and hmac.compare_digest(stored, digest(device["token"])):
                    con.execute(
                        text(
                            "DELETE FROM personal_devices WHERE account_id=:id AND subscription_id=:sub"
                        ),
                        {"id": identifier, "sub": device.get("id")},
                    )
            con.execute(
                text("DELETE FROM personal_sessions WHERE token_hash=:token"),
                {"token": digest(token)},
            )
        elif action == "logout-all":
            con.execute(
                text("DELETE FROM personal_sessions WHERE account_id=:id"),
                {"id": identifier},
            )
            con.execute(
                text("DELETE FROM personal_devices WHERE account_id=:id"),
                {"id": identifier},
            )
        elif action == "delete":
            rate_limit("delete:" + identifier, 5)
            hashed = con.execute(
                text("SELECT password_hash FROM personal_accounts WHERE id=:id"),
                {"id": identifier},
            ).scalar()
            if not check_password(values.get("password"), hashed):
                raise PermissionError("Password non valida")
            for table in (
                "personal_sessions",
                "personal_profiles",
                "personal_alerts",
                "personal_devices",
                "personal_google_links",
                "personal_google_challenges",
            ):
                con.execute(
                    text(f"DELETE FROM {table} WHERE account_id=:id"),
                    {"id": identifier},
                )
            con.execute(
                text("DELETE FROM personal_accounts WHERE id=:id"), {"id": identifier}
            )
        elif action == "device":
            sub_id, sub_token = values.get("id", ""), values.get("token", "")
            if (
                not isinstance(sub_id, str)
                or not isinstance(sub_token, str)
                or len(sub_token) > 128
            ):
                raise ValueError("Dispositivo non valido")
            stored = con.execute(
                text("SELECT token_hash FROM push_subscriptions WHERE id=:id"),
                {"id": sub_id},
            ).scalar()
            if not stored or not hmac.compare_digest(stored, digest(sub_token)):
                raise PermissionError("Autorizzazione del dispositivo richiesta")
            con.execute(
                text(
                    "INSERT INTO personal_devices(subscription_id,account_id) VALUES(:sub,:id) ON CONFLICT(subscription_id) DO UPDATE SET account_id=excluded.account_id"
                ),
                {"sub": sub_id, "id": identifier},
            )
        else:
            raise ValueError("Azione non disponibile")
    return {"ok": True}


async def private_api(request):
    # Profile helpers are also imported by the lean ingest worker. Only the
    # HTTP boundary needs Starlette, which is supplied by the web application.
    from starlette.concurrency import run_in_threadpool
    from starlette.responses import JSONResponse

    from v5_api import origin_matches

    action = request.path_params["action"]
    token = request.cookies.get(COOKIE, "")
    try:
        if request.method == "GET" and action == "google-config":
            from v53_google import client_id

            return JSONResponse({"enabled": bool(client_id())}, headers=HEADERS)
        await run_in_threadpool(ensure_schema)
        if request.method == "GET":
            if action != "profile":
                raise ValueError("Azione non disponibile")
            account = await run_in_threadpool(authenticate, token)
            return JSONResponse(
                await run_in_threadpool(read_profile, account), headers=HEADERS
            )
        if not origin_matches(request):
            return JSONResponse(
                {"error": "Origine non consentita"}, status_code=403, headers=HEADERS
            )
        if request.headers.get("content-type", "").split(";")[0] != "application/json":
            raise ValueError("Formato JSON richiesto")
        account = None
        if action not in {"register", "login", "recover", "google-start", "google"}:
            account = await run_in_threadpool(authenticate, token)
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > (2500000 if action == "profile" else 8192):
                return JSONResponse(
                    {"error": "Richiesta troppo grande"},
                    status_code=413,
                    headers=HEADERS,
                )
        values = json.loads(body)
        if not isinstance(values, dict):
            raise TypeError("Richiesta non valida")
        if action in {"google-start", "google"}:
            from v53_google import (
                CHALLENGE_COOKIE,
                MAX_AGE,
                finish_google,
                start_google,
            )

            address = request.client.host if request.client else "unknown"
            flags = {
                "secure": True,
                "httponly": True,
                "samesite": "strict",
                "path": "/",
            }
            if action == "google-start":
                config, challenge = await run_in_threadpool(
                    start_google, values.get("mode"), token, address
                )
                response = JSONResponse(config, headers=HEADERS)
                response.set_cookie(
                    CHALLENGE_COOKIE, challenge, max_age=MAX_AGE, **flags
                )
                return response
            account, new_token, recovery = await run_in_threadpool(
                finish_google,
                values.get("credential"),
                request.cookies.get(CHALLENGE_COOKIE, ""),
                token,
                address,
            )
            profile = await run_in_threadpool(read_profile, account)
            response = JSONResponse({**profile, "recovery": recovery}, headers=HEADERS)
            response.delete_cookie(CHALLENGE_COOKIE, **flags)
            if new_token:
                response.set_cookie(COOKIE, new_token, max_age=30 * 86400, **flags)
            return response
        if action in {"register", "login", "recover"}:
            account, new_token = await run_in_threadpool(
                account_login,
                action,
                values,
                request.client.host if request.client else "unknown",
            )
            response = JSONResponse(
                {
                    **(
                        await run_in_threadpool(
                            read_profile, {k: account[k] for k in ("id", "username")}
                        )
                    ),
                    "recovery": account["recovery"],
                },
                headers=HEADERS,
            )
            response.set_cookie(
                COOKIE,
                new_token,
                secure=True,
                httponly=True,
                samesite="strict",
                max_age=30 * 86400,
                path="/",
            )
            return response
        result = await run_in_threadpool(account_action, action, values, account, token)
        response = JSONResponse(result, headers=HEADERS)
        if action in {"logout", "logout-all", "delete"}:
            response.delete_cookie(
                COOKIE, path="/", secure=True, httponly=True, samesite="strict"
            )
        return response
    except RateLimitError as exc:
        return JSONResponse(
            {"error": str(exc)},
            status_code=429,
            headers={**HEADERS, "Retry-After": "900"},
        )
    except ConflictError as exc:
        return JSONResponse({"error": str(exc)}, status_code=409, headers=HEADERS)
    except PermissionError as exc:
        return JSONResponse({"error": str(exc)}, status_code=401, headers=HEADERS)
    except (ValueError, KeyError, TypeError, OverflowError):
        return JSONResponse(
            {
                "error": "Valori non validi. Controlla nome, password (12–128 caratteri), piani e soglie."
            },
            status_code=400,
            headers=HEADERS,
        )
    except Exception:  # noqa: BLE001 - never expose personal data, hashes or DB details
        return JSONResponse(
            {
                "error": "Profilo temporaneamente non disponibile. Conserva le modifiche sul dispositivo."
            },
            status_code=503,
            headers=HEADERS,
        )
