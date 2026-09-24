"""Small same-origin API. Public weather is allowlisted; writes require auth."""

from __future__ import annotations

import csv
import hashlib
import hmac
import io
import json
import os
import re
import threading
import time
import zipfile
from pathlib import Path
from urllib.parse import urlsplit

from starlette.concurrency import run_in_threadpool
from starlette.responses import (
    FileResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
)
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

from v5_data import public_stations, read_snapshot

ROOT = Path(__file__).parent
_cache: dict[str, tuple[float, dict]] = {}
_lock = threading.Lock()
SAFE_ID = re.compile(r"^[a-z0-9-]{1,80}$")


def snapshot_for(station_id: str):
    if not SAFE_ID.fullmatch(station_id):
        raise ValueError("Località non valida")
    if station_id not in {item["id"] for item in public_stations()}:
        raise ValueError("Località non disponibile")
    with _lock:
        now = time.monotonic()
        cached = _cache.get(station_id)
        # Read the published revision; a second relative 600-second cache used
        # to keep an old revision for almost twenty minutes. This never fetches
        # providers: the browser still has exactly one ten-minute refresh timer.
        result = read_snapshot(station_id)
        if cached and cached[1] == result:
            return cached[1]
        _cache[station_id] = (now, result)
        return result


async def snapshot(request):
    try:
        result = await run_in_threadpool(
            snapshot_for, request.path_params["station_id"]
        )
    except ValueError:
        return JSONResponse({"error": "Località non disponibile"}, status_code=404)
    except Exception:  # noqa: BLE001 - public boundary must not expose DB internals
        return JSONResponse(
            {
                "error": "Dati temporaneamente non disponibili; conserva l’ultima fotografia."
            },
            status_code=503,
            headers={"Cache-Control": "no-store"},
        )
    body = json.dumps(result, ensure_ascii=False, allow_nan=False).encode()
    etag = '"' + hashlib.sha256(body).hexdigest() + '"'
    headers = {
        "Cache-Control": "private, no-cache",
        "ETag": etag,
        "X-Content-Type-Options": "nosniff",
    }
    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers=headers)
    return Response(body, media_type="application/json", headers=headers)


async def stations(request):
    try:
        result = await run_in_threadpool(public_stations)
        return JSONResponse(
            {"stations": result, "version": "5.1.0", "refresh_seconds": 600}
        )
    except Exception:  # noqa: BLE001 - public boundary
        return JSONResponse(
            {"error": "Elenco località non disponibile"}, status_code=503
        )


async def home(request):
    if any(
        key in request.query_params for key in ("tab", "admin", "admin_token", "city")
    ):
        return RedirectResponse(
            "/pro/?" + request.url.query,
            headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"},
        )
    return FileResponse(
        ROOT / "static/v5/index.html",
        headers={"Cache-Control": "no-cache", "Referrer-Policy": "no-referrer"},
    )


async def service_worker(request):
    return FileResponse(
        ROOT / "static/v5/sw.js",
        media_type="application/javascript",
        headers={"Cache-Control": "no-cache", "Service-Worker-Allowed": "/"},
    )


async def health(request):
    return PlainTextResponse("ok", headers={"Cache-Control": "no-store"})


async def asset_links(request):
    return FileResponse(
        ROOT / "static/well-known/assetlinks.json", media_type="application/json"
    )


async def widget(request):
    try:
        value = await run_in_threadpool(snapshot_for, request.path_params["station_id"])
    except ValueError:
        return JSONResponse({"error": "Località non disponibile"}, status_code=404)
    except Exception:  # noqa: BLE001 - no database/provider details
        return JSONResponse({"error": "Dati non disponibili"}, status_code=503)
    current = value.get("current", {})
    return JSONResponse(
        {
            "station_id": value["station"]["id"],
            "name": value["station"]["name"],
            "generated_at": value.get("generated_at"),
            "observed_at": value.get("observed_at"),
            **{
                k: current.get(k)
                for k in ("temp_c", "humidity", "wind_kmh", "pressure_hpa")
            },
        },
        headers={"Cache-Control": "private, no-cache"},
    )


async def export_history(request):
    try:
        data = await run_in_threadpool(snapshot_for, request.path_params["station_id"])
    except (ValueError, KeyError):
        return JSONResponse({"error": "Località non disponibile"}, status_code=404)
    buffer = io.StringIO()
    fields = [
        "date",
        "status",
        "samples",
        "expected",
        "temp_min_c",
        "temp_max_c",
        "temp_mean_c",
        "humidity_mean",
        "rain_mm",
    ]
    writer = csv.DictWriter(buffer, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(data["history"]["calendar"])
    return Response(
        buffer.getvalue().encode("utf-8-sig"),
        media_type="text/csv",
        headers={
            "Content-Disposition": 'attachment; filename="meteo-pro-storico.csv"',
            "Cache-Control": "no-store",
        },
    )


def admin_allowed(request) -> bool:
    token = (os.getenv("ADMIN_ACCESS_TOKEN") or os.getenv("ADMIN_TOKEN") or "").strip()
    supplied = request.headers.get("authorization", "").removeprefix("Bearer ")
    return bool(token) and bool(supplied) and hmac.compare_digest(token, supplied)


def origin_matches(request) -> bool:
    """Keep host/port exact, including HTTPS terminated by Render's proxy."""
    try:
        origin = urlsplit(request.headers.get("origin", ""))
        target = urlsplit(str(request.base_url))
        return (
            bool(origin.netloc)
            and origin.netloc == target.netloc
            and origin.scheme in {target.scheme, "https"}
            and not origin.path
            and not origin.query
            and not origin.fragment
        )
    except ValueError:
        return False


async def import_history(request):
    if not admin_allowed(request):
        return JSONResponse(
            {"error": "Accesso amministratore richiesto"},
            status_code=401,
            headers={"Cache-Control": "no-store"},
        )
    origin = request.headers.get("origin")
    if origin and not origin_matches(request):
        return JSONResponse({"error": "Origine non consentita"}, status_code=403)
    content_length = request.headers.get("content-length", "0")
    if not content_length.isdigit() or int(content_length) > 8_000_000:
        return JSONResponse(
            {"error": "File troppo grande (massimo 8 MB)"}, status_code=413
        )
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > 8_000_000:
            return JSONResponse({"error": "File troppo grande"}, status_code=413)
    station_id = request.path_params["station_id"]
    try:
        from station_daily import parse_ecowitt_daily_export, upsert_daily_summaries
        from v5_data import records, station_settings

        await run_in_threadpool(station_settings, station_id)
        # Bytes never enter the repository, logs, public snapshots or disk.
        with zipfile.ZipFile(io.BytesIO(body)) as archive:
            if (
                len(archive.infolist()) > 2000
                or sum(item.file_size for item in archive.infolist()) > 64_000_000
            ):
                raise ValueError("Archivio troppo grande")
        frame = await run_in_threadpool(parse_ecowitt_daily_export, io.BytesIO(body))
        if len(frame) > 5000 or frame.empty:
            raise ValueError("Numero di giornate non valido")
        digest = hashlib.sha256(body).hexdigest()
        confirmed = request.query_params.get("confirm")
        if confirmed and not hmac.compare_digest(confirmed, digest):
            raise ValueError("Il file è cambiato dopo l’anteprima")
        written = (
            await run_in_threadpool(upsert_daily_summaries, frame, station_id)
            if confirmed
            else 0
        )
        if confirmed:
            # Data appear at the next ordinary publication, not a hidden refresh.
            message = "Importato; sarà visibile al prossimo ciclo di 10 minuti."
        else:
            message = "Anteprima: nessuna modifica eseguita."
        return JSONResponse(
            {
                "sha256": digest,
                "days": len(frame),
                "first": str(frame.local_date.min()),
                "last": str(frame.local_date.max()),
                "preview": records(frame.head(10)),
                "written": written,
                "message": message,
            },
            headers={"Cache-Control": "no-store"},
        )
    except Exception:  # noqa: BLE001 - import details can contain private workbook data
        return JSONResponse(
            {
                "error": "Export giornaliero Ecowitt non valido o importazione non disponibile."
            },
            status_code=400,
            headers={"Cache-Control": "no-store"},
        )


async def push_key(request):
    from v5_push import push_keys

    key = await run_in_threadpool(push_keys)
    return JSONResponse(
        {"public_key": key["public_key"] if key else None},
        headers={"Cache-Control": "no-store"},
    )


async def push_subscription(request):
    from v5_push import delete_subscription, save_subscription

    headers = {"Cache-Control": "no-store"}
    if not origin_matches(request):
        return JSONResponse(
            {"error": "Origine non consentita"}, status_code=403, headers=headers
        )
    if request.headers.get("content-type", "").split(";")[0] != "application/json":
        return JSONResponse(
            {"error": "Formato non valido"}, status_code=415, headers=headers
        )
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > 8192:
            return JSONResponse(
                {"error": "Richiesta troppo grande"}, status_code=413, headers=headers
            )
    try:
        value = json.loads(body)
        if not isinstance(value, dict):
            raise TypeError("Richiesta non valida")
        token = request.headers.get("authorization", "").removeprefix("Bearer ")
        if request.method == "DELETE":
            identifier = value.get("id")
            if not isinstance(identifier, str) or not re.fullmatch(
                r"[a-f0-9]{64}", identifier
            ):
                raise ValueError("Sottoscrizione non valida")
            await run_in_threadpool(delete_subscription, identifier, token)
            return JSONResponse({"deleted": True}, headers=headers)
        result = await run_in_threadpool(save_subscription, value, token)
        return JSONResponse(result, headers=headers)
    except PermissionError:
        return JSONResponse(
            {
                "error": "Autorizzazione dispositivo richiesta. Disattiva il push nel browser, poi riattivalo."
            },
            status_code=403,
            headers=headers,
        )
    except (ValueError, TypeError):
        return JSONResponse(
            {"error": "Sottoscrizione o preferenze non valide"},
            status_code=400,
            headers=headers,
        )


async def extra_tool(request):
    from v5_extensions import catalog, city_forecast, planner, search_city

    headers = {"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"}
    try:
        action = request.path_params["action"]
        if request.method == "GET" and action == "catalog":
            return JSONResponse({"targets": catalog()}, headers=headers)
        if request.method == "GET" and action == "cities":
            value = await run_in_threadpool(
                search_city, request.query_params.get("q", "")
            )
            return JSONResponse({"results": value}, headers=headers)
        if request.method == "GET" and action == "city":
            value = await run_in_threadpool(
                city_forecast, request.query_params.get("id", "")
            )
            return JSONResponse(value, headers=headers)
        if request.method == "POST" and action == "planner":
            if not origin_matches(request):
                return JSONResponse(
                    {"error": "Origine non consentita"},
                    status_code=403,
                    headers=headers,
                )
            if (
                request.headers.get("content-type", "").split(";")[0]
                != "application/json"
            ):
                raise ValueError("Formato JSON richiesto")
            body = bytearray()
            async for chunk in request.stream():
                body.extend(chunk)
                if len(body) > 16384:
                    return JSONResponse(
                        {"error": "Richiesta troppo grande"},
                        status_code=413,
                        headers=headers,
                    )
            values = json.loads(body)
            value = await run_in_threadpool(planner, values["station_id"], values)
            return JSONResponse(value, headers=headers)
        return JSONResponse(
            {"error": "Strumento non disponibile"}, status_code=404, headers=headers
        )
    except (ValueError, TypeError, KeyError, OverflowError):
        return JSONResponse(
            {
                "error": "Parametri non validi. Verifica selezione, date e valori; per la città ripeti la ricerca."
            },
            status_code=400,
            headers=headers,
        )
    except Exception:  # noqa: BLE001 - never expose coordinates or upstream URLs
        return JSONResponse(
            {
                "error": "Fonte temporaneamente non disponibile. I dati salvati restano datati."
            },
            status_code=503,
            headers=headers,
        )


def public_routes():
    return [
        Route("/", home),
        Route("/sw.js", service_worker),
        Route("/_stcore/health", health),
        Route("/.well-known/assetlinks.json", asset_links),
        Route("/api/v5/widget/{station_id}", widget),
        Route("/api/v5/stations", stations),
        Route("/api/v5/tools/{action}", extra_tool, methods=["GET", "POST"]),
        Route("/api/v5/snapshot/{station_id}", snapshot),
        Route("/api/v5/history/{station_id}.csv", export_history),
        Route("/api/v5/import/{station_id}", import_history, methods=["POST"]),
        Route("/api/v5/push/key", push_key),
        Route(
            "/api/v5/push/subscription", push_subscription, methods=["POST", "DELETE"]
        ),
        Mount("/assets/v5", StaticFiles(directory=ROOT / "static/v5")),
        Mount("/app/static", StaticFiles(directory=ROOT / "static")),
    ]
