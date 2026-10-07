"""Private, immutable cold history; failed transfers retain the database copy.

The existing gated cron is the only automatic writer. No object deletion API
is used: historical objects are shared by all three operational backups.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy import text

from db import get_engine

log = logging.getLogger(__name__)
PREFIX = "meteo/history/v1/"
LOCK_KEY = 0x4D4554454F5232
MAX_BYTES = 8_000_000_000


def archive_requested():
    return os.getenv("R2_ARCHIVE_ENABLED", "false").lower() in {"true", "1", "yes"}


@dataclass(frozen=True)
class R2Config:
    endpoint: str
    bucket: str
    access_key: str = field(repr=False)
    secret_key: str = field(repr=False)
    max_bytes: int = MAX_BYTES

    @classmethod
    def from_env(cls):
        endpoint = os.getenv("R2_ENDPOINT", "").strip().rstrip("/")
        bucket = os.getenv("R2_BUCKET", "").strip()
        access = os.getenv("R2_ACCESS_KEY_ID", "").strip()
        secret = os.getenv("R2_SECRET_ACCESS_KEY", "").strip()
        if not all((endpoint, bucket, access, secret)):
            raise ValueError("R2 configuration incomplete")
        if not re.fullmatch(
            r"https://[a-f0-9]{32}(?:\.eu|\.us|\.fedramp)?\.r2\.cloudflarestorage\.com",
            endpoint,
        ) or not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]", bucket):
            raise ValueError("Invalid R2 endpoint or bucket")
        # A user may lower the budget, but cannot accidentally remove the
        # two-GB reserve within the account's ten-GB Standard storage allowance.
        maximum = int(os.getenv("R2_ARCHIVE_MAX_BYTES", str(MAX_BYTES)))
        if not 0 < maximum <= MAX_BYTES:
            raise ValueError("R2 budget must be at most eight GB")
        return cls(endpoint, bucket, access, secret, maximum)

    @property
    def identity(self):
        return hashlib.sha256((self.endpoint + "/" + self.bucket).encode()).hexdigest()


class R2Store:
    def __init__(self, cfg):
        # Imported only when the archive is configured, never on weather reads.
        import boto3
        from botocore.config import Config

        self.cfg = cfg
        self.client = boto3.client(
            "s3",
            endpoint_url=cfg.endpoint,
            aws_access_key_id=cfg.access_key,
            aws_secret_access_key=cfg.secret_key,
            region_name="auto",
            config=Config(
                signature_version="s3v4",
                connect_timeout=3,
                read_timeout=5,
                retries={"total_max_attempts": 1},
                s3={"addressing_style": "path"},
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )

    def inventory(self, deadline):
        """Count every object, including orphan uploads and other bucket files."""
        used, token = 0, None
        while True:
            if time.monotonic() >= deadline:
                raise TimeoutError("R2 inventory deferred")
            args = {"Bucket": self.cfg.bucket, "MaxKeys": 1000}
            if token:
                args["ContinuationToken"] = token
            page = self.client.list_objects_v2(**args)
            used += sum(item["Size"] for item in page.get("Contents", []))
            if not page.get("IsTruncated"):
                return used
            next_token = page.get("NextContinuationToken")
            if not next_token or next_token == token:
                raise ValueError("Incomplete R2 inventory")
            token = next_token

    def exists(self, key):
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.cfg.bucket, Key=key)
            return True
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in {
                "404",
                "NoSuchKey",
                "NotFound",
            }:
                raise
            return False

    def put_new(self, key, data):
        from botocore.exceptions import ClientError

        try:
            self.client.put_object(
                Bucket=self.cfg.bucket,
                Key=key,
                Body=data,
                ContentType="application/zlib",
                IfNoneMatch="*",
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in {
                "PreconditionFailed",
                "412",
            }:
                raise
            # Existing immutable objects still require a complete read/check.

    def get(self, key, size):
        response = self.client.get_object(Bucket=self.cfg.bucket, Key=key)
        body = response["Body"]
        try:
            data = body.read(size + 1)
        finally:
            body.close()
        if response.get("ContentLength") != size or len(data) != size:
            raise ValueError("R2 object length mismatch")
        return data


def _key(block, data):
    return (
        PREFIX
        + block["source_table"]
        + "/"
        + block["sha256"]
        + "-"
        + hashlib.sha256(data).hexdigest()
        + ".zlib"
    )


def validate_reference(block):
    from compact_history import CODEC, LEGACY_CODEC, TARGETS

    table, checksum = block["source_table"], block["sha256"]
    if (
        table not in TARGETS
        or block["codec"] not in {CODEC, LEGACY_CODEC}
        or not re.fullmatch(r"[a-f0-9]{64}", checksum)
        or block["archive_key"] != table + ":" + checksum
        or not re.fullmatch(
            re.escape(PREFIX + table + "/" + checksum + "-") + r"[a-f0-9]{64}\.zlib",
            block.get("object_key") or "",
        )
        or not re.fullmatch(r"[a-f0-9]{64}", block.get("object_store") or "")
        or not 0 < int(block.get("object_bytes") or 0) <= 32 * 1024 * 1024
        or int(block["row_count"]) <= 0
    ):
        raise ValueError("Invalid external history reference")


def resolve_payload(block, *, store=None):
    """Never return partial/unverified history when an object is unavailable."""
    from compact_history import decode_block

    if block.get("payload"):
        return block["payload"]
    validate_reference(block)
    store = store or R2Store(R2Config.from_env())
    if block["object_store"] != store.cfg.identity:
        raise ValueError("R2 destination differs from the archive")
    data = store.get(block["object_key"], int(block["object_bytes"]))
    if _key(block, data) != block["object_key"]:
        raise ValueError("R2 object hash mismatch")
    payload = base64.b64encode(data).decode("ascii")
    decode_block(payload, block["sha256"], int(block["row_count"]), block["codec"])
    return payload


def _status(con, state):
    con.execute(
        text(
            "INSERT INTO meta(k,v) VALUES('r2_status',:v) ON CONFLICT(k) DO UPDATE SET v=excluded.v"
        ),
        {
            "v": json.dumps(
                {**state, "checked_at": datetime.now(timezone.utc).isoformat()}
            )
        },
    )
    con.commit()


def _save_usage(con, usage):
    con.execute(
        text(
            "INSERT INTO meta(k,v) VALUES('r2_usage',:v) ON CONFLICT(k) DO UPDATE SET v=excluded.v"
        ),
        {"v": json.dumps(usage)},
    )
    con.commit()


def _usage(con, store, deadline):
    """Reconcile every six hours; persist reservations before remote writes.

    A failed PUT may conservatively overcount until reconciliation. An upload
    followed by a failed DB commit remains counted. This avoids repeatedly
    listing years of objects, keeping operation usage within a modest budget.
    The bucket must be dedicated to this application's single gated writer.
    """
    value = con.execute(text("SELECT v FROM meta WHERE k='r2_usage'")).scalar()
    con.commit()
    usage = json.loads(value) if value else None
    now = datetime.now(timezone.utc)
    if usage and usage.get("store") == store.cfg.identity:
        age = (now - datetime.fromisoformat(usage["inventoried_at"])).total_seconds()
        if (
            0 <= age < 6 * 3600
            and isinstance(usage["bytes"], int)
            and usage["bytes"] >= 0
        ):
            return usage
    usage = {
        "store": store.cfg.identity,
        "bytes": store.inventory(deadline),
        "inventoried_at": now.isoformat(),
    }
    _save_usage(con, usage)
    return usage


def offload_history(*, engine=None, store=None, max_blocks=10, seconds=20):
    """Bounded, resumable move after PUT, GET and full codec validation."""
    if store is None and not archive_requested():
        return {"state": "disabled", "blocks": 0}
    engine = engine or get_engine()
    state = {"state": "deferred", "blocks": 0}
    with engine.connect() as con:
        postgres = con.dialect.name == "postgresql"
        locked = False
        try:
            if postgres:
                locked = bool(
                    con.execute(
                        text("SELECT pg_try_advisory_lock(:k)"), {"k": LOCK_KEY}
                    ).scalar()
                )
                con.commit()
                if not locked:
                    return {"state": "busy", "blocks": 0}
            store = store or R2Store(R2Config.from_env())
            identities = (
                con.execute(
                    text(
                        "SELECT DISTINCT object_store FROM compact_archives WHERE object_key IS NOT NULL"
                    )
                )
                .scalars()
                .all()
            )
            con.commit()
            if any(value != store.cfg.identity for value in identities):
                raise ValueError("R2 destination differs from the archive")
            deadline = time.monotonic() + seconds
            usage = _usage(con, store, deadline)
            used = usage["bytes"]
            state.update(
                state="online",
                used_bytes=used,
                max_bytes=store.cfg.max_bytes,
                inventoried_at=usage["inventoried_at"],
            )
            for _ in range(max_blocks):
                if time.monotonic() >= deadline:
                    break
                block = (
                    con.execute(
                        text(
                            "SELECT * FROM compact_archives WHERE payload<>'' ORDER BY archived_at,archive_key LIMIT 1"
                        )
                    )
                    .mappings()
                    .first()
                )
                con.commit()
                if not block:
                    break
                from compact_history import decode_block

                decode_block(
                    block["payload"],
                    block["sha256"],
                    block["row_count"],
                    block["codec"],
                )
                data = base64.b64decode(block["payload"], validate=True)
                key = _key(block, data)
                if len(data) > 32 * 1024 * 1024:
                    raise ValueError("History block exceeds transfer limit")
                exists = store.exists(key)
                if not exists and used + len(data) > store.cfg.max_bytes:
                    state["state"] = "capacity"
                    break
                if not exists:
                    used += len(data)
                    usage["bytes"] = used
                    _save_usage(con, usage)
                    state["used_bytes"] = used
                    store.put_new(key, data)
                remote = {
                    **block,
                    "payload": "",
                    "object_key": key,
                    "object_bytes": len(data),
                    "object_store": store.cfg.identity,
                }
                if resolve_payload(remote, store=store) != block["payload"]:
                    raise ValueError("R2 round trip mismatch")
                # If this commit fails, the local payload remains; next cycle
                # inventories the orphan and reuses the same immutable object.
                changed = con.execute(
                    text(
                        "UPDATE compact_archives SET payload='',object_key=:key,object_bytes=:n,object_store=:id WHERE archive_key=:archive AND payload=:payload"
                    ),
                    {
                        "key": key,
                        "n": len(data),
                        "id": store.cfg.identity,
                        "archive": block["archive_key"],
                        "payload": block["payload"],
                    },
                ).rowcount
                con.execute(
                    text(
                        "INSERT INTO meta(k,v) VALUES('r2_destination',:v) ON CONFLICT(k) DO UPDATE SET v=excluded.v"
                    ),
                    {
                        "v": json.dumps(
                            {"endpoint": store.cfg.endpoint, "bucket": store.cfg.bucket}
                        )
                    },
                )
                con.commit()
                state["blocks"] += changed
                state["used_bytes"] = used
            _status(con, state)
        except Exception as exc:  # noqa: BLE001 - independent from weather publication
            con.rollback()
            state.update(state="deferred", error=type(exc).__name__)
            log.warning("Archivio R2 rinviato (%s)", type(exc).__name__)
            _status(con, state)
        finally:
            if locked:
                con.rollback()
                con.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": LOCK_KEY})
                con.commit()
    from source_health import record_source_result

    record_source_result(
        "r2_history",
        success=state["state"] == "online",
        rows_received=state["blocks"],
        error="limite di spazio raggiunto"
        if state["state"] == "capacity"
        else "trasferimento rinviato: " + state["error"]
        if state.get("error")
        else None,
        engine=engine,
    )
    return state


def storage_status(*, engine=None):
    """Database-only diagnostics: no cloud requests on a web refresh."""
    engine = engine or get_engine()
    with engine.connect() as con:
        value = con.execute(text("SELECT v FROM meta WHERE k='r2_status'")).scalar()
        counts = con.execute(
            text(
                "SELECT COUNT(*),COALESCE(SUM(object_bytes),0) FROM compact_archives WHERE object_key IS NOT NULL"
            )
        ).one()
        pending = con.execute(
            text("SELECT COUNT(*) FROM compact_archives WHERE payload<>''")
        ).scalar()
    state = (
        json.loads(value)
        if value
        else {"state": "waiting" if archive_requested() else "disabled"}
    )
    if not archive_requested():
        state["state"] = "paused" if counts[0] else "disabled"
    return {
        **state,
        "remote_blocks": counts[0],
        "remote_bytes": counts[1],
        "pending_blocks": pending,
    }
