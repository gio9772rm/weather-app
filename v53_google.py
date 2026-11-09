"""Optional Google Identity Services linked to the existing private account."""

from __future__ import annotations

import hashlib
import hmac
import os
import re
import secrets

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from db import get_engine
from v53_profiles import (
    HASH_LOCK,
    ConflictError,
    authenticate,
    digest,
    issue_session,
    rate_limit,
)

CHALLENGE_COOKIE = "__Host-MeteoGoogle"
MAX_AGE = 600
_TOKEN = re.compile(r"^[A-Za-z0-9_-]{43}$")


def client_id():
    value = os.environ.get("GOOGLE_CLIENT_ID", "").strip()
    return (
        value
        if re.fullmatch(r"[a-zA-Z0-9-]{10,128}\.apps\.googleusercontent\.com", value)
        else ""
    )


def _session(token):
    try:
        return authenticate(token)
    except PermissionError:
        return None


def start_google(mode, session_token, address):
    configured = client_id()
    if not configured:
        raise PermissionError("Accesso Google non ancora configurato")
    account = _session(session_token)
    if mode not in {"signin", "link"} or (mode == "link") != bool(account):
        raise PermissionError("Riapri la scheda Account per continuare")
    rate_limit("google-start:" + address, 24)
    rate_limit("google-start:global", 600)
    challenge, nonce = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    now = pd.Timestamp.now(tz="UTC")
    with get_engine().begin() as con:
        con.execute(
            text("DELETE FROM personal_google_challenges WHERE expires_at<=:now"),
            {"now": now.isoformat()},
        )
        con.execute(
            text(
                "INSERT INTO personal_google_challenges(token_hash,nonce,account_id,session_hash,expires_at) VALUES(:key,:nonce,:id,:session,:end)"
            ),
            {
                "key": digest(challenge),
                "nonce": nonce,
                "id": account["id"] if account else "",
                "session": digest(session_token) if account else "",
                "end": (now + pd.Timedelta(seconds=MAX_AGE)).isoformat(),
            },
        )
    return {"client_id": configured, "nonce": nonce, "max_age": MAX_AGE}, challenge


def _verify(credential, audience):
    from google.auth.exceptions import GoogleAuthError, TransportError
    from google.auth.transport.requests import Request
    from google.oauth2 import id_token

    transport = Request()

    def bounded(url, **kwargs):
        return transport(url, **{**kwargs, "timeout": 8})

    try:
        return id_token.verify_oauth2_token(credential, bounded, audience)
    except TransportError as exc:
        raise RuntimeError("Verifica Google temporaneamente non disponibile") from exc
    except (ValueError, GoogleAuthError) as exc:
        raise PermissionError("Identità Google non verificata") from exc
    finally:
        transport.session.close()


def _subject_key(subject):
    # Keep the provider identifier private. A stable, app-specific salt
    # allows matching the same Google identity without storing its raw ID.
    with HASH_LOCK:
        return hashlib.pbkdf2_hmac(
            "sha256", subject.encode(), b"meteo-pro:google-subject:v1", 600000
        ).hex()


def finish_google(credential, challenge, session_token, address):
    configured = client_id()
    if not configured:
        raise PermissionError("Accesso Google non ancora configurato")
    if (
        not isinstance(credential, str)
        or not 20 <= len(credential) <= 7000
        or not _TOKEN.fullmatch(challenge)
    ):
        raise PermissionError("Richiedi di nuovo l’accesso Google")
    rate_limit("google-finish:" + address, 12)
    rate_limit("google-finish:global", 300)
    key = digest(challenge)
    now = pd.Timestamp.now(tz="UTC").isoformat()
    with get_engine().connect() as con:
        bound = (
            con.execute(
                text(
                    "SELECT nonce,account_id,session_hash FROM personal_google_challenges WHERE token_hash=:key AND expires_at>:now"
                ),
                {"key": key, "now": now},
            )
            .mappings()
            .first()
        )
    account = _session(session_token)
    if (
        not bound
        or bound["account_id"] != (account["id"] if account else "")
        or (
            account
            and not hmac.compare_digest(bound["session_hash"], digest(session_token))
        )
    ):
        raise PermissionError("Sessione Google scaduta: riprova dalla scheda Account")
    claims = _verify(credential, configured)
    nonce, subject = claims.get("nonce"), claims.get("sub")
    if (
        not isinstance(nonce, str)
        or not hmac.compare_digest(nonce.encode(), bound["nonce"].encode())
        or not isinstance(subject, str)
        or not 1 <= len(subject) <= 255
    ):
        raise PermissionError("Identità Google non associata a questa richiesta")
    subject_hash = _subject_key(subject)
    email = (
        str(claims.get("email", ""))[:254]
        if claims.get("email_verified") is True
        else ""
    )
    recovery = None
    new_token = None
    try:
        with get_engine().begin() as con:
            consumed = con.execute(
                text(
                    "DELETE FROM personal_google_challenges WHERE token_hash=:key AND expires_at>:now RETURNING token_hash"
                ),
                {"key": key, "now": pd.Timestamp.now(tz="UTC").isoformat()},
            ).scalar()
            if not consumed:
                raise PermissionError("Richiesta Google scaduta o già usata")
            linked = (
                con.execute(
                    text(
                        "SELECT a.id,a.username FROM personal_google_links g JOIN personal_accounts a ON a.id=g.account_id WHERE g.subject_hash=:subject"
                    ),
                    {"subject": subject_hash},
                )
                .mappings()
                .first()
            )
            if account:
                # A local account must be authenticated when linking. Never merge by email.
                still_active = con.execute(
                    text(
                        "SELECT account_id FROM personal_sessions WHERE token_hash=:token AND expires_at>:now"
                    ),
                    {
                        "token": digest(session_token),
                        "now": pd.Timestamp.now(tz="UTC").isoformat(),
                    },
                ).scalar()
                if still_active != account["id"]:
                    raise PermissionError(
                        "Accedi di nuovo al profilo prima di collegarlo"
                    )
                if linked and linked["id"] != account["id"]:
                    raise ConflictError(
                        "Questo account Google è già collegato a un altro profilo"
                    )
            elif linked:
                account = dict(linked)
                account["auth_method"] = "google"
                new_token = issue_session(account["id"], con, "google")
            else:
                if (
                    con.execute(text("SELECT COUNT(*) FROM personal_accounts")).scalar()
                    >= 200
                ):
                    raise ValueError("Registrazioni temporaneamente sospese")
                account = {
                    "id": secrets.token_hex(16),
                    "username": "google-" + secrets.token_hex(8),
                }
                recovery = secrets.token_urlsafe(32)
                con.execute(
                    text(
                        "INSERT INTO personal_accounts(id,username,password_hash,recovery_hash,created_at) VALUES(:id,:username,'',:recovery,:now)"
                    ),
                    {**account, "recovery": digest(recovery), "now": now},
                )
                con.execute(
                    text(
                        "INSERT INTO personal_profiles(account_id,revision,payload,updated_at) VALUES(:id,0,'{}',:now)"
                    ),
                    {"id": account["id"], "now": now},
                )
                account["auth_method"] = "google"
                new_token = issue_session(account["id"], con, "google")
            con.execute(
                text(
                    "INSERT INTO personal_google_links(subject_hash,account_id,email) VALUES(:subject,:id,:email) ON CONFLICT(subject_hash) DO UPDATE SET email=excluded.email WHERE personal_google_links.account_id=excluded.account_id"
                ),
                {"subject": subject_hash, "id": account["id"], "email": email},
            )
            if (
                con.execute(
                    text(
                        "SELECT account_id FROM personal_google_links WHERE subject_hash=:subject"
                    ),
                    {"subject": subject_hash},
                ).scalar()
                != account["id"]
            ):
                raise ConflictError(
                    "Questo account Google è già collegato a un altro profilo"
                )
    except IntegrityError as exc:
        raise ConflictError("Il profilo è già collegato a un account Google") from exc
    return account, new_token, recovery
