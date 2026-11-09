from sqlalchemy import text

import v53_google
from tests.test_v53 import client, register, write

ORIGIN = {"origin": "https://testserver"}
CLIENT_ID = "123456789012-unit-test.apps.googleusercontent.com"
CREDENTIAL = "credential-for-unit-test"


def post(c, action, payload=None, headers=ORIGIN):
    return c.post("/api/v5/personal/" + action, json=payload or {}, headers=headers)


def prepare(c, mode):
    r = post(c, "google-start", {"mode": mode})
    assert r.status_code == 200, r.text
    assert "no-store" in r.headers["cache-control"]
    assert "HttpOnly" in r.headers["set-cookie"]
    assert "Secure" in r.headers["set-cookie"]
    assert r.json()["client_id"] == CLIENT_ID
    return r.json()["nonce"]


def test_google_links_existing_profile_without_merging_by_email(
    sqlite_engine, monkeypatch
):
    monkeypatch.delenv("GOOGLE_CLIENT_ID", raising=False)
    a, b = client(), client()
    assert a.get("/api/v5/personal/google-config").json() == {"enabled": False}
    assert post(a, "google-start", {"mode": "signin"}).status_code == 401
    monkeypatch.setenv("GOOGLE_CLIENT_ID", CLIENT_ID)
    assert b.get("/api/v5/personal/google-config").json() == {"enabled": True}
    account = register(a)
    profile = {
        "identity": {"name": "Osservatore", "note": "Privato", "accent": "violet"}
    }
    assert write(a, account, profile).status_code == 200
    other = register(b, "second-google-test")
    assert post(a, "google-start", {"mode": "link"}, {}).status_code == 403
    claims = {
        "sub": "test-subject-first",
        "email_verified": True,
        "email": "observer@example.test",
        "nonce": "wrong-nonce",
    }

    def verify(token, audience):
        assert token == CREDENTIAL and audience == CLIENT_ID
        return claims.copy()

    monkeypatch.setattr(v53_google, "_verify", verify)
    nonce = prepare(a, "link")
    challenge = a.cookies.get(v53_google.CHALLENGE_COOKIE)
    assert post(a, "google", {"credential": CREDENTIAL}).status_code == 401
    claims["nonce"] = nonce
    linked = post(a, "google", {"credential": CREDENTIAL})
    assert linked.status_code == 200, linked.text
    assert linked.json()["account"]["id"] == account["account"]["id"]
    assert linked.json()["account"]["auth_method"] == "password"
    assert linked.json()["account"]["google"]["email"] == claims["email"]
    a.cookies.set(v53_google.CHALLENGE_COOKIE, challenge, path="/")
    assert post(a, "google", {"credential": CREDENTIAL}).status_code == 401
    a.cookies.delete(v53_google.CHALLENGE_COOKIE)

    claims["nonce"] = prepare(b, "link")
    assert post(b, "google", {"credential": CREDENTIAL}).status_code == 409
    assert (
        b.get("/api/v5/personal/profile").json()["account"]["id"]
        == other["account"]["id"]
    )

    assert post(a, "logout").status_code == 200
    claims["nonce"] = prepare(a, "signin")
    result = post(a, "google", {"credential": CREDENTIAL})
    assert result.status_code == 200, result.text
    body = a.get("/api/v5/personal/profile").json()
    assert body["account"]["id"] == account["account"]["id"]
    assert body["account"]["auth_method"] == "google"
    assert body["profile"]["identity"]["name"] == "Osservatore"
    with sqlite_engine.connect() as con:
        stored_subject = con.execute(
            text("SELECT subject_hash FROM personal_google_links")
        ).scalar()
        assert len(stored_subject) == 64 and claims["sub"] not in stored_subject


def test_new_google_account_can_be_recovered_and_deleted(sqlite_engine, monkeypatch):
    monkeypatch.setenv("GOOGLE_CLIENT_ID", CLIENT_ID)
    c = client()
    claims = {"sub": "test-subject-new", "nonce": prepare(c, "signin")}
    monkeypatch.setattr(v53_google, "_verify", lambda *_: claims.copy())
    result = post(c, "google", {"credential": CREDENTIAL})
    assert result.status_code == 200, result.text
    body = result.json()
    assert body["account"]["auth_method"] == "google"
    assert body["recovery"] and body["profile"] == {}
    assert post(c, "delete", {"password": body["recovery"]}).status_code == 401
    password = "backup-password-for-google-test"
    recovered = post(
        c,
        "recover",
        {
            "username": body["account"]["username"],
            "recovery": body["recovery"],
            "password": password,
        },
    )
    assert recovered.status_code == 200, recovered.text
    assert recovered.json()["recovery"] != body["recovery"]
    assert recovered.json()["account"]["id"] == body["account"]["id"]
    assert post(c, "delete", {"password": password}).status_code == 200
    assert c.get("/api/v5/personal/profile").status_code == 401
    with sqlite_engine.connect() as con:
        assert (
            con.execute(text("SELECT COUNT(*) FROM personal_google_links")).scalar()
            == 0
        )
