import hashlib
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

MODULE = importlib.util.spec_from_file_location("android_release", Path(__file__).parents[1] / "android/release_tools.py")
release_tools = importlib.util.module_from_spec(MODULE)
MODULE.loader.exec_module(release_tools)


@pytest.fixture
def release(tmp_path, monkeypatch):
    (tmp_path / "android").mkdir()
    (tmp_path / "static").mkdir()
    config = json.loads((Path(__file__).parents[1] / "android/release.json").read_text())
    (tmp_path / "android/release.json").write_text(json.dumps(config))
    apk = tmp_path / "signed.apk"
    apk.write_bytes(b"fixture" * 1024)

    def run(args, **kwargs):
        if args[0] == "apksigner":
            return SimpleNamespace(stdout="Signer #1 certificate SHA-256 digest: " + config["certificateSha256"].replace(":", "").lower())
        return SimpleNamespace(stdout=f"package: name='{config['applicationId']}' versionCode='{config['versionCode']}' versionName='{config['versionName']}'\nsdkVersion:'23'")

    monkeypatch.setattr(release_tools.subprocess, "run", run)
    return tmp_path, apk, config


def test_android_publication_checks_identity_and_preserves_releases(release):
    root, apk, config = release
    manifest = release_tools.prepare(apk, Path("apksigner"), Path("aapt"), root)
    assert manifest["sha256"] == hashlib.sha256(apk.read_bytes()).hexdigest()
    assert (root / f"static/android/MeteoPro-{config['versionName']}.apk").read_bytes() == apk.read_bytes()
    with pytest.raises(ValueError, match="increase versionCode"):
        release_tools.prepare(apk, Path("apksigner"), Path("aapt"), root)


def test_android_wrong_signature_never_creates_public_files(release, monkeypatch):
    root, apk, _ = release
    monkeypatch.setattr(release_tools.subprocess, "run", lambda *a, **k: SimpleNamespace(stdout="Signer #1 certificate SHA-256 digest: " + "f" * 64))
    with pytest.raises(ValueError, match="production signature"):
        release_tools.prepare(apk, Path("apksigner"), Path("aapt"), root)
    assert not (root / "static/android").exists()


def test_published_android_manifest_matches_apk_when_present():
    root = Path(__file__).parents[1]
    path = root / "static/android/manifest.json"
    if not path.exists():
        pytest.skip("First signed release has not been published")
    manifest = json.loads(path.read_text())
    config = json.loads((root / "android/release.json").read_text())
    assert manifest["versionCode"] <= config["versionCode"]
    assert manifest["applicationId"] == "com.gio9772rm.meteopro"
    apk = root / f"static/android/MeteoPro-{manifest['versionName']}.apk"
    assert apk.stat().st_size == manifest["sizeBytes"]
    assert hashlib.sha256(apk.read_bytes()).hexdigest() == manifest["sha256"]
