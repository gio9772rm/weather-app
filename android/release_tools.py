"""Verify a signed APK and atomically prepare the public OTA files. No private keys."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ORIGIN = "https://weather-app-v3-w2jd.onrender.com"


def prepare(apk: Path, apksigner: Path, aapt: Path, root: Path = ROOT):
    release = json.loads((root / "android/release.json").read_text())
    version = release["versionName"]
    if not re.fullmatch(r"\d{1,3}\.\d{1,3}\.\d{1,3}", version):
        raise ValueError("Invalid version")
    if release["applicationId"] != "com.gio9772rm.meteopro":
        raise ValueError("Unexpected Android identity")
    result = subprocess.run(
        [str(apksigner), "verify", "--verbose", "--print-certs", str(apk)],
        check=True, capture_output=True, text=True,
    ).stdout
    fingerprints = re.findall(r"Signer #\d+ certificate SHA-256 digest: ([0-9a-fA-F]+)", result)
    expected = release["certificateSha256"].replace(":", "").lower()
    if fingerprints != [expected]:
        raise ValueError("APK does not have the configured production signature")
    badging = subprocess.run([str(aapt), "dump", "badging", str(apk)], check=True, capture_output=True, text=True).stdout
    package = re.search(r"package: name='([^']+)' versionCode='(\d+)' versionName='([^']+)'", badging)
    sdk = re.search(r"sdkVersion:'(\d+)'", badging)
    if not package or (package[1], int(package[2]), package[3]) != (release["applicationId"], release["versionCode"], version):
        raise ValueError("APK package or version differs from release.json")
    if not sdk or int(sdk[1]) != release["minSdk"]:
        raise ValueError("Minimum Android differs from release.json")
    output = root / "static/android"
    output.mkdir(exist_ok=True)
    manifest_path = output / "manifest.json"
    if manifest_path.exists():
        previous = json.loads(manifest_path.read_text())
        if previous["versionCode"] >= release["versionCode"]:
            raise ValueError("A release must increase versionCode; published APKs are immutable")
    body = apk.read_bytes()
    if not 1024 <= len(body) <= 64 * 1024 * 1024:
        raise ValueError("Unexpected APK size")
    filename = f"MeteoPro-{version}.apk"
    target = output / filename
    if target.exists():
        raise ValueError("Published APK already exists")
    shutil.copyfile(apk, target)
    manifest = {"schema": 1, **release, "apkUrl": f"{ORIGIN}/app/static/android/{filename}",
                "sizeBytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apk", type=Path, required=True)
    parser.add_argument("--apksigner", type=Path, required=True)
    parser.add_argument("--aapt", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(prepare(args.apk, args.apksigner, args.aapt), indent=2))
