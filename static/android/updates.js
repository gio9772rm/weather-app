"use strict";
(async () => {
  const status = document.querySelector("#status");
  if (/Android/i.test(navigator.userAgent)) document.querySelector("#native").hidden = false;
  try {
    const response = await fetch("/app/static/android/manifest.json", { cache: "no-store" });
    if (!response.ok) throw Error("unavailable");
    const manifest = await response.json();
    const version = manifest.versionName;
    const expected = `https://weather-app-v3-w2jd.onrender.com/app/static/android/MeteoPro-${version}.apk`;
    if (manifest.schema !== 1 || manifest.applicationId !== "com.gio9772rm.meteopro" ||
        !/^\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(version) || manifest.apkUrl !== expected ||
        !/^[a-f0-9]{64}$/i.test(manifest.sha256) || !Number.isSafeInteger(manifest.versionCode)) throw Error("invalid");
    status.textContent = `Versione ${version} · ${(manifest.sizeBytes / 1048576).toLocaleString("it-IT", {maximumFractionDigits: 1})} MB`;
    status.className = "version";
    document.querySelector("#notes").textContent = String(manifest.notes || "").slice(0, 4000);
    const link = document.querySelector("#download");
    link.href = expected; link.hidden = false;
    document.querySelector("#hash").textContent = manifest.sha256;
    document.querySelector("#certificate").textContent = manifest.certificateSha256;
    document.querySelector("#verification").hidden = false;
  } catch {
    status.textContent = "Non è possibile verificare un APK disponibile in questo momento. Riprova più tardi; il sito resta utilizzabile.";
  }
})();
