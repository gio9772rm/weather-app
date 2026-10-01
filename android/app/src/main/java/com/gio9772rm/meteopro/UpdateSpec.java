package com.gio9772rm.meteopro;

import org.json.JSONObject;

/** Only a versioned APK on our HTTPS origin is an update candidate. */
final class UpdateSpec {
    static final String ORIGIN = "https://weather-app-v3-w2jd.onrender.com";
    static final String PACKAGE = "com.gio9772rm.meteopro";
    static final long MAX_APK = 64L * 1024 * 1024;
    final int code, minSdk;
    final long size;
    final String version, url, hash, certificate, notes;

    UpdateSpec(JSONObject json) throws Exception {
        if (json.getInt("schema") != 1 || !PACKAGE.equals(json.getString("applicationId")))
            throw new IllegalArgumentException("Canale di aggiornamento non valido");
        code = json.getInt("versionCode");
        minSdk = json.getInt("minSdk");
        size = json.getLong("sizeBytes");
        version = json.getString("versionName");
        url = json.getString("apkUrl");
        hash = json.getString("sha256");
        certificate = json.getString("certificateSha256").replace(":", "");
        notes = json.optString("notes", "");
        if (code <= 0 || minSdk < 23 || minSdk > 100 || size < 1024 || size > MAX_APK
                || !version.matches("[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}")
                || !hash.matches("[a-fA-F0-9]{64}") || !certificate.matches("[a-fA-F0-9]{64}")
                || notes.length() > 4000
                || !url.equals(ORIGIN + "/app/static/android/MeteoPro-" + version + ".apk"))
            throw new IllegalArgumentException("Metadati di aggiornamento non validi");
    }

    boolean newerThan(int installed, int sdk) { return code > installed && sdk >= minSdk; }
}
