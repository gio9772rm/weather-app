package com.gio9772rm.meteopro;

import org.json.JSONObject;
import org.junit.Test;
import static org.junit.Assert.*;

public class UpdateSpecTest {
    private JSONObject release() throws Exception {
        return new JSONObject().put("schema", 1).put("applicationId", UpdateSpec.PACKAGE)
                .put("versionCode", 50300).put("versionName", "5.3.0").put("minSdk", 23)
                .put("sizeBytes", 3000000).put("sha256", "a".repeat(64))
                .put("certificateSha256", "b".repeat(64))
                .put("apkUrl", UpdateSpec.ORIGIN + "/app/static/android/MeteoPro-5.3.0.apk");
    }
    @Test public void upgradeOnlyOnSupportedAndroid() throws Exception {
        UpdateSpec spec = new UpdateSpec(release());
        assertTrue(spec.newerThan(50200, 36));
        assertFalse(spec.newerThan(50300, 36));
        assertFalse(spec.newerThan(50400, 36));
        assertFalse(spec.newerThan(50200, 22));
    }
    @Test public void rejectsExternalOrAmbiguousDownloads() throws Exception {
        for (String url : new String[]{"http://weather-app-v3-w2jd.onrender.com/app/static/android/MeteoPro-5.3.0.apk",
                "https://evil.example/MeteoPro-5.3.0.apk", UpdateSpec.ORIGIN + ".evil.example/app/static/android/MeteoPro-5.3.0.apk",
                UpdateSpec.ORIGIN + "/app/static/android/../MeteoPro-5.3.0.apk",
                UpdateSpec.ORIGIN + "/app/static/android/MeteoPro-5.3.0.apk?redirect=evil"}) {
            JSONObject json = release().put("apkUrl", url);
            assertThrows(Exception.class, () -> new UpdateSpec(json));
        }
    }
    @Test public void rejectsIdentityDigestAndSizeErrors() throws Exception {
        for (JSONObject invalid : new JSONObject[]{release().put("applicationId", "com.gio9772rm.meteov4"),
                release().put("sha256", "bad"), release().put("certificateSha256", ""),
                release().put("sizeBytes", UpdateSpec.MAX_APK + 1), release().put("sizeBytes", 0),
                release().put("schema", 2), release().put("versionName", "../../evil")})
            assertThrows(Exception.class, () -> new UpdateSpec(invalid));
    }
}
