package com.gio9772rm.meteopro;

import android.net.Uri;
import java.util.Arrays;

/** Keeps navigation in the public app origin, including widget shortcuts. */
public class LauncherActivity extends com.google.androidbrowserhelper.trusted.LauncherActivity {
    static final String ORIGIN = "https://weather-app-v3-w2jd.onrender.com";
    @Override protected void onCreate(android.os.Bundle state) {
        UpdateJob.schedule(this);
        super.onCreate(state);
    }
    @Override protected Uri getLaunchingUrl() {
        Uri input = getIntent().getData();
        Uri.Builder url = Uri.parse(ORIGIN + "/").buildUpon();
        if (input != null && "https".equals(input.getScheme()) && "weather-app-v3-w2jd.onrender.com".equals(input.getHost())) {
            String page = input.getQueryParameter("page");
            String station = input.getQueryParameter("station");
            if (page != null && Arrays.asList("today","forecast","stations","astronomy","maps","cities","planner","journal","activities","inbox","notifications","more").contains(page)) url.appendQueryParameter("page", page);
            if ("roma-primary".equals(station) || "comacchio-secondary".equals(station)) url.appendQueryParameter("station", station);
        }
        return url.appendQueryParameter("src","twa").appendQueryParameter("shell", String.valueOf(BuildConfig.VERSION_CODE))
                .appendQueryParameter("app", BuildConfig.APPLICATION_ID).build();
    }
}
