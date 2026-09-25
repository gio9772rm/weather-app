package com.gio9772rm.meteopro;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.content.pm.PackageInfo;
import android.content.pm.PackageManager;
import android.content.pm.Signature;
import android.net.ConnectivityManager;
import android.net.NetworkCapabilities;
import android.os.Build;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.HttpsURLConnection;
import org.json.JSONObject;

final class Updater {
    static final long INTERVAL = 6 * 60 * 60 * 1000L;
    static final Object LOCK = new Object();
    static final int NOTIFICATION = 520;
    interface Progress { void update(String message); }
    static SharedPreferences prefs(Context c) { return c.getSharedPreferences("updates", Context.MODE_PRIVATE); }
    static File apk(Context c) { return new File(c.getFilesDir(), "meteo-update.apk"); }
    static void status(Context c, String text, Progress progress) {
        prefs(c).edit().putString("status", text).apply();
        if (progress != null) progress.update(text);
    }
    static UpdateSpec saved(Context c) {
        try { return new UpdateSpec(new JSONObject(prefs(c).getString("manifest", "{}"))); }
        catch (Exception e) { return null; }
    }
    static boolean unmetered(Context c) {
        ConnectivityManager cm = c.getSystemService(ConnectivityManager.class);
        NetworkCapabilities net = cm == null ? null : cm.getNetworkCapabilities(cm.getActiveNetwork());
        return net != null && net.hasCapability(NetworkCapabilities.NET_CAPABILITY_NOT_METERED);
    }
    static HttpsURLConnection connect(String url) throws Exception {
        HttpsURLConnection con = (HttpsURLConnection) new URL(url).openConnection();
        con.setInstanceFollowRedirects(false);
        con.setConnectTimeout(15000);
        con.setReadTimeout(20000);
        con.setUseCaches(false);
        con.setRequestProperty("Cache-Control", "no-cache");
        con.setRequestProperty("Accept-Encoding", "identity");
        con.setRequestProperty("User-Agent", "MeteoPro/" + BuildConfig.VERSION_NAME);
        return con;
    }
    static void check(Context c, boolean manual, boolean download, AtomicBoolean cancelled, Progress progress) {
        synchronized (LOCK) {
            long now = System.currentTimeMillis(), last = prefs(c).getLong("attempt", 0);
            if (!manual && now >= last && now - last < INTERVAL) return;
            prefs(c).edit().putLong("attempt", now).apply();
            HttpsURLConnection con = null;
            try {
                status(c, "Verifica della versione disponibile…", progress);
                con = connect(UpdateSpec.ORIGIN + "/app/static/android/manifest.json");
                if (con.getResponseCode() != 200) throw new java.io.IOException("Canale non disponibile");
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                try (InputStream in = con.getInputStream()) {
                    byte[] block = new byte[2048]; int n;
                    while ((n = in.read(block)) != -1) {
                        if (cancelled.get() || bytes.size() + n > 32768) throw new java.io.IOException("Controllo interrotto");
                        bytes.write(block, 0, n);
                    }
                }
                String body = bytes.toString(StandardCharsets.UTF_8.name());
                UpdateSpec spec = new UpdateSpec(new JSONObject(body));
                // The endpoint cannot change which signing identity we trust.
                if (!fingerprint(c.getPackageManager().getPackageInfo(c.getPackageName(), flags())).equalsIgnoreCase(spec.certificate))
                    throw new SecurityException("Firma del canale non riconosciuta");
                prefs(c).edit().putString("manifest", body).putLong("checked", now).apply();
                if (spec.code <= BuildConfig.VERSION_CODE) {
                    apk(c).delete();
                    c.getSystemService(NotificationManager.class).cancel(NOTIFICATION);
                    status(c, "L’app è aggiornata (" + BuildConfig.VERSION_NAME + ").", progress);
                    return;
                }
                if (Build.VERSION.SDK_INT < spec.minSdk) {
                    status(c, "La nuova versione richiede un Android più recente. Puoi continuare a usare questa app.", progress);
                    return;
                }
                if (apk(c).exists()) {
                    try { verify(c, apk(c), spec); }
                    catch (Exception e) { apk(c).delete(); }
                }
                boolean networkOK = !prefs(c).getBoolean("wifi_only", true) || unmetered(c);
                if (!apk(c).exists() && download && networkOK) download(c, spec, cancelled, progress);
                if (cancelled.get()) return;
                boolean ready = apk(c).exists();
                status(c, ready ? "Versione " + spec.version + " verificata, pronta da installare."
                        : "Disponibile " + spec.version + (download && !networkOK ? ": in attesa di Wi-Fi o rete non a consumo." : "."), progress);
                notifyUpdate(c, ready);
                if (ready && !manual && prefs(c).getBoolean("auto_install", false)
                        && Build.VERSION.SDK_INT >= 31 && c.getPackageManager().canRequestPackageInstalls())
                    UpdateInstaller.install(c, spec, true);
            } catch (Exception e) {
                if (!cancelled.get()) status(c, "Aggiornamento non completato. Riprova più tardi; la versione attuale resta utilizzabile.", progress);
            } finally { if (con != null) con.disconnect(); }
        }
    }
    private static void download(Context c, UpdateSpec spec, AtomicBoolean cancelled, Progress progress) throws Exception {
        File part = new File(c.getFilesDir(), "meteo-update.part");
        HttpsURLConnection con = connect(spec.url);
        try {
            if (con.getResponseCode() != 200) throw new java.io.IOException("Download non disponibile");
            String header = con.getHeaderField("Content-Length");
            long length = header == null ? -1 : Long.parseLong(header);
            if (length != -1 && length != spec.size) throw new SecurityException("Dimensione inattesa");
            long total = 0; int percent = -1;
            try (InputStream in = con.getInputStream(); FileOutputStream out = new FileOutputStream(part)) {
                byte[] block = new byte[32768]; int n;
                while ((n = in.read(block)) != -1) {
                    total += n;
                    if (cancelled.get() || total > spec.size) throw new java.io.IOException("Download interrotto");
                    out.write(block, 0, n);
                    int next = (int) (total * 100 / spec.size);
                    if (next / 5 != percent / 5 || percent < 0) {
                        percent = next; status(c, "Download aggiornamento: " + next + "%", progress);
                    }
                }
                out.getFD().sync();
            }
            verify(c, part, spec);
            if (!part.renameTo(apk(c))) throw new java.io.IOException("Salvataggio non riuscito");
        } finally { con.disconnect(); part.delete(); }
    }
    @SuppressWarnings("deprecation")
    static int flags() { return Build.VERSION.SDK_INT >= 28 ? PackageManager.GET_SIGNING_CERTIFICATES : PackageManager.GET_SIGNATURES; }
    @SuppressWarnings("deprecation")
    static String fingerprint(PackageInfo info) throws Exception {
        if (info == null) throw new SecurityException("APK non valido");
        Signature[] signatures = Build.VERSION.SDK_INT >= 28 && info.signingInfo != null
                ? info.signingInfo.getApkContentsSigners() : info.signatures;
        if (signatures == null || signatures.length != 1) throw new SecurityException("Firma non valida");
        return hex(MessageDigest.getInstance("SHA-256").digest(signatures[0].toByteArray()));
    }
    static String hex(byte[] data) {
        StringBuilder out = new StringBuilder();
        for (byte b : data) out.append(String.format(Locale.ROOT, "%02x", b & 255));
        return out.toString();
    }
    @SuppressWarnings("deprecation")
    static void verify(Context c, File file, UpdateSpec spec) throws Exception {
        if (!spec.newerThan(BuildConfig.VERSION_CODE, Build.VERSION.SDK_INT) || file.length() != spec.size)
            throw new SecurityException("Versione o dimensione non valida");
        MessageDigest sha = MessageDigest.getInstance("SHA-256");
        try (InputStream in = new FileInputStream(file)) {
            byte[] block = new byte[32768]; int n;
            while ((n = in.read(block)) != -1) sha.update(block, 0, n);
        }
        if (!hex(sha.digest()).equalsIgnoreCase(spec.hash)) throw new SecurityException("Integrità non valida");
        PackageManager pm = c.getPackageManager();
        PackageInfo candidate = pm.getPackageArchiveInfo(file.getAbsolutePath(), flags());
        if (candidate == null || !c.getPackageName().equals(candidate.packageName)
                || candidate.versionCode != spec.code || !spec.version.equals(candidate.versionName)
                || !fingerprint(candidate).equalsIgnoreCase(spec.certificate)
                || !fingerprint(candidate).equals(fingerprint(pm.getPackageInfo(c.getPackageName(), flags()))))
            throw new SecurityException("Identità dell’app non valida");
        // Android's PackageInstaller verifies the cryptographic APK signatures again.
    }
    static void notifyUpdate(Context c, boolean ready) {
        NotificationManager manager = c.getSystemService(NotificationManager.class);
        if (Build.VERSION.SDK_INT >= 33 && c.checkSelfPermission(android.Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) return;
        if (Build.VERSION.SDK_INT >= 26) manager.createNotificationChannel(new NotificationChannel("updates", "Aggiornamenti app", NotificationManager.IMPORTANCE_DEFAULT));
        Intent intent = new Intent(c, UpdateActivity.class);
        PendingIntent open = PendingIntent.getActivity(c, 0, intent, PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
        Notification.Builder builder = Build.VERSION.SDK_INT >= 26 ? new Notification.Builder(c, "updates") : new Notification.Builder(c);
        manager.notify(NOTIFICATION, builder.setSmallIcon(R.drawable.ic_meteo).setContentTitle("Aggiornamento Meteo Pro")
                .setContentText(ready ? "APK verificato. Tocca per installare." : "Una nuova versione è disponibile.")
                .setContentIntent(open).setAutoCancel(true).setOnlyAlertOnce(true).build());
    }
}
