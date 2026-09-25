package com.gio9772rm.meteopro;

import android.app.Activity;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.graphics.Color;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.provider.Settings;
import android.widget.Button;
import android.widget.LinearLayout;
import android.widget.ScrollView;
import android.widget.Switch;
import android.widget.TextView;
import java.lang.ref.WeakReference;
import java.text.DateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

/** Native update controls work even while the website is unavailable. */
public class UpdateActivity extends Activity {
    private static WeakReference<UpdateActivity> visible = new WeakReference<>(null);
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private TextView status, details;
    private Button check, download, install;
    private boolean busy;
    private LinearLayout layout;
    private final android.content.SharedPreferences.OnSharedPreferenceChangeListener changes = (prefs, key) -> runOnUiThread(this::refresh);
    @Override public void onCreate(Bundle state) {
        super.onCreate(state);
        ScrollView scroll = new ScrollView(this);
        layout = new LinearLayout(this); layout.setOrientation(LinearLayout.VERTICAL);
        int pad = (int) (24 * getResources().getDisplayMetrics().density);
        layout.setPadding(pad, pad, pad, pad); layout.setBackgroundColor(Color.rgb(12, 24, 40)); scroll.addView(layout);
        text("Meteo Pro", 30);
        text("Aggiornamenti", 23);
        text("App " + BuildConfig.VERSION_NAME + " · Il sito si aggiorna separatamente, senza reinstallare l’APK.", 16);
        status = text("", 18); details = text("", 15);
        check = button("Controlla aggiornamenti", () -> runCheck(false));
        download = button("Scarica aggiornamento", () -> runCheck(true));
        install = button("Installa aggiornamento", this::installUpdate);
        toggle("Scarica automaticamente le nuove versioni", "auto_download", true);
        toggle("Scarica solo su Wi-Fi o rete non a consumo", "wifi_only", true);
        Switch auto = toggle("Installa automaticamente quando Android lo consente", "auto_install", false);
        auto.setEnabled(Build.VERSION.SDK_INT >= 31);
        text("Il controllo periodico avviene circa ogni 6 ore; Android può rinviarlo per risparmiare batteria. Se serve una conferma, riceverai un avviso. Puoi rimandare l’installazione.", 15);
        button("Consenti installazioni da Meteo Pro", this::allowInstalls);
        button("Abilita avvisi aggiornamenti", () -> {
            if (Build.VERSION.SDK_INT >= 33 && checkSelfPermission(android.Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED)
                requestPermissions(new String[]{android.Manifest.permission.POST_NOTIFICATIONS}, 520);
            else if (Build.VERSION.SDK_INT >= 26) startActivity(new Intent(Settings.ACTION_APP_NOTIFICATION_SETTINGS).putExtra(Settings.EXTRA_APP_PACKAGE, getPackageName()));
            else startActivity(new Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS, Uri.parse("package:" + getPackageName())));
        });
        button("Apri il meteo", () -> { startActivity(new Intent(this, LauncherActivity.class)); finish(); });
        setContentView(scroll);
        UpdateJob.schedule(this); refresh();
    }
    private TextView text(String value, int size) {
        TextView view = new TextView(this); view.setText(value); view.setTextColor(Color.WHITE); view.setTextSize(size);
        view.setPadding(0, 12, 0, 16); layout.addView(view); return view;
    }
    private Button button(String label, Runnable action) {
        Button view = new Button(this); view.setText(label); view.setAllCaps(false); view.setTextColor(Color.rgb(12, 24, 40));
        view.setBackgroundTintList(android.content.res.ColorStateList.valueOf(Color.rgb(193, 225, 250)));
        layout.addView(view); view.setOnClickListener(v -> action.run()); return view;
    }
    private Switch toggle(String label, String key, boolean value) {
        Switch toggle = new Switch(this); toggle.setText(label); toggle.setTextColor(Color.WHITE); toggle.setTextSize(16);
        toggle.setPadding(0, 18, 0, 18); toggle.setChecked(Updater.prefs(this).getBoolean(key, value)); layout.addView(toggle);
        toggle.setOnCheckedChangeListener((view, checked) -> Updater.prefs(this).edit().putBoolean(key, checked).apply());
        return toggle;
    }
    private void refresh() {
        if (status == null) return;
        status.setText(Updater.prefs(this).getString("status", "Pronto per verificare gli aggiornamenti."));
        UpdateSpec spec = Updater.saved(this);
        long checked = Updater.prefs(this).getLong("checked", 0);
        String description = checked == 0 ? "Nessun controllo completato." : "Ultimo controllo: " + DateFormat.getDateTimeInstance().format(new Date(checked));
        if (spec != null) description += "\n\nDisponibile: " + spec.version + "\n" + spec.notes;
        details.setText(description);
        boolean newer = spec != null && spec.newerThan(BuildConfig.VERSION_CODE, Build.VERSION.SDK_INT);
        check.setEnabled(!busy); download.setEnabled(!busy && newer); install.setEnabled(!busy && newer && Updater.apk(this).exists());
    }
    private void runCheck(boolean fetch) {
        if (busy) return;
        busy = true; refresh();
        new Thread(() -> {
            Updater.check(this, true, fetch, cancelled, message -> runOnUiThread(() -> { if (!isDestroyed()) status.setText(message); }));
            runOnUiThread(() -> { busy = false; if (!isDestroyed()) refresh(); });
        }, "meteo-update-manual").start();
    }
    private void allowInstalls() {
        if (Build.VERSION.SDK_INT >= 26) startActivity(new Intent(Settings.ACTION_MANAGE_UNKNOWN_APP_SOURCES, Uri.parse("package:" + getPackageName())));
        else startActivity(new Intent(Settings.ACTION_SECURITY_SETTINGS));
    }
    private void installUpdate() {
        if (Build.VERSION.SDK_INT >= 26 && !getPackageManager().canRequestPackageInstalls()) { allowInstalls(); return; }
        if (busy) return;
        busy = true; refresh();
        new Thread(() -> {
            try { UpdateInstaller.install(this, Updater.saved(this), false); }
            catch (Exception e) { Updater.status(this, "Installazione non avviata. Controlla e scarica nuovamente l’aggiornamento.", null); }
            runOnUiThread(() -> { busy = false; if (!isDestroyed()) refresh(); });
        }, "meteo-install").start();
    }
    static boolean showConfirmation(Intent intent) {
        UpdateActivity activity = visible.get();
        if (activity == null || activity.isFinishing()) return false;
        activity.startActivity(intent); return true;
    }
    @Override protected void onResume() { super.onResume(); visible = new WeakReference<>(this); Updater.prefs(this).registerOnSharedPreferenceChangeListener(changes); refresh(); }
    @Override protected void onPause() { if (visible.get() == this) visible.clear(); Updater.prefs(this).unregisterOnSharedPreferenceChangeListener(changes); super.onPause(); }
    @Override protected void onDestroy() { cancelled.set(true); super.onDestroy(); }
}
