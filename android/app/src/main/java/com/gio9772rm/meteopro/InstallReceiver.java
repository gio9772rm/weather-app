package com.gio9772rm.meteopro;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageInstaller;
import android.content.pm.PackageManager;
import android.os.Build;

public class InstallReceiver extends BroadcastReceiver {
    @Override public void onReceive(Context c, Intent intent) {
        if (!(c.getPackageName() + ".INSTALL_RESULT").equals(intent.getAction())
                || intent.getIntExtra(PackageInstaller.EXTRA_SESSION_ID, -2) != Updater.prefs(c).getInt("session", -1)) return;
        int status = intent.getIntExtra(PackageInstaller.EXTRA_STATUS, PackageInstaller.STATUS_FAILURE);
        if (status == PackageInstaller.STATUS_PENDING_USER_ACTION) {
            Intent confirm = intent.getParcelableExtra(Intent.EXTRA_INTENT);
            if (confirm == null) return;
            Updater.status(c, "Android richiede la conferma di installazione.", null);
            if (UpdateActivity.showConfirmation(confirm)) return;
            if (Build.VERSION.SDK_INT >= 33 && c.checkSelfPermission(android.Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) return;
            NotificationManager manager = c.getSystemService(NotificationManager.class);
            if (Build.VERSION.SDK_INT >= 26) manager.createNotificationChannel(new NotificationChannel("updates", "Aggiornamenti app", NotificationManager.IMPORTANCE_DEFAULT));
            PendingIntent open = PendingIntent.getActivity(c, 521, confirm, PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
            Notification.Builder notice = Build.VERSION.SDK_INT >= 26 ? new Notification.Builder(c, "updates") : new Notification.Builder(c);
            manager.notify(Updater.NOTIFICATION, notice.setSmallIcon(R.drawable.ic_meteo).setContentTitle("Conferma aggiornamento Meteo Pro")
                    .setContentText("Tocca per proseguire con l’installazione Android.").setContentIntent(open).setAutoCancel(true).build());
        } else {
            Updater.prefs(c).edit().remove("session").apply();
            Updater.status(c, status == PackageInstaller.STATUS_SUCCESS ? "Aggiornamento installato."
                    : "Installazione non completata o annullata. Puoi riprovare dalla schermata Aggiornamenti.", null);
            if (status == PackageInstaller.STATUS_SUCCESS) {
                Updater.apk(c).delete(); c.getSystemService(NotificationManager.class).cancel(Updater.NOTIFICATION);
            }
        }
    }
}
