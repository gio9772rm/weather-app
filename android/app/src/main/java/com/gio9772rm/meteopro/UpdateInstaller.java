package com.gio9772rm.meteopro;

import android.app.PendingIntent;
import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageInstaller;
import android.os.Build;
import java.io.FileInputStream;
import java.io.OutputStream;

final class UpdateInstaller {
    static void install(Context c, UpdateSpec spec, boolean automatic) throws Exception {
        synchronized (Updater.LOCK) {
            Updater.verify(c, Updater.apk(c), spec);
            PackageInstaller installer = c.getPackageManager().getPackageInstaller();
            for (PackageInstaller.SessionInfo pending : installer.getMySessions()) {
                if (c.getPackageName().equals(pending.getAppPackageName())) {
                    if (automatic) return; // Never replace a pending Android confirmation in the background.
                    installer.abandonSession(pending.getSessionId());
                }
            }
            PackageInstaller.SessionParams params = new PackageInstaller.SessionParams(PackageInstaller.SessionParams.MODE_FULL_INSTALL);
            params.setAppPackageName(c.getPackageName());
            params.setSize(spec.size);
            if (Build.VERSION.SDK_INT >= 31) params.setRequireUserAction(automatic
                    ? PackageInstaller.SessionParams.USER_ACTION_NOT_REQUIRED : PackageInstaller.SessionParams.USER_ACTION_REQUIRED);
            if (Build.VERSION.SDK_INT >= 33) params.setPackageSource(PackageInstaller.PACKAGE_SOURCE_DOWNLOADED_FILE);
            int id = installer.createSession(params);
            boolean committed = false;
            try (PackageInstaller.Session session = installer.openSession(id);
                 FileInputStream input = new FileInputStream(Updater.apk(c))) {
                try (OutputStream out = session.openWrite("base.apk", 0, spec.size)) {
                    byte[] block = new byte[32768]; int count;
                    while ((count = input.read(block)) != -1) out.write(block, 0, count);
                    session.fsync(out);
                }
                Intent callback = new Intent(c, InstallReceiver.class).setAction(c.getPackageName() + ".INSTALL_RESULT");
                int flags = PendingIntent.FLAG_UPDATE_CURRENT;
                if (Build.VERSION.SDK_INT >= 31) flags |= PendingIntent.FLAG_MUTABLE;
                PendingIntent result = PendingIntent.getBroadcast(c, id, callback, flags);
                Updater.prefs(c).edit().putInt("session", id).apply();
                session.commit(result.getIntentSender());
                committed = true;
                Updater.status(c, "Installazione affidata ad Android. Conferma se richiesto.", null);
            } finally { if (!committed) installer.abandonSession(id); }
        }
    }
}
