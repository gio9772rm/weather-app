package com.gio9772rm.meteopro;

import android.app.job.JobInfo;
import android.app.job.JobParameters;
import android.app.job.JobScheduler;
import android.app.job.JobService;
import android.content.ComponentName;
import android.content.Context;
import java.util.concurrent.atomic.AtomicBoolean;

public class UpdateJob extends JobService {
    private final java.util.Map<Integer, AtomicBoolean> active = new java.util.concurrent.ConcurrentHashMap<>();
    static void schedule(Context c) {
        JobScheduler scheduler = c.getSystemService(JobScheduler.class);
        if (scheduler == null) return;
        ComponentName component = new ComponentName(c, UpdateJob.class);
        boolean scheduled = false;
        for (JobInfo job : scheduler.getAllPendingJobs()) if (job.getId() == 520) scheduled = true;
        if (!scheduled) scheduler.schedule(new JobInfo.Builder(520, component)
                .setRequiredNetworkType(JobInfo.NETWORK_TYPE_ANY).setPeriodic(Updater.INTERVAL).setPersisted(true).build());
        long now = System.currentTimeMillis(), last = Updater.prefs(c).getLong("attempt", 0);
        if (now < last || now - last >= Updater.INTERVAL) scheduler.schedule(new JobInfo.Builder(521, component)
                .setRequiredNetworkType(JobInfo.NETWORK_TYPE_ANY).build());
    }
    @Override public boolean onStartJob(JobParameters params) {
        AtomicBoolean token = new AtomicBoolean(false); active.put(params.getJobId(), token);
        new Thread(() -> {
            Updater.check(this, false, Updater.prefs(this).getBoolean("auto_download", true), token, null);
            active.remove(params.getJobId(), token);
            if (!token.get()) jobFinished(params, false);
        }, "meteo-updates").start();
        return true;
    }
    @Override public boolean onStopJob(JobParameters params) {
        AtomicBoolean token = active.remove(params.getJobId());
        if (token != null) token.set(true);
        return true;
    }
}
