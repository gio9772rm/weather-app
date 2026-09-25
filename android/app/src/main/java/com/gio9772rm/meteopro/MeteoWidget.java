package com.gio9772rm.meteopro;

import android.app.PendingIntent;
import android.app.job.JobInfo;
import android.app.job.JobScheduler;
import android.appwidget.AppWidgetManager;
import android.appwidget.AppWidgetProvider;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.net.Uri;
import android.widget.RemoteViews;
import org.json.JSONObject;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

public class MeteoWidget extends AppWidgetProvider {
    static final int PERIODIC = 501, ONCE = 502;
    static SharedPreferences prefs(Context context) { return context.getSharedPreferences("meteo_widget", Context.MODE_PRIVATE); }
    static String station(Context context, int id) { return prefs(context).getString("station_" + id,"roma-primary"); }
    static int[] ids(Context context) { return AppWidgetManager.getInstance(context).getAppWidgetIds(new ComponentName(context,MeteoWidget.class)); }
    @Override public void onUpdate(Context context, AppWidgetManager manager, int[] ids) {
        for (int id : ids) render(context,id);
        schedule(context,true); schedule(context,false);
    }
    static void schedule(Context context, boolean periodic) {
        JobScheduler scheduler = context.getSystemService(JobScheduler.class);
        int id = periodic ? PERIODIC : ONCE;
        if (scheduler == null || ids(context).length == 0) return;
        if (periodic) for (JobInfo job : scheduler.getAllPendingJobs()) if (job.getId() == id) return;
        JobInfo.Builder builder = new JobInfo.Builder(id,new ComponentName(context,WidgetJob.class)).setRequiredNetworkType(JobInfo.NETWORK_TYPE_ANY);
        if (periodic) builder.setPeriodic(1800000L).setPersisted(true);
        scheduler.schedule(builder.build());
    }
    @Override public void onReceive(Context context, Intent intent) {
        super.onReceive(context,intent);
        String action = intent.getAction();
        int id = intent.getIntExtra(AppWidgetManager.EXTRA_APPWIDGET_ID,-1);
        if (id < 0) return;
        if ((context.getPackageName()+".SWITCH").equals(action)) {
            String next = "roma-primary".equals(station(context,id)) ? "comacchio-secondary" : "roma-primary";
            prefs(context).edit().putString("station_"+id,next).apply(); render(context,id); schedule(context,false);
        } else if ((context.getPackageName()+".REFRESH").equals(action)) schedule(context,false);
    }
    @Override public void onDeleted(Context context, int[] ids) { for (int id:ids) prefs(context).edit().remove("station_"+id).apply(); }
    @Override public void onDisabled(Context context) {
        JobScheduler scheduler=context.getSystemService(JobScheduler.class);
        if(scheduler!=null){scheduler.cancel(PERIODIC);scheduler.cancel(ONCE);}
    }
    static String value(JSONObject json, String key) { double v=json.optDouble(key,Double.NaN); return (!Double.isNaN(v) && !Double.isInfinite(v))?String.format(Locale.ITALY,"%.1f",v):"—"; }
    static void render(Context context, int id) {
        String station=station(context,id);
        RemoteViews views=new RemoteViews(context.getPackageName(),R.layout.meteo_widget);
        views.setTextViewText(R.id.widget_station,"roma-primary".equals(station)?"Meteo Pro · Roma":"Meteo Pro · Comacchio");
        try {
            JSONObject json=new JSONObject(prefs(context).getString("data_"+station,"{}"));
            views.setTextViewText(R.id.widget_temp,value(json,"temp_c")+" °C");
            views.setTextViewText(R.id.widget_details,"UR "+value(json,"humidity")+"% · vento "+value(json,"wind_kmh")+" km/h");
            String raw=json.optString("observed_at","");
            String date="Misura non disponibile";
            if(!raw.isEmpty()) {
                SimpleDateFormat parser=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss",Locale.US);parser.setTimeZone(TimeZone.getTimeZone("UTC"));
                SimpleDateFormat display=new SimpleDateFormat("dd/MM HH:mm",Locale.ITALY);display.setTimeZone(TimeZone.getTimeZone("Europe/Rome"));
                java.util.Date parsed=parser.parse(raw);
                if(parsed!=null) date="Misura del "+display.format(parsed)+" · ora italiana";
            }
            views.setTextViewText(R.id.widget_time,date);
        } catch(Exception ignored) { views.setTextViewText(R.id.widget_time,"Dati salvati non disponibili · apri l’app"); }
        int flags=PendingIntent.FLAG_UPDATE_CURRENT|PendingIntent.FLAG_IMMUTABLE;
        Intent open=new Intent(context,LauncherActivity.class).setData(Uri.parse(LauncherActivity.ORIGIN+"/?station="+station));
        views.setOnClickPendingIntent(R.id.widget_root,PendingIntent.getActivity(context,id,open,flags));
        for(String action:new String[]{"SWITCH","REFRESH"}) {
            Intent event=new Intent(context,MeteoWidget.class).setAction(context.getPackageName()+"."+action).putExtra(AppWidgetManager.EXTRA_APPWIDGET_ID,id);
            views.setOnClickPendingIntent(action.equals("SWITCH")?R.id.widget_switch:R.id.widget_refresh,PendingIntent.getBroadcast(context,id,event,flags));
        }
        AppWidgetManager.getInstance(context).updateAppWidget(id,views);
    }
}
