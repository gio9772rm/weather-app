package com.gio9772rm.meteov4;

import android.app.job.JobParameters;
import android.app.job.JobService;
import org.json.JSONObject;
import javax.net.ssl.HttpsURLConnection;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/** Small public snapshot only; never contains an Ecowitt/API key. */
public class WidgetJob extends JobService {
    private static final Object ACQUISITION = new Object();
    private final AtomicBoolean stopped=new AtomicBoolean(false);
    @Override public boolean onStartJob(JobParameters params) {
        stopped.set(false);
        new Thread(()->{
            Set<String> stations=new HashSet<>(); for(int id:MeteoWidget.ids(this)) stations.add(MeteoWidget.station(this,id));
            for(String station:stations) {
                if(stopped.get())break;
                synchronized (ACQUISITION) {
                long last=MeteoWidget.prefs(this).getLong("attempt_"+station,0),now=System.currentTimeMillis();
                if(now>=last && now-last<600000L)continue;
                MeteoWidget.prefs(this).edit().putLong("attempt_"+station,now).apply();
                }
                HttpsURLConnection connection=null;
                try {
                    connection=(HttpsURLConnection)new URL(LauncherActivity.ORIGIN+"/api/v5/widget/"+station).openConnection();
                    connection.setConnectTimeout(10000);connection.setReadTimeout(15000);connection.setInstanceFollowRedirects(false);
                    if(connection.getResponseCode()!=200)continue;
                    ByteArrayOutputStream buffer=new ByteArrayOutputStream();
                    try(InputStream input=connection.getInputStream()) {byte[] block=new byte[2048];int size;while((size=input.read(block))!=-1){buffer.write(block,0,size);if(buffer.size()>16384)throw new java.io.IOException("Snapshot too large");}}
                    String body=buffer.toString(StandardCharsets.UTF_8.name());JSONObject json=new JSONObject(body);
                    if(station.equals(json.optString("station_id")))MeteoWidget.prefs(this).edit().putString("data_"+station,body).apply();
                }catch(Exception ignored){/* Retain dated cached measurements, never fabricate zeros. */}
                finally{if(connection!=null)connection.disconnect();}
            }
            if(!stopped.get()){for(int id:MeteoWidget.ids(this))MeteoWidget.render(this,id);jobFinished(params,false);}
        },"meteo-widget").start();return true;
    }
    @Override public boolean onStopJob(JobParameters params){stopped.set(true);return false;}
}
