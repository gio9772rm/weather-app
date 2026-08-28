

-- Meteo V3 schema. SQL is deliberately portable between SQLite and PostgreSQL.

CREATE TABLE IF NOT EXISTS station_raw (
  Time TEXT PRIMARY KEY,
  Temp_C REAL,
  Humidity REAL,
  Pressure_hPa REAL,
  Wind_kmh REAL,
  WindGust_kmh REAL,
  WindDir REAL,
  Rain_mm REAL,
  wind_ms REAL,
  rain_rate_mm_h REAL,
  rain_total_mm REAL,
  solar_w_m2 REAL,
  uv_index REAL,
  source TEXT,
  data_quality TEXT
);

CREATE TABLE IF NOT EXISTS station_3h (
  Time TEXT PRIMARY KEY,
  Temp_C REAL,
  Humidity REAL,
  Pressure_hPa REAL,
  Wind_kmh REAL,
  WindGust_kmh REAL,
  Rain_mm REAL,
  WindDir REAL,
  sample_count INTEGER
);

-- Kept for compatibility with the V1/V2 dashboard and historical data.
CREATE TABLE IF NOT EXISTS forecast_ow (
  Time TEXT PRIMARY KEY,
  Temp_C REAL,
  Humidity REAL,
  Pressure_hPa REAL,
  Clouds REAL,
  Wind_mps REAL,
  WindDir REAL,
  Rain_mm REAL,
  Snow_mm REAL
);

CREATE TABLE IF NOT EXISTS forecast_runs (
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  issued_at TEXT NOT NULL,
  valid_time TEXT NOT NULL,
  interval_hours REAL NOT NULL DEFAULT 1,
  lead_hours REAL,
  temp_c REAL,
  feels_like_c REAL,
  humidity REAL,
  dewpoint_c REAL,
  pressure_hpa REAL,
  wind_kmh REAL,
  wind_gust_kmh REAL,
  wind_dir REAL,
  rain_mm REAL,
  snow_mm REAL,
  precip_probability REAL,
  clouds REAL,
  cloud_low REAL,
  cloud_mid REAL,
  cloud_high REAL,
  visibility_m REAL,
  cape_j_kg REAL,
  freezing_level_m REAL,
  wind_300hpa_kmh REAL,
  humidity_700hpa REAL,
  geopotential_500hpa_m REAL,
  temperature_850hpa_c REAL,
  weather_code TEXT,
  description TEXT,
  is_day INTEGER,
  fetched_at TEXT NOT NULL,
  PRIMARY KEY (provider, model, issued_at, valid_time)
);

CREATE INDEX IF NOT EXISTS idx_forecast_runs_valid_time
  ON forecast_runs (valid_time);

CREATE INDEX IF NOT EXISTS idx_forecast_runs_issued_at
  ON forecast_runs (issued_at);

-- Station-aware mirrors are additive: the legacy tables stay untouched while
-- future stations can coexist without colliding on timestamp-only keys.
CREATE TABLE IF NOT EXISTS station_profiles (
  station_id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  latitude REAL,
  longitude REAL,
  elevation_m REAL,
  timezone TEXT NOT NULL,
  source TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'primary',
  enabled INTEGER NOT NULL DEFAULT 1,
  privacy_level TEXT NOT NULL DEFAULT 'private_location',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS station_observations (
  station_id TEXT NOT NULL,
  time TEXT NOT NULL,
  temp_c REAL,
  humidity REAL,
  pressure_hpa REAL,
  wind_kmh REAL,
  windgust_kmh REAL,
  winddir REAL,
  rain_mm REAL,
  wind_ms REAL,
  rain_rate_mm_h REAL,
  rain_total_mm REAL,
  solar_w_m2 REAL,
  uv_index REAL,
  source TEXT,
  data_quality TEXT,
  PRIMARY KEY (station_id, time),
  FOREIGN KEY (station_id) REFERENCES station_profiles(station_id)
);

CREATE INDEX IF NOT EXISTS idx_station_observations_time
  ON station_observations (station_id, time);

CREATE TABLE IF NOT EXISTS forecast_scores (
  evaluated_at TEXT NOT NULL,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  variable TEXT NOT NULL,
  horizon TEXT NOT NULL,
  n INTEGER NOT NULL,
  bias REAL,
  mae REAL,
  rmse REAL,
  brier REAL,
  holdout_n INTEGER,
  holdout_mae REAL,
  persistence_mae REAL,
  skill_vs_persistence REAL,
  reliability_gap REAL,
  PRIMARY KEY (evaluated_at, provider, model, variable, horizon)
);

CREATE TABLE IF NOT EXISTS forecast_regime_scores (
  evaluated_at TEXT NOT NULL,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  variable TEXT NOT NULL,
  horizon TEXT NOT NULL,
  regime TEXT NOT NULL,
  n INTEGER NOT NULL,
  bias REAL,
  mae REAL,
  rmse REAL,
  brier REAL,
  PRIMARY KEY (evaluated_at, provider, model, variable, horizon, regime)
);

CREATE INDEX IF NOT EXISTS idx_forecast_regime_scores_evaluated
  ON forecast_regime_scores (evaluated_at);

-- Official observations are deliberately isolated from station_raw: they can
-- validate and regularise forecast statistics but never impersonate Ecowitt.
CREATE TABLE IF NOT EXISTS official_observations (
  source TEXT NOT NULL,
  station_id TEXT NOT NULL,
  time TEXT NOT NULL,
  station_name TEXT,
  latitude REAL,
  longitude REAL,
  elevation_m REAL,
  distance_km REAL,
  temp_c REAL,
  dewpoint_c REAL,
  humidity REAL,
  pressure_hpa REAL,
  wind_kmh REAL,
  wind_gust_kmh REAL,
  wind_dir REAL,
  rain_mm REAL,
  precip_observed INTEGER,
  clouds REAL,
  visibility_m REAL,
  quality_flag TEXT,
  raw_observation TEXT,
  fetched_at TEXT NOT NULL,
  PRIMARY KEY (source, station_id, time)
);

CREATE INDEX IF NOT EXISTS idx_official_observations_time
  ON official_observations (time);

CREATE TABLE IF NOT EXISTS forecast_reference_scores (
  evaluated_at TEXT NOT NULL,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  source TEXT NOT NULL,
  station_id TEXT NOT NULL,
  variable TEXT NOT NULL,
  horizon TEXT NOT NULL,
  n INTEGER NOT NULL,
  bias REAL,
  mae REAL,
  rmse REAL,
  brier REAL,
  transfer_bias REAL,
  transfer_mae REAL,
  site_correlation REAL,
  reference_weight REAL NOT NULL,
  PRIMARY KEY (
    evaluated_at, provider, model, source, station_id, variable, horizon
  )
);

CREATE INDEX IF NOT EXISTS idx_forecast_reference_scores_evaluated
  ON forecast_reference_scores (evaluated_at);

CREATE TABLE IF NOT EXISTS forecast_reliability (
  evaluated_at TEXT NOT NULL,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  horizon TEXT NOT NULL,
  probability_bin INTEGER NOT NULL,
  n INTEGER NOT NULL,
  mean_probability REAL,
  observed_frequency REAL,
  brier REAL,
  PRIMARY KEY (evaluated_at,provider,model,horizon,probability_bin)
);

CREATE INDEX IF NOT EXISTS idx_forecast_reliability_evaluated
  ON forecast_reliability (evaluated_at);

CREATE TABLE IF NOT EXISTS forecast_blend (
  valid_time TEXT PRIMARY KEY,
  issued_at TEXT NOT NULL,
  temp_c REAL,
  feels_like_c REAL,
  humidity REAL,
  dewpoint_c REAL,
  pressure_hpa REAL,
  wind_kmh REAL,
  wind_gust_kmh REAL,
  wind_dir REAL,
  rain_mm REAL,
  snow_mm REAL,
  precip_probability REAL,
  clouds REAL,
  cloud_low REAL,
  cloud_mid REAL,
  cloud_high REAL,
  visibility_m REAL,
  cape_j_kg REAL,
  freezing_level_m REAL,
  wind_300hpa_kmh REAL,
  humidity_700hpa REAL,
  geopotential_500hpa_m REAL,
  temperature_850hpa_c REAL,
  weather_code TEXT,
  description TEXT,
  is_day INTEGER,
  temp_uncertainty_c REAL,
  confidence REAL,
  provider_count INTEGER,
  provider_weights TEXT,
  method TEXT
);

-- Every emitted blend is retained before the current cache is replaced.  This
-- lets the UI explain how the forecast changed between two consecutive runs
-- without changing the fast, one-row-per-hour forecast_blend table.
CREATE TABLE IF NOT EXISTS forecast_blend_history (
  issued_at TEXT NOT NULL,
  valid_time TEXT NOT NULL,
  temp_c REAL,
  feels_like_c REAL,
  humidity REAL,
  dewpoint_c REAL,
  pressure_hpa REAL,
  wind_kmh REAL,
  wind_gust_kmh REAL,
  wind_dir REAL,
  rain_mm REAL,
  snow_mm REAL,
  precip_probability REAL,
  clouds REAL,
  cloud_low REAL,
  cloud_mid REAL,
  cloud_high REAL,
  visibility_m REAL,
  cape_j_kg REAL,
  freezing_level_m REAL,
  wind_300hpa_kmh REAL,
  humidity_700hpa REAL,
  geopotential_500hpa_m REAL,
  temperature_850hpa_c REAL,
  weather_code TEXT,
  description TEXT,
  is_day INTEGER,
  temp_uncertainty_c REAL,
  confidence REAL,
  provider_count INTEGER,
  provider_weights TEXT,
  method TEXT,
  PRIMARY KEY (issued_at, valid_time)
);

CREATE INDEX IF NOT EXISTS idx_forecast_blend_history_valid
  ON forecast_blend_history (valid_time, issued_at);

-- Probabilistic guidance stays separate from deterministic providers so forty
-- perturbed members cannot accidentally outweigh Ecowitt-calibrated forecasts.
CREATE TABLE IF NOT EXISTS forecast_ensemble_runs (
  source TEXT NOT NULL,
  model TEXT NOT NULL,
  issued_at TEXT NOT NULL,
  valid_time TEXT NOT NULL,
  variable TEXT NOT NULL,
  p10 REAL,
  p25 REAL,
  p50 REAL,
  p75 REAL,
  p90 REAL,
  mean REAL,
  member_count INTEGER NOT NULL,
  event_probability REAL,
  fetched_at TEXT NOT NULL,
  PRIMARY KEY (source, model, issued_at, valid_time, variable)
);

CREATE INDEX IF NOT EXISTS idx_forecast_ensemble_latest
  ON forecast_ensemble_runs (issued_at, valid_time, variable);

-- Long-form external environmental observations. EEA air measurements use it
-- now, while measured pollen and other official feeds can be added later
-- without mixing any value into station_raw (Ecowitt remains the sole primary).
CREATE TABLE IF NOT EXISTS environment_observations (
  source TEXT NOT NULL,
  station_id TEXT NOT NULL,
  time TEXT NOT NULL,
  metric TEXT NOT NULL,
  value REAL,
  unit TEXT,
  station_name TEXT,
  latitude REAL,
  longitude REAL,
  distance_km REAL,
  quality_flag TEXT,
  is_modelled INTEGER NOT NULL DEFAULT 0,
  fetched_at TEXT NOT NULL,
  PRIMARY KEY (source, station_id, time, metric)
);

CREATE INDEX IF NOT EXISTS idx_environment_observations_metric_time
  ON environment_observations (metric, time);

-- Additive V4.2 stores for the local climatology and official alert banners.
CREATE TABLE IF NOT EXISTS climate_normals (
  source TEXT NOT NULL,
  month INTEGER NOT NULL,
  day INTEGER NOT NULL,
  hour INTEGER NOT NULL,
  metric TEXT NOT NULL,
  p10 REAL,
  p50 REAL,
  p90 REAL,
  sample_years INTEGER,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (source, month, day, hour, metric)
);

CREATE TABLE IF NOT EXISTS climate_reference_normals (
  station_id TEXT NOT NULL,
  source TEXT NOT NULL,
  period_start INTEGER NOT NULL,
  period_end INTEGER NOT NULL,
  month INTEGER NOT NULL,
  metric TEXT NOT NULL,
  value REAL,
  unit TEXT,
  sample_years INTEGER,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (station_id, source, period_start, period_end, month, metric)
);

-- Only point values and local summary statistics are persisted. Raster tiles
-- and lightning coordinates are never archived in the application database.
CREATE TABLE IF NOT EXISTS radar_local_snapshots (
  station_id TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  sri_observed_at TEXT,
  vmi_observed_at TEXT,
  lightning_observed_at TEXT,
  sri_point_mm_h REAL,
  sri_mean_mm_h REAL,
  sri_max_mm_h REAL,
  sri_echo_fraction REAL,
  vmi_point_dbz REAL,
  vmi_max_dbz REAL,
  lightning_10km INTEGER,
  lightning_25km INTEGER,
  lightning_50km INTEGER,
  nearest_lightning_km REAL,
  fetched_at TEXT NOT NULL,
  PRIMARY KEY (station_id, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_radar_local_snapshots_latest
  ON radar_local_snapshots (station_id, observed_at);

CREATE TABLE IF NOT EXISTS official_alerts (
  source TEXT NOT NULL,
  alert_id TEXT NOT NULL,
  issued_at TEXT NOT NULL,
  starts_at TEXT,
  ends_at TEXT,
  severity TEXT,
  title TEXT,
  description TEXT,
  area TEXT,
  source_url TEXT,
  fetched_at TEXT NOT NULL,
  PRIMARY KEY (source, alert_id, issued_at)
);

CREATE TABLE IF NOT EXISTS ingest_log (
  id TEXT PRIMARY KEY,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  component TEXT NOT NULL,
  status TEXT NOT NULL,
  rows_written INTEGER DEFAULT 0,
  message TEXT
);

CREATE TABLE IF NOT EXISTS source_health (
  source TEXT PRIMARY KEY,
  last_attempt_at TEXT,
  last_success_at TEXT,
  last_observation_at TEXT,
  status TEXT NOT NULL DEFAULT 'waiting',
  rows_received INTEGER NOT NULL DEFAULT 0,
  latency_ms REAL,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS meta (
  k TEXT PRIMARY KEY,
  v TEXT
);

CREATE TABLE IF NOT EXISTS user_prefs (
  k TEXT PRIMARY KEY,
  v TEXT
);
