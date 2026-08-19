

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
  PRIMARY KEY (evaluated_at, provider, model, variable, horizon)
);

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
  weather_code TEXT,
  description TEXT,
  is_day INTEGER,
  temp_uncertainty_c REAL,
  confidence REAL,
  provider_count INTEGER,
  provider_weights TEXT,
  method TEXT
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

CREATE TABLE IF NOT EXISTS meta (
  k TEXT PRIMARY KEY,
  v TEXT
);

CREATE TABLE IF NOT EXISTS user_prefs (
  k TEXT PRIMARY KEY,
  v TEXT
);
