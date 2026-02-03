

-- raw station observations (most frequent cadence)
CREATE TABLE IF NOT EXISTS station_raw (
  Time TEXT PRIMARY KEY,
  Temp_C REAL,
  Humidity REAL,
  Pressure_hPa REAL,
  Wind_kmh REAL,
  WindGust_kmh REAL,
  WindDir REAL,
  Rain_mm REAL,
  wind_ms REAL
);

-- station observations aggregated to 3h
CREATE TABLE IF NOT EXISTS station_3h (
  Time TEXT PRIMARY KEY,
  Temp_C REAL,
  Humidity REAL,
  Pressure_hPa REAL,
  Wind_kmh REAL,
  WindGust_kmh REAL,
  Rain_mm REAL
);

-- openweather 5-day forecast (3h step)
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

-- metadata
CREATE TABLE IF NOT EXISTS meta (
  k TEXT PRIMARY KEY,
  v TEXT
);
