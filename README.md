# Meteo Locale — Web Dashboard (Streamlit)

Questa mini‑piattaforma ti permette di:
- Ingerire automaticamente **dati della tua stazione** (CSV 3h) e il **forecast 5 giorni OpenWeather**
- Visualizzare tutto via **browser** con una **grafica pulita**
- Esportare il forecast corretto in **CSV**

## 1) Installazione
```bash
cd weather_webapp
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# poi apri .env e imposta:
# OWM_API_KEY, LAT, LON, STATION_CSV, SQLITE_PATH (opzionale)
```

## 2) Primo popolamento DB
```bash
python weather_ingest.py
```

## 3) Avviare il web (browser)
```bash
streamlit run app_streamlit.py
```
Usa il pulsante **"Aggiorna dati (ingest)"** per richiamare l'ingest dal web.

## 4) Aggiornamento automatico
Esegui ogni 6 ore (esempi):

### Linux / macOS (cron)
```
crontab -e
0 */6 * * * cd $(pwd)/weather_webapp && . .venv/bin/activate && python weather_ingest.py >> ingest.log 2>&1
```

### Windows (Task Scheduler)
- Programma: `python`
- Argomenti: `weather_ingest.py`
- Cartella di lavoro: la directory `weather_webapp`

## Note
- Il file stazione può essere quello già **ripulito e risamplato a 3h** che abbiamo generato: 
  `/mnt/data/station_clean_3h_2022_2025.csv`
- Se in futuro avremo accesso **diretto** alla tua stazione (API/gateway), possiamo sostituire STATION_CSV con un endpoint o uno script di lettura diretta.
