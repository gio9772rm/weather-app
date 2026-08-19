# Changelog Meteo V3

## Previsioni

- aggiunto Open‑Meteo `best_match` con passo orario e orizzonte configurabile fino a 7 giorni;
- mantenuto OpenWeather come secondo provider a 3 ore;
- aggiunto archivio delle emissioni con `issued_at`, `valid_time`, modello e lead time;
- aggiunta verifica su osservazioni locali: bias, MAE, RMSE e Brier score;
- aggiunta fusione con pesi inversi al MAE e prior 60/40;
- aggiunta correzione sull'anomalia attuale della stazione con decadimento a 12 ore;
- aggiunti incertezza, fiducia e numero provider per ogni ora;
- aggiunta conservazione controllata dello storico derivato.

## Stazione e pioggia

- corretto l'errore pandas su timestamp UTC già timezone-aware;
- separati intensità, totale cumulativo e incremento pioggia;
- aggiunto fallback `intensità × tempo` marcato come stima;
- aggiunti controlli di intervallo e campo `data_quality`;
- aggiunti solare e indice UV;
- aggiunta media circolare della direzione del vento;
- lo stato della stazione deriva dall'ultimo campione realmente salvato, non da un semplice file `last_ingest`.

## Interfaccia

- nuovo layout responsive chiaro;
- continuità grafica tra osservazioni e previsioni;
- schede 7 giorni e tabella oraria a 72 ore;
- avvisi per pioggia, raffiche e scarsa concordanza;
- viste separate per stazione, qualità dei provider, astronomia e radar;
- rimosse dalla UI pubblica le funzioni che eseguivano script e mostravano log completi.

## Operazioni e sicurezza

- unificati i tre scheduler nella sola pipeline GitHub ogni 10 minuti;
- rimosso il cron dal Blueprint Render;
- uniformato PostgreSQL su psycopg 3;
- aggiunte migrazioni additive SQLite/PostgreSQL;
- rimossi database, fogli storici, dump Ecowitt, cache e strumenti obsoleti dal tracking Git;
- ampliato `.gitignore`;
- aggiunti script Windows semplici e separati per setup, aggiornamento manuale e dashboard;
- aggiunti 10 test automatici e workflow CI.
