# Changelog Meteo V3

## Affidabilità del meteo città

- aggiunto fallback automatico MET Norway quando Open‑Meteo non è raggiungibile da Render;
- uniformati dati orari e giornalieri del fallback alle stesse tabelle e grafici dell'app;
- indicata nell'interfaccia la fonte effettivamente usata per ogni città.

## Ricerca città e rifiniture dell'interfaccia

- aggiunta al menu laterale la modalità Meteo città con ricerca mondiale per nome o CAP;
- aggiunti condizioni correnti, previsione internet oraria e giornaliera, grafico, mappa e download CSV;
- garantita la separazione completa fra dati internet delle città e misure/correzioni della stazione Ecowitt;
- estesa la tabella fino a 7 giorni con selezione ogni 1, 3 o 6 ore;
- sostituite le griglie con tabelle responsive a intestazione fissa e realmente scura, inclusi accuratezza, qualità dati e astronomia;
- aggiunti test di avvio per entrambe le modalità dell'interfaccia.

## Acquisizione stabile su Render

- il Cron Job Render torna a essere la sorgente primaria ogni 5 minuti;
- GitHub Actions conserva una riconciliazione giornaliera idempotente di 7 giorni;
- un advisory lock PostgreSQL impedisce sovrapposizioni fra Render, GitHub e avvii locali;
- l'ingest fallisce se Ecowitt restituisce un campione più vecchio della soglia configurata;
- la dashboard riduce a 60 secondi la cache di osservazioni, stato e log;
- Python è fissato alla serie 3.11 per avere lo stesso runtime in CI e su Render.

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
- schede 7 giorni e tabella fino a 7 giorni con passo configurabile;
- avvisi per pioggia, raffiche e scarsa concordanza;
- viste separate per stazione, qualità dei provider, astronomia e radar;
- rimosse dalla UI pubblica le funzioni che eseguivano script e mostravano log completi.

## Operazioni e sicurezza

- mantenuto il Cron Job Render come acquisizione primaria e GitHub come riconciliazione giornaliera;
- uniformato PostgreSQL su psycopg 3;
- aggiunte migrazioni additive SQLite/PostgreSQL;
- rimossi database, fogli storici, dump Ecowitt, cache e strumenti obsoleti dal tracking Git;
- ampliato `.gitignore`;
- aggiunti script Windows semplici e separati per setup, aggiornamento manuale e dashboard;
- aggiunti 10 test automatici e workflow CI.
