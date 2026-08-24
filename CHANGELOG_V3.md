# Changelog Meteo V3

## Contrasto coerente dei componenti Streamlit

- il tema scelto nell'app ora prevale anche quando il browser conserva una preferenza Streamlit differente;
- corretti sfondo dell'intestazione, pulsante di apertura laterale, radio, schede, etichette dei widget e badge delle metriche in modalità chiara e scura;
- preservati il testo bianco del riquadro principale e i tracciati trasparenti delle icone di sistema;
- la scheda Sistema ora usa la telemetria condivisa del Cron Job e non scambia per disattivate le fonti le cui chiavi sono volutamente assenti dal Web Service;
- aggiunti stati di selezione e focus ad alto contrasto senza alterare i colori semantici di tabelle e grafici.

## Affidabilità, valutazione, backup ed esperienza

- aggiunta la scheda Sistema con salute per Ecowitt, provider, fonti ufficiali, combinazione e backup, inclusi latenza, righe, errori consecutivi e freschezza;
- aggiunti copertura a 5 minuti, buco massimo e conteggio anomalie nelle ultime 24 ore;
- estesi i controlli Ecowitt a salti anomali, sensori fermi e raffiche incoerenti, conservando le misure sospette con flag espliciti;
- introdotta una validazione temporale recente con MAE holdout, baseline di persistenza e skill per variabile e orizzonte;
- aggiunto il diagramma di affidabilità della probabilità di pioggia con fasce del 10%;
- aggiunta la correlazione fra sito ufficiale ed Ecowitt al peso prudenziale delle fonti secondarie;
- aggiunto backup portatile locale con schema, manifest, conteggi e SHA-256, senza trasferire automaticamente il database fuori da Render;
- fissate le dipendenze collaudate in `constraints.txt` e aggiunto Dependabot settimanale per patch/minor;
- aggiunti tema nativo chiaro/scuro, schede scorrevoli su mobile e stato della vista nell'URL;
- mantenuti esclusi notifiche e connettore CFR finché quest'ultimo non viene esplicitamente attivato.

## ARSIAL attiva e CFR predisposto

- integrata la stazione pubblica ARSIAL/SIARL Roma-Lanciani come riferimento secondario per temperatura, umidità, pressione, vento, raffiche, direzione e pioggia quando disponibili;
- aggiunta la scoperta degli export CSV pubblici Superset e del registro stazioni open data, senza chiavi aggiuntive;
- normalizzati formati CSV/JSON, decimali italiani, righe larghe o per singola grandezza, unità e timestamp UTC dichiarati dal portale, convertiti in locale soltanto dalla UI;
- mantenuta Ecowitt come unica sorgente locale primaria e limitato il contributo complessivo della rete esterna;
- resi gli errori ARSIAL indipendenti e non bloccanti per acquisizione, previsioni e METAR;
- predisposto un connettore CFR CSV/JSON con HTTPS, token opzionale e filtro stazioni, completamente dormiente, invisibile e privo di chiamate finché non viene attivato esplicitamente;
- aggiunti test per parsing, scoperta Superset, cambio ora solare/legale e mancata disponibilità delle fonti.

## Rete ufficiale secondaria

- integrate le osservazioni METAR pubbliche Aviation Weather di Roma Fiumicino (`LIRF`) e Roma Ciampino (`LIRA`), senza nuove chiavi API;
- Ecowitt rimane la sorgente locale primaria e le osservazioni aeroportuali sono isolate in una tabella dedicata;
- aggiunta una calibrazione di trasferimento: temperatura, umidità, pressione e vento esterni entrano nelle statistiche solo dopo aver imparato lo scarto stabile rispetto alla Ecowitt;
- estesi bias, MAE e RMSE a punto di rugiada, raffiche e direzione del vento; aggiunte verifiche ufficiali per nuvole, visibilità e probabilità di precipitazione;
- contributo ufficiale limitato per configurazione al 20%, con fallback non bloccante se il servizio esterno non risponde;
- nuova sezione di accuratezza con stato delle stazioni ufficiali e punteggi distinti per fonte.

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
