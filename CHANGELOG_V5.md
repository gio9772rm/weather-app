# Meteo Pro V5 — 5.0.0

Rilascio unico di sito/PWA e contenuti dell’app Android esistente.

## Esperienza

Home a riquadri configurabili, navigazione mobile fissa, temi chiaro/scuro,
stazione globale, grafici con legenda legata agli stessi colori delle serie,
separazione fra misure e previsioni, confronto delle emissioni e archivio con
copertura consultabile. La tabella a 1/3/6 ore usa blocchi temporali reali:
medie, raffica/probabilità massime, pioggia sommata solo con tutte le ore presenti.
Le interruzioni non vengono interpolate. Le previsioni del giorno corrente
descrivono le ore residue.

L’interfaccia avanzata resta in `/pro/`. La ricerca per città, radar e nowcast,
rapporti PDF/CSV, pianificatore target/ottiche, fonti ufficiali e diagnostica
rimangono accessibili. Le statistiche di calibrazione di Roma non compaiono
come statistiche della seconda località.

## Dati e astronomia

- Roma e Comacchio hanno snapshot, meteo, aria/pollini, emissioni e astronomia
  distinti. Le coordinate precise rimangono sul server.
- Comacchio usa ICON-2I dove disponibile e best-match a completamento. Nessun
  valore di probabilità viene dedotto dai millimetri: se manca in ICON-2I,
  si usa la probabilità best-match per la stessa ora e località, specificando
  l'origine nei dettagli astronomici. Se entrambe mancano, la qualità resta
  incompleta. Nessun
  ensemble fittizio e nessuna percentuale di fiducia inventata. Gli errori
  vengono confrontati con i suoi campioni, su 14 giorni, distinguendo orizzonte
  e validazione temporale. La calibrazione automatica di Comacchio resta inattiva.
- Le ore astronomiche vengono tagliate all’istante della fotografia e ai
  crepuscoli reali (6/12/18°), in UTC per gestire correttamente ora legale e solare.
  Mancanze e buchi spezzano le finestre. Le penalità spiegano nuvole, pioggia,
  vento, raffiche, condensa e proxy atmosferici.
- Serie canonica a cinque minuti anche per aggregati a tre ore, scoring e
  correzione iniziale. Archivio grezzo conservato. L’assenza di dati pioggia
  non viene trasformata in zero.
- Verifica di Roma ridotta nel database a un’emissione per ora-obiettivo,
  modello e orizzonte (la più lontana disponibile, criterio conservativo).
  Evita campioni correlati contati più volte e il picco di memoria del cron
  a 512 MB. Le emissioni originali restano archiviate. Riepiloghi giornalieri
  elaborati in blocchi di 31 giorni con continuità dei contatori ai confini.
- Import giornaliero XLSX limitato a 8 MB/5.000 date, decompressione limitata,
  autenticazione amministratore, anteprima e conferma del digest. Nessun dato
  orario inventato. Il risultato appare al normale ciclo successivo.

## Offline e avvisi

Una sola lettura automatica ogni 600 secondi; navigazione e cambio stazione
riusano i dati già caricati. Il pulsante manuale riavvia il timer. API con TTL
600, snapshot generati dal cron esistente. Forecast e CAMS restano orari.

Il service worker conserva solo shell e API pubbliche esplicitamente consentite.
Le copie offline mostrano l’istante originario e non sono dichiarate live. Le
preferenze permettono di disabilitarle o eliminarle. Pannelli amministrativi,
token, import e sottoscrizioni push non entrano nella cache offline.

Gli avvisi sono facoltativi: pioggia, raffiche, finestra astronomica, stazione
non aggiornata. Consenso del browser, fascia silenziosa locale, soglie e
revoca dal dispositivo. La valutazione avviene solo nel cron a 10 minuti,
con snapshot freschi, prenotazione atomica e deduplicazione di almeno quattro
ore per tipo. Gli endpoint scaduti vengono rimossi; le sottoscrizioni non
rinnovate scadono dopo 90 giorni. La consegna dipende dal browser/sistema e
non sostituisce allerte ufficiali o sistemi di sicurezza.

Le chiavi VAPID vengono generate una volta in PostgreSQL. Recapiti push e
chiavi sono privati e rientrano nei backup cifrati esistenti. Non sono richieste
modifiche ai segreti Ecowitt o ai cooldown. Gli export manuali del database
sono riservati e non vanno pubblicati.

## Distribuzione

Stesso comando: `streamlit run app_streamlit.py`. Streamlit 1.62 espone la
shell ASGI e monta `app_pro.py`; dipendenze bloccate in `constraints.txt`.
Schema 10 additivo e portabile SQLite/PostgreSQL. Backup include anche
`station_daily_summaries` e i nuovi archivi V5.

Controlli: suite pytest, Ruff, privacy audit, API/auth/import/push, verifica
crittografica push senza rete, browser desktop/mobile e temi, timer simulato,
offline, isolamento delle due località e regressioni della UI Pro.

Per il rollback si può ripristinare il commit V4.9.9 senza cancellare tabelle.
I nuovi archivi sono additivi e non cambiano la struttura delle misure storiche.

## Android nativo

L’APK e la firma esistenti sono conservati. Il sito aggiornato è disponibile
nel contenitore TWA e come PWA installabile. Un widget nativo e una nuova
build firmata richiedono i sorgenti Android e la procedura di firma originali,
non presenti nel repository: non vengono dichiarati rilasciati in questa build.
