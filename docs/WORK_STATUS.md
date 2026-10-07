# Stato verificabile del lavoro — 7 ottobre 2026

## Archivio R2 predisposto

- Database misurato il 7 ottobre: circa 377 MB rispetto ai circa 772 MB
  precedenti. Il recupero maggiore della compattazione è ora avvenuto.
- Schema 15, blocchi immutabili R2 verificati con upload e rilettura prima
  di liberare il payload locale. Limite interno di 8 GB con inventario completo
  del bucket; gli errori conservano i nuovi blocchi nel DB.
- R2 resta disabilitato senza configurazione dell'account: non sono stati
  trasferiti oggetti esterni da questa modifica. Passaggi e limiti nella
  guida `docs/ARCHIVIO_R2.md`.
- Tre backup operativi con riferimenti allo storico, ricostruzione completa
  mensile e ZIP autosufficiente su richiesta. I riferimenti non costituiscono
  tre copie autonome degli oggetti esterni.
- Previsioni e calibrazione continuano con i dati recenti locali; le precedenti
  eliminazioni a scadenza dello storico meteo diventano archiviazioni verificate.
- Comacchio mantenuta: previsioni e calibrazione proprie, nessuna correzione
  automatica di Roma. Circa 9,1 MB di valori nei tre archivi principali,
  esclusi indici e strutture ausiliarie.
- Validazione SQLite e SDK R2 simulato; PostgreSQL 18 e gate di rilascio in CI.
  Verificare merge e deploy prima di dichiarare l'aggiornamento online.

## V5.6: precedente rilascio

- PR #86 integrata in `main`, commit
  `fe9aeb8101f4cd9eebbe2e960830d2233f0f1ba4`. Tutti i gate CI verdi, anche
  PostgreSQL 18, browser, Android e CodeQL. Sito e cron live su questo commit.
- Schema 14. Rimosso il solo indice duplicato su `station_observations`:
  circa 15,5 MiB recuperati inizialmente. Primo cron V5.6 riuscito il 6 ottobre
  alle 21:45 UTC: 50.000 righe archiviate in circa 1 MiB. Il recupero fisico
  maggiore è stato completato nei cicli successivi, come misurato sopra.
- La manutenzione conserva lo storico ensemble e i punteggi in blocchi
  verificati, inclusi nei backup; nessuna cancellazione delle misure.
  Dettagli, budget e requisiti in `CHANGELOG_V5_6.md`.
- Quel primo ciclo ha rinviato `forecast_scores` perché i vecchi Brier
  contengono `NaN`. L'aggiornamento di questo documento accompagna il codec v2
  che conserva NaN/infinito con tipi espliciti e legge anche i blocchi v1.
  Verificare la PR della correzione, CI PostgreSQL e deploy prima di concludere.
- Calibrazione automatica predisposta **solo per due ore asciutte previste
  6–12 ore prima**, separata per stazione/modello. Si attiva con dati sufficienti
  e beneficio verificato su un periodo successivo indipendente; peggioramento,
  scadenza o controllo datato riportano alle frequenze originali. La richiesta
  dell'utente del 6 ottobre autorizza questa attivazione automatica prudente.
- Stato pubblico verificato dopo il primo ciclo: Roma 68 finestre e Comacchio
  89, entrambe su nove giorni e senza casi piovosi completi nel campione.
  Mancano almeno 21 giorni al requisito temporale, intorno al 27 ottobre;
  pioggia, copertura e prova possono rinviare l'attivazione. Non promettere date.
- Probabilità orarie di pioggia, altri anticipi e fotografia restano originali.
  Nessun nuovo APK, piano Render, provider o timer meteo.

## Riferimento storico V5.5

Questo file registra la ripresa V5.5. Per conoscere l'esito della pubblicazione
controllare la PR che contiene questo commit, i suoi controlli e i deploy Render.
Non ricostruire il lavoro dalle vecchie chat e non ripubblicare gli stessi APK.
I limiti sulla calibrazione indicati nella sezione storica sono superati dalla
richiesta V5.6 e dalla predisposizione descritta sopra.

## Base verificata

- Repository: `gio9772rm/weather-app`, branch predefinito `main`.
- Prima della ripresa: `6804273a8321552721b4597786fdf95fef3ab277`, V5.4.1, PR #82.
- Lo stesso commit era live sul sito e sul cron Render. Test del sito, Android,
  controlli grafici e CodeQL superati; ingest e health successivi riusciti.
- V5.2 (#76/#78), V5.3 (#79/#80), V5.4 (#81) e radar semplificato (#82)
  erano già integrati. Non richiedono nuove implementazioni.

## Lavoro recuperato

I commit locali `9123cda` e `2b84465` contenevano la V5.5 non ancora pubblicata.
I loro contenuti sono riuniti nella pubblicazione di `feat/v5-5-calibration-alerts`;
gli hash sopra identificano i commit originali della copia locale recuperata.

1. Pannello campioni e requisiti per lo studio della calibrazione, per stazione.
2. Verifica prospettica delle soglie degli avvisi, incluse mancate e falsi allarmi.
3. Integrazione Android OTA della PR #75, APK firmato 5.2.0 e manifesto coerenti.
4. Sedici aggiornamenti compatibili di dipendenze dalla PR #77.

La verifica del recupero ha inoltre rilevato il backup fallito nel run
`36792177005`: `_csv.Error: field larger than field limit (131072)`.
La correzione include backup, verifica e ripristino di campi grandi e multilinea.

## Verifiche e ordine di pubblicazione

- Prima del recupero: 314 test Python e 12 Node riusciti. La prova browser locale
  era bloccata dall'avvio di Chromium, quindi non costituiva un successo grafico.
- Dopo la correzione: 315 test Python e 12 Node riusciti, incluso il ripristino
  esatto di campi grandi; Ruff e audit privacy riusciti. Firma APK verificata
  con apksigner, dimensione e SHA-256 corrispondenti al manifesto.
- Aprire una sola PR dalla branch recuperata. Richiedere CI completa verde,
  compresi browser e Android, sul commit finale prima del merge.
- Verificare poi che sito e cron Render usino il commit di merge; controllare
  versione web, disponibilità APK/manifesto e primo ciclo dei prodotti V5.5.
- Rieseguire il backup automatico corretto e verificare l'artefatto cifrato.

## Limiti e attività ancora distinte

- La calibrazione resta disattivata: superare le soglie permette uno studio,
  non applica correzioni automatiche. Non promettere una data senza campioni.
- Le statistiche degli avvisi simulano soglie; non certificano invii push.
- Il telefono reale non è stato collaudato. I segreti di firma automatica
  `ANDROID_KEYSTORE_B64` e `ANDROID_KEYSTORE_PASSWORD` non sono stati verificati.
  Non cambiare firma, package o APK 5.2.0 già preparati.
- Streamlit 1.62, NumPy 2.3, SQLAlchemy 2.0 e pytest 8 restano intenzionalmente
  nei rami attuali. Le parti residue delle PR dipendenze #77/#32 sono separate.
- La PR Android #75 è superata dal contenuto V5.5 dopo il merge; evitare un
  secondo merge dei vecchi file in conflitto.
- Cadenza meteo di 600 secondi, separazione Roma/Comacchio e dati privati invariati.
