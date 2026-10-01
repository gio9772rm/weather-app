# Ripresa verificabile del lavoro — 1 ottobre 2026

Questo file registra la ripresa V5.5. Per conoscere l'esito della pubblicazione
controllare la PR che contiene questo commit, i suoi controlli e i deploy Render.
Non ricostruire il lavoro dalle vecchie chat e non ripubblicare gli stessi APK.

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
