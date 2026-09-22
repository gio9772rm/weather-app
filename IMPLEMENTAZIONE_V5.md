# Meteo Pro V5 — rilascio unico

Obiettivo autorizzato: aggiornare sito e app insieme, con commit/push e merge
dopo CI verde. Nessun rilascio intermedio, nessuna cancellazione dello storico.

## Vincoli

- Snapshot pagina e cron ogni 600 secondi. I provider mantengono la propria cadenza.
- Roma e Comacchio non condividono osservazioni o calibrazione.
- Coordinate esatte, MAC, credenziali e import originali restano riservati.
- I dati mancanti non vengono inventati; offline sempre datato e dichiarato.
- Si conserva l'APK esistente e la sua firma. Non sono disponibili i sorgenti
  nativi e il materiale di firma: un nuovo widget Android non può essere
  distribuito come aggiornamento firmato finché non vengono recuperati.

## Architettura

Un unico servizio e lo stesso comando Render: `streamlit run app_streamlit.py`.
Il supporto ASGI di Streamlit 1.62 ospita la nuova PWA, API pubbliche limitate
ai dati autorizzati e gli strumenti scientifici completi sotto `/pro/`.
La configurazione già bloccata in constraints.txt è compatibile.

## Controlli di rilascio

- Test unitari: isolamento, pioggia, copertura, buio, DST, ore residue, privacy.
- Test API: validazione, limiti, assenza di segreti, origine delle scritture.
- Test browser: desktop/mobile, entrambi i temi, navigazione, offline, timer.
- CI verde prima del merge. Conferma deploy live e primo cron successivo.

## Stato della lavorazione

V5 pubblicata con PR #69, commit 6f12e70: 234 test e 32 viste browser verdi,
inclusi timer e offline. Sito e cron Render distribuiti il 22 settembre 2026.
Il vecchio processo già in corso prima del deploy è terminato alle 19:18 UTC;
successivamente 15 acquisizioni per stazione e tre ricalcoli previsionali
completati, con picco di memoria campionato circa 332 MiB su 512 MiB.
Snapshot, previsioni e ambiente presenti per entrambe le stazioni.

Il collaudo online ha evidenziato la probabilità mancante nelle ore ICON-2I
di Comacchio. La correzione riusa il campo best-match della stessa ora e
località, senza nuove chiamate e senza cambiare cooldown o calibrazione Roma.

Il widget nativo rimane subordinato ai materiali Android sopra indicati.
