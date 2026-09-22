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

PWA, ASGI, strumenti Pro, selettore globale, astronomia residua, snapshot,
offline, import guidato e push sono implementati. Verifica indipendente della
seconda stazione e serie canonica estesa a scoring/3h. Suite di base: 229 test
verdi prima degli ultimi casi di regressione. I test browser comprendono V5
e Pro. Restano il passaggio CI, il merge e la verifica del deploy/cron.

Il widget nativo rimane subordinato ai materiali Android sopra indicati.
