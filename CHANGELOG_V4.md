# Changelog Meteo V4

## V4.6 · recupero fonti, salute e accessibilità

- ARSIAL/SIARL passa dalla sospensione fissa al recupero automatico: un sondaggio ogni sei ore evita di sovraccaricare il portale instabile e attiva l'archiviazione appena torna un export orario valido;
- CFR Lazio via MeteoHub resta il riferimento regionale operativo e ogni dato SIARL continua a essere separato dalle misure Ecowitt;
- il controllo salute parte anche a ogni merge su `main`; se il runner pianificato ritarda, conserva per 24 ore l'ultimo esito indipendente distinguendolo dal controllo continuo Render e continua a rendere visibili i guasti effettivi;
- la pagina Sistema espone subito stato della verifica automatica, backup cloud cifrato e numero di fonti utilizzabili, prima della tabella tecnica;
- input numerici, pulsanti, simboli, placeholder e didascalie di Astronomia Pro seguono il tema scelto senza ereditare la palette del sistema operativo;
- il gate browser copre Oggi, Sistema, Panoramica e Astronomia in entrambe le modalità e su desktop/mobile, con verifica WCAG AA dei controlli astronomici;
- restano deliberatamente esclusi notifiche meteo personali, attivazione di una seconda stazione e un ulteriore sistema di backup.

## V4.5.2 · rifiniture del planner

- la tabella Astronomia Pro mostra un trattino quando non esiste ancora un profilo ottico, invece del valore tecnico `nan`, e usa precisioni numeriche leggibili;
- finestre astronomiche e card di pianificazione usano sempre le abbreviazioni italiane dei giorni, indipendentemente dalla lingua del server;
- nessuna variazione dello schema dati.

## V4.5.1 · coda previsionale archiviata completa

- per gli istanti già trascorsi, l'ultima emissione pubblicata entro l'ora prevista prevale sull'eventuale riga retrospettiva dell'emissione corrente: un valore `null` del provider non interrompe più il confronto delle due ore precedenti;
- il gate Playwright controlla il primo valore numerico realmente disegnabile, non soltanto il primo timestamp della traccia: la copertura visibile deve iniziare tra 105 e 135 minuti prima della linea locale **Adesso**;
- nessuna variazione dello schema dati: la correzione riusa `forecast_blend_history` e non sposta artificialmente i timestamp.

## V4.5 · Astronomia Pro e orari locali verificabili

- planner astronomico esteso con target RA/Dec personali, dimensione apparente, profili ottica/camera, campo inquadrato e campionamento;
- maschera circolare dell'orizzonte locale a otto direzioni, usata per escludere finestre coperte da tetti, alberi o rilievi;
- esportazione calendario ICS all'istante UTC corretto, diario osservativo CSV e configurazione planner JSON mantenuti nella sessione browser;
- assi temporali Plotly serializzati con offset `Europe/Rome` esplicito per allineare tracce e linea tratteggiata **Adesso**, inclusi i passaggi CET/CEST;
- due ore di previsione precedente recuperate da `forecast_blend_history` e conservate nei grafici principali per il confronto con le misure, senza traslare o reinterpretare i timestamp;
- contratti browser estesi alle viste Panoramica e Astronomia su desktop/mobile e tema chiaro/scuro; schema database invariato.

## V4.4 · affidabilità, diagnostica e pianificazione

- controllo salute con endpoint Render effettivo e issue GitHub operative deduplicate per salute, ingestione, backup e prova di ripristino;
- ripristino portatile in un nuovo SQLite, verifica d'integrità e prova mensile automatica sull'ultimo backup cifrato non scaduto;
- diagnostica Ecowitt per sensore con freschezza, copertura, buco massimo, qualità e telemetria batteria/segnale sanitizzata;
- pianificatore astronomico personale con catalogo deep-sky, altezza/azimut, condizioni meteo e distanza dalla Luna;
- contratti visuali Playwright desktop/mobile e chiaro/scuro con screenshot CI;
- audit privacy del tree corrente e GitHub Actions bloccate a SHA immutabili;
- schema V8 additivo con `ecowitt_telemetry`; nessuna riscrittura distruttiva dello storico Git.

## V4.3.1 · salute, continuità e backup automatico

- health check GitHub ogni 30 minuti per database, freschezza Ecowitt e previsione combinata, affiancato al riavvio automatico Render;
- stato delle fonti con archivio utilizzabile e strategia di fallback, senza trasformare un servizio esterno opzionale in guasto generale;
- retry HTTP estesi anche alle interrogazioni EEA in POST e rispetto di `Retry-After`;
- backup giornaliero alle 22:07 `Europe/Rome`, cifrato prima dell'upload, con scadenza automatica dopo 30 giorni e stato cloud separato dalla verifica locale;
- badge **LIVE** verde animato e **NON LIVE** rosso per Ecowitt, aria EEA e radar DPC, basati sull'età reale della singola misura;
- corretta l'escape CSS che mostrava `�93`/simboli illeggibili nei comandi di espansione;
- tabella Sistema più compatta in modalità Semplice, valori tecnici formattati e dettagli completi conservati in modalità Esperta.

## V4.3 · radar ufficiale, calibrazione e UX espandibile

- osservazione DPC SRI/VMI distinta dal nowcast RainViewer e dalla previsione Windy, con fulmini indicati come non disponibili quando manca il frame ufficiale;
- ICON-2I esplicito nelle prime 72 ore, calibrazione per regime e riferimento climatico ERA5-Land 1991–2020;
- CFR Lazio via MeteoHub come riferimento regionale operativo; ARSIAL/SIARL resta integrata ma è sospesa di default finché l'export pubblico non torna stabile;
- registro multi-stazione, rapporti mensili PDF/CSV e predisposizione additiva per una futura seconda Ecowitt;
- riquadri meteo e di pianificazione espandibili nello stesso punto con clic o tastiera;
- bollettini ufficiali spostati subito sotto **Pianifica la giornata**;
- stato backup corretto: manuale prima della prima esecuzione, operativo solo dopo ZIP e checksum verificati, con compatibilità per gli archivi V3;
- schema V7 additivo, coordinate private escluse da UI, rapporti e configurazione pubblica.

## V4.2 · contesto climatico, misure e fonti ufficiali

- climatologia locale Ecowitt per mese e ora con mediana, fascia P10–P90 e scarto del valore corrente;
- dicitura esplicita **baseline locale** finché lo storico non ha profondità sufficiente per essere considerato una normale climatica;
- pollini realmente misurati dalla stazione POLLnet/ISPRA più vicina, separati dalla previsione modellistica CAMS;
- esclusione dei livelli botanici figli per evitare il doppio conteggio delle concentrazioni POLLnet;
- data, distanza ed età del campione sempre visibili; una misura arretrata viene indicata come archivio e non come livello attuale;
- riepilogo DPC nazionale e collegamenti agli ultimi bollettini del Centro Funzionale Regionale Lazio direttamente nella home;
- distinzione netta tra allerte ufficiali e soglie contestuali interne della dashboard;
- modalità **Semplice** ed **Esperta** conservata nell’URL: la seconda aggiunge fasce statistiche, confronti grezzi e metadati;
- moduli isolati e non bloccanti, archivi V6 additivi e nessuna nuova chiave API.

## V4.1 · esperienza, probabilità e dati ambientali osservati

- confronto delle due ultime emissioni con indicatore di stabilità e dettaglio delle variazioni;
- archivio additivo `forecast_blend_history` e guida ICON-EPS P10/P50/P90 in `forecast_ensemble_runs`;
- probabilità ensemble usata con peso prudente, senza trasformare i membri in provider indipendenti;
- nowcast RainViewer puntuale con gestione esplicita dell'assenza di fotogrammi futuri;
- aria osservata EEA UTD separata da Ecowitt e affiancata al modello CAMS;
- finestra orientativa per arieggiare casa, preferiti e confronto Roma–città;
- provenienza/età/qualità dei dati e riepilogo quotidiano PNG;
- palette semantica Okabe–Ito, testi e tratteggi per non affidare il significato al solo colore;
- predisposti gli archivi poi attivati nella V4.2 per climatologia, pollini misurati, bollettini in pagina e modalità semplice/esperta;
- schema dati V5, migrazione solo additiva e V3 invariata nel ramo di archivio.

## Home quotidiana

- nuova scheda predefinita **Oggi**, costruita sopra la pipeline V3 senza modificare l'acquisizione Ecowitt;
- intestazione dinamica per giorno, notte, cielo sereno, nuvole e precipitazioni;
- provenienza del valore corrente sempre esplicita: misura Ecowitt oppure previsione combinata;
- riepilogo leggibile delle prossime 24 ore con temperature, pioggia, raffiche e fiducia;
- timeline oraria scorrevole con temperatura, nuvole, probabilità/quantità di pioggia e vento;
- schede rapide per prima pioggia, raffica massima, escursione termica e concordanza dei modelli;
- recap quotidiano con passeggiata, pollini, momento astronomico migliore e stato della Luna;
- rimossi dal recap bicicletta e bucato per privilegiare le informazioni più utili alla stazione.

## Aria e pollini

- nuova scheda **Aria** per stazione locale e città cercate;
- indice europeo AQI, PM2.5, PM10, NO₂, ozono, UV e principali pollini europei;
- grafico a 72 ore e tabella compatta a 48 ore con colori semantici;
- caricamento su richiesta e cache di 30 minuti: sulla home alimenta solo il recap pollini;
- errori isolati e non bloccanti, senza nuove chiavi API;
- attribuzione Open-Meteo/CAMS e distinzione visibile fra previsione modellistica e sensore locale.

## Responsive e accessibilità

- sostituite le sei metriche strette con una griglia adattiva 6→3→2 colonne;
- migliorati spaziatura, leggibilità mobile, focus e contrasto dei nuovi componenti;
- rese sempre visibili le icone delle nuvole con un fondino ad alto contrasto;
- scrollbar evidenti in entrambi i temi per timeline, schede e tabelle orizzontali;
- chiavi di lettura collocate accanto a ogni pannello dei grafici multipli e legende uniformate;
- preservati tema chiaro/scuro, URL condivisibile e tutte le viste tecniche V3.

## Compatibilità e rollback

- nessuna migrazione distruttiva; le nuove tabelle V5 vengono create automaticamente;
- Cron Job, riconciliazione, database e fonti ufficiali restano invariati;
- V3 conservata al commit `527e3a47ebdeefdc480d5dd007246f3d5a3c125d` nel ramo `archive/meteo-v3-stable`;
- V4 in produzione su `main`; ogni correzione successiva passa da CI e pull request.
