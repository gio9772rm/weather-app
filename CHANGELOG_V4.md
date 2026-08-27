# Changelog Meteo V4

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
