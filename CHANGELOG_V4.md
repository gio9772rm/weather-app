# Changelog Meteo V4

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

- nessuna migrazione distruttiva e nessuna nuova tabella necessaria;
- Cron Job, riconciliazione, database e fonti ufficiali restano invariati;
- V3 conservata al commit `527e3a47ebdeefdc480d5dd007246f3d5a3c125d` nel ramo `archive/meteo-v3-stable`;
- V4 in produzione su `main`; ogni correzione successiva passa da CI e pull request.
