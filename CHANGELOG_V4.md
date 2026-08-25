# Changelog Meteo V4

## Home quotidiana

- nuova scheda predefinita **Oggi**, costruita sopra la pipeline V3 senza modificare l'acquisizione Ecowitt;
- intestazione dinamica per giorno, notte, cielo sereno, nuvole e precipitazioni;
- provenienza del valore corrente sempre esplicita: misura Ecowitt oppure previsione combinata;
- riepilogo leggibile delle prossime 24 ore con temperature, pioggia, raffiche e fiducia;
- timeline oraria scorrevole con temperatura, probabilità/quantità di pioggia e vento;
- schede rapide per prima pioggia, raffica massima, escursione termica e concordanza dei modelli;
- indici orientativi e momento migliore per passeggiata, bicicletta, bucato e astronomia.

## Aria e pollini

- nuova scheda **Aria** per stazione locale e città cercate;
- indice europeo AQI, PM2.5, PM10, NO₂, ozono, UV e principali pollini europei;
- grafico a 72 ore e tabella compatta a 48 ore con colori semantici;
- caricamento su richiesta e cache di 30 minuti: la fonte ambientale non rallenta la home;
- errori isolati e non bloccanti, senza nuove chiavi API;
- attribuzione Open-Meteo/CAMS e distinzione visibile fra previsione modellistica e sensore locale.

## Responsive e accessibilità

- sostituite le sei metriche strette con una griglia adattiva 6→3→2 colonne;
- migliorati spaziatura, leggibilità mobile, focus e contrasto dei nuovi componenti;
- preservati tema chiaro/scuro, URL condivisibile e tutte le viste tecniche V3.

## Compatibilità e rollback

- nessuna migrazione distruttiva e nessuna nuova tabella necessaria;
- Cron Job, riconciliazione, database e fonti ufficiali restano invariati;
- V3 conservata al commit `527e3a47ebdeefdc480d5dd007246f3d5a3c125d` nel ramo `archive/meteo-v3-stable`;
- sviluppo e CI V4 sul ramo `meteo-v4`, senza deploy automatico su Render finché non viene unito a `main`.
