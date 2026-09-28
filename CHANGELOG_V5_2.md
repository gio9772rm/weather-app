# V5.2 — precisione locale e strumenti del sito

## Sette miglioramenti

1. **Correzione locale verificata:** temperatura, umidità, vento e pioggia per ciascuna stazione e orizzonte. Il periodo di addestramento precede le emissioni della validazione; una correzione entra nel sito solo con almeno 30 giorni, 120 campioni di addestramento, 30 di validazione e un miglioramento di almeno il 10% rispetto alla base e alla persistenza sugli stessi riscontri. La pioggia richiede anche eventi bagnati sufficienti. Le correzioni scadono dopo 26 ore senza nuova verifica; originali e misure restano conservati.
2. **Home più leggibile:** prossima pioggia, finestra diurna, variazioni significative dall’ultima consultazione e dettagli espandibili. Il confronto tra visite viene conservato solo se è abilitata la copia locale.
3. **Sessioni astronomiche utilizzabili:** incrocio di geometria del target, buio, distanza dalla Luna e intervalli meteo. Profili e soglie personali per nuvole, vento, raffiche, margine di rugiada e probabilità di pioggia; lacune e dati essenziali mancanti interrompono la sessione.
4. **Qualità e provenienza:** una pagina con origine, ora effettiva e stato di ogni misura, sensori apparentemente fermi, salti, valori implausibili e reset pioggia. La diagnosi legge i campioni originali, senza correggerli in archivio.
5. **Ricostruzione eventi:** finestre fino a 24 ore nei 14 giorni precedenti, misure locali, radar osservato e ultima previsione effettivamente pubblicata prima dell’inizio. Le revisioni successive restano escluse; l’assenza di un’emissione archiviata è dichiarata.
6. **Archivio mensile:** copertura, pioggia disponibile, giorni piovosi, sequenze asciutte, estremi del periodo disponibile, confronto fra stazioni ed esportazione CSV. Giorni incompleti esclusi da totali e record; le lacune interrompono le sequenze. I riepiloghi importati restano riconoscibili.
7. **Confronto dei modelli:** pagina “Quanto ci prende?” con errori, periodi, riscontri, validazione e persistenza. Gli archivi a scadenza fissa sono distinti dalle acquisizioni operative; modelli con periodi diversi non formano una classifica omogenea. I nuovi modelli rimangono in valutazione.

## Fonti integrate

- Open-Meteo Previous Runs: ICON ed ECMWF, 45 giorni e scadenze fisse di uno e tre giorni; aggiornamento giornaliero. La scadenza ricostruita non viene presentata come emissione realmente pubblicata sul sito.
- WeatherNext 2: media ensemble, intervalli nativi di sei ore, acquisizione ogni 12 ore. Nessuna interpolazione oraria né confronto improprio tra accumuli di sei ore e pioggia oraria.
- Radar DPC: fotogrammi storici su richiesta e download ufficiale GeoTIFF tramite API. Finestra massima 14 giorni, orari arrotondati a cinque minuti, URL di download limitati agli host ufficiali. Inquadratura sul centro abitato pubblico, senza coordinate private.

Documentazione: [Previous Runs](https://open-meteo.com/en/docs/previous-runs-api), [Ensemble](https://open-meteo.com/en/docs/ensemble-api), [API DPC](https://dpc-radar.readthedocs.io/it/latest/).

## Esercizio e compatibilità

- Migrazione additiva dello schema a versione 12; `location_model_runs` entra nei backup con conservazione locale di 90 giorni.
- Acquisizioni dentro il cron esistente, con cooldown separati e conservazione dell’ultimo prodotto in caso di errore. Un solo ciclo browser di 600 secondi.
- `V5_RESEARCH_ENABLED=false` disabilita le nuove acquisizioni WeatherNext/Previous Runs. Archiviazione e verifica dei dati locali continuano.
- Nessuna modifica al progetto Android o alla firma. L’interfaccia web resta compatibile con il contenitore esistente.
- I risultati locali diventano disponibili progressivamente, quando esistono abbastanza riscontri; l’integrazione di una fonte non garantisce che migliori la previsione.

## Verifiche

Suite Python, Ruff, audit privacy e controlli di sintassi JavaScript. Contratti visuali estesi a qualità, modelli, archivio ed eventi in entrambi i temi, desktop e mobile; esecuzione completa nel workflow GitHub. Nuovi test per causalità, isolamento delle stazioni, accumuli, lacune, sessioni, fonti native e revisioni pubblicate.

### Correzione dei riscontri dopo il primo rilascio

La verifica sui dati reali ha rilevato che mescolare timestamp ISO con e senza frazioni di secondo poteva escludere emissioni storiche valide, lasciando vuoti i riscontri di Roma. Il parser accetta ora entrambe le precisioni, mantenendo UTC. Test di regressione confrontano gli stessi risultati con fonti in ordine diverso. La cache della verifica ha una nuova revisione: si ricalcola nel successivo ciclo ordinario e poi rispetta nuovamente il cooldown di sei ore. Nessuna modifica alle misure archiviate, alle soglie di calibrazione o alla cadenza di dieci minuti.
