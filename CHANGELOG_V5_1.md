# Meteo Pro V5.1 · integrazione completa

## Aggiornamenti e dati

Un unico intervallo minimo di 600 secondi coordina Render e GitHub. Il flag
`--force-forecast` non scavalca più il blocco globale delle acquisizioni.
La riconciliazione richiesta viene accodata; il cron assorbe fino a 180 secondi
di anticipo rispetto al prossimo ciclo, conservando il lock tra i processi.
Eliminato il secondo controllo sull'ora di fine della misura, che provocava
salti di un turno. La risposta pubblica legge l'ultima revisione pubblicata e
usa ETag: nessuna seconda cache relativa di 600 secondi. Il browser conserva
un solo timer di 600 secondi e nessun refresh alla navigazione.

## Strumenti nella V5

- Radar osservato RainViewer con cursore e animazione, copertura e orario di
  composizione. Valori DPC/fulmini distinti. Inquadratura su centri abitati
  pubblici, senza coordinate private. RainViewer ha ritirato il nowcast API:
  le prossime tre ore sono etichettate come previsione modellistica.
- Ricerca città/CAP e preferiti, fuso della città e previsione internet senza
  attribuire misure della stazione alla città cercata.
- Pianificatore: otto target al massimo, finestra futura, soglie di quota/Luna,
  orizzonte manuale, geometria del sensore, FoV, grafico quote e CSV.
- Grafico ensemble con mediana e fascia P10–P90; scadenza a 12 ore, buchi
  preservati e descrizione corretta dell'incertezza. Comacchio ha un ensemble
  proprio, acquisito al massimo una volta l'ora con cooldown anche sugli errori.
- Scoring di Comacchio su 30 giorni: riferimento di persistenza causale,
  correzione del bias addestrata sull'80% iniziale e verifica sul 20% successivo.
  Nessuna applicazione automatica; almeno 30 giornate e 30 campioni di validazione
  per considerare un candidato con vantaggio di almeno il 10%.

## Funzioni personali

Diario locale con modifica, esportazione/importazione JSON validata e confronto
tra previsione archiviata e qualità osservata. Le note non vengono pubblicate.
Le bozze dei moduli restano durante il refresh. Il registro distingue eventi
rilevati dal dispositivo e push effettivamente ricevuti dal service worker.
Soglie pioggia/raffiche configurabili, eventi deduplicati e rientro solo con dati
completi. Finestre di attività per passeggiata, bici e lavori esterni con soglie
personalizzate, durata continua, ore diurne e gestione esplicita delle mancanze.

## Android e distribuzione

Sorgenti Android ricostruiti con widget e collegamenti rapidi, versionCode 5.
APK attuale mantenuto: il nuovo release deve essere firmato con il keystore
originale. Il widget rispetta i limiti Android (periodico 30 minuti); la pagina
aperta resta a dieci minuti. CI include compilazione e lint Android.
Vedere `android/README.md` per recupero firma e istruzioni Windows.

Schema 11 additivo: solo `v5_products`, incluso nei backup cifrati esistenti.
Nessuna modifica dei segreti o dei marker/cooldown del recupero storico profondo.
