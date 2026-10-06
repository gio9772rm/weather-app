# V5.6 — Storico compatto e calibrazione automatica delle finestre

## Spazio senza nuovi servizi a pagamento

- Lo storico di `forecast_ensemble_runs` oltre due giorni e dei tre archivi di
  punteggi oltre sette giorni passa in blocchi compressi nel medesimo database.
  L'ultima emissione resta sempre operativa, anche se il provider si interrompe.
- Nessuna cancellazione delle misure della stazione, delle traiettorie per le
  finestre, dei modelli locali o delle previsioni usate nelle verifiche.
- Ogni blocco conserva tutte le colonne e tutti i valori originali, con checksum
  SHA-256 e verifica della decompressione. Spostamento e rimozione dalle tabelle
  operative sono nella stessa transazione: un errore annulla entrambi.
- Lavoro incrementale dopo la pubblicazione, nel cron esistente: massimo 20
  blocchi da 2.500 righe e un budget di 35 secondi per ciclo. Un errore rinvia
  questa manutenzione, senza invalidare l'acquisizione meteo.
- Eliminato soltanto l'indice non univoco identico alla chiave primaria di
  `station_observations`. Lo schema verifica le colonne prima di rimuoverlo.
- Quando una tabella ha completato il passaggio, PostgreSQL ricopia soltanto le
  poche righe operative e verifica l'uguaglianza in entrambe le direzioni. Lo
  scambio è atomico, con attesa del lock limitata a due secondi, copia limitata
  a 100.000 righe e controllo prudenziale dello spazio. Niente `VACUUM FULL`.
  Dipendenze, privilegi personalizzati e altri schemi imprevisti impediscono
  lo scambio; il lavoro viene rinviato se un backup o lettore occupa la tabella.
- `iter_history()` in `compact_history.py` permette ricerca/esportazione di tutte
  le righe, recenti e compresse, con verifica dei blocchi e filtri temporali.
- Il backup portabile include `compact_archives`, ne verifica anche i checksum
  interni e ripristina i blocchi in SQLite. PostgreSQL usa una sola fotografia
  consistente e protegge tutte le tabelle interessate contro scambi simultanei.

Lo spazio fisico viene misurato dopo il passaggio, non dedotto dal numero di
righe cancellate. La compressione riduce la crescita ma non rende illimitato il
disco: restano lo storico osservativo e gli archivi deterministici già esistenti.

## Attivazione automatica prudente

La prima calibrazione automatica riguarda **due ore asciutte** (meno di 0,1 mm
per ciascuna ora), previste **6–12 ore prima**, per stazione e modello separati.
Le percentuali orarie di pioggia, gli altri anticipi e le finestre fotografiche
restano frequenze originali. Non si ricostruiscono traiettorie congiunte dai
vecchi quantili orari: quel vecchio archivio non sostituisce i riscontri delle
finestre raccolte prospetticamente dalla V5.3.

- Raccolta: almeno 30 giorni verificati, 200 finestre non sovrapposte, 30 asciutte,
  30 piovose e cinque giorni con pioggia nel campione.
- Un solo metodo fissato: regressione logistica monotona con regolarizzazione
  verso le frequenze originali; nessuna ricerca di parametri sui giorni di prova.
- Apprendimento: almeno 120 finestre, 20 giorni e 20 casi per ciascun esito.
  L'ultima osservazione di apprendimento precede la prima acquisizione della
  previsione del periodo di prova: si evita anche la contaminazione dovuta
  all'anticipo della previsione.
- Prova: ultimi sette giorni osservati, almeno 50 finestre, dieci casi per
  ciascun esito e due giorni piovosi. Miglioramento Brier di almeno il 10%
  rispetto sia alle frequenze originali sia al riferimento climatologico
  calcolato solo sull'apprendimento. Il log loss non deve peggiorare e il limite
  inferiore al 95%, ricampionando giorni interi, deve essere positivo per
  entrambi i confronti. Sono criteri operativi del progetto, non una garanzia.
- Superata la prova, si attivano automaticamente esattamente i coefficienti
  verificati. Si applicano solo a nuove previsioni, stesso modello/stazione,
  evento/anticipo verificato e campo di probabilità visto nell'apprendimento.
- Si conservano probabilità originale, corretta e identificativo della
  correzione nelle nuove previsioni archiviate prima degli eventi.
- Controllo ogni sei ore nel ciclo esistente, senza nuove chiamate a provider
  o timer meteo. Monitoraggio sui veri casi successivi all'attivazione:
  almeno sette giorni, 50 finestre e dieci casi per ciascun esito. Un peggioramento
  Brier oltre il 5% con evidenza nel ricampionamento per giorni sospende la
  correzione. Scadenza a 30 giorni, cambio modello o controllo oltre 12 ore
  riportano alle frequenze originali. Le nuove prove fallite sono distanziate
  di almeno sette giorni; nessun ritocco continuo per far passare una prova.
- La pioggia richiede ancora 12 campioni validi per ora; niente riempimento di
  buchi o uso di pioggia stimata. Una segnalazione specifica di umidità, temperatura
  o vento non invalida automaticamente il sensore pioggia. Non cambia il filtro
  delle altre verifiche deterministiche.
- Il sito mostra stato, requisiti, risultati della prova e percentuale originale
  accanto a quella corretta. Una copia offline o scaduta mostra le frequenze
  originali. L'APK 5.2.0 esistente continua a funzionare con il nuovo sito.

Al 6 ottobre i riepiloghi V5.5 riportano otto giorni verificati a Roma e nove a
Comacchio, senza finestre piovose complete nel campione selezionato. Il requisito
temporale è raggiungibile non prima di circa 21–22 giorni, verso il 27–28 ottobre,
se la raccolta continua. Non è una data promessa di attivazione: pioggia, numero
di casi, copertura e risultato della prova possono rinviarla.

## Verifica

Test di andata/ritorno con null, float, Unicode e testi multilinea; annullamento
in caso di errore; backup/ripristino; mantenimento dell'ultima emissione durante
un'interruzione. Un servizio PostgreSQL 18 isolato in CI verifica inoltre la
ricopia fisica, gli indici e la concorrenza con un backup.

Test della calibrazione su causalità, attivazione con beneficio, rifiuto senza
beneficio, campioni futuri, separazione dei modelli e delle stazioni, campo di
applicazione, valori originali conservati, scadenza, degrado e copertura pioggia.
Restano attivi tutti i controlli precedenti: Python, JavaScript, privacy,
contratti visuali desktop/mobile e build/lint/test Android.
