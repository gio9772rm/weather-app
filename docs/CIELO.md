# Cielo: dati, grafico e passaggio al piano

La scheda Cielo confronta tre serate locali, includendo quella già in corso
quando è ancora buio. L'intervallo di buio dipende dal profilo osservativo.
La vista Pro apre la stessa scheda con la postazione e il tema selezionati.

`POST /api/v5/tools/cielo` legge la fotografia meteo già pubblicata per la
postazione abilitata. Non avvia acquisizioni, non chiama fornitori meteo e non
scrive configurazioni. Restituisce le tre sintesi e un solo grafico con i
soggetti per la data scelta. L'orario visualizzato usa il fuso della postazione;
intervalli, durata e cursore usano istanti UTC. Il browser riusa il risultato
quando si muove il cursore o si cambiano i filtri del catalogo. In assenza di
rete resta consultabile la previsione oraria della fotografia salvata.

Le finestre usano le soglie del pianificatore, il buio del profilo, l'altezza
del soggetto sopra gli ostacoli e la separazione dalla Luna quando è sopra
l'orizzonte. Meteo mancante o oltre soglia interrompe la finestra. La Luna,
il campo inquadrato e la compatibilità dei filtri sono stime orientative.
Il pulsante **Usa questa finestra** precompila una bozza con gli istanti esatti,
il soggetto, la postazione e le soglie; il calcolo del piano li ricontrolla.

I punti di integrazione per il seguito sono:

| Estensione | Dove si collega |
| --- | --- |
| Postazioni mondiali | `ObservingSite` e `ObservingContext` ricevono posizione privata, fuso IANA e fotografia dello stesso luogo. Aggiungere un risolutore per postazioni personali e una sorgente meteo esplicita; la correzione di una stazione non si applica altrove. Il contratto pubblico restituisce soltanto ID, nome e fuso. |
| Budget di ripresa e NINA | Le finestre continue di `target_sessions` sono l'ingresso di `optimize_schedule`. Aggiungere esposizioni, filtri, autofocus, dithering e meridian flip al budget del piano, prima dell'esportazione NINA esistente. Conservare gli istanti UTC per combinare notti e fusi. |
| Orizzonte e panorama | `target_geometry` usa `horizon_altitudes` sia per la vista Cielo sia per il piano. Import e export HRZ possono alimentare la maschera circolare della singola postazione; una futura immagine panoramica resta un livello visivo separato, privato. |

Il catalogo attuale resta quello del pianificatore. Le serie del grafico sono
campionate a 15 minuti e la geometria è approssimata. Queste stime sono per
organizzare una sessione e non sono misure di seeing o controlli di sicurezza.
