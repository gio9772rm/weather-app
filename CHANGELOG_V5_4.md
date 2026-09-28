# Meteo Pro V5.4

## Riepilogo e affidabilità

- «Le prossime ore» riassume sei intervalli orari: primo segnale di pioggia, picco di vento/raffica, nuvolosità ed eventuali schiarite. Espone orari, quantità, probabilità e copertura. I buchi non diventano ore asciutte o serene.
- L’affidabilità distingue aggiornamento delle misure/fotografia, completezza delle prossime dodici ore, disaccordo termico fra modelli e riscontri delle probabilità ensemble. Le copie offline e le emissioni vecchie restano riconoscibili.
- Il confronto termico usa solo emissioni acquisite dal vivo, non più vecchie di dodici ore, della stessa stazione e per le stesse ore. Ogni modello conta una volta per ora. Esclude ricostruzioni retrospettive, valori mancanti e la previsione combinata. Lo scarto di 3 °C è una soglia descrittiva, non una probabilità di successo.
- La tabella oraria sostituisce la precedente percentuale euristica di «Fiducia» con la completezza dei dati del blocco.

## Pianificatore

- Percorso in tre passaggi: notte, strumento, oggetti. Soglie, ostacoli e priorità sono nei dettagli espandibili. Navigazione da tastiera, convalida del passaggio e riepilogo prima del calcolo.
- Il refresh conserva passaggio e campi in compilazione; le bozze restano separate per stazione. Un calcolo fallito non associa nuove impostazioni al risultato precedente.
- Esportazione JSON per il sequenziatore avanzato N.I.N.A. 3.2 dai blocchi utilizzabili del piano fotografico. Coordinate J2000, attese negli orari locali, centratura, pose LIGHT e limiti orari. Gain/offset, binning, esposizione e flip al meridiano sono rivedibili prima del download.
- L’esportazione rifiuta piani scaduti, blocchi trascorsi/sovrapposti, coordinate assenti e notti con cambio di ora legale. Vedi [formato e limiti N.I.N.A.](docs/NINA_EXPORT.md).

## Calibrazione ensemble

Il punto 5 resta in raccolta. Alla verifica del 28 settembre 2026 erano disponibili tre acquisizioni ensemble per stazione, tutte della stessa giornata, e nessuna finestra prospettica già verificata. Questo campione non giustifica una ricalibrazione. Continua la verifica esistente su finestre non sovrapposte e osservazioni successive; nessuna probabilità nuova viene presentata come calibrata.

Nessuna migrazione di schema, nuovo timer, nuova chiamata ai provider o modifica alle firme Android. I dati personali non entrano nelle fotografie meteo pubbliche.

## Verifica

- Suite Python, controlli Ruff e audit privacy.
- Test Node su dati mancanti, continuità, freschezza, struttura/riferimenti N.I.N.A., coordinate negative, mezzanotte, input invalidi e cambio di ora legale.
- Percorso reale nel browser in tema chiaro/scuro su desktop e mobile: tre passaggi, bozza conservata al refresh, calcolo dal server e download JSON. Restano attivi i controlli multi-browser sui profili, sui conflitti e sull’offline.
- Il JSON viene confrontato con i contratti ufficiali di N.I.N.A.; l’applicazione Windows e l’attrezzatura non vengono eseguite dai test Linux.
