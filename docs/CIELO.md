# Cielo: dati, grafico e passaggio al piano

La scheda Cielo confronta tre serate locali, includendo quella già in corso
quando è ancora buio. L'intervallo di buio dipende dal profilo osservativo.
La vista Pro apre la stessa scheda con la postazione e il tema selezionati.

`POST /api/v5/tools/cielo` legge la fotografia meteo già pubblicata per Roma
o Comacchio. Per una postazione mondiale richiesta dall’utente, riusa una
previsione globale Open-Meteo per 15 minuti, con MET Norway come riserva.
Non avvia acquisizioni periodiche. Restituisce le tre sintesi e un solo grafico
con i soggetti per la data scelta. L'orario visualizzato usa il fuso della postazione;
intervalli, durata e cursore usano istanti UTC. Il browser riusa il risultato
quando si muove il cursore o si cambiano i filtri del catalogo. In assenza di
rete resta consultabile la previsione oraria della fotografia locale salvata.

I suggerimenti usano un gruppo di soggetti diffusi nei due emisferi, ordinati
per finestre praticabili e inquadratura. La ricerca separata trova nomi e
sigle in [OpenNGC](../assets_data/README.md), carica un soggetto per volta
nel grafico e lo rende selezionabile nel pianificatore.

Le finestre usano le soglie del pianificatore, il buio del profilo, l'altezza
del soggetto sopra gli ostacoli e la separazione dalla Luna quando è sopra
l'orizzonte. Meteo mancante o oltre soglia interrompe la finestra. La Luna,
il campo inquadrato e la compatibilità dei filtri sono stime orientative.
Il pulsante **Usa questa finestra** precompila una bozza con gli istanti esatti,
il soggetto, la postazione e le soglie; il calcolo del piano li ricontrolla.

**Postazioni mondiali.** Cerca una città oppure inserisci coordinate, quota
e fuso IANA. Una città salvata conserva il suo ID GeoNames anche dopo un
riavvio del servizio. Il meteo globale non usa la calibrazione di Roma o
Comacchio e non attiva avvisi periodici. Le coordinate immesse passano nel
corpo delle richieste e, se salvate con un account, nel profilo privato.
Il rapporto pubblico restituisce nome, ID e fuso; la previsione globale
richiede al fornitore il punto meteo della postazione.

**Budget.** La ricetta ripete una serie di pose per filtro nei blocchi
programmati. Stima pose intere, tempo di scaricamento, cambi filtro, autofocus
iniziale e periodico, dithering e riserva al passaggio del meridiano. Preparazione
e cambio soggetto sono già fuori dai blocchi. Il bilancio per notte e totale
si esporta in CSV; la [sequenza base N.I.N.A.](NINA_EXPORT.md) si esporta una
notte per volta.

**Orizzonte.** In Cielo scegli la postazione, apri **Orizzonte della
postazione** e carica un file `.hrz` o `.txt` con coppie azimut e altezza
in gradi (Nord 0°, Est 90°). L'anteprima mostra il profilo degli ostacoli.
Puoi scaricarlo di nuovo in formato `.hrz` o rimuoverlo. Cielo e Pianificatore
applicano la maschera alla postazione a cui appartiene. Nel Pianificatore
puoi specificare punti diversi per il singolo piano.

**Obiettivi per filtro.** In Cielo seleziona un soggetto e inserisci, per
ogni filtro, l'obiettivo, le ore già raccolte e la durata della posa.
**Confronta le tre notti** stima quanto una finestra può contribuire alle
ore mancanti. Usa buio, ostacoli, soglie meteo e lunari e i tempi di
preparazione della ricetta. Se c'è una finestra utile, **Usa la notte
consigliata** apre una bozza del piano con la finestra e i filtri ancora
da completare. Le stime non incrementano le ore raccolte: aggiornare
quelle ore dopo la sessione cambia il confronto successivo.

**Account.** L'icona del profilo apre la scheda Account da ogni vista.
Mostra il metodo di accesso e le preferenze principali. Puoi scegliere
un nome, una foto, un colore e una nota privata. Preferiti, strumenti,
postazioni, orizzonti, progetti e piani si sincronizzano dopo l'accesso.
L'ospite può usarli sul dispositivo. Vedi [attivazione di Google](ACCOUNT_GOOGLE.md).

Le serie del grafico sono campionate a 15 minuti. Geometria, campo e budget
sono stime per organizzare una sessione; non misurano il seeing e non
governano le protezioni dell’attrezzatura.
