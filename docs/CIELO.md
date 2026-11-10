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

**Orizzonte da foto.** Apri **Crea da foto panoramica** nella stessa riga.
Usa una foto sferica 360° × 180° (2:1), livellata e presa dal punto del
telescopio. Indica sulla foto il Nord geografico e la quota 0°, quindi
traccia gli ostacoli tutt'intorno. La foto viene letta sul dispositivo;
nel profilo rimangono le coppie azimut e altezza. Rivedi il tracciato
prima di sostituire un orizzonte già salvato.

**Obiettivi per filtro.** In Cielo seleziona un soggetto e inserisci, per
ogni filtro, l'obiettivo, le ore già raccolte e la durata della posa.
**Confronta le tre notti** stima quanto una finestra può contribuire alle
ore mancanti. Usa buio, ostacoli, soglie meteo e lunari e i tempi di
preparazione della ricetta. Se c'è una finestra utile, **Usa la notte
consigliata** apre una bozza del piano con la finestra e i filtri ancora
da completare. Le stime non incrementano le ore raccolte: aggiornare
quelle ore dopo la sessione cambia il confronto successivo.

**Chiudi serata.** Da un progetto con obiettivi, carica il CSV o TSV
delle pose accettate. Il modello minimo ha intestazioni
`filtro;pose_valide;secondi_per_posa`. Le intestazioni del file si
possono associare a mano (anche per report del Tracker o di altri programmi).
Se il CSV contiene più soggetti, esiti o date di serata, associa anche
quelle colonne per escludere le righe che non appartengono alla serata.
Controlla anteprima e filtri prima del salvataggio. I totali del file
si aggiungono alle ore manuali. La stessa data e lotto sostituiscono il
riepilogo precedente, e lo stesso file non può essere contato in due lotti
dello stesso progetto. Puoi rimuovere un riepilogo dal registro.
FITS, percorsi e contenuto del CSV restano sul dispositivo.

**Confronta due postazioni.** Scegli due postazioni e un filtro ancora da
completare. Vedi la migliore finestra continua delle prossime tre notti
locali per ciascuna: le soglie e lo strumento sono uguali, orizzonte e fuso
seguono la postazione. Le ore utili sono stime verso l'obiettivo del filtro.
Il confronto è su richiesta e non cambia la postazione attiva.

**Ultimo controllo.** Oggi e Cielo affiancano l'orario del radar osservato
alle prossime ore previste. Radar e satellite si aprono dalla riga dei
dettagli. La fascia **La notte ora per ora** usa le finestre calcolate dal
pianificatore: tocca un intervallo per leggere soglie, nuvole, raffiche,
Luna e ostacoli pertinenti. Quando il sito dispone di due previsioni
confrontabili, un disaccordo rilevante mostra la stessa ora e lo stesso
strato di nuvole. Le postazioni senza dati comparabili non ereditano
i modelli di Roma.

**Inquadratura sul cielo.** Seleziona soggetto e strumento per sovrapporre
il sensore al campo DSS2 Color servito da [CDS HiPS2FITS](https://alasky.cds.unistra.fr/hips-image-services/hips2fits).
Puoi scegliere orientamento, griglia e sovrapposizione, e scaricare i centri
ICRS dei pannelli in CSV. Il servizio immagini riceve le coordinate
celesti del soggetto, senza la posizione della postazione. Quando
l'immagine non è raggiungibile restano il disegno e il CSV del mosaico.

**Verifica pioggia.** La scheda *Campioni per la calibrazione* distingue
gli intervalli di cinque minuti senza misure da quelli con pioggia non
utilizzabile. Mostra i giorni con più esclusioni nel periodo di raccolta.
La verifica delle due ore asciutte conserva i criteri e i dati precedenti.

**Account.** L'icona del profilo apre la scheda Account da ogni vista.
Mostra il metodo di accesso e le preferenze principali. Puoi scegliere
un nome, una foto, un colore e una nota privata. Preferiti, strumenti,
postazioni, orizzonti, progetti e piani si sincronizzano dopo l'accesso.
L'ospite può usarli sul dispositivo. Vedi [attivazione di Google](ACCOUNT_GOOGLE.md).

Le serie del grafico sono campionate a 15 minuti. Geometria, campo e budget
sono stime per organizzare una sessione; non misurano il seeing e non
governano le protezioni dell’attrezzatura.
