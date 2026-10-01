# V5.5 — Campioni, avvisi e distribuzione Android

La pagina **Quanto ci prende?** mostra l'avanzamento della raccolta ensemble,
separatamente per Roma e Comacchio: cicli, giorni con riscontri, finestre asciutte
e con pioggia, copertura, previsioni mancanti e osservazioni incomplete.
Le finestre ancora future rimangono in attesa. Finestre ripetute non aumentano il
campione e una lacuna non diventa assenza di pioggia.

I criteri prudenziali per iniziare lo studio sono 30 giorni con riscontri,
200 finestre, almeno 30 asciutte e 30 con pioggia distribuite su almeno 5 giorni.
Sono criteri operativi del progetto, non soglie universali di validità statistica.
Il loro raggiungimento non attiva una correzione: occorrono una verifica su giorni
successivi indipendenti e un vantaggio sui riferimenti. La calibrazione delle
probabilità resta disattivata. Non è promessa una data; la stima preliminare di
4–8 settimane dipende anche dalla pioggia e dalla continuità dei dati.

Le statistiche degli avvisi, visibili anche in **Avvisi personali**, confrontano
soglie di pioggia 30/40/60/80% e raffiche 30/40/60 km/h. Riportano ore centrate,
falsi allarmi, mancate, assenze corrette, precisione e richiamo con denominatori
espliciti. Si usa una sola previsione già pubblicata per ora, conosciuta 2–8 ore
prima dell'inizio dell'intervallo, su 30 giorni di archivio. Ogni ora osservata
richiede 12 campioni validi da cinque minuti. Sono simulazioni delle soglie,
non conteggi di notifiche consegnate; impostazioni personali e invii non cambiano.

La raccolta usa il cron esistente: nessuna nuova chiamata ai provider e nessun
nuovo timer browser. I due prodotti statistici si ricalcolano ogni sei ore;
il cambio di chiave della verifica finestre permette il primo calcolo al normale
ciclo dopo il deploy. Nessuna migrazione database. Radar esperto mantenuto nella
vista semplificata, aggiornamento meteo ogni 600 secondi.

## Android

Integrata la PR #75: Meteo Pro nativa **5.2.0**, identità distinta da MeteoV4,
schermata aggiornamenti, controlli OTA, download e installazione tramite Android.
La versione web è 5.5.0: non occorre un APK nuovo per modifiche alle pagine.
APK originale firmato verificato con Android Build Tools 35: firma v1/v2/v3,
un solo certificato atteso, identità/versione/SDK e SHA-256 controllati. Tutti i
404 file dell'APK non firmato del run CI 36175084524 (commit c05ca3c) coincidono;
le sole aggiunte sono le firme. Sorgenti `android/app` identici a quel commit.

APK e manifesto sono pubblicati insieme. Il parser della firma accetta le forme
`Signer #1` e `V3.0 Signer:` e rifiuta firmatari multipli o inattesi.
La firma automatica delle future versioni richiede i segreti Actions
`ANDROID_KEYSTORE_B64` e `ANDROID_KEYSTORE_PASSWORD`: la loro configurazione non
è verificabile dai connettori di questa sessione. Lo script già predisposto nel
backup privato della firma consente la configurazione da Windows con `gh`.
L'installazione e l'aggiornamento su telefono reale richiedono ancora collaudo.

## Dipendenze e verifiche

Integrati 16 aggiornamenti compatibili dalla proposta Dependabot #77. Conservati
Streamlit 1.62, NumPy 2.3 e SQLAlchemy 2.0 per evitare di unire migrazioni di questi
componenti alla ripresa; pytest resta nel ramo 8. Gli altri vincoli rimangono
bloccati in `constraints.txt`.

Verifiche: suite Python, Node, Ruff, audit privacy, firma APK e confronto con
l'artefatto CI. La suite visuale copre i nuovi pannelli su desktop, mobile e 320px,
i due temi, il radar e la pagina OTA. Il merge richiede CI completa verde.
