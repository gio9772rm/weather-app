# Archivio storico R2: configurazione e recupero

Prima della configurazione il sito continua normalmente: i blocchi compressi
restano in PostgreSQL. R2 conserva le vecchie emissioni e verifiche senza nuove
chiamate ai provider meteo. Dati recenti per previsioni e calibrazione restano
locali. Le misure delle stazioni non vengono eliminate o trasferite dalle
tabelle operative.

## Cloudflare

1. Accedere a [Cloudflare](https://dash.cloudflare.com/), aprire **Storage &
   databases → R2 Object Storage** e completare l'attivazione. Possono essere
   richiesti dati di fatturazione anche per utilizzi nella quota gratuita.
2. Creare un bucket dedicato, per esempio `meteo-history`, con classe **Standard**.
   La quota gratuita non riguarda Infrequent Access. Se si sceglie la
   giurisdizione EU, usare l'endpoint EU mostrato dal pannello.
3. Mantenere il bucket privato: non abilitare `r2.dev` o domini pubblici e non
   impostare regole di scadenza/eliminazione degli oggetti.
4. In **Manage API tokens**, creare un token **Object Read & Write** limitato
   a questo bucket. Salvare Access Key ID, Secret Access Key ed endpoint S3.
   Il secret viene mostrato una volta sola. Non servono permessi amministrativi,
   Workers o altri servizi.

## GitHub: preparare il recupero prima dei trasferimenti

In **Settings → Secrets and variables → Actions → New repository secret**
creare `R2_ENDPOINT`, `R2_BUCKET`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`.
I valori sono quelli ottenuti in Cloudflare. Per GitHub è sufficiente un secondo
token **Object Read Only** sullo stesso bucket; il workflow di ripristino usa
solo letture. Il token Read & Write funziona ugualmente.

Non inviare chiavi in chat e non inserirle nella repo. Le due chiavi S3 non
sono il token Cloudflare REST.

## Render

Creare un **Environment Group** e collegarlo sia al servizio web
`weather-app-v3` sia al cron `weather-app`, oppure inserire queste variabili
in **Environment** di entrambi:

| Variabile | Valore |
| --- | --- |
| `R2_ARCHIVE_ENABLED` | `true` |
| `R2_ENDPOINT` | Endpoint S3 mostrato in Cloudflare |
| `R2_BUCKET` | `meteo-history`, o il nome scelto |
| `R2_ACCESS_KEY_ID` | Chiave del token Read & Write |
| `R2_SECRET_ACCESS_KEY` | Secret della chiave |
| `R2_ARCHIVE_MAX_BYTES` | `8000000000`, opzionale: già predefinito |

L'endpoint ordinario è `https://ACCOUNT_ID.r2.cloudflarestorage.com`; quello
EU è `https://ACCOUNT_ID.eu.r2.cloudflarestorage.com`. Copiare il valore reale
dal pannello, senza nome del bucket alla fine. Variabili già presenti a livello
di servizio prevalgono sul gruppo e devono essere coerenti. Salvare e
ridistribuire entrambi i servizi per applicare i valori.

Il prossimo cron ordinario, entro circa 10 minuti oltre il tempo del deploy,
inizia il trasferimento. Il passaggio iniziale richiede più cicli: massimo
10 blocchi e budget di 20 secondi per ciclo, dopo la pubblicazione meteo.
In **Sistema** controllare stato R2, blocchi trasferiti, blocchi in attesa e
spazio inventariato nel bucket.

## Integrità e spazio

- Le chiavi degli oggetti derivano dal contenuto; scritture condizionali
  impediscono sovrascritture. Dopo ogni upload il cron rilegge l'intero oggetto,
  verifica hash, codec e conteggio e solo dopo libera il payload nel DB.
  Errori conservano la copia locale. Upload senza commit vengono riutilizzati.
- Il cron conta tutto il bucket, anche oggetti estranei o non indicizzati.
  I nuovi caricamenti si fermano a **8 GB**. Si può abbassare il limite;
  valori superiori sono rifiutati. Usare un bucket dedicato all'app: altri
  scrittori concorrenti possono consumare spazio fra i controlli.
- I **10 GB-mese Standard**, un milione di operazioni Class A e dieci milioni
  Class B gratuiti sono condivisi tra tutti i bucket dell'account. Il limite
  dell'app controlla lo spazio, non è un blocco di fatturazione Cloudflare e
  non controlla richieste manuali o altre applicazioni. Monitorare anche il
  pannello Cloudflare. Il normale uso dell'app richiede poche operazioni
  rispetto alle quote previste.
- Se R2 non risponde o il limite è pieno, i nuovi blocchi restano compressi nel
  database e previsioni/calibrazione continuano. Lo storico già trasferito
  richiede R2 per la consultazione completa: un oggetto mancante produce un
  errore esplicito, senza presentare una serie incompleta come valida.
- Endpoint e bucket sono identificati negli indici. Cambiarli dopo il primo
  trasferimento viene rifiutato; ruotare le chiavi sullo stesso bucket è
  consentito. `R2_ARCHIVE_ENABLED=false` sospende nuovi upload: mantenere le
  altre variabili per leggere lo storico già esterno.
- Non vengono eliminati oggetti R2. Dopo il primo trasferimento completo,
  una ricopia atomica controllata dell'indice libera anche spazio fisico
  PostgreSQL. Lock occupati, dipendenze inattese o spazio insufficiente
  rinviano il lavoro. Nessun `VACUUM FULL`.

## Dati operativi e Comacchio

Finestre, modelli e previsioni locali mantengono **90 giorni** locali; modelli
primari e blend almeno **120**, misure ufficiali **180**, punteggi **7**, quantili
ensemble **2**. Il periodo primario aumenta se una configurazione personalizzata
richiede più storia per i punteggi. L'ultima emissione viene mantenuta per
sorgente/stazione anche durante lunghe interruzioni. Le precedenti eliminazioni
dello storico meteo a scadenza diventano archiviazioni senza perdita.
Solo i log operativi continuano a scadere dopo 30 giorni.

Comacchio ha acquisizione, previsioni, verifiche e calibrazione proprie;
non corregge automaticamente Roma e viene mantenuta. Il 7 ottobre 2026 i suoi
valori memorizzati occupano circa **9,1 MB** nei tre archivi principali,
esclusi indici, pagine libere e strutture ausiliarie. Il possibile risparmio
fisico è di ordine **10–15 MB**, non centinaia. Eliminare righe da PostgreSQL
non riduce necessariamente i file allocati. L'archivio R2 affronta la crescita
delle emissioni senza perdere il servizio della seconda stazione.

## Backup e ripristino

I tre backup giornalieri cifrati su GitHub conservano dati operativi, blocchi
ancora locali e indice completo degli oggetti R2. Il manifest v3 dichiara
numero, dimensione e destinazioni dei blocchi esterni necessari. ZIP e indice
si verificano anche durante un'interruzione R2; non si riscaricano tutti gli
anni di storico ogni giorno e non si moltiplica per tre lo spazio R2.

I tre ZIP con riferimenti dipendono dal bucket per lo storico esterno e **non
sono tre copie autonome di quei dati**. Per una seconda copia indipendente,
creare periodicamente uno ZIP completo sul PC, con le quattro variabili R2
configurate in modo sicuro anche sul PC, oltre a `DATABASE_URL`:

```cmd
python backup_database.py --output backups\completo.zip --include-external-history
python backup_database.py --verify backups\completo.zip
```

Il file contiene dati privati: conservarlo su supporto protetto. La rotazione
GitHub riguarda solo i backup giornalieri, non file manuali sul PC.

Il workflow **Prova ripristino mensile**, eseguibile anche da **Actions → Run
workflow**, verifica lo ZIP cifrato e ricostruisce in un SQLite nuovo **tutti
gli oggetti R2**, uno per volta. Oggetti mancanti, checksum diversi o chiavi
mancanti fanno fallire la prova e aprono l'avviso operativo. L'esercizio non
modifica il database di produzione. Verifica e ripristino leggono i CSV in
streaming per contenere la RAM anche con anni di storico.

Recupero manuale in un file non ancora esistente:

```cmd
python backup_database.py --restore backups\giornaliero.zip --restore-sqlite data\recupero.sqlite --materialize-external-history
```

Senza `--materialize-external-history`, la copia ripristinata conserva i
riferimenti e richiede lo stesso R2 per leggere tutto lo storico. I backup
v1/v2 esistenti restano leggibili e autosufficienti. Il ripristino non
sovrascrive database locali esistenti.

Fonti ufficiali: [attivazione](https://developers.cloudflare.com/r2/get-started/),
[quote Standard](https://developers.cloudflare.com/r2/pricing/),
[token ed endpoint](https://developers.cloudflare.com/r2/api/tokens/),
[bucket privati](https://developers.cloudflare.com/r2/buckets/create-buckets/),
[S3](https://developers.cloudflare.com/r2/api/s3/api/).
