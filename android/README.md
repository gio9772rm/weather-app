# Meteo Pro Android 5.2 · OTA

Nuova app `com.gio9772rm.meteopro`, installabile accanto a `com.gio9772rm.meteov4`.
Non sostituisce la vecchia firma. I dati meteo e lo storico restano sul server;
preferenze browser, diario locale e iscrizioni push vanno verificati sul dispositivo.
Esportare il diario prima di cambiare browser o cancellarne i dati.

## Uso

Scaricare l'APK firmato da **Altro → App e aggiornamenti** sul sito.
Per la prima installazione Android richiede l'autorizzazione della fonte di download.
Tenere premuta l'icona dell'app → **Aggiornamenti**, oppure aprire la stessa schermata
tramite **App e aggiornamenti → Apri aggiornamenti nell'app**.

- Controllo all'apertura quando sono trascorse sei ore e lavoro periodico ogni sei ore.
  Android può ritardare il lavoro per risparmio energetico; non è una scadenza garantita.
- Download automatico predefinito su rete non a consumo; entrambe le opzioni si possono cambiare.
- Installazione con conferma; su Android 12+ si può abilitare l'installazione automatica.
  Occorre consentire installazioni da Meteo Pro. Anche in modalità automatica Android
  può richiedere una conferma: viene mostrata una notifica quando consentita.
- Il download è conservato in memoria privata dell'app. Sono verificati host HTTPS,
  dimensione, SHA-256, package, versione e certificato della versione installata.
  PackageInstaller verifica nuovamente le firme crittografiche. Nessun downgrade.
- I nuovi APK hanno URL immutabili. Il manifesto OTA e l'APK sono pubblicati insieme.
- L'APK contiene solo endpoint pubblici: nessuna chiave Ecowitt o credenziale database.

Le pagine web ricevono le novità al caricamento della nuova versione del sito/cache;
non serve un APK per ogni modifica web. La pagina meteo mantiene il ciclo di 600 secondi.
Il widget Roma/Comacchio ha un lavoro periodico di 30 minuti soggetto ai limiti Android,
con almeno 10 minuti tra tentativi per stazione. Gli aggiornamenti OTA non acquisiscono
misure meteo e non modificano questi intervalli.

## Compilazione e rilascio

JDK 17, SDK 36, Gradle 8.13 e AGP 8.13.0. Android minimo: 6 (API 23).
`android/release.json` definisce versione nativa e impronta pubblica della firma.
Incrementare **versionCode** e **versionName** solo quando serve un APK nuovo.
Aggiornare anche le note. Il sito può essere pubblicato indipendentemente.

```sh
gradle -p android :app:assembleRelease :app:lintRelease :app:testReleaseUnitTest
```

La CI produce un APK non firmato per ogni PR. Il workflow **Android OTA - Release**
può firmare e pubblicare solo dopo il successo della CI completa sul commit corrente
in main e se release.json richiede una versione superiore a quella pubblicata.
Non usa segreti su PR. Se manca la firma, segnala il rilascio in attesa e non pubblica
APK non firmati. Lo stesso numero di versione non viene mai sovrascritto.

Il rilascio automatico usa due segreti GitHub Actions del repository:
`ANDROID_KEYSTORE_B64` e `ANDROID_KEYSTORE_PASSWORD`; alias `meteo-pro`.
**Non aggiungere il keystore o la password al repository pubblico.**
Dal backup privato della firma, sul PC Windows già autenticato con `gh`, eseguire:

```powershell
powershell -ExecutionPolicy Bypass -File .\Configure-Signing.ps1
```

Lo script invia il keystore e la password esclusivamente ai due segreti GitHub Actions
di `gio9772rm/weather-app`, senza stamparli. Avvia poi il workflow di rilascio.
Il backup privato contiene quanto serve per la continuità delle firme: conservarne
una seconda copia protetta. La chiave pubblica/certificato non permette di ricrearlo.

Il workflow prepara APK e manifest, li verifica con `apksigner`/`aapt` e fa un normale
push senza forzare main. Una modifica concorrente blocca il push. Render distribuisce
il commit insieme. I commit del bot non innescano altre Actions; tutti i controlli
su sorgente e firma sono eseguiti prima. Una versione correttiva richiede un nuovo
versionCode; non si effettua downgrade dei telefoni.

## Firma locale

L'artefatto CI contiene l'APK non firmato e gli strumenti ufficiali SDK di firma.
Usare `apksigner sign --ks meteo-pro.jks --ks-key-alias meteo-pro ...` e lasciare
richiedere la password oppure usare un file/env locale. `android/release_tools.py`
verifica firma, package, versione e SDK prima di generare i file pubblici.
La distribuzione contiene esclusivamente l'APK firmato, mai il keystore.

Prima di considerare verificata l'installazione su un telefono reale: installare la
nuova app, aprire sito e schermata Aggiornamenti, aggiungere il widget, verificare
permessi notifiche/installazione, rete assente e annullamento di un aggiornamento.
Compilazione, lint e unit test non sostituiscono il collaudo sul dispositivo.

## Vecchia app

Il file `static/MeteoV4.apk` e la sua configurazione restano disponibili.
L'app precedente è una TWA Bubblewrap, versione 4, identità `com.gio9772rm.meteov4`.
La sua chiave privata non si ricava dall'APK. La nuova app usa una chiave distinta;
entrambe le identità sono dichiarate in `assetlinks.json`.

Riferimenti ufficiali:
- https://developer.android.com/reference/android/content/pm/PackageInstaller.SessionParams
- https://developer.android.com/studio/publish/app-signing
- https://developer.android.com/distribute/marketing-tools/alternative-distribution
