# Meteo Pro Android · sorgenti recuperabili e widget

Il file ricevuto `MeteoV4.apk` è una Trusted Web Activity (TWA): apre il sito
Meteo Pro nel browser Android. Le nuove pagine V5 sono quindi utilizzabili
anche dall'app già installata, senza sostituire l'APK.

## APK esistente verificato

- Identificatore: `com.gio9772rm.meteov4`.
- Versione Android: `versionCode=4`, `versionName=4`, target SDK 36.
- SHA-256 del file: `3f55d6292f1a36b431cf3cd1bef1e4c089cffbcd25d85882d435e5d86a3a19ad`.
- Certificato pubblico di firma, SHA-256:
  `2A:87:75:3D:55:EC:90:82:C0:56:3C:8D:12:51:33:F7:41:D1:F2:32:7F:5A:BA:FD:5C:A6:ED:3E:09:31:FD:DF`.

Questo progetto è una nuova implementazione della parte Android, non il
recupero dei sorgenti originali dall'APK. Mantiene l'identificatore e porta
`versionCode` a 5. La CI produce un APK release **non firmato**, da firmare con
la chiave originale. Non viene sostituito il download pubblico dell'APK attuale
finché manca una firma verificata.

## Cosa aggiunge

- Widget ridimensionabile con Roma/Comacchio selezionabili, temperatura,
  umidità, vento e data/ora italiana della misura. Nessun segreto meteo nell'app.
- Pulsanti per cambiare stazione, richiedere un aggiornamento e aprire il sito.
- Conservazione dell'ultima misura datata quando la rete non è disponibile.
- Collegamenti rapidi a radar e astronomia; navigazione limitata al dominio dell'app.
- Endpoint compatto `/api/v5/widget/{station_id}`.

La pagina aperta mantiene un solo aggiornamento automatico ogni 600 secondi.
Il widget segue i limiti Android: richiesta periodica ogni 30 minuti,
eventualmente rinviata dal risparmio energetico. Anche i tentativi manuali del
widget rispettano un minimo di dieci minuti per località. Non promette dati
live mentre Android ha sospeso il lavoro. Requisito della nuova shell: Android 6+
(API 23); il vecchio APK rimane disponibile per i dispositivi precedenti.

## Come trovare il file di firma su Windows

Sul PC usato per creare l'APK apri **Prompt dei comandi** e lancia:

```bat
where /r "%USERPROFILE%" *.jks *.keystore twa-manifest.json
where /r C:\Meteo *.jks *.keystore twa-manifest.json
```

La ricerca è in sola lettura e può durare qualche minuto. Controlla anche gli
ZIP scaricati quando hai creato l'app con PWABuilder/Bubblewrap: il pacchetto
Android può contenere `signing.keystore`, `android.keystore`, un `.jks`,
`twa-manifest.json` o istruzioni con alias della chiave. Se trovi il manifest,
il campo `signingKey` può indicare il percorso della chiave e l'alias.

**Occorrono il file `.jks`/`.keystore`, l'alias e le password associate.**
La chiave privata e le password non sono contenute nel certificato pubblico
dell'APK. Non aggiungerle al repository, a una issue o a un commento pubblico.
Puoi effettuare la firma direttamente sul tuo PC, così la chiave non esce dal
computer. Per leggere alias e impronta, `keytool` chiede la password senza
inserirla nella riga del comando:

```bat
keytool -list -v -keystore "C:\percorso\signing.keystore"
```

Verifica che l'impronta SHA-256 corrisponda a quella sopra. Se avevi usato un
servizio di generazione, recupera il pacchetto/backup da quel servizio.
Se la chiave è perduta e l'app era distribuita direttamente come APK, una
nuova chiave non produce un aggiornamento installabile sopra l'app esistente.
Play App Signing è un percorso diverso, applicabile solo se la distribuzione
originale era effettivamente tramite Google Play.

## Compilare e firmare

Apri la cartella `android` in Android Studio con JDK 17, Android SDK 36 e Gradle
8.13 (AGP 8.13.0). Il workflow del repository compila e controlla il progetto
anche senza installare Android Studio sul tuo PC. L'artefatto GitHub Actions
`meteo-pro-android-unsigned-...` contiene `app-release-unsigned.apk`.

Da una distribuzione locale di Gradle 8.13:

```bat
gradle -p android :app:assembleRelease :app:lintRelease
```

La firma si può effettuare con Android Studio, **Build → Generate Signed
Bundle / APK → APK**, selezionando il keystore originale, oppure con
`apksigner sign --ks ... --ks-key-alias ... --out MeteoPro.apk app-release-unsigned.apk`.
Lascia che lo strumento chieda le password. Verifica quindi con
`apksigner verify --verbose --print-certs MeteoPro.apk`.

Prima di sostituire il download pubblico serve la prova su telefono: aggiornare
sopra la versione 4 senza disinstallarla, aprire le pagine, aggiungere il widget,
cambiare stazione e verificare rete assente/risparmio energetico. La compilazione
e il lint non sostituiscono questa prova né dimostrano la compatibilità della firma.

Documentazione ufficiale:
- https://developer.android.com/studio/publish/app-signing
- https://developer.android.com/develop/ui/views/appwidgets/advanced
- https://github.com/GoogleChrome/android-browser-helper
