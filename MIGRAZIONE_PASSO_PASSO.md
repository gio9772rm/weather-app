# Migrazione Meteo V3 — guida passo per passo

Questa procedura è pensata per Windows 11 e per il repository `gio9772rm/weather-app`. Non richiede di cancellare il database PostgreSQL.

## Prima di iniziare

Occorrono:

- Git for Windows oppure GitHub Desktop;
- Python 3.11 o 3.12;
- accesso amministratore al repository GitHub;
- accesso al servizio Render e al PostgreSQL già usato;
- il pacchetto di consegna Meteo V3.

Non pubblicare mai `.env`, la stringa `DATABASE_URL`, le chiavi Ecowitt/OpenWeather o il file storico della stazione.

## 1. Salva i dati privati

Nel vecchio progetto crea una cartella esterna, per esempio `C:\Meteo\backup-privato`, e copia:

- `.env`;
- `storico_stazione.xlsx` e ogni CSV/XLSX;
- `data\weather.db`, solo se contiene dati locali che vuoi conservare.

Da PowerShell, adattando il percorso:

```powershell
New-Item -ItemType Directory -Force C:\Meteo\backup-privato
Copy-Item C:\Meteo\weather-app\.env C:\Meteo\backup-privato\.env -ErrorAction SilentlyContinue
Copy-Item C:\Meteo\weather-app\storico_stazione.xlsx C:\Meteo\backup-privato\ -ErrorAction SilentlyContinue
Copy-Item C:\Meteo\weather-app\data\weather.db C:\Meteo\backup-privato\ -ErrorAction SilentlyContinue
```

Il nuovo ramo rimuove questi file dal repository pubblico, ma non modifica il PostgreSQL online.

Nota importante: un normale commit di eliminazione li toglie dalla versione corrente, ma non riscrive la cronologia Git precedente. Questa consegna non forza una riscrittura della storia, perché cambierebbe gli hash di tutti i commit e richiederebbe coordinamento. Se il foglio o i vecchi dump contengono informazioni che vuoi rimuovere anche dalla cronologia pubblica, completa prima la migrazione e poi pianifichiamo separatamente una pulizia con `git filter-repo`. Ruota subito le chiavi se sospetti che siano mai state inserite in un file tracciato.

## 2. Applica la modifica su un ramo separato

Estrai il pacchetto di consegna. Al suo interno troverai `weather-app-v3.patch`.

Apri PowerShell:

```powershell
cd C:\Meteo
git clone https://github.com/gio9772rm/weather-app.git weather-app-v3
cd weather-app-v3
git switch -c meteo-v3
git am --keep-cr "C:\Users\TUO_NOME\Downloads\Meteo-V3-consegna\weather-app-v3.patch"
```

Sostituisci il percorso del patch con quello reale. L'opzione `--keep-cr` serve perché alcuni file della versione precedente usavano terminazioni di riga Windows. Se `git am` segnala che il ramo contiene già la modifica, esegui `git am --abort` e controlla `git log -1`.

Il patch è solo un mezzo di trasporto e include anche le istruzioni tecniche per eliminare i vecchi file binari. Tienilo nella cartella Download: **non copiarlo dentro il repository, non aggiungerlo con `git add` e non caricarlo su GitHub**.

Apri poi la cartella con GitHub Desktop tramite **File → Add local repository** per vedere graficamente tutte le modifiche.

## 3. Configura e prova in locale

Nella cartella `C:\Meteo\weather-app-v3`:

1. fai doppio clic su `SETUP_WINDOWS.bat`;
2. apri `.env` con Blocco note;
3. recupera i valori dal tuo backup privato e usa i nuovi nomi mostrati in `.env.example`;
4. salva `.env`;
5. esegui `UPDATE_NOW.bat`;
6. esegui `RUN_WEATHER_APP.bat`;
7. visita `http://localhost:8501`.

Controlla che:

- la pillola **Stazione** sia verde e mostri un orario recente;
- la pillola **Previsioni** sia verde;
- nella tab **7 giorni** compaiano ore e schede;
- la tab **Astronomia** mostri il punteggio notturno;
- la pioggia attuale abbia due misure distinte: mm e mm/h.

Esegui anche i test:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\ruff.exe check .
```

Il risultato atteso è `10 passed` e `All checks passed`.

## 4. Configura GitHub Actions

Apri il repository su GitHub e vai in:

**Settings → Secrets and variables → Actions → Secrets**

Verifica o crea questi repository secrets:

| Secret | Nota |
|---|---|
| `DATABASE_URL` | URL esterno PostgreSQL completo, normalmente con SSL |
| `ECOWITT_APPLICATION_KEY` | consigliato; il workflow accetta anche il vecchio `ECOWITT_APP_KEY` |
| `ECOWITT_API_KEY` | API key Ecowitt |
| `ECOWITT_MAC` | MAC esatto della stazione |
| `OPENWEATHER_API_KEY` | opzionale ma consigliato per il secondo modello |

Nella scheda **Variables** crea:

| Variable | Esempio |
|---|---|
| `LAT` | `41.9028` |
| `LON` | `12.4964` |
| `ELEVATION_M` | `20` |
| `LOCATION_NAME` | `Mia stazione` |

Usa le coordinate reali della stazione, non necessariamente quelle dell'esempio.

## 5. Pubblica il ramo e apri la pull request

Da PowerShell:

```powershell
git status
git push -u origin meteo-v3
```

Su GitHub:

1. apri **Pull requests**;
2. scegli **New pull request**;
3. base `main`, compare `meteo-v3`;
4. attendi il check **Meteo V3 - Test**;
5. apri **Files changed** e verifica che `.env`, database e fogli storici non siano presenti;
6. fai **Merge pull request** solo quando il check è verde.

## 6. Avvia la prima pipeline cloud

Dopo il merge:

1. apri la scheda **Actions**;
2. seleziona **Meteo V3 - Pipeline**;
3. premi **Run workflow**;
4. lascia attivo **Aggiorna subito anche le previsioni**;
5. attendi il segno verde;
6. apri i log e verifica le righe `Stazione:` e `Previsioni:`. Non incollare pubblicamente eventuali log completi.

Il primo avvio crea le nuove tabelle e aggiunge colonne a quelle esistenti. L'operazione è idempotente: eseguirla più volte non duplica i record.

La calibrazione dei provider non appare immediatamente. Servono previsioni archiviate che siano poi diventate osservabili; i primi punteggi arrivano normalmente nelle ore successive.

## 7. Disattiva il cron duplicato su Render

Questa operazione è manuale e importante: evita che GitHub e Render scrivano contemporaneamente.

1. apri **Render Dashboard**;
2. seleziona il vecchio servizio cron, normalmente `weather-ingest-ecowitt`;
3. apri **Settings**;
4. scegli **Suspend** per le prime 24 ore; non cancellarlo subito;
5. non sospendere il PostgreSQL e non cancellare il servizio web;
6. dopo 24 ore di pipeline GitHub regolare, puoi eliminare definitivamente il vecchio cron.

## 8. Aggiorna il servizio web Render

Se il servizio esiste già:

1. aprilo su Render;
2. verifica che punti al ramo `main`;
3. usa build command `pip install -r requirements.txt`;
4. usa start command `streamlit run app_streamlit.py --server.address 0.0.0.0 --server.port $PORT`;
5. lascia come secret soltanto `DATABASE_URL`;
6. configura `LOCATION_NAME`, `LAT`, `LON`, `ELEVATION_M` e `LOCAL_TZ=Europe/Rome`;
7. premi **Manual Deploy → Deploy latest commit**.

Le chiavi Ecowitt e OpenWeather servono al workflow GitHub, non alla dashboard pubblica Render.

Se gestisci Render tramite Blueprint, sincronizza il nuovo `render.yaml`: contiene soltanto il servizio web e non crea un secondo cron.

## 9. Verifica finale

Nelle prime 24 ore controlla:

- GitHub Actions ogni 10 minuti senza errori ripetuti;
- orario dell'ultimo dato stazione inferiore a 45 minuti;
- emissione previsione inferiore a 3 ore;
- nessun cron Render ancora attivo;
- crescita regolare delle tabelle `forecast_runs`, `forecast_scores` e `forecast_blend`;
- assenza di `.env`, `.db`, `.xlsx` e dump Ecowitt nella scheda **Code** di GitHub.

Il comando locale `python check_db.py` mostra conteggi e orari senza stampare credenziali o valori meteorologici.

## 10. Importa lo storico senza pubblicarlo

Metti il foglio nella cartella privata e, con `DATABASE_URL` configurata nel `.env` locale, esegui:

```powershell
.\.venv\Scripts\python.exe import_historical.py "C:\Meteo\backup-privato\storico_stazione.xlsx"
```

Il file resta sul PC. Non aggiungerlo mai a Git.

## Problemi comuni

### `Credenziali Ecowitt mancanti`

Controlla i tre secret Ecowitt. `ECOWITT_APPLICATION_KEY` e `ECOWITT_APP_KEY` sono alternative; API key e MAC sono sempre richiesti.

### OpenWeather non funziona

La pipeline continua con Open‑Meteo. Correggi `OPENWEATHER_API_KEY` e forza una nuova previsione dalla pagina Actions.

### Database non raggiungibile

Verifica che `DATABASE_URL` sia l'URL esterno del PostgreSQL, che il database sia attivo e che l'URL richieda SSL quando previsto dal provider.

### Dashboard online ma dati vecchi

Controlla prima GitHub Actions. Se il job è verde, verifica che Actions e Render usino la stessa `DATABASE_URL`.

### Il patch non si applica

Non forzare. Esegui:

```powershell
git am --abort
git status
```

Poi riparti da un clone pulito di `main` oppure usa il sorgente completo incluso nel pacchetto come riferimento.

## Rollback

Se la pipeline cloud presenta un problema:

1. sospendi **Meteo V3 - Pipeline** da GitHub Actions;
2. riattiva temporaneamente il vecchio cron Render, se lo avevi soltanto sospeso;
3. su GitHub usa **Revert** sulla pull request V3 oppure crea un revert con Git;
4. ridistribuisci l'ultimo commit stabile su Render.

Le modifiche al database sono additive. Le vecchie versioni ignorano le nuove tabelle, quindi non è necessario cancellarle durante un rollback.
