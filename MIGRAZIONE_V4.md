# Meteo V4 — prova, pubblicazione e ritorno alla V3

La V4 modifica soprattutto l'esperienza grafica. Non cancella né trasforma in modo distruttivo i dati PostgreSQL e non cambia il Cron Job di acquisizione.

## Sicurezze già predisposte

- V3 stabile: ramo `archive/meteo-v3-stable`;
- commit V3: `527e3a47ebdeefdc480d5dd007246f3d5a3c125d`;
- sviluppo V4: ramo `meteo-v4`;
- produzione Render: ramo `main`, quindi resta V3 fino al merge finale.

## Provare la V4 in locale su Windows 11

```bat
cd /d C:\Meteo\weather-app-v3-upload
git fetch origin
git switch -c meteo-v4 origin/meteo-v4
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m streamlit run app_streamlit.py
```

La V4 usa lo stesso `.env` e lo stesso database della V3. Per una prova completamente isolata, copia `.env` e imposta temporaneamente `DATABASE_URL=` e `SQLITE_PATH=./data/weather-v4-test.db`.

## Pubblicazione

La pull request V4 deve essere unita a `main` soltanto dopo:

1. CI verde;
2. prova di tema chiaro e scuro su desktop e telefono;
3. verifica di Stazione, Previsioni, Astronomia, Radar e Sistema;
4. creazione e verifica di un backup PostgreSQL;
5. conferma esplicita della pubblicazione.

Render distribuirà automaticamente il nuovo commit di `main`.

## Ripristino

Per un problema soltanto grafico, il ripristino più rapido è **Rollback** nella pagina del Web Service Render. Il Cron Job e il database non vengono modificati.

Per ripristinare stabilmente anche il codice, crea un nuovo commit che rimetta su `main` l'albero del ramo `archive/meteo-v3-stable`; non cancellare la cronologia con `git reset --hard` o force-push. La procedura può essere eseguita tramite una pull request di ripristino verificabile.
