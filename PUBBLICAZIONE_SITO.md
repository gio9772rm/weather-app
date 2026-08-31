# Preparazione del sito pubblico

L'indirizzo Render corrente è già raggiungibile da Internet, ma non va considerato
un lancio pubblico completo. Prima di promuoverlo a un pubblico ampio conviene
separare nettamente la dashboard consultabile dagli strumenti operativi.

## Architettura consigliata

1. **Area pubblica**: Oggi, previsioni, aria, astronomia e radar, con posizione
   arrotondata, fonti, data di aggiornamento e fallback sempre visibili.
2. **Area operativa protetta**: Sistema, diagnostica Ecowitt, errori delle fonti,
   stato backup e comandi amministrativi dietro autenticazione.
3. **Dati personali nel browser**: target RA/Dec, profili ottici, orizzonte e diario
   restano nella sessione o in export locale; non diventano record pubblici.
4. **Backend unico**: Ecowitt resta primaria; il sito pubblico legge dal database
   e non avvia ingestioni concorrenti.

## Lavori necessari prima del lancio

- introdurre un flag server-side `PUBLIC_MODE` che non si limiti a nascondere
  elementi via CSS, ma non costruisca proprio viste e query amministrative;
- aggiungere autenticazione per l'area operativa e ruoli distinti lettore/admin;
- scegliere dominio e nome pubblico, configurare DNS su Render e verificare il
  certificato TLS gestito automaticamente;
- aggiungere pagina Informazioni, contatti, privacy, attribuzioni/licenze delle
  fonti e una spiegazione chiara che previsioni e score astronomici non sono
  allerte ufficiali né misure di seeing;
- definire limiti di traffico, cache per query costose, timeout e degradazione
  controllata delle fonti esterne; nessuna chiave deve arrivare al browser;
- configurare monitoraggio sintetico della home e delle viste principali,
  budget di latenza/errori e procedura di rollback;
- verificare accessibilità WCAG, navigazione tastiera, mobile, contrasto,
  metadati social, favicon, sitemap e pagina di errore;
- svolgere un test pubblico limitato prima dell'indicizzazione generale.

## Percorso di rilascio

1. Creare la modalità pubblica e i test che provano l'assenza di diagnostica,
   coordinate precise, segreti e dati di sessione.
2. Pubblicare una Preview Render separata, alimentata dallo stesso schema in sola
   lettura o da dati sanitizzati.
3. Eseguire test di carico e accessibilità, quindi collegare il dominio.
4. Aprire gradualmente l'accesso e mantenere la dashboard amministrativa su un
   ingresso autenticato distinto.

Riferimenti operativi: [domini personalizzati Render](https://render.com/docs/custom-domains),
[servizi web e health check](https://render.com/docs/web-services),
[gestione dei segreti Streamlit](https://docs.streamlit.io/develop/concepts/connections/secrets-management)
e [autenticazione Streamlit](https://docs.streamlit.io/develop/concepts/connections/authentication).

