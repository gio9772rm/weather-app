# Attivare Accedi con Google per Meteo Pro

Il sito funziona come prima quando `GOOGLE_CLIENT_ID` non è configurato.
L'accesso è facoltativo e gli account Meteo Pro esistenti restano disponibili.

1. Apri [Google Auth Platform](https://console.cloud.google.com/auth/clients)
   nel progetto Google Cloud da usare per Meteo Pro. Configura Branding e
   Pubblico. Per il primo collaudo, aggiungi il tuo indirizzo tra gli utenti di
   test se il progetto è in modalità di test.
2. In **Client → Crea client**, scegli **Applicazione web**. In **Origini
   JavaScript autorizzate** aggiungi `https://weather-app-v3-w2jd.onrender.com`
   senza percorso o barra finale. Se usi anche un dominio pubblico diverso,
   aggiungi la sua origine. Questa integrazione usa il pulsante Google in
   una finestra di accesso: non richiede un URI di reindirizzamento né un
   client secret.
3. Nelle variabili di ambiente del **servizio web Meteo Pro** su Render,
   aggiungi `GOOGLE_CLIENT_ID` con il valore che termina in
   `.apps.googleusercontent.com`. Salva e rilascia il servizio.
4. Da Account, con la sessione Meteo Pro già aperta, usa **Collega account
   Google**. Esci e torna con **Continua con Google** per verificare che
   preferiti e piani siano gli stessi. Per un nuovo utente, Google crea un
   profilo e mostra il codice di recupero da conservare.

Il server verifica il token Google, il destinatario e il nonce monouso.
Il collegamento usa l'identità Google verificata, senza unire account che
hanno la stessa email. Meteo Pro non richiede accesso a Gmail, Drive o
Calendar. Il codice di recupero consente di impostare una password di
riserva anche per i profili creati con Google.

[Istruzioni ufficiali Google per il client web](https://developers.google.com/identity/gsi/web/guides/get-google-api-clientid).
