# Esportazione N.I.N.A.

Dal risultato del pianificatore, scegliere **Esporta sequenza N.I.N.A.**. È disponibile per Fotografia cielo profondo quando esiste almeno un blocco utilizzabile. Impostare esposizione, gain, offset e binning; −1 per gain/offset mantiene il valore predefinito della camera in N.I.N.A.

Aprire il JSON nel **sequenziatore avanzato N.I.N.A. 3.2**, nella notte indicata nel nome della sequenza. Il fuso del PC deve coincidere con quello del piano. Le istruzioni native «Wait for time» e «Loop until time» memorizzano l’ora locale, non una data assoluta. Il passaggio di mezzanotte usa il rollover notturno di N.I.N.A.; le notti con cambio di ora legale vengono rifiutate dall’esportatore.

La sequenza contiene:

1. Le tre aree native Start, Targets ed End.
2. Un contenitore DSO per blocco, con coordinate J2000 e angolo di campo del piano.
3. Attesa fino all’inizio della preparazione, centratura con plate solving e attesa fino all’inizio delle riprese.
4. Pose LIGHT ripetute fino alla fine del blocco, protette anche dal limite orario del DSO. Un errore di centratura salta il contenitore. Se selezionato, il trigger nativo di flip al meridiano usa il profilo N.I.N.A.

Start ed End sono vuote: completarle per la propria attrezzatura. Connessione, raffreddamento, filtro, fuoco, guida, parcheggio e sicurezza meteo dipendono dal profilo dell’utente. La rotazione è annotata nel target; non viene aggiunto un comando al rotatore. Il tempo utile può diminuire se centratura o flip durano più della preparazione prevista. La previsione non aggiorna automaticamente una sequenza già scaricata.

L’esportazione è locale nel browser e non richiede plugin N.I.N.A. Non legge né carica sequenze personali sul server. Rifiuta un piano più vecchio di venti minuti, blocchi già iniziati, sovrapposizioni, pose più lunghe del blocco e parametri invalidi. La conferma nel modulo riguarda l’uso della sequenza scaricata; non avvia hardware.

## Contratto e verifica

Serializzazione originale realizzata sui nomi/proprietà pubbliche del [repository ufficiale N.I.N.A.](https://github.com/isbeorn/nina), senza includere sequenze personali o codice sorgente upstream:

- [Corpus di compatibilità 3.2](https://github.com/isbeorn/nina/blob/bfc7277ca9b7bac52d82e0494593735bd43a7eef/NINA.Test/Sequencer/Serialization/LegacySequences/v3.2/master-3.2-all-sequence-entities.sequence.json).
- [Manifest del corpus](https://github.com/isbeorn/nina/blob/bfc7277ca9b7bac52d82e0494593735bd43a7eef/NINA.Test/Sequencer/Serialization/LegacySequences/v3.2/master-3.2-all-sequence-entities.manifest.json), generato dal commit `2393eae581145ed5b8114bf07c48ca2580540fd5`.
- [InputCoordinates](https://github.com/isbeorn/nina/blob/2393eae581145ed5b8114bf07c48ca2580540fd5/NINA.Astrometry/InputCoordinates.cs): `DecDegrees` con segno e `NegativeDec` anche per declinazioni fra −1° e 0°.
- [TimeCondition](https://github.com/isbeorn/nina/blob/2393eae581145ed5b8114bf07c48ca2580540fd5/NINA.Sequencer/Conditions/TimeCondition.cs), [TimeProvider](https://github.com/isbeorn/nina/blob/2393eae581145ed5b8114bf07c48ca2580540fd5/NINA.Sequencer/Utility/DateTimeProvider/TimeProvider.cs) e [TakeExposure](https://github.com/isbeorn/nina/blob/2393eae581145ed5b8114bf07c48ca2580540fd5/NINA.Sequencer/SequenceItem/Imaging/TakeExposure.cs).

I test controllano struttura, campi, riferimenti, orari, limiti e download reale. Non costituiscono una prova di importazione/esecuzione nell’applicazione Windows né di compatibilità con driver o plugin dell’attrezzatura: prima dell’avvio, rivedere la sequenza in N.I.N.A.
