# Catalogo per la ricerca in Cielo

`openngc.tsv.gz` contiene dati adattati da **OpenNGC, Mattia Verga e contributori**,
[versione be3cd6c](https://github.com/mattiaverga/OpenNGC/tree/be3cd6c5498806a2fbb2f7e0452ef802dbb02be0),
distribuiti con licenza [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).
Il file derivato è distribuito con la stessa licenza. Le fonti originali sono
`database_files/NGC.csv` e `database_files/addendum.csv`; i crediti completi
sono nel [progetto OpenNGC](https://github.com/mattiaverga/OpenNGC#data-sources).

Modifiche: sono selezionati gli oggetti di cielo profondo con coordinate;
ascensione retta e declinazione sono convertite in gradi, gli identificativi
normalizzati, le colonne ridotte ai dati di ricerca e inquadratura. Il TSV
è compresso e non viene inviato al browser. Per rigenerarlo:

```sh
python scripts/build_sky_catalog.py NGC.csv addendum.csv
```
