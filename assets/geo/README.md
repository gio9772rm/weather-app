# Indice dei centri abitati

`public-cities.json.gz` è derivato da [GeoNames](https://www.geonames.org/export/),
file `cities15000.zip` e `countryInfo.txt` del 12 novembre 2026,
con licenza [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
Sono conservati solo identificativo, nome, Paese, fuso, popolazione e coordinate
pubbliche arrotondate. La popolazione aiuta a distinguere un centro metropolitano
dai quartieri indicizzati. Sono escluse le sezioni e le località abbandonate.
Le coordinate degli utenti e delle stazioni non sono incluse.

Per ricostruirlo dopo aver scaricato i due file dal
[server GeoNames](https://download.geonames.org/export/dump/):

```sh
python scripts/build_public_city_index.py cities15000.zip countryInfo.txt
```
