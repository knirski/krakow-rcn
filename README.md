# Kraków Property Transaction Map

Interactive deck.gl map of all property transactions from [Rejestr Cen Nieruchomości (RCN)](https://eco.um.krakow.pl/rcn) for the city of Kraków.

Data is automatically downloaded, converted from GML, and deployed to GitHub Pages weekly.

## Data source

[Miasto Kraków - Rejestr Cen i Wartości Nieruchomości](https://eco.um.krakow.pl/rcn)

## How it works

1. Data is downloaded locally from `eco.um.krakow.pl` (the server blocks cloud IPs)
2. `convert.py` converts the GML to GeoPackage, resolving xlink cross-references between transactions, properties, plots, buildings, and apartments
3. `export_deckgl.py` exports joined transaction data as gzipped JSON
4. The output (`site/`) is committed and deployed to GitHub Pages on push

## Updating data

```bash
# Download and extract (must be done locally - server blocks cloud IPs)
curl -o rcn.zip "https://rzeczoznawca.eco.um.krakow.pl/RCN/1261_RCN.zip"
unzip rcn.zip -d gml_data

# Convert (needs gdal-bin)
python3 convert.py gml_data rcn.gpkg

# Export
python3 export_deckgl.py rcn.gpkg site

# Commit and push - Pages deploys automatically
git add site && git commit -m "Update data" && git push
```
