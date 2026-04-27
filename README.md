# ideam-dhime

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.19740803.svg)](https://doi.org/10.5281/zenodo.19740803)

Cliente Python para descargar series hidrometeorologicas desde DHIME (IDEAM, Colombia), con chunking por frecuencia, consolidacion de CSVs y catalogo maestro de estaciones embebido.

## Estado de la version

- Version actual: `0.3.0`
- Soporta descarga por lote y paralela.
- Soporta entrada minima por estacion: `station_code + variable_id`.
- Incluye catalogo maestro unificado en `docs/guia_estaciones.xlsx`.

## Instalacion

```bash
pip install -e .
```

## Donde se guardan los resultados

Los archivos se guardan en la carpeta indicada por `download_path`.

- Si usas `download_path="./datos_hidro"` y ejecutas desde la raiz del proyecto, se guardan en `./datos_hidro`.
- En lote, cada estacion genera un `*-final.csv` consolidado (si `merge_chunks=True`).

## Uso rapido (API publica)

### Una estacion

```python
from ideam_dhime import download_dhime_data

csv_final = download_dhime_data(
    download_path="./datos_hidro",
    station_code="21017020",
    variable_id=7,
    min_date="01/01/1970",  # opcional
)
print(csv_final)
```

### Lote (paralelo)

```python
from ideam_dhime import batch_download

requests = [
    {"download_path": "./datos_hidro", "station_code": "21017020", "variable_id": 7},
    {"download_path": "./datos_hidro", "station_code": "21017030", "variable_id": 7},
]

results = batch_download(
    requests=requests,
    download_path="./datos_hidro",
    parallel=True,
    workers=2,
    min_date="01/01/1970",  # opcional
    merge_chunks=True,
    keep_chunks=False,
)

for item in results:
    print(item.status, item.request.station_code, item.message, item.csv_final)
```

## Regla de fechas (v0.3.0)

- Si el usuario no pasa fechas, se consulta historico completo disponible (desde `01/01/1900` como piso tecnico).
- `min_date` permite imponer un piso global o por solicitud (por ejemplo `01/01/1970`).
- Si una ventana no tiene datos (`SIN_DATOS_EN_RANGO` o `SIN_SOLAPE`), se omite y se consolida lo que si exista.
- Solo falla una estacion si no hay datos en ninguna ventana o si ocurre un error tecnico real.

## Catalogo maestro de estaciones

- Archivo auditable: `docs/guia_estaciones.xlsx`.
- Snapshots runtime:
  - `ideam_dhime/stations_generated.py`
  - `ideam_dhime/locations_generated.py`
- Regeneracion desde carpeta local `CNE/`:

```bash
python -m ideam_dhime.regenerate_stations -v
```

Tambien disponible como script:

```bash
ideam-dhime-regenerate-stations -v
```

## Script de ejemplo

Ejecuta:

```bash
python examples/example_batch.py
```

Ese ejemplo ya viene preparado para:

- conjunto minimal de 10 estaciones,
- modo paralelo (`workers=2`),
- `min_date="01/01/1970"`.

## Comandos utiles de validacion

```bash
pytest -q
```

## Notas de empaquetado (PyPI)

- `docs/guia_estaciones_calidad.xlsx` no se publica como artefacto oficial de uso.
- Los resultados de descarga (`datos_hidro/`, `test_downloads/`) estan ignorados en `.gitignore`.

## Licencia

GNU GPL v3 o posterior. Ver `LICENSE`.
