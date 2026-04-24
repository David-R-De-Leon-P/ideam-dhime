# ideam-dhime

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.19740803.svg)](https://doi.org/10.5281/zenodo.19740803)

Cliente en **Python** para descargar series hidrometeorológicas desde el portal **DHIME** del [IDEAM](https://www.ideam.gov.co/) (Colombia). Automatiza el flujo en el navegador con **Selenium** (Chrome), extrae el ZIP y renombra el CSV resultante.

> **Aviso:** el portal DHIME puede cambiar su HTML o políticas de uso. Este paquete replica un flujo probado en un momento dado; si el sitio cambia, tal vez haya que actualizar selectores/tiempos o **regenerar el catálogo de variables** (ver más abajo).

## Requisitos

- Python **3.9** o superior
- **Google Chrome** instalado (Selenium 4 gestiona el controlador)
- Conexión a Internet

## Instalación

```bash
pip install -e .
```

Tras publicar en GitHub:

```bash
pip install git+https://github.com/David-R-De-Leon-P/IDEAM_scrapping.git
```

Esto instala:

- el paquete importable `ideam_dhime`,
- el **catálogo de variables** (`VARIABLES_IDEAM`) con su fecha de generación,
- el comando de consola **`ideam-dhime-regenerate`** para actualizar el catálogo.

## Estructura del proyecto

```
IDEAM_scrapping/
├── ideam_dhime/                 # Paquete Python instalable
│   ├── __init__.py              # API pública
│   ├── scraper.py               # download_dhime_data (una estación)
│   ├── batch.py                 # batch_download (lote, paralelo opcional)
│   ├── session.py               # DHIMESession (Chrome reutilizable + chunking)
│   ├── chunking.py              # split_windows (ventanas de 25 años)
│   ├── requests_model.py        # StationRequest + coerce_request
│   ├── driver.py                # Context manager de Chrome
│   ├── navigation.py            # safe_click robusto
│   ├── station.py               # Selección de estación vía Kendo UI
│   ├── download.py              # Espera del ZIP + extracción/renombrado
│   ├── catalog.py               # VARIABLES_IDEAM + CATALOG_GENERATED_AT
│   ├── catalog_builder.py       # Lógica de regeneración del catálogo
│   ├── regenerate_catalog.py    # CLI (python -m / ideam-dhime-regenerate)
│   ├── constants.py             # URLs, XPaths, timeouts
│   └── exceptions.py            # Jerarquía de errores
├── docs/
│   ├── algorithm.md             # Flujo y diagramas
│   └── guia_variables.txt       # Guía legible del catálogo (snapshot)
├── examples/
│   └── example_batch.py         # 1 estación y 4 estaciones, serie/paralelo
├── tests/                       # Pruebas unitarias (sin navegador)
│   ├── test_chunking.py
│   ├── test_requests_model.py
│   ├── test_catalog.py
│   ├── test_batch_partition.py
│   └── test_package_api.py
├── pyproject.toml
├── requirements.txt
├── MANIFEST.in
├── CHANGELOG.md
├── LICENSE
├── CITATION.cff
└── README.md
```

## Catálogo de variables (incluido en el paquete)

El paquete distribuye un **snapshot** del catálogo IDEAM en [`ideam_dhime/catalog.py`](ideam_dhime/catalog.py):

- **`VARIABLES_IDEAM`**: mapeo `ID -> (categoría de menú, nombre de variable)`.
- **`CATALOG_GENERATED_AT`**: fecha ISO del snapshot publicado.

La versión legible para humanos está en [`docs/guia_variables.txt`](docs/guia_variables.txt) (misma fecha de referencia).

> **Primera ejecución:** si es la primera vez que usas el paquete, imprime `CATALOG_GENERATED_AT`. Si la fecha es antigua y el portal IDEAM ha cambiado, ejecuta el comando de regeneración (siguiente sección) antes de intentar descargas masivas.

## Uso rápido

```python
from ideam_dhime import CATALOG_GENERATED_AT, download_dhime_data, VARIABLES_IDEAM

print("Catálogo snapshot:", CATALOG_GENERATED_AT)
print("ID 7 →", VARIABLES_IDEAM[7])   # ('Caudal', 'Caudal medio diario')

csv_path = download_dhime_data(
    download_path="./datos_hidro",
    date_ini="01/01/2022",
    date_fin="31/12/2023",
    department="Huila",
    municipality="La Plata",
    station_code="2105700152",
    variable_id=7,
)
print(csv_path)
```

Ejemplo completo: [`examples/example_batch.py`](examples/example_batch.py) (incluye un caso de 1 estación y otro de 4, ejecutable en modo `serial` o `parallel`).

## Modo lote (v0.2.1)

`batch_download` ejecuta múltiples estaciones y divide automáticamente periodos largos en ventanas de máximo 25 años (por defecto).

```python
from ideam_dhime import StationRequest, batch_download

requests = [
    StationRequest(
        download_path="./datos_hidro",
        date_ini="01/01/1970",
        date_fin="31/12/2025",
        department="Huila",
        municipality="La Plata",
        station_code="2105700152",
        variable_id=7,
    ),
    StationRequest(
        download_path="./datos_hidro",
        date_ini="01/01/2000",
        date_fin="31/12/2020",
        department="Antioquia",
        municipality="Medellín",
        station_code="25027680",
        variable_id=7,
    ),
]

results = batch_download(requests)
for item in results:
    print(item.status, item.request.station_code, item.csv_paths, item.message)
```

Notas:
- Si una estación falla, el lote continúa y el resultado queda con `status="ERROR"`.
- Si una estación supera 25 años, se generan varios CSV (uno por ventana).
- `download_dhime_data(...)` sigue disponible para modo una-estación.

### Aislamiento de descargas (sin colisiones de ZIP)

Cada descarga unitaria (estación + ventana) usa su propia carpeta temporal:

- se crea `download_path/<stem_csv>/`,
- el ZIP se descarga y procesa dentro de esa carpeta,
- el CSV final se mueve a `download_path`,
- la carpeta temporal se elimina al final (éxito o error).

Esto evita choques cuando IDEAM reutiliza nombres de ZIP.

### Paralelismo opcional (multiproceso)

Puedes habilitarlo con `parallel=True` y `workers`:

```python
from ideam_dhime import StationRequest, batch_download

if __name__ == "__main__":
    results = batch_download(
        requests,
        parallel=True,
        workers=2,
        max_years=25,
    )
```

Notas importantes:
- Por defecto `parallel=False` (secuencial, una sola sesión Chrome).
- Cada worker abre su propio Chrome (más consumo de RAM/CPU).
- En Windows debes usar `if __name__ == "__main__":`.
- El motor deduplica chunks repetidos antes de repartirlos entre workers.
- Evita correr dos scripts distintos al mismo tiempo sobre el mismo `download_path` compartido.

### Parámetros de `download_dhime_data`

| Parámetro | Descripción |
|-----------|-------------|
| `download_path` | Carpeta donde se guardan ZIP/CSV (se crea si no existe). |
| `date_ini`, `date_fin` | Fechas en formato **`dd/mm/aaaa`**, como en el portal. |
| `variable_id` | **Recomendado.** ID según `VARIABLES_IDEAM`; rellena `parameter` y `variable_code`. |
| `parameter` | Texto exacto del menú de parámetros (solo si no usas `variable_id`). |
| `variable_code` | Texto de la variable usado en el `onclick` del portal (solo si no usas `variable_id`). |
| `department`, `municipality` | Texto **exacto** de departamento y municipio en los listados. |
| `station_code` | Código de estación en el desplegable Kendo. |
| `time_wait` | Timeout del `WebDriverWait` en segundos (por defecto **25**). |

## Regenerar el catálogo

Si el IDEAM cambia nombres en el menú o en la tabla de variables, **no** edites el diccionario a mano: regenéralo. El paquete expone un comando oficial que abre Chrome, recorre el menú y escribe los archivos actualizados.

```bash
# Una vez instalado el paquete:
ideam-dhime-regenerate --output-dir docs

# Equivalente (sin depender del script de consola):
python -m ideam_dhime.regenerate_catalog --output-dir docs
```

Opciones relevantes:

| Flag | Efecto |
|------|--------|
| `--output-dir, -o` | Carpeta de salida (por defecto: directorio actual). |
| `--no-python` | No escribir `catalog_generated.py`. |
| `--no-txt` | No escribir `guia_variables.txt`. |
| `-v, --verbose` | Activa logs `DEBUG`. |

Salida típica:

- `catalog_generated.py`: pegar su `VARIABLES_IDEAM` sobre [`ideam_dhime/catalog.py`](ideam_dhime/catalog.py) y actualizar `CATALOG_GENERATED_AT`.
- `guia_variables.txt`: reemplaza [`docs/guia_variables.txt`](docs/guia_variables.txt) para el siguiente release.

También puedes llamar a [`generate_catalog`](ideam_dhime/catalog_builder.py) desde Python si necesitas integrarlo en otro flujo.

## Logging

Los mensajes usan el logger **`ideam_dhime`**. Por defecto verás `INFO`; el detalle de cada clic está en `DEBUG`.

```python
import logging
logging.basicConfig(level=logging.INFO)
# logging.getLogger("ideam_dhime").setLevel(logging.DEBUG)   # trazas detalladas
# logging.getLogger("ideam_dhime").setLevel(logging.WARNING) # silenciar casi todo
```

## Algoritmo y flujo

Descripción paso a paso, diagramas y árbol del generador del catálogo en [`docs/algorithm.md`](docs/algorithm.md).

## Limitaciones

- Depende de la interfaz actual de DHIME (selectores frágiles).
- Usa tiempos de espera fijos (`sleep`) que forman parte del comportamiento validado.
- Requiere Chrome; no está probado con otros navegadores.

## Desarrollo y tests

```bash
pip install -e .[dev]
pytest -q
```

Las pruebas unitarias cubren chunking de fechas, particionado del motor de lote, catálogo indexado, modelo `StationRequest` y la API pública del paquete. No abren el navegador, así que pueden correr en CI.

## Contribuciones

Issues y pull requests son bienvenidos. Si cambias XPaths o tiempos, indica en el PR cómo probaste contra el portal real.

## Autor

- David De Leon Perez
- ORCID: [0000-0002-1846-9750](https://orcid.org/0000-0002-1846-9750)
- Proyecto mantenido por David De Leon Perez; la afiliación organizacional puede evolucionar en versiones futuras.

## Cómo citar

GitHub muestra un botón de cita si mantienes actualizado [`CITATION.cff`](CITATION.cff). Edita autores, ORCID y URL antes de publicar.

```bibtex
@software{ideam_dhime,
  author  = {De Leon Perez, David},
  title   = {ideam-dhime: cliente DHIME IDEAM},
  url     = {https://github.com/David-R-De-Leon-P/IDEAM_scrapping},
  doi     = {10.5281/zenodo.19740803},
  version = {0.2.3},
  year    = {2026},
}
```

## Licencia

Distribuido bajo la **GNU General Public License v3.0 o posterior**. Ver [LICENSE](LICENSE).

Los datos obtenidos del IDEAM están sujetos a las **condiciones de uso** del propio portal; este software no las sustituye ni modifica.
