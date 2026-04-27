# Release v0.3.0

Release enfocada en robustez de descarga, chunking por frecuencia y catalogo maestro unificado.

## Alcance cerrado

- Chunking por frecuencia (`Frequency`) con limites en anios o dias.
- Descarga lote/secuencial/paralela con limpieza temporal robusta.
- Consolidacion de chunks por estacion con `merge_station_csvs`.
- Manejo tolerante de ventanas sin datos:
  - si hay datos parciales, se compila parcial;
  - si no hay datos en todo el periodo, se reporta error de no-datos.
- Entrada minima: `station_code + variable_id` (resolviendo depto/municipio desde snapshot).
- Piso temporal configurable con `min_date` (global o por request).
- Catalogo maestro:
  - `docs/guia_estaciones.xlsx`
  - `ideam_dhime/stations_generated.py`
  - `ideam_dhime/locations_generated.py`
- Regeneracion de catalogo desde `CNE/` con:
  - `python -m ideam_dhime.regenerate_stations`
  - `ideam-dhime-regenerate-stations`

## Validacion

- Pruebas locales: `pytest -q`
- Resultado final: `66 passed`

## Notas de publicacion

- `docs/guia_estaciones_calidad.xlsx` se mantiene como artefacto operativo local (no requerido para consumo del paquete).
- Resultados de descargas (`datos_hidro/`, `test_downloads/`) quedan fuera de control de versiones.
