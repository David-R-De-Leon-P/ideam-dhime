# Changelog

Formato basado en [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/)
y [Semantic Versioning](https://semver.org/lang/es/).

## [0.3.0] - 2026-04-27

### Añadido
- Chunking por frecuencia con límites por años o días según `variable_id`.
- Módulo de frecuencias (`Frequency`, `FREQUENCY_LIMITS`, `infer_frequency_from_name`).
- Consolidación de chunks con `merge_station_csvs`.
- Catálogo maestro de estaciones en runtime (`StationMetadata`, `resolve_station_metadata`).
- Regeneración de catálogo desde carpeta local `CNE/` con CLI `ideam-dhime-regenerate-stations`.
- Snapshots embebidos de estaciones y ubicaciones (`stations_generated.py`, `locations_generated.py`).
- Soporte de entrada mínima en descargas: `station_code + variable_id`.
- Parámetro opcional `min_date` para fijar fecha mínima global o por solicitud.

### Cambiado
- `batch_download` ahora conserva resultados parciales cuando algunas ventanas no tienen datos.
- `download_dhime_data` y flujo de sesión alineados con chunking por frecuencia y catálogo maestro.
- Salida del catálogo oficial en `docs/guia_estaciones.xlsx` para mejor compatibilidad con Excel.
- Mensaje de `StationNotFoundError` enriquecido con estación, variable y periodo consultado.

### Corregido
- Manejo robusto de diálogos/overlays del portal que bloqueaban interacción entre ventanas.
- Limpieza temporal reforzada para ejecución paralela y reintentos en Windows.
- Reintentos de borrado de archivos en `download.py` para tolerar bloqueos `WinError 32`.
- Normalización y cruce de nombres de ubicación para reducir falsos no-match en catálogo.

## [0.2.4] - 2026-04-24

### Notes
- Actualización de metadatos: DOI de Zenodo añadido a CITATION.cff y README.md.
- Sin cambios funcionales en la API de `ideam_dhime`.

## [0.2.3] - 2026-04-24

### Notas
- Release de mantenimiento para establecer el archivado con DOI en Zenodo mediante la integración GitHub–Zenodo.
- Sin cambios funcionales en la API de `ideam_dhime`.

## [0.2.2] - 2026-04-24

### Cambiado
- Release de mantenimiento enfocado en el flujo de publicación e interoperabilidad de archivado (GitHub Releases + DOI Zenodo).

### Notas
- Sin cambios funcionales en la API de `ideam_dhime`.
- Destinado a activar la ingesta de Zenodo tras la integración con el repositorio.

## [0.2.1] - 2026-04-22

### Añadido
- Ventanas de descarga con `max_years=25` por defecto (antes 30) para respetar los límites del portal.
- Aislamiento por descarga: cada estación+ventana usa una carpeta temporal propia (`download_path/<stem>/`), evitando colisiones cuando IDEAM reutiliza el nombre del ZIP.
- Paralelismo opcional en `batch_download(..., parallel=True, workers=N)` mediante `ProcessPoolExecutor` (una sesión Chrome por worker).
- Trazas por PID en los workers (`[PID X] intentando|OK|ERROR ...`) para diagnóstico en paralelo.
- Aserciones defensivas sobre particiones (ninguna vacía ni duplicada).

### Cambiado
- `DHIMESession.__exit__` es idempotente y resetea `browser`, `wait`, `_ctx`.
- `build_browser` cierra el driver de forma defensiva (`browser.quit()` + `browser.service.stop()`).

## [0.2.0] - 2026-04-22

### Añadido
- Sesión reutilizable `DHIMESession` para múltiples descargas sin reabrir Chrome.
- `batch_download(requests)` para ejecutar lotes de `StationRequest`.
- División automática de rangos de fecha largos en ventanas.

### Mantenido
- API pública `download_dhime_data(...)` compatible con la versión 0.1.0.

## [0.1.0] - 2026-04-21

### Añadido
- Primer release público del paquete `ideam_dhime`.
- `download_dhime_data(...)` orquestando Selenium/Chrome contra el portal DHIME.
- Catálogo `VARIABLES_IDEAM` incluido como snapshot + utilidad `ideam-dhime-regenerate`.