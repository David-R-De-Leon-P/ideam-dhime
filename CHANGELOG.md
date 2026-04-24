# Changelog

Formato basado en [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/)
y [Semantic Versioning](https://semver.org/lang/es/).

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
