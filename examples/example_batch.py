# -*- coding: utf-8 -*-
"""
Ejemplo de descarga en lote con ideam-dhime (v0.3.0+).

Muestra dos enfoques:
  A) Entrada mínima: solo station_code + variable_id.
     El catálogo embebido resuelve automáticamente departamento y municipio.
  B) Entrada completa: station_code + variable_id + fechas + ubicación explícita.
     Útil cuando se quiere acotar el periodo o sobreescribir la ubicación.

También ilustra el modo paralelo (parallel=True) con múltiples workers.

Ejecutar desde la raíz del proyecto:
    python examples/example_batch.py
"""

from __future__ import annotations

import sys
from pathlib import Path

from ideam_dhime import CATALOG_GENERATED_AT, StationRequest, batch_download

# ── Configuración ─────────────────────────────────────────────────────────────

DOWNLOAD_DIR = Path("./datos_hidro")
VARIABLE_ID = 7  # Caudal medio diario

# Cambia estas dos banderas antes de ejecutar:
#   EXAMPLE_SET   → "minimal"   entrada mínima, 10 estaciones del Huila
#                 → "full"      entrada completa, 4 estaciones con fechas
#   PARALLEL      → True        un proceso de Chrome por worker
#                 → False       una sola sesión (recomendado para pruebas rápidas)
EXAMPLE_SET = "minimal"
PARALLEL = True
WORKERS = 2


# ── Enfoque A: entrada mínima (station_code + variable_id) ───────────────────
# El catálogo embebido resuelve departamento y municipio automáticamente.
# No se necesita conocer ni indicar la ubicación.

REQUESTS_MINIMAL = [
    {"download_path": str(DOWNLOAD_DIR), "station_code": code, "variable_id": VARIABLE_ID}
    for code in [
        "21017020",  # SAN AGUSTIN
        "21017030",  # CASCADA SIMON BOLIVAR - AUT
        "21017040",  # SALADO BLANCO
        "21017050",  # PITALITO 2
        "21017060",  # MAGDALENA LA
        "21027010",  # PERICONGO
        "21037010",  # PUENTE GARCES
        "21037020",  # SAN MARCOS - AUT
        "21037030",  # LIBANO EL
        "21037080",  # GUACHARO EL
    ]
]


# ── Enfoque B: entrada completa (con fechas y ubicación explícita) ────────────
# Útil para acotar el periodo o cuando se quiere sobreescribir la ubicación
# del catálogo por algún motivo puntual.

REQUESTS_FULL = [
    StationRequest(
        download_path=str(DOWNLOAD_DIR),
        date_ini="13/02/1987",
        date_fin="30/11/2025",
        department="Bogotá",
        municipality="Bogota, D.C",
        station_code="35027150",
        variable_id=VARIABLE_ID,
    ),
    StationRequest(
        download_path=str(DOWNLOAD_DIR),
        date_ini="22/02/2006",
        date_fin="31/12/2025",
        department="Huila",
        municipality="Elías",
        station_code="21017040",
        variable_id=VARIABLE_ID,
    ),
    StationRequest(
        download_path=str(DOWNLOAD_DIR),
        date_ini="01/01/2022",
        date_fin="31/12/2023",
        department="Huila",
        municipality="La Plata",
        station_code="2105700152",
        variable_id=VARIABLE_ID,
    ),
    StationRequest(
        download_path=str(DOWNLOAD_DIR),
        date_ini="22/02/2006",
        date_fin="31/12/2025",
        department="Huila",
        municipality="Elías",
        station_code="21017070",
        variable_id=VARIABLE_ID,
    ),
]


# ── Ejecución ─────────────────────────────────────────────────────────────────

def run_example() -> int:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\nCatálogo embebido generado en: {CATALOG_GENERATED_AT}")

    selected = REQUESTS_MINIMAL if EXAMPLE_SET == "minimal" else REQUESTS_FULL
    mode = "paralelo" if PARALLEL else "serie"
    print(
        f"Ejecutando conjunto='{EXAMPLE_SET}' modo='{mode}' "
        f"workers={WORKERS if PARALLEL else 1} estaciones={len(selected)}\n"
    )

    results = batch_download(
        requests=selected,
        download_path=DOWNLOAD_DIR,
        time_wait=25,
        min_date="01/01/1970",  # Piso opcional para estudios (ej. series desde 1970)
        merge_chunks=True,   # fusiona los chunks por estación en un solo CSV
        keep_chunks=False,   # elimina los chunks intermedios
        parallel=PARALLEL,
        workers=WORKERS,
    )

    ok = [r for r in results if r.status == "OK"]
    err = [r for r in results if r.status != "OK"]

    print("=" * 60)
    print(f"  Completadas: {len(ok)} / {len(results)}")
    print("=" * 60)
    for r in ok:
        csv_info = str(r.csv_final or (r.csv_paths[0] if r.csv_paths else "—"))
        print(f"  OK   {r.request.station_code}  → {Path(csv_info).name}")
    if err:
        print()
        for r in err:
            print(f"  ERR  {r.request.station_code}  {r.message}")
    print("=" * 60 + "\n")

    return 0 if not err else 1


if __name__ == "__main__":
    sys.exit(run_example())
