# -*- coding: utf-8 -*-
"""Ejemplo configurable: 1 estación o 4 estaciones, en serie o paralelo."""

from ideam_dhime import CATALOG_GENERATED_AT, StationRequest, batch_download

DOWNLOAD_DIR = "./datos_hidro"
VARIABLE_ID = 7

# Cambia estas dos banderas antes de dar "play":
# - EXAMPLE_SET: "one" o "four"
# - EXECUTION_MODE: "serial" o "parallel"
EXAMPLE_SET = "four"
EXECUTION_MODE = "parallel"
WORKERS = 2

# Nota: la fecha "01/01/202" que pasaste parece typo; se ajusta a 01/01/2022.
REQUESTS_FOUR = [
    StationRequest(
        download_path=DOWNLOAD_DIR,
        date_ini="13/02/1987",
        date_fin="30/11/2025",
        department="Bogotá",
        municipality="Bogota, D.C",
        station_code="35027150",
        variable_id=VARIABLE_ID,
    ),
    StationRequest(
        download_path=DOWNLOAD_DIR,
        date_ini="22/02/2006",
        date_fin="31/12/2025",
        department="Huila",
        municipality="Elías",
        station_code="21017040",
        variable_id=VARIABLE_ID,
    ),
    StationRequest(
        download_path=DOWNLOAD_DIR,
        date_ini="01/01/2022",
        date_fin="31/12/2023",
        department="Huila",
        municipality="La Plata",
        station_code="2105700152",
        variable_id=VARIABLE_ID,
    ),
    StationRequest(
        download_path=DOWNLOAD_DIR,
        date_ini="22/02/2006",
        date_fin="31/12/2025",
        department="Huila",
        municipality="Elías",
        station_code="21017070",
        variable_id=VARIABLE_ID,
    ),
]

REQUESTS_ONE = [REQUESTS_FOUR[2]]


def run_example() -> None:
    print(f"Catálogo incluido en el paquete (snapshot): {CATALOG_GENERATED_AT}")
    selected = REQUESTS_ONE if EXAMPLE_SET == "one" else REQUESTS_FOUR
    parallel = EXECUTION_MODE == "parallel"

    print(
        f"Ejecutando ejemplo='{EXAMPLE_SET}' modo='{EXECUTION_MODE}' "
        f"workers={WORKERS if parallel else 1} estaciones={len(selected)}"
    )

    results = batch_download(
        requests=selected,
        time_wait=25,
        max_years=25,
        parallel=parallel,
        workers=WORKERS,
    )

    print("\nResumen")
    ok = 0
    err = 0
    for result in results:
        code = result.request.station_code
        if result.status == "OK":
            ok += 1
            print(f"OK {code}: {len(result.csv_paths)} archivo(s)")
        else:
            err += 1
            print(f"ERROR {code}: {result.message}")
    print(f"Total OK: {ok}")
    print(f"Total ERROR: {err}")


if __name__ == "__main__":
    run_example()
