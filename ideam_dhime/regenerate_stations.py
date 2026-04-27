# -*- coding: utf-8 -*-
"""Comando para regenerar el catálogo maestro de estaciones DHIME."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from ideam_dhime.station_catalog_builder import regenerate_station_catalog


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ideam-dhime-regenerate-stations",
        description=(
            "Regenera el catálogo maestro desde una carpeta CNE local con los dos Excel "
            "oficiales y el DBF del shape."
        ),
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path("CNE"),
        help="Carpeta con los dos Excel CNE y el DBF del shape (por defecto: ./CNE).",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=None,
        help="Raíz donde escribir docs/ e ideam_dhime/ (por defecto: repo actual).",
    )
    parser.add_argument(
        "--cleanup-sources",
        action="store_true",
        help="Eliminar los archivos fuente de CNE al terminar. Por defecto se conservan.",
    )
    parser.add_argument(
        "--no-trust-cne-automatic-telemetry",
        action="store_true",
        help=(
            "No aplicar la regla que conserva CNE para estaciones automáticas con/sin "
            "telemetría que no aparecen en el DBF."
        ),
    )
    parser.add_argument(
        "--no-trust-cne-pre-1970-suspended",
        action="store_true",
        help=(
            "No aplicar la regla que conserva CNE para estaciones suspendidas antes "
            "del 01/01/1970."
        ),
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Mostrar mensajes DEBUG del logger ideam_dhime.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s %(name)s: %(message)s")

    report = regenerate_station_catalog(
        source_dir=args.source_dir,
        output_root=args.output_root,
        cleanup_sources=args.cleanup_sources,
        trust_cne_for_automatic_telemetry=not args.no_trust_cne_automatic_telemetry,
        trust_cne_for_pre_1970_suspended=not args.no_trust_cne_pre_1970_suspended,
    )
    print(f"Catálogo generado: {report.output_csv}")
    print(f"Filas: {report.rows_total}; observaciones de calidad: {report.quality_rows}")
    if report.stations_py:
        print(f"Snapshot estaciones: {report.stations_py}")
    if report.locations_py:
        print(f"Snapshot ubicaciones: {report.locations_py}")
    if report.quality_csv:
        print(f"Reporte calidad: {report.quality_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
