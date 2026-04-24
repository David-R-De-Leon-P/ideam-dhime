# -*- coding: utf-8 -*-
"""
Comando de regeneración del catálogo IDEAM.

Ejecución:

    python -m ideam_dhime.regenerate_catalog [--output-dir RUTA]

O bien, si el paquete está instalado:

    ideam-dhime-regenerate [--output-dir RUTA]

Abre Chrome y recorre el menú de parámetros del portal DHIME para reconstruir
``VARIABLES_IDEAM``. Escribe ``catalog_generated.py`` (listo para fusionar en
``ideam_dhime/catalog.py``) y ``guia_variables.txt`` (referencia para humanos).
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from ideam_dhime.catalog_builder import generate_catalog


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ideam-dhime-regenerate",
        description=(
            "Regenera el catálogo IDEAM (VARIABLES_IDEAM) abriendo Chrome contra "
            "el portal DHIME. Escribe catalog_generated.py y guia_variables.txt."
        ),
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        type=Path,
        default=Path("."),
        help="Carpeta donde escribir los archivos de salida (por defecto: directorio actual).",
    )
    parser.add_argument(
        "--no-python",
        action="store_true",
        help="No escribir catalog_generated.py.",
    )
    parser.add_argument(
        "--no-txt",
        action="store_true",
        help="No escribir guia_variables.txt.",
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

    catalogo = generate_catalog(
        output_dir=args.output_dir,
        write_python=not args.no_python,
        write_txt=not args.no_txt,
    )

    if not catalogo:
        print("No se extrajo ningún elemento del portal.", file=sys.stderr)
        return 1

    print(f"{len(catalogo)} variables detectadas. Archivos escritos en {args.output_dir.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
