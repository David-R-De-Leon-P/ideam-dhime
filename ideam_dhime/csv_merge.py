# -*- coding: utf-8 -*-
"""Consolidación de CSVs descargados por chunks DHIME."""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Iterable


def _detect_dialect(path: Path) -> csv.Dialect:
    sample = path.read_text(encoding="utf-8-sig", errors="replace")[:4096]
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        return csv.get_dialect("excel")


def _find_date_column(header: list[str], preferred: str = "Fecha") -> str | None:
    normalized = {column.strip().lower(): column for column in header}
    return normalized.get(preferred.strip().lower())


def _parse_date(value: str) -> datetime:
    raw = (value or "").strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return datetime.max


def merge_station_csvs(
    csv_paths: Iterable[str | Path],
    output_path: str | Path,
    *,
    keep_chunks: bool = True,
    date_column: str = "Fecha",
) -> Path:
    """
    Fusiona CSVs de una misma solicitud en un archivo final ordenado.

    Deduplica filas completas para no perder mediciones subdiarias que compartan
    fecha pero tengan otra columna de hora/valor.
    """
    paths = [Path(path) for path in csv_paths]
    if not paths:
        raise ValueError("csv_paths no puede estar vacío")

    header: list[str] | None = None
    rows: list[dict[str, str]] = []
    seen_rows: set[tuple[str, ...]] = set()
    dialect = _detect_dialect(paths[0])

    for path in paths:
        if not path.exists():
            raise FileNotFoundError(f"No existe CSV para fusionar: {path}")
        current_dialect = _detect_dialect(path)
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, dialect=current_dialect)
            if reader.fieldnames is None:
                continue
            current_header = [column.strip() for column in reader.fieldnames]
            if header is None:
                header = current_header
            elif current_header != header:
                raise ValueError(
                    f"Encabezados incompatibles al fusionar CSVs: {path}"
                )

            for row in reader:
                normalized_row = {column: (row.get(column) or "") for column in header}
                signature = tuple(normalized_row[column] for column in header)
                if signature in seen_rows:
                    continue
                seen_rows.add(signature)
                rows.append(normalized_row)

    if header is None:
        raise ValueError("No se encontró encabezado en los CSVs a fusionar")

    detected_date_column = _find_date_column(header, preferred=date_column)
    if detected_date_column is not None:
        rows.sort(key=lambda row: (_parse_date(row.get(detected_date_column, "")), tuple(row.values())))

    final_path = Path(output_path)
    final_path.parent.mkdir(parents=True, exist_ok=True)
    with final_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header, dialect=dialect)
        writer.writeheader()
        writer.writerows(rows)

    if not keep_chunks:
        for path in paths:
            if path.resolve() == final_path.resolve():
                continue
            try:
                path.unlink()
            except FileNotFoundError:
                pass

    return final_path
