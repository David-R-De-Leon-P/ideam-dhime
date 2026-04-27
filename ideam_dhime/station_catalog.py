# -*- coding: utf-8 -*-
"""Resolución runtime de estaciones desde el catálogo maestro embebido."""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from pathlib import Path

from ideam_dhime.exceptions import LocationNotFoundError, StationNotFoundError


@dataclass(frozen=True)
class StationMetadata:
    """Metadatos oficiales mínimos para resolver una estación DHIME."""

    station_code: str
    station_name: str
    department: str
    municipality: str
    entity: str = ""
    fecha_ini_op: str = ""
    fecha_fin_op: str = ""
    latitude: str = ""
    longitude: str = ""
    category: str = ""


def normalize_location_name(value: str) -> str:
    """Normaliza nombres para comparar ubicaciones con o sin tildes/mayúsculas."""
    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.upper().strip().split())


def resolve_station_metadata(station_code: str) -> StationMetadata:
    """Devuelve metadatos oficiales para ``station_code`` desde el snapshot embebido."""
    from ideam_dhime.stations_generated import STATIONS_DHIME

    code = str(station_code).strip()
    try:
        return STATIONS_DHIME[code]
    except KeyError as exc:
        raise StationNotFoundError(
            f"station_code={code!r} no existe en el catálogo maestro embebido."
        ) from exc


def resolve_location(department: str, municipality: str) -> tuple[str, str]:
    """Valida y devuelve una ubicación oficial desde el snapshot embebido."""
    from ideam_dhime.locations_generated import OFFICIAL_LOCATIONS

    target = (normalize_location_name(department), normalize_location_name(municipality))
    for official_department, official_municipality in OFFICIAL_LOCATIONS:
        candidate = (
            normalize_location_name(official_department),
            normalize_location_name(official_municipality),
        )
        if candidate == target:
            return official_department, official_municipality
    raise LocationNotFoundError(
        f"No existe ubicación oficial para department={department!r}, "
        f"municipality={municipality!r}."
    )


def station_catalog_path() -> Path:
    """Ruta del catálogo Excel incluido en la distribución del repo."""
    return Path(__file__).resolve().parents[1] / "docs" / "guia_estaciones.xlsx"
