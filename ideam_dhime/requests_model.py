# -*- coding: utf-8 -*-
"""Modelos de entrada para descargas individuales o por lote."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ideam_dhime.exceptions import StationNotFoundError
from ideam_dhime.station_catalog import resolve_station_metadata


@dataclass(frozen=True)
class StationRequest:
    """Solicitud de descarga DHIME para una estación."""

    download_path: str | Path
    date_ini: str | None = None
    date_fin: str | None = None
    department: str | None = None
    municipality: str | None = None
    station_code: str = ""
    variable_id: int = 0
    max_years: int | None = None
    max_days: int | None = None
    min_date: str | None = None


def _with_resolved_station_metadata(req: StationRequest) -> StationRequest:
    if req.department and req.municipality:
        return req
    try:
        metadata = resolve_station_metadata(req.station_code)
    except StationNotFoundError:
        from ideam_dhime.catalog import VARIABLES_IDEAM

        var_entry = VARIABLES_IDEAM.get(req.variable_id)
        if var_entry:
            _categoria, parametro, _freq = var_entry
            var_desc = f"variable_id={req.variable_id} · {parametro}"
        else:
            var_desc = f"variable_id={req.variable_id}"
        date_ini_str = req.date_ini or "inicio histórico"
        date_fin_str = req.date_fin or "hoy"
        raise StationNotFoundError(
            f"\n"
            f"  Estación  : {req.station_code}\n"
            f"  Variable  : {var_desc}\n"
            f"  Periodo   : {date_ini_str} → {date_fin_str}\n"
            f"\n"
            f"  La estación '{req.station_code}' no se encuentra en el catálogo maestro embebido.\n"
            f"  Por favor verifique manualmente el código en:\n"
            f"    https://dhime.ideam.gov.co/atencionciudadano/\n"
            f"\n"
            f"  Si el código es correcto y el error persiste,\n"
            f"  repórtelo al mantenedor de la librería en GitHub."
        ) from None
    return StationRequest(
        download_path=req.download_path,
        date_ini=req.date_ini,
        date_fin=req.date_fin,
        department=req.department or metadata.department,
        municipality=req.municipality or metadata.municipality,
        station_code=req.station_code,
        variable_id=req.variable_id,
        max_years=req.max_years,
        max_days=req.max_days,
        min_date=req.min_date,
    )


def coerce_request(item: Any) -> StationRequest:
    """
    Acepta:
    - StationRequest
    - tuple/list de 7 elementos (orden del dataclass)
    - dict con llaves del dataclass
    """
    if isinstance(item, StationRequest):
        return _with_resolved_station_metadata(item)

    if isinstance(item, dict):
        return _with_resolved_station_metadata(StationRequest(
            download_path=item["download_path"],
            date_ini=item.get("date_ini"),
            date_fin=item.get("date_fin"),
            department=item.get("department"),
            municipality=item.get("municipality"),
            station_code=item["station_code"],
            variable_id=int(item["variable_id"]),
            max_years=item.get("max_years"),
            max_days=item.get("max_days"),
            min_date=item.get("min_date"),
        ))

    if isinstance(item, (tuple, list)) and len(item) in (7, 8, 9, 10):
        return _with_resolved_station_metadata(StationRequest(
            download_path=item[0],
            date_ini=item[1],
            date_fin=item[2],
            department=item[3],
            municipality=item[4],
            station_code=item[5],
            variable_id=int(item[6]),
            max_years=item[7] if len(item) in (9, 10) else None,
            max_days=item[8] if len(item) in (9, 10) else None,
            min_date=item[7] if len(item) == 8 else (item[9] if len(item) == 10 else None),
        ))

    raise TypeError(
        "Formato inválido. Usa StationRequest, dict compatible o tuple/list de 7, 8, 9 o 10 elementos."
    )
