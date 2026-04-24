# -*- coding: utf-8 -*-
"""Modelos de entrada para descargas individuales o por lote."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class StationRequest:
    """Solicitud de descarga DHIME para una estación."""

    download_path: str | Path
    date_ini: str
    date_fin: str
    department: str
    municipality: str
    station_code: str
    variable_id: int


def coerce_request(item: Any) -> StationRequest:
    """
    Acepta:
    - StationRequest
    - tuple/list de 7 elementos (orden del dataclass)
    - dict con llaves del dataclass
    """
    if isinstance(item, StationRequest):
        return item

    if isinstance(item, dict):
        return StationRequest(
            download_path=item["download_path"],
            date_ini=item["date_ini"],
            date_fin=item["date_fin"],
            department=item["department"],
            municipality=item["municipality"],
            station_code=item["station_code"],
            variable_id=int(item["variable_id"]),
        )

    if isinstance(item, (tuple, list)) and len(item) == 7:
        return StationRequest(
            download_path=item[0],
            date_ini=item[1],
            date_fin=item[2],
            department=item[3],
            municipality=item[4],
            station_code=item[5],
            variable_id=int(item[6]),
        )

    raise TypeError(
        "Formato inválido. Usa StationRequest, dict compatible o tuple/list de 7 elementos."
    )
