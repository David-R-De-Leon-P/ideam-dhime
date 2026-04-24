# -*- coding: utf-8 -*-
"""Orquestación pública de descarga DHIME (compatibilidad v0.1.0)."""

from __future__ import annotations

from pathlib import Path

from ideam_dhime.constants import DEFAULT_TIMEOUT
from ideam_dhime.requests_model import StationRequest
from ideam_dhime.session import DHIMESession


def download_dhime_data(
    download_path: str | Path,
    date_ini: str,
    date_fin: str,
    parameter: str | None = None,
    variable_code: str | None = None,
    department: str | None = None,
    municipality: str | None = None,
    station_code: str | None = None,
    time_wait: int = DEFAULT_TIMEOUT,
    variable_id: int | None = None,
):
    """
    API pública estable de una estación.

    Si el rango supera 30 años retorna lista de paths; en caso contrario, un Path.
    """
    if variable_id is None and (parameter is None or variable_code is None):
        raise ValueError("Debe indicar variable_id o bien parameter + variable_code.")

    if not department or not municipality or not station_code:
        raise ValueError("department, municipality y station_code son obligatorios.")

    req = StationRequest(
        download_path=download_path,
        date_ini=date_ini,
        date_fin=date_fin,
        department=department,
        municipality=municipality,
        station_code=station_code,
        variable_id=int(variable_id) if variable_id is not None else -1,
    )

    with DHIMESession(download_path=download_path, time_wait=time_wait) as session:
        csvs = session.download_one(req, parameter=parameter, variable_code=variable_code)

    return csvs[0] if len(csvs) == 1 else csvs
