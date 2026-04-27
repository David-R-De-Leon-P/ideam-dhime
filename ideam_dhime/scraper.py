# -*- coding: utf-8 -*-
"""Orquestación pública de descarga DHIME (compatibilidad v0.1.0)."""

from __future__ import annotations

from pathlib import Path

from ideam_dhime.constants import DEFAULT_TIMEOUT
from ideam_dhime.csv_merge import merge_station_csvs
from ideam_dhime.requests_model import StationRequest
from ideam_dhime.session import DHIMESession


def download_dhime_data(
    download_path: str | Path,
    date_ini: str | None = None,
    date_fin: str | None = None,
    parameter: str | None = None,
    variable_code: str | None = None,
    department: str | None = None,
    municipality: str | None = None,
    station_code: str | None = None,
    time_wait: int = DEFAULT_TIMEOUT,
    variable_id: int | None = None,
    max_years: int | None = None,
    min_date: str | None = None,
    max_days: int | None = None,
    merge_chunks: bool = True,
    keep_chunks: bool = True,
):
    """
    API pública estable de una estación.

    Si el rango supera 30 años retorna lista de paths; en caso contrario, un Path.
    """
    if variable_id is None and (parameter is None or variable_code is None):
        raise ValueError("Debe indicar variable_id o bien parameter + variable_code.")

    if not station_code:
        raise ValueError("station_code es obligatorio.")

    req = StationRequest(
        download_path=download_path,
        date_ini=date_ini,
        date_fin=date_fin,
        department=department,
        municipality=municipality,
        station_code=station_code,
        variable_id=int(variable_id) if variable_id is not None else -1,
        max_years=max_years,
        max_days=max_days,
        min_date=min_date,
    )

    with DHIMESession(download_path=download_path, time_wait=time_wait) as session:
        csvs = session.download_one(
            req,
            parameter=parameter,
            variable_code=variable_code,
            max_years=max_years,
            min_date=min_date,
            max_days=max_days,
        )

    if merge_chunks and csvs:
        effective_ini = date_ini or "01011900"
        effective_fin = date_fin or "historico"
        clean_ini = effective_ini.replace("/", "")
        clean_fin = effective_fin.replace("/", "")
        variable_label = variable_id if variable_id is not None else variable_code
        final_name = f"{station_code}-{variable_label}-{clean_ini}-{clean_fin}-final.csv"
        final_path = merge_station_csvs(
            csvs,
            Path(download_path) / final_name.replace(" ", "_"),
            keep_chunks=keep_chunks,
        )
        return final_path

    return csvs[0] if len(csvs) == 1 else csvs
