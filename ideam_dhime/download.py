# -*- coding: utf-8 -*-
"""Espera de ZIP y extracción/renombrado del CSV."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from pathlib import Path
from zipfile import ZipFile

from ideam_dhime.constants import DOWNLOAD_TIMEOUT
from ideam_dhime.exceptions import DHIMEError, DownloadTimeoutError, NoDataInRangeError

logger = logging.getLogger("ideam_dhime")


def _safe_unlink(path: Path, retries: int = 6, delay_s: float = 0.4) -> None:
    """
    Elimina archivos con reintentos para tolerar bloqueos transitorios en Windows/Dropbox.
    """
    last_exc: Exception | None = None
    for _ in range(retries):
        try:
            path.unlink()
            return
        except FileNotFoundError:
            return
        except PermissionError as exc:
            last_exc = exc
            time.sleep(delay_s)
        except OSError as exc:
            # WinError 32: archivo en uso por otro proceso.
            if getattr(exc, "winerror", None) == 32:
                last_exc = exc
                time.sleep(delay_s)
                continue
            raise
    if last_exc is not None:
        raise DHIMEError(f"No se pudo eliminar archivo bloqueado: {path}") from last_exc


def wait_for_zip(
    target_dir: Path,
    timeout: int = DOWNLOAD_TIMEOUT,
    no_data_checker: Callable[[], str | None] | None = None,
) -> Path:
    """
    Espera hasta que no haya ``.crdownload`` y exista al menos un ``.zip``.

    Misma lógica que el bucle en ``scrapper.py``.
    """
    logger.debug("[Cálculo] Esperando a que finalice la descarga del archivo ZIP...")
    end_time_descarga = time.time() + timeout
    descarga_completa = False
    zip_descargado: Path | None = None

    while time.time() < end_time_descarga:
        if no_data_checker is not None:
            no_data_msg = no_data_checker()
            if no_data_msg:
                raise NoDataInRangeError(no_data_msg)

        crdownloads = list(target_dir.glob("*.crdownload"))
        zips = list(target_dir.glob("*.zip"))

        if len(crdownloads) == 0 and len(zips) > 0:
            zip_descargado = max(zips, key=lambda p: p.stat().st_mtime)
            descarga_completa = True
            break
        time.sleep(1)

    if not descarga_completa or not zip_descargado:
        raise DownloadTimeoutError("Tiempo de espera agotado. La descarga no se completó.")

    logger.info("Archivo descargado: %s", zip_descargado.name)
    return zip_descargado


def extract_and_rename(
    zip_path: Path,
    target_dir: Path,
    station_code: str,
    variable_code: str,
    date_ini: str,
    date_fin: str,
) -> Path:
    """
    Descomprime el ZIP, toma el CSV más reciente y lo renombra al patrón del original.
    """
    if not zip_path.exists():
        raise DHIMEError(f"No existe el archivo ZIP: {zip_path}")

    with ZipFile(zip_path, "r") as zf:
        zf.extractall(target_dir)

    csv_files = list(target_dir.glob("*.csv"))
    if not csv_files:
        raise DHIMEError("No se encontró ningún archivo CSV dentro del ZIP.")

    csv_extraido = max(csv_files, key=lambda p: p.stat().st_mtime)

    clean_date_ini = date_ini.replace("/", "")
    clean_date_fin = date_fin.replace("/", "")

    final_file_name = f"{station_code}-{variable_code}-{clean_date_ini}-{clean_date_fin}.csv".replace(
        " ", "_"
    )
    final_file_path = target_dir / final_file_name

    if final_file_path.exists():
        _safe_unlink(final_file_path)

    csv_extraido.replace(final_file_path)
    _safe_unlink(zip_path)

    logger.info("ÉXITO TOTAL: Archivo %s guardado en %s", final_file_name, target_dir)
    return final_file_path
