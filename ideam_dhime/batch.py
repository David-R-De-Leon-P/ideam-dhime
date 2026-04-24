# -*- coding: utf-8 -*-
"""API de descargas en lote, secuencial o paralela."""

from __future__ import annotations

import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from ideam_dhime.chunking import split_windows
from ideam_dhime.constants import DEFAULT_TIMEOUT
from ideam_dhime.requests_model import StationRequest, coerce_request
from ideam_dhime.session import DHIMESession


@dataclass(frozen=True)
class DownloadResult:
    request: StationRequest
    csv_paths: list[Path]
    windows: list[tuple[str, str]]
    status: str
    message: str


@dataclass(frozen=True)
class ChunkJob:
    station_code: str
    variable_id: int
    date_ini: str
    date_fin: str
    department: str
    municipality: str
    download_path: str


@dataclass(frozen=True)
class ChunkExecution:
    key: tuple[str, int, str, str, str, str, str]
    status: str
    message: str
    csv_path: str | None


def _chunk_key(job: ChunkJob) -> tuple[str, int, str, str, str, str, str]:
    return (
        job.station_code,
        job.variable_id,
        job.date_ini,
        job.date_fin,
        job.department,
        job.municipality,
        job.download_path,
    )


def _expand_to_chunks(
    requests: list[StationRequest],
    max_years: int,
    base_path: Path | None,
) -> tuple[list[ChunkJob], list[list[tuple[str, int, str, str, str, str, str]]]]:
    seen: set[tuple[str, int, str, str, str, str, str]] = set()
    jobs: list[ChunkJob] = []
    request_to_keys: list[list[tuple[str, int, str, str, str, str, str]]] = []

    for req in requests:
        target_path = str((base_path or Path(req.download_path)).absolute())
        keys_for_request: list[tuple[str, int, str, str, str, str, str]] = []
        windows = split_windows(req.date_ini, req.date_fin, max_years=max_years)
        for win_ini, win_fin in windows:
            job = ChunkJob(
                station_code=req.station_code,
                variable_id=req.variable_id,
                date_ini=win_ini,
                date_fin=win_fin,
                department=req.department,
                municipality=req.municipality,
                download_path=target_path,
            )
            key = _chunk_key(job)
            keys_for_request.append(key)
            if key not in seen:
                seen.add(key)
                jobs.append(job)
        request_to_keys.append(keys_for_request)
    return jobs, request_to_keys


def _partition(items: list[ChunkJob], workers: int) -> list[list[ChunkJob]]:
    workers = max(1, min(workers, len(items)))
    out: list[list[ChunkJob]] = [[] for _ in range(workers)]
    for idx, item in enumerate(items):
        out[idx % workers].append(item)
    return [p for p in out if p]


def _run_chunks_partition(
    partition: list[ChunkJob],
    time_wait: int,
    max_years: int,
) -> list[ChunkExecution]:
    results: list[ChunkExecution] = []
    if not partition:
        return results

    pid = os.getpid()
    codes = [job.station_code for job in partition]
    preview = ", ".join(codes[:5])
    print(
        f"[PID {pid}] Partition size={len(partition)} stations={preview}"
        f"{'...' if len(codes) > 5 else ''}",
        flush=True,
    )

    grouped: dict[str, list[ChunkJob]] = {}
    for job in partition:
        grouped.setdefault(job.download_path, []).append(job)

    def _is_broken_session_error(exc: Exception) -> bool:
        msg = str(exc).lower()
        markers = (
            "invalid session id",
            "disconnected",
            "not connected to devtools",
            "session deleted as the browser has closed",
        )
        return any(marker in msg for marker in markers)

    for download_path, jobs in grouped.items():
        session: DHIMESession | None = None
        completed_keys: set[tuple[str, int, str, str, str, str, str]] = set()
        try:
            for job in jobs:
                if session is None:
                    session = DHIMESession(download_path=download_path, time_wait=time_wait)
                    session.__enter__()
                    print(
                        f"[PID {pid}] sesion abierta para download_path={download_path}",
                        flush=True,
                    )

                print(
                    f"[PID {pid}] intentando {job.station_code} "
                    f"{job.date_ini}->{job.date_fin}",
                    flush=True,
                )
                req = StationRequest(
                    download_path=download_path,
                    date_ini=job.date_ini,
                    date_fin=job.date_fin,
                    department=job.department,
                    municipality=job.municipality,
                    station_code=job.station_code,
                    variable_id=job.variable_id,
                )
                key = _chunk_key(job)

                def _try_download_once() -> list[Path]:
                    assert session is not None
                    return session.download_one(req, max_years=max_years)

                try:
                    csv_paths = _try_download_once()
                except Exception as exc:  # noqa: BLE001
                    if _is_broken_session_error(exc):
                        print(
                            f"[PID {pid}] sesion rota detectada; reabriendo navegador "
                            f"para {job.station_code} {job.date_ini}->{job.date_fin}",
                            flush=True,
                        )
                        try:
                            if session is not None:
                                session.__exit__(None, None, None)
                        finally:
                            session = None

                        try:
                            session = DHIMESession(download_path=download_path, time_wait=time_wait)
                            session.__enter__()
                            print(
                                f"[PID {pid}] sesion reabierta; reintento inmediato",
                                flush=True,
                            )
                            csv_paths = _try_download_once()
                        except Exception as retry_exc:  # noqa: BLE001
                            results.append(
                                ChunkExecution(
                                    key=key,
                                    status="ERROR",
                                    message=str(retry_exc),
                                    csv_path=None,
                                )
                            )
                            print(
                                f"[PID {pid}] ERROR {job.station_code} "
                                f"{job.date_ini}->{job.date_fin}: {retry_exc}",
                                flush=True,
                            )
                            continue
                    else:
                        results.append(
                            ChunkExecution(
                                key=key,
                                status="ERROR",
                                message=str(exc),
                                csv_path=None,
                            )
                        )
                        print(
                            f"[PID {pid}] ERROR {job.station_code} "
                            f"{job.date_ini}->{job.date_fin}: {exc}",
                            flush=True,
                        )
                        continue

                csv_path = str(csv_paths[0]) if csv_paths else None
                results.append(
                    ChunkExecution(
                        key=key,
                        status="OK",
                        message="Descarga completada",
                        csv_path=csv_path,
                    )
                )
                print(
                    f"[PID {pid}] OK {job.station_code} "
                    f"{job.date_ini}->{job.date_fin}",
                    flush=True,
                )
                completed_keys.add(key)
        except BaseException as exc:
            print(f"[PID {pid}] excepcion en worker; forzando cierre: {exc}", flush=True)
            for pending in jobs:
                pkey = _chunk_key(pending)
                if pkey in completed_keys:
                    continue
                results.append(
                    ChunkExecution(
                        key=pkey,
                        status="ERROR",
                        message=f"WORKER_FATAL: {exc}",
                        csv_path=None,
                    )
                )
        finally:
            if session is not None:
                try:
                    session.__exit__(None, None, None)
                except Exception:
                    pass
            print(
                f"[PID {pid}] sesion cerrada para download_path={download_path}",
                flush=True,
            )
    return results


def _extract_window_from_key(key: tuple[str, int, str, str, str, str, str]) -> tuple[str, str]:
    return (key[2], key[3])


def _collect_request_result(
    request: StationRequest,
    keys: Iterable[tuple[str, int, str, str, str, str, str]],
    executed: dict[tuple[str, int, str, str, str, str, str], ChunkExecution],
) -> DownloadResult:
    csv_paths: list[Path] = []
    windows: list[tuple[str, str]] = []
    errors: list[str] = []

    for key in keys:
        windows.append(_extract_window_from_key(key))
        item = executed.get(key)
        if item is None:
            errors.append(f"Sin resultado para ventana {key[2]}-{key[3]}")
            continue
        if item.status != "OK" or not item.csv_path:
            errors.append(f"{key[2]}-{key[3]}: {item.message}")
            continue
        csv_paths.append(Path(item.csv_path))

    if errors:
        return DownloadResult(
            request=request,
            csv_paths=csv_paths,
            windows=windows,
            status="ERROR",
            message=" | ".join(errors),
        )

    return DownloadResult(
        request=request,
        csv_paths=csv_paths,
        windows=windows,
        status="OK",
        message="Descarga completada",
    )


def batch_download(
    requests,
    download_path: str | Path | None = None,
    time_wait: int = DEFAULT_TIMEOUT,
    max_years: int = 25,
    parallel: bool = False,
    workers: int = 2,
) -> list[DownloadResult]:
    """
    Ejecuta solicitudes DHIME en lote.

    - parallel=False (default): una sola sesión de Chrome.
    - parallel=True: multiproceso (1 sesión por worker).
    """
    normalized = [coerce_request(item) for item in requests]
    if not normalized:
        return []

    if workers < 1:
        raise ValueError("workers debe ser >= 1")

    base_path = Path(download_path).absolute() if download_path else None
    jobs, request_to_keys = _expand_to_chunks(normalized, max_years=max_years, base_path=base_path)

    executed: dict[tuple[str, int, str, str, str, str, str], ChunkExecution] = {}

    if parallel and len(jobs) > 1 and workers > 1:
        partitions = _partition(jobs, workers)
        if not partitions:
            raise RuntimeError("No se generaron particiones para ejecución paralela.")
        if any(len(p) == 0 for p in partitions):
            raise RuntimeError("Se detectaron particiones vacías en modo paralelo.")
        signatures = [tuple(sorted(_chunk_key(j) for j in p)) for p in partitions]
        if len(set(signatures)) != len(signatures):
            raise RuntimeError("Se detectaron particiones duplicadas en modo paralelo.")
        print(
            f"[batch] jobs={len(jobs)} workers={len(partitions)} "
            f"partitions_sizes={[len(p) for p in partitions]}",
            flush=True,
        )
        with ProcessPoolExecutor(max_workers=len(partitions)) as pool:
            futures = [
                pool.submit(_run_chunks_partition, partition, time_wait, max_years)
                for partition in partitions
            ]
            for fut in as_completed(futures):
                for item in fut.result():
                    executed[item.key] = item
    else:
        for item in _run_chunks_partition(jobs, time_wait, max_years):
            executed[item.key] = item

    results: list[DownloadResult] = []
    for req, keys in zip(normalized, request_to_keys):
        effective = req
        if base_path is not None:
            effective = StationRequest(
                download_path=str(base_path),
                date_ini=req.date_ini,
                date_fin=req.date_fin,
                department=req.department,
                municipality=req.municipality,
                station_code=req.station_code,
                variable_id=req.variable_id,
            )
        results.append(_collect_request_result(effective, keys, executed))
    return results
