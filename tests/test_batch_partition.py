# -*- coding: utf-8 -*-
"""Tests para lógica interna de partición y expansión de chunks (sin Selenium)."""

from __future__ import annotations

from pathlib import Path

from ideam_dhime import StationRequest
from ideam_dhime.batch import (
    ChunkExecution,
    _chunk_key,
    _collect_request_result,
    _expand_to_chunks,
    _partition,
)


def _req(station_code: str, date_ini: str, date_fin: str) -> StationRequest:
    return StationRequest(
        download_path="./datos",
        date_ini=date_ini,
        date_fin=date_fin,
        department="Huila",
        municipality="La Plata",
        station_code=station_code,
        variable_id=7,
    )


def test_expand_to_chunks_splits_and_deduplicates():
    requests = [
        _req("A", "01/01/1970", "31/12/2025"),
        _req("A", "01/01/1970", "31/12/2025"),
    ]
    jobs, request_to_keys = _expand_to_chunks(requests, max_years=25, base_path=None)
    assert len(jobs) == 3
    assert len(request_to_keys) == 2
    assert request_to_keys[0] == request_to_keys[1]


def test_partition_round_robin_is_disjoint():
    requests = [_req(f"S{i}", "01/01/2020", "31/12/2020") for i in range(4)]
    jobs, _ = _expand_to_chunks(requests, max_years=25, base_path=None)
    partitions = _partition(jobs, workers=2)
    assert len(partitions) == 2
    all_codes = [j.station_code for p in partitions for j in p]
    assert sorted(all_codes) == ["S0", "S1", "S2", "S3"]
    sigs = [tuple(sorted(_chunk_key(j) for j in p)) for p in partitions]
    assert len(set(sigs)) == len(sigs)


def test_partition_trims_empty_buckets():
    requests = [_req("A", "01/01/2020", "31/12/2020")]
    jobs, _ = _expand_to_chunks(requests, max_years=25, base_path=None)
    partitions = _partition(jobs, workers=4)
    assert partitions == [jobs]


def test_expand_respects_base_path():
    base = Path("./otra_carpeta").absolute()
    jobs, _ = _expand_to_chunks(
        [_req("A", "01/01/2020", "31/12/2020")], max_years=25, base_path=base
    )
    assert jobs[0].download_path == str(base)


def test_expand_to_chunks_uses_frequency_limits_by_default():
    req = StationRequest(
        download_path="./datos",
        date_ini="01/01/2024",
        date_fin="31/01/2024",
        department="Huila",
        municipality="La Plata",
        station_code="A",
        variable_id=69,
    )
    jobs, _ = _expand_to_chunks([req], max_years=None, base_path=None)
    assert [(job.date_ini, job.date_fin) for job in jobs] == [
        ("01/01/2024", "25/01/2024"),
        ("26/01/2024", "31/01/2024"),
    ]


def test_expand_to_chunks_respects_request_overrides():
    req = StationRequest(
        download_path="./datos",
        date_ini="01/01/2024",
        date_fin="20/01/2024",
        department="Huila",
        municipality="La Plata",
        station_code="A",
        variable_id=69,
        max_days=7,
    )
    jobs, _ = _expand_to_chunks([req], max_years=None, base_path=None)
    assert [(job.date_ini, job.date_fin) for job in jobs] == [
        ("01/01/2024", "07/01/2024"),
        ("08/01/2024", "14/01/2024"),
        ("15/01/2024", "20/01/2024"),
    ]


def test_expand_to_chunks_applies_global_min_date():
    req = _req("A", "01/01/1900", "31/12/1980")
    jobs, _ = _expand_to_chunks([req], max_years=25, base_path=None, min_date="01/01/1970")
    assert [(job.date_ini, job.date_fin) for job in jobs] == [
        ("01/01/1970", "31/12/1980"),
    ]


def test_expand_to_chunks_request_min_date_overrides_global():
    req = StationRequest(
        download_path="./datos",
        date_ini="01/01/1900",
        date_fin="31/12/1980",
        department="Huila",
        municipality="La Plata",
        station_code="A",
        variable_id=7,
        min_date="01/01/1980",
    )
    jobs, _ = _expand_to_chunks([req], max_years=25, base_path=None, min_date="01/01/1970")
    assert [(job.date_ini, job.date_fin) for job in jobs] == [
        ("01/01/1980", "31/12/1980"),
    ]


def test_collect_request_result_skips_no_data_and_merges(tmp_path):
    req = StationRequest(
        download_path=tmp_path,
        date_ini="01/01/1970",
        date_fin="31/12/2026",
        department="Huila",
        municipality="La Plata",
        station_code="21017050",
        variable_id=7,
    )
    keys = [
        (req.station_code, req.variable_id, "01/01/1970", "31/12/1994", req.department, req.municipality, str(tmp_path)),
        (req.station_code, req.variable_id, "01/01/1995", "31/12/2019", req.department, req.municipality, str(tmp_path)),
        (req.station_code, req.variable_id, "01/01/2020", "31/12/2026", req.department, req.municipality, str(tmp_path)),
    ]
    csv_a = tmp_path / "a.csv"
    csv_b = tmp_path / "b.csv"
    csv_a.write_text("Fecha,Valor\n01/01/1990,1\n", encoding="utf-8")
    csv_b.write_text("Fecha,Valor\n01/01/2000,2\n", encoding="utf-8")
    executed = {
        keys[0]: ChunkExecution(key=keys[0], status="OK", message="ok", csv_path=str(csv_a)),
        keys[1]: ChunkExecution(key=keys[1], status="OK", message="ok", csv_path=str(csv_b)),
        keys[2]: ChunkExecution(
            key=keys[2],
            status="ERROR",
            message="SIN_DATOS_EN_RANGO: estación 21017050",
            csv_path=None,
        ),
    }

    result = _collect_request_result(
        req,
        keys,
        executed,
        min_date=None,
        merge_chunks=True,
        keep_chunks=False,
    )

    assert result.status == "OK"
    assert "parcialmente" in result.message
    assert result.csv_final is not None
    assert result.csv_final.exists()


def test_collect_request_result_returns_error_when_all_no_data(tmp_path):
    req = StationRequest(
        download_path=tmp_path,
        date_ini="01/01/1970",
        date_fin="31/12/2026",
        department="Huila",
        municipality="La Plata",
        station_code="21017060",
        variable_id=7,
    )
    key = (req.station_code, req.variable_id, "01/01/1970", "31/12/2026", req.department, req.municipality, str(tmp_path))
    executed = {
        key: ChunkExecution(
            key=key,
            status="ERROR",
            message="SIN_SOLAPE: estación 21017060",
            csv_path=None,
        )
    }

    result = _collect_request_result(
        req,
        [key],
        executed,
        min_date=None,
        merge_chunks=True,
        keep_chunks=False,
    )

    assert result.status == "ERROR"
    assert "No hay datos para el periodo solicitado" in result.message


def test_final_csv_path_uses_global_min_date_when_request_has_no_dates(tmp_path):
    req = StationRequest(
        download_path=tmp_path,
        station_code="21017020",
        variable_id=7,
        department="Huila",
        municipality="La Plata",
    )
    key = (req.station_code, req.variable_id, "01/01/1970", "31/12/1994", req.department, req.municipality, str(tmp_path))
    csv_chunk = tmp_path / "chunk.csv"
    csv_chunk.write_text("Fecha,Valor\n01/01/1970,1\n", encoding="utf-8")
    executed = {
        key: ChunkExecution(key=key, status="OK", message="ok", csv_path=str(csv_chunk))
    }
    result = _collect_request_result(
        req,
        [key],
        executed,
        min_date="01/01/1970",
        merge_chunks=True,
        keep_chunks=False,
    )
    assert result.csv_final is not None
    assert result.csv_final.name.startswith("21017020-7-01011970-")
