# -*- coding: utf-8 -*-
"""Tests para lógica interna de partición y expansión de chunks (sin Selenium)."""

from __future__ import annotations

from pathlib import Path

from ideam_dhime import StationRequest
from ideam_dhime.batch import _chunk_key, _expand_to_chunks, _partition


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
