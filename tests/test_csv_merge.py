# -*- coding: utf-8 -*-
"""Tests para consolidación de CSVs de chunks."""

from __future__ import annotations

import csv

import pytest

from ideam_dhime.csv_merge import merge_station_csvs


def _write_csv(path, rows, header=("Fecha", "Valor")):
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def test_merge_station_csvs_sorts_and_deduplicates(tmp_path):
    chunk_a = tmp_path / "a.csv"
    chunk_b = tmp_path / "b.csv"
    output = tmp_path / "final.csv"
    _write_csv(
        chunk_a,
        [
            {"Fecha": "02/01/2024", "Valor": "2"},
            {"Fecha": "01/01/2024", "Valor": "1"},
        ],
    )
    _write_csv(
        chunk_b,
        [
            {"Fecha": "02/01/2024", "Valor": "2"},
            {"Fecha": "03/01/2024", "Valor": "3"},
        ],
    )

    final = merge_station_csvs([chunk_a, chunk_b], output)

    assert final == output
    with output.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows == [
        {"Fecha": "01/01/2024", "Valor": "1"},
        {"Fecha": "02/01/2024", "Valor": "2"},
        {"Fecha": "03/01/2024", "Valor": "3"},
    ]
    assert chunk_a.exists()
    assert chunk_b.exists()


def test_merge_station_csvs_can_remove_chunks(tmp_path):
    chunk = tmp_path / "chunk.csv"
    output = tmp_path / "final.csv"
    _write_csv(chunk, [{"Fecha": "01/01/2024", "Valor": "1"}])

    merge_station_csvs([chunk], output, keep_chunks=False)

    assert output.exists()
    assert not chunk.exists()


def test_merge_station_csvs_rejects_incompatible_headers(tmp_path):
    chunk_a = tmp_path / "a.csv"
    chunk_b = tmp_path / "b.csv"
    _write_csv(chunk_a, [{"Fecha": "01/01/2024", "Valor": "1"}])
    _write_csv(chunk_b, [{"Fecha": "01/01/2024", "Caudal": "1"}], header=("Fecha", "Caudal"))

    with pytest.raises(ValueError):
        merge_station_csvs([chunk_a, chunk_b], tmp_path / "final.csv")
