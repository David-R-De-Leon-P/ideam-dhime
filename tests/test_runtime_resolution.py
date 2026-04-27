# -*- coding: utf-8 -*-
"""Tests de resolución runtime del catálogo maestro."""

from __future__ import annotations

import pytest

from ideam_dhime import StationNotFoundError
from ideam_dhime.station_catalog import StationMetadata, resolve_station_metadata


def test_resolve_station_metadata_from_generated_snapshot(monkeypatch):
    import ideam_dhime.stations_generated as generated

    metadata = StationMetadata(
        station_code="123",
        station_name="ESTACION TEST",
        department="HUILA",
        municipality="LA PLATA",
    )
    monkeypatch.setattr(generated, "STATIONS_DHIME", {"123": metadata})

    assert resolve_station_metadata("123") is metadata


def test_resolve_station_metadata_unknown_raises(monkeypatch):
    import ideam_dhime.stations_generated as generated

    monkeypatch.setattr(generated, "STATIONS_DHIME", {})

    with pytest.raises(StationNotFoundError):
        resolve_station_metadata("NO_EXISTE")
