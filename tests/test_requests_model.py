# -*- coding: utf-8 -*-
"""Tests para ideam_dhime.requests_model."""

from __future__ import annotations

import pytest

from ideam_dhime.requests_model import StationRequest, coerce_request
from ideam_dhime.station_catalog import StationMetadata


BASE = dict(
    download_path="./datos",
    date_ini="01/01/2020",
    date_fin="31/12/2020",
    department="Huila",
    municipality="La Plata",
    station_code="2105700152",
    variable_id=7,
)


def test_coerce_from_dataclass():
    req = StationRequest(**BASE)
    assert coerce_request(req) is req


def test_coerce_from_dict():
    req = coerce_request({**BASE, "max_years": 8, "max_days": 25, "min_date": "01/01/1970"})
    assert isinstance(req, StationRequest)
    assert req.station_code == "2105700152"
    assert req.variable_id == 7
    assert req.max_years == 8
    assert req.max_days == 25
    assert req.min_date == "01/01/1970"


def test_coerce_from_tuple():
    as_tuple = (
        BASE["download_path"],
        BASE["date_ini"],
        BASE["date_fin"],
        BASE["department"],
        BASE["municipality"],
        BASE["station_code"],
        BASE["variable_id"],
    )
    req = coerce_request(as_tuple)
    assert isinstance(req, StationRequest)
    assert req.variable_id == 7


def test_coerce_from_tuple_with_overrides():
    as_tuple = (
        BASE["download_path"],
        BASE["date_ini"],
        BASE["date_fin"],
        BASE["department"],
        BASE["municipality"],
        BASE["station_code"],
        BASE["variable_id"],
        8,
        25,
    )
    req = coerce_request(as_tuple)
    assert req.max_years == 8
    assert req.max_days == 25


def test_coerce_from_tuple_with_min_date():
    as_tuple = (
        BASE["download_path"],
        BASE["date_ini"],
        BASE["date_fin"],
        BASE["department"],
        BASE["municipality"],
        BASE["station_code"],
        BASE["variable_id"],
        "01/01/1970",
    )
    req = coerce_request(as_tuple)
    assert req.min_date == "01/01/1970"


def test_coerce_invalid_type():
    with pytest.raises(TypeError):
        coerce_request(42)


def test_coerce_invalid_tuple_length():
    with pytest.raises(TypeError):
        coerce_request(("only", "two"))


def test_coerce_resolves_department_and_municipality_from_station_code(monkeypatch):
    import ideam_dhime.stations_generated as generated

    monkeypatch.setattr(
        generated,
        "STATIONS_DHIME",
        {
            "2105700152": StationMetadata(
                station_code="2105700152",
                station_name="LA PLATA",
                department="HUILA",
                municipality="LA PLATA",
            )
        },
    )

    req = coerce_request(
        {
            "download_path": "./datos",
            "station_code": "2105700152",
            "variable_id": 7,
        }
    )

    assert req.department == "HUILA"
    assert req.municipality == "LA PLATA"
    assert req.date_ini is None
    assert req.date_fin is None


def test_station_not_found_error_includes_context(monkeypatch):
    """El mensaje de StationNotFoundError menciona código, variable y periodo."""
    import ideam_dhime.stations_generated as generated
    from ideam_dhime.exceptions import StationNotFoundError

    monkeypatch.setattr(generated, "STATIONS_DHIME", {})

    with pytest.raises(StationNotFoundError) as exc_info:
        coerce_request(
            {
                "download_path": "./datos",
                "station_code": "9999999",
                "variable_id": 7,
                "date_ini": "01/01/2020",
                "date_fin": "31/12/2022",
            }
        )

    msg = str(exc_info.value)
    assert "9999999" in msg
    assert "variable_id=7" in msg
    assert "01/01/2020" in msg
    assert "31/12/2022" in msg
    assert "dhime.ideam.gov.co" in msg
