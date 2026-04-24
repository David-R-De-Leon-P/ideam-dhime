# -*- coding: utf-8 -*-
"""Tests para ideam_dhime.requests_model."""

from __future__ import annotations

import pytest

from ideam_dhime.requests_model import StationRequest, coerce_request


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
    req = coerce_request(dict(BASE))
    assert isinstance(req, StationRequest)
    assert req.station_code == "2105700152"
    assert req.variable_id == 7


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


def test_coerce_invalid_type():
    with pytest.raises(TypeError):
        coerce_request(42)


def test_coerce_invalid_tuple_length():
    with pytest.raises(TypeError):
        coerce_request(("only", "two"))
