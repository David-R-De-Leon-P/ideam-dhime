# -*- coding: utf-8 -*-
"""Tests para ideam_dhime.catalog."""

from __future__ import annotations

import re

import pytest

from ideam_dhime import (
    CATALOG_GENERATED_AT,
    Frequency,
    UnknownVariableIdError,
    VARIABLES_IDEAM,
    resolve_frequency,
    resolve_variable,
)


def test_catalog_generated_at_is_iso_date():
    assert re.match(r"^\d{4}-\d{2}-\d{2}$", CATALOG_GENERATED_AT)


def test_catalog_non_empty_and_ids_are_sequential():
    assert len(VARIABLES_IDEAM) > 0
    ids = sorted(VARIABLES_IDEAM.keys())
    assert ids[0] == 1
    assert ids == list(range(1, len(ids) + 1))


def test_resolve_variable_known_id():
    category, name = resolve_variable(7)
    assert category == "Caudal"
    assert name == "Caudal medio diario"


def test_resolve_frequency_known_id():
    assert resolve_frequency(7) is Frequency.DAILY
    assert resolve_frequency(69) is Frequency.TWO_MINUTES


def test_catalog_entries_include_frequency():
    category, name, frequency = VARIABLES_IDEAM[7]
    assert category == "Caudal"
    assert name == "Caudal medio diario"
    assert frequency is Frequency.DAILY


def test_resolve_variable_unknown_id_raises():
    with pytest.raises(UnknownVariableIdError):
        resolve_variable(10 ** 9)


def test_resolve_frequency_unknown_id_raises():
    with pytest.raises(UnknownVariableIdError):
        resolve_frequency(10 ** 9)
