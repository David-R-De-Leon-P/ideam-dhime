# -*- coding: utf-8 -*-
"""Tests para ideam_dhime.chunking."""

from __future__ import annotations

from datetime import datetime

import pytest

from ideam_dhime.chunking import split_30y, split_by_days, split_for_frequency, split_windows
from ideam_dhime.frequencies import Frequency


def test_split_25_years_default():
    assert split_windows("01/01/1970", "31/12/2025") == [
        ("01/01/1970", "31/12/1994"),
        ("01/01/1995", "31/12/2019"),
        ("01/01/2020", "31/12/2025"),
    ]


def test_split_single_day():
    assert split_windows("01/01/2020", "01/01/2020") == [("01/01/2020", "01/01/2020")]


def test_split_handles_leap_day():
    assert split_windows("29/02/2020", "28/02/2050") == [
        ("29/02/2020", "27/02/2045"),
        ("28/02/2045", "28/02/2050"),
    ]


def test_split_custom_max_years():
    windows = split_windows("01/01/2000", "31/12/2030", max_years=10)
    assert windows[0][0] == "01/01/2000"
    assert windows[-1][1] == "31/12/2030"
    assert all(len(pair) == 2 for pair in windows)


def test_windows_stay_inside_original_range_and_are_contiguous():
    ini = "13/02/1987"
    fin = "30/11/2025"
    windows = split_windows(ini, fin, max_years=25)
    assert windows[0][0] == ini
    assert windows[-1][1] == fin

    prev_end = None
    for w_ini, w_fin in windows:
        dt_ini = datetime.strptime(w_ini, "%d/%m/%Y")
        dt_fin = datetime.strptime(w_fin, "%d/%m/%Y")
        assert dt_ini <= dt_fin
        if prev_end is not None:
            assert (dt_ini - prev_end).days == 1
        prev_end = dt_fin


def test_split_invalid_inputs():
    with pytest.raises(ValueError):
        split_windows("01/01/2020", "01/01/2020", max_years=0)
    with pytest.raises(ValueError):
        split_windows("31/12/2025", "01/01/1970")


def test_split_30y_alias_equals_default():
    assert split_30y("01/01/1970", "31/12/2025") == split_windows(
        "01/01/1970", "31/12/2025"
    )


def test_split_by_days_is_contiguous():
    assert split_by_days("01/01/2024", "31/01/2024", max_days=10) == [
        ("01/01/2024", "10/01/2024"),
        ("11/01/2024", "20/01/2024"),
        ("21/01/2024", "30/01/2024"),
        ("31/01/2024", "31/01/2024"),
    ]


def test_split_for_frequency_uses_frequency_limits():
    assert split_for_frequency("01/01/1970", "31/12/2025", Frequency.DAILY) == [
        ("01/01/1970", "31/12/1994"),
        ("01/01/1995", "31/12/2019"),
        ("01/01/2020", "31/12/2025"),
    ]
    assert split_for_frequency("01/01/2024", "31/01/2024", Frequency.TWO_MINUTES) == [
        ("01/01/2024", "25/01/2024"),
        ("26/01/2024", "31/01/2024"),
    ]


def test_split_for_frequency_allows_overrides():
    assert split_for_frequency(
        "01/01/2024", "20/01/2024", Frequency.TWO_MINUTES, max_days=7
    ) == [
        ("01/01/2024", "07/01/2024"),
        ("08/01/2024", "14/01/2024"),
        ("15/01/2024", "20/01/2024"),
    ]
    assert split_for_frequency("01/01/2000", "31/12/2009", Frequency.DAILY, max_years=4) == [
        ("01/01/2000", "31/12/2003"),
        ("01/01/2004", "31/12/2007"),
        ("01/01/2008", "31/12/2009"),
    ]
