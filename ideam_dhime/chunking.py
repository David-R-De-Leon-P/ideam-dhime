# -*- coding: utf-8 -*-
"""Utilidades para trocear rangos en ventanas de años."""

from __future__ import annotations

from datetime import date, timedelta

DATE_FMT = "%d/%m/%Y"


def _parse_ddmmyyyy(value: str) -> date:
    day, month, year = value.split("/")
    return date(int(year), int(month), int(day))


def _format_ddmmyyyy(value: date) -> str:
    return value.strftime(DATE_FMT)


def _add_years_safe(d: date, years: int) -> date:
    """Suma años manejando 29-febrero."""
    try:
        return d.replace(year=d.year + years)
    except ValueError:
        return d.replace(month=2, day=28, year=d.year + years)


def split_windows(date_ini: str, date_fin: str, max_years: int = 25) -> list[tuple[str, str]]:
    """
    Divide [date_ini, date_fin] en ventanas de máximo ``max_years``.

    Por defecto usa 25 años para mantenerse por debajo del límite del portal.
    """
    if max_years <= 0:
        raise ValueError("max_years debe ser mayor que cero")
    start = _parse_ddmmyyyy(date_ini)
    end = _parse_ddmmyyyy(date_fin)
    if start > end:
        raise ValueError("date_ini no puede ser mayor que date_fin")

    windows: list[tuple[str, str]] = []
    current_start = start
    while current_start <= end:
        next_start = _add_years_safe(current_start, max_years)
        current_end = min(end, next_start - timedelta(days=1))
        windows.append((_format_ddmmyyyy(current_start), _format_ddmmyyyy(current_end)))
        current_start = current_end + timedelta(days=1)
    return windows


def split_30y(date_ini: str, date_fin: str) -> list[tuple[str, str]]:
    """Alias compatible: conserva el nombre histórico, usando 25 años por defecto."""
    return split_windows(date_ini, date_fin, max_years=25)


if __name__ == "__main__":
    assert split_windows("01/01/1970", "31/12/2025", 25) == [
        ("01/01/1970", "31/12/1994"),
        ("01/01/1995", "31/12/2019"),
        ("01/01/2020", "31/12/2025"),
    ]
    assert split_windows("01/01/2020", "01/01/2020", 25) == [
        ("01/01/2020", "01/01/2020")
    ]
    assert split_windows("29/02/2020", "28/02/2050", 25) == [
        ("29/02/2020", "27/02/2045"),
        ("28/02/2045", "28/02/2050"),
    ]
    print("chunking.py OK")
