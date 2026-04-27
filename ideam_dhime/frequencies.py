# -*- coding: utf-8 -*-
"""Frecuencias DHIME y límites conservadores para dividir consultas."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Final


class Frequency(str, Enum):
    """Frecuencia de una variable del catálogo DHIME."""

    ANNUAL = "anual"
    MONTHLY = "mensual"
    DAILY = "diario"
    HOURLY = "horario"
    TEN_MINUTES = "cada_10_minutos"
    FIVE_MINUTES = "cada_05_minutos"
    TWO_MINUTES = "cada_02_minutos"
    MINUTAL = "minutal"
    TWICE_DAILY = "02_veces_al_dia"
    THREE_TIMES_DAILY = "03_veces_al_dia"


@dataclass(frozen=True)
class FrequencyLimit:
    """Límite seguro de consulta para una frecuencia."""

    max_years: int | None = None
    max_days: int | None = None


FREQUENCY_LIMITS: Final[dict[Frequency, FrequencyLimit]] = {
    Frequency.ANNUAL: FrequencyLimit(max_years=40),
    Frequency.MONTHLY: FrequencyLimit(max_years=40),
    Frequency.DAILY: FrequencyLimit(max_years=25),
    Frequency.HOURLY: FrequencyLimit(max_years=8),
    Frequency.TEN_MINUTES: FrequencyLimit(max_days=25),
    Frequency.FIVE_MINUTES: FrequencyLimit(max_days=25),
    Frequency.TWO_MINUTES: FrequencyLimit(max_days=25),
    Frequency.MINUTAL: FrequencyLimit(max_days=25),
    Frequency.TWICE_DAILY: FrequencyLimit(max_years=4),
    Frequency.THREE_TIMES_DAILY: FrequencyLimit(max_years=4),
}


def _norm(value: str) -> str:
    return (
        value.lower()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
    )


def infer_frequency_from_name(name: str) -> Frequency:
    """Infiere la frecuencia desde el nombre oficial de variable DHIME."""
    text = _norm(name)

    if any(token in text for token in ("700, 1300 y 1800", "07:00", "13:00", "19:00")):
        return Frequency.THREE_TIMES_DAILY
    if "500 a 1800" in text or "24 horas" in text:
        return Frequency.DAILY
    if "cada 10 min" in text or "10 minutal" in text:
        return Frequency.TEN_MINUTES
    if "cada 05 min" in text or "cada 5 min" in text or "5 minutal" in text:
        return Frequency.FIVE_MINUTES
    if "cada 02 min" in text or "cada 2 min" in text or "2 minutal" in text:
        return Frequency.TWO_MINUTES
    if "minutal" in text:
        return Frequency.MINUTAL
    if "horaria" in text or "horario" in text:
        return Frequency.HOURLY
    if "mensual" in text:
        return Frequency.MONTHLY
    if "anual" in text:
        return Frequency.ANNUAL
    if "diaria" in text or "diario" in text or "dia pluviometrico" in text:
        return Frequency.DAILY

    # El portal tiende a omitir la etiqueta de frecuencia en algunos nombres
    # diarios; usamos diario como valor conservador de compatibilidad.
    return Frequency.DAILY
