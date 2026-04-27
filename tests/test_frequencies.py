# -*- coding: utf-8 -*-
"""Tests para frecuencias DHIME."""

from __future__ import annotations

from ideam_dhime.frequencies import FREQUENCY_LIMITS, Frequency, infer_frequency_from_name


def test_frequency_limits_are_conservative():
    assert FREQUENCY_LIMITS[Frequency.DAILY].max_years == 25
    assert FREQUENCY_LIMITS[Frequency.HOURLY].max_years == 8
    assert FREQUENCY_LIMITS[Frequency.TWO_MINUTES].max_days == 25
    assert FREQUENCY_LIMITS[Frequency.THREE_TIMES_DAILY].max_years == 4


def test_infer_frequency_from_name_common_cases():
    assert infer_frequency_from_name("Caudal medio diario") is Frequency.DAILY
    assert infer_frequency_from_name("Caudal máximo mensual") is Frequency.MONTHLY
    assert infer_frequency_from_name("Caudal medio anual") is Frequency.ANNUAL
    assert infer_frequency_from_name("Radiación solar global horaria VALIDADA") is Frequency.HOURLY
    assert infer_frequency_from_name("Nivel mínimo cada 2 minutos") is Frequency.TWO_MINUTES
    assert infer_frequency_from_name("Velocidad 10 minutal del viento media diaria") is Frequency.TEN_MINUTES
    assert infer_frequency_from_name("Nubosidad de las 700, 1300 y 1800") is Frequency.THREE_TIMES_DAILY
