# -*- coding: utf-8 -*-
"""
Catálogo indexado IDEAM incluido en el paquete:
ID -> (categoría, nombre de variable, frecuencia).

La categoría es el texto del desplegable de parámetros; el nombre de variable
es el de la tabla ``DatosBuscarVariables`` (y se usa como ``variable_code`` en
la descarga). La constante ``CATALOG_GENERATED_AT`` documenta la fecha del
snapshot distribuido con esta versión del código.
"""

from __future__ import annotations

from typing import Final

from ideam_dhime.exceptions import UnknownVariableIdError
from ideam_dhime.frequencies import Frequency, infer_frequency_from_name

# Fecha (ISO 8601) en que se fijó este snapshot para publicación del paquete.
CATALOG_GENERATED_AT: Final[str] = "2026-04-21"

_VARIABLES_IDEAM_BASE: dict[int, tuple[str, str]] = {
    1: ('Brillo solar', 'Brillo solar de 500 a 1800'),
    2: ('Brillo solar', 'Brillo solar total diario'),
    3: ('Brillo solar', 'Brillo solar total mensual'),
    4: ('Brillo solar', 'Brillo solar total anual'),
    5: ('Caudal', 'Caudal máximo diario'),
    6: ('Caudal', 'Caudal mínimo diario'),
    7: ('Caudal', 'Caudal medio diario'),
    8: ('Caudal', 'Caudal máximo mensual'),
    9: ('Caudal', 'Caudal medio mensual'),
    10: ('Caudal', 'Caudal mínimo mensual'),
    11: ('Caudal', 'Caudal máximo anual'),
    12: ('Caudal', 'Caudal medio anual'),
    13: ('Caudal', 'Caudal mínimo anual'),
    14: ('Concentración media de sedimentos', 'Concentración media diaria en Kg/m3'),
    15: ('Concentración media de sedimentos', 'Concentración media mínima mensual'),
    16: ('Concentración media de sedimentos', 'Concentración media máxima mensual'),
    17: ('Concentración media de sedimentos', 'Concentración media mensual'),
    18: ('Concentración media de sedimentos', 'Concentración media anual'),
    19: ('Concentración media de sedimentos', 'Concentración media mínima anual'),
    20: ('Concentración media de sedimentos', 'Concentración media máxima anual'),
    21: ('Concentración superficial de sedimentos', 'Concentración superficial promedio diario de sedimentos'),
    22: ('Dirección del viento', 'Dirección del viento de las 24 horas en grados'),
    23: ('Dirección del viento', 'Dirección del viento en grados de la máxima velocidad en el día'),
    24: ('Dirección del viento', 'Dirección vectorial del viento media diaria'),
    25: ('Dirección del viento', 'Dirección vectorial del viento media mensual'),
    26: ('Dirección del viento', 'Dirección vectorial del viento media anual'),
    27: ('Evaporación', 'Evaporación total diaria'),
    28: ('Fenómenos atmosféricos', 'Fenómeno atmosférico de las 700, 1300 y 1800'),
    29: ('Fenómenos atmosféricos', 'Fenómeno atmosférico predominante diario'),
    30: ('Humedad relativa', 'Humedad relativa del aire a 2 metros cada 2 minutos'),
    31: ('Humedad relativa', 'Humedad relativa del aire a 10 cm horaria'),
    32: ('Humedad relativa', 'Humedad relativa del aire a 2 metros horaria'),
    33: ('Humedad relativa', 'Húmeda relativa calculada horaria'),
    34: ('Humedad relativa', 'Humedad relativa del aire a 10 cm media diaria'),
    35: ('Humedad relativa', 'Húmeda relativa calculada mínima diaria'),
    36: ('Humedad relativa', 'Humedad relativa del aire a 2 metros media diaria'),
    37: ('Humedad relativa', 'Humedad relativa del aire a 10 cm mínima diaria'),
    38: ('Humedad relativa', 'Humedad relativa del aire 2 minutal a 2 metros máxima diaria'),
    39: ('Humedad relativa', 'Humedad relativa del aire a 2 metros mínima diaria'),
    40: ('Humedad relativa', 'Humedad relativa del aire 2 minutal a 2 metros (medición cada 2 minutos) mínima diaria'),
    41: ('Humedad relativa', 'Humedad relativa del aire 2 minutal a 2 metros (medición cada 2 minutos) media diaria'),
    42: ('Humedad relativa', 'Húmeda relativa calculada máxima diaria'),
    43: ('Humedad relativa', 'Humedad relativa del aire a 2 metros máxima diaria'),
    44: ('Humedad relativa', 'Humedad relativa del aire a 10 cm máxima diaria'),
    45: ('Humedad relativa', 'Humedad relativa del aire 2 minutal a 2 metros media mensual'),
    46: ('Humedad relativa', 'Húmeda relativa calculada mínima mensual'),
    47: ('Humedad relativa', 'Húmeda relativa calculada máxima mensual'),
    48: ('Humedad relativa', 'Humedad relativa del aire 2 minutal a 2 metros máxima mensual'),
    49: ('Humedad relativa', 'Humedad relativa del aire 2 minutal a 2 metros mínima mensual'),
    50: ('Humedad relativa', 'Humedad relativa del aire a 2 metros máxima mensual'),
    51: ('Humedad relativa', 'Humedad relativa del aire a 2 metros media mensual'),
    52: ('Humedad relativa', 'Humedad relativa del aire a 10 cm mínima mensual'),
    53: ('Humedad relativa', 'Humedad relativa del aire a 10 cm máxima mensual'),
    54: ('Humedad relativa', 'Humedad relativa del aire a 2 metros mínima mensual'),
    55: ('Humedad relativa', 'Humedad relativa del aire a 10 cm media mensual'),
    56: ('Humedad relativa', 'Húmeda relativa  media mensual'),
    57: ('Humedad relativa', 'Húmeda relativa calculada máxima anual'),
    58: ('Humedad relativa', 'Humedad relativa del aire 2 minutal a 2 metros máxima anual'),
    59: ('Humedad relativa', 'Humedad relativa del aire a 10 cm media anual'),
    60: ('Humedad relativa', 'Húmeda relativa calculada mínima anual'),
    61: ('Humedad relativa', 'Humedad relativa del aire a 2 metros media anual'),
    62: ('Humedad relativa', 'Humedad relativa del aire 2 minutal a 2 metros media anual'),
    63: ('Humedad relativa', 'Humedad relativa del aire 2 minutal a 2 metros mínima anual'),
    64: ('Humedad relativa', 'Humedad relativa del aire a 10 cm mínima anual'),
    65: ('Humedad relativa', 'Humedad relativa del aire a 2 metros máxima anual'),
    66: ('Humedad relativa', 'Humedad relativa del aire a 2 metros mínima anual'),
    67: ('Humedad relativa', 'Húmeda relativa calculada media anual'),
    68: ('Humedad relativa', 'Humedad relativa del aire a 10 cm máxima anual'),
    69: ('Nivel', 'Nivel mínimo cada 2 minutos'),
    70: ('Nivel', 'Nivel medio diario'),
    71: ('Nivel', 'Nivel mínimo diario'),
    72: ('Nivel', 'Nivel máximo diario'),
    73: ('Nivel', 'Nivel máximo mensual'),
    74: ('Nivel', 'Nivel medio mensual'),
    75: ('Nivel', 'Nivel mínimo mensual'),
    76: ('Nivel', 'Nivel máximo anual'),
    77: ('Nivel', 'Nivel medio anual'),
    78: ('Nivel', 'Nivel mínimo anual'),
    79: ('Nubosidad', 'Nubosidad de las 700, 1300 y 1800'),
    80: ('Nubosidad', 'Nubosidad 07:00 HLC media mensual'),
    81: ('Nubosidad', 'Nubosidad 07:00 HLC'),
    82: ('Nubosidad', 'Nubosidad 13:00 HLC'),
    83: ('Nubosidad', 'Nubosidad 19:00 HLC'),
    84: ('Precipitación', 'Día pluviométrico (convencional)'),
    85: ('Precipitación', 'Precipitación total mensual'),
    86: ('Precipitación', 'Precipitación total anual'),
    87: ('Radiación ultravioleta', 'Radiación UVA (longitud de onda 340 nm) media horaria'),
    88: ('Radiación ultravioleta', 'Radiación UVA (longitud de onda 340 nm) máxima horaria'),
    89: ('Radiación ultravioleta', 'Radiación UVA (longitud de onda 380 nm) media horaria'),
    90: ('Radiación ultravioleta', 'Radiación UVA (longitud de onda 380 nm) máxima horaria'),
    91: ('Radiación ultravioleta', 'Radiación UVB (longitud de onda 305 nm) media horaria'),
    92: ('Radiación ultravioleta', 'Radiación visible (PAR) máxima horaria'),
    93: ('Radiación ultravioleta', 'Radiación UVB (longitud de onda 320 nm) media horaria'),
    94: ('Radiación ultravioleta', 'Radiación UVB (longitud de onda 320 nm) máxima horaria'),
    95: ('Radiación ultravioleta', 'Radiación visible (PAR) media horaria'),
    96: ('Radiación ultravioleta', 'Radiación UVB (longitud de onda 305 nm) máxima horaria'),
    97: ('Radicación solar', 'Radiación solar global cada dos minutos VALIDADA'),
    98: ('Radicación solar', 'Radiación solar global horaria VALIDADA'),
    99: ('Radicación solar', 'Radiación solar global diaria VALIDADA'),
    100: ('Temperatura', 'Temperatura del aire a 2 metros'),
    101: ('Temperatura', 'Temperatura mínima del aire a 2 metros'),
    102: ('Temperatura', 'Temperatura seca de las 700, 1300 y 1800'),
    103: ('Temperatura', 'Temperatura húmeda de las 700, 1300 y 1800'),
    104: ('Temperatura', 'Temperatura seca media diaria'),
    105: ('Temperatura', 'Temperatura máxima diaria'),
    106: ('Temperatura', 'Temperatura mínima diaria'),
    107: ('Temperatura', 'Temperatura seca mínima diaria'),
    108: ('Temperatura', 'Temperatura seca máxima diaria'),
    109: ('Temperatura', 'Temperatura mínima absoluta mensual'),
    110: ('Temperatura', 'Temperatura seca máxima mensual'),
    111: ('Temperatura', 'Temperatura máxima media mensual'),
    112: ('Temperatura', 'Temperatura del aire media mensual'),
    113: ('Temperatura', 'Temperatura mínima media mensual'),
    114: ('Temperatura', 'Temperatura húmeda media anual'),
    115: ('Temperatura', 'Temperatura máxima media anual'),
    116: ('Temperatura', 'Temperatura mínima máxima anual'),
    117: ('Temperatura', 'Temperatura húmeda máxima anual'),
    118: ('Temperatura', 'Temperatura húmeda mínima anual'),
    119: ('Temperatura', 'Temperatura seca mínima anual'),
    120: ('Temperatura', 'Temperatura mínima media anual'),
    121: ('Temperatura', 'Temperatura seca máxima anual'),
    122: ('Temperatura', 'Temperatura máxima mínima anual'),
    123: ('Temperatura', 'Temperatura mínima absoluta anual'),
    124: ('Temperatura', 'Temperatura máxima absoluta anual'),
    125: ('Temperatura', 'Temperatura del punto de rocío mínima diaria'),
    126: ('Transporte de sedimentos', 'Transporte medio diario se obtiene en Ktn/dia'),
    127: ('Transporte de sedimentos', 'Transporte medio diario a partir de la CM'),
    128: ('Transporte de sedimentos', 'Transporte máximo mensual'),
    129: ('Transporte de sedimentos', 'null'),
    130: ('Transporte de sedimentos', 'Transporte medio total mensual a partir de la CM'),
    131: ('Transporte de sedimentos', 'Transporte medio mensual a partir de la CM'),
    132: ('Transporte de sedimentos', 'Transporte medio máximo mensual a partir de la CM'),
    133: ('Transporte de sedimentos', 'Transporte total mensual'),
    134: ('Transporte de sedimentos', 'null'),
    135: ('Transporte de sedimentos', 'Transporte medio anual a partir de la CM'),
    136: ('Transporte de sedimentos', 'null'),
    137: ('Transporte de sedimentos', 'null'),
    138: ('Transporte de sedimentos', 'Transporte medio máximo anual a partir de la CM'),
    139: ('Transporte de sedimentos', 'Transporte medio total anual a partir de la CM'),
    140: ('Velocidad del viento', 'Velocidad vectorial 10 minutal del viento media diaria'),
    141: ('Velocidad del viento', 'Velocidad del viento vectorial media diaria'),
    142: ('Velocidad del viento', 'Velocidad del viento cada 2 min'),
    143: ('Velocidad del viento', 'Velocidad del viento cada 10 min'),
    144: ('Velocidad del viento', 'Velocidad 10 minutal del viento media horaria'),
    145: ('Velocidad del viento', 'Velocidad del viento de las 24 horas'),
    146: ('Velocidad del viento', 'Velocidad del viento 2 minutal media horaria'),
    147: ('Velocidad del viento', 'Velocidad vectorial 2 minutal del viento media diaria'),
    148: ('Velocidad del viento', 'Velocidad del viento 2 minutal media diaria'),
    149: ('Velocidad del viento', 'Velocidad 10 minutal del viento media diaria'),
    150: ('Velocidad del viento', 'Velocidad del viento máxima del día'),
    151: ('Velocidad del viento', 'Velocidad del viento vectorial media mensual'),
    152: ('Velocidad del viento', 'Velocidad vectorial 2 minutal del viento media mensual'),
    153: ('Velocidad del viento', 'Velocidad vectorial 10 minutal del viento media mensual'),
    154: ('Velocidad del viento', 'Velocidad 10 minutal del viento media mensual'),
    155: ('Velocidad del viento', 'Velocidad del viento 2 minutal media mensual'),
    156: ('Velocidad del viento', 'Velocidad del viento 2 minutal media anual'),
    157: ('Velocidad del viento', 'Velocidad 10 minutal del viento media anual'),
    158: ('Velocidad del viento', 'Velocidad del viento vectorial media anual'),
    159: ('Velocidad del viento', 'Velocidad vectorial 2 minutal del viento media anual'),
    160: ('Velocidad del viento', 'Velocidad del viento media anual'),
    161: ('Velocidad del viento', 'Velocidad vectorial 10 minutal del viento media anual'),
}

VARIABLES_IDEAM: dict[int, tuple[str, str, Frequency]] = {
    variable_id: (categoria, parametro, infer_frequency_from_name(parametro))
    for variable_id, (categoria, parametro) in _VARIABLES_IDEAM_BASE.items()
}


def resolve_variable(variable_id: int) -> tuple[str, str]:
    """
    Resuelve un ID del catálogo a ``(parameter, variable_code)`` para la descarga.

    ``parameter`` es la categoría del menú; ``variable_code`` es el nombre de la
    variable tal como lo usa el portal en el ``onclick`` del input.
    """
    try:
        categoria, parametro, _frequency = VARIABLES_IDEAM[variable_id]
    except KeyError as exc:
        raise UnknownVariableIdError(
            f"variable_id={variable_id} no existe en el catálogo."
        ) from exc
    return categoria, parametro


def resolve_frequency(variable_id: int) -> Frequency:
    """Resuelve un ID del catálogo a la frecuencia inferida para chunking."""
    try:
        _categoria, _parametro, frequency = VARIABLES_IDEAM[variable_id]
    except KeyError as exc:
        raise UnknownVariableIdError(
            f"variable_id={variable_id} no existe en el catálogo."
        ) from exc
    return frequency
