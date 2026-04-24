# -*- coding: utf-8 -*-
"""Excepciones específicas del cliente DHIME."""


class DHIMEError(Exception):
    """Error base para fallos al interactuar con el portal DHIME."""


class StationNotFoundError(DHIMEError):
    """No se encontró el código de estación en el desplegable Kendo."""


class DownloadTimeoutError(DHIMEError):
    """Tiempo de espera agotado esperando el archivo ZIP."""


class NavigationError(DHIMEError):
    """Fallo al navegar o hacer clic en la interfaz (incl. términos de uso)."""


class UnknownVariableIdError(DHIMEError):
    """El ``variable_id`` indicado no existe en el catálogo ``VARIABLES_IDEAM``."""


class NoDataInRangeError(DHIMEError):
    """El portal indica que no hay información para el rango consultado."""
