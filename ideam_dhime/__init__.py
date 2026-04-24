# -*- coding: utf-8 -*-
"""Cliente para descargar series desde DHIME (IDEAM)."""

from ideam_dhime.batch import DownloadResult, batch_download
from ideam_dhime.catalog import CATALOG_GENERATED_AT, VARIABLES_IDEAM, resolve_variable
from ideam_dhime.catalog_builder import generate_catalog
from ideam_dhime.exceptions import (
    DHIMEError,
    DownloadTimeoutError,
    NavigationError,
    NoDataInRangeError,
    StationNotFoundError,
    UnknownVariableIdError,
)
from ideam_dhime.requests_model import StationRequest, coerce_request
from ideam_dhime.scraper import download_dhime_data
from ideam_dhime.session import DHIMESession

__version__ = "0.2.1"

__all__ = [
    "CATALOG_GENERATED_AT",
    "DHIMEError",
    "DHIMESession",
    "DownloadResult",
    "DownloadTimeoutError",
    "NavigationError",
    "NoDataInRangeError",
    "StationNotFoundError",
    "StationRequest",
    "UnknownVariableIdError",
    "VARIABLES_IDEAM",
    "__version__",
    "batch_download",
    "coerce_request",
    "download_dhime_data",
    "generate_catalog",
    "resolve_variable",
]
