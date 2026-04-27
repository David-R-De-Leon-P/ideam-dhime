# -*- coding: utf-8 -*-
"""Cliente para descargar series desde DHIME (IDEAM)."""

from ideam_dhime.batch import DownloadResult, batch_download
from ideam_dhime.catalog import (
    CATALOG_GENERATED_AT,
    VARIABLES_IDEAM,
    resolve_frequency,
    resolve_variable,
)
from ideam_dhime.catalog_builder import generate_catalog
from ideam_dhime.csv_merge import merge_station_csvs
from ideam_dhime.exceptions import (
    DHIMEError,
    DownloadTimeoutError,
    LocationNotFoundError,
    NavigationError,
    NoDataInRangeError,
    StationNotFoundError,
    UnknownVariableIdError,
)
from ideam_dhime.requests_model import StationRequest, coerce_request
from ideam_dhime.scraper import download_dhime_data
from ideam_dhime.session import DHIMESession
from ideam_dhime.frequencies import FREQUENCY_LIMITS, Frequency, infer_frequency_from_name
from ideam_dhime.station_catalog import StationMetadata, resolve_station_metadata, station_catalog_path
from ideam_dhime.station_catalog_builder import regenerate_station_catalog

__version__ = "0.3.0"

__all__ = [
    "CATALOG_GENERATED_AT",
    "DHIMEError",
    "DHIMESession",
    "DownloadResult",
    "DownloadTimeoutError",
    "FREQUENCY_LIMITS",
    "Frequency",
    "LocationNotFoundError",
    "NavigationError",
    "NoDataInRangeError",
    "StationNotFoundError",
    "StationMetadata",
    "StationRequest",
    "UnknownVariableIdError",
    "VARIABLES_IDEAM",
    "__version__",
    "batch_download",
    "coerce_request",
    "download_dhime_data",
    "generate_catalog",
    "infer_frequency_from_name",
    "merge_station_csvs",
    "regenerate_station_catalog",
    "resolve_frequency",
    "resolve_station_metadata",
    "resolve_variable",
    "station_catalog_path",
]
