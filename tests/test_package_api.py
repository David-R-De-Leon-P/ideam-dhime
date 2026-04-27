# -*- coding: utf-8 -*-
"""Comprueba que la API pública expuesta en __init__ sea estable."""

from __future__ import annotations

import re
from pathlib import Path

import ideam_dhime


def test_public_symbols_present():
    expected = {
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
    }
    assert expected.issubset(set(ideam_dhime.__all__))
    for name in expected:
        assert hasattr(ideam_dhime, name), name


def test_version_is_semver():
    parts = ideam_dhime.__version__.split(".")
    assert len(parts) == 3
    assert all(p.isdigit() for p in parts)


def test_version_matches_pyproject():
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    text = pyproject.read_text(encoding="utf-8")
    match = re.search(r'^version\s*=\s*"([^"]+)"', text, flags=re.MULTILINE)
    assert match is not None
    assert ideam_dhime.__version__ == match.group(1)
