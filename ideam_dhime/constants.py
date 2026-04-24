# -*- coding: utf-8 -*-
"""Constantes de URL, timeouts y preferencias de Chrome (sin alterar valores del script original)."""

from __future__ import annotations

from typing import Final

DHIME_URL: Final[str] = "http://dhime.ideam.gov.co/atencionciudadano/"

DEFAULT_TIMEOUT: Final[int] = 25
DOWNLOAD_TIMEOUT: Final[int] = 60
SAFE_CLICK_TIMEOUT: Final[int] = 15
CATALOG_BUILDER_TIMEOUT: Final[int] = 20

CHROME_PREFS: Final[dict[str, object]] = {
    "download.prompt_for_download": False,
    "profile.default_content_setting_values.automatic_downloads": 1,
    "profile.default_content_setting_values.insecure_content": 1,
    "safebrowsing.enabled": False,
    "safebrowsing.disable_download_protection": True,
}

CHROME_ARGUMENTS: Final[tuple[str, ...]] = (
    "--start-maximized",
    "--allow-running-insecure-content",
    "--ignore-certificate-errors",
    "--disable-features=InsecureDownloadWarnings",
    "--unsafely-treat-insecure-origin-as-secure=http://dhime.ideam.gov.co",
)

# Selectores estáticos reutilizables (mismos que en scrapper.py)
XPATH_PARAMETER_MENU: Final[str] = "//div[@id='pnlEstandar']/table/tbody/tr/td[2]/span/span/span"
XPATH_DEPARTMENT_MENU: Final[str] = "//div[@id='first']/table/tbody/tr/td[2]/span/span/span"
XPATH_MUNICIPALITY_MENU: Final[str] = "//div[@id='first']/table/tbody/tr[2]/td[2]/span/span/span"
XPATH_FILTER_BUTTON: Final[str] = "//div[contains(@class, 'jimu-btn') and contains(text(), 'Filtrar')]"
XPATH_CHECKBOX_METADATA: Final[str] = "//input[contains(@id, 'checkMetaData')]"
XPATH_ADD_TO_QUERY: Final[str] = "//div[@id='first']/div[6]/div"
XPATH_DOWNLOAD_BUTTON: Final[str] = "//div[@id='second']/div/div[4]/div"

JS_REMOVE_OVERLAYS: Final[str] = """
    document.querySelectorAll('.k-loading-mask, .jimu-overlay').forEach(e => e.remove());
"""

JS_BODY_CLICK: Final[str] = "document.body.click();"
