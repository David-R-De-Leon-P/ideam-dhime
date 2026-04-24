# -*- coding: utf-8 -*-
"""Construcción y ciclo de vida del WebDriver Chrome."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait

from ideam_dhime.constants import CHROME_ARGUMENTS, CHROME_PREFS, DEFAULT_TIMEOUT


@contextmanager
def build_browser(
    download_dir: Path,
    timeout: int = DEFAULT_TIMEOUT,
) -> Iterator[tuple[WebDriver, WebDriverWait]]:
    """
    Crea Chrome con las mismas opciones anti-bloqueo que el script original.

    Parameters
    ----------
    download_dir:
        Carpeta absoluta donde Chrome guardará las descargas.
    timeout:
        Segundos para WebDriverWait (equivale a ``time_wait`` del original).
    """
    chrome_options = Options()
    prefs = {**CHROME_PREFS, "download.default_directory": str(download_dir)}
    chrome_options.add_experimental_option("prefs", prefs)
    for arg in CHROME_ARGUMENTS:
        chrome_options.add_argument(arg)

    service = Service()
    browser = webdriver.Chrome(service=service, options=chrome_options)
    wait = WebDriverWait(browser, timeout)
    try:
        yield browser, wait
    finally:
        try:
            browser.quit()
        except Exception:
            pass
        try:
            browser.service.stop()
        except Exception:
            pass


def set_browser_download_dir(browser: WebDriver, download_dir: Path) -> None:
    """Cambia en caliente la carpeta de descargas de Chrome (CDP)."""
    browser.execute_cdp_cmd(
        "Browser.setDownloadBehavior",
        {"behavior": "allow", "downloadPath": str(download_dir)},
    )
