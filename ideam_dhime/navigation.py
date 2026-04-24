# -*- coding: utf-8 -*-
"""Clics tolerantes a fallos (equivalente a ``click_nativo_seguro`` del script original)."""

from __future__ import annotations

import logging
import time

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import ElementClickInterceptedException, StaleElementReferenceException

from ideam_dhime.constants import JS_REMOVE_OVERLAYS, SAFE_CLICK_TIMEOUT
from ideam_dhime.exceptions import NavigationError

logger = logging.getLogger("ideam_dhime")


def safe_click(
    browser: WebDriver,
    wait: WebDriverWait,
    xpath_selector: str,
    step_name: str,
    timeout: int = SAFE_CLICK_TIMEOUT,
) -> None:
    """
    Intenta clic nativo con reintentos; si falla, intenta clic vía JavaScript.

    Lógica idéntica a ``click_nativo_seguro`` en ``scrapper.py``.
    """
    logger.debug("[Intento] %s...", step_name)
    browser.execute_script(JS_REMOVE_OVERLAYS)
    end_time = time.time() + timeout
    while time.time() < end_time:
        try:
            element = wait.until(EC.presence_of_element_located((By.XPATH, xpath_selector)))
            browser.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            time.sleep(0.5)
            clic_element = wait.until(EC.element_to_be_clickable((By.XPATH, xpath_selector)))
            clic_element.click()
            logger.debug("[Éxito] %s", step_name)
            return
        except (ElementClickInterceptedException, StaleElementReferenceException):
            time.sleep(1)
        except Exception:
            time.sleep(1)

    try:
        element = browser.find_element(By.XPATH, xpath_selector)
        browser.execute_script("arguments[0].click();", element)
        logger.debug("[Éxito] %s (Plan JS)", step_name)
    except Exception as exc:
        raise NavigationError(f"Fallo definitivo al hacer clic en {step_name}.") from exc
