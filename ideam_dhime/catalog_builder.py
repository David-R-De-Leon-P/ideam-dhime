# -*- coding: utf-8 -*-
"""
Generación del catálogo IDEAM leyendo la tabla ``DatosBuscarVariables`` en el portal.

Misma estrategia que ``dict_create.py``: despertar Kendo, iterar categorías del menú
``Parametro_listbox`` y extraer filas de la tabla. Asigna IDs secuenciales 1..N.
"""

from __future__ import annotations

import logging
import time
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from ideam_dhime.constants import (
    CATALOG_BUILDER_TIMEOUT,
    DHIME_URL,
    JS_BODY_CLICK,
    XPATH_PARAMETER_MENU,
)
from ideam_dhime.driver import build_browser
from ideam_dhime.navigation import safe_click

logger = logging.getLogger("ideam_dhime")

_JS_CLEAR_VARIABLES_TABLE = """
    var tabla = document.querySelector("#DatosBuscarVariables tbody");
    if(tabla) tabla.innerHTML = "";
"""


def _escape_single_quotes(s: str) -> str:
    return s.replace("\\", "\\\\").replace("'", "\\'")


def _write_catalog_python(
    path: Path, catalog: dict[int, tuple[str, str]], generated_at: str
) -> None:
    total = len(catalog)
    lines = [
        "# -*- coding: utf-8 -*-",
        "# Salida de generate_catalog: copiar VARIABLES_IDEAM a ideam_dhime/catalog.py",
        "# y fijar allí CATALOG_GENERATED_AT con la misma fecha que abajo.",
        f"# Fecha de generación de este archivo: {generated_at}",
        f"# Total de entradas: {total}",
        "",
        "VARIABLES_IDEAM = {",
    ]
    for vid in sorted(catalog):
        cat, param = catalog[vid]
        lines.append(
            f"    {vid}: ('{_escape_single_quotes(cat)}', '{_escape_single_quotes(param)}'),"
        )
    lines.append("}")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_guia_txt(
    path: Path, catalog: dict[int, tuple[str, str]], generated_at: str
) -> None:
    body_lines = [
        f"Catálogo generado el {generated_at} (fecha ISO).",
        "Versión incluida en el paquete: ver CATALOG_GENERATED_AT en ideam_dhime/catalog.py.",
        "",
        "========================================================",
        " CATÁLOGO DE VARIABLES IDEAM PARA DESCARGA",
        "========================================================",
        "",
    ]
    for vid in sorted(catalog):
        cat, param = catalog[vid]
        body_lines.append(
            f"ID = {vid}; Descarga la variable '{_escape_single_quotes(cat)}' "
            f"y su parámetro '{_escape_single_quotes(param)}'"
        )
    body_lines.append("")
    path.write_text("\n".join(body_lines), encoding="utf-8")


def generate_catalog(
    output_dir: Path | None = None,
    write_python: bool = True,
    write_txt: bool = True,
    python_filename: str = "catalog_generated.py",
    txt_filename: str = "guia_variables.txt",
) -> dict[int, tuple[str, str]]:
    """
    Abre el portal, recorre cada categoría del menú de parámetros y construye
    ``{id: (categoría, nombre_variable)}``.

    Parameters
    ----------
    output_dir:
        Carpeta donde escribir archivos. Si es ``None``, no se escribe nada.
    write_python / write_txt:
        Controlan la generación de ``catalog_generated.py`` y ``guia_variables.txt``.
    python_filename / txt_filename:
        Nombres de archivo dentro de ``output_dir``.
    """
    logger.info("Iniciando generación de catálogo IDEAM (lectura de tabla)...")

    catalog: dict[int, tuple[str, str]] = {}
    next_id = 1

    with TemporaryDirectory(prefix="ideam_dhime_catalog_") as tmp:
        tmp_path = Path(tmp)
        with build_browser(tmp_path, CATALOG_BUILDER_TIMEOUT) as (browser, wait):
            browser.get(DHIME_URL)

            logger.debug("Pasando pantalla de bienvenida...")
            wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".checkbox"))).click()
            browser.find_element(By.CSS_SELECTOR, ".enable-btn").click()
            time.sleep(3)

            logger.debug("Despertando menú Kendo (parámetros)...")
            safe_click(browser, wait, XPATH_PARAMETER_MENU, "Despertar menú Parámetros")
            time.sleep(2)

            elementos_menu = browser.find_elements(
                By.XPATH, "//ul[@id='Parametro_listbox']/li"
            )
            total_categorias = len(elementos_menu)
            logger.info("Menú despertado: %s categorías en la lista.", total_categorias)

            browser.execute_script(JS_BODY_CLICK)
            time.sleep(1)

            for i in range(1, total_categorias + 1):
                safe_click(
                    browser,
                    wait,
                    XPATH_PARAMETER_MENU,
                    f"Abrir menú parámetros (iteración {i})",
                )
                time.sleep(1)

                item = browser.find_element(
                    By.XPATH, f"//ul[@id='Parametro_listbox']/li[{i}]"
                )
                nombre_categoria = item.text.strip()

                if not nombre_categoria or "seleccione" in nombre_categoria.lower():
                    browser.execute_script(JS_BODY_CLICK)
                    continue

                logger.info("[%s/%s] Extrayendo categoría: %s", i, total_categorias, nombre_categoria)

                browser.execute_script(_JS_CLEAR_VARIABLES_TABLE)

                browser.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'}); arguments[0].click();",
                    item,
                )

                try:
                    wait.until(
                        EC.presence_of_element_located(
                            (
                                By.XPATH,
                                "//table[@id='DatosBuscarVariables']//input[@type='radio']",
                            )
                        )
                    )
                    time.sleep(1.5)
                except Exception:
                    logger.warning(
                        "ArcGIS no devolvió variables para la categoría %s.",
                        nombre_categoria,
                    )
                    browser.execute_script(JS_BODY_CLICK)
                    continue

                filas = browser.find_elements(
                    By.XPATH, "//table[@id='DatosBuscarVariables']/tbody/tr"
                )
                opciones = 0

                for fila in filas:
                    try:
                        td_input = fila.find_element(By.XPATH, "./td[1]//input")
                        codigo = td_input.get_attribute("value")

                        if not codigo or codigo == "on":
                            onclick_txt = td_input.get_attribute("onclick")
                            if onclick_txt and "'" in onclick_txt:
                                codigo = onclick_txt.split("'")[1]

                        td_nombre = fila.find_element(By.XPATH, "./td[2]")
                        nombre_var = td_nombre.get_attribute("title")
                        if not nombre_var:
                            nombre_var = td_nombre.text

                        nombre_var = nombre_var.strip().replace("\n", " ")

                        if nombre_var and codigo and codigo != "on":
                            catalog[next_id] = (nombre_categoria, nombre_var)
                            next_id += 1
                            opciones += 1
                    except Exception:
                        continue

                logger.debug("  -> %s variables extraídas de la tabla.", opciones)
                browser.execute_script(JS_BODY_CLICK)

    total_final = len(catalog)
    if total_final:
        logger.info(
            "Escaneo finalizado: %s entradas en el catálogo generado.", total_final
        )
    else:
        logger.error("El catálogo generado está vacío.")

    if output_dir is not None:
        generated_at = date.today().isoformat()
        out = Path(output_dir).absolute()
        out.mkdir(parents=True, exist_ok=True)
        if write_python:
            py_path = out / python_filename
            _write_catalog_python(py_path, catalog, generated_at)
            logger.info("Escrito: %s", py_path)
        if write_txt:
            txt_path = out / txt_filename
            _write_guia_txt(txt_path, catalog, generated_at)
            logger.info("Escrito: %s", txt_path)

    return catalog
