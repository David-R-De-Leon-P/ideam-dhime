# -*- coding: utf-8 -*-
"""Selección de estación vía Kendo UI (inyección JavaScript)."""

from __future__ import annotations

import logging
import time

from selenium.webdriver.remote.webdriver import WebDriver

from ideam_dhime.exceptions import StationNotFoundError

logger = logging.getLogger("ideam_dhime")


def _kendo_station_script(station_code: str) -> str:
    """Mismo bloque JS que en ``scrapper.py`` (f-string sobre ``station_code``)."""
    return f"""
            var ddl = $("#nombreEstacion").data("kendoDropDownList");
            if (ddl) {{
                var data = ddl.dataSource.data();
                var valField = ddl.options.dataValueField; // Kendo sabe cuál es el campo de valor real

                for (var i = 0; i < data.length; i++) {{
                    var item = data[i];
                    // Convertimos todo el objeto a texto plano de forma segura
                    var strItem = JSON.stringify(item) || String(item);

                    // Si el código está en alguna parte del objeto
                    if (strItem.indexOf('{station_code}') !== -1) {{
                        var valToSet = valField ? item[valField] : item;
                        ddl.value(valToSet);
                        ddl.trigger("change"); // Informa al portal que hubo un cambio
                        return true;
                    }}
                }}
            }}
            return false;
        """


def select_station_kendo(browser: WebDriver, station_code: str, municipality: str) -> None:
    """
    Selecciona la estación en el desplegable Kendo por coincidencia de código.

    Parameters
    ----------
    municipality:
        Solo para el mensaje de error si no se encuentra la estación.
    """
    logger.debug("[Intento] Seleccionando la estación %s internamente...", station_code)
    script = _kendo_station_script(station_code)
    exito_kendo = browser.execute_script(script)
    if not exito_kendo:
        raise StationNotFoundError(
            f"No se encontró la estación {station_code} en la lista de {municipality}."
        )
    logger.info("Estación %s seleccionada en el sistema.", station_code)
    time.sleep(2.0)
