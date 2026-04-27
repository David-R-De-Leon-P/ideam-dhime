# -*- coding: utf-8 -*-
"""Sesión reutilizable de DHIME para descargas por lote."""

from __future__ import annotations

import logging
import os
import shutil
import time
from datetime import date
from pathlib import Path

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC

from ideam_dhime.catalog import resolve_frequency, resolve_variable
from ideam_dhime.chunking import split_for_frequency
from ideam_dhime.constants import (
    DEFAULT_TIMEOUT,
    DHIME_URL,
    JS_BODY_CLICK,
    XPATH_ADD_TO_QUERY,
    XPATH_CHECKBOX_METADATA,
    XPATH_DEPARTMENT_MENU,
    XPATH_DOWNLOAD_BUTTON,
    XPATH_FILTER_BUTTON,
    XPATH_MUNICIPALITY_MENU,
    XPATH_PARAMETER_MENU,
)
from ideam_dhime.download import extract_and_rename, wait_for_zip
from ideam_dhime.driver import build_browser, set_browser_download_dir
from ideam_dhime.exceptions import DownloadTimeoutError, NavigationError, NoDataInRangeError
from ideam_dhime.frequencies import Frequency
from ideam_dhime.navigation import safe_click
from ideam_dhime.requests_model import StationRequest
from ideam_dhime.station import select_station_kendo

logger = logging.getLogger("ideam_dhime")

CLEANUP_FAILED_PREFIX = "cleanup_failed_dirs"


def cleanup_pending_file_for(download_path: str | Path, pid: int | None = None) -> Path:
    """Archivo de pendientes aislado por proceso para evitar escrituras compartidas."""
    process_id = os.getpid() if pid is None else pid
    return Path(download_path).absolute() / f"{CLEANUP_FAILED_PREFIX}_{process_id}.txt"


def sweep_failed_cleanup_dirs(download_path: str | Path) -> set[Path]:
    """
    Barre todos los archivos de pendientes de una carpeta de descarga.

    Pensado para ejecutarse desde el proceso padre al terminar workers paralelos.
    Devuelve las carpetas que siguen sin poder eliminarse.
    """
    base = Path(download_path).absolute()
    pending_files = sorted(base.glob(f"{CLEANUP_FAILED_PREFIX}*.txt"))
    pending_dirs: set[Path] = set()

    for pending_file in pending_files:
        try:
            for line in pending_file.read_text(encoding="utf-8").splitlines():
                value = line.strip()
                if value:
                    pending_dirs.add(Path(value))
        except Exception:
            logger.warning("No se pudo leer archivo de pendientes: %s", pending_file, exc_info=True)

    still_pending = {path for path in pending_dirs if not DHIMESession._cleanup_temp_dir(path)}

    for pending_file in pending_files:
        try:
            pending_file.unlink()
        except FileNotFoundError:
            pass
        except Exception:
            logger.warning("No se pudo borrar archivo de pendientes: %s", pending_file, exc_info=True)

    if still_pending:
        unresolved_file = base / f"{CLEANUP_FAILED_PREFIX}_pending.txt"
        try:
            unresolved_file.write_text(
                "\n".join(str(path) for path in sorted(still_pending, key=str)) + "\n",
                encoding="utf-8",
            )
        except Exception:
            logger.warning(
                "No se pudo escribir archivo central de pendientes: %s",
                unresolved_file,
                exc_info=True,
            )
    return still_pending


def _default_date_fin() -> str:
    today = date.today()
    return f"{today.day:02d}/{today.month:02d}/{today.year:04d}"


class DHIMESession:
    """Context manager para reutilizar un único navegador en múltiples descargas."""

    def __init__(self, download_path: str | Path, time_wait: int = DEFAULT_TIMEOUT):
        self.download_path = Path(download_path).absolute()
        self.time_wait = time_wait
        self._ctx = None
        self.browser = None
        self.wait = None
        self._cleanup_failed_file = cleanup_pending_file_for(self.download_path)
        self._cleanup_failed_dirs: set[Path] = set()

    def __enter__(self) -> "DHIMESession":
        self.download_path.mkdir(parents=True, exist_ok=True)
        self._ctx = build_browser(self.download_path, self.time_wait)
        self.browser, self.wait = self._ctx.__enter__()
        self._open_portal_once()
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            self._retry_failed_cleanup_dirs()
            if self._ctx is not None:
                return self._ctx.__exit__(exc_type, exc, tb)
        finally:
            self._ctx = None
            self.browser = None
            self.wait = None
            self._cleanup_failed_dirs.clear()
        return False

    def _open_portal_once(self) -> None:
        assert self.browser is not None and self.wait is not None
        self.browser.get(DHIME_URL)
        checkbox = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".checkbox")))
        checkbox.click()
        enable_btn = self.browser.find_element(By.CSS_SELECTOR, ".enable-btn")
        self.browser.execute_script("arguments[0].click();", enable_btn)

    def _hard_reset_portal(self) -> None:
        """Recarga completa del portal para recuperar estado tras errores de UI."""
        logger.warning("Aplicando hard reset del portal DHIME.")
        self._open_portal_once()
        self._switch_tab("Consultar")

    def _switch_tab(self, tab_text: str) -> None:
        assert self.browser is not None and self.wait is not None
        xpath = (
            "//li[normalize-space(.)='{}']|//a[normalize-space(.)='{}']|"
            "//span[normalize-space(.)='{}']"
        ).format(tab_text, tab_text, tab_text)
        safe_click(self.browser, self.wait, xpath, f"Ir a pestaña {tab_text}")

    def _click_limpiar(self) -> None:
        assert self.browser is not None and self.wait is not None
        xpath = (
            "//div[contains(@class,'jimu-btn') and contains(normalize-space(.), 'Limpiar')]"
            "|//button[contains(normalize-space(.), 'Limpiar')]"
        )
        safe_click(self.browser, self.wait, xpath, "Limpiar formulario")

    def _aplicar_consulta(
        self,
        req: StationRequest,
        date_ini: str,
        date_fin: str,
        parameter: str,
        variable_code: str,
    ) -> tuple[str, str]:
        assert self.browser is not None and self.wait is not None
        self._dismiss_open_dijit_dialogs()

        date_input_ini = self.wait.until(EC.presence_of_element_located((By.ID, "datepicker")))
        date_input_ini.click()
        date_input_ini.clear()
        date_input_ini.send_keys(date_ini, Keys.ENTER)

        date_input_fin = self.browser.find_element(By.ID, "datepicker1")
        date_input_fin.click()
        date_input_fin.clear()
        date_input_fin.send_keys(date_fin, Keys.ENTER)

        safe_click(self.browser, self.wait, XPATH_PARAMETER_MENU, "Desplegar menú de Parámetros")
        safe_click(
            self.browser,
            self.wait,
            f"//li[normalize-space(text())='{parameter}']",
            f"Seleccionar parámetro ({parameter})",
        )
        time.sleep(1.0)

        safe_click(
            self.browser,
            self.wait,
            f"//input[contains(@onclick,'{variable_code}')]",
            f"Seleccionar variable ({variable_code})",
        )
        time.sleep(1.0)

        safe_click(self.browser, self.wait, XPATH_DEPARTMENT_MENU, "Desplegar menú de Departamentos")
        safe_click(
            self.browser,
            self.wait,
            f"//ul[@id='deptos2_listbox']//li[normalize-space(text())='{req.department}']",
            f"Seleccionar departamento ({req.department})",
        )
        time.sleep(1.5)

        safe_click(self.browser, self.wait, XPATH_MUNICIPALITY_MENU, "Desplegar menú de Municipios")
        safe_click(
            self.browser,
            self.wait,
            f"//ul[@id='municipio2_listbox']//li[normalize-space(text())='{req.municipality}']",
            f"Seleccionar municipio ({req.municipality})",
        )

        self.browser.execute_script(JS_BODY_CLICK)
        time.sleep(5.0)

        select_station_kendo(self.browser, req.station_code, req.municipality)

        safe_click(self.browser, self.wait, XPATH_FILTER_BUTTON, "Clic en FILTRAR")
        time.sleep(5.0)

        # Si el portal abrió un dialog de "no hay información" tras el FILTRAR,
        # cerrarlo antes de revisar la tabla para no dejar el overlay abierto.
        no_data_after_filter = self._close_no_data_dialog_if_present()
        if no_data_after_filter:
            raise NoDataInRangeError(
                f"SIN_DATOS_EN_RANGO: estación {req.station_code} "
                f"{date_ini}->{date_fin} (dialog sin datos tras FILTRAR)"
            )

        rango_real = self._leer_rango_real_en_tabla(req.station_code)
        if rango_real is None:
            # Último intento: cerrar cualquier dialog que pudiera haberse abierto tarde
            self._dismiss_open_dijit_dialogs()
            raise NoDataInRangeError(
                f"SIN_DATOS_EN_RANGO: estación {req.station_code} "
                f"{date_ini}->{date_fin} (sin fila en tabla de resultados)"
            )

        safe_click(self.browser, self.wait, XPATH_CHECKBOX_METADATA, "Marcar checkbox estación")
        safe_click(self.browser, self.wait, XPATH_ADD_TO_QUERY, "Agregar a consulta")
        time.sleep(3.0)
        return rango_real

    def _ir_a_descargar_y_descargar(
        self,
        station_code: str,
        variable_code: str,
        date_ini: str,
        date_fin: str,
    ) -> Path:
        assert self.browser is not None and self.wait is not None
        clean_ini = date_ini.replace("/", "")
        clean_fin = date_fin.replace("/", "")
        stem = f"{station_code}-{variable_code}-{clean_ini}-{clean_fin}".replace(" ", "_")
        temp_dir = self.download_path / stem
        temp_dir.mkdir(parents=True, exist_ok=True)

        self._switch_tab("Descargar")
        safe_click(self.browser, self.wait, XPATH_DOWNLOAD_BUTTON, "Botón descargar")

        try:
            set_browser_download_dir(self.browser, temp_dir)
            accept_btn = self.wait.until(
                EC.element_to_be_clickable((By.ID, "dijit_form_Button_2_label"))
            )
            self.browser.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});", accept_btn
            )
            time.sleep(0.5)
            accept_btn.click()
        except Exception as exc:
            raise NavigationError("Fallo en ventana de términos.") from exc

        try:
            zip_path = wait_for_zip(
                temp_dir,
                no_data_checker=lambda: self._detect_no_data_message(
                    station_code, date_ini, date_fin
                ),
            )
            csv_in_temp = extract_and_rename(
                zip_path,
                temp_dir,
                station_code,
                variable_code,
                date_ini,
                date_fin,
            )
            final_path = self.download_path / csv_in_temp.name
            if final_path.exists():
                final_path.unlink()
            csv_in_temp.replace(final_path)
            time.sleep(1.0)
            return final_path
        except (NoDataInRangeError, DownloadTimeoutError):
            # Si hay popup de "sin datos" o timeout de descarga, recargamos desde cero
            # para evitar que la UI quede bloqueada y permitir continuar el lote.
            try:
                self._hard_reset_portal()
            except Exception:
                logger.warning("Falló hard reset tras no-data/timeout.", exc_info=True)
            raise
        finally:
            try:
                set_browser_download_dir(self.browser, self.download_path)
            except Exception:
                logger.debug("No fue posible restaurar download dir base.", exc_info=True)
            cleaned = self._cleanup_temp_dir(temp_dir)
            if not cleaned:
                self._register_failed_cleanup_dir(temp_dir)

    @staticmethod
    def _cleanup_temp_dir(temp_dir: Path, retries: int = 5, delay_s: float = 0.5) -> bool:
        """
        Limpieza defensiva de la carpeta temporal por descarga.

        En Windows/Dropbox un archivo puede quedar bloqueado unos segundos
        después de mover/extraer, por eso reintentamos antes de advertir.
        """
        if not temp_dir.exists():
            return True

        last_exc: Exception | None = None
        for _ in range(retries):
            try:
                shutil.rmtree(temp_dir)
                return True
            except Exception as exc:
                last_exc = exc
                time.sleep(delay_s)

        logger.warning(
            "No se pudo eliminar carpeta temporal: %s",
            temp_dir,
            exc_info=last_exc,
        )
        return False

    def _register_failed_cleanup_dir(self, temp_dir: Path) -> None:
        if temp_dir in self._cleanup_failed_dirs:
            return
        self._cleanup_failed_dirs.add(temp_dir)
        try:
            self._cleanup_failed_file.parent.mkdir(parents=True, exist_ok=True)
            with self._cleanup_failed_file.open("a", encoding="utf-8") as f:
                f.write(f"{temp_dir}\n")
        except Exception:
            logger.warning(
                "No se pudo registrar carpeta pendiente de limpieza: %s",
                temp_dir,
                exc_info=True,
            )

    def _retry_failed_cleanup_dirs(self) -> None:
        pending: set[Path] = set(self._cleanup_failed_dirs)
        if self._cleanup_failed_file.exists():
            try:
                for line in self._cleanup_failed_file.read_text(encoding="utf-8").splitlines():
                    value = line.strip()
                    if value:
                        pending.add(Path(value))
            except Exception:
                logger.warning(
                    "No se pudo leer archivo de pendientes: %s",
                    self._cleanup_failed_file,
                    exc_info=True,
                )

        if not pending:
            if self._cleanup_failed_file.exists():
                try:
                    self._cleanup_failed_file.unlink()
                except Exception:
                    logger.warning(
                        "No se pudo borrar archivo de pendientes vacío: %s",
                        self._cleanup_failed_file,
                        exc_info=True,
                    )
            return

        still_pending: set[Path] = set()
        for path in pending:
            if not self._cleanup_temp_dir(path):
                still_pending.add(path)

        self._cleanup_failed_dirs = still_pending

        if not still_pending:
            if self._cleanup_failed_file.exists():
                try:
                    self._cleanup_failed_file.unlink()
                except Exception:
                    logger.warning(
                        "No se pudo eliminar archivo de pendientes: %s",
                        self._cleanup_failed_file,
                        exc_info=True,
                    )
            return

        try:
            with self._cleanup_failed_file.open("w", encoding="utf-8") as f:
                for path in sorted(still_pending, key=str):
                    f.write(f"{path}\n")
        except Exception:
            logger.warning(
                "No se pudo reescribir archivo de pendientes: %s",
                self._cleanup_failed_file,
                exc_info=True,
            )

    def _dismiss_open_dijit_dialogs(self) -> None:
        """
        Cierra cualquier dijitDialog visible antes de interactuar con el formulario.

        Cubre dos tipos:
        - Dialogs de "no hay información" (detectados por texto).
        - Dialogs de confirmación de descarga (detectados por underlay visible).

        Intenta primero clic en «Aceptar»; si falla, elimina el underlay vía JS.
        """
        assert self.browser is not None

        # 1) Cerrar dialogs de "no hay información" explícitamente por texto
        self._close_no_data_dialog_if_present()

        # 2) Buscar cualquier underlay dijit todavía visible
        underlay_xpath = (
            "//div[contains(@class,'dijitDialogUnderlay')"
            " and not(contains(@style,'display: none'))"
            " and not(contains(@style,'display:none'))]"
        )
        underlays = self.browser.find_elements(By.XPATH, underlay_xpath)
        if not underlays:
            return

        logger.debug("Detectado dijitDialog abierto; intentando cerrarlo.")
        accept_xpath = (
            "//div[contains(@class,'dijitDialog')"
            " and not(contains(@style,'display: none'))"
            " and not(contains(@style,'display:none'))]"
            "//*[self::span or self::button][normalize-space()='Aceptar']"
            "|//div[contains(@class,'dijitDialog')"
            " and not(contains(@style,'display: none'))"
            " and not(contains(@style,'display:none'))]"
            "//*[contains(@class,'dijitDialogCloseIcon')]"
        )
        btns = self.browser.find_elements(By.XPATH, accept_xpath)
        for btn in btns:
            try:
                self.browser.execute_script("arguments[0].click();", btn)
                time.sleep(0.5)
                logger.debug("dijitDialog cerrado vía clic en botón.")
                return
            except Exception:
                pass

        # Último recurso: eliminar underlay directamente del DOM
        self.browser.execute_script(
            "document.querySelectorAll('.dijitDialogUnderlay').forEach(e => e.remove());"
        )
        logger.debug("dijitDialog underlay eliminado vía JavaScript.")

    def _limpiar_y_volver_a_consultar(self) -> None:
        self._dismiss_open_dijit_dialogs()
        self._switch_tab("Consultar")
        self._click_limpiar()
        time.sleep(1.0)
        self._switch_tab("Consultar")

    def _close_no_data_dialog_if_present(self) -> str | None:
        """
        Si aparece el popup "no hay información para el rango seleccionado",
        pulsa "Aceptar" para liberar la UI y devuelve el mensaje detectado.
        """
        assert self.browser is not None
        markers = (
            "no hay información para el rango seleccionado",
            "no hay informacion para el rango seleccionado",
            "no se encontró información",
            "no se encontro informacion",
            "sin información",
            "sin informacion",
        )

        dialog_xpath = (
            "//div[contains(@class,'dijitDialog') and not(contains(@style,'display: none'))]"
        )
        dialogs = self.browser.find_elements(By.XPATH, dialog_xpath)
        for dialog in dialogs:
            text = (dialog.text or "").strip()
            lower = text.lower()
            marker = next((m for m in markers if m in lower), None)
            if marker is None:
                continue

            accept_candidates = dialog.find_elements(
                By.XPATH,
                ".//span[normalize-space()='Aceptar']"
                "|.//button[normalize-space()='Aceptar']"
                "|.//input[@value='Aceptar']"
                "|.//*[contains(@id,'form_Button_0_label')]",
            )
            if accept_candidates:
                btn = accept_candidates[0]
                self.browser.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", btn
                )
                time.sleep(0.1)
                self.browser.execute_script("arguments[0].click();", btn)
            return marker
        return None

    def _detect_no_data_message(self, station_code: str, date_ini: str, date_fin: str) -> str | None:
        """Detecta respuestas del portal indicando ausencia de datos en el rango."""
        assert self.browser is not None
        marker_dialog = self._close_no_data_dialog_if_present()
        if marker_dialog is not None:
            return (
                f"SIN_DATOS_EN_RANGO: estación {station_code} "
                f"{date_ini}->{date_fin} ({marker_dialog})"
            )

        text = self.browser.page_source.lower()
        markers = (
            "no hay información para el rango seleccionado",
            "no hay informacion para el rango seleccionado",
            "no se encontró información",
            "no se encontro informacion",
            "sin información",
            "sin informacion",
        )
        for marker in markers:
            if marker in text:
                return (
                    f"SIN_DATOS_EN_RANGO: estación {station_code} "
                    f"{date_ini}->{date_fin} ({marker})"
                )
        return None

    @staticmethod
    def _parse_date_any(text: str) -> date:
        value = (text or "").strip()
        if "-" in value:
            yyyy, mm, dd = value.split("-")
            return date(int(yyyy), int(mm), int(dd))
        dd, mm, yyyy = value.split("/")
        return date(int(yyyy), int(mm), int(dd))

    @staticmethod
    def _fmt_ddmmyyyy(value: date) -> str:
        return f"{value.day:02d}/{value.month:02d}/{value.year:04d}"

    def _leer_rango_real_en_tabla(self, station_code: str) -> tuple[str, str] | None:
        """
        Lee FechaIni/FechaFin reales de la fila de la estación en la tabla de resultados.
        Devuelve (date_ini, date_fin) en formato dd/mm/yyyy.
        """
        assert self.browser is not None
        rows = self.browser.find_elements(By.XPATH, "//table[contains(@id,'DatosBuscarMeta')]/tbody/tr")
        station_code_clean = station_code.strip()
        for row in rows:
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) < 5:
                continue
            code_text = (cells[1].text or "").strip()
            if code_text != station_code_clean:
                continue
            ini_raw = (cells[3].text or "").strip()
            fin_raw = (cells[4].text or "").strip()
            if not ini_raw or not fin_raw:
                continue
            ini = self._parse_date_any(ini_raw)
            fin = self._parse_date_any(fin_raw)
            return self._fmt_ddmmyyyy(ini), self._fmt_ddmmyyyy(fin)
        return None

    def download_one(
        self,
        req: StationRequest,
        *,
        parameter: str | None = None,
        variable_code: str | None = None,
        max_years: int | None = None,
        min_date: str | None = None,
        max_days: int | None = None,
    ) -> list[Path]:
        """Descarga una estación, con chunking automático por frecuencia."""
        frequency = Frequency.DAILY
        if parameter is None or variable_code is None:
            parameter, variable_code = resolve_variable(req.variable_id)
            frequency = resolve_frequency(req.variable_id)
        logger.info("Descargando estación %s (%s)", req.station_code, variable_code)
        requested_ini = req.date_ini or "01/01/1900"
        effective_min = req.min_date or min_date
        if effective_min:
            requested_ini = self._fmt_ddmmyyyy(
                max(self._parse_date_any(requested_ini), self._parse_date_any(effective_min))
            )
        requested_fin = req.date_fin or _default_date_fin()

        # Preflight con rango solicitado completo para obtener FechaIni/FechaFin reales
        # del portal y ajustar la descarga al solape real disponible.
        self._switch_tab("Consultar")
        real_ini, real_fin = self._aplicar_consulta(
            req,
            requested_ini,
            requested_fin,
            parameter,
            variable_code,
        )
        self._limpiar_y_volver_a_consultar()

        req_ini_dt = self._parse_date_any(requested_ini)
        req_fin_dt = self._parse_date_any(requested_fin)
        real_ini_dt = self._parse_date_any(real_ini)
        real_fin_dt = self._parse_date_any(real_fin)
        eff_ini_dt = max(req_ini_dt, real_ini_dt)
        eff_fin_dt = min(req_fin_dt, real_fin_dt)
        if eff_ini_dt > eff_fin_dt:
            raise NoDataInRangeError(
                f"SIN_SOLAPE: estación {req.station_code} "
                f"solicitado={requested_ini}->{requested_fin} "
                f"real={real_ini}->{real_fin}"
            )
        eff_ini = self._fmt_ddmmyyyy(eff_ini_dt)
        eff_fin = self._fmt_ddmmyyyy(eff_fin_dt)

        outputs: list[Path] = []
        effective_max_years = max_years if max_years is not None else req.max_years
        effective_max_days = max_days if max_days is not None else req.max_days
        for win_ini, win_fin in split_for_frequency(
            eff_ini,
            eff_fin,
            frequency,
            max_years=effective_max_years,
            max_days=effective_max_days,
        ):
            self._switch_tab("Consultar")
            self._aplicar_consulta(req, win_ini, win_fin, parameter, variable_code)
            output = self._ir_a_descargar_y_descargar(
                req.station_code,
                variable_code,
                win_ini,
                win_fin,
            )
            outputs.append(output)
            self._limpiar_y_volver_a_consultar()

        return outputs
