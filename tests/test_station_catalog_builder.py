# -*- coding: utf-8 -*-
"""Tests del núcleo del catálogo maestro de estaciones."""

from __future__ import annotations

import zipfile

from openpyxl import load_workbook

from ideam_dhime.station_catalog_builder import (
    RESOURCE_OTHER_EXCEL,
    RESOURCE_PRIMARY_EXCEL,
    RESOURCE_SHAPE_DBF,
    RESOURCE_SHAPE_ZIP,
    build_station_catalog_from_rows,
    _classify_downloaded_resource,
    compile_station_rows,
    correct_station_locations,
    extract_dbf_from_shape_zip,
    find_local_station_catalog_resources,
    read_official_locations_from_dbf,
    _match_official,
    _text_quality_score,
)


def test_compile_station_rows_deduplicates_with_primary_precedence():
    other = [
        {
            "CODIGO": "001",
            "NOMBRE": "OTRA",
            "DEPARTAMENTO": "Bolivar",
            "MUNICIPIO": "Cartagena",
        }
    ]
    primary = [
        {
            "CODIGO": "001",
            "NOMBRE": "IDEAM",
            "DEPARTAMENTO": "Bolívar",
            "MUNICIPIO": "Cartagena de Indias",
        }
    ]

    rows = compile_station_rows(primary, other)

    assert len(rows) == 1
    assert rows[0]["station_name"] == "IDEAM"
    assert rows[0]["entidad"] == "IDEAM"


def test_correct_station_locations_uses_department_before_municipality():
    rows = [
        {
            "station_code": "001",
            "station_name": "TEST",
            "depto_original": "Bolivar",
            "municipio_original": "Cartagena de Indias",
            "entidad": "IDEAM",
        }
    ]
    official_locations = [("Bolívar", "Cartagena de Indias")]

    corrected, quality = correct_station_locations(rows, official_locations)

    assert quality == []
    assert corrected[0]["depto_oficial"] == "Bolívar"
    assert corrected[0]["municipio_oficial"] == "Cartagena de Indias"
    assert corrected[0]["metodo_match_depto"] == "normalized"


def test_correct_station_locations_prefers_station_code_metadata():
    rows = [
        {
            "station_code": "001",
            "station_name": "NOMBRE MALO",
            "depto_original": "Texto corrupto",
            "municipio_original": "Texto corrupto",
            "entidad": "IDEAM",
        }
    ]
    station_locations = {
        "001": {
            "station_name": "Nombre oficial",
            "department": "Bolívar",
            "municipality": "Cartagena de Indias",
        }
    }

    corrected, quality = correct_station_locations(
        rows,
        official_locations=[],
        station_locations=station_locations,
    )

    assert quality == []
    assert corrected[0]["station_name"] == "Nombre oficial"
    assert corrected[0]["depto_oficial"] == "Bolívar"
    assert corrected[0]["municipio_oficial"] == "Cartagena de Indias"
    assert corrected[0]["metodo_match_depto"] == "station_code"


def test_correct_station_locations_trusts_cne_for_automatic_telemetry_by_default():
    rows = [
        {
            "station_code": "AUTO1",
            "station_name": "AUTO",
            "depto_original": "Depto CNE",
            "municipio_original": "Municipio CNE",
            "tecnologia": "Automática con Telemetría",
            "entidad": "IDEAM",
        }
    ]

    corrected, quality = correct_station_locations(rows, official_locations=[])

    assert quality == []
    assert corrected[0]["depto_oficial"] == "Depto CNE"
    assert corrected[0]["municipio_oficial"] == "Municipio CNE"
    assert corrected[0]["metodo_match_depto"] == "cne_automatic_telemetry"


def test_correct_station_locations_checks_technology_before_station_code():
    rows = [
        {
            "station_code": "AUTO1",
            "station_name": "AUTO",
            "depto_original": "Depto CNE",
            "municipio_original": "Municipio CNE",
            "tecnologia": "Automática con Telemetría",
            "entidad": "IDEAM",
        }
    ]
    station_locations = {
        "AUTO1": {
            "station_name": "Nombre DBF",
            "department": "Depto DBF",
            "municipality": "Municipio DBF",
        }
    }

    corrected, quality = correct_station_locations(
        rows,
        official_locations=[],
        station_locations=station_locations,
    )

    assert quality == []
    assert corrected[0]["depto_oficial"] == "Depto CNE"
    assert corrected[0]["municipio_oficial"] == "Municipio CNE"
    assert corrected[0]["metodo_match_depto"] == "cne_automatic_telemetry"


def test_correct_station_locations_can_disable_automatic_telemetry_trust():
    rows = [
        {
            "station_code": "AUTO1",
            "station_name": "AUTO",
            "depto_original": "Depto CNE",
            "municipio_original": "Municipio CNE",
            "tecnologia": "Automática sin Telemetría",
            "entidad": "IDEAM",
        }
    ]

    _corrected, quality = correct_station_locations(
        rows,
        official_locations=[],
        trust_cne_for_automatic_telemetry=False,
    )

    assert len(quality) == 1


def test_correct_station_locations_trusts_cne_for_pre_1970_suspended():
    rows = [
        {
            "station_code": "OLD1",
            "station_name": "OLD",
            "depto_original": "Depto CNE",
            "municipio_original": "Municipio CNE",
            "fecha_fin_op": "31/12/1969",
            "entidad": "IDEAM",
        }
    ]

    corrected, quality = correct_station_locations(rows, official_locations=[])

    assert quality == []
    assert corrected[0]["depto_oficial"] == "Depto CNE"
    assert corrected[0]["municipio_oficial"] == "Municipio CNE"
    assert corrected[0]["metodo_match_depto"] == "cne_suspended_pre_1970"


def test_correct_station_locations_can_disable_pre_1970_suspended_trust():
    rows = [
        {
            "station_code": "OLD1",
            "station_name": "OLD",
            "depto_original": "Depto CNE",
            "municipio_original": "Municipio CNE",
            "fecha_fin_op": "31/12/1969",
            "entidad": "IDEAM",
        }
    ]

    _corrected, quality = correct_station_locations(
        rows,
        official_locations=[],
        trust_cne_for_pre_1970_suspended=False,
    )

    assert len(quality) == 1


def test_match_official_ignores_parenthetical_suffix():
    matched, method, score = _match_official("El Carmen", ["El Carmen (Choco)"])

    assert matched == "El Carmen (Choco)"
    assert method == "normalized_parenthetical"
    assert score == "1.000"


def test_extract_dbf_from_shape_zip_keeps_only_dbf_and_removes_zip(tmp_path):
    shape_zip = tmp_path / "shape.zip"
    with zipfile.ZipFile(shape_zip, "w") as zf:
        zf.writestr("CNE.dbf", "dbf")
        zf.writestr("CNE.shp", "shp")
        zf.writestr("CNE.prj", "prj")

    dbf = extract_dbf_from_shape_zip(shape_zip, tmp_path / "shape")

    assert dbf.name == "CNE.dbf"
    assert dbf.exists()
    assert not shape_zip.exists()
    assert [path.name for path in dbf.parent.iterdir()] == ["CNE.dbf"]


def test_classify_downloaded_resource_accepts_dhime_short_names(tmp_path):
    assert _classify_downloaded_resource(tmp_path / "CNE_OE.xls") == RESOURCE_OTHER_EXCEL
    assert _classify_downloaded_resource(tmp_path / "CNE_IDEAM.xls") == RESOURCE_PRIMARY_EXCEL
    assert _classify_downloaded_resource(tmp_path / "CNE_SHAPE.zip") == RESOURCE_SHAPE_ZIP
    assert _classify_downloaded_resource(tmp_path / "CNE.dbf") == RESOURCE_SHAPE_DBF


def test_find_local_station_catalog_resources_accepts_cne_folder(tmp_path):
    cne = tmp_path / "CNE"
    cne.mkdir()
    primary = cne / "CNE_IDEAM.xls"
    other = cne / "CNE_OE.xls"
    dbf = cne / "CNE.dbf"
    primary.write_text("primary", encoding="utf-8")
    other.write_text("other", encoding="utf-8")
    dbf.write_text("dbf", encoding="utf-8")

    resources = find_local_station_catalog_resources(cne)

    assert resources[RESOURCE_PRIMARY_EXCEL] == primary
    assert resources[RESOURCE_OTHER_EXCEL] == other
    assert resources[RESOURCE_SHAPE_DBF] == dbf


def test_find_local_station_catalog_resources_accepts_nested_single_dbf(tmp_path):
    cne = tmp_path / "CNE"
    shape = cne / "shape"
    shape.mkdir(parents=True)
    primary = cne / "CATALOGO_NACIONAL_DE_ESTACIONES_(EXCEL).xls"
    other = cne / "CATALOGO_NACIONAL_DE_OTRAS_ENTIDADES_(EXCEL).xls"
    dbf = shape / "CatalogoEstaciones.dbf"
    primary.write_text("primary", encoding="utf-8")
    other.write_text("other", encoding="utf-8")
    dbf.write_text("dbf", encoding="utf-8")

    resources = find_local_station_catalog_resources(cne)

    assert resources[RESOURCE_PRIMARY_EXCEL] == primary
    assert resources[RESOURCE_OTHER_EXCEL] == other
    assert resources[RESOURCE_SHAPE_DBF] == dbf


def test_find_local_station_catalog_resources_accepts_arbitrary_names(tmp_path):
    cne = tmp_path / "CNE"
    cne.mkdir()
    primary = cne / "estaciones_ideam.xls"
    other = cne / "otras_entidades.xls"
    dbf = cne / "ubicaciones_oficiales.dbf"
    primary.write_text("primary", encoding="utf-8")
    other.write_text("other", encoding="utf-8")
    dbf.write_text("dbf", encoding="utf-8")

    resources = find_local_station_catalog_resources(cne)

    assert resources[RESOURCE_PRIMARY_EXCEL] == primary
    assert resources[RESOURCE_OTHER_EXCEL] == other
    assert resources[RESOURCE_SHAPE_DBF] == dbf


def test_build_station_catalog_from_rows_writes_outputs(tmp_path):
    primary = [
        {
            "CODIGO": "001",
            "NOMBRE": "IDEAM",
            "DEPARTAMENTO": "Bolivar",
            "MUNICIPIO": "Cartagena de Indias",
        }
    ]
    other = []
    locations = [("Bolívar", "Cartagena de Indias")]
    output_xlsx = tmp_path / "guia_estaciones.xlsx"
    quality_xlsx = tmp_path / "guia_estaciones_calidad.xlsx"
    stations_py = tmp_path / "stations_generated.py"
    locations_py = tmp_path / "locations_generated.py"

    report = build_station_catalog_from_rows(
        primary,
        other,
        locations,
        output_xlsx,
        quality_report=quality_xlsx,
        stations_py=stations_py,
        locations_py=locations_py,
        generated_at="2026-04-27",
        sources={"test": "fixture"},
    )

    assert report.rows_total == 1
    assert report.quality_rows == 0
    workbook = load_workbook(output_xlsx, read_only=True)
    sheet = workbook.active
    header = [cell.value for cell in sheet[1]]
    first_row = [cell.value for cell in sheet[2]]
    row = dict(zip(header, first_row))
    assert header == ["CODIGO", "NOMBRE", "DEPARTAMENTO", "MUNICIPIO"]
    assert row["CODIGO"] == "001"
    assert row["DEPARTAMENTO"] == "Bolívar"
    assert row["MUNICIPIO"] == "Cartagena de Indias"
    assert stations_py.exists()
    assert locations_py.exists()
    assert quality_xlsx.exists()


def test_read_official_locations_prefers_dbf_description_fields(monkeypatch, tmp_path):
    class FakeDBF:
        def __init__(self, *_args, **_kwargs):
            self.field_names = ["DEPARTAMEN", "MUNICIPIO", "d_DEPARTAM", "d_MUNICIPI"]

        def __iter__(self):
            return iter(
                [
                    {
                        "DEPARTAMEN": 91.0,
                        "MUNICIPIO": 91540.0,
                        "d_DEPARTAM": "Amazonas",
                        "d_MUNICIPI": "Puerto Nariño",
                    }
                ]
            )

    import sys
    import types

    fake_module = types.SimpleNamespace(DBF=FakeDBF)
    monkeypatch.setitem(sys.modules, "dbfread", fake_module)

    locations = read_official_locations_from_dbf(tmp_path / "fake.dbf")

    assert locations == [("Amazonas", "Puerto Nariño")]


def test_text_quality_score_penalizes_mojibake_box_chars():
    assert _text_quality_score(["Choc├│", "Pluviom├étrica"]) > _text_quality_score(
        ["Chocó", "Pluviométrica"]
    )
