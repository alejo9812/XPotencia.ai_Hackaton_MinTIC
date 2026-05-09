from __future__ import annotations

import pandas as pd

from pae_risk_tracker.retrieval.search_index import materialize_search_index
from pae_risk_tracker.risk.opacity_criteria import build_opacity_criteria_report
from pae_risk_tracker.storage.duckdb_store import DuckDBStore


def test_opacity_criteria_report_verifies_repository_and_study_coverage(tmp_path):
    store = DuckDBStore(tmp_path / "tracker.duckdb")
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    contracts = pd.DataFrame(
        [
            {
                "contract_id": "C-1",
                "process_id": "P-1",
                "entity_name": "Alcaldia de Ibague",
                "supplier_name": "Proveedor Uno",
                "department": "Tolima",
                "municipality": "Ibague",
                "modality": "Contratacion Directa",
                "status": "Adjudicado",
                "amount": 150_000_000,
                "date": "2025-01-05",
                "risk_score": 75,
                "risk_level": "alto",
                "object_text": "PAE alimentacion escolar",
                "justification": "",
                "url_process": "https://example.com/process/1",
            },
            {
                "contract_id": "C-2",
                "process_id": "P-2",
                "entity_name": "Alcaldia de Cali",
                "supplier_name": "Proveedor Dos",
                "department": "Valle del Cauca",
                "municipality": "Cali",
                "modality": "Licitacion Publica",
                "status": "Adjudicado",
                "amount": 45_000_000,
                "date": "2025-02-11",
                "risk_score": 18,
                "risk_level": "bajo",
                "object_text": "Compra de alimentos con especificaciones claras",
                "justification": "Justificacion tecnica visible",
                "url_process": "https://example.com/process/2",
            },
        ]
    )
    processes = pd.DataFrame(
        [
            {
                "id_del_proceso": "P-1",
                "id_del_portafolio": "PF-1",
                "entidad": "Alcaldia de Ibague",
                "modalidad_de_contratacion": "Licitacion publica",
                "fecha_de_publicacion_del": "2025-01-01T00:00:00.000",
                "proveedores_unicos_con": 3,
                "respuestas_al_procedimiento": 3,
                "precio_base": 200_000_000,
                "valor_total_adjudicacion": 180_000_000,
                "nombre_del_procedimiento": "Programa de Alimentacion Escolar PAE",
                "urlproceso": {"url": "https://example.com/process/1"},
            },
            {
                "id_del_proceso": "P-2",
                "id_del_portafolio": "PF-2",
                "entidad": "Alcaldia de Cali",
                "modalidad_de_contratacion": "Licitacion publica",
                "fecha_de_publicacion_del": "2025-02-01T00:00:00.000",
                "proveedores_unicos_con": 8,
                "respuestas_al_procedimiento": 6,
                "precio_base": 60_000_000,
                "valor_total_adjudicacion": 55_000_000,
                "nombre_del_procedimiento": "Compra de alimentos para PAE",
                "urlproceso": {"url": "https://example.com/process/2"},
            },
        ]
    )
    additions = pd.DataFrame(
        [
            {
                "addition_id": "A-1",
                "contract_id": "C-1",
                "addition_type": "ADICION",
                "addition_description": "Adicion de valor",
                "addition_date": "2025-02-01",
            }
        ]
    )
    paco = pd.DataFrame(
        [
            {
                "source_key": "disciplinary",
                "source_name": "PACO disciplinary",
                "event_type": "disciplinary_sanction",
                "reference": "Proveedor Uno",
                "status": "vigente",
                "description": "Sancion disciplinaria",
                "subject_name": "Proveedor Uno",
                "entity_name": "Alcaldia de Ibague",
                "search_text": "PROVEEDOR UNO ALCALDIA DE IBAGUE",
                "source_row_hash": "hash-1",
            }
        ]
    )

    store.write_frame("pae_contracts_scored", contracts, replace=True)
    store.write_frame("pae_processes", processes, replace=True)
    store.write_frame("pae_additions", additions, replace=True)
    store.write_frame("paco_events", paco, replace=True)
    materialize_search_index(store, processed_dir)

    report = build_opacity_criteria_report(store, processed_dir=processed_dir)
    payload = report.to_dict()

    assert report.criteria_count == 7
    assert report.covered_count == 7
    assert report.overall_status == "verified"
    assert report.coverage_ratio == 1.0
    assert payload["gaps"] == []
    assert payload["data_snapshot"]["duckdb_tables"]["pae_search_index"] == 5
    assert payload["data_snapshot"]["active_validation_source_count"] >= 4
    assert any(reference["origin"] == "study" for reference in payload["study_references"])
    assert any(reference["origin"] == "local_repository" for reference in payload["repository_references"])
    planning = next(family for family in payload["families"] if family["id"] == "planning")
    assert planning["status"] == "covered"
    assert "RF-07" in planning["flag_codes"]
    assert planning["study_references"]
    assert planning["local_references"]

