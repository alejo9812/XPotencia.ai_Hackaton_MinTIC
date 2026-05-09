from __future__ import annotations

import pandas as pd

from pae_risk_tracker.agent.orchestrator import run_agent_query
from pae_risk_tracker.retrieval.search_index import materialize_search_index
from pae_risk_tracker.storage.duckdb_store import DuckDBStore


def test_agent_routes_criteria_queries_to_knowledge_search(tmp_path):
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
            }
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
            }
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
    store.write_frame("pae_contracts_scored", contracts, replace=True)
    store.write_frame("pae_processes", processes, replace=True)
    store.write_frame("pae_additions", additions, replace=True)
    store.write_frame(
        "validation_runs",
        pd.DataFrame(
            [
                {
                    "run_id": "validation-20260509-000003-000001",
                    "created_at": "2026-05-09T00:00:03+00:00",
                    "source_table": "pae_search_index",
                    "candidate_count": 1,
                    "paco_count": 1,
                    "secop_count": 1,
                    "external_count": 1,
                    "hit_count": 1,
                    "clear_count": 0,
                    "blocked_count": 0,
                    "error_count": 0,
                    "observation_count": 3,
                    "registry_source_count": 4,
                    "snapshot_count": 3,
                    "overall_status": "review_needed",
                    "report_path": str(tmp_path / "validation.json"),
                    "snapshot_dir": str(tmp_path / "snapshots"),
                }
            ]
        ),
        replace=True,
    )
    materialize_search_index(store, processed_dir)

    result = run_agent_query(
        store,
        "quiero revisar el criterio de opacidad con repositorios y estudios",
        limit=5,
        processed_dir=processed_dir,
    )
    payload = result.to_dict()

    assert payload["source_table"] == "opacity_criteria_knowledge"
    assert payload["returned_rows"] > 0
    assert any(row["reference_kind"] == "study" for row in payload["rows"])
    assert any(row["reference_kind"] == "local_repository" for row in payload["rows"])
    assert payload["validation"]["criteria"]["overall_status"] == "verified"
    assert payload["validation"]["latest_run"]["overall_status"] == "review_needed"
    assert payload["analysis"]["summary"]

