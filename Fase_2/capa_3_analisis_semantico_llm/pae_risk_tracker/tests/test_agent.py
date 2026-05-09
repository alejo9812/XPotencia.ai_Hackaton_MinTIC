from __future__ import annotations

import pandas as pd

from pae_risk_tracker.agent.orchestrator import run_agent_query
from pae_risk_tracker.retrieval.search_index import materialize_search_index
from pae_risk_tracker.storage.duckdb_store import DuckDBStore


def test_agent_orchestrator_returns_mock_analysis(tmp_path):
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
                "risk_flags_json": '[{"code":"RF-07"},{"code":"RF-08"}]',
                "risk_dimension_scores_json": "{}",
                "object_text": "PAE alimentacion escolar",
                "justification": "",
                "url_process": "",
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
                "urlproceso": {"url": "https://example.com/process"},
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
                    "run_id": "validation-20260509-000001-000001",
                    "created_at": "2026-05-09T00:00:01+00:00",
                    "source_table": "pae_search_index",
                    "candidate_count": 1,
                    "paco_count": 1,
                    "secop_count": 1,
                    "external_count": 0,
                    "hit_count": 1,
                    "clear_count": 0,
                    "blocked_count": 0,
                    "error_count": 0,
                    "observation_count": 2,
                    "registry_source_count": 0,
                    "snapshot_count": 1,
                    "overall_status": "review_needed",
                    "report_path": str(tmp_path / "validation.json"),
                    "snapshot_dir": str(tmp_path / "snapshots"),
                }
            ]
        ),
        replace=True,
    )
    store.write_frame(
        "validation_observations",
        pd.DataFrame(
            [
                {
                    "run_id": "validation-20260509-000001-000001",
                    "stage": "paco",
                    "status": "hit",
                    "source_key": "disciplinary",
                    "source_name": "PACO disciplinary",
                    "source_kind": "local_paco",
                    "scope": "local",
                    "url": "",
                    "domain": "",
                    "record_type": "contract",
                    "record_id": "C-1",
                    "contract_id": "C-1",
                    "process_id": "P-1",
                    "entity_name": "Alcaldia de Ibague",
                    "supplier_name": "Proveedor Uno",
                    "department": "Tolima",
                    "municipality": "Ibague",
                    "evidence": "Proveedor Uno",
                    "confidence": 90,
                    "http_status": None,
                    "content_type": "",
                    "robots_status": "unknown",
                    "content_hash": "",
                    "byte_count": 0,
                    "title": "Proveedor Uno",
                    "description": "",
                    "text_excerpt": "",
                    "snapshot_path": "",
                    "error_message": "",
                    "inspected_at": "2026-05-09T00:00:01+00:00",
                }
            ]
        ),
        replace=True,
    )
    materialize_search_index(store, processed_dir)

    result = run_agent_query(store, "PAE en Tolima de 2025 mayores a 50 millones", limit=5, processed_dir=processed_dir)
    payload = result.to_dict()

    assert payload["llm_mode"] == "mock"
    assert payload["analysis"]["summary"]
    assert payload["analysis"]["recommendations"]
    assert payload["evidence_rows"]
    assert payload["plan"]["department"] == "Tolima"
    assert payload["validation"]["latest_run"]["overall_status"] == "review_needed"
    assert payload["validation"]["observations"]
    assert payload["validation"]["criteria"]["overall_status"] == "verified"
    assert payload["validation"]["criteria"]["criteria_count"] == 7
