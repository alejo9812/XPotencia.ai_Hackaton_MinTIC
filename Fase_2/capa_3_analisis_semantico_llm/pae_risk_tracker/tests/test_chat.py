from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
from fastapi.testclient import TestClient

from pae_risk_tracker.api.server import create_app
from pae_risk_tracker.retrieval.search_index import materialize_search_index
from pae_risk_tracker.storage.duckdb_store import DuckDBStore


def test_chat_bootstrap_detail_and_report_memory(tmp_path):
    db_path = tmp_path / "pae.duckdb"
    processed_dir = tmp_path / "processed"
    output_dir = tmp_path / "outputs"
    processed_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    store = DuckDBStore(db_path)
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
                "secop_url": "https://example.com/process/C-1",
                "start_date": "2025-01-05",
                "end_date": "2025-03-05",
                "year": 2025,
                "month": 1,
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
                "urlproceso": {"url": "https://example.com/process/C-1"},
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
    validation_runs = pd.DataFrame(
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
                "observation_count": 1,
                "registry_source_count": 0,
                "snapshot_count": 1,
                "overall_status": "review_needed",
                "report_path": str(tmp_path / "validation.json"),
                "snapshot_dir": str(tmp_path / "snapshots"),
            }
        ]
    )
    validation_observations = pd.DataFrame(
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
    )

    store.write_frame("pae_contracts_scored", contracts, replace=True)
    store.write_frame("pae_processes", processes, replace=True)
    store.write_frame("pae_additions", additions, replace=True)
    store.write_frame("validation_runs", validation_runs, replace=True)
    store.write_frame("validation_observations", validation_observations, replace=True)
    materialize_search_index(store, processed_dir)

    settings = SimpleNamespace(duckdb_path=db_path, output_dir=output_dir, processed_dir=processed_dir)
    app = create_app(store=store, settings=settings)
    client = TestClient(app)

    bootstrap = client.get("/chat/bootstrap", params={"session_id": "session-1"}).json()
    assert bootstrap["intent"] == "project_overview"
    assert bootstrap["view_type"] == "project_overview"
    assert bootstrap["data"]["author"] == "Alejandro Montes"
    assert "Ver contratos con mayor riesgo" in bootstrap["suggested_actions"]
    assert bootstrap["data"]["decision_support"]["patterns"]
    assert bootstrap["data"]["decision_support"]["graph_suggestions"]

    detail = client.post(
        "/chat/respond",
        json={
            "session_id": "session-1",
            "query": "Muestra el detalle del contrato C-1",
            "limit": 5,
        },
    ).json()
    assert detail["intent"] == "contract_detail"
    assert detail["view_type"] == "contract_detail"
    assert detail["session_state"]["last_contract_id"] == "C-1"
    assert detail["data"]["contract"]["contract_id"] == "C-1"
    assert detail["data"]["decision_support"]["patterns"]
    assert detail["data"]["decision_support"]["graph_suggestions"]

    report = client.post(
        "/chat/respond",
        json={
            "session_id": "session-1",
            "query": "Genera un reporte ejecutivo",
            "limit": 5,
        },
    ).json()
    assert report["intent"] == "report_generation"
    assert report["view_type"] == "report_preview"
    assert report["data"]["scope_label"] == "Contrato C-1"
    assert report["session_state"]["last_contract_id"] == "C-1"
    assert report["session_state"]["last_report_type"] == "executive"
    assert report["data"]["export"]["available"] is True
    assert report["data"]["decision_support"]["guidance"]
    assert report["data"]["decision_support"]["graph_suggestions"]
