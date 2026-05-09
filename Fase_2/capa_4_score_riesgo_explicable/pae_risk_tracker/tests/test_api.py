from __future__ import annotations

import pandas as pd
from fastapi.testclient import TestClient

from pae_risk_tracker.api.server import create_app
from pae_risk_tracker.storage.duckdb_store import DuckDBStore


def test_api_endpoints_smoke(tmp_path):
    db_path = tmp_path / "pae.duckdb"
    store = DuckDBStore(db_path)
    frame = pd.DataFrame(
        [
            {
                "contract_id": "C-1",
                "process_id": "P-1",
                "entity_name": "Alcaldia de Ibague",
                "entity_nit": "890123456-7",
                "department": "Tolima",
                "municipality": "Ibague",
                "supplier_name": "Proveedor Uno",
                "supplier_nit": "900123456-1",
                "modality": "Contratacion Directa",
                "state": "Adjudicado",
                "object_text": "PAE alimentacion escolar",
                "justification": "",
                "amount": 100_000_000,
                "risk_score": 88,
                "risk_level": "critico",
                "red_flags": '["RF-07","RF-08"]',
                "evidence": '{"RF-07": "Baja competencia", "RF-08": "Adiciones relevantes"}',
                "limitations": "Faltan soportes de publicidad y trazabilidad.",
                "recommended_action": "Prioridad critica para revision documental inmediata.",
                "risk_summary": "Se detectan señales de alerta por baja competencia y adiciones.",
                "risk_limitations": "Requiere revisar soportes y expedientes.",
                "risk_dimension_scores_json": "{}",
                "secop_url": "https://example.com/contract/C-1",
                "start_date": "2025-01-05",
                "end_date": "2025-03-05",
                "year": 2025,
                "month": 1,
            }
        ]
    )
    store.write_frame("pae_contracts_scored", frame, replace=True)
    store.write_frame(
        "validation_runs",
        pd.DataFrame(
            [
                {
                    "run_id": "validation-20260509-000002-000001",
                    "created_at": "2026-05-09T00:00:02+00:00",
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
                    "run_id": "validation-20260509-000002-000001",
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
                    "inspected_at": "2026-05-09T00:00:02+00:00",
                }
            ]
        ),
        replace=True,
    )

    app = create_app()
    app.state.store = store
    client = TestClient(app)

    assert client.get("/health").json()["status"] == "ok"

    search = client.get("/contracts", params={"query": "PAE", "limit": 5}).json()
    assert search["returned_rows"] == 1
    assert search["rows"][0]["contract_id"] == "C-1"
    assert search["rows"][0]["entity_nit"] == "890123456-7"
    assert search["rows"][0]["supplier_nit"] == "900123456-1"
    assert search["rows"][0]["risk_level"] == "Critico"
    assert search["rows"][0]["limitations"]

    contract_lookup = client.get("/contracts/C-1").json()
    assert contract_lookup["found"] is True
    assert contract_lookup["row"]["entity_nit"] == "890123456-7"
    assert contract_lookup["row"]["supplier_nit"] == "900123456-1"
    assert contract_lookup["row"]["risk_level"] == "Critico"

    risk = client.get("/contracts/C-1/risk").json()
    assert risk["found"] is True
    assert risk["risk"]["risk_score"] == 88
    assert risk["risk"]["risk_level"] == "Critico"
    assert risk["risk"]["red_flags"]
    assert risk["risk"]["limitations"]

    high_risk = client.get("/reports/high-risk").json()
    assert high_risk["total_rows"] == 1
    assert high_risk["rows"][0]["risk_level"] == "Critico"

    agent = client.post("/agent/query", json={"query": "PAE en Tolima de 2025 mayores a 50 millones", "limit": 5}).json()
    assert agent["plan"]["department"] == "Tolima"
    assert agent["plan"]["min_amount"] == 50_000_000.0
    assert agent["llm_mode"] == "mock"
    assert agent["analysis"]["summary"]
    assert isinstance(agent["evidence_rows"], list)
    assert agent["evidence_rows"]
    assert agent["validation"]["latest_run"]["overall_status"] == "review_needed"
    assert agent["validation"]["criteria"]["overall_status"] == "verified"
    assert agent["validation"]["criteria"]["criteria_count"] == 7

    latest_validation = client.get("/validation/latest").json()
    assert latest_validation["observation_count"] == 1
    assert latest_validation["latest_run"]["run_id"] == "validation-20260509-000002-000001"

    contract_validation = client.get("/validation/contracts/C-1").json()
    assert contract_validation["observation_count"] == 1

    diagnostics = client.get("/diagnostics/process", params={"limit": 1, "synthetic_count": 2}).json()
    assert diagnostics["overall_status"] == "ready"
    assert diagnostics["real_case_count"] == 1
    assert diagnostics["synthetic_case_count"] == 2
    assert diagnostics["real_cases"]
    assert diagnostics["synthetic_cases"]
