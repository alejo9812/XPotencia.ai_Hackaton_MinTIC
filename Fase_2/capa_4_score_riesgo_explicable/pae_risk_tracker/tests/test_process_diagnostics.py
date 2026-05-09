from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pae_risk_tracker.diagnostics.process_diagnostics import write_process_diagnostic_report
from pae_risk_tracker.storage.duckdb_store import DuckDBStore


def test_process_diagnostics_builds_real_and_synthetic_cases(tmp_path):
    store = DuckDBStore(tmp_path / "tracker.duckdb")
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    scored = pd.DataFrame(
        [
            {
                "contract_id": "C-1",
                "process_id": "P-1",
                "entity": "Alcaldia de Ibague",
                "supplier": "Proveedor Uno",
                "department": "Tolima",
                "municipality": "Ibague",
                "modality": "Contratacion Directa",
                "risk_score": 88,
                "risk_level": "critico",
                "amount": 150_000_000,
                "risk_flags_json": json.dumps([{"code": "RF-07"}, {"code": "RF-08"}], ensure_ascii=False),
                "risk_summary": "Baja competencia y trazabilidad incompleta.",
                "risk_limitations": "Faltan soportes de publicidad y expediente.",
                "secop_url": "https://example.com/1",
            },
            {
                "contract_id": "C-2",
                "process_id": "P-2",
                "entity": "Alcaldia de Cali",
                "supplier": "Proveedor Dos",
                "department": "Valle del Cauca",
                "municipality": "Cali",
                "modality": "Licitacion Publica",
                "risk_score": 64,
                "risk_level": "alto",
                "amount": 90_000_000,
                "risk_flags_json": json.dumps([{"code": "RF-22"}, {"code": "RF-26"}], ensure_ascii=False),
                "risk_summary": "Adiciones relevantes en la ejecucion.",
                "risk_limitations": "Cruzar actas de modificacion.",
                "secop_url": "https://example.com/2",
            },
            {
                "contract_id": "C-3",
                "process_id": "P-3",
                "entity": "Alcaldia de Manizales",
                "supplier": "Proveedor Tres",
                "department": "Caldas",
                "municipality": "Manizales",
                "modality": "Licitacion Publica",
                "risk_score": 22,
                "risk_level": "bajo",
                "amount": 20_000_000,
                "risk_flags_json": json.dumps([], ensure_ascii=False),
                "risk_summary": "Sin senales fuertes.",
                "risk_limitations": "Sin limitaciones relevantes.",
                "secop_url": "https://example.com/3",
            },
        ]
    )
    store.write_frame("pae_contracts_scored", scored, replace=True)
    store.write_frame(
        "validation_runs",
        pd.DataFrame(
            [
                {
                    "run_id": "validation-20260509-000010-000001",
                    "created_at": "2026-05-09T00:00:10+00:00",
                    "source_table": "pae_search_index",
                    "candidate_count": 3,
                    "paco_count": 1,
                    "secop_count": 1,
                    "external_count": 1,
                    "hit_count": 1,
                    "clear_count": 1,
                    "blocked_count": 0,
                    "error_count": 0,
                    "observation_count": 2,
                    "registry_source_count": 2,
                    "snapshot_count": 2,
                    "overall_status": "review_needed",
                    "report_path": str(tmp_path / "validation.json"),
                    "snapshot_dir": str(tmp_path / "snapshots"),
                }
            ]
        ),
        replace=True,
    )

    output_json = tmp_path / "process_diagnostics.json"
    output_csv = tmp_path / "process_diagnostic_cases.csv"
    report = write_process_diagnostic_report(
        store,
        processed_dir=processed_dir,
        output_json=output_json,
        output_csv=output_csv,
        limit=2,
        synthetic_count=3,
    )
    payload = report.to_dict()

    assert report.total_records == 3
    assert report.real_case_count == 2
    assert report.synthetic_case_count == 3
    assert report.criteria_status == "verified"
    assert report.traceability_gap_count >= 1
    assert payload["real_cases"][0]["diagnosis"]
    assert any(case["source_kind"] == "synthetic" for case in payload["synthetic_cases"])
    assert output_json.exists()
    assert output_csv.exists()

    json_payload = json.loads(output_json.read_text(encoding="utf-8"))
    csv_payload = pd.read_csv(output_csv)
    assert json_payload["overall_status"] == "ready"
    assert len(json_payload["real_cases"]) == 2
    assert set(csv_payload["case_kind"]) == {"real", "synthetic"}

