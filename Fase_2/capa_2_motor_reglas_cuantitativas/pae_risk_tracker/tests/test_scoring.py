from __future__ import annotations

import json

import pandas as pd

from pae_risk_tracker.risk.scoring import score_contracts_frame, summarize_scores


def test_scoring_flags_and_order():
    frame = pd.DataFrame(
        [
            {
                "contract_id": "C-1",
                "entity_name": "Alcaldia de Ibague",
                "supplier_name": "Proveedor Uno SAS",
                "modality": "Contratacion Directa",
                "object_text": "Servicio integral de apoyo logistico",
                "justification": "",
                "amount": 1_000_000_000,
                "estimated_amount": 1_020_000_000,
                "date": "2025-01-20",
                "start_date": "",
                "end_date": "",
                "duration_days": 0,
                "addition_value": 500_000_000,
                "addition_days": 45,
                "participants": 1,
                "offers": 1,
                "department": "Tolima",
                "municipality": "Ibague",
                "has_url_process": False,
                "has_justification": False,
            },
            {
                "contract_id": "C-2",
                "entity_name": "Alcaldia de Ibague",
                "supplier_name": "Proveedor Uno SAS",
                "modality": "Contratacion Directa",
                "object_text": "Servicio integral de apoyo logistico",
                "justification": "",
                "amount": 120_000_000,
                "estimated_amount": 0,
                "date": "2025-01-22",
                "start_date": "",
                "end_date": "",
                "duration_days": 0,
                "addition_value": 0,
                "addition_days": 0,
                "participants": 1,
                "offers": 1,
                "department": "Tolima",
                "municipality": "Ibague",
                "has_url_process": False,
                "has_justification": False,
            },
            {
                "contract_id": "C-3",
                "entity_name": "Alcaldia de Cali",
                "supplier_name": "Proveedor Dos SAS",
                "modality": "Licitacion Publica",
                "object_text": "Compra de alimentos para PAE con especificaciones claras",
                "justification": "Justificacion tecnica visible",
                "amount": 5_000_000,
                "estimated_amount": 6_000_000,
                "date": "2025-03-10",
                "start_date": "2025-03-15",
                "end_date": "2025-06-15",
                "duration_days": 92,
                "addition_value": 0,
                "addition_days": 0,
                "participants": 8,
                "offers": 6,
                "department": "Valle del Cauca",
                "municipality": "Cali",
                "has_url_process": True,
                "has_justification": True,
            },
        ]
    )

    scored, summary = score_contracts_frame(frame)

    assert int(scored.loc[0, "risk_score"]) > int(scored.loc[2, "risk_score"])
    assert str(scored.loc[0, "risk_level"]) in {"alto", "critico"}

    flags = json.loads(scored.loc[0, "risk_flags_json"])
    codes = {flag["code"] for flag in flags}
    assert "RF-07" in codes
    assert "RF-08" in codes
    assert "RF-22" in codes or "RF-20" in codes

    summary_counts = summarize_scores(scored)["level_counts"]
    assert summary_counts["critico"] >= 0
    assert summary["total_records"] == 3
