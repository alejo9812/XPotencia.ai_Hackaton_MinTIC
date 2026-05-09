from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def test_canonical_phase2_outputs_exist_and_are_consistent():
    project_root = Path(__file__).resolve().parents[1]
    outputs_dir = project_root / "data" / "outputs"
    config_path = project_root / "config" / "export_contract.json"

    required_fields = {
        "contract_id",
        "process_id",
        "entity",
        "entity_nit",
        "supplier",
        "supplier_nit",
        "department",
        "municipality",
        "object",
        "modality",
        "status",
        "initial_value",
        "final_value",
        "start_date",
        "end_date",
        "year",
        "month",
        "risk_score",
        "risk_level",
        "red_flags",
        "evidence",
        "secop_url",
        "recommended_action",
        "limitations",
    }

    export_contract = json.loads(config_path.read_text(encoding="utf-8"))
    export_field_names = {field["name"] for field in export_contract["fields"]}
    assert required_fields.issubset(export_field_names)

    ranking_csv = pd.read_csv(outputs_dir / "pae_risk_ranking.csv")
    assert required_fields.issubset(set(ranking_csv.columns))

    ranking_json = json.loads((outputs_dir / "pae_risk_ranking.json").read_text(encoding="utf-8"))
    assert isinstance(ranking_json, list)
    assert ranking_json
    first_ranking = ranking_json[0]
    assert required_fields.issubset(first_ranking.keys())
    assert isinstance(first_ranking["red_flags"], list)

    audit_cards = json.loads((outputs_dir / "pae_audit_cards.json").read_text(encoding="utf-8"))
    assert isinstance(audit_cards, list)
    assert audit_cards
    first_card = audit_cards[0]
    assert required_fields.issubset(first_card.keys())
    assert isinstance(first_card["red_flags"], list)
    assert first_card["evidence"] is not None
