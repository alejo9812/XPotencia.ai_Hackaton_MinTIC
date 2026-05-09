from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

import pandas as pd

from ..config import load_scoring_registry
from .evidence import RiskAssessment
from .rules_engine import evaluate_frame


def score_contracts_frame(frame: pd.DataFrame, external_tables: dict[str, pd.DataFrame] | None = None) -> tuple[pd.DataFrame, dict[str, Any]]:
    scored, summary = evaluate_frame(frame, external_tables=external_tables)
    return scored, summary


def score_contracts_records(records: list[dict[str, Any]], external_tables: dict[str, pd.DataFrame] | None = None) -> tuple[list[RiskAssessment], dict[str, Any]]:
    frame = pd.DataFrame(records)
    scored, summary = score_contracts_frame(frame, external_tables=external_tables)
    assessments: list[RiskAssessment] = []
    for _, row in scored.iterrows():
        assessments.append(
            RiskAssessment(
                contract_id=str(row.get("contract_id") or row.get("process_id") or ""),
                risk_score=int(row.get("risk_score") or 0),
                risk_level=str(row.get("risk_level") or "bajo"),
                flags=list(row.get("risk_flags") or []),
                summary=str(row.get("risk_summary") or ""),
                limitations=str(row.get("risk_limitations") or ""),
                dimension_scores=json.loads(row.get("risk_dimension_scores_json") or "{}"),
            )
        )
    return assessments, summary


def summarize_scores(scored: pd.DataFrame) -> dict[str, Any]:
    if scored.empty:
        return {
            "total_records": 0,
            "average_score": 0,
            "level_counts": {"bajo": 0, "medio": 0, "alto": 0, "critico": 0},
            "top_flags": [],
        }

    levels = scored["risk_level"].value_counts().to_dict()
    top_flags = _top_flags(scored)
    return {
        "total_records": int(len(scored)),
        "average_score": round(float(scored["risk_score"].mean()), 2),
        "level_counts": {
            "bajo": int(levels.get("bajo", 0)),
            "medio": int(levels.get("medio", 0)),
            "alto": int(levels.get("alto", 0)),
            "critico": int(levels.get("critico", 0)),
        },
        "top_flags": top_flags,
    }


def _top_flags(scored: pd.DataFrame) -> list[dict[str, Any]]:
    counter: dict[str, int] = {}
    for payload in scored["risk_flags"].tolist():
        for flag in payload or []:
            code = str(flag.code if hasattr(flag, "code") else flag.get("code"))
            counter[code] = counter.get(code, 0) + 1
    return [
        {"code": code, "count": count}
        for code, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:10]
    ]

