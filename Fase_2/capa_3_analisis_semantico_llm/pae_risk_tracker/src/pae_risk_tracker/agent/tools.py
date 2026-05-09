from __future__ import annotations

import re
from typing import Any, Dict, List

from ..config import normalize_text


def build_query_plan(query: str) -> dict[str, Any]:
    text = query.strip()
    normalized = normalize_text(text)
    plan: dict[str, Any] = {"query": text}

    if any(
        term in normalized
        for term in (
            "OPACIDAD",
            "TRANSPARENCIA",
            "CRITERIO DE OPACIDAD",
            "CRITERIOS DE OPACIDAD",
            "RED FLAG",
            "RED FLAGS",
            "BIBLIOGRAFIA",
            "REFERENCIA",
            "FUENTE",
            "METODOLOGIA",
        )
    ):
        plan["intent"] = "criteria"
        return plan

    if any(term in normalized for term in ("PAE", "ALIMENTACION ESCOLAR", "RACIONES", "OPERADOR ALIMENTARIO", "COMPLEMENTO ALIMENTARIO")):
        plan["query"] = "PAE"

    if any(term in normalized for term in ("ADICION", "PRORROGA", "OTROSI")):
        plan["record_type"] = "addition"
    elif any(term in normalized for term in ("PROCESO", "PLIEGO", "CONVOCATORIA", "LICITACION")):
        plan["record_type"] = "process"
    elif "CONTRATO" in normalized:
        plan["record_type"] = "contract"

    if match := re.search(r"\b(20\d{2})\b", normalized):
        year = int(match.group(1))
        plan["date_from"] = f"{year}-01-01"
        plan["date_to"] = f"{year}-12-31"

    if match := re.search(r"(?:MAYOR(?:ES)? A|MAS DE|DESDE|SUPERIOR A)?\s*(\d[\d\.,]*)\s*(MIL MILLONES|MILLONES?|MILES?)", normalized):
        amount = _parse_amount(match.group(1), match.group(2))
        if amount:
            plan["min_amount"] = amount

    if "CONTRATACION DIRECTA" in normalized:
        plan["modality"] = "Contratacion Directa"
    if "LICITACION" in normalized:
        plan["modality"] = "Licitacion Publica"

    for raw_department, canonical in _DEPARTMENT_ALIASES.items():
        if raw_department in normalized:
            plan["department"] = canonical
            break

    return plan


def select_evidence_rows(frame, limit: int = 10) -> List[Dict[str, Any]]:
    if frame is None or frame.empty:
        return []

    selected = frame.copy()
    if "record_type" in selected.columns:
        order_map = {"contract": 0, "addition": 1, "process": 2}
        selected["_record_order"] = selected["record_type"].map(lambda value: order_map.get(str(value), 3))
    else:
        selected["_record_order"] = 3

    for column in ("risk_score", "amount"):
        if column not in selected.columns:
            selected[column] = 0
        selected[column] = selected[column].fillna(0).astype(float)

    selected = selected.sort_values(["_record_order", "risk_score", "amount"], ascending=[True, False, False], na_position="last")
    selected = selected.head(limit).drop(columns=["_record_order"], errors="ignore")
    return selected.to_dict(orient="records")


def _parse_amount(raw: str, unit: str | None) -> float:
    digits = re.sub(r"[^\d]", "", raw)
    if not digits:
        return 0.0
    value = float(digits)
    normalized_unit = (unit or "").replace("Ó", "O").replace("ó", "o")
    if "MIL MILLONES" in normalized_unit:
        value *= 1_000_000_000
    elif "MILLON" in normalized_unit:
        value *= 1_000_000
    elif "MIL" in normalized_unit:
        value *= 1_000
    return value


_DEPARTMENT_ALIASES = {
    "TOLIMA": "Tolima",
    "VALLE DEL CAUCA": "Valle del Cauca",
    "CUNDINAMARCA": "Cundinamarca",
    "ANTIOQUIA": "Antioquia",
    "BOGOTA": "Bogota",
    "BOYACA": "Boyaca",
    "NARINO": "Narino",
}
