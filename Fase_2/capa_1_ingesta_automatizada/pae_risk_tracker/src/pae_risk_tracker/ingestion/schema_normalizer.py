from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from ..config import alias_lookup, normalize_digits, normalize_text


@dataclass(frozen=True)
class NormalizedRow:
    raw: dict[str, Any]
    values: dict[str, Any]


DEFAULT_CORE_ALIASES = {
    "contract_id": ["id_contrato", "id_del_contrato", "referencia_del_contrato", "referencia_contrato"],
    "process_id": ["proceso_de_compra", "id_del_proceso", "id_procedimiento", "id_proceso", "referencia_del_proceso"],
    "process_reference": ["referencia_del_contrato", "referencia_del_proceso", "referencia_proceso"],
    "entity_name": ["nombre_entidad", "entidad", "entidad_compradora", "nombre_entidad_creadora"],
    "entity_nit": ["nit_entidad", "codigo_entidad_creadora", "codigo_entidad"],
    "supplier_name": ["proveedor_adjudicado", "proveedor", "nombre_proveedor_objeto_de", "nombre", "nombre_grupo"],
    "supplier_doc": ["documento_proveedor", "nit_proveedor", "nit", "as_codigo_proveedor_objeto", "nit_grupo"],
    "object_text": [
        "descripcion_del_proceso",
        "nombre_del_procedimiento",
        "descripcion_del_procedimiento",
        "descripcion",
        "notas",
        "tipo",
        "condiciones_de_entrega",
        "justificacion_modalidad_de",
    ],
    "justification": ["justificacion_modalidad_de", "justificacion_modalidad", "justificacion"],
    "modality": ["modalidad_de_contratacion", "tipo_de_contrato", "fase", "tipo"],
    "status": ["estado_contrato", "estado"],
    "amount": ["valor", "valor_total", "valor_a_pagar", "valor_total_de_la_factura", "valor_neto_de_la_factura"],
    "estimated_amount": ["valor_estimado", "valor_presupuesto", "valor_base", "valor_de_referencia"],
    "date": [
        "fecha_de_firma",
        "fecha_de_publicacion_del_proceso",
        "fecharegistro",
        "fecha_factura",
        "fecha_de_emision",
        "fecha_evento",
    ],
    "start_date": ["fecha_de_inicio_del_contrato", "fecha_de_inicio", "fecha_inicio", "inicio_del_contrato"],
    "end_date": ["fecha_de_fin_del_contrato", "fecha_de_fin", "fecha_fin", "fin_del_contrato"],
    "duration_days": ["dias_adicionados", "dias_adicionales", "plazo_dias", "duracion_dias"],
    "addition_value": ["valor_adicionado", "valor_de_adicion", "valor_adicion", "adicion_valor"],
    "addition_days": ["dias_adicionados", "dias_adicionales", "tiempo_adicionado"],
    "participants": ["numero_de_proponentes", "numero_participantes", "participantes", "oferentes"],
    "offers": ["numero_de_ofertas", "ofertas", "propuestas", "bids"],
    "department": ["departamento", "departamento_entidad", "departamento_grupo", "localizaci_n"],
    "municipality": ["ciudad", "ciudad_entidad", "municipio", "minucipio_grupo", "ubicacion"],
    "url_process": ["urlproceso", "url_proceso"],
}

DEFAULT_ADDITION_ALIASES = {
    "addition_id": ["identificador", "id_adicion", "id"],
    "contract_id": ["id_contrato", "id_del_contrato"],
    "addition_type": ["tipo"],
    "addition_description": ["descripcion", "notas", "detalle"],
    "addition_date": ["fecharegistro", "fecha_registro", "fecha"],
}


def resolve_core_columns(column_names: list[str], alias_map: dict[str, list[str]] | None = None) -> dict[str, str | None]:
    return alias_lookup(column_names, alias_map or DEFAULT_CORE_ALIASES)


def resolve_addition_columns(column_names: list[str], alias_map: dict[str, list[str]] | None = None) -> dict[str, str | None]:
    return alias_lookup(column_names, alias_map or DEFAULT_ADDITION_ALIASES)


def normalize_row(row: dict[str, Any], resolved_columns: dict[str, str | None] | None = None) -> dict[str, Any]:
    resolved_columns = resolved_columns or {}
    raw = dict(row)

    def pick(role: str) -> Any:
        column = resolved_columns.get(role)
        if column:
            return raw.get(column)
        for candidate in DEFAULT_CORE_ALIASES.get(role, []):
            if candidate in raw:
                return raw.get(candidate)
        return None

    text_blob = " ".join(
        str(pick(role) or "")
        for role in ("object_text", "modality", "supplier_name", "entity_name", "status")
    ).strip()
    normalized_blob = normalize_text(text_blob)
    amount_value = pick("amount")
    amount = _to_number(amount_value)
    date_value = pick("date")
    date = _to_date(date_value)

    values = {
        "contract_id": _coerce_text(pick("contract_id")),
        "process_id": _coerce_text(pick("process_id")),
        "entity_name": _coerce_text(pick("entity_name")),
        "entity_nit": normalize_digits(pick("entity_nit")),
        "supplier_name": _coerce_text(pick("supplier_name")),
        "supplier_doc": normalize_digits(pick("supplier_doc")),
        "object_text": _coerce_text(pick("object_text")),
        "justification": _coerce_text(pick("justification")),
        "modality": _coerce_text(pick("modality")),
        "status": _coerce_text(pick("status")),
        "amount": amount,
        "estimated_amount": _to_number(pick("estimated_amount")),
        "date": date,
        "start_date": _to_date(pick("start_date")),
        "end_date": _to_date(pick("end_date")),
        "duration_days": _to_int(pick("duration_days")),
        "addition_value": _to_number(pick("addition_value")),
        "addition_days": _to_int(pick("addition_days")),
        "participants": _to_int(pick("participants")),
        "offers": _to_int(pick("offers")),
        "department": _coerce_text(pick("department")),
        "municipality": _coerce_text(pick("municipality")),
        "url_process": _coerce_text(pick("url_process")),
        "search_blob": normalized_blob,
        "object_length": len(normalized_blob.split()),
    }
    return values


def classify_pae_record(record: dict[str, Any], keywords: dict[str, Any]) -> dict[str, Any]:
    blob = normalize_text(record.get("search_blob") or record.get("object_text") or "")
    high = _hits(blob, keywords.get("high_confidence", []))
    medium = _hits(blob, keywords.get("medium_confidence", []))
    low = _hits(blob, keywords.get("low_confidence", []))
    exclude = _hits(blob, keywords.get("exclude", []))

    if exclude:
        confidence = "descartar"
        score = 0
        terms = exclude
    elif high:
        confidence = "alto"
        score = 100
        terms = high
    elif medium:
        confidence = "medio"
        score = 75
        terms = medium
    elif low:
        confidence = "bajo"
        score = 45
        terms = low
    else:
        confidence = "descartar"
        score = 0
        terms = []

    output = dict(record)
    output.update(
        {
            "pae_confidence": confidence,
            "pae_match_score": score,
            "pae_match_terms": ", ".join(terms),
        }
    )
    return output


def normalize_addition_row(row: dict[str, Any], resolved_columns: dict[str, str | None] | None = None) -> dict[str, Any]:
    resolved_columns = resolved_columns or {}
    raw = dict(row)

    def pick(role: str) -> Any:
        column = resolved_columns.get(role)
        if column:
            return raw.get(column)
        for candidate in DEFAULT_ADDITION_ALIASES.get(role, []):
            if candidate in raw:
                return raw.get(candidate)
        return None

    addition_id = _coerce_text(pick("addition_id"))
    contract_id = _coerce_text(pick("contract_id"))
    addition_type = _coerce_text(pick("addition_type"))
    addition_description = _coerce_text(pick("addition_description"))
    addition_date = _to_date(pick("addition_date"))
    search_blob = normalize_text(" ".join([addition_id, contract_id, addition_type, addition_description, addition_date]))

    return {
        "addition_id": addition_id,
        "contract_id": contract_id,
        "addition_type": addition_type,
        "addition_description": addition_description,
        "addition_date": addition_date,
        "addition_match_terms": _hits(search_blob, ["adicion", "prorroga", "modificacion", "otrosi"]),
        "addition_confidence": _classify_addition_confidence(search_blob),
        "search_blob": search_blob,
    }


def dedupe_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for row in rows:
        key = str(
            row.get("contract_id")
            or row.get("process_id")
            or row.get("referencia_del_contrato")
            or row.get("referencia_contrato")
            or ""
        ).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def dedupe_addition_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for row in rows:
        key = str(
            row.get("addition_id")
            or "|".join(
                [
                    str(row.get("contract_id") or "").strip(),
                    str(row.get("addition_date") or "").strip(),
                    str(row.get("addition_type") or "").strip(),
                    str(row.get("addition_description") or "").strip(),
                ]
            )
        ).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def rows_to_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _coerce_text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _to_number(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if pd.isna(value):
            return 0.0
        return float(value)
    text = str(value).strip().replace("$", "").replace(" ", "")
    if not text:
        return 0.0
    try:
        if "," in text and "." in text:
            if text.rfind(",") > text.rfind("."):
                text = text.replace(".", "").replace(",", ".")
            else:
                text = text.replace(",", "")
        elif "," in text:
            text = text.replace(".", "").replace(",", ".")
        return float(text)
    except Exception:
        return 0.0


def _to_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(round(_to_number(value)))
    except Exception:
        return 0


def _to_date(value: Any) -> str:
    if value in (None, ""):
        return ""
    parsed = pd.to_datetime(value, errors="coerce", utc=False)
    if pd.isna(parsed):
        return ""
    return parsed.date().isoformat()


def _hits(blob: str, terms: list[str]) -> list[str]:
    hits = []
    for term in terms:
        candidate = normalize_text(term)
        if candidate and candidate in blob:
            hits.append(term)
    return hits


def _classify_addition_confidence(blob: str) -> str:
    if not blob:
        return "descartar"
    if any(token in blob for token in ("ADICION", "PRORROGA", "MODIFICACION")):
        return "alto"
    if "OTROSI" in blob or "TERMINO" in blob:
        return "medio"
    return "bajo"
