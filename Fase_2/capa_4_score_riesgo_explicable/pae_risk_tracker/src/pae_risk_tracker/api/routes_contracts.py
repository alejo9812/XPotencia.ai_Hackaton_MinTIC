from __future__ import annotations

import json
from typing import Any, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Request

from ..risk.scoring import score_contracts_frame
from ..storage.duckdb_store import DuckDBStore
from .schemas import (
    ApiResponse,
    ContractRiskResponse,
    HighRiskResponse,
    SearchContractsRequest,
    SearchContractsResponse,
)

router = APIRouter(tags=["contracts"])


@router.get("/health", response_model=ApiResponse)
def health(request: Request) -> ApiResponse:
    store = _get_store(request)
    return ApiResponse(status="ok" if store.path.exists() else "starting")


@router.get("/contracts", response_model=SearchContractsResponse)
@router.get("/contracts/search", response_model=SearchContractsResponse)
def search_contracts(
    request: Request,
    query: Optional[str] = None,
    entity_name: Optional[str] = None,
    department: Optional[str] = None,
    municipality: Optional[str] = None,
    supplier_name: Optional[str] = None,
    modality: Optional[str] = None,
    state: Optional[str] = None,
    min_amount: Optional[float] = None,
    max_amount: Optional[float] = None,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> SearchContractsResponse:
    store = _get_store(request)
    table_name = _base_table(store)
    if table_name is None:
        return SearchContractsResponse(source_table="", total_rows=0, returned_rows=0, rows=[])

    sql, params = _build_search_sql(
        table_name,
        query=query,
        entity_name=entity_name,
        department=department,
        municipality=municipality,
        supplier_name=supplier_name,
        modality=modality,
        state=state,
        min_amount=min_amount,
        max_amount=max_amount,
        limit=limit,
        offset=offset,
    )
    frame = store.query_frame(sql, params)
    count_sql, count_params = _build_search_sql(
        table_name,
        query=query,
        entity_name=entity_name,
        department=department,
        municipality=municipality,
        supplier_name=supplier_name,
        modality=modality,
        state=state,
        min_amount=min_amount,
        max_amount=max_amount,
        limit=None,
        offset=None,
        count_only=True,
    )
    total_rows = int(store.query_frame(count_sql, count_params).iloc[0, 0]) if count_sql else 0
    rows = [_canonicalize_contract_row(row) for row in frame.to_dict(orient="records")]
    return SearchContractsResponse(
        source_table=table_name,
        total_rows=total_rows,
        returned_rows=int(len(frame)),
        rows=rows,
    )


@router.get("/contracts/{contract_id}", response_model=ContractRiskResponse)
@router.get("/contracts/{contract_id}/risk", response_model=ContractRiskResponse)
def contract_risk(request: Request, contract_id: str) -> ContractRiskResponse:
    store = _get_store(request)
    source_table = _base_table(store)
    if source_table is None:
        raise HTTPException(status_code=404, detail="No contract data table is available.")

    row = _fetch_contract_row(store, source_table, contract_id)
    if row is None and source_table == "pae_contracts_scored" and store.has_table("pae_contracts_core"):
        row = _fetch_contract_row(store, "pae_contracts_core", contract_id)
        source_table = "pae_contracts_core"
    if row is None:
        raise HTTPException(status_code=404, detail=f"Contract {contract_id!r} was not found.")

    row = _canonicalize_contract_row(row)
    if source_table == "pae_contracts_scored" and "risk_score" in row:
        risk = _canonicalize_risk_payload(row)
    else:
        frame = pd.DataFrame([row])
        scored, _ = score_contracts_frame(frame, external_tables=_load_context_tables(store))
        risk = _canonicalize_risk_payload(scored.iloc[0].to_dict())
    return ContractRiskResponse(
        source_table=source_table,
        contract_id=str(contract_id),
        found=True,
        row=row,
        risk=risk,
    )


@router.get("/reports/high-risk", response_model=HighRiskResponse)
def high_risk_contracts(
    request: Request,
    limit: int = Query(default=25, ge=1, le=500),
    threshold: Optional[int] = Query(default=None, ge=0, le=100),
) -> HighRiskResponse:
    store = _get_store(request)
    source_table = _base_table(store)
    if source_table is None:
        return HighRiskResponse(source_table="", threshold=threshold or 61, total_rows=0, returned_rows=0, rows=[])

    threshold_value = int(threshold if threshold is not None else request.app.state.settings.high_risk_threshold)
    scored = _ensure_scored_frame(store, source_table)
    filtered = scored[scored["risk_score"].fillna(0).astype(float) >= threshold_value]
    total_rows = int(len(filtered))
    if "amount" not in filtered.columns:
        filtered = filtered.assign(
            _sort_amount=[
                _first_number(
                    row,
                    "amount",
                    "final_value",
                    "valor_final",
                    "valor_total_adjudicacion",
                    "precio_base",
                )
                for row in filtered.to_dict(orient="records")
            ]
        )
    else:
        filtered = filtered.assign(_sort_amount=pd.to_numeric(filtered["amount"], errors="coerce").fillna(0))
    filtered = filtered.sort_values(["risk_score", "_sort_amount"], ascending=[False, False]).head(limit)
    rows = [_canonicalize_contract_row(row) for row in filtered.to_dict(orient="records")]
    return HighRiskResponse(
        source_table=source_table,
        threshold=threshold_value,
        total_rows=total_rows,
        returned_rows=int(len(filtered)),
        rows=rows,
    )


def _get_store(request: Request) -> DuckDBStore:
    store = getattr(request.app.state, "store", None)
    if store is None:
        raise HTTPException(status_code=500, detail="DuckDB store is not configured.")
    return store


def _base_table(store: DuckDBStore) -> Optional[str]:
    if store.has_table("pae_contracts_scored"):
        return "pae_contracts_scored"
    if store.has_table("pae_contracts_core"):
        return "pae_contracts_core"
    return None


def _ensure_scored_frame(store: DuckDBStore, source_table: str) -> pd.DataFrame:
    if source_table == "pae_contracts_scored":
        return store.read_frame("SELECT * FROM pae_contracts_scored")
    if not store.has_table(source_table):
        return pd.DataFrame()
    frame = store.read_frame(f"SELECT * FROM {source_table}")
    scored, _ = score_contracts_frame(frame, external_tables=_load_context_tables(store))
    return scored


def _load_context_tables(store: DuckDBStore) -> dict[str, pd.DataFrame]:
    candidates = [
        "pae_additions",
        "additions",
        "paco_events",
        "paco_disciplinary",
        "paco_penal",
        "paco_fiscal",
        "paco_contractual",
        "paco_collusion",
        "sanctions",
    ]
    tables: dict[str, pd.DataFrame] = {}
    for table_name in candidates:
        if not store.has_table(table_name):
            continue
        tables[table_name] = store.read_frame(f"SELECT * FROM {table_name}")
    return tables


def _fetch_contract_row(store: DuckDBStore, table_name: str, contract_id: str) -> Optional[dict[str, Any]]:
    sql = f"""
        SELECT *
        FROM {table_name}
        WHERE contract_id = ? OR process_id = ?
        ORDER BY contract_id, process_id
        LIMIT 1
    """
    frame = store.query_frame(sql, [contract_id, contract_id])
    if frame.empty:
        return None
    return frame.iloc[0].to_dict()


def _extract_risk_payload(row: dict[str, Any]) -> dict[str, Any]:
    return _canonicalize_risk_payload(row)


def _canonicalize_contract_row(row: dict[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    payload["entity"] = _first_text(payload, "entity", "entity_name", "entidad", "nombre_entidad", "process_entity_norm")
    payload["entity_nit"] = _first_text(payload, "entity_nit", "nit_entidad", "entity_doc", "codigo_entidad_creadora", "codigo_entidad")
    payload["supplier"] = _first_text(payload, "supplier", "supplier_name", "proveedor", "proveedor_adjudicado")
    payload["supplier_nit"] = _first_text(payload, "supplier_nit", "supplier_doc_norm", "supplier_doc", "documento_proveedor", "nit_proveedor")
    payload["department"] = _first_text(payload, "department", "departamento", "department_name")
    payload["municipality"] = _first_text(payload, "municipality", "municipio", "municipality_name")
    payload["object"] = _first_text(payload, "object", "object_text", "objeto", "descripcion_del_proceso")
    payload["modality"] = _first_text(payload, "modality", "modality_text", "modalidad", "modalidad_de_contratacion")
    payload["status"] = _first_text(payload, "status", "estado", "estado_contrato")
    payload["initial_value"] = _first_number(payload, "initial_value", "valor_inicial", "value_initial", "estimated_amount", "precio_base")
    payload["final_value"] = _first_number(payload, "final_value", "valor_final", "value_final", "amount", "valor_total_adjudicacion")
    payload["start_date"] = _first_text(payload, "start_date", "date", "fecha_de_firma", "fecha_de_publicacion_del")
    payload["end_date"] = _first_text(payload, "end_date", "fecha_de_fin", "fecha_de_terminacion")
    payload["year"] = _first_number(payload, "year", "core_year", "process_year")
    payload["month"] = _first_number(payload, "month", "signature_month", "contract_month")
    payload["risk_score"] = int(_first_number(payload, "risk_score", "score") or 0)
    payload["risk_level"] = _normalize_level(payload.get("risk_level"), payload["risk_score"])
    payload["red_flags"] = _parse_red_flags_payload(payload)
    payload["evidence"] = _canonical_evidence(payload)
    payload["secop_url"] = _first_text(payload, "secop_url", "url_secop", "url_process", "urlproceso")
    payload["recommended_action"] = _first_text(payload, "recommended_action", "audit_recommendation", "recomendacion") or _derive_recommended_action(payload)
    payload["limitations"] = _first_text(payload, "limitations", "risk_limitations", "riskLimitations") or _first_text(payload, "huecos_de_informacion")
    payload["risk_flags_json"] = _parse_json_value(payload.get("risk_flags_json"))
    payload["risk_dimension_scores_json"] = _parse_json_value(payload.get("risk_dimension_scores_json"))
    payload["risk_summary"] = _first_text(payload, "risk_summary", "score_explanation", "explanation")
    payload["risk_limitations"] = _first_text(payload, "risk_limitations", "limitations")
    return payload


def _canonicalize_risk_payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = _canonicalize_contract_row(row)
    payload["summary"] = _first_text(payload, "summary", "risk_summary", "score_explanation", "explanation")
    payload["limitations"] = _first_text(payload, "limitations", "risk_limitations") or _derive_limitations(payload)
    payload["evidence"] = _canonical_evidence(payload)
    payload["red_flags"] = _parse_red_flags_payload(payload)
    payload["recommended_action"] = _first_text(payload, "recommended_action", "audit_recommendation", "recomendacion") or _derive_recommended_action(payload)
    return payload


def _canonical_evidence(payload: dict[str, Any]) -> Any:
    for key in ("evidence", "audit_evidence", "flag_evidence"):
        value = payload.get(key)
        if value not in (None, "", [], {}):
            if isinstance(value, str):
                parsed = _parse_json_value(value)
                if parsed is not None:
                    return parsed
            return value

    parsed_flags = _parse_red_flags_payload(payload)
    if parsed_flags:
        return parsed_flags

    raw_flags = payload.get("risk_flags_json")
    parsed = _parse_json_value(raw_flags)
    if parsed is not None:
        return parsed

    return {}


def _derive_recommended_action(payload: dict[str, Any]) -> str:
    score = int(_first_number(payload, "risk_score", "score") or 0)
    if score >= 76:
        return "Prioridad critica para revision documental inmediata."
    if score >= 56:
        return "Prioridad alta para revision documental."
    if score >= 31:
        return "Requiere revision complementaria."
    return "Mantener en monitoreo rutinario."


def _derive_limitations(payload: dict[str, Any]) -> str:
    raw_limitations = _first_text(payload, "limitations", "risk_limitations")
    if raw_limitations:
        return raw_limitations
    gaps = payload.get("huecos_de_informacion")
    if isinstance(gaps, list) and gaps:
        return " | ".join(str(item).strip() for item in gaps if str(item).strip())
    return ""


def _parse_red_flags_payload(payload: dict[str, Any]) -> list[str]:
    for key in ("red_flags", "red_flags_activadas", "activated_flags", "risk_flags"):
        value = payload.get(key)
        if value in (None, "", [], {}):
            continue
        if isinstance(value, list):
            codes: list[str] = []
            for item in value:
                if isinstance(item, dict):
                    code = str(item.get("code") or "").strip()
                    if code:
                        codes.append(code)
                else:
                    text = str(item or "").strip()
                    if text:
                        codes.append(text)
            return codes
        if isinstance(value, str):
            parsed = _parse_json_value(value)
            if isinstance(parsed, list):
                codes = []
                for item in parsed:
                    if isinstance(item, dict):
                        code = str(item.get("code") or "").strip()
                        if code:
                            codes.append(code)
                    else:
                        text = str(item or "").strip()
                        if text:
                            codes.append(text)
                if codes:
                    return codes
            return [item.strip() for item in value.replace("[", "").replace("]", "").split(",") if item.strip()]
    return []


def _first_text(payload: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = payload.get(key)
        if value in (None, "", [], {}):
            continue
        if isinstance(value, list):
            cleaned = " | ".join(str(item).strip() for item in value if str(item).strip())
            if cleaned:
                return cleaned
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _first_number(payload: dict[str, Any], *keys: str) -> float:
    for key in keys:
        value = payload.get(key)
        if value in (None, "", [], {}):
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if pd.notna(number):
            return number
    return 0.0


def _normalize_level(value: Any, score: Any | None = None) -> str:
    text = str(value or "").strip().lower()
    if text == "alto":
        return "Alto"
    if text == "medio":
        return "Medio"
    if text == "critico" or text == "crítico":
        return "Critico"
    if text == "bajo":
        return "Bajo"
    try:
        numeric_score = float(score) if score is not None else None
    except (TypeError, ValueError):
        numeric_score = None
    if numeric_score is not None:
        if numeric_score >= 85:
            return "Critico"
        if numeric_score >= 56:
            return "Alto"
        if numeric_score >= 31:
            return "Medio"
        return "Bajo"
    return "Bajo"


def _parse_json_value(value: Any) -> Any:
    if value in (None, "", [], {}):
        return None
    if isinstance(value, (list, dict)):
        return value
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except Exception:
        return None


def _build_search_sql(
    table_name: str,
    *,
    query: Optional[str],
    entity_name: Optional[str],
    department: Optional[str],
    municipality: Optional[str],
    supplier_name: Optional[str],
    modality: Optional[str],
    state: Optional[str],
    min_amount: Optional[float],
    max_amount: Optional[float],
    limit: Optional[int],
    offset: Optional[int],
    count_only: bool = False,
) -> tuple[str, list[Any]]:
    where: list[str] = ["1 = 1"]
    params: list[Any] = []

    def add_like(column: str, value: Optional[str]) -> None:
        if value:
            where.append(f"LOWER(COALESCE({column}, '')) LIKE ?")
            params.append(f"%{value.lower()}%")

    add_like("entity_name", entity_name)
    add_like("department", department)
    add_like("municipality", municipality)
    add_like("supplier_name", supplier_name)
    add_like("modality", modality)
    add_like("state", state)

    if query:
        where.append(
            "("
            "LOWER(COALESCE(contract_id, '')) LIKE ? OR "
            "LOWER(COALESCE(process_id, '')) LIKE ? OR "
            "LOWER(COALESCE(entity_name, '')) LIKE ? OR "
            "LOWER(COALESCE(supplier_name, '')) LIKE ? OR "
            "LOWER(COALESCE(object_text, '')) LIKE ? OR "
            "LOWER(COALESCE(justification, '')) LIKE ?"
            ")"
        )
        query_value = f"%{query.lower()}%"
        params.extend([query_value] * 6)

    if min_amount is not None:
        where.append("COALESCE(CAST(amount AS DOUBLE), 0) >= ?")
        params.append(min_amount)

    if max_amount is not None:
        where.append("COALESCE(CAST(amount AS DOUBLE), 0) <= ?")
        params.append(max_amount)

    where_sql = " AND ".join(where)
    if count_only:
        return f"SELECT COUNT(*) FROM {table_name} WHERE {where_sql}", params

    limit_sql = "" if limit is None else " LIMIT ?"
    if limit is not None:
        params.append(limit)
    offset_sql = ""
    if offset is not None:
        offset_sql = " OFFSET ?"
        params.append(offset)
    sql = f"SELECT * FROM {table_name} WHERE {where_sql} ORDER BY COALESCE(CAST(amount AS DOUBLE), 0) DESC, contract_id{limit_sql}{offset_sql}"
    return sql, params
