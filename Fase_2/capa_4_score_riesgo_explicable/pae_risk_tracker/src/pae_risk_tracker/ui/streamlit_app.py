from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from ..agent.orchestrator import run_agent_query
from ..api.settings import load_api_settings
from ..diagnostics.process_diagnostics import build_process_diagnostic_report
from ..retrieval.search_index import SEARCH_INDEX_TABLE
from ..risk.scoring import score_contracts_frame
from ..storage.duckdb_store import DuckDBStore


st.set_page_config(page_title="PAE Risk Tracker", layout="wide")


@st.cache_resource
def _get_store() -> DuckDBStore:
    settings = load_api_settings()
    return DuckDBStore(settings.duckdb_path)


def _load_frame(store: DuckDBStore, sql: str, params: list[Any] | None = None) -> pd.DataFrame:
    try:
        return store.query_frame(sql, params)
    except Exception:
        return pd.DataFrame()


def _latest_validation(store: DuckDBStore) -> dict[str, Any]:
    if not store.has_table("validation_runs"):
        return {}
    frame = _load_frame(store, "SELECT * FROM validation_runs ORDER BY created_at DESC LIMIT 1")
    if frame.empty:
        return {}
    return frame.iloc[0].to_dict()


def _validation_observations(store: DuckDBStore, run_id: str, limit: int = 25) -> pd.DataFrame:
    if not run_id or not store.has_table("validation_observations"):
        return pd.DataFrame()
    return _load_frame(
        store,
        "SELECT * FROM validation_observations WHERE run_id = ? ORDER BY inspected_at DESC LIMIT ?",
        [run_id, limit],
    )


def _format_money(value: Any) -> str:
    try:
        amount = float(value)
    except Exception:
        return "-"
    if amount >= 1_000_000_000:
        return f"${amount / 1_000_000_000:.1f}B"
    if amount >= 1_000_000:
        return f"${amount / 1_000_000:.1f}M"
    if amount >= 1_000:
        return f"${amount / 1_000:.1f}K"
    return f"${amount:.0f}"


st.title("PAE Risk Tracker")
st.caption("Contratos, validación PACO/SECOP y seguimiento de fuentes externas en una sola vista.")

store = _get_store()

with st.sidebar:
    st.header("Filtros")
    query = st.text_input("Consulta del agente", value="PAE Tolima riesgo alto")
    limit = st.slider("Límite de resultados", min_value=5, max_value=50, value=10, step=1)
    threshold = st.slider("Umbral alto riesgo", min_value=0, max_value=100, value=61, step=1)
    run_query = st.button("Ejecutar agente")

latest_validation = _latest_validation(store)
validation_df = _validation_observations(store, str(latest_validation.get("run_id") or ""), limit=25)

col_a, col_b, col_c, col_d = st.columns(4)
col_a.metric("Tabla índice", SEARCH_INDEX_TABLE if store.has_table(SEARCH_INDEX_TABLE) else "sin índice")
col_b.metric("Validación", str(latest_validation.get("overall_status") or "sin corridas"))
col_c.metric("Observaciones", int(latest_validation.get("observation_count") or 0))
col_d.metric("Snapshots", int(latest_validation.get("snapshot_count") or 0))

if latest_validation:
    st.subheader("Última corrida de validación")
    st.json(latest_validation, expanded=False)

if not validation_df.empty:
    st.subheader("Observaciones recientes")
    st.dataframe(validation_df, use_container_width=True, hide_index=True)

diagnostic_report = build_process_diagnostic_report(
    store,
    processed_dir=load_api_settings().processed_dir,
    limit=5,
    synthetic_count=4,
)
diagnostic_payload = diagnostic_report.to_dict()

st.subheader("Diagnostico del proceso")
diag_col_a, diag_col_b, diag_col_c, diag_col_d = st.columns(4)
diag_col_a.metric("Casos reales", diagnostic_report.real_case_count)
diag_col_b.metric("Casos sinteticos", diagnostic_report.synthetic_case_count)
diag_col_c.metric("Brechas trazabilidad", diagnostic_report.traceability_gap_count)
diag_col_d.metric("Cobertura opacidad", f"{diagnostic_report.criteria_coverage_ratio:.0%}")

with st.expander("Resumen del diagnostico", expanded=False):
    st.json(
        {
            "overall_status": diagnostic_report.overall_status,
            "source_table": diagnostic_report.source_table,
            "criteria_status": diagnostic_report.criteria_status,
            "validation_status": diagnostic_report.validation_status,
            "gaps": diagnostic_payload["gaps"],
            "process_steps": diagnostic_payload["process_steps"],
        },
        expanded=False,
    )

if diagnostic_report.real_cases:
    st.subheader("Casos reales priorizados")
    st.dataframe(pd.DataFrame([case.to_dict() for case in diagnostic_report.real_cases]), use_container_width=True, hide_index=True)
else:
    st.info("No hay casos reales suficientes para priorizar diagnostico.")

with st.expander("Casos sinteticos de guia", expanded=False):
    st.dataframe(pd.DataFrame([case.to_dict() for case in diagnostic_report.synthetic_cases]), use_container_width=True, hide_index=True)

st.subheader("Contratos con mayor riesgo")
if store.has_table("pae_contracts_scored"):
    risk_frame = _load_frame(store, "SELECT * FROM pae_contracts_scored WHERE COALESCE(risk_score, 0) >= ? ORDER BY risk_score DESC, amount DESC LIMIT ?", [threshold, limit])
else:
    risk_frame = pd.DataFrame()

if not risk_frame.empty:
    display_frame = risk_frame.copy()
    if "amount" in display_frame.columns:
        display_frame["amount"] = display_frame["amount"].apply(_format_money)
    st.dataframe(display_frame, use_container_width=True, hide_index=True)
else:
    st.info("No hay contratos cargados o el índice no está materializado todavía.")

if run_query:
    result = run_agent_query(store, query, limit=limit, processed_dir=load_api_settings().processed_dir)
    st.subheader("Respuesta del agente")
    st.json(
        {
            "plan": result.plan,
            "validation": result.validation,
            "analysis": result.analysis,
            "llm_mode": result.llm_mode,
            "llm_model": result.llm_model,
        },
        expanded=False,
    )
    st.subheader("Evidencia seleccionada")
    st.dataframe(pd.DataFrame(result.evidence_rows), use_container_width=True, hide_index=True)

st.divider()
st.caption("La validación prioriza PACO, luego SECOP y finalmente fuentes externas permitidas.")
