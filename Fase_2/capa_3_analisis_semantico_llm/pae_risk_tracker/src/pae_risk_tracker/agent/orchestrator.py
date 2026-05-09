from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from ..config import load_llm_registry
from ..paths import PROCESSED_DIR
from ..retrieval.search_index import SEARCH_INDEX_TABLE, ensure_search_index, search_index_sql
from ..risk.scoring import score_contracts_frame
from ..risk.opacity_criteria import build_opacity_criteria_report
from ..storage.duckdb_store import DuckDBStore
from .knowledge import search_criteria_knowledge
from .llm_client import LLMAnalysis, LLMClient, MockLLMClient
from .tools import build_query_plan, select_evidence_rows
from ..api.routes_contracts import (
    _base_table,
    _build_search_sql,
    _canonicalize_contract_row,
    _ensure_scored_frame,
    _load_context_tables,
)


@dataclass(frozen=True)
class AgentRunResult:
    query: str
    plan: dict[str, Any]
    source_table: str
    total_rows: int
    returned_rows: int
    rows: list[dict[str, Any]]
    evidence_rows: list[dict[str, Any]]
    validation: dict[str, Any]
    analysis: dict[str, Any]
    llm_mode: str
    llm_model: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def run_agent_query(
    store: DuckDBStore,
    query: str,
    limit: int = 10,
    llm_client: Optional[LLMClient] = None,
    processed_dir: Path | None = None,
    force_index_refresh: bool = False,
) -> AgentRunResult:
    settings = load_llm_registry()
    processed_dir = processed_dir or PROCESSED_DIR
    max_records = int(settings.get("max_records_per_call", 10))
    limit = max(1, min(int(limit), max_records))

    plan = build_query_plan(query)
    if plan.get("intent") == "criteria":
        knowledge_result = search_criteria_knowledge(store, query, limit, processed_dir=processed_dir)
        analysis = _analyze_with_llm(query, plan, knowledge_result.evidence_rows, knowledge_result.validation, llm_client, settings)
        return AgentRunResult(
            query=query,
            plan=plan,
            source_table=knowledge_result.source_table,
            total_rows=knowledge_result.total_rows,
            returned_rows=knowledge_result.returned_rows,
            rows=knowledge_result.rows,
            evidence_rows=knowledge_result.evidence_rows,
            validation=knowledge_result.validation,
            analysis=analysis.to_dict(),
            llm_mode=analysis.provider,
            llm_model=analysis.model,
        )

    if force_index_refresh:
        from ..retrieval.search_index import materialize_search_index

        materialize_search_index(store, processed_dir)
    else:
        ensure_search_index(store, processed_dir)

    source_table = SEARCH_INDEX_TABLE if store.has_table(SEARCH_INDEX_TABLE) else _base_table(store)
    if source_table is None:
        validation = _load_validation_context(store, [], processed_dir=processed_dir)
        analysis = _analyze_with_llm(query, plan, [], validation, llm_client, settings)
        return AgentRunResult(
            query=query,
            plan=plan,
            source_table="",
            total_rows=0,
            returned_rows=0,
            rows=[],
            evidence_rows=[],
            validation=validation,
            analysis=analysis.to_dict(),
            llm_mode=analysis.provider,
            llm_model=analysis.model,
        )

    if source_table == SEARCH_INDEX_TABLE:
        sql, params = search_index_sql(
            query=plan.get("query"),
            entity_name=plan.get("entity_name"),
            department=plan.get("department"),
            municipality=plan.get("municipality"),
            supplier_name=plan.get("supplier_name"),
            modality=plan.get("modality"),
            state=plan.get("state"),
            record_type=plan.get("record_type"),
            min_amount=plan.get("min_amount"),
            max_amount=plan.get("max_amount"),
            limit=limit,
            offset=0,
        )
    else:
        sql, params = _build_search_sql(
            source_table,
            query=plan.get("query"),
            entity_name=plan.get("entity_name"),
            department=plan.get("department"),
            municipality=plan.get("municipality"),
            supplier_name=plan.get("supplier_name"),
            modality=plan.get("modality"),
            state=plan.get("state"),
            min_amount=plan.get("min_amount"),
            max_amount=plan.get("max_amount"),
            limit=limit,
            offset=0,
        )

    frame = store.query_frame(sql, params)
    if frame.empty and source_table != "pae_contracts_core" and store.has_table("pae_contracts_core"):
        core_frame = store.read_frame("SELECT * FROM pae_contracts_core")
        scored_frame, _ = score_contracts_frame(core_frame, external_tables=_load_context_tables(store))
        frame = scored_frame.head(limit)
        source_table = "pae_contracts_core"

    evidence_rows = [_canonicalize_contract_row(row) for row in select_evidence_rows(frame, limit=limit)]
    validation = _load_validation_context(store, evidence_rows, processed_dir=processed_dir)
    analysis = _analyze_with_llm(query, plan, evidence_rows, validation, llm_client, settings)
    return AgentRunResult(
        query=query,
        plan=plan,
        source_table=source_table,
        total_rows=int(len(frame)),
        returned_rows=int(len(frame)),
        rows=[_canonicalize_contract_row(row) for row in frame.to_dict(orient="records")],
        evidence_rows=evidence_rows,
        validation=validation,
        analysis=analysis.to_dict(),
        llm_mode=analysis.provider,
        llm_model=analysis.model,
    )


def _analyze_with_llm(
    query: str,
    plan: dict[str, Any],
    evidence_rows: list[dict[str, Any]],
    validation: dict[str, Any],
    llm_client: Optional[LLMClient],
    settings: dict[str, Any],
) -> LLMAnalysis:
    client = llm_client
    if client is None:
        provider = str(settings.get("provider", "mock")).lower()
        if provider == "mock":
            client = MockLLMClient(
                model=str(settings.get("model", "mock")),
                provider=provider,
                prompt_version=str(settings.get("prompt_version", "2026-05-09.mock-v1")),
            )
        else:
            client = MockLLMClient(
                model=str(settings.get("model", "mock")),
                provider="mock",
                prompt_version=str(settings.get("prompt_version", "2026-05-09.mock-v1")),
            )
    payload = {
        "query": query,
        "plan": plan,
        "evidence_rows": evidence_rows,
        "validation": validation,
        "system_prompt": "PAE risk tracker agent",
        "max_records_per_call": int(settings.get("max_records_per_call", 10)),
    }
    return client.analyze(payload)


def _load_validation_context(
    store: DuckDBStore,
    evidence_rows: list[dict[str, Any]],
    *,
    processed_dir: Path | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    processed_dir = processed_dir or PROCESSED_DIR
    with ThreadPoolExecutor(max_workers=2) as executor:
        latest_future = executor.submit(_latest_validation_run, store)
        criteria_future = executor.submit(
            build_opacity_criteria_report,
            store,
            processed_dir=processed_dir,
        )
        latest_run = latest_future.result()
        criteria = criteria_future.result()

    context: dict[str, Any] = {
        "latest_run": latest_run,
        "observations": [],
        "criteria": criteria.to_dict(),
        "criteria_status": criteria.overall_status,
        "criteria_coverage_ratio": criteria.coverage_ratio,
    }

    if not store.has_table("validation_observations") or not evidence_rows:
        return context

    ids: list[str] = []
    seen: set[str] = set()
    for row in evidence_rows:
        for key in ("contract_id", "process_id", "record_id"):
            value = str(row.get(key) or "").strip()
            if not value or value in seen:
                continue
            seen.add(value)
            ids.append(value)
    if not ids:
        return context

    placeholders = ", ".join(["?"] * len(ids))
    sql = (
        "SELECT * FROM validation_observations "
        f"WHERE contract_id IN ({placeholders}) OR process_id IN ({placeholders}) OR record_id IN ({placeholders}) "
        "ORDER BY inspected_at DESC LIMIT ?"
    )
    params = [*ids, *ids, *ids, limit]
    observations = store.query_frame(sql, params)
    context["observations"] = observations.to_dict(orient="records")
    context["observation_count"] = int(len(observations))
    context["matched_ids"] = ids
    if not context["latest_run"] and not observations.empty:
        context["latest_run"] = {"status": "observed", "source": "validation_observations"}
    return context


def _latest_validation_run(store: DuckDBStore) -> dict[str, Any]:
    if not store.has_table("validation_runs"):
        return {}
    latest = store.query_frame("SELECT * FROM validation_runs ORDER BY created_at DESC LIMIT 1")
    if latest.empty:
        return {}
    return latest.iloc[0].to_dict()
