from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request

from ..storage.duckdb_store import DuckDBStore
from .schemas import ValidationContractResponse, ValidationLatestResponse

router = APIRouter(tags=["validation"])


@router.get("/validation/latest", response_model=ValidationLatestResponse)
def latest_validation(request: Request, limit: int = Query(default=25, ge=1, le=200)) -> ValidationLatestResponse:
    store = _get_store(request)
    summary = _latest_run(store)
    if summary is None:
        return ValidationLatestResponse(latest_run={}, observations=[], observation_count=0)
    observations = _observations_for_run(store, str(summary.get("run_id") or ""), limit=limit)
    return ValidationLatestResponse(
        latest_run=summary,
        observations=observations,
        observation_count=int(len(observations)),
    )


@router.get("/validation/contracts/{contract_id}", response_model=ValidationContractResponse)
def contract_validation(
    request: Request,
    contract_id: str,
    limit: int = Query(default=25, ge=1, le=200),
) -> ValidationContractResponse:
    store = _get_store(request)
    observations = _observations_for_contract(store, contract_id, limit=limit)
    return ValidationContractResponse(
        contract_id=contract_id,
        observation_count=int(len(observations)),
        observations=observations,
    )


def _get_store(request: Request) -> DuckDBStore:
    store = getattr(request.app.state, "store", None)
    if store is None:
        raise HTTPException(status_code=500, detail="DuckDB store is not configured.")
    return store


def _latest_run(store: DuckDBStore) -> Optional[dict]:
    if not store.has_table("validation_runs"):
        return None
    frame = store.query_frame("SELECT * FROM validation_runs ORDER BY created_at DESC LIMIT 1")
    if frame.empty:
        return None
    return frame.iloc[0].to_dict()


def _observations_for_run(store: DuckDBStore, run_id: str, *, limit: int) -> list[dict]:
    if not run_id or not store.has_table("validation_observations"):
        return []
    frame = store.query_frame(
        "SELECT * FROM validation_observations WHERE run_id = ? ORDER BY inspected_at DESC LIMIT ?",
        [run_id, limit],
    )
    return frame.to_dict(orient="records")


def _observations_for_contract(store: DuckDBStore, contract_id: str, *, limit: int) -> list[dict]:
    if not contract_id or not store.has_table("validation_observations"):
        return []
    frame = store.query_frame(
        """
        SELECT *
        FROM validation_observations
        WHERE contract_id = ? OR process_id = ? OR record_id = ?
        ORDER BY inspected_at DESC
        LIMIT ?
        """,
        [contract_id, contract_id, contract_id, limit],
    )
    return frame.to_dict(orient="records")
