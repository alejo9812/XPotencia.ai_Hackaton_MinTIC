from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from ..agent.orchestrator import run_agent_query
from ..storage.duckdb_store import DuckDBStore
from .schemas import AgentQueryRequest, AgentQueryResponse

router = APIRouter(tags=["agent"])


@router.post("/agent/query", response_model=AgentQueryResponse)
def query_agent(request: Request, payload: AgentQueryRequest) -> AgentQueryResponse:
    store = _get_store(request)
    result = run_agent_query(store, payload.query, limit=payload.limit, processed_dir=request.app.state.settings.processed_dir)
    return AgentQueryResponse(**result.to_dict())


def _get_store(request: Request) -> DuckDBStore:
    store = getattr(request.app.state, "store", None)
    if store is None:
        raise HTTPException(status_code=500, detail="DuckDB store is not configured.")
    return store
