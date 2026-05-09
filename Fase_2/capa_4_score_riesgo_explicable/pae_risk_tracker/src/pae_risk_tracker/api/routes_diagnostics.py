from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from ..diagnostics.process_diagnostics import build_process_diagnostic_report
from ..storage.duckdb_store import DuckDBStore
from .schemas import ProcessDiagnosticsResponse

router = APIRouter(tags=["diagnostics"])


@router.get("/diagnostics/process", response_model=ProcessDiagnosticsResponse)
def process_diagnostics(
    request: Request,
    limit: int = Query(default=8, ge=1, le=50),
    synthetic_count: int = Query(default=4, ge=0, le=20),
) -> ProcessDiagnosticsResponse:
    store = _get_store(request)
    report = build_process_diagnostic_report(
        store,
        processed_dir=request.app.state.settings.processed_dir,
        limit=limit,
        synthetic_count=synthetic_count,
    )
    return ProcessDiagnosticsResponse(**report.to_dict())


def _get_store(request: Request) -> DuckDBStore:
    store = getattr(request.app.state, "store", None)
    if store is None:
        raise HTTPException(status_code=500, detail="DuckDB store is not configured.")
    return store

