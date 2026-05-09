from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query, Request

from ..retrieval.search_index import (
    SEARCH_INDEX_TABLE,
    ensure_search_index,
    search_index_count_sql,
    search_index_sql,
)
from ..storage.duckdb_store import DuckDBStore
from .routes_contracts import _get_store
from .schemas import SearchRecordsResponse

router = APIRouter(tags=["search"])


@router.get("/records/search", response_model=SearchRecordsResponse)
def search_records(
    request: Request,
    query: Optional[str] = None,
    entity_name: Optional[str] = None,
    department: Optional[str] = None,
    municipality: Optional[str] = None,
    supplier_name: Optional[str] = None,
    modality: Optional[str] = None,
    state: Optional[str] = None,
    record_type: Optional[str] = None,
    min_amount: Optional[float] = None,
    max_amount: Optional[float] = None,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> SearchRecordsResponse:
    store = _get_store(request)
    _ensure_index(store, request)
    if not store.has_table(SEARCH_INDEX_TABLE):
        return SearchRecordsResponse(source_table="", total_rows=0, returned_rows=0, rows=[])

    sql, params = search_index_sql(
        query=query,
        entity_name=entity_name,
        department=department,
        municipality=municipality,
        supplier_name=supplier_name,
        modality=modality,
        state=state,
        record_type=record_type,
        min_amount=min_amount,
        max_amount=max_amount,
        limit=limit,
        offset=offset,
    )
    frame = store.query_frame(sql, params)
    count_sql, count_params = search_index_count_sql(
        query=query,
        entity_name=entity_name,
        department=department,
        municipality=municipality,
        supplier_name=supplier_name,
        modality=modality,
        state=state,
        record_type=record_type,
        min_amount=min_amount,
        max_amount=max_amount,
    )
    total_rows = int(store.query_frame(count_sql, count_params).iloc[0, 0]) if count_sql else 0
    return SearchRecordsResponse(
        source_table=SEARCH_INDEX_TABLE,
        total_rows=total_rows,
        returned_rows=int(len(frame)),
        rows=frame.to_dict(orient="records"),
    )


def _ensure_index(store: DuckDBStore, request: Request) -> None:
    settings = request.app.state.settings
    ensure_search_index(store, settings.processed_dir)
