from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from ..chat_service import ChatService
from ..storage.duckdb_store import DuckDBStore
from .schemas import ChatRequest, ChatResponse

router = APIRouter(tags=["chat"])


@router.get("/chat/bootstrap", response_model=ChatResponse)
def chat_bootstrap(
    request: Request,
    session_id: str = Query(default="default", max_length=128),
) -> ChatResponse:
    service = _get_chat_service(request)
    return ChatResponse(**service.bootstrap(session_id=session_id))


@router.post("/chat/respond", response_model=ChatResponse)
def chat_respond(request: Request, payload: ChatRequest) -> ChatResponse:
    service = _get_chat_service(request)
    return ChatResponse(**service.respond(session_id=payload.session_id, query=payload.query, limit=payload.limit))


def _get_chat_service(request: Request) -> ChatService:
    service = getattr(request.app.state, "chat_service", None)
    if service is not None:
        return service

    store = getattr(request.app.state, "store", None)
    if store is None:
        raise HTTPException(status_code=500, detail="DuckDB store is not configured.")

    settings = getattr(request.app.state, "settings", None)
    if settings is None:
        raise HTTPException(status_code=500, detail="API settings are not configured.")

    service = ChatService(store, settings)
    request.app.state.chat_service = service
    return service


def _get_store(request: Request) -> DuckDBStore:
    store = getattr(request.app.state, "store", None)
    if store is None:
        raise HTTPException(status_code=500, detail="DuckDB store is not configured.")
    return store


