from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from ..chat_service import ChatService
from ..paths import ensure_runtime_dirs
from ..storage.duckdb_store import DuckDBStore
from .routes_agent import router as agent_router
from .routes_chat import router as chat_router
from .routes_contracts import router as contracts_router
from .routes_diagnostics import router as diagnostics_router
from .routes_search import router as search_router
from .routes_validation import router as validation_router
from .settings import load_api_settings


def create_app(*, store: DuckDBStore | None = None, settings=None, chat_service: ChatService | None = None) -> FastAPI:
    ensure_runtime_dirs()
    settings = settings or load_api_settings()
    app = FastAPI(
        title="PAE Risk Tracker API",
        version="0.1.0",
        description="API interna para consultar contratos SECOP II y señales de riesgo del PAE.",
    )
    app.state.settings = settings
    app.state.store = store or DuckDBStore(settings.duckdb_path)
    app.state.chat_service = chat_service or ChatService(app.state.store, settings)
    app.include_router(contracts_router)
    app.include_router(search_router)
    app.include_router(validation_router)
    app.include_router(diagnostics_router)
    app.include_router(agent_router)
    app.include_router(chat_router)
    return app


app = create_app()


@app.get("/", include_in_schema=False)
def root() -> JSONResponse:
    return JSONResponse(
        {
            "status": "ok",
            "name": "PAE Risk Tracker API",
            "docs": "/docs",
            "health": "/health",
        }
    )

