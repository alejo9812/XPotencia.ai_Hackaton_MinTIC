from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def build_chat_response(
    *,
    session_id: str,
    intent: str,
    message: str,
    view_type: str,
    data: dict[str, Any],
    suggested_actions: list[str],
    limitations: str,
    session_state: dict[str, Any],
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "session_id": str(session_id or "default"),
        "intent": str(intent or "unknown_query"),
        "message": str(message or ""),
        "view_type": str(view_type or "unknown_query"),
        "data": data or {},
        "suggested_actions": _unique_strings(suggested_actions),
        "limitations": str(limitations or ""),
        "session_state": session_state or {},
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            **(metadata or {}),
        },
    }


def build_bootstrap_response(
    *,
    session_id: str,
    message: str,
    view_type: str,
    data: dict[str, Any],
    suggested_actions: list[str],
    limitations: str,
    session_state: dict[str, Any],
) -> dict[str, Any]:
    return build_chat_response(
        session_id=session_id,
        intent="project_overview",
        message=message,
        view_type=view_type,
        data=data,
        suggested_actions=suggested_actions,
        limitations=limitations,
        session_state=session_state,
        metadata={"bootstrap": True},
    )


def build_error_response(
    *,
    session_id: str,
    message: str,
    limitations: str,
    session_state: dict[str, Any],
    view_type: str = "unknown_query",
) -> dict[str, Any]:
    return build_chat_response(
        session_id=session_id,
        intent="unknown_query",
        message=message,
        view_type=view_type,
        data={},
        suggested_actions=[
            "Ver contratos con mayor riesgo",
            "Buscar contrato por ID",
            "Comparar proveedores",
            "Generar reporte",
        ],
        limitations=limitations,
        session_state=session_state,
        metadata={"error": True},
    )


def _unique_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result

