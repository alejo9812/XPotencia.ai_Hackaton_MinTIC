from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_epoch() -> float:
    return datetime.now(timezone.utc).timestamp()


@dataclass
class ChatSessionState:
    session_id: str
    created_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)
    last_active_epoch: float = field(default_factory=_now_epoch)
    last_query: str = ""
    last_intent: str = "project_overview"
    last_view_type: str = "project_overview"
    last_contract_id: str = ""
    last_supplier: str = ""
    last_entity: str = ""
    last_department: str = ""
    last_municipality: str = ""
    last_report_type: str = ""
    last_filters: dict[str, Any] = field(default_factory=dict)
    last_result_ids: list[str] = field(default_factory=list)
    followups: list[dict[str, Any]] = field(default_factory=list)

    def touch(self) -> None:
        self.updated_at = _now_iso()
        self.last_active_epoch = _now_epoch()

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["followups"] = [dict(item) for item in self.followups]
        payload["last_result_ids"] = [str(item) for item in self.last_result_ids if str(item).strip()]
        payload["last_filters"] = dict(self.last_filters)
        return payload


class ChatMemoryStore:
    def __init__(self, max_sessions: int = 200, ttl_hours: int = 12) -> None:
        self.max_sessions = max(1, int(max_sessions))
        self.ttl_seconds = max(1, int(ttl_hours)) * 60 * 60
        self._sessions: dict[str, ChatSessionState] = {}
        self._lock = Lock()

    def get(self, session_id: str) -> ChatSessionState:
        session_key = self._normalize_session_id(session_id)
        with self._lock:
            self._cleanup_locked()
            state = self._sessions.get(session_key)
            if state is None:
                state = ChatSessionState(session_id=session_key)
                self._sessions[session_key] = state
            return state

    def save(self, state: ChatSessionState) -> ChatSessionState:
        state.touch()
        session_key = self._normalize_session_id(state.session_id)
        with self._lock:
            self._cleanup_locked()
            self._sessions[session_key] = state
            self._trim_locked()
            return state

    def update(self, session_id: str, **patch: Any) -> ChatSessionState:
        state = self.get(session_id)
        for key, value in patch.items():
            if not hasattr(state, key):
                continue
            setattr(state, key, value)
        return self.save(state)

    def add_followup(self, session_id: str, followup: dict[str, Any]) -> ChatSessionState:
        state = self.get(session_id)
        state.followups = self._merge_followup(state.followups, followup)
        return self.save(state)

    def clear(self, session_id: str) -> None:
        session_key = self._normalize_session_id(session_id)
        with self._lock:
            self._sessions.pop(session_key, None)

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            self._cleanup_locked()
            return [state.to_dict() for state in self._sessions.values()]

    def _cleanup_locked(self) -> None:
        if not self._sessions:
            return

        now = _now_epoch()
        expired = [
            session_id
            for session_id, state in self._sessions.items()
            if now - float(getattr(state, "last_active_epoch", now)) > self.ttl_seconds
        ]
        for session_id in expired:
            self._sessions.pop(session_id, None)

        self._trim_locked()

    def _trim_locked(self) -> None:
        if len(self._sessions) <= self.max_sessions:
            return

        ordered = sorted(
            self._sessions.items(),
            key=lambda item: float(getattr(item[1], "last_active_epoch", 0.0)),
        )
        overflow = len(self._sessions) - self.max_sessions
        for session_id, _ in ordered[:overflow]:
            self._sessions.pop(session_id, None)

    def _normalize_session_id(self, session_id: str) -> str:
        value = str(session_id or "").strip()
        return value or "default"

    def _merge_followup(self, existing: list[dict[str, Any]], followup: dict[str, Any]) -> list[dict[str, Any]]:
        contract_id = str(followup.get("contract_id") or "").strip()
        merged = [dict(item) for item in existing if isinstance(item, dict)]
        if contract_id:
            merged = [item for item in merged if str(item.get("contract_id") or "").strip() != contract_id]
        merged.insert(0, dict(followup))
        return merged[:20]


GLOBAL_CHAT_MEMORY = ChatMemoryStore()

