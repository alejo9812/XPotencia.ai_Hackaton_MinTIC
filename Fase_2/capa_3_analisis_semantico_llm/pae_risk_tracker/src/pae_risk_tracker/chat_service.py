from __future__ import annotations

import logging
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any

from .agent.orchestrator import run_agent_query
from .agent.tools import build_query_plan
from .chat_memory import GLOBAL_CHAT_MEMORY, ChatSessionState
from .chat_repository import ChatRepository, ChatViewResult
from .intent_classifier import ClassifiedIntent, classify_intent
from .response_builder import build_bootstrap_response, build_chat_response
from .storage.duckdb_store import DuckDBStore

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ChatResolution:
    result: ChatViewResult
    analysis: dict[str, Any] | None = None


class ChatService:
    def __init__(
        self,
        store: DuckDBStore,
        settings: Any,
        *,
        output_dir: Any | None = None,
        processed_dir: Any | None = None,
    ) -> None:
        self.store = store
        self.settings = settings
        self.repository = ChatRepository(
            store,
            output_dir=output_dir or getattr(settings, "output_dir", None),
            processed_dir=processed_dir or getattr(settings, "processed_dir", None),
        )

    def bootstrap(self, session_id: str = "default") -> dict[str, Any]:
        state = GLOBAL_CHAT_MEMORY.get(session_id)
        self.repository.refresh()
        result = self.repository.bootstrap(state.to_dict())
        session_state = self._commit_session_state(session_id, state, result, last_query="", intent="project_overview")
        return build_bootstrap_response(
            session_id=session_id,
            message=result.message,
            view_type=result.view_type,
            data=result.data,
            suggested_actions=result.suggested_actions,
            limitations=result.limitations,
            session_state=session_state,
        )

    def respond(self, session_id: str, query: str, limit: int = 10) -> dict[str, Any]:
        state = GLOBAL_CHAT_MEMORY.get(session_id)
        state_snapshot = state.to_dict()
        classification = classify_intent(query, state_snapshot)
        plan = build_query_plan(query)
        result = self._resolve_intent(query, limit, state, classification, plan)
        updated_state = self._commit_session_state(
            session_id,
            state,
            result.result,
            last_query=query,
            intent=classification.intent,
            classification=classification,
            plan=plan,
            analysis=result.analysis,
        )
        metadata = {
            "confidence": classification.confidence,
            "depth": classification.depth,
            "report_type": classification.report_type,
            "comparison_mode": classification.comparison_mode,
            "analysis_mode": result.analysis.get("analysis_mode") if result.analysis else classification.depth,
            "plan": plan,
        }
        return build_chat_response(
            session_id=session_id,
            intent=classification.intent,
            message=result.result.message,
            view_type=result.result.view_type,
            data=result.result.data,
            suggested_actions=result.result.suggested_actions,
            limitations=result.result.limitations,
            session_state=updated_state,
            metadata=metadata,
        )

    def _resolve_intent(
        self,
        query: str,
        limit: int,
        session_state: ChatSessionState,
        classification: ClassifiedIntent,
        plan: dict[str, Any],
    ) -> ChatResolution:
        normalized_limit = max(1, min(int(limit), 20))
        session_snapshot = session_state.to_dict()
        filters = self._build_filters(query, plan, session_snapshot)
        intent = classification.intent

        if intent == "project_overview":
            return ChatResolution(self.repository.build_project_overview(session_state=session_snapshot))

        if intent == "dashboard_summary":
            return ChatResolution(self.repository.build_dashboard_summary(filters=filters))

        if intent in {"contract_search", "top_risk_contracts"}:
            if intent == "top_risk_contracts":
                result = self.repository.build_search_view(
                    query=query,
                    filters=filters,
                    limit=normalized_limit,
                    title="Contratos con mayor riesgo preliminar",
                )
            else:
                result = self.repository.build_search_view(
                    query=query,
                    filters=filters,
                    limit=normalized_limit,
                    title="Resultados de búsqueda",
                )
            return ChatResolution(result)

        if intent in {"supplier_comparison", "entity_comparison", "region_summary"}:
            mode = {
                "supplier_comparison": "supplier",
                "entity_comparison": "entity",
                "region_summary": "region",
            }[intent]
            return ChatResolution(self.repository.build_comparison(mode=mode, filters=filters, limit=min(normalized_limit, 8)))

        if intent in {"followup_list", "followup_creation"}:
            contract = self._resolve_target_contract(classification, session_snapshot)
            if intent == "followup_creation" and contract is None:
                return ChatResolution(
                    self.repository.build_missing_contract_response(
                        view_type="followup_panel",
                        intent=intent,
                        message=(
                            "Para crear un seguimiento necesito un ID de contrato o un contrato previamente consultado."
                        ),
                    )
                )
            if intent == "followup_creation" and contract is not None:
                self.repository.refresh()
                followup_entry = {
                    "contract_id": contract.get("contract_id"),
                    "entity": contract.get("entity"),
                    "supplier": contract.get("supplier"),
                    "department": contract.get("department"),
                    "municipality": contract.get("municipality"),
                    "status": "Pendiente",
                    "notes": self._followup_notes(query, contract),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "next_action": contract.get("recommended_action") or "Revisar soportes, red flags y trazabilidad.",
                }
                GLOBAL_CHAT_MEMORY.add_followup(session_state.session_id, followup_entry)
            refreshed_state = GLOBAL_CHAT_MEMORY.get(session_state.session_id).to_dict()
            contract_for_panel = contract or self._resolve_target_contract(classification, refreshed_state)
            contract_payload = self.repository.find_contract(contract_for_panel.get("contract_id")) if contract_for_panel else None
            return ChatResolution(
                self.repository.build_followup_panel(refreshed_state, contract=contract_payload),
            )

        if intent in {"contract_detail", "contract_risk_analysis", "red_flags_explanation", "report_generation"}:
            contract = self._resolve_target_contract(classification, session_snapshot)
            if contract is None and intent != "report_generation":
                return ChatResolution(
                    self.repository.build_missing_contract_response(
                        view_type="contract_detail" if intent != "red_flags_explanation" else "red_flags_panel",
                        intent=intent,
                        message="Necesito un ID de contrato para profundizar en el análisis.",
                    )
                )

            analysis = self._maybe_run_deep_analysis(query, classification, contract, limit) if contract else None

            if intent == "report_generation":
                report_type = classification.report_type or session_snapshot.get("last_report_type") or "executive"
                if contract is None:
                    contract = self._resolve_contract_from_history(session_snapshot)
                if contract is None:
                    return ChatResolution(
                        self.repository.build_missing_contract_response(
                            view_type="report_preview",
                            intent=intent,
                            message=(
                                "Para generar un reporte necesito un contrato o un conjunto de filtros previamente consultados."
                            ),
                        )
                    )
                if not analysis:
                    analysis = self._maybe_run_deep_analysis(query, classification, contract, limit)
                return ChatResolution(
                    self.repository.build_report_preview(
                        report_type=report_type,
                        contract=contract,
                        analysis=analysis or {},
                    ),
                    analysis or {},
                )

            if intent == "red_flags_explanation" and contract is None:
                return ChatResolution(self.repository.build_red_flags_panel(contract=None, analysis=analysis or {}))
            if intent == "red_flags_explanation" and contract is not None:
                return ChatResolution(self.repository.build_red_flags_panel(contract=contract, analysis=analysis or {}), analysis or {})

            if contract is None:
                return ChatResolution(
                    self.repository.build_missing_contract_response(
                        view_type="contract_detail",
                        intent=intent,
                        message="Necesito un ID de contrato para mostrar su detalle.",
                    )
                )

            return ChatResolution(
                self.repository.build_contract_detail(contract, analysis=analysis or {}, session_state=session_snapshot),
                analysis or {},
            )

        if intent == "unknown_query":
            return ChatResolution(
                self.repository.build_project_overview(session_state=session_snapshot),
            )

        return ChatResolution(
            self.repository.build_project_overview(session_state=session_snapshot),
        )

    def _maybe_run_deep_analysis(
        self,
        query: str,
        classification: ClassifiedIntent,
        contract: dict[str, Any] | None,
        limit: int,
    ) -> dict[str, Any]:
        if contract is None:
            return {}
        if classification.depth != "deep" and classification.intent != "report_generation":
            return {}

        prompt = self._deep_prompt(query, classification, contract)
        try:
            result = run_agent_query(
                self.store,
                prompt,
                limit=max(3, min(limit, 8)),
                processed_dir=getattr(self.settings, "processed_dir", None),
            )
        except Exception:  # pragma: no cover - defensive fallback
            logger.exception("Failed to run deep analysis for chat query.")
            return {}

        payload = result.to_dict()
        analysis = dict(payload.get("analysis") or {})
        return {
            "summary": analysis.get("summary") or "",
            "explanation": analysis.get("explanation") or "",
            "recommendations": analysis.get("recommendations") or [],
            "audit_questions": analysis.get("audit_questions") or [],
            "graph_suggestions": analysis.get("graph_suggestions") or [],
            "rows": payload.get("rows") or [],
            "evidence_rows": payload.get("evidence_rows") or [],
            "validation": payload.get("validation") or {},
            "plan": payload.get("plan") or {},
            "analysis_mode": payload.get("llm_mode") or "mock",
            "analysis_model": payload.get("llm_model") or "mock",
        }

    def _deep_prompt(self, query: str, classification: ClassifiedIntent, contract: dict[str, Any]) -> str:
        contract_id = str(contract.get("contract_id") or "").strip()
        entity = str(contract.get("entity") or "").strip()
        supplier = str(contract.get("supplier") or "").strip()
        report_hint = classification.report_type or "executivo"
        if classification.intent == "report_generation":
            return (
                f"Genera un reporte {report_hint} del contrato {contract_id} de la entidad {entity} con proveedor {supplier}. "
                f"Prioriza evidencia, limitaciones, red flags y recomendaciones. Consulta: {query}"
            )
        return (
            f"Explica por qué el contrato {contract_id} de la entidad {entity} con proveedor {supplier} es riesgoso. "
            f"Resume red flags, evidencia y limitaciones. Consulta: {query}"
        )

    def _resolve_target_contract(
        self,
        classification: ClassifiedIntent,
        session_snapshot: dict[str, Any],
    ) -> dict[str, Any] | None:
        contract_id = str(classification.contract_id or session_snapshot.get("last_contract_id") or "").strip()
        if not contract_id and session_snapshot.get("last_result_ids"):
            first = str((session_snapshot.get("last_result_ids") or [None])[0] or "").strip()
            contract_id = first
        if not contract_id and classification.intent == "followup_creation":
            contract_id = str(session_snapshot.get("last_contract_id") or "").strip()
        if not contract_id and classification.intent == "report_generation":
            contract_id = str(session_snapshot.get("last_contract_id") or "").strip()
        if not contract_id:
            return None
        contract = self.repository.find_contract(contract_id)
        return contract

    def _resolve_contract_from_history(self, session_snapshot: dict[str, Any]) -> dict[str, Any] | None:
        contract_id = str(session_snapshot.get("last_contract_id") or "").strip()
        if not contract_id:
            result_ids = session_snapshot.get("last_result_ids") or []
            contract_id = str(result_ids[0]).strip() if result_ids else ""
        if not contract_id:
            return None
        return self.repository.find_contract(contract_id)

    def _build_filters(self, query: str, plan: dict[str, Any], session_snapshot: dict[str, Any]) -> dict[str, Any]:
        filters: dict[str, Any] = {
            "query": query,
            "department": plan.get("department") or session_snapshot.get("last_department"),
            "municipality": plan.get("municipality") or session_snapshot.get("last_municipality"),
            "modality": plan.get("modality"),
            "status": plan.get("state"),
            "min_amount": plan.get("min_amount"),
            "max_amount": plan.get("max_amount"),
            "date_from": plan.get("date_from"),
            "date_to": plan.get("date_to"),
        }
        if session_snapshot.get("last_supplier"):
            filters["supplier"] = session_snapshot.get("last_supplier")
        if session_snapshot.get("last_entity"):
            filters["entity"] = session_snapshot.get("last_entity")
        if session_snapshot.get("last_contract_id") and not plan.get("department"):
            filters["contract_id"] = session_snapshot.get("last_contract_id")
        return {key: value for key, value in filters.items() if value not in (None, "", [], {}, "all")}

    def _followup_notes(self, query: str, contract: dict[str, Any]) -> str:
        if query.strip():
            return query.strip()
        recommended = contract.get("recommended_action") or contract.get("audit_recommendation")
        if recommended:
            return str(recommended)
        return "Seguimiento creado por el asistente."

    def _unique_preserve_order(self, values: list[str]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for value in values:
            text = str(value or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            ordered.append(text)
        return ordered

    def _commit_session_state(
        self,
        session_id: str,
        state: ChatSessionState,
        result: ChatViewResult,
        *,
        last_query: str,
        intent: str,
        classification: ClassifiedIntent | None = None,
        plan: dict[str, Any] | None = None,
        analysis: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        patch: dict[str, Any] = {
            "last_query": last_query,
            "last_intent": intent,
            "last_view_type": result.view_type,
        }
        context = dict(result.context or {})
        if context.get("last_contract_id"):
            patch["last_contract_id"] = context.get("last_contract_id")
        if context.get("last_supplier"):
            patch["last_supplier"] = context.get("last_supplier")
        if context.get("last_entity"):
            patch["last_entity"] = context.get("last_entity")
        if context.get("last_department"):
            patch["last_department"] = context.get("last_department")
        if context.get("last_municipality"):
            patch["last_municipality"] = context.get("last_municipality")
        if context.get("last_report_type"):
            patch["last_report_type"] = context.get("last_report_type")
        if context.get("last_filters"):
            patch["last_filters"] = context.get("last_filters")
        if context.get("last_result_ids"):
            patch["last_result_ids"] = self._unique_preserve_order([str(item).strip() for item in context.get("last_result_ids") or [] if str(item).strip()])

        if result.view_type == "followup_panel" and result.data.get("followups") is not None:
            patch["followups"] = result.data.get("followups")

        if classification and classification.report_type:
            patch["last_report_type"] = classification.report_type

        updated = GLOBAL_CHAT_MEMORY.update(session_id, **{key: value for key, value in patch.items() if value not in (None, "", [], {}, "all")})
        return updated.to_dict()
