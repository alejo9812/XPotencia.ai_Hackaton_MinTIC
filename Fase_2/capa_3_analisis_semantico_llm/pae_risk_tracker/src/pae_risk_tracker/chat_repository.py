from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from .api.routes_contracts import (
    _base_table,
    _canonicalize_contract_row,
    _canonicalize_risk_payload,
    _ensure_scored_frame,
    _fetch_contract_row,
    _load_context_tables,
)
from .config import load_risk_registry, load_scoring_registry, normalize_text
from .paths import OUTPUT_DIR, PROCESSED_DIR
from .risk.scoring import score_contracts_frame
from .storage.duckdb_store import DuckDBStore


@dataclass(frozen=True)
class ChatViewResult:
    view_type: str
    message: str
    data: dict[str, Any]
    suggested_actions: list[str]
    limitations: str
    context: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["data"] = self.data or {}
        payload["suggested_actions"] = [str(item) for item in self.suggested_actions if str(item).strip()]
        payload["limitations"] = str(self.limitations or "")
        payload["context"] = dict(self.context or {})
        return payload


class ChatRepository:
    def __init__(
        self,
        store: DuckDBStore,
        *,
        output_dir: Path | None = None,
        processed_dir: Path | None = None,
    ) -> None:
        self.store = store
        self.output_dir = Path(output_dir or OUTPUT_DIR)
        self.processed_dir = Path(processed_dir or PROCESSED_DIR)
        self._signature = ""
        self._loaded_at = ""
        self._records: list[dict[str, Any]] = []
        self._records_by_id: dict[str, dict[str, Any]] = {}
        self._audit_cards: dict[str, dict[str, Any]] = {}
        self._summary_payload: dict[str, Any] = {}
        self._top_k_payload: list[dict[str, Any]] = []
        self._risk_catalog = self._load_risk_catalog()
        self._scoring_registry = self._load_scoring_registry()
        self.refresh(force=True)

    @property
    def records(self) -> list[dict[str, Any]]:
        self.refresh()
        return list(self._records)

    @property
    def summary(self) -> dict[str, Any]:
        self.refresh()
        return dict(self._summary_payload)

    @property
    def top_k(self) -> list[dict[str, Any]]:
        self.refresh()
        return list(self._top_k_payload)

    @property
    def source_label(self) -> str:
        return "SECOP II preprocesado (tracker PAE)"

    def refresh(self, force: bool = False) -> None:
        signature = self._build_signature()
        if not force and signature == self._signature and self._records:
            return

        audit_cards = self._load_audit_cards()
        raw_records = self._load_raw_records()
        normalized_records = [self._merge_audit_card(record, audit_cards) for record in raw_records]
        normalized_records = [record for record in normalized_records if record.get("contract_id")]
        normalized_records.sort(key=self._record_sort_key, reverse=True)

        summary_payload, top_k_payload = self._load_summary_payload(normalized_records)

        self._signature = signature
        self._loaded_at = datetime.now(timezone.utc).isoformat()
        self._records = normalized_records
        self._records_by_id = {
            self._normalize_key(record.get("contract_id")): record
            for record in normalized_records
            if self._normalize_key(record.get("contract_id"))
        }
        self._audit_cards = audit_cards
        self._summary_payload = summary_payload
        self._top_k_payload = top_k_payload

    def bootstrap(self, session_state: dict[str, Any] | None = None) -> ChatViewResult:
        state = session_state or {}
        top_records = self.top_risk_contracts(limit=5, filters=state.get("last_filters") or {})
        metrics = self._build_metrics(self.records)
        decision_support = self._build_decision_support(mode="overview", records=self.records, rows=top_records)
        data = {
            "project_name": "Agente de IA para Detección de Opacidad en Contratos Públicos PAE",
            "author": "Alejandro Montes",
            "source_label": self.source_label,
            "loaded_at": self._loaded_at,
            "metrics": metrics,
            "risk_distribution": self._build_risk_distribution(self.records),
            "top_flags": self._build_top_flags(self.records),
            "top_contracts": top_records,
            "decision_support": decision_support,
            "quick_actions": self._default_quick_actions(),
            "methodology": [
                "Primero se consulta la base estructurada y los indicadores precalculados.",
                "Luego se revisa la caché de contratos, red flags y resúmenes agregados.",
                "Solo si hace falta se profundiza con búsqueda semántica o LLM.",
                "El análisis es preliminar y no sustituye la revisión documental humana.",
            ],
            "data_sources": [
                "Contratos y scores precalculados del tracker PAE.",
                "Fichas de auditoría y red flags calculadas.",
                "Índices por contrato, proveedor, entidad y territorio.",
            ],
            "warnings": [
                "La información es de Alejandro Montes para la fase 2 del prototipo.",
                "No se deben inventar datos cuando la evidencia no esté disponible.",
                "El resultado prioriza revisión, no emite juicios legales.",
            ],
        }
        return ChatViewResult(
            view_type="project_overview",
            message=(
                "Hola. Soy el asistente de análisis de contratos públicos PAE. Puedo ayudarte a consultar "
                "contratos, detectar señales preliminares de opacidad, generar reportes y hacer seguimiento "
                "a contratos específicos. La información del proyecto es de Alejandro Montes. ¿Qué deseas analizar?"
            ),
            data=data,
            suggested_actions=[
                "Ver contratos con mayor riesgo",
                "Buscar contrato por ID",
                "Comparar proveedores",
                "Generar reporte",
                "Ver red flags",
                "Crear seguimiento",
                "Ver resumen del dashboard",
            ],
            limitations=self._default_limitations(),
            context={
                "last_view_type": "project_overview",
                "last_intent": "project_overview",
                "last_result_ids": [record["contract_id"] for record in top_records],
            },
        )

    def search_contracts(self, query: str, limit: int, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self.refresh()
        filters = filters or {}
        normalized_query = self._normalize_query(query)
        candidates = self._filtered_records(filters)

        if not normalized_query:
            scored = candidates
        else:
            scored = sorted(
                (
                    (self._score_query_match(record, normalized_query), record)
                    for record in candidates
                ),
                key=lambda item: (item[0], item[1].get("risk_score", 0), item[1].get("final_value", 0)),
                reverse=True,
            )
            scored = [record for score, record in scored if score > 0]

        if not scored and normalized_query:
            exact = [record for record in candidates if self._normalize_key(record.get("contract_id")) == normalized_query]
            if exact:
                scored = exact

        if not scored:
            scored = candidates

        return [self._build_contract_summary(record) for record in scored[: max(1, int(limit))]]

    def top_risk_contracts(self, limit: int, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self.refresh()
        filtered = self._filtered_records(filters or {})
        ordered = sorted(filtered, key=self._record_sort_key, reverse=True)
        return [self._build_contract_summary(record) for record in ordered[: max(1, int(limit))]]

    def find_contract(self, contract_id: str) -> dict[str, Any] | None:
        self.refresh()
        contract_key = self._normalize_key(contract_id)
        if not contract_key:
            return None

        record = self._records_by_id.get(contract_key)
        if record is not None:
            return dict(record)

        source_table = _base_table(self.store)
        if source_table is None:
            return None

        row = _fetch_contract_row(self.store, source_table, contract_id)
        if row is None and source_table == "pae_contracts_scored" and self.store.has_table("pae_contracts_core"):
            row = _fetch_contract_row(self.store, "pae_contracts_core", contract_id)
            source_table = "pae_contracts_core"
        if row is None:
            return None

        if source_table == "pae_contracts_scored":
            return self._merge_audit_card(_canonicalize_contract_row(row), self._audit_cards)

        frame = pd.DataFrame([row])
        scored, _ = score_contracts_frame(frame, external_tables=_load_context_tables(self.store))
        canonical = _canonicalize_contract_row(scored.iloc[0].to_dict())
        return self._merge_audit_card(canonical, self._audit_cards)

    def build_contract_detail(
        self,
        contract: dict[str, Any],
        *,
        analysis: dict[str, Any] | None = None,
        session_state: dict[str, Any] | None = None,
    ) -> ChatViewResult:
        session_state = session_state or {}
        score = int(contract.get("risk_score") or 0)
        level = self._normalize_risk_level(contract.get("risk_level"), score)
        red_flags = self._red_flags_for_contract(contract)
        peer_summary = self._build_peer_summary(contract)
        analysis_payload = analysis or {}
        decision_support = self._build_decision_support(mode="contract_detail", contract=contract, records=[contract] + peer_summary.get("peer_top_contracts", []), peer_summary=peer_summary, analysis=analysis_payload)
        data = {
            "contract": self._build_contract_detail_payload(contract),
            "peer_summary": peer_summary,
            "analysis": analysis_payload,
            "red_flags": red_flags,
            "evidence": contract.get("evidence") or contract.get("flag_evidence") or {},
            "limitations": self._contract_limitations(contract),
            "actions": self._contract_actions(contract),
            "summary_line": self._build_summary_line(contract),
            "analysis_mode": analysis_payload.get("analysis_mode", "quick"),
            "decision_support": decision_support,
        }
        message = analysis_payload.get("summary") or (
            f"Encontré el contrato {contract.get('contract_id')} con score {score}/100 ({level}). "
            "Abajo verás su ficha, las señales que justifican priorizarlo y qué conviene profundizar primero."
        )
        context = self._context_for_contract(contract, session_state=session_state)
        return ChatViewResult(
            view_type="contract_detail",
            message=message,
            data=data,
            suggested_actions=[
                "Generar reporte ejecutivo",
                "Ver red flags",
                "Crear seguimiento",
                "Comparar proveedores",
            ],
            limitations=self._default_limitations(),
            context=context,
        )

    def build_red_flags_panel(
        self,
        contract: dict[str, Any] | None = None,
        *,
        analysis: dict[str, Any] | None = None,
    ) -> ChatViewResult:
        if contract:
            flags = self._red_flags_for_contract(contract)
            decision_support = self._build_decision_support(mode="red_flags", contract=contract, records=[contract], analysis=analysis or {})
            data = {
                "scope": "contract",
                "contract": self._build_contract_summary(contract),
                "flags": flags,
                "analysis": analysis or {},
                "decision_support": decision_support,
                "headline": (
                    f"Se encontraron {len(flags)} señales de alerta en el contrato {contract.get('contract_id')}. "
                    "Esto sirve para priorizar revisión, no para cerrar conclusiones."
                    if flags
                    else f"No encontré red flags activas en el contrato {contract.get('contract_id')}. "
                    "Aun así, conviene contrastarlo con pares y documentación de soporte."
                ),
            }
            context = self._context_for_contract(contract)
            return ChatViewResult(
                view_type="red_flags_panel",
                message=data["headline"],
                data=data,
                suggested_actions=[
                    "Generar reporte ejecutivo",
                    "Crear seguimiento",
                    "Ver contratos similares",
                ],
                limitations=self._contract_limitations(contract),
                context=context,
            )

        top_flags = self._build_top_flags(self.records)
        decision_support = self._build_decision_support(mode="red_flags", rows=top_flags, records=self.records)
        data = {
            "scope": "global",
            "flags": top_flags,
            "headline": "Estas son las señales de alerta más frecuentes en el universo PAE precalculado. "
            "Úsalas para decidir dónde profundizar primero.",
            "analysis": analysis or {},
            "decision_support": decision_support,
        }
        return ChatViewResult(
            view_type="red_flags_panel",
            message=data["headline"],
            data=data,
            suggested_actions=[
                "Ver contratos con mayor riesgo",
                "Comparar proveedores",
                "Ver resumen del dashboard",
            ],
            limitations=self._default_limitations(),
            context={},
        )

    def build_comparison(
        self,
        *,
        mode: str,
        filters: dict[str, Any] | None = None,
        limit: int = 8,
    ) -> ChatViewResult:
        self.refresh()
        filters = filters or {}
        records = self._filtered_records(filters)
        grouped = self._group_records(records, mode=mode)
        rows = sorted(
            (
                {
                    "name": name,
                    "contract_count": len(items),
                    "total_value": self._sum_amount(items),
                    "average_risk": self._mean_score(items),
                    "red_flag_count": self._sum_red_flags(items),
                    "department_count": len({self._normalize_text(item.get("department")) for item in items if item.get("department")}),
                    "municipality_count": len({self._normalize_text(item.get("municipality")) for item in items if item.get("municipality")}),
                    "entities": self._unique_values(items, "entity"),
                    "suppliers": self._unique_values(items, "supplier"),
                    "top_contracts": [self._build_contract_summary(item) for item in sorted(items, key=self._record_sort_key, reverse=True)[:3]],
                    "top_flags": self._top_flags_for_group(items),
                }
                for name, items in grouped.items()
            ),
            key=lambda item: (item["contract_count"], item["total_value"], item["average_risk"]),
            reverse=True,
        )[: max(1, int(limit))]

        decision_support = self._build_decision_support(mode="comparison", rows=rows, records=records)
        display_mode = {
            "supplier": "supplier_comparison",
            "entity": "entity_comparison",
            "region": "region_summary",
        }.get(mode, "supplier_comparison")
        label = {
            "supplier": "proveedores",
            "entity": "entidades",
            "region": "territorios",
        }.get(mode, "proveedores")
        message = f"Preparé una comparación de {label} con los registros precalculados del tracker PAE."
        data = {
            "mode": mode,
            "rows": rows,
            "filters": self._clean_filters(filters),
            "chart": [
                {
                    "label": row["name"],
                    "value": row["average_risk"],
                    "subtitle": f"{row['contract_count']} contratos · {row['red_flag_count']} red flags",
                    "tone": self._tone_for_score(row["average_risk"]),
                }
                for row in rows
            ],
            "headline": message,
            "decision_support": decision_support,
        }
        return ChatViewResult(
            view_type=display_mode,
            message=message,
            data=data,
            suggested_actions=[
                "Ver contratos con mayor riesgo",
                "Generar reporte ejecutivo",
                "Ver red flags",
            ],
            limitations=self._default_limitations(),
            context={"last_filters": self._clean_filters(filters)},
        )

    def build_dashboard_summary(self, filters: dict[str, Any] | None = None) -> ChatViewResult:
        self.refresh()
        filters = filters or {}
        records = self._filtered_records(filters)
        metrics = self._build_metrics(records)
        decision_support = self._build_decision_support(mode="dashboard", records=records)
        data = {
            "metrics": metrics,
            "risk_distribution": self._build_risk_distribution(records),
            "top_flags": self._build_top_flags(records),
            "top_contracts": [self._build_contract_summary(record) for record in sorted(records, key=self._record_sort_key, reverse=True)[:8]],
            "top_suppliers": self._comparison_rows(records, "supplier", limit=5),
            "top_entities": self._comparison_rows(records, "entity", limit=5),
            "top_territories": self._comparison_rows(records, "region", limit=5),
            "filters": self._clean_filters(filters),
            "interpretation": self._build_dashboard_interpretation(metrics),
            "decision_support": decision_support,
        }
        return ChatViewResult(
            view_type="dashboard_summary",
            message="Resumen ejecutivo del dashboard con métricas, riesgos y señales agregadas para identificar patrones y priorizar revisión.",
            data=data,
            suggested_actions=[
                "Ver contratos con mayor riesgo",
                "Comparar proveedores",
                "Ver red flags",
                "Generar reporte",
            ],
            limitations=self._default_limitations(),
            context={"last_filters": self._clean_filters(filters)},
        )

    def build_project_overview(self, session_state: dict[str, Any] | None = None) -> ChatViewResult:
        return self.bootstrap(session_state=session_state)

    def build_report_preview(
        self,
        *,
        report_type: str,
        contract: dict[str, Any] | None = None,
        comparison_rows: list[dict[str, Any]] | None = None,
        analysis: dict[str, Any] | None = None,
    ) -> ChatViewResult:
        report_type = self._normalize_report_type(report_type)
        analysis = analysis or {}
        comparison_rows = [row for row in (comparison_rows or []) if isinstance(row, dict)]
        if contract is None and not comparison_rows:
            top_contract = self.top_risk_contracts(limit=1)
            contract = self.find_contract(top_contract[0]["contract_id"]) if top_contract else None

        if contract is None and not comparison_rows:
            message = "No tengo suficiente contexto para construir el reporte. Comparte un contrato, proveedor o entidad."
            return ChatViewResult(
                view_type="report_preview",
                message=message,
                data={
                    "report_type": report_type,
                    "missing_context": True,
                    "sections": [],
                    "export": {"available": False, "formats": []},
                },
                suggested_actions=[
                    "Analizar contrato por ID",
                    "Ver contratos con mayor riesgo",
                    "Comparar proveedores",
                ],
                limitations=self._default_limitations(),
                context={},
            )

        if contract is not None:
            sections = self._build_contract_report_sections(contract, report_type=report_type, analysis=analysis)
            scope_label = f"Contrato {contract.get('contract_id')}"
            context = self._context_for_contract(contract)
            result_ids = [str(contract.get("contract_id") or "").strip()]
        else:
            sections = self._build_comparison_report_sections(comparison_rows or [], report_type=report_type, analysis=analysis)
            scope_label = "Comparación agregada"
            context = {"last_report_type": report_type}
            result_ids = []

        decision_support = self._build_decision_support(
            mode="report",
            contract=contract,
            rows=comparison_rows,
            records=([contract] if contract else []) + comparison_rows,
            analysis=analysis,
        )

        export_markdown = self._build_markdown_report(
            report_type=report_type,
            scope_label=scope_label,
            sections=sections,
            analysis=analysis,
        )
        data = {
            "report_type": report_type,
            "scope_label": scope_label,
            "title": self._report_title(report_type, scope_label),
            "summary": analysis.get("summary") or self._report_summary(contract, report_type),
            "sections": sections,
            "highlights": self._report_highlights(contract, comparison_rows or [], analysis),
            "export": {
                "available": True,
                "formats": ["markdown", "txt"],
                "filename": self._report_filename(report_type, scope_label),
                "markdown": export_markdown,
            },
            "analysis": analysis,
            "decision_support": decision_support,
        }
        return ChatViewResult(
            view_type="report_preview",
            message=data["summary"] or f"Te preparo un reporte {report_type} preliminar para justificar la alerta y decidir dónde profundizar.",
            data=data,
            suggested_actions=[
                "Crear seguimiento",
                "Ver red flags",
                "Generar versión técnica",
                "Volver al contrato",
            ],
            limitations=self._contract_limitations(contract) if contract else self._default_limitations(),
            context=context | {"last_report_type": report_type, "last_result_ids": result_ids},
        )

    def build_followup_panel(
        self,
        session_state: dict[str, Any],
        *,
        contract: dict[str, Any] | None = None,
    ) -> ChatViewResult:
        followups = list(session_state.get("followups") or [])
        active_contract = contract or (self.find_contract(str(session_state.get("last_contract_id") or "")) if session_state.get("last_contract_id") else None)
        if active_contract and not followups:
            followups = [
                {
                    "contract_id": active_contract.get("contract_id"),
                    "entity": active_contract.get("entity"),
                    "supplier": active_contract.get("supplier"),
                    "status": "Pendiente",
                    "notes": "Seguimiento sugerido por el asistente.",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "next_action": active_contract.get("recommended_action") or "Revisar soportes y red flags.",
                }
            ]

        summary = {
            "total": len(followups),
            "open": len([item for item in followups if str(item.get("status") or "").lower() in {"pendiente", "open", "abierto"}]),
            "closed": len([item for item in followups if str(item.get("status") or "").lower() in {"cerrado", "closed"}]),
        }
        data = {
            "followups": followups,
            "summary": summary,
            "active_contract": self._build_contract_summary(active_contract) if active_contract else None,
            "headline": "Seguimientos disponibles en esta sesión.",
            "history": followups,
            "next_action": self._suggest_followup_action(active_contract, followups),
        }
        message = (
            f"Tienes {summary['total']} contratos marcados para seguimiento en esta sesión."
            if summary["total"]
            else "No hay contratos marcados para seguimiento todavía."
        )
        return ChatViewResult(
            view_type="followup_panel",
            message=message,
            data=data,
            suggested_actions=[
                "Crear seguimiento",
                "Generar reporte ejecutivo",
                "Ver contrato reciente",
            ],
            limitations=self._default_limitations(),
            context=self._context_for_contract(active_contract) if active_contract else {},
        )

    def build_search_view(
        self,
        *,
        query: str,
        filters: dict[str, Any] | None = None,
        limit: int = 10,
        title: str = "Contratos encontrados",
    ) -> ChatViewResult:
        filters = filters or {}
        matches = self.search_contracts(query, limit=limit, filters=filters)
        decision_support = self._build_decision_support(mode="search", rows=matches, records=matches)
        data = {
            "query": query,
            "filters": self._clean_filters(filters),
            "contracts": matches,
            "columns": ["ID", "Entidad", "Proveedor", "Valor", "Región", "Score", "Red flags"],
            "total": len(matches),
            "title": title,
            "decision_support": decision_support,
        }
        message = (
            "Encontré los contratos PAE con mayor coincidencia para tu búsqueda. "
            "El orden te ayuda a decidir qué revisar primero."
            if matches
            else "No encontré contratos con esos filtros. Prueba con otro ID, entidad, proveedor o territorio."
        )
        context = self._context_from_results(matches, filters)
        return ChatViewResult(
            view_type="risk_contracts_table",
            message=message,
            data=data,
            suggested_actions=[
                "Ver detalle del contrato",
                "Generar reporte ejecutivo",
                "Crear seguimiento",
                "Comparar proveedores",
            ],
            limitations=self._default_limitations(),
            context=context,
        )

    def build_missing_contract_response(
        self,
        *,
        view_type: str,
        intent: str,
        message: str,
    ) -> ChatViewResult:
        return ChatViewResult(
            view_type=view_type,
            message=message,
            data={
                "missing_fields": ["contract_id"],
                "hint": "Puedes probar con un ID como CO1.PCCNTR.123 o buscar por proveedor, entidad o municipio.",
            },
            suggested_actions=[
                "Buscar contrato por ID",
                "Ver contratos con mayor riesgo",
                "Comparar proveedores",
            ],
            limitations=self._default_limitations(),
            context={"last_intent": intent},
        )

    def build_not_found_response(
        self,
        *,
        view_type: str,
        intent: str,
        contract_id: str,
    ) -> ChatViewResult:
        return ChatViewResult(
            view_type=view_type,
            message=(
                f"No encontré un contrato con el ID {contract_id}. "
                "Puedes intentar buscar por proveedor, entidad contratante, municipio o palabra clave."
            ),
            data={
                "query": contract_id,
                "not_found": True,
                "contract_id": contract_id,
            },
            suggested_actions=[
                "Buscar contrato por ID",
                "Ver contratos con mayor riesgo",
                "Comparar proveedores",
                "Ver resumen del dashboard",
            ],
            limitations=self._default_limitations(),
            context={"last_intent": intent},
        )

    def _load_raw_records(self) -> list[dict[str, Any]]:
        output_records = self._load_json_list(self.output_dir / "pae_risk_ranking.json")
        if output_records:
            return [self._coerce_contract_record(record) for record in output_records]
        return self._load_store_records()

    def _load_store_records(self) -> list[dict[str, Any]]:
        source_table = _base_table(self.store)
        if source_table is None:
            return []

        if source_table == "pae_contracts_scored":
            frame = self.store.read_frame("SELECT * FROM pae_contracts_scored")
        else:
            frame = _ensure_scored_frame(self.store, source_table)

        if frame.empty:
            return []

        records = [self._coerce_contract_record(row) for row in frame.to_dict(orient="records")]
        return records

    def _load_audit_cards(self) -> dict[str, dict[str, Any]]:
        cards = self._load_json_list(self.output_dir / "pae_audit_cards.json")
        result: dict[str, dict[str, Any]] = {}
        for card in cards:
            contract_id = self._normalize_key(card.get("contract_id"))
            if contract_id:
                result[contract_id] = dict(card)
        return result

    def _load_summary_payload(self, records: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        summary_json = self._load_json_object(self.output_dir / "pae_risk_scores.json")
        if summary_json:
            summary = dict(summary_json.get("summary") or {})
            top_k = [self._coerce_contract_record(item) for item in summary_json.get("top_k") or []]
            if summary:
                return self._normalize_summary(summary, records), top_k

        summary = self._normalize_summary({}, records)
        top_k = [
            {
                "contract_id": record.get("contract_id"),
                "risk_score": record.get("risk_score"),
                "risk_level": record.get("risk_level"),
                "summary": self._build_summary_line(record),
                "limitations": record.get("limitations") or record.get("risk_limitations") or "",
                "flags": self._red_flags_for_contract(record),
            }
            for record in sorted(records, key=self._record_sort_key, reverse=True)[:10]
        ]
        return summary, top_k

    def _build_summary_payload_from_json(self, summary_json: dict[str, Any], records: list[dict[str, Any]]) -> dict[str, Any]:
        summary = dict(summary_json.get("summary") or {})
        if summary:
            return self._normalize_summary(summary, records)
        return self._normalize_summary({}, records)

    def _normalize_summary(self, summary: dict[str, Any], records: list[dict[str, Any]]) -> dict[str, Any]:
        level_counts = summary.get("level_counts") if isinstance(summary.get("level_counts"), dict) else {}
        if not level_counts:
            level_counts = self._build_level_counts(records)

        total_records = int(summary.get("total_records") or len(records))
        average_score = float(summary.get("average_score") or self._mean_score(records))
        top_flags = summary.get("top_flags") if isinstance(summary.get("top_flags"), list) else []
        if not top_flags:
            top_flags = self._build_top_flags(records)
        return {
            "total_records": total_records,
            "average_score": round(average_score, 2),
            "level_counts": self._normalize_level_counts(level_counts),
            "top_flags": top_flags,
        }

    def _build_level_counts(self, records: list[dict[str, Any]]) -> dict[str, int]:
        counts = Counter()
        for record in records:
            counts[self._normalize_level_key(record.get("risk_level"))] += 1
        return {key: int(counts.get(key, 0)) for key in ("bajo", "medio", "alto", "critico")}

    def _normalize_level_counts(self, counts: dict[str, Any]) -> dict[str, int]:
        result = {}
        for key in ("bajo", "medio", "alto", "critico"):
            result[key] = int(counts.get(key, 0))
        return result

    def _build_top_flags(self, records: list[dict[str, Any]], limit: int = 10) -> list[dict[str, Any]]:
        counter: Counter[str] = Counter()
        for record in records:
            for flag in self._red_flags_for_contract(record):
                code = self._normalize_key(flag.get("code"))
                if code:
                    counter[code] += 1

        top_flags = []
        for code, count in counter.most_common(limit):
            catalog = self._risk_catalog.get(code, {})
            top_flags.append(
                {
                    "code": code,
                    "label": catalog.get("label") or catalog.get("name") or code,
                    "severity": catalog.get("severity") or "Media",
                    "count": int(count),
                    "color": catalog.get("color") or self._tone_color(catalog.get("severity") or "Media"),
                }
            )
        return top_flags

    def _build_risk_distribution(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        counts = self._build_level_counts(records)
        total = sum(counts.values()) or 1
        labels = [
            ("Bajo", "bajo", "#16a34a"),
            ("Medio", "medio", "#f59e0b"),
            ("Alto", "alto", "#ef4444"),
            ("Crítico", "critico", "#b91c1c"),
        ]
        return [
            {
                "label": label,
                "key": key,
                "count": counts.get(key, 0),
                "percent": round((counts.get(key, 0) / total) * 100, 1),
                "color": color,
            }
            for label, key, color in labels
        ]

    def _build_metrics(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        total_contracts = len(records)
        total_value = self._sum_amount(records)
        average_risk = self._mean_score(records)
        top_entity = self._top_group_label(records, "entity")
        top_supplier = self._top_group_label(records, "supplier")
        top_region = self._top_group_label(records, "department")
        total_red_flags = self._sum_red_flags(records)
        return {
            "total_contracts": total_contracts,
            "total_value": round(total_value, 2),
            "average_risk": round(average_risk, 2),
            "high_risk_contracts": len([record for record in records if self._is_high_risk(record)]),
            "medium_risk_contracts": len([record for record in records if self._normalize_level_key(record.get("risk_level")) == "medio"]),
            "low_risk_contracts": len([record for record in records if self._normalize_level_key(record.get("risk_level")) == "bajo"]),
            "total_red_flags": total_red_flags,
            "top_entity": top_entity,
            "top_supplier": top_supplier,
            "top_region": top_region,
        }

    def _build_dashboard_interpretation(self, metrics: dict[str, Any]) -> str:
        level = self._normalize_level_key("critico" if metrics.get("high_risk_contracts", 0) else "medio")
        return (
            f"En el universo visible hay {metrics.get('total_contracts', 0)} contratos, "
            f"un score promedio de {metrics.get('average_risk', 0):.1f}/100 y {metrics.get('total_red_flags', 0)} red flags activas. "
            "La lectura es preventiva: prioriza revisión documental antes que conclusiones sancionatorias."
            if level
            else "Resumen preliminar del universo PAE."
        )

    def _build_contract_summary(self, record: dict[str, Any] | None) -> dict[str, Any]:
        if not record:
            return {}
        return {
            "contract_id": record.get("contract_id"),
            "process_id": record.get("process_id"),
            "entity": record.get("entity"),
            "supplier": record.get("supplier"),
            "department": record.get("department"),
            "municipality": record.get("municipality"),
            "modality": record.get("modality"),
            "status": record.get("status"),
            "initial_value": record.get("initial_value", 0),
            "final_value": record.get("final_value", 0),
            "risk_score": record.get("risk_score", 0),
            "risk_level": self._normalize_risk_level(record.get("risk_level"), record.get("risk_score", 0)),
            "red_flags": [flag.get("code") for flag in self._red_flags_for_contract(record)],
            "red_flag_count": len(self._red_flags_for_contract(record)),
            "recommended_action": record.get("recommended_action") or record.get("audit_recommendation") or "",
            "limitations": record.get("limitations") or record.get("risk_limitations") or "",
            "summary_line": self._build_summary_line(record),
        }

    def _build_contract_detail_payload(self, record: dict[str, Any]) -> dict[str, Any]:
        red_flags = self._red_flags_for_contract(record)
        return {
            **self._build_contract_summary(record),
            "object": record.get("object") or "",
            "start_date": record.get("start_date") or "",
            "end_date": record.get("end_date") or "",
            "addition_value": record.get("addition_value") or 0,
            "addition_percentage": record.get("addition_percentage") or 0,
            "score_explanation": record.get("score_explanation") or record.get("audit_score_explanation") or "",
            "audit_score_explanation": record.get("audit_score_explanation") or record.get("score_explanation") or "",
            "required_manual_checks": record.get("required_manual_checks") or "",
            "audit_recommendation": record.get("audit_recommendation") or record.get("recommended_action") or "",
            "red_flags": red_flags,
            "red_flag_details": red_flags,
            "audit_summary": record.get("audit_summary") or {},
            "audit_evidence": record.get("audit_evidence") or {},
            "huecos_de_informacion": record.get("huecos_de_informacion") or [],
            "documentos_a_revisar": record.get("documentos_a_revisar") or [],
            "evidence": record.get("evidence") or record.get("flag_evidence") or {},
            "source": self.source_label,
        }

    def _build_summary_line(self, record: dict[str, Any]) -> str:
        flags = ", ".join(flag.get("label") or flag.get("code") or "" for flag in self._red_flags_for_contract(record))
        if not flags:
            flags = "sin red flags relevantes"
        return (
            f"Contrato {record.get('contract_id')} con score {int(record.get('risk_score') or 0)}/100 "
            f"({self._normalize_risk_level(record.get('risk_level'), record.get('risk_score'))}). "
            f"Señales principales: {flags}."
        )


    def _build_decision_support(
        self,
        *,
        mode: str,
        contract: dict[str, Any] | None = None,
        records: list[dict[str, Any]] | None = None,
        rows: list[dict[str, Any]] | None = None,
        peer_summary: dict[str, Any] | None = None,
        analysis: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        contract = contract or {}
        records = [record for record in (records or []) if isinstance(record, dict)]
        rows = [row for row in (rows or []) if isinstance(row, dict)]
        analysis = analysis or {}

        patterns = self._decision_pattern_insights(
            mode=mode,
            contract=contract,
            records=records,
            rows=rows,
            peer_summary=peer_summary or {},
            analysis=analysis,
        )
        graph_suggestions = self._decision_graph_suggestions(
            mode=mode,
            contract=contract,
            records=records,
            rows=rows,
            peer_summary=peer_summary or {},
            analysis=analysis,
        )

        guidance_map = {
            "overview": "Usa este panel para identificar donde conviene profundizar primero, no para cerrar conclusiones.",
            "contract_detail": "Este contrato se debe leer por combinacion de red flags, contexto y comparacion con pares.",
            "red_flags": "Las red flags no son una sentencia; son una ruta para revisar competencia, trazabilidad y ejecucion.",
            "comparison": "La comparacion sirve para saber si el riesgo es aislado o se concentra en pocos actores.",
            "dashboard": "El resumen agregado te ayuda a priorizar territorios, entidades y proveedores antes de entrar al detalle.",
            "report": "El reporte resume la evidencia para justificar por que vale la pena profundizar.",
            "search": "La tabla de resultados debe leerse como una lista de priorizacion y contraste.",
            "followup": "El seguimiento te ayuda a dejar trazabilidad sobre los contratos que ya muestran alertas.",
        }

        focus_areas = self._unique_preserve_order(
            [
                "Revisar competencia",
                "Comparar con pares",
                "Validar trazabilidad documental",
                "Cruzar valor y adiciones",
                "Mirar concentracion territorial",
            ]
        )
        if contract:
            focus_areas.insert(0, "Abrir la ficha del contrato")
        if rows:
            focus_areas.insert(0, "Usar la tabla o grafica comparativa")
        if peer_summary:
            focus_areas.append("Contrastar con el promedio de pares")

        contract_id = str(contract.get("contract_id") or "").strip()
        return {
            "title": "Guia de decision",
            "guidance": guidance_map.get(mode, guidance_map["overview"]),
            "patterns": patterns,
            "graph_suggestions": graph_suggestions,
            "focus_areas": focus_areas[:5],
            "why_now": (
                f"Contrato {contract_id}" if contract_id else "Priorizar por score, red flags y cobertura documental"
            ),
        }

    def _decision_pattern_insights(
        self,
        *,
        mode: str,
        contract: dict[str, Any],
        records: list[dict[str, Any]],
        rows: list[dict[str, Any]],
        peer_summary: dict[str, Any],
        analysis: dict[str, Any],
    ) -> list[str]:
        insights: list[str] = []
        if contract:
            flags = self._red_flags_for_contract(contract)
            if flags:
                labels = [str(flag.get("label") or flag.get("code") or "") for flag in flags[:3] if str(flag.get("label") or flag.get("code") or "").strip()]
                insights.append(
                    f"El contrato activa {len(flags)} red flags, sobre todo {', '.join(labels)}."
                    if labels
                    else f"El contrato activa {len(flags)} red flags."
                )
            score = self._safe_number(contract.get("risk_score")) or 0
            peer_average = self._safe_number(peer_summary.get("peer_average_risk")) or 0
            if peer_average:
                gap = round(score - peer_average, 1)
                if abs(gap) >= 10:
                    direction = "por encima" if gap > 0 else "por debajo"
                    insights.append(f"El score esta {abs(gap):.1f} puntos {direction} del promedio de sus pares.")
            addition_pct = self._safe_number(contract.get("addition_percentage")) or 0
            if addition_pct:
                insights.append(f"Las adiciones representan {addition_pct:.1f}% del valor inicial.")
            if contract.get("limitations") or contract.get("risk_limitations"):
                insights.append("La trazabilidad tiene limitaciones y conviene revisar el expediente completo.")

        if rows:
            if mode == "red_flags":
                top_labels = [str(row.get("label") or row.get("name") or row.get("code") or "").strip() for row in rows[:3]]
                top_labels = [label for label in top_labels if label]
                if top_labels:
                    insights.append(
                        "Las alertas que mas se repiten son "
                        + ", ".join(top_labels[:3])
                        + ". Esto ayuda a identificar patrones y no casos aislados."
                    )
                if len(rows) >= 2 and (self._safe_number(rows[0].get("count")) or 0) > (self._safe_number(rows[1].get("count")) or 0):
                    insights.append("La frecuencia de la primera alerta supera a las siguientes, asi que conviene revisar si el patron se repite por entidad o proveedor.")
            if mode in {"comparison", "dashboard", "overview", "report"}:
                top_row = rows[0]
                bottom_row = rows[-1]
                if (self._safe_number(top_row.get("contract_count")) or 0) - (self._safe_number(bottom_row.get("contract_count")) or 0) >= 2:
                    insights.append("La concentracion no esta distribuida de forma pareja: hay grupos que acumulan mas contratos que otros.")
                if (self._safe_number(top_row.get("average_risk")) or 0) - (self._safe_number(bottom_row.get("average_risk")) or 0) >= 10:
                    insights.append("La brecha de riesgo entre grupos muestra que no todos los actores tienen el mismo nivel de alerta.")
            if mode == "search" and rows:
                insights.append("La tabla permite ver si el score alto se acompana de red flags y de poca trazabilidad.")

        if records and not insights:
            high_risk = len([record for record in records if self._is_high_risk(record)])
            if high_risk:
                insights.append(f"Hay {high_risk} contratos en rango alto o critico dentro del subconjunto actual.")

        if analysis.get("summary") and not insights:
            insights.append(str(analysis.get("summary")))

        if not insights:
            insights.append("La lectura debe hacerse comparando score, red flags y evidencia documental antes de decidir.")

        return self._unique_preserve_order(insights)[:4]

    def _decision_graph_suggestions(
        self,
        *,
        mode: str,
        contract: dict[str, Any],
        records: list[dict[str, Any]],
        rows: list[dict[str, Any]],
        peer_summary: dict[str, Any],
        analysis: dict[str, Any],
    ) -> list[str]:
        graphs: list[str] = []
        if mode in {"overview", "dashboard"}:
            graphs.extend([
                "Distribucion de riesgo por nivel",
                "Top red flags por frecuencia",
                "Comparacion de proveedores o entidades",
                "Mapa o tabla territorial por departamento o municipio",
                "Ranking de contratos por score y valor",
            ])
        elif mode == "contract_detail":
            graphs.extend([
                "Score del contrato frente al promedio de pares",
                "Red flags del contrato",
                "Valor inicial vs valor final",
                "Cobertura documental y trazabilidad",
                "Comparables por proveedor y entidad",
            ])
        elif mode == "comparison":
            graphs.extend([
                "Barras por grupo comparado",
                "Riesgo promedio por grupo",
                "Top contratos por cada grupo",
                "Red flags frecuentes por grupo",
            ])
        elif mode == "red_flags":
            graphs.extend([
                "Frecuencia de red flags",
                "Red flags por entidad o proveedor",
                "Cobertura documental y trazabilidad",
            ])
        elif mode == "report":
            graphs.extend([
                "Distribucion de riesgo por nivel",
                "Comparacion de pares",
                "Evolucion de valor y adiciones",
                "Cobertura documental y trazabilidad",
            ])
        else:
            graphs.extend([
                "Distribucion de riesgo por nivel",
                "Comparacion de proveedores o entidades",
                "Top red flags por frecuencia",
            ])

        if contract and (self._safe_number(contract.get("addition_percentage")) or 0) > 0:
            graphs.append("Evolucion de valor y adiciones")
        if peer_summary:
            graphs.append("Score del contrato frente al promedio de pares")
        if rows and any((self._safe_number(row.get("average_risk")) or 0) for row in rows):
            graphs.append("Ranking de contratos por score y valor")
        if analysis.get("graph_suggestions"):
            graphs.extend([str(item) for item in analysis.get("graph_suggestions") or []])

        return self._unique_preserve_order(graphs)[:6]
    def _build_peer_summary(self, contract: dict[str, Any]) -> dict[str, Any]:
        peers = [
            record
            for record in self.records
            if self._normalize_key(record.get("contract_id")) != self._normalize_key(contract.get("contract_id"))
            and (
                self._normalize_key(record.get("entity")) == self._normalize_key(contract.get("entity"))
                or self._normalize_key(record.get("supplier")) == self._normalize_key(contract.get("supplier"))
                or self._normalize_key(record.get("department")) == self._normalize_key(contract.get("department"))
            )
        ]
        peers.sort(key=self._record_sort_key, reverse=True)
        peers = peers[:5]
        return {
            "peer_count": len(peers),
            "peer_average_risk": round(self._mean_score(peers), 2),
            "peer_average_value": round(self._mean_value(peers), 2),
            "peer_top_contracts": [self._build_contract_summary(record) for record in peers[:3]],
        }

    def _context_for_contract(self, contract: dict[str, Any] | None, *, session_state: dict[str, Any] | None = None) -> dict[str, Any]:
        if not contract:
            return {}
        context = {
            "last_contract_id": contract.get("contract_id") or "",
            "last_supplier": contract.get("supplier") or "",
            "last_entity": contract.get("entity") or "",
            "last_department": contract.get("department") or "",
            "last_municipality": contract.get("municipality") or "",
        }
        if session_state and session_state.get("last_filters"):
            context["last_filters"] = dict(session_state.get("last_filters") or {})
        return {key: value for key, value in context.items() if value}

    def _context_from_results(self, results: list[dict[str, Any]], filters: dict[str, Any]) -> dict[str, Any]:
        context: dict[str, Any] = {"last_filters": self._clean_filters(filters)}
        if results:
            first = results[0]
            if first.get("contract_id"):
                context["last_contract_id"] = first.get("contract_id")
            if first.get("supplier"):
                context["last_supplier"] = first.get("supplier")
            if first.get("entity"):
                context["last_entity"] = first.get("entity")
            if first.get("department"):
                context["last_department"] = first.get("department")
            if first.get("municipality"):
                context["last_municipality"] = first.get("municipality")
            context["last_result_ids"] = [str(item.get("contract_id") or "").strip() for item in results if item.get("contract_id")]
        return {key: value for key, value in context.items() if value not in (None, "", [], {})}

    def _comparison_rows(self, records: list[dict[str, Any]], mode: str, limit: int = 5) -> list[dict[str, Any]]:
        grouped = self._group_records(records, mode=mode)
        rows = []
        for name, items in grouped.items():
            items = sorted(items, key=self._record_sort_key, reverse=True)
            rows.append(
                {
                    "name": name,
                    "contract_count": len(items),
                    "total_value": round(self._sum_amount(items), 2),
                    "average_risk": round(self._mean_score(items), 2),
                    "red_flag_count": self._sum_red_flags(items),
                    "top_contracts": [self._build_contract_summary(record) for record in items[:3]],
                    "top_flags": self._top_flags_for_group(items),
                    "entities": self._unique_values(items, "entity"),
                    "suppliers": self._unique_values(items, "supplier"),
                    "departments": self._unique_values(items, "department"),
                    "municipalities": self._unique_values(items, "municipality"),
                }
            )
        rows.sort(key=lambda row: (row["contract_count"], row["total_value"], row["average_risk"]), reverse=True)
        return rows[: max(1, int(limit))]

    def _build_contract_report_sections(
        self,
        contract: dict[str, Any],
        *,
        report_type: str,
        analysis: dict[str, Any],
    ) -> list[dict[str, Any]]:
        red_flags = self._red_flags_for_contract(contract)
        sections = [
            {
                "title": "Resumen ejecutivo",
                "paragraphs": [
                    analysis.get("summary")
                    or f"El contrato {contract.get('contract_id')} presenta un score de {int(contract.get('risk_score') or 0)}/100 ({self._normalize_risk_level(contract.get('risk_level'), contract.get('risk_score'))})."
                ],
            },
            {
                "title": "Red flags activas",
                "bullets": [
                    f"{flag.get('code')} - {flag.get('label')} ({flag.get('severity')})"
                    for flag in red_flags
                ] or ["No se detectaron red flags en la ficha disponible."],
            },
            {
                "title": "Evidencia y limitaciones",
                "bullets": [
                    self._contract_limitations(contract),
                    self._build_summary_line(contract),
                ],
            },
        ]
        if analysis.get("recommendations"):
            sections.append(
                {
                    "title": "Recomendaciones",
                    "bullets": [str(item) for item in analysis.get("recommendations") or []],
                }
            )
        if analysis.get("audit_questions"):
            sections.append(
                {
                    "title": "Preguntas de auditoría",
                    "bullets": [str(item) for item in analysis.get("audit_questions") or []],
                }
            )
        sections.append(
            {
                "title": "Tipo de reporte",
                "paragraphs": [f"Se preparó una vista previa {report_type} a partir del contrato focal."],
            }
        )
        return sections

    def _build_comparison_report_sections(
        self,
        rows: list[dict[str, Any]],
        *,
        report_type: str,
        analysis: dict[str, Any],
    ) -> list[dict[str, Any]]:
        bullets = [
            f"{row.get('name')} · {row.get('contract_count')} contratos · score promedio {row.get('average_risk')}"
            for row in rows[:5]
        ]
        return [
            {
                "title": "Resumen de comparación",
                "paragraphs": [
                    analysis.get("summary") or f"Se analizaron {len(rows)} grupos comparativos para el reporte {report_type}."
                ],
            },
            {
                "title": "Grupos destacados",
                "bullets": bullets or ["No hay grupos comparativos suficientes."],
            },
        ]

    def _build_markdown_report(
        self,
        *,
        report_type: str,
        scope_label: str,
        sections: list[dict[str, Any]],
        analysis: dict[str, Any],
    ) -> str:
        lines = [
            f"# Reporte {report_type.title()}",
            "",
            f"## Alcance",
            scope_label,
            "",
        ]
        if analysis.get("summary"):
            lines.extend(["## Resumen", str(analysis.get("summary")), ""])

        for section in sections:
            lines.append(f"## {section.get('title')}")
            for paragraph in section.get("paragraphs") or []:
                lines.append(str(paragraph))
            for bullet in section.get("bullets") or []:
                lines.append(f"- {bullet}")
            lines.append("")
        return "\n".join(lines).strip()

    def _report_title(self, report_type: str, scope_label: str) -> str:
        return f"Reporte {report_type.title()} · {scope_label}"

    def _report_filename(self, report_type: str, scope_label: str) -> str:
        safe_scope = normalize_text(scope_label).lower().replace(" ", "_")[:50] or "reporte"
        return f"pae_{report_type}_{safe_scope}.md"

    def _report_summary(self, contract: dict[str, Any] | None, report_type: str) -> str:
        if contract is None:
            return f"Vista previa del reporte {report_type} preparada con datos agregados para apoyar la decisión."
        return self._build_summary_line(contract)

    def _report_highlights(self, contract: dict[str, Any] | None, rows: list[dict[str, Any]], analysis: dict[str, Any]) -> list[str]:
        highlights: list[str] = []
        if contract:
            highlights.append(self._build_summary_line(contract))
            if contract.get("recommended_action"):
                highlights.append(str(contract.get("recommended_action")))
        for row in rows[:3]:
            highlights.append(
                f"{row.get('name')} · {row.get('contract_count')} contratos · score promedio {row.get('average_risk')}"
            )
        if analysis.get("summary"):
            highlights.append(str(analysis.get("summary")))
        return self._unique_preserve_order(highlights)[:6]

    def _contract_actions(self, contract: dict[str, Any]) -> list[str]:
        actions = [
            "Generar reporte ejecutivo",
            "Crear seguimiento",
            "Ver red flags",
            "Comparar proveedores",
        ]
        if contract.get("secop_url"):
            actions.insert(0, "Abrir enlace del proceso")
        return actions

    def _suggest_followup_action(self, contract: dict[str, Any] | None, followups: list[dict[str, Any]]) -> str:
        if contract:
            return contract.get("recommended_action") or "Revisar soportes, competencia y adiciones."
        if followups:
            return "Revisar el seguimiento más reciente."
        return "Marca un contrato para seguimiento y vuelve a consultarlo."

    def _filtered_records(self, filters: dict[str, Any]) -> list[dict[str, Any]]:
        records = list(self.records)
        if not filters:
            return records

        cleaned = self._clean_filters(filters)
        query = self._normalize_query(cleaned.pop("query", ""))
        if query:
            records = [record for record in records if self._record_matches_query(record, query)]

        for key in ("contract_id", "entity", "supplier", "department", "municipality", "modality", "status"):
            value = self._normalize_query(cleaned.get(key))
            if not value:
                continue
            records = [record for record in records if self._normalize_key(record.get(key)) == value or value in self._normalize_key(record.get(key))]

        risk_level = self._normalize_level_key(cleaned.get("risk_level"))
        if risk_level and risk_level != "all":
            records = [record for record in records if self._normalize_level_key(record.get("risk_level")) == risk_level]

        red_flag = self._normalize_key(cleaned.get("red_flag"))
        if red_flag and red_flag != "all":
            records = [record for record in records if any(self._normalize_key(flag.get("code")) == red_flag for flag in self._red_flags_for_contract(record))]

        min_amount = self._safe_number(cleaned.get("min_amount"))
        if min_amount is not None:
            records = [record for record in records if self._safe_number(record.get("final_value")) >= min_amount]

        max_amount = self._safe_number(cleaned.get("max_amount"))
        if max_amount is not None:
            records = [record for record in records if self._safe_number(record.get("final_value")) <= max_amount]

        date_from = cleaned.get("date_from")
        if date_from:
            records = [record for record in records if self._compare_dates(record.get("start_date"), date_from, operator=">=")]

        date_to = cleaned.get("date_to")
        if date_to:
            records = [record for record in records if self._compare_dates(record.get("start_date"), date_to, operator="<=")]

        return records

    def _record_matches_query(self, record: dict[str, Any], query: str) -> bool:
        haystack = self._record_search_blob(record)
        if query in haystack:
            return True
        if self._normalize_key(record.get("contract_id")) == query:
            return True
        if self._normalize_key(record.get("process_id")) == query:
            return True
        return False

    def _record_search_blob(self, record: dict[str, Any]) -> str:
        pieces: list[str] = []
        for key in (
            "contract_id",
            "process_id",
            "entity",
            "entity_nit",
            "supplier",
            "supplier_nit",
            "department",
            "municipality",
            "object",
            "modality",
            "status",
            "score_explanation",
            "audit_score_explanation",
            "recommended_action",
            "limitations",
            "risk_limitations",
        ):
            value = record.get(key)
            if value:
                pieces.append(str(value))
        for flag in self._red_flags_for_contract(record):
            pieces.extend([str(flag.get("code") or ""), str(flag.get("label") or ""), str(flag.get("description") or ""), str(flag.get("evidence") or "")])
        return self._normalize_text(" ".join(pieces))

    def _score_query_match(self, record: dict[str, Any], query: str) -> int:
        score = 0
        contract_id = self._normalize_key(record.get("contract_id"))
        process_id = self._normalize_key(record.get("process_id"))
        haystack = self._record_search_blob(record)
        if query == contract_id or query == process_id:
            score += 1_000
        if query in contract_id or query in process_id:
            score += 600
        if query in haystack:
            score += 200
        tokens = [token for token in query.split(" ") if len(token) > 2]
        score += sum(15 for token in tokens if token in haystack)
        score += int(self._safe_number(record.get("risk_score")) or 0)
        score += int(self._safe_number(record.get("red_flag_count")) or len(self._red_flags_for_contract(record))) * 4
        return score

    def _red_flags_for_contract(self, record: dict[str, Any]) -> list[dict[str, Any]]:
        flags = record.get("red_flag_details")
        if isinstance(flags, list) and flags:
            return [self._normalize_flag(flag) for flag in flags if self._normalize_flag(flag)]

        raw_flags = record.get("audit_red_flags_activadas") or record.get("red_flags") or []
        if isinstance(raw_flags, list):
            normalized = []
            for item in raw_flags:
                if isinstance(item, dict):
                    flag = self._normalize_flag(item)
                    if flag:
                        normalized.append(flag)
                    continue
                code = str(item or "").strip()
                if not code:
                    continue
                catalog = self._risk_catalog.get(self._normalize_key(code), {})
                normalized.append(
                    {
                        "code": code,
                        "label": catalog.get("label") or catalog.get("name") or code,
                        "severity": catalog.get("severity") or "Media",
                        "category": catalog.get("category") or "general",
                        "description": catalog.get("description") or catalog.get("name") or code,
                        "evidence": self._flag_evidence(record, code) or catalog.get("evidence_hint") or "",
                        "weight": catalog.get("weight") or 0,
                        "color": catalog.get("color") or self._tone_color(catalog.get("severity") or "Media"),
                    }
                )
            return normalized
        return []

    def _normalize_flag(self, flag: dict[str, Any]) -> dict[str, Any] | None:
        if not isinstance(flag, dict):
            return None
        code = str(flag.get("code") or flag.get("name") or "").strip()
        if not code:
            return None
        catalog = self._risk_catalog.get(self._normalize_key(code), {})
        return {
            "code": code,
            "label": str(flag.get("label") or flag.get("name") or catalog.get("label") or catalog.get("name") or code).strip(),
            "severity": str(flag.get("severity") or catalog.get("severity") or "Media").strip(),
            "category": str(flag.get("category") or catalog.get("category") or "general").strip(),
            "description": str(flag.get("description") or catalog.get("description") or catalog.get("name") or code).strip(),
            "evidence": str(flag.get("evidence") or flag.get("evidence_hint") or catalog.get("evidence_hint") or "").strip(),
            "weight": self._safe_number(flag.get("weight")) or catalog.get("weight") or 0,
            "color": str(flag.get("color") or catalog.get("color") or self._tone_color(flag.get("severity") or catalog.get("severity") or "Media")).strip(),
        }

    def _flag_evidence(self, record: dict[str, Any], code: str) -> str:
        evidence = record.get("flag_evidence") or record.get("evidence") or {}
        if isinstance(evidence, dict):
            value = evidence.get(code) or evidence.get(self._normalize_key(code))
            if value:
                return str(value)
        return ""

    def _merge_audit_card(self, record: dict[str, Any], audit_cards: dict[str, dict[str, Any]]) -> dict[str, Any]:
        contract_id = self._normalize_key(record.get("contract_id"))
        audit_card = audit_cards.get(contract_id)
        merged = _canonicalize_contract_row(record)
        if audit_card:
            merged = _canonicalize_risk_payload({**merged, **audit_card})
        return merged

    def _coerce_contract_record(self, record: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(record, dict):
            return {}
        canonical = _canonicalize_contract_row(record)
        canonical["risk_level"] = self._normalize_risk_level(canonical.get("risk_level"), canonical.get("risk_score"))
        canonical["risk_score"] = int(self._safe_number(canonical.get("risk_score")) or 0)
        if "red_flag_details" not in canonical or not canonical.get("red_flag_details"):
            canonical["red_flag_details"] = self._red_flags_for_contract(canonical)
        canonical["red_flag_count"] = int(canonical.get("red_flag_count") or len(canonical.get("red_flag_details") or []))
        canonical["summary_line"] = canonical.get("summary_line") or self._build_summary_line(canonical)
        return canonical

    def _build_contract_summary(self, record: dict[str, Any]) -> dict[str, Any]:
        summary = self._coerce_contract_record(record)
        return {
            "contract_id": summary.get("contract_id"),
            "process_id": summary.get("process_id"),
            "entity": summary.get("entity"),
            "supplier": summary.get("supplier"),
            "department": summary.get("department"),
            "municipality": summary.get("municipality"),
            "modality": summary.get("modality"),
            "status": summary.get("status"),
            "initial_value": summary.get("initial_value", 0),
            "final_value": summary.get("final_value", 0),
            "risk_score": summary.get("risk_score", 0),
            "risk_level": summary.get("risk_level", "Bajo"),
            "red_flags": [flag.get("code") for flag in self._red_flags_for_contract(summary)],
            "red_flag_count": int(summary.get("red_flag_count") or len(self._red_flags_for_contract(summary))),
            "recommended_action": summary.get("recommended_action") or summary.get("audit_recommendation") or "",
            "limitations": summary.get("limitations") or summary.get("risk_limitations") or "",
            "summary_line": summary.get("summary_line") or self._build_summary_line(summary),
        }

    def _group_records(self, records: list[dict[str, Any]], *, mode: str) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for record in records:
            if mode == "supplier":
                key = record.get("supplier") or "Sin proveedor"
            elif mode == "entity":
                key = record.get("entity") or "Sin entidad"
            elif mode == "region":
                key = record.get("department") or record.get("municipality") or "Sin región"
            else:
                key = record.get("department") or "Sin región"
            grouped[str(key).strip() or "Sin dato"].append(record)
        return grouped

    def _top_group_label(self, records: list[dict[str, Any]], key: str) -> dict[str, Any]:
        rows = self._comparison_rows(records, key if key in {"supplier", "entity", "region"} else "region", limit=1)
        return rows[0] if rows else {"name": "Sin dato", "contract_count": 0}

    def _unique_values(self, records: list[dict[str, Any]], key: str) -> list[str]:
        values = [str(record.get(key) or "").strip() for record in records if str(record.get(key) or "").strip()]
        return self._unique_preserve_order(values)

    def _sum_amount(self, records: list[dict[str, Any]]) -> float:
        return float(sum(self._safe_number(record.get("final_value")) or 0 for record in records))

    def _mean_value(self, records: list[dict[str, Any]]) -> float:
        values = [self._safe_number(record.get("final_value")) or 0 for record in records]
        return float(sum(values) / len(values)) if values else 0.0

    def _mean_score(self, records: list[dict[str, Any]]) -> float:
        values = [self._safe_number(record.get("risk_score")) or 0 for record in records]
        return float(sum(values) / len(values)) if values else 0.0

    def _sum_red_flags(self, records: list[dict[str, Any]]) -> int:
        return int(sum(len(self._red_flags_for_contract(record)) for record in records))

    def _top_flags_for_group(self, records: list[dict[str, Any]], limit: int = 3) -> list[dict[str, Any]]:
        counter: Counter[str] = Counter()
        for record in records:
            for flag in self._red_flags_for_contract(record):
                code = self._normalize_key(flag.get("code"))
                if code:
                    counter[code] += 1
        top = []
        for code, count in counter.most_common(limit):
            catalog = self._risk_catalog.get(code, {})
            top.append(
                {
                    "code": code,
                    "label": catalog.get("label") or catalog.get("name") or code,
                    "count": int(count),
                    "severity": catalog.get("severity") or "Media",
                }
            )
        return top

    def _build_top_flags_for_record(self, record: dict[str, Any]) -> list[str]:
        return [flag.get("label") or flag.get("code") or "" for flag in self._red_flags_for_contract(record)]

    def _default_quick_actions(self) -> list[dict[str, Any]]:
        return [
            {"label": "Ver contratos con mayor riesgo", "query": "Muestra los contratos PAE con mayor riesgo"},
            {"label": "Buscar contrato por ID", "query": "Busca el contrato"},
            {"label": "Comparar proveedores", "query": "Compara proveedores PAE"},
            {"label": "Generar reporte", "query": "Genera un reporte ejecutivo"},
            {"label": "Ver red flags", "query": "Muestra las red flags más frecuentes"},
            {"label": "Crear seguimiento", "query": "Crear seguimiento del contrato más riesgoso"},
            {"label": "Ver resumen del dashboard", "query": "Muestra el resumen del dashboard"},
        ]

    def _load_risk_catalog(self) -> dict[str, dict[str, Any]]:
        payload = load_risk_registry() or {}
        flags = payload.get("flags") or {}
        catalog: dict[str, dict[str, Any]] = {}
        if isinstance(flags, dict):
            for code, flag in flags.items():
                normalized_code = self._normalize_key(code)
                if not normalized_code:
                    continue
                severity = self._normalize_severity(flag.get("weight"))
                catalog[normalized_code] = {
                    "code": str(code).strip(),
                    "label": str(flag.get("label") or code).strip(),
                    "name": str(flag.get("label") or code).strip(),
                    "severity": severity,
                    "category": str(flag.get("dimension") or "general").strip(),
                    "description": str(flag.get("label") or flag.get("description") or code).strip(),
                    "evidence_hint": str(flag.get("label") or code).strip(),
                    "weight": int(self._safe_number(flag.get("weight")) or 0),
                    "color": self._tone_color(severity),
                }
        return catalog

    def _load_scoring_registry(self) -> dict[str, Any]:
        return load_scoring_registry() or {}

    def _build_signature(self) -> str:
        paths = [
            self.output_dir / "pae_risk_ranking.json",
            self.output_dir / "pae_audit_cards.json",
            self.output_dir / "pae_risk_scores.json",
            self.store.path,
        ]
        return "|".join(self._file_signature(path) for path in paths)

    def _file_signature(self, path: Path) -> str:
        try:
            stats = path.stat()
            return f"{stats.st_mtime_ns}:{stats.st_size}"
        except OSError:
            return "missing"

    def _load_json_list(self, path: Path) -> list[dict[str, Any]]:
        try:
            if not path.exists():
                return []
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        return []

    def _load_json_object(self, path: Path) -> dict[str, Any]:
        try:
            if not path.exists():
                return {}
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _normalize_query(self, value: Any) -> str:
        return self._normalize_text(value)

    def _normalize_text(self, value: Any) -> str:
        return normalize_text(value)

    def _normalize_key(self, value: Any) -> str:
        return self._normalize_text(value)

    def _normalize_level_key(self, value: Any) -> str:
        text = self._normalize_text(value)
        if text in {"CRITICO", "CRITICAL"}:
            return "critico"
        if text.startswith("ALTO"):
            return "alto"
        if text.startswith("MEDIO"):
            return "medio"
        if text.startswith("BAJO"):
            return "bajo"
        return ""

    def _normalize_report_type(self, value: Any) -> str:
        text = self._normalize_text(value)
        if text in {"TECNICO", "TECHNICAL"}:
            return "technical"
        if text in {"CIUDADANO", "CIUDADANA"}:
            return "citizen"
        if text in {"SEGUIMIENTO", "FOLLOWUP"}:
            return "followup"
        if text in {"EJECUTIVO", "EXECUTIVE"}:
            return "executive"
        return "executive"

    def _normalize_severity(self, weight: Any) -> str:
        numeric = int(self._safe_number(weight) or 0)
        if numeric >= 8:
            return "Alta"
        if numeric >= 5:
            return "Media"
        return "Baja"

    def _normalize_risk_level(self, value: Any, score: Any = None) -> str:
        text = self._normalize_text(value)
        if text.startswith("CRIT"):
            return "Critico"
        if text.startswith("ALTO"):
            return "Alto"
        if text.startswith("MEDIO"):
            return "Medio"
        if text.startswith("BAJO"):
            return "Bajo"
        numeric = self._safe_number(score) or 0
        if numeric >= 85:
            return "Critico"
        if numeric >= 56:
            return "Alto"
        if numeric >= 31:
            return "Medio"
        return "Bajo"

    def _tone_for_score(self, score: Any) -> str:
        numeric = self._safe_number(score) or 0
        if numeric >= 85:
            return "critical"
        if numeric >= 56:
            return "high"
        if numeric >= 31:
            return "medium"
        return "low"

    def _tone_color(self, severity: str) -> str:
        normalized = self._normalize_text(severity)
        if normalized.startswith("ALTA"):
            return "#dc2626"
        if normalized.startswith("MEDIA"):
            return "#f59e0b"
        return "#16a34a"

    def _safe_number(self, value: Any) -> float | None:
        if value is None or value == "":
            return None
        if isinstance(value, bool):
            return float(int(value))
        if isinstance(value, (int, float)):
            if pd.isna(value):
                return None
            return float(value)
        text = str(value).strip().replace(" ", "").replace(",", ".")
        try:
            return float(text)
        except Exception:
            return None

    def _compare_dates(self, left: Any, right: Any, *, operator: str) -> bool:
        left_value = self._parse_date(left)
        right_value = self._parse_date(right)
        if left_value is None or right_value is None:
            return True
        if operator == ">=":
            return left_value >= right_value
        if operator == "<=":
            return left_value <= right_value
        return True

    def _parse_date(self, value: Any) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text[:10])
        except Exception:
            return None

    def _default_limitations(self) -> str:
        return (
            "El análisis es preliminar y depende de la calidad de los datos públicos disponibles. "
            "No reemplaza la revisión documental ni valida hechos no presentes en las fuentes."
        )

    def _contract_limitations(self, contract: dict[str, Any] | None) -> str:
        if not contract:
            return self._default_limitations()
        text = str(contract.get("limitations") or contract.get("risk_limitations") or contract.get("required_manual_checks") or "").strip()
        if text:
            return text
        return self._default_limitations()

    def _clean_filters(self, filters: dict[str, Any]) -> dict[str, Any]:
        cleaned: dict[str, Any] = {}
        for key, value in filters.items():
            if value in (None, "", [], {}, "all"):
                continue
            cleaned[key] = value
        return cleaned

    def _is_high_risk(self, record: dict[str, Any]) -> bool:
        return self._normalize_level_key(record.get("risk_level")) in {"alto", "critico"} or int(self._safe_number(record.get("risk_score")) or 0) >= 61

    def _record_sort_key(self, record: dict[str, Any]) -> tuple[float, float, float]:
        return (
            float(self._safe_number(record.get("risk_score")) or 0),
            float(self._safe_number(record.get("final_value")) or 0),
            float(self._safe_number(record.get("red_flag_count")) or len(self._red_flags_for_contract(record))),
        )

    def _build_summary_line_for_records(self, records: Iterable[dict[str, Any]]) -> str:
        records = list(records)
        if not records:
            return "Sin registros."
        top = records[0]
        return self._build_summary_line(top)

    def _unique_preserve_order(self, values: list[str]) -> list[str]:
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
