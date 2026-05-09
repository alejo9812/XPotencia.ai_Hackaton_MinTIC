from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any

from .config import normalize_text


CONTRACT_ID_PATTERN = re.compile(
    r"\b(?:CO\d+[A-Z0-9]*(?:\.[A-Z0-9]+)+|PAE-\d{4}-\d+|[A-Z]-\d+|P-\d+)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ClassifiedIntent:
    intent: str
    confidence: float
    contract_id: str = ""
    report_type: str = ""
    comparison_mode: str = ""
    depth: str = "quick"
    entities: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["entities"] = dict(self.entities)
        return payload


def classify_intent(query: str, session_state: dict[str, Any] | None = None) -> ClassifiedIntent:
    text = str(query or "").strip()
    normalized = normalize_text(text)
    contract_id = _extract_contract_id(text)
    report_type = _detect_report_type(normalized)
    comparison_mode = _detect_comparison_mode(normalized)
    last_contract_id = str((session_state or {}).get("last_contract_id") or "").strip()
    last_supplier = str((session_state or {}).get("last_supplier") or "").strip()
    last_entity = str((session_state or {}).get("last_entity") or "").strip()

    if not normalized:
        return ClassifiedIntent(
            intent="project_overview",
            confidence=1.0,
            depth="quick",
            entities=_build_entities(contract_id=last_contract_id, report_type=""),
        )

    if _looks_like_overview(normalized):
        return ClassifiedIntent(
            intent="project_overview",
            confidence=0.96,
            depth="quick",
            entities=_build_entities(contract_id=contract_id or last_contract_id),
        )

    if _looks_like_followup_list(normalized):
        return ClassifiedIntent(
            intent="followup_list",
            confidence=0.93,
            depth="quick",
            entities=_build_entities(contract_id=contract_id or last_contract_id),
        )

    if _looks_like_followup_creation(normalized):
        return ClassifiedIntent(
            intent="followup_creation",
            confidence=0.94,
            depth="quick",
            entities=_build_entities(contract_id=contract_id or last_contract_id),
        )

    if _looks_like_report(normalized, report_type):
        return ClassifiedIntent(
            intent="report_generation",
            confidence=0.96,
            report_type=report_type or "executive",
            depth="deep" if contract_id or last_contract_id else "quick",
            entities=_build_entities(contract_id=contract_id or last_contract_id, report_type=report_type or "executive"),
        )

    if _looks_like_red_flags(normalized):
        return ClassifiedIntent(
            intent="red_flags_explanation",
            confidence=0.93,
            depth="deep" if contract_id or last_contract_id else "quick",
            entities=_build_entities(contract_id=contract_id or last_contract_id),
        )

    if _looks_like_dashboard(normalized):
        return ClassifiedIntent(
            intent="dashboard_summary",
            confidence=0.94,
            depth="quick",
            entities=_build_entities(contract_id=contract_id or last_contract_id),
        )

    if _looks_like_top_risk(normalized):
        return ClassifiedIntent(
            intent="top_risk_contracts",
            confidence=0.95,
            depth="quick",
            entities=_build_entities(contract_id=contract_id or last_contract_id),
        )

    if _looks_like_supplier_comparison(normalized):
        return ClassifiedIntent(
            intent="supplier_comparison",
            confidence=0.95,
            comparison_mode="supplier",
            depth="quick",
            entities=_build_entities(
                contract_id=contract_id or last_contract_id,
                comparison_mode="supplier",
                supplier_hint=last_supplier,
            ),
        )

    if _looks_like_entity_comparison(normalized):
        return ClassifiedIntent(
            intent="entity_comparison",
            confidence=0.95,
            comparison_mode="entity",
            depth="quick",
            entities=_build_entities(
                contract_id=contract_id or last_contract_id,
                comparison_mode="entity",
                entity_hint=last_entity,
            ),
        )

    if _looks_like_region_summary(normalized):
        return ClassifiedIntent(
            intent="region_summary",
            confidence=0.93,
            depth="quick",
            entities=_build_entities(contract_id=contract_id or last_contract_id),
        )

    if contract_id:
        depth = "deep" if _looks_like_risk_analysis(normalized) else "quick"
        intent = "contract_risk_analysis" if depth == "deep" else "contract_detail"
        return ClassifiedIntent(
            intent=intent,
            confidence=0.98,
            contract_id=contract_id,
            depth=depth,
            entities=_build_entities(contract_id=contract_id),
        )

    if _looks_like_risk_analysis(normalized):
        return ClassifiedIntent(
            intent="contract_risk_analysis",
            confidence=0.88,
            depth="deep",
            entities=_build_entities(contract_id=last_contract_id),
        )

    if _looks_like_contract_search(normalized):
        return ClassifiedIntent(
            intent="contract_search",
            confidence=0.9,
            depth="quick",
            entities=_build_entities(contract_id=contract_id or last_contract_id),
        )

    if _looks_like_general_help(normalized):
        return ClassifiedIntent(
            intent="project_overview",
            confidence=0.86,
            depth="quick",
            entities=_build_entities(contract_id=contract_id or last_contract_id),
        )

    return ClassifiedIntent(
        intent="unknown_query",
        confidence=0.5,
        depth="quick",
        entities=_build_entities(contract_id=contract_id or last_contract_id),
    )


def _extract_contract_id(text: str) -> str:
    for match in CONTRACT_ID_PATTERN.findall(text):
        candidate = str(match or "").strip()
        if candidate:
            return candidate
    return ""


def _detect_report_type(normalized: str) -> str:
    if "TECNICO" in normalized:
        return "technical"
    if "CIUDADANO" in normalized or "CIUDADANA" in normalized:
        return "citizen"
    if "SEGUIMIENTO" in normalized:
        return "followup"
    if "EJECUTIVO" in normalized:
        return "executive"
    return ""


def _detect_comparison_mode(normalized: str) -> str:
    if "PROVEEDOR" in normalized or "PROVEEDORES" in normalized:
        return "supplier"
    if "ENTIDAD" in normalized or "ENTIDADES" in normalized:
        return "entity"
    return ""


def _looks_like_overview(normalized: str) -> bool:
    keywords = (
        "QUE ES",
        "COMO FUNCIONA",
        "QUE HACES",
        "AYUDA",
        "RESUMEN DEL PROYECTO",
        "SOBRE EL PROYECTO",
        "EXPLICA EL SISTEMA",
        "PANEL INICIAL",
    )
    return any(term in normalized for term in keywords)


def _looks_like_general_help(normalized: str) -> bool:
    return any(term in normalized for term in ("QUE PUEDES HACER", "AYUDAME", "COMO TE USO", "START", "INICIO"))


def _looks_like_contract_search(normalized: str) -> bool:
    return any(term in normalized for term in ("BUSCA", "BUSCAR", "ENCUENTRA", "CONSULTA", "MUESTRA CONTRATOS", "CONTRATO"))


def _looks_like_risk_analysis(normalized: str) -> bool:
    return any(term in normalized for term in ("POR QUE", "PORQUE", "EXPLICA EL RIESGO", "ANALIZA", "RIESGOSO", "SCORE", "OPACO"))


def _looks_like_top_risk(normalized: str) -> bool:
    return any(term in normalized for term in ("MAYOR RIESGO", "MAS RIESGOSOS", "TOP RIESGO", "TOP CONTRATOS", "MAYOR SCORE"))


def _looks_like_red_flags(normalized: str) -> bool:
    return any(term in normalized for term in ("RED FLAG", "RED FLAGS", "SENALES", "ALERTA", "BANDERA ROJA"))


def _looks_like_supplier_comparison(normalized: str) -> bool:
    return "COMPAR" in normalized and ("PROVEEDOR" in normalized or "PROVEEDORES" in normalized)


def _looks_like_entity_comparison(normalized: str) -> bool:
    return "COMPAR" in normalized and ("ENTIDAD" in normalized or "ENTIDADES" in normalized)


def _looks_like_region_summary(normalized: str) -> bool:
    return any(term in normalized for term in ("DEPARTAMENTO", "MUNICIPIO", "REGION", "REGIONAL", "TERRITORIO"))


def _looks_like_dashboard(normalized: str) -> bool:
    return any(term in normalized for term in ("DASHBOARD", "KPI", "METRIC", "RESUMEN GENERAL", "PANORAMA", "TABLERO"))


def _looks_like_report(normalized: str, report_type: str) -> bool:
    return "REPORTE" in normalized or "INFORME" in normalized or bool(report_type)


def _looks_like_followup_creation(normalized: str) -> bool:
    creation_terms = (
        "CREA SEGUIMIENTO",
        "CREAR SEGUIMIENTO",
        "MARCA SEGUIMIENTO",
        "MARCAR SEGUIMIENTO",
        "AGREGA SEGUIMIENTO",
        "AGREGAR SEGUIMIENTO",
        "GUARDA SEGUIMIENTO",
        "GUARDAR SEGUIMIENTO",
        "NUEVO SEGUIMIENTO",
    )
    return any(term in normalized for term in creation_terms)


def _looks_like_followup_list(normalized: str) -> bool:
    if any(term in normalized for term in ("LISTA DE SEGUIMIENTO", "VER SEGUIMIENTOS", "SEGUIMIENTOS GUARDADOS", "MIS SEGUIMIENTOS", "SEGUIMIENTO ACTIVO")):
        return True
    if "SEGUIMIENTO" in normalized and not _looks_like_followup_creation(normalized):
        return True
    return False


def _build_entities(**kwargs: Any) -> dict[str, Any]:
    return {key: value for key, value in kwargs.items() if value not in (None, "", [], {})}

