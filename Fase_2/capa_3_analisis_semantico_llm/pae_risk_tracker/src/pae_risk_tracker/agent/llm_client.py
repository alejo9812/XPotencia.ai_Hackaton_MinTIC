from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from urllib import request as urllib_request

from ..config import normalize_text
from .prompts import PROMPT_VERSION, SYSTEM_PROMPT


@dataclass(frozen=True)
class LLMAnalysis:
    summary: str
    explanation: str
    recommendations: List[str]
    audit_questions: List[str]
    graph_suggestions: List[str] = field(default_factory=list)
    prompt_version: str = PROMPT_VERSION
    provider: str = "mock"
    model: str = "mock"

    def to_dict(self) -> dict[str, Any]:
        return {
            "summary": self.summary,
            "explanation": self.explanation,
            "recommendations": self.recommendations,
            "audit_questions": self.audit_questions,
            "graph_suggestions": self.graph_suggestions,
            "prompt_version": self.prompt_version,
            "provider": self.provider,
            "model": self.model,
        }


class LLMClient(ABC):
    @abstractmethod
    def analyze(self, payload: dict[str, Any]) -> LLMAnalysis:
        raise NotImplementedError


@dataclass
class MockLLMClient(LLMClient):
    model: str = "mock"
    provider: str = "mock"
    prompt_version: str = PROMPT_VERSION

    def analyze(self, payload: dict[str, Any]) -> LLMAnalysis:
        rows = list(payload.get("evidence_rows") or [])
        plan = dict(payload.get("plan") or {})
        validation = dict(payload.get("validation") or {})
        top_flags = _top_flag_codes(rows)
        top_limitations = _top_limitations(rows)
        top_scores = [int(float(row.get("risk_score") or 0)) for row in rows if row.get("risk_score") is not None]
        high_risk_count = sum(1 for score in top_scores if score >= 61)

        summary = _build_summary(plan, rows, high_risk_count)
        explanation = _build_explanation(plan, rows, top_flags, top_limitations, high_risk_count, validation)
        recommendations = _build_recommendations(top_flags, rows)
        audit_questions = _build_questions(plan, rows, top_flags)
        graph_suggestions = _build_graph_suggestions(plan, rows, top_flags, validation)

        return LLMAnalysis(
            summary=summary,
            explanation=explanation,
            recommendations=recommendations,
            audit_questions=audit_questions,
            graph_suggestions=graph_suggestions,
            prompt_version=self.prompt_version,
            provider=self.provider,
            model=self.model,
        )


@dataclass
class OpenAICompatibleLLMClient(LLMClient):
    base_url: str
    api_key: str
    model: str
    provider: str = "openai-compatible"
    prompt_version: str = PROMPT_VERSION
    timeout_seconds: int = 30
    temperature: float = 0.0

    def analyze(self, payload: dict[str, Any]) -> LLMAnalysis:
        body = {
            "model": self.model,
            "temperature": self.temperature,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
        }
        url = self.base_url.rstrip("/") + "/v1/chat/completions"
        request = urllib_request.Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            },
            method="POST",
        )
        with urllib_request.urlopen(request, timeout=self.timeout_seconds) as response:
            raw = response.read().decode("utf-8")
        parsed = json.loads(raw)
        text = _extract_text(parsed)
        try:
            data = json.loads(text)
        except Exception:
            data = {}

        return LLMAnalysis(
            summary=str(data.get("summary") or ""),
            explanation=str(data.get("explanation") or ""),
            recommendations=list(data.get("recommendations") or []),
            audit_questions=list(data.get("audit_questions") or []),
            graph_suggestions=list(data.get("graph_suggestions") or []),
            prompt_version=self.prompt_version,
            provider=self.provider,
            model=self.model,
        )


def _extract_text(payload: dict[str, Any]) -> str:
    choices = payload.get("choices") or []
    if not choices:
        return ""
    message = choices[0].get("message") or {}
    return str(message.get("content") or "")


def _top_flag_codes(rows: List[Dict[str, Any]]) -> List[str]:
    counts: dict[str, int] = {}
    for row in rows:
        flags = _parse_flags(row)
        for flag in flags:
            counts[flag] = counts.get(flag, 0) + 1
    return [code for code, _ in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:5]]


def _parse_flags(row: Dict[str, Any]) -> List[str]:
    raw = row.get("risk_flags_json")
    if not raw:
        raw = row.get("red_flags") or row.get("red_flags_activadas") or row.get("audit_red_flags_activadas")
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [
                    str(item.get("code") or item).strip()
                    for item in parsed
                    if (isinstance(item, dict) and (item.get("code") or item.get("name"))) or str(item).strip()
                ]
        except Exception:
            return []
    if isinstance(raw, list):
        return [
            str(item.get("code") or item).strip()
            for item in raw
            if (isinstance(item, dict) and (item.get("code") or item.get("name"))) or str(item).strip()
        ]
    return []


def _top_limitations(rows: List[Dict[str, Any]]) -> List[str]:
    limitations: List[str] = []
    for row in rows:
        for key in ("risk_limitations", "limitations", "required_manual_checks"):
            value = str(row.get(key) or "").strip()
            if value and value not in limitations:
                limitations.append(value)
            if len(limitations) >= 3:
                return limitations
    return limitations


def _build_summary(plan: dict[str, Any], rows: List[Dict[str, Any]], high_risk_count: int) -> str:
    total = len(rows)
    target = str(plan.get("query") or plan.get("department") or "la consulta").strip()
    return f"Se revisaron {total} registros para {target}. {high_risk_count} quedaron con prioridad alta o critica."


def _build_explanation(
    plan: dict[str, Any],
    rows: List[Dict[str, Any]],
    top_flags: List[str],
    top_limitations: List[str],
    high_risk_count: int,
    validation: dict[str, Any],
) -> str:
    if not rows:
        return "No se encontraron registros relevantes con los filtros disponibles."

    clauses: List[str] = []
    if high_risk_count:
        clauses.append(f"{high_risk_count} registros superan el umbral alto.")
    if top_flags:
        clauses.append("Se activaron se??ales como " + ", ".join(top_flags[:3]) + ".")
    if top_limitations:
        clauses.append("Las limitaciones mas visibles son " + "; ".join(top_limitations[:2]) + ".")
    if plan.get("record_type"):
        clauses.append(f"La consulta se enfoco en {plan['record_type']}.")
    if plan.get("department"):
        clauses.append(f"Se acoto a {plan['department']}.")

    latest_run = dict(validation.get("latest_run") or {})
    if latest_run:
        clauses.append(f"La ultima validacion quedo en estado {latest_run.get('overall_status') or 'desconocido'}.")
    if validation.get("observations"):
        clauses.append(f"Se encontraron {len(validation.get('observations') or [])} observaciones de trazabilidad.")
    criteria = dict(validation.get("criteria") or {})
    if criteria:
        clauses.append(
            f"El mapa de opacidad quedo {criteria.get('overall_status') or 'desconocido'} con "
            f"{criteria.get('covered_count') or 0}/{criteria.get('criteria_count') or 0} familias verificadas."
        )
    clauses.append("La alerta se interpreta como priorizacion, no como acusacion.")
    clauses.append("Antes de decidir conviene contrastar competencia, concentracion, ejecucion y trazabilidad.")

    return " ".join(clauses) if clauses else "La evidencia disponible es limitada y requiere revision manual."


def _build_recommendations(top_flags: List[str], rows: List[Dict[str, Any]]) -> List[str]:
    flag_map = {
        "RF-07": "Verificar trazabilidad documental: URL, justificacion y fechas.",
        "RF-08": "Revisar competencia y numero de oferentes.",
        "RF-12": "Contrastar tasas bajas de oferta con el expediente completo.",
        "RF-14": "Analizar concentracion del proveedor por entidad.",
        "RF-20": "Comparar el valor con historicos similares para detectar atipicos.",
        "RF-22": "Revisar adiciones, prorrogas y otrosies del contrato.",
        "RF-23": "Validar la combinacion de valor alto y baja competencia.",
    }
    recommendations = [flag_map[code] for code in top_flags if code in flag_map]
    if not recommendations:
        recommendations.append("Revisar manualmente el expediente antes de priorizar cierre.")
    if any(not (row.get("url_process") or row.get("secop_url")) for row in rows):
        recommendations.insert(0, "Completar o validar el enlace publico del proceso.")
    recommendations.append("Cruzar el hallazgo con la distribucion de riesgo y la comparacion de pares.")
    return _dedupe_keep_order(recommendations)[:5]


def _build_questions(plan: dict[str, Any], rows: List[Dict[str, Any]], top_flags: List[str]) -> List[str]:
    questions = [
        "?Que soporte documental justifica la modalidad y el valor?",
        "?Cuantos oferentes o manifestaciones reales de interes hubo?",
        "?Existen adiciones, prorrogas u otrosies asociados?",
    ]
    if plan.get("department"):
        questions.insert(0, f"?La evidencia del departamento {plan['department']} coincide con el expediente?")
    if "RF-07" in top_flags:
        questions.append("?Donde esta publicada la trazabilidad completa del proceso?")
    if any(not (row.get("url_process") or row.get("secop_url")) for row in rows):
        questions.append("?El enlace publico del proceso puede ser consultado por terceros?")
    return _dedupe_keep_order(questions)[:5]


def _build_graph_suggestions(
    plan: dict[str, Any],
    rows: List[Dict[str, Any]],
    top_flags: List[str],
    validation: dict[str, Any],
) -> List[str]:
    graphs: List[str] = [
        "Distribucion de riesgo por nivel",
        "Top red flags por frecuencia",
        "Comparacion de proveedores o entidades",
    ]
    if any(flag in {"RF-10", "RF-14", "RF-15", "RF-16"} for flag in top_flags):
        graphs.append("Concentracion por proveedor y entidad")
    if any(flag in {"RF-22", "RF-26", "RF-27", "RF-28", "RF-29"} for flag in top_flags):
        graphs.append("Evolucion de valor y adiciones")
    if any(flag in {"RF-30", "RF-31", "RF-32", "RF-33", "RF-34", "RF-35", "RF-36", "RF-37"} for flag in top_flags):
        graphs.append("Cobertura documental y trazabilidad")
    if plan.get("department") or plan.get("municipality"):
        graphs.append("Mapa o tabla territorial por departamento o municipio")
    if rows:
        graphs.append("Ranking de contratos por score y valor")
    if validation.get("observations"):
        graphs.append("Cruce de validacion documental contra evidencia publica")
    return _dedupe_keep_order(graphs)[:6]


def _dedupe_keep_order(items: List[str]) -> List[str]:
    seen = set()
    output: List[str] = []
    for item in items:
        key = normalize_text(item)
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output
