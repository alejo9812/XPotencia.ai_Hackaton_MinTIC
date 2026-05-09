from __future__ import annotations

import json
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from ..paths import OUTPUT_DIR, PROCESSED_DIR
from ..retrieval.search_index import ensure_search_index
from ..risk.opacity_criteria import build_opacity_criteria_report
from ..risk.scoring import score_contracts_frame
from ..storage.duckdb_store import DuckDBStore


TRACEABILITY_FLAGS = {"RF-07", "RF-30", "RF-31", "RF-32", "RF-33", "RF-34", "RF-35", "RF-36", "RF-37"}
PLANNING_FLAGS = {"RF-01", "RF-02", "RF-03", "RF-04", "RF-05", "RF-06", "RF-07"}
COMPETITION_FLAGS = {"RF-08", "RF-09", "RF-10", "RF-11", "RF-12", "RF-13", "RF-14", "RF-15", "RF-16", "RF-23"}
VALUE_FLAGS = {"RF-20", "RF-21"}
EXECUTION_FLAGS = {"RF-22", "RF-24", "RF-25", "RF-26", "RF-27", "RF-28", "RF-29"}
PACO_FLAGS = {"RF-17", "RF-18", "RF-19"}

STAGE_CHECKLISTS: dict[str, tuple[str, ...]] = {
    "planeacion": (
        "Revisar estudios previos y justificacion.",
        "Confirmar modalidad de contratacion.",
        "Validar coherencia entre objeto y presupuesto.",
    ),
    "competencia": (
        "Verificar numero de oferentes.",
        "Analizar concentracion del proveedor.",
        "Comparar contra procesos similares.",
    ),
    "valor": (
        "Cruzar valor estimado con valor adjudicado.",
        "Comparar contra contratos historicos.",
        "Revisar outliers de monto o valor por dia.",
    ),
    "ejecucion": (
        "Cruzar adiciones y prorrogas.",
        "Revisar actas, pagos y soportes.",
        "Confirmar cambios de valor y plazo.",
    ),
    "trazabilidad": (
        "Confirmar URL SECOP y expediente completo.",
        "Buscar soportes de publicacion y fechas.",
        "Cruzar informacion con PACO y fuentes externas.",
    ),
    "paco": (
        "Revisar antecedentes PACO y sanciones.",
        "Escalar hallazgo para revision reforzada.",
        "Documentar el antecedente y su alcance.",
    ),
    "general": (
        "Revisar evidencia documental disponible.",
        "Comparar el caso con procesos similares.",
    ),
}

STAGE_DESCRIPTIONS: dict[str, str] = {
    "planeacion": "El caso apunta a revisar la etapa de planeacion y la modalidad.",
    "competencia": "El caso apunta a la competencia y la concentracion de proveedores.",
    "valor": "El caso apunta a valor atipico o comparacion financiera.",
    "ejecucion": "El caso apunta a ejecucion, adiciones y prorrogas.",
    "trazabilidad": "El caso apunta a trazabilidad documental insuficiente.",
    "paco": "El caso apunta a antecedentes PACO o sanciones.",
    "general": "El caso requiere revision integral con la evidencia disponible.",
    "mixto": "El caso combina varias senales y requiere revision integral.",
}


@dataclass(frozen=True)
class DiagnosticCase:
    case_id: str
    source_kind: str
    stage: str
    contract_id: str
    process_id: str
    entity: str
    supplier: str
    department: str
    municipality: str
    modality: str
    risk_score: int
    risk_level: str
    primary_flags: tuple[str, ...]
    diagnosis: str
    review_checklist: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["primary_flags"] = list(self.primary_flags)
        payload["review_checklist"] = list(self.review_checklist)
        return payload


@dataclass(frozen=True)
class ProcessDiagnosticReport:
    generated_at: str
    overall_status: str
    source_table: str
    total_records: int
    real_case_count: int
    synthetic_case_count: int
    high_risk_count: int
    medium_risk_count: int
    low_risk_count: int
    traceability_gap_count: int
    criteria_status: str
    criteria_coverage_ratio: float
    validation_status: str
    criteria: dict[str, Any]
    validation: dict[str, Any]
    top_departments: tuple[dict[str, Any], ...]
    top_suppliers: tuple[dict[str, Any], ...]
    top_modalities: tuple[dict[str, Any], ...]
    process_steps: tuple[dict[str, Any], ...]
    real_cases: tuple[DiagnosticCase, ...]
    synthetic_cases: tuple[DiagnosticCase, ...]
    gaps: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["top_departments"] = [dict(item) for item in self.top_departments]
        payload["top_suppliers"] = [dict(item) for item in self.top_suppliers]
        payload["top_modalities"] = [dict(item) for item in self.top_modalities]
        payload["process_steps"] = [dict(item) for item in self.process_steps]
        payload["real_cases"] = [item.to_dict() for item in self.real_cases]
        payload["synthetic_cases"] = [item.to_dict() for item in self.synthetic_cases]
        payload["gaps"] = list(self.gaps)
        return payload


def build_process_diagnostic_report(
    store: DuckDBStore,
    *,
    processed_dir: Path | None = None,
    limit: int = 8,
    synthetic_count: int = 4,
) -> ProcessDiagnosticReport:
    processed_dir = processed_dir or PROCESSED_DIR
    processed_dir = Path(processed_dir)
    processed_dir.mkdir(parents=True, exist_ok=True)

    if any(store.has_table(name) for name in ("pae_contracts_scored", "pae_contracts_core", "pae_contracts_enriched", "pae_processes", "pae_additions")):
        ensure_search_index(store, processed_dir)

    with ThreadPoolExecutor(max_workers=2) as executor:
        criteria_future = executor.submit(build_opacity_criteria_report, store, processed_dir=processed_dir)
        validation_future = executor.submit(_latest_validation_run, store)
        criteria_report = criteria_future.result()
        latest_validation = validation_future.result()

    scored_frame, source_table = _load_scored_frame(store)
    total_records = int(len(scored_frame))
    real_cases = _build_real_cases(scored_frame, limit=limit)
    synthetic_cases = _build_synthetic_cases(max(0, int(synthetic_count)))

    risk_levels = _risk_level_counts(scored_frame)
    traceability_gap_count = _traceability_gap_count(scored_frame)
    top_departments = _top_counts(scored_frame, ("department", "departamento", "department_name"))
    top_suppliers = _top_counts(scored_frame, ("supplier", "supplier_name", "proveedor"))
    top_modalities = _top_counts(scored_frame, ("modality", "modality_text", "modalidad"))
    validation_status = str(latest_validation.get("overall_status") or "not_run")
    criteria_status = str(criteria_report.overall_status)
    gaps = _build_gaps(total_records, criteria_report, latest_validation, traceability_gap_count)
    overall_status = "ready" if total_records > 0 else "synthetic_only"

    process_steps = (
        {
            "stage": "planeacion",
            "focus": "Modalidad, justificacion, objeto y presupuesto.",
            "what_to_check": "Verificar si la planeacion explica la modalidad y el valor.",
        },
        {
            "stage": "competencia",
            "focus": "Oferentes, pliegos y concentracion de proveedores.",
            "what_to_check": "Confirmar si hubo competencia real o un unico participante.",
        },
        {
            "stage": "valor",
            "focus": "Valor estimado, valor adjudicado y outliers.",
            "what_to_check": "Comparar contra contratos similares y contra el estimado.",
        },
        {
            "stage": "ejecucion",
            "focus": "Adiciones, prorrogas, pagos y cambios de alcance.",
            "what_to_check": "Confirmar si la ejecucion coincide con el contrato original.",
        },
        {
            "stage": "trazabilidad",
            "focus": "URL SECOP, soportes, expediente y fuentes cruzadas.",
            "what_to_check": "Corroborar trazabilidad completa y evidencia documental.",
        },
        {
            "stage": "paco",
            "focus": "Antecedentes disciplinarios, fiscales y contractuales.",
            "what_to_check": "Revisar si existen sanciones o antecedentes relacionados.",
        },
    )

    return ProcessDiagnosticReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        overall_status=overall_status,
        source_table=source_table,
        total_records=total_records,
        real_case_count=len(real_cases),
        synthetic_case_count=len(synthetic_cases),
        high_risk_count=risk_levels["high"],
        medium_risk_count=risk_levels["medium"],
        low_risk_count=risk_levels["low"],
        traceability_gap_count=traceability_gap_count,
        criteria_status=criteria_status,
        criteria_coverage_ratio=float(criteria_report.coverage_ratio),
        validation_status=validation_status,
        criteria=criteria_report.to_dict(),
        validation=latest_validation,
        top_departments=top_departments,
        top_suppliers=top_suppliers,
        top_modalities=top_modalities,
        process_steps=process_steps,
        real_cases=tuple(real_cases),
        synthetic_cases=tuple(synthetic_cases),
        gaps=tuple(gaps),
    )


def write_process_diagnostic_report(
    store: DuckDBStore,
    *,
    processed_dir: Path | None = None,
    output_json: Path | None = None,
    output_csv: Path | None = None,
    limit: int = 8,
    synthetic_count: int = 4,
) -> ProcessDiagnosticReport:
    report = build_process_diagnostic_report(
        store,
        processed_dir=processed_dir,
        limit=limit,
        synthetic_count=synthetic_count,
    )
    output_json = Path(output_json or (OUTPUT_DIR / "process_diagnostics.json"))
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(report.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")

    if output_csv is not None:
        rows = [case.to_dict() | {"case_kind": "real"} for case in report.real_cases]
        rows.extend(case.to_dict() | {"case_kind": "synthetic"} for case in report.synthetic_cases)
        if rows:
            pd.DataFrame(rows).to_csv(output_csv, index=False, encoding="utf-8")
        else:
            Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame().to_csv(output_csv, index=False, encoding="utf-8")

    return report


def _load_scored_frame(store: DuckDBStore) -> tuple[pd.DataFrame, str]:
    candidates = ("pae_contracts_scored", "pae_contracts_enriched", "pae_contracts_core")
    for table_name in candidates:
        if not store.has_table(table_name):
            continue
        frame = store.read_frame(f"SELECT * FROM {table_name}")
        if frame.empty:
            continue
        if table_name == "pae_contracts_scored":
            return frame, table_name
        scored, _ = score_contracts_frame(frame, external_tables=_load_context_tables(store))
        return scored, table_name
    return pd.DataFrame(), ""


def _build_real_cases(frame: pd.DataFrame, *, limit: int) -> list[DiagnosticCase]:
    if frame.empty:
        return []
    ordered = frame.copy()
    if "risk_score" in ordered.columns:
        score_series = pd.to_numeric(ordered["risk_score"], errors="coerce").fillna(0)
    else:
        score_series = pd.Series([0] * len(ordered), index=ordered.index)
    if "amount" in ordered.columns:
        amount_series = pd.to_numeric(ordered["amount"], errors="coerce").fillna(0)
    else:
        amount_series = pd.Series([0] * len(ordered), index=ordered.index)
    ordered = ordered.assign(_score_sort=score_series, _amount_sort=amount_series)
    ordered = ordered.sort_values(["_score_sort", "_amount_sort"], ascending=[False, False], na_position="last").head(max(1, int(limit)))
    return [_case_from_row(row, source_kind="real", index=index) for index, row in enumerate(ordered.to_dict(orient="records"), start=1)]


def _build_synthetic_cases(limit: int) -> list[DiagnosticCase]:
    if limit <= 0:
        return []
    templates = (
        {
            "case_id": "SYN-PL-001",
            "stage": "planeacion",
            "contract_id": "SYN-C-001",
            "process_id": "SYN-P-001",
            "entity": "Municipio de Ejemplo Norte",
            "supplier": "Proveedor Simulado Uno",
            "department": "Tolima",
            "municipality": "Ibague",
            "modality": "Contratacion Directa",
            "risk_score": 86,
            "risk_level": "critico",
            "primary_flags": ("RF-01", "RF-03", "RF-07"),
            "diagnosis": "Planeacion debil: modalidad directa, ventana escolar y trazabilidad incompleta.",
        },
        {
            "case_id": "SYN-CO-001",
            "stage": "competencia",
            "contract_id": "SYN-C-002",
            "process_id": "SYN-P-002",
            "entity": "Municipio de Ejemplo Centro",
            "supplier": "Proveedor Simulado Dos",
            "department": "Cundinamarca",
            "municipality": "Soacha",
            "modality": "Licitacion Publica",
            "risk_score": 78,
            "risk_level": "alto",
            "primary_flags": ("RF-08", "RF-09", "RF-14"),
            "diagnosis": "Baja competencia y concentracion del proveedor; comparar oferentes y historicos.",
        },
        {
            "case_id": "SYN-EJ-001",
            "stage": "ejecucion",
            "contract_id": "SYN-C-003",
            "process_id": "SYN-P-003",
            "entity": "Entidad Educativa Simulada",
            "supplier": "Union Temporal Diagnostico",
            "department": "Antioquia",
            "municipality": "Medellin",
            "modality": "Regimen Especial",
            "risk_score": 74,
            "risk_level": "alto",
            "primary_flags": ("RF-22", "RF-26", "RF-27", "RF-28"),
            "diagnosis": "Ejecucion con adiciones y prorrogas; contrastar actas, pagos y cambios de alcance.",
        },
        {
            "case_id": "SYN-TR-001",
            "stage": "trazabilidad",
            "contract_id": "SYN-C-004",
            "process_id": "SYN-P-004",
            "entity": "Entidad de Ejemplo Sur",
            "supplier": "Proveedor Simulado Cuatro",
            "department": "Valle del Cauca",
            "municipality": "Cali",
            "modality": "Licitacion Publica",
            "risk_score": 81,
            "risk_level": "critico",
            "primary_flags": ("RF-30", "RF-33", "RF-34", "RF-36", "RF-37"),
            "diagnosis": "Trazabilidad incompleta: revisar URL SECOP, fechas, soportes y evidencia documental.",
        },
    )
    cases = [
        DiagnosticCase(
            case_id=template["case_id"],
            source_kind="synthetic",
            stage=template["stage"],
            contract_id=template["contract_id"],
            process_id=template["process_id"],
            entity=template["entity"],
            supplier=template["supplier"],
            department=template["department"],
            municipality=template["municipality"],
            modality=template["modality"],
            risk_score=int(template["risk_score"]),
            risk_level=template["risk_level"],
            primary_flags=tuple(template["primary_flags"]),
            diagnosis=template["diagnosis"],
            review_checklist=_checklist_for_stage(template["stage"]),
        )
        for template in templates[:limit]
    ]
    return cases


def _case_from_row(row: dict[str, Any], *, source_kind: str, index: int) -> DiagnosticCase:
    primary_flags = tuple(_extract_flag_codes(row))
    stage = _diagnostic_stage(primary_flags)
    diagnosis = _build_diagnosis(primary_flags, row, stage)
    return DiagnosticCase(
        case_id=str(row.get("contract_id") or row.get("process_id") or f"{source_kind.upper()}-{index:03d}"),
        source_kind=source_kind,
        stage=stage,
        contract_id=_first_text(row, "contract_id", "id_contrato", "record_id"),
        process_id=_first_text(row, "process_id", "proceso_de_compra", "id_del_proceso"),
        entity=_first_text(row, "entity", "entity_name", "entidad", "nombre_entidad"),
        supplier=_first_text(row, "supplier", "supplier_name", "proveedor", "proveedor_adjudicado"),
        department=_first_text(row, "department", "departamento", "department_name"),
        municipality=_first_text(row, "municipality", "municipio", "municipality_name"),
        modality=_first_text(row, "modality", "modality_text", "modalidad", "modalidad_de_contratacion"),
        risk_score=int(_first_number(row, "risk_score", "score") or 0),
        risk_level=_first_text(row, "risk_level") or _resolve_risk_level(int(_first_number(row, "risk_score", "score") or 0)),
        primary_flags=primary_flags,
        diagnosis=diagnosis,
        review_checklist=_build_checklist(primary_flags, row, stage),
    )


def _build_diagnosis(primary_flags: tuple[str, ...], row: dict[str, Any], stage: str) -> str:
    flags = set(primary_flags)
    parts: list[str] = []
    if flags & TRACEABILITY_FLAGS:
        parts.append("Trazabilidad insuficiente: revisar URL SECOP, fechas, soportes y expediente.")
    if flags & COMPETITION_FLAGS:
        parts.append("Competencia debil: revisar oferentes, pliegos y concentracion de proveedores.")
    if flags & EXECUTION_FLAGS:
        parts.append("Ejecucion con cambios: contrastar adiciones, prorrogas, pagos y actas.")
    if flags & VALUE_FLAGS:
        parts.append("Valor atipico: comparar el monto contra historicos y contratos similares.")
    if flags & PLANNING_FLAGS:
        parts.append("Planeacion sensible: revisar modalidad, justificacion y coherencia del objeto.")
    if flags & PACO_FLAGS:
        parts.append("Antecedente PACO o sancion: elevar la revision documental.")

    limitations = _first_text(row, "limitations", "risk_limitations")
    if limitations:
        parts.append(f"Limitaciones visibles: {limitations}")

    if not parts:
        return STAGE_DESCRIPTIONS.get(stage, STAGE_DESCRIPTIONS["general"])

    return " ".join(parts)


def _build_checklist(primary_flags: tuple[str, ...], row: dict[str, Any], stage: str) -> tuple[str, ...]:
    stages = _bucket_hits(primary_flags)
    if stage not in stages and stage != "general" and stage != "mixto":
        stages = [stage, *stages]
    checklist: list[str] = []
    for bucket in stages or [stage]:
        for item in _checklist_for_stage(bucket):
            if item not in checklist:
                checklist.append(item)
    if _first_text(row, "limitations", "risk_limitations"):
        note = "Usar la limitacion registrada como guia de revision."
        if note not in checklist:
            checklist.append(note)
    return tuple(checklist)


def _bucket_hits(primary_flags: tuple[str, ...]) -> list[str]:
    flags = set(primary_flags)
    hits: list[str] = []
    if flags & PLANNING_FLAGS:
        hits.append("planeacion")
    if flags & COMPETITION_FLAGS:
        hits.append("competencia")
    if flags & VALUE_FLAGS:
        hits.append("valor")
    if flags & EXECUTION_FLAGS:
        hits.append("ejecucion")
    if flags & TRACEABILITY_FLAGS:
        hits.append("trazabilidad")
    if flags & PACO_FLAGS:
        hits.append("paco")
    return hits


def _diagnostic_stage(primary_flags: tuple[str, ...]) -> str:
    hits = _bucket_hits(primary_flags)
    if not hits:
        return "general"
    unique_hits = list(dict.fromkeys(hits))
    if len(unique_hits) == 1:
        return unique_hits[0]
    return "mixto"


def _checklist_for_stage(stage: str) -> tuple[str, ...]:
    return STAGE_CHECKLISTS.get(stage, STAGE_CHECKLISTS["general"])


def _build_gaps(total_records: int, criteria_report: Any, latest_validation: dict[str, Any], traceability_gap_count: int) -> list[str]:
    gaps: list[str] = []
    if total_records <= 0:
        gaps.append("No hay contratos reales cargados; se muestran casos sinteticos de guia.")
    if getattr(criteria_report, "overall_status", "") != "verified":
        gaps.append(f"El mapa de opacidad esta {getattr(criteria_report, 'overall_status', 'desconocido')}.")
    if not latest_validation:
        gaps.append("No existe una corrida de validacion reciente.")
    if traceability_gap_count > 0:
        gaps.append(f"Hay {traceability_gap_count} casos con brechas de trazabilidad.")
    return gaps


def _risk_level_counts(frame: pd.DataFrame) -> dict[str, int]:
    if frame.empty or "risk_level" not in frame.columns:
        return {"high": 0, "medium": 0, "low": 0}
    normalized = frame["risk_level"].fillna("").astype(str).str.lower()
    return {
        "high": int(normalized.isin({"alto", "critico"}).sum()),
        "medium": int(normalized.eq("medio").sum()),
        "low": int(normalized.eq("bajo").sum()),
    }


def _traceability_gap_count(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    count = 0
    for row in frame.to_dict(orient="records"):
        flags = set(_extract_flag_codes(row))
        if flags & TRACEABILITY_FLAGS:
            count += 1
    return count


def _top_counts(frame: pd.DataFrame, candidates: tuple[str, ...]) -> tuple[dict[str, Any], ...]:
    if frame.empty:
        return tuple()
    series = None
    for column in candidates:
        if column in frame.columns:
            series = frame[column]
            break
    if series is None:
        return tuple()
    values = [str(value).strip() for value in series.fillna("").astype(str).tolist() if str(value).strip()]
    counter = Counter(values)
    return tuple({"value": value, "count": count} for value, count in counter.most_common(5))


def _load_context_tables(store: DuckDBStore) -> dict[str, pd.DataFrame]:
    candidates = [
        "pae_additions",
        "additions",
        "paco_events",
        "paco_disciplinary",
        "paco_penal",
        "paco_fiscal",
        "paco_contractual",
        "paco_collusion",
        "sanctions",
    ]
    tables: dict[str, pd.DataFrame] = {}
    for table_name in candidates:
        if not store.has_table(table_name):
            continue
        tables[table_name] = store.read_frame(f"SELECT * FROM {table_name}")
    return tables


def _latest_validation_run(store: DuckDBStore) -> dict[str, Any]:
    if not store.has_table("validation_runs"):
        return {}
    frame = store.query_frame("SELECT * FROM validation_runs ORDER BY created_at DESC LIMIT 1")
    if frame.empty:
        return {}
    return frame.iloc[0].to_dict()


def _extract_flag_codes(row: dict[str, Any]) -> list[str]:
    candidates = (
        "primary_flags",
        "risk_flags",
        "risk_flags_json",
        "red_flags",
        "red_flags_activadas",
        "activated_flags",
    )
    for key in candidates:
        value = row.get(key)
        if value in (None, "", [], {}):
            continue
        if isinstance(value, list):
            return [_flag_code(item) for item in value if _flag_code(item)]
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except Exception:
                parsed = None
            if isinstance(parsed, list):
                return [_flag_code(item) for item in parsed if _flag_code(item)]
            if isinstance(parsed, dict):
                code = _flag_code(parsed)
                return [code] if code else []
            text = value.replace("[", "").replace("]", "").replace('"', "").replace("'", "")
            return [item.strip() for item in text.split(",") if item.strip()]
        if isinstance(value, dict):
            code = _flag_code(value)
            return [code] if code else []
    return []


def _flag_code(item: Any) -> str:
    if isinstance(item, dict):
        return str(item.get("code") or item.get("flag") or "").strip()
    if hasattr(item, "code"):
        return str(getattr(item, "code") or "").strip()
    text = str(item or "").strip()
    return text


def _first_text(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value in (None, "", [], {}):
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _first_number(row: dict[str, Any], *keys: str) -> float:
    for key in keys:
        value = row.get(key)
        if value in (None, "", [], {}):
            continue
        try:
            return float(value)
        except Exception:
            continue
    return 0.0


def _resolve_risk_level(score: int) -> str:
    if score >= 76:
        return "critico"
    if score >= 56:
        return "alto"
    if score >= 31:
        return "medio"
    return "bajo"
