from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any

import json
import pandas as pd

from ..config import load_risk_registry, normalize_text
from .evidence import RiskFlag
from .indicators import DEFAULT_THRESHOLDS, SANCTION_TERMS, build_indicator_bundle


@dataclass(frozen=True)
class RuleContext:
    frame: pd.DataFrame
    config: dict[str, Any]
    thresholds: dict[str, Any]
    stats: dict[str, Any]
    external_tables: dict[str, pd.DataFrame] | None = None


def load_rule_config() -> dict[str, Any]:
    payload = load_risk_registry()
    dimensions = {entry["id"]: int(entry.get("weight", 0)) for entry in payload.get("dimensions", [])}
    flags = payload.get("flags", {})
    return {
        "dimensions": dimensions,
        "flags": flags,
    }


def evaluate_frame(frame: pd.DataFrame, external_tables: dict[str, pd.DataFrame] | None = None) -> tuple[pd.DataFrame, dict[str, Any]]:
    config = load_rule_config()
    thresholds = DEFAULT_THRESHOLDS.copy()
    enriched_frame = _attach_additions_context(frame, external_tables)
    bundle = build_indicator_bundle(enriched_frame, thresholds)
    ctx = RuleContext(bundle.frame, config, bundle.thresholds, bundle.stats, external_tables=external_tables)

    assessments: list[Any] = []
    for _, row in ctx.frame.iterrows():
        assessment = assess_row(row, ctx)
        assessments.append(assessment)

    scored = ctx.frame.copy()
    scored["risk_score"] = [item.risk_score for item in assessments]
    scored["risk_level"] = [item.risk_level for item in assessments]
    scored["risk_flags"] = [item.flags for item in assessments]
    scored["risk_flags_json"] = [json.dumps([flag.to_dict() for flag in item.flags], ensure_ascii=False) for item in assessments]
    scored["risk_dimension_scores_json"] = [json.dumps(item.dimension_scores, ensure_ascii=False) for item in assessments]
    scored["risk_summary"] = [item.summary for item in assessments]
    scored["risk_limitations"] = [item.limitations for item in assessments]

    summary = build_summary(scored, assessments)
    return scored, summary


def assess_row(row: pd.Series, ctx: RuleContext):
    row = _normalize_row_values(row)
    flags: list[RiskFlag] = []
    flag_defs = ctx.config["flags"]

    def add(code: str, evidence: str) -> None:
        definition = flag_defs.get(code)
        if not definition:
            return
        weight = int(definition.get("weight", 0))
        flags.append(
            RiskFlag(
                code=code,
                dimension=str(definition.get("dimension", "unknown")),
                label=str(definition.get("label", code)),
                weight=weight,
                evidence=evidence,
                source="SECOP II",
            )
        )

    normalized_modality = normalize_text(row.get("modality"))
    object_text = str(row.get("object_text") or "")
    normalized_object = normalize_text(object_text)
    justification = str(row.get("justification") or "")
    normalized_justification = normalize_text(justification)
    supplier = str(row.get("supplier_name") or "")
    entity = str(row.get("entity_name") or "")
    supplier_repeat = int(row.get("supplier_contract_count_same_entity") or 0)
    supplier_share = float(row.get("supplier_repeat_share_same_entity") or 0)
    supplier_amount_share = float(row.get("supplier_amount_share_same_entity") or 0)
    similar_texts = int(row.get("similar_text_count_same_entity") or 0)
    amount = float(row.get("amount") or 0)
    estimated_amount = float(row.get("estimated_amount") or 0)
    participants = int(row.get("participants") or 0)
    offers = int(row.get("offers") or 0)
    duration_days = int(row.get("effective_duration_days") or 0)
    addition_value = float(row.get("addition_value") or 0)
    addition_count = int(row.get("addition_count") or 0)
    addition_ratio = float(row.get("addition_ratio") or 0)
    value_per_day = float(row.get("value_per_day") or 0)
    p90 = float(ctx.stats.get("amount_p90") or 0)
    p95 = float(ctx.stats.get("amount_p95") or 0)
    value_per_day_p95 = float(ctx.stats.get("value_per_day_p95") or 0)
    amount_median = float(ctx.stats.get("amount_median") or 0)
    amount_mad = float(ctx.stats.get("amount_mad") or 0)
    amount_iqr = float(ctx.stats.get("amount_iqr") or 0)
    entity_median = float(row.get("entity_median_amount") or 0)
    entity_iqr = float(row.get("entity_amount_iqr") or 0)
    missing_fields = list(row.get("missing_critical_fields") or [])
    year_month = int(row.get("contract_month") or 0)

    # Planning
    if normalized_modality and ("LICITACION PUBLICA" not in normalized_modality) and not normalized_justification:
        add("RF-01", f"Modalidad {normalized_modality or 'sin dato'} without visible justification.")

    if ("CONTRATACION DIRECTA" in normalized_modality or normalized_modality == "DIRECTA") and (
        supplier_repeat >= 2 or supplier_share >= 0.5
    ):
        add("RF-02", f"Direct contracting appears recurrent with {supplier_repeat} contracts for the same entity-supplier pair.")

    if year_month in ctx.thresholds["school_start_months"] and normalized_object:
        add("RF-03", f"Contract date falls in the school start window and the object is {object_text[:120]!r}.")

    if row.get("description_is_generic"):
        add("RF-04", f"Generic object text with {int(row.get('object_word_count') or 0)} words and {int(row.get('generic_term_hits') or 0)} generic hits.")

    if similar_texts >= 1:
        add("RF-05", f"Repeated or highly similar object text within the same entity ({similar_texts} similar records).")

    if duration_days and duration_days <= ctx.thresholds["short_duration_days"] and similar_texts >= 1:
        add("RF-06", f"Short duration ({duration_days} days) repeated with similar text.")

    if not row.get("has_url_process", True):
        add("RF-07", "No public process URL is present in the record.")

    # Competition
    if participants <= 1 or offers <= 1:
        add("RF-08", f"Low competition: participants={participants}, offers={offers}.")

    if amount >= p90 and participants <= 2:
        add("RF-09", f"High value contract ({amount:.0f}) with only {participants} participants.")

    if supplier_repeat >= ctx.thresholds["repeat_supplier_count"]:
        add("RF-10", f"Supplier repeats {supplier_repeat} times within the same entity.")

    if int(row.get("entity_contract_count") or 0) >= 3 and int(row.get("supplier_entity_count") or 0) <= 2:
        add("RF-11", f"Same small bidder pool across the entity ({int(row.get('supplier_entity_count') or 0)} suppliers).")

    if participants > 0 and offers <= max(1, participants // 2):
        add("RF-12", f"High rejection or low offer rate: offers={offers}, participants={participants}.")

    if estimated_amount > 0 and abs(amount - estimated_amount) / estimated_amount <= 0.05:
        add("RF-13", f"Adjudicated value is very close to the estimated amount ({estimated_amount:.0f}).")

    if supplier_share >= 0.5 or supplier_amount_share >= 0.5:
        add("RF-14", f"Supplier concentration is high in this entity (share={supplier_share:.2f}, amount_share={supplier_amount_share:.2f}).")

    if int(row.get("supplier_municipality_count") or 0) >= 2:
        add("RF-15", f"Supplier appears in {int(row.get('supplier_municipality_count') or 0)} municipalities.")

    if any(term in normalize_text(supplier) for term in {"CONSORCIO", "UNION TEMPORAL"}):
        add("RF-16", f"Supplier name suggests a consortium or temporary union: {supplier}.")

    # PACO / sanctions context - optional
    sanctions = _optional_sanctions_hit(ctx.external_tables, row)
    if sanctions:
        add("RF-17", str(sanctions.get("message") or "PACO context matched sanction-related antecedents."))
        event_type = str(sanctions.get("event_type") or "")
        if event_type == "fiscal_responsibility":
            add("RF-18", str(sanctions.get("message") or "PACO fiscal responsibility antecedent matched."))
        if event_type == "collusion_case":
            add("RF-19", str(sanctions.get("message") or "PACO collusion antecedent matched."))

    # Value outlier
    robust_z = _robust_z_score(amount, amount_median, amount_mad)
    if amount >= p95 or robust_z >= 3.5:
        add("RF-20", f"Amount {amount:.0f} is a robust outlier (p95={p95:.0f}, robust_z={robust_z:.2f}).")

    if duration_days > 0:
        value_per_day_outlier = value_per_day >= value_per_day_p95 or _robust_z_score(value_per_day, float(ctx.stats.get("value_per_day_median") or 0), float(ctx.stats.get("value_per_day_mad") or 0)) >= 3.5
        if value_per_day_outlier:
            add("RF-21", f"Value per day is atypical ({value_per_day:.0f} vs p95={value_per_day_p95:.0f}).")

    # Execution
    if addition_count >= 2 or (addition_value > 0 and addition_ratio >= ctx.thresholds["addition_ratio_high"]):
        add("RF-22", f"Addition ratio is high ({addition_ratio:.2%}).")

    if amount >= p90 and (participants <= 2 or offers <= 1):
        add("RF-23", f"High value and low competition: amount={amount:.0f}, participants={participants}, offers={offers}.")

    if addition_count > 0 or addition_value > 0:
        add("RF-26", f"Additions present with addition_count={addition_count}.")

    if addition_count > 0 or int(row.get("addition_days") or 0) > 0 or duration_days > 0 and int(row.get("addition_days") or 0) >= 30:
        add("RF-27", f"Time extension observed ({int(row.get('addition_days') or 0)} additional days).")

    if addition_count > 0 or addition_value > 0 or int(row.get("addition_days") or 0) > 0:
        add("RF-28", "Modifications or additions are present in the record.")

    if not row.get("has_url_process", True) and not row.get("has_justification", True) and row.get("description_is_generic"):
        add("RF-34", "The record lacks URL, justification, and has generic text, reducing traceability.")

    start_missing = pd.isna(row.get("start_date"))
    end_missing = pd.isna(row.get("end_date"))
    if start_missing or end_missing:
        add("RF-30", "Contract lacks start and/or end dates needed for lifecycle traceability.")
        add("RF-36", "Contract lacks complete lifecycle dates for traceability.")

    # Documented missing critical fields
    if missing_fields:
        add("RF-30", f"Critical fields missing: {', '.join(missing_fields[:4])}.")

    return _finalize_assessment(row, flags, ctx)


def _finalize_assessment(row: pd.Series, flags: list[RiskFlag], ctx: RuleContext):
    dimension_caps = ctx.config["dimensions"]
    dimension_scores: dict[str, int] = defaultdict(int)
    for flag in flags:
        dimension_scores[flag.dimension] += flag.weight

    capped_scores = {dimension: min(score, dimension_caps.get(dimension, score)) for dimension, score in dimension_scores.items()}
    risk_score = min(100, int(round(sum(capped_scores.values()))))
    risk_level = _resolve_level(risk_score)
    summary = _build_summary(row, risk_score, risk_level, flags)
    limitations = _build_limitations(row, ctx)
    return _assessment_from_values(
        contract_id=str(row.get("contract_id") or row.get("process_id") or ""),
        risk_score=risk_score,
        risk_level=risk_level,
        flags=flags,
        summary=summary,
        limitations=limitations,
        dimension_scores=dict(sorted(capped_scores.items())),
    )


def _assessment_from_values(**kwargs):
    from .evidence import RiskAssessment

    return RiskAssessment(**kwargs)


def _resolve_level(score: int) -> str:
    if score >= 76:
        return "critico"
    if score >= 56:
        return "alto"
    if score >= 31:
        return "medio"
    return "bajo"


def build_summary(scored: pd.DataFrame, assessments: list[Any]) -> dict[str, Any]:
    level_counts = scored["risk_level"].value_counts().to_dict()
    flag_counts: dict[str, int] = {}
    for assessment in assessments:
        for flag in assessment.flags:
            flag_counts[flag.code] = flag_counts.get(flag.code, 0) + 1
    top_flags = [
        {"code": code, "count": count}
        for code, count in sorted(flag_counts.items(), key=lambda item: (-item[1], item[0]))[:10]
    ]
    return {
        "total_records": int(len(scored)),
        "average_score": round(float(scored["risk_score"].mean()), 2) if not scored.empty else 0.0,
        "level_counts": {
            "bajo": int(level_counts.get("bajo", 0)),
            "medio": int(level_counts.get("medio", 0)),
            "alto": int(level_counts.get("alto", 0)),
            "critico": int(level_counts.get("critico", 0)),
        },
        "top_flags": top_flags,
    }


def _build_summary(row: pd.Series, score: int, level: str, flags: list[RiskFlag]) -> str:
    if not flags:
        return f"Contrato {row.get('contract_id') or row.get('process_id') or 'sin-id'} con score {score}/100 ({level}). No se activaron reglas relevantes."
    flag_labels = ", ".join(flag.label for flag in flags[:3])
    return (
        f"Contrato {row.get('contract_id') or row.get('process_id') or 'sin-id'} con score {score}/100 ({level}). "
        f"Principales señales: {flag_labels}."
    )


def _build_limitations(row: pd.Series, ctx: RuleContext) -> str:
    missing = list(row.get("missing_critical_fields") or [])
    notes = []
    if missing:
        notes.append(f"Campos faltantes: {', '.join(missing[:5])}.")
    if float(row.get("estimated_amount") or 0) <= 0:
        notes.append("No hay valor estimado, por lo que las comparaciones de adjudicacion son parciales.")
    if int(row.get("addition_value") or 0) <= 0 and int(row.get("addition_days") or 0) <= 0:
        notes.append("No se observa informacion de adiciones en el contrato core.")
    if ctx.external_tables is None:
        notes.append("No se cruzaron tablas secundarias como sanciones, pagos o adiciones.")
    return " ".join(notes) if notes else "Sin limitaciones relevantes detectadas con las columnas disponibles."


def _optional_sanctions_hit(external_tables: dict[str, pd.DataFrame] | None, row: pd.Series) -> dict[str, str] | None:
    if not external_tables:
        return None
    supplier_doc = normalize_text(str(row.get("supplier_doc") or ""))
    supplier_name = normalize_text(str(row.get("supplier_name") or ""))
    entity_doc = normalize_text(str(row.get("entity_doc") or row.get("entity_nit") or row.get("entity_id") or ""))
    entity_name = normalize_text(str(row.get("entity_name") or ""))
    contract_id = normalize_text(str(row.get("contract_id") or ""))
    process_id = normalize_text(str(row.get("process_id") or ""))

    match_targets = [supplier_doc, supplier_name, entity_doc, entity_name, contract_id, process_id]
    match_targets = [value for value in match_targets if value]

    for table_name, sanctions in external_tables.items():
        if sanctions is None or sanctions.empty:
            continue
        if not (table_name == "sanctions" or table_name.startswith("paco_")):
            continue

        candidate_columns = [column for column in ("subject_doc", "subject_name", "entity_doc", "entity_name", "reference", "description", "search_text", "event_type") if column in sanctions.columns]
        if not candidate_columns:
            continue

        normalized_frame = sanctions[candidate_columns].copy()
        for column in candidate_columns:
            normalized_frame[column] = normalized_frame[column].fillna("").astype(str).map(normalize_text)
        match_mask = pd.Series(False, index=sanctions.index)
        for target in match_targets:
            for column in candidate_columns:
                match_mask |= normalized_frame[column].eq(target)
                if column == "search_text" and target:
                    match_mask |= normalized_frame[column].str.contains(target, na=False)

        if not bool(match_mask.any()):
            row_blob = normalize_text(" ".join(str(value) for value in sanctions.fillna("").astype(str).head(20).to_numpy().ravel()))
            if not any(term in row_blob for term in SANCTION_TERMS):
                continue
            match_mask = pd.Series([True] * len(sanctions), index=sanctions.index)

        matched = sanctions.loc[match_mask]
        event_type = ""
        if "event_type" in matched.columns:
            event_type = next((str(value) for value in matched["event_type"].fillna("").astype(str).tolist() if str(value).strip()), "")
        if not event_type:
            event_type = _fallback_paco_event_type(table_name)
        return {
            "message": f"PACO context matched in {table_name}.",
            "event_type": event_type,
            "source_table": table_name,
        }
    return None


def _attach_additions_context(frame: pd.DataFrame, external_tables: dict[str, pd.DataFrame] | None) -> pd.DataFrame:
    if not external_tables:
        return frame
    additions = external_tables.get("additions")
    if additions is None:
        additions = external_tables.get("pae_additions")
    if additions is None or additions.empty:
        return frame

    additions_frame = additions.copy()
    if "contract_id" not in additions_frame.columns:
        return frame

    additions_frame["contract_id"] = additions_frame["contract_id"].fillna("").astype(str)
    additions_frame = additions_frame[additions_frame["contract_id"].str.strip().ne("")]
    if additions_frame.empty:
        return frame

    grouped = additions_frame.groupby("contract_id", dropna=False)
    summary = grouped.agg(
        addition_count=("contract_id", "count"),
        addition_last_date=("addition_date", "max"),
    ).reset_index()
    summary["has_additions_ctx"] = summary["addition_count"].fillna(0).astype(int) > 0
    summary["addition_count_ctx"] = summary["addition_count"].fillna(0).astype(int)
    summary = summary.rename(columns={"addition_last_date": "addition_last_date_ctx"})
    summary = summary[["contract_id", "addition_count_ctx", "addition_last_date_ctx", "has_additions_ctx"]]

    merged = frame.copy()
    if "contract_id" not in merged.columns:
        merged["contract_id"] = ""
    merged["contract_id"] = merged["contract_id"].fillna("").astype(str)
    merged = merged.merge(summary, on="contract_id", how="left", suffixes=("", "_add"))
    existing_add_count = pd.to_numeric(merged["addition_count"], errors="coerce") if "addition_count" in merged.columns else pd.Series([0] * len(merged), index=merged.index)
    context_add_count = pd.to_numeric(merged["addition_count_ctx"], errors="coerce") if "addition_count_ctx" in merged.columns else pd.Series([0] * len(merged), index=merged.index)
    merged["addition_count"] = existing_add_count.fillna(0).astype(int)
    merged["addition_count"] = merged["addition_count"].where(merged["addition_count"].gt(0), context_add_count.fillna(0).astype(int))

    existing_last_date = merged["addition_last_date"] if "addition_last_date" in merged.columns else pd.Series([pd.NaT] * len(merged), index=merged.index)
    context_last_date = merged["addition_last_date_ctx"] if "addition_last_date_ctx" in merged.columns else pd.Series([pd.NaT] * len(merged), index=merged.index)
    merged["addition_last_date"] = existing_last_date.where(existing_last_date.notna(), context_last_date)

    if "addition_days" in merged.columns:
        merged["addition_days"] = pd.to_numeric(merged["addition_days"], errors="coerce").fillna(0).astype(int)
    else:
        merged["addition_days"] = 0
    merged["has_additions"] = merged["addition_count"].fillna(0).astype(int).gt(0)
    merged = merged.drop(columns=[column for column in ["addition_count_ctx", "addition_last_date_ctx", "has_additions_ctx"] if column in merged.columns])
    return merged


def _robust_z_score(value: float, median: float, mad: float) -> float:
    if value <= 0:
        return 0.0
    scale = mad * 1.4826 if mad > 0 else 0.0
    if scale <= 0:
        return 0.0
    return abs(value - median) / scale


def _normalize_row_values(row: pd.Series) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in row.items():
        if value is None:
            normalized[key] = None
            continue
        try:
            if pd.isna(value):
                normalized[key] = None
                continue
        except Exception:
            pass
        normalized[key] = value
    return normalized


def _fallback_paco_event_type(table_name: str) -> str:
    if table_name in {"paco_fiscal"}:
        return "fiscal_responsibility"
    if table_name in {"paco_collusion"}:
        return "collusion_case"
    if table_name in {"paco_penal"}:
        return "penal_sanction"
    if table_name in {"paco_disciplinary"}:
        return "disciplinary_sanction"
    if table_name in {"paco_contractual", "sanctions"}:
        return "contractual_sanction"
    return "paco_context"
