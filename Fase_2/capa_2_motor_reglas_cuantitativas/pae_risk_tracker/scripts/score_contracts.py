from __future__ import annotations

import argparse
import json
import math
import re
import warnings
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline_utils import (
    CONFIG_DIR,
    OUTPUT_DIR,
    PROCESSED_DIR,
    load_json_config,
    normalize_digits,
    normalize_text,
    save_json,
)


ENRICHED_CORE_PATH = PROCESSED_DIR / "pae_contracts_enriched.parquet"
RANKING_OUTPUT_PATH = OUTPUT_DIR / "pae_risk_ranking.csv"
RANKING_JSON_OUTPUT_PATH = OUTPUT_DIR / "pae_risk_ranking.json"
AUDIT_OUTPUT_PATH = OUTPUT_DIR / "pae_audit_cards.json"
SCORING_CONFIG_PATH = CONFIG_DIR / "scoring.yml"
RISK_FLAGS_CONFIG_PATH = CONFIG_DIR / "risk_flags.yml"


DIMENSION_FLAG_MAP = {
    "competition": ["RF-08", "RF-09", "RF-11", "RF-12", "RF-13", "RF-20", "RF-21", "RF-23"],
    "supplier_concentration": ["RF-10", "RF-14", "RF-15", "RF-16"],
    "value_outlier": ["RF-20", "RF-21"],
    "execution": ["RF-22", "RF-24", "RF-25", "RF-26", "RF-27", "RF-28", "RF-29"],
    "paco_context": ["RF-17", "RF-18", "RF-19"],
    "traceability": ["RF-30", "RF-31", "RF-32", "RF-33", "RF-34", "RF-35", "RF-36", "RF-37"],
    "planning": ["RF-01", "RF-02", "RF-03", "RF-04", "RF-05", "RF-06", "RF-07"],
}

MISSING_LAYER_CHECKS = [
    "Cruzar adiciones, pagos, facturas, garantias y sanciones contractuales.",
    "Cruzar antecedentes PACO por proveedor, entidad y contrato.",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build an explainable PAE risk ranking and audit cards.")
    parser.add_argument("--enriched", type=str, default=str(ENRICHED_CORE_PATH))
    parser.add_argument("--ranking-output", type=str, default=str(RANKING_OUTPUT_PATH))
    parser.add_argument("--ranking-json-output", type=str, default=str(RANKING_JSON_OUTPUT_PATH))
    parser.add_argument("--audit-output", type=str, default=str(AUDIT_OUTPUT_PATH))
    return parser.parse_args()


def main() -> None:
    warnings.filterwarnings("ignore", message="Mean of empty slice", category=RuntimeWarning)
    args = parse_args()
    scoring_config = load_json_config(SCORING_CONFIG_PATH)
    risk_flags_config = load_json_config(RISK_FLAGS_CONFIG_PATH)

    frame = pd.read_parquet(Path(args.enriched))
    if frame.empty:
        raise SystemExit("Enriched PAE parquet is empty.")

    scored = canonicalize_contracts(frame)
    scored = add_derived_metrics(scored)
    scored = add_risk_flags(scored)
    scored, dimension_scores = apply_scoring(scored, scoring_config, risk_flags_config)

    ranking = build_ranking(scored, dimension_scores)
    audit_cards = build_audit_cards(scored, dimension_scores, risk_flags_config)

    ranking_path = Path(args.ranking_output)
    ranking_path.parent.mkdir(parents=True, exist_ok=True)
    ranking.to_csv(ranking_path, index=False, encoding="utf-8")
    ranking_json_path = Path(args.ranking_json_output)
    ranking_records = ranking.copy()
    ranking_records["red_flags"] = ranking_records["red_flags"].apply(parse_json_field)
    ranking_records["evidence"] = ranking_records["evidence"].apply(parse_json_field)
    ranking_records["dimension_scores"] = ranking_records["dimension_scores"].apply(parse_json_field)
    save_json(ranking_json_path, sanitize_json_payload(ranking_records.to_dict(orient="records")))
    save_json(Path(args.audit_output), sanitize_json_payload(audit_cards))

    print(f"Ranking written to {ranking_path}")
    print(f"Ranking JSON written to {ranking_json_path}")
    print(f"Audit cards written to {args.audit_output}")


def canonicalize_contracts(frame: pd.DataFrame) -> pd.DataFrame:
    scored = frame.copy()
    scored["contract_id"] = coalesce_series(scored, ["id_contrato"])
    scored["process_id"] = coalesce_series(scored, ["proceso_de_compra", "id_del_portafolio"])
    scored["entity_name"] = coalesce_series(scored, ["nombre_entidad", "entidad", "entidad_compradora"])
    scored["entity_nit"] = coalesce_series(scored, ["nit_entidad", "entity_nit", "codigo_entidad_creadora", "codigo_entidad", "entity_doc"])
    scored["supplier_name"] = coalesce_series(scored, ["proveedor_adjudicado", "proveedor"])
    scored["supplier_doc"] = coalesce_series(scored, ["documento_proveedor", "nit_proveedor", "supplier_nit"])
    scored["object_text"] = coalesce_series(scored, ["descripcion_del_proceso", "nombre_del_procedimiento"])
    scored["modality_text"] = coalesce_series(scored, ["modalidad_de_contratacion_x", "modalidad_de_contratacion_y", "modalidad_de_contratacion"])
    scored["department_name"] = coalesce_series(scored, ["departamento"])
    scored["municipality_name"] = coalesce_series(scored, ["ciudad", "municipio"])
    scored["status"] = coalesce_series(scored, ["estado_contrato", "estado"])
    scored["start_date"] = coalesce_series(scored, ["fecha_de_firma", "fecha_de_publicacion_del"])
    scored["end_date"] = coalesce_series(scored, ["fecha_de_terminacion", "fecha_de_fin"])
    scored["url_secop"] = coalesce_series(scored, ["urlproceso_x", "urlproceso_y", "urlproceso"])
    scored["process_url"] = scored["url_secop"].map(extract_url)

    scored["entity_norm"] = scored["entity_name"].map(normalize_text)
    scored["supplier_norm"] = scored["supplier_name"].map(normalize_text)
    scored["supplier_doc_norm"] = scored["supplier_doc"].map(normalize_digits_or_empty)
    scored["entity_nit"] = scored["entity_nit"].map(normalize_digits_or_empty)
    scored["supplier_nit"] = scored["supplier_doc_norm"]
    scored["object_norm"] = scored["object_text"].map(normalize_text)
    scored["modality_norm"] = scored["modality_text"].map(normalize_text)
    scored["department_norm"] = scored["department_name"].map(normalize_text)
    scored["municipality_norm"] = scored["municipality_name"].map(normalize_text)

    scored["core_year"] = scored.get("core_year", pd.Series([pd.NA] * len(scored))).fillna(
        pd.to_datetime(scored.get("fecha_de_firma"), errors="coerce").dt.year
    )
    process_year_series = pd.to_datetime(scored.get("fecha_de_publicacion_del"), errors="coerce").dt.year
    if "process_year" in scored.columns:
        scored["process_year"] = pd.to_numeric(scored["process_year"], errors="coerce").combine_first(process_year_series)
    else:
        scored["process_year"] = process_year_series
    scored["signature_month"] = pd.to_datetime(scored.get("fecha_de_firma"), errors="coerce").dt.month
    scored["year"] = pd.to_numeric(scored.get("core_year"), errors="coerce").combine_first(
        pd.to_numeric(scored.get("process_year"), errors="coerce")
    )
    scored["month"] = scored["signature_month"]

    scored["value_initial"] = scored.apply(
        lambda row: first_numeric(row, ["precio_base"]),
        axis=1,
    )
    scored["value_final_raw"] = scored.apply(
        lambda row: first_numeric(row, ["valor_total_adjudicacion"]),
        axis=1,
    )
    scored["value_final"] = scored["value_final_raw"].where(scored["value_final_raw"].gt(0), scored["value_initial"])
    scored["contract_value"] = scored.apply(
        lambda row: first_numeric(row, ["valor_total_adjudicacion", "precio_base"]),
        axis=1,
    )
    scored["duration_raw"] = scored.apply(lambda row: first_numeric(row, ["duracion"]), axis=1)
    scored["duration_unit"] = scored.apply(lambda row: first_text(row, ["unidad_de_duracion"]), axis=1)
    scored["duration_days"] = scored.apply(resolve_duration_days, axis=1)
    scored["value_per_day"] = scored.apply(resolve_value_per_day, axis=1)

    scored["num_oferentes_reported"] = scored.apply(lambda row: first_numeric(row, ["num_oferentes"]), axis=1)
    scored["num_oferentes_effective"] = scored["num_oferentes_reported"].where(scored["num_oferentes_reported"] > 0)
    scored["bidder_rows"] = scored.apply(lambda row: first_numeric(row, ["bidder_rows"]), axis=1)
    scored["unique_suppliers"] = scored.apply(lambda row: first_numeric(row, ["unique_suppliers"]), axis=1)
    scored["competition_signal"] = scored.apply(lambda row: first_text(row, ["competition_signal"]), axis=1)

    scored["entity_total_contracts"] = scored.groupby("entity_norm")["contract_id"].transform("count")
    scored["supplier_total_contracts"] = scored.groupby(["entity_norm", "supplier_norm"])["contract_id"].transform("count")
    scored["supplier_total_pae_contracts"] = scored.groupby("supplier_norm")["contract_id"].transform("count")
    scored["supplier_total_pae_value"] = scored.groupby("supplier_norm")["contract_value"].transform("sum")
    scored["supplier_municipalities"] = scored.groupby("supplier_norm")["municipality_norm"].transform(lambda series: series.replace("", pd.NA).dropna().nunique())
    entity_totals = scored.groupby("entity_norm")["contract_id"].transform("count").replace(0, pd.NA)
    scored["supplier_share_in_entity"] = scored["supplier_total_contracts"] / entity_totals
    scored["supplier_share_in_entity"] = scored["supplier_share_in_entity"].fillna(0.0)
    scored["same_entity_supplier_recurrent"] = scored["supplier_total_contracts"].fillna(0).astype(int).gt(1)
    scored["same_entity_supplier_high_share"] = scored["entity_total_contracts"].fillna(0).astype(int).ge(2) & scored["supplier_share_in_entity"].ge(0.6)

    value_reference = scored["contract_value"].fillna(0)
    scored["value_percentile_year"] = 0.0
    for _, index in scored.groupby("core_year").groups.items():
        values = scored.loc[index, "contract_value"]
        non_null = values.dropna()
        if non_null.empty:
            continue
        ranks = non_null.rank(method="average")
        scored.loc[non_null.index, "value_percentile_year"] = ranks / float(len(non_null))
    scored["estimated_vs_awarded_ratio"] = scored.apply(resolve_estimated_vs_awarded_ratio, axis=1)

    return scored


def add_derived_metrics(frame: pd.DataFrame) -> pd.DataFrame:
    scored = frame.copy()
    scored["has_process_join"] = scored["process_id"].notna() & scored["process_id"].astype(str).str.strip().ne("")
    scored["has_url"] = scored["url_secop"].notna() & scored["url_secop"].astype(str).str.strip().ne("")
    scored["has_bidder_evidence"] = scored["num_oferentes_reported"].gt(0) | scored["bidder_rows"].gt(0) | scored["unique_suppliers"].gt(0)
    scored["generic_object_hits"] = scored["object_norm"].apply(count_generic_object_hits)
    scored["object_generic_score"] = scored["generic_object_hits"].clip(upper=3)
    scored["non_competitive_modality"] = scored["modality_norm"].apply(is_non_competitive_modality)
    scored["direct_contracting"] = scored["modality_norm"].apply(is_direct_contracting)
    scored["single_bidder_known"] = scored["num_oferentes_effective"].eq(1)
    scored["low_bidder_count_known"] = scored["num_oferentes_effective"].le(2)
    scored["competition_outlier"] = scored["value_percentile_year"].ge(0.8) & scored["low_bidder_count_known"]
    scored["value_outlier"] = scored["value_percentile_year"].ge(0.9)
    scored["high_value_low_competition"] = scored["value_outlier"] & scored["low_bidder_count_known"]
    scored["award_close_to_estimate"] = scored["estimated_vs_awarded_ratio"].between(0.95, 1.05, inclusive="both")
    scored["traceability_incomplete"] = (~scored["has_url"]) | (~scored["has_process_join"]) | (~scored["has_bidder_evidence"])
    scored["recurrent_same_entity_supplier"] = scored["same_entity_supplier_recurrent"]
    scored["supplier_high_share_entity"] = scored["same_entity_supplier_high_share"]
    scored["supplier_many_contracts"] = scored["supplier_total_pae_contracts"].fillna(0).astype(int).ge(3)
    scored["generic_object"] = scored["generic_object_hits"].ge(2)
    scored["possible_fragmentation"] = scored["entity_total_contracts"].fillna(0).astype(int).ge(2) & scored["same_entity_supplier_recurrent"]
    scored["has_additions"] = False
    scored["has_contractual_sanction"] = False
    scored["has_paco_context"] = False
    scored["missing_payment_plan"] = True
    scored["missing_invoices"] = True
    scored["missing_guarantee"] = True
    scored["missing_process_url"] = ~scored["has_url"]
    scored["missing_traceability_support"] = scored["traceability_incomplete"]
    return scored


def add_risk_flags(frame: pd.DataFrame) -> pd.DataFrame:
    scored = frame.copy()

    scored["RF-01"] = scored["non_competitive_modality"]
    scored["RF-02"] = scored["direct_contracting"] & scored["same_entity_supplier_recurrent"]
    scored["RF-03"] = scored["signature_month"].fillna(0).le(2) & scored["signature_month"].gt(0)
    scored["RF-04"] = scored["generic_object"]
    scored["RF-05"] = scored["possible_fragmentation"]
    scored["RF-06"] = scored["duration_days"].fillna(0).gt(0) & scored["duration_days"].le(45) & scored["same_entity_supplier_recurrent"]
    scored["RF-07"] = scored["traceability_incomplete"]

    scored["RF-08"] = scored["single_bidder_known"]
    scored["RF-09"] = scored["low_bidder_count_known"] & scored["contract_value"].fillna(0).ge(scored["contract_value"].median())
    scored["RF-10"] = scored["supplier_many_contracts"]
    scored["RF-11"] = scored["competition_signal"].str.contains("MEDIA|BAJA", case=False, na=False) & scored["supplier_many_contracts"]
    scored["RF-12"] = scored["competition_signal"].str.contains("BAJA", case=False, na=False) & scored["low_bidder_count_known"]
    scored["RF-13"] = scored["award_close_to_estimate"] & scored["value_final_raw"].fillna(0).gt(0)

    scored["RF-14"] = scored["supplier_high_share_entity"]
    scored["RF-15"] = scored["supplier_municipalities"].fillna(0).astype(int).gt(1)
    scored["RF-16"] = scored["supplier_norm"].astype(str).str.contains(" UT | UNION TEMPORAL | CONSORCIO ", case=False, regex=True, na=False)

    scored["RF-17"] = scored["has_contractual_sanction"]
    scored["RF-18"] = scored["has_paco_context"]
    scored["RF-19"] = False

    scored["RF-20"] = scored["value_outlier"]
    scored["RF-21"] = scored["value_per_day"].fillna(0).gt(scored["value_per_day"].median()) & scored["duration_days"].fillna(0).gt(0)

    scored["RF-22"] = scored["has_additions"] & scored["contract_value"].fillna(0).gt(0)
    scored["RF-23"] = scored["high_value_low_competition"]
    scored["RF-24"] = False
    scored["RF-25"] = False

    scored["RF-26"] = scored["has_additions"]
    scored["RF-27"] = False
    scored["RF-28"] = False
    scored["RF-29"] = scored["has_contractual_sanction"]

    scored["RF-30"] = False
    scored["RF-31"] = False
    scored["RF-32"] = False
    scored["RF-33"] = scored["missing_process_url"]
    scored["RF-34"] = scored["traceability_incomplete"]
    scored["RF-35"] = False
    scored["RF-36"] = scored["traceability_incomplete"]
    scored["RF-37"] = scored["traceability_incomplete"] & scored["num_oferentes_reported"].gt(0)

    return scored


def apply_scoring(
    frame: pd.DataFrame,
    scoring_config: dict[str, Any],
    risk_flags_config: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    scored = frame.copy()
    flag_weights = {flag_id: details.get("weight", 0) for flag_id, details in risk_flags_config.get("flags", {}).items()}
    flag_labels = {flag_id: details.get("label", flag_id) for flag_id, details in risk_flags_config.get("flags", {}).items()}
    dimension_weights = {entry["id"]: entry["weight"] for entry in risk_flags_config.get("dimensions", [])}

    for flag_id in flag_weights:
        if flag_id not in scored.columns:
            scored[flag_id] = False

    dimension_rows: list[dict[str, Any]] = []
    total_scores: list[int] = []
    risk_levels: list[str] = []
    activated_flags_col: list[str] = []
    explanations: list[str] = []
    manual_checks_col: list[str] = []

    for _, row in scored.iterrows():
        activated_flags = [flag_id for flag_id in flag_weights if bool(row.get(flag_id))]
        activated_flags_col.append(json.dumps(activated_flags, ensure_ascii=False))

        dimension_scores = {}
        total_score = 0
        for dimension_id, dimension_weight in dimension_weights.items():
            score = 0
            for flag_id in DIMENSION_FLAG_MAP.get(dimension_id, []):
                if bool(row.get(flag_id)):
                    score += int(flag_weights.get(flag_id, 0))
            score = min(int(dimension_weight), score)
            dimension_scores[dimension_id] = score
            total_score += score

        total_score = max(scoring_config.get("score_min", 0), min(scoring_config.get("score_max", 100), int(total_score)))
        total_scores.append(total_score)
        risk_levels.append(resolve_risk_level(total_score, scoring_config.get("levels", [])))
        explanations.append(build_explanation(row, dimension_scores, flag_labels))
        manual_checks_col.append(build_manual_checks(row))
        dimension_rows.append({"dimension_scores": json.dumps(dimension_scores, ensure_ascii=False)})

    scored["score"] = total_scores
    scored["risk_level"] = risk_levels
    scored["activated_flags"] = activated_flags_col
    scored["score_explanation"] = explanations
    scored["required_manual_checks"] = manual_checks_col
    scored["dimension_scores"] = [entry["dimension_scores"] for entry in dimension_rows]
    return scored, pd.DataFrame(dimension_rows)


def build_ranking(frame: pd.DataFrame, dimension_scores: pd.DataFrame) -> pd.DataFrame:
    ranking = frame.copy()
    ranking["dimension_scores"] = dimension_scores["dimension_scores"]
    ranking["entity"] = ranking["entity_name"]
    ranking["department"] = ranking["department_name"]
    ranking["municipality"] = ranking["municipality_name"]
    ranking["object"] = ranking["object_text"]
    ranking["modality"] = ranking["modality_text"]
    ranking["supplier"] = ranking["supplier_name"]
    ranking["initial_value"] = ranking["value_initial"]
    ranking["final_value"] = ranking["value_final"].fillna(ranking["contract_value"])
    ranking["risk_score"] = ranking["score"]
    ranking["red_flags"] = ranking["activated_flags"]
    ranking["evidence"] = ranking.apply(lambda row: json.dumps(sanitize_json_payload(collect_evidence(row)), ensure_ascii=False), axis=1)
    ranking["secop_url"] = ranking["process_url"]
    ranking["recommended_action"] = ranking.apply(build_recommendation, axis=1)
    ranking["limitations"] = ranking.apply(build_limitations, axis=1)
    ranking["risk_summary"] = ranking["score_explanation"]
    ranking["risk_limitations"] = ranking["limitations"]
    ranking["red_flags_activadas"] = ranking["activated_flags"]
    ranking["tiene_sanciones"] = ranking["has_contractual_sanction"] | ranking["has_paco_context"]
    ranking["tiene_antecedente_paco"] = ranking["has_paco_context"]
    ranking["valor_inicial"] = ranking["value_initial"]
    ranking["valor_final"] = ranking["value_final"].fillna(ranking["contract_value"])
    ranking["modalidad"] = ranking["modality_text"]
    ranking["entidad"] = ranking["entity_name"]
    ranking["proveedor"] = ranking["supplier_name"]
    ranking["departamento"] = ranking["department_name"]
    ranking["municipio"] = ranking["municipality_name"]
    ranking["objeto"] = ranking["object_text"]
    ranking["url_secop"] = ranking["process_url"]
    ranking["num_oferentes"] = ranking["num_oferentes_reported"]

    columns = [
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
        "entidad",
        "proveedor",
        "supplier_doc_norm",
        "departamento",
        "municipio",
        "objeto",
        "initial_value",
        "final_value",
        "start_date",
        "end_date",
        "year",
        "month",
        "valor_inicial",
        "valor_final",
        "modalidad",
        "num_oferentes",
        "bidder_rows",
        "unique_suppliers",
        "has_additions",
        "tiene_sanciones",
        "tiene_antecedente_paco",
        "risk_score",
        "score",
        "risk_level",
        "red_flags",
        "red_flags_activadas",
        "evidence",
        "secop_url",
        "recommended_action",
        "limitations",
        "url_secop",
        "risk_summary",
        "risk_limitations",
        "score_explanation",
        "required_manual_checks",
        "dimension_scores",
    ]
    ranking = ranking[columns].sort_values(["score", "valor_final", "valor_inicial"], ascending=[False, False, False], na_position="last")
    return ranking


def build_audit_cards(frame: pd.DataFrame, dimension_scores: pd.DataFrame, risk_flags_config: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    cards: list[dict[str, Any]] = []
    for index, row in frame.reset_index(drop=True).iterrows():
        evidence = collect_evidence(row)
        red_flags = json.loads(row.get("activated_flags", "[]"))
        dimensions = json.loads(dimension_scores.iloc[index]["dimension_scores"])
        recommended_action = build_recommendation(row)
        limitations = build_limitations(row)
        card = {
            "contract_id": row.get("contract_id"),
            "process_id": row.get("process_id"),
            "entity": row.get("entity_name"),
            "entity_nit": row.get("entity_nit"),
            "supplier": row.get("supplier_name"),
            "supplier_nit": row.get("supplier_nit"),
            "department": row.get("department_name"),
            "municipality": row.get("municipality_name"),
            "object": row.get("object_text"),
            "modality": row.get("modality_text"),
            "status": row.get("status"),
            "initial_value": to_json_number(row.get("value_initial")),
            "final_value": to_json_number(row.get("value_final")),
            "start_date": row.get("start_date") or row.get("date") or "",
            "end_date": row.get("end_date") or "",
            "year": to_json_number(row.get("year")),
            "month": to_json_number(row.get("month")),
            "risk_score": int(row.get("score", 0)),
            "risk_level": row.get("risk_level"),
            "red_flags": red_flags,
            "evidence": evidence,
            "secop_url": row.get("process_url"),
            "recommended_action": recommended_action,
            "limitations": limitations,
            "risk_summary": row.get("risk_summary") or row.get("score_explanation"),
            "risk_limitations": row.get("risk_limitations") or limitations,
            "score": int(row.get("score", 0)),
            "risk_level": row.get("risk_level"),
            "summary": {
                "entity": row.get("entity_name"),
                "entity_nit": row.get("entity_nit"),
                "supplier": row.get("supplier_name"),
                "supplier_nit": row.get("supplier_nit"),
                "department": row.get("department_name"),
                "municipality": row.get("municipality_name"),
                "object": row.get("object_text"),
                "modality": row.get("modality_text"),
                "value_initial": to_json_number(row.get("value_initial")),
                "value_final": to_json_number(row.get("value_final")),
                "num_oferentes": to_json_number(row.get("num_oferentes_reported")),
                "duration_days": to_json_number(row.get("duration_days")),
                "secop_url": row.get("process_url"),
                "status": row.get("status"),
            },
            "dimension_scores": dimensions,
            "red_flags_activadas": red_flags,
            "audit_red_flags_activadas": red_flags,
            "flag_evidence": evidence,
            "evidence": evidence,
            "huecos_de_informacion": build_gaps(row),
            "documentos_a_revisar": build_documents_to_review(row),
            "recomendacion": recommended_action,
            "audit_recommendation": recommended_action,
            "score_explanation": row.get("score_explanation"),
            "audit_score_explanation": row.get("score_explanation"),
            "audit_dimension_scores": dimensions,
        }
        cards.append(card)
    return cards


def collect_evidence(row: pd.Series) -> dict[str, Any]:
    return {
        "process_id": row.get("process_id"),
        "competition_signal": row.get("competition_signal"),
        "num_oferentes_reported": to_json_number(row.get("num_oferentes_reported")),
        "bidder_rows": to_json_number(row.get("bidder_rows")),
        "unique_suppliers": to_json_number(row.get("unique_suppliers")),
        "supplier_total_contracts": to_json_number(row.get("supplier_total_contracts")),
        "supplier_share_in_entity": to_json_number(row.get("supplier_share_in_entity")),
        "value_percentile_year": to_json_number(row.get("value_percentile_year")),
        "estimated_vs_awarded_ratio": to_json_number(row.get("estimated_vs_awarded_ratio")),
        "url_secop": row.get("process_url"),
        "entity_nit": row.get("entity_nit"),
        "supplier_nit": row.get("supplier_nit"),
    }


def build_gaps(row: pd.Series) -> list[str]:
    gaps = []
    if not row.get("has_bidder_evidence"):
        gaps.append("No hay evidencia suficiente de competencia en el conjunto reducido.")
    if not row.get("has_url"):
        gaps.append("No se encontro URL SECOP utilizable.")
    gaps.extend(MISSING_LAYER_CHECKS)
    return gaps


def build_documents_to_review(row: pd.Series) -> list[str]:
    documents = ["Proceso SECOP completo", "Contrato y estudios previos"]
    if row.get("has_process_join"):
        documents.append("Informe de seleccion o adjudicacion")
    if row.get("traceability_incomplete"):
        documents.extend(["Soportes de pagos", "Facturas", "Garantias", "Actas de supervision"])
    return unique_preserve_order(documents)


def build_recommendation(row: pd.Series) -> str:
    score = int(row.get("score", 0))
    if score >= 76:
        return "Prioridad critica para revision documental inmediata."
    if score >= 56:
        return "Prioridad alta para revision documental."
    if score >= 31:
        return "Requiere revision complementaria."
    return "Mantener en monitoreo rutinario."


def build_limitations(row: pd.Series) -> str:
    limitation_text = clean_text(row.get("risk_limitations"))
    if limitation_text:
        return limitation_text

    manual_checks = clean_text(row.get("required_manual_checks"))
    if manual_checks:
        return manual_checks

    return " | ".join(build_gaps(row))


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return text


def build_manual_checks(row: pd.Series) -> str:
    checks = []
    if row.get("traceability_incomplete"):
        checks.append("Completar trazabilidad documental.")
    if not row.get("has_additions"):
        checks.append("Cruzar adiciones y prorrogas.")
    if not row.get("has_paco_context"):
        checks.append("Cruzar antecedentes PACO.")
    if row.get("num_oferentes_reported", 0) == 0:
        checks.append("Validar numero de oferentes en SECOP.")
    return " | ".join(unique_preserve_order(checks))


def build_explanation(row: pd.Series, dimension_scores: dict[str, int], flag_labels: dict[str, str]) -> str:
    parts = []
    for dimension_id, score in dimension_scores.items():
        if score <= 0:
            continue
        label = dimension_id.replace("_", " ").title()
        parts.append(f"{label}: {score}")

    activated = [flag_labels[flag_id] for flag_id in flag_labels if bool(row.get(flag_id))]
    if activated:
        parts.append("Señales: " + "; ".join(activated[:5]))
    return " | ".join(parts) if parts else "Sin señales fuertes con la evidencia disponible."


def resolve_risk_level(score: int, levels: list[dict[str, Any]]) -> str:
    for level in levels:
        if level["min"] <= score <= level["max"]:
            return level["label"]
    return "Desconocido"


def resolve_duration_days(row: pd.Series) -> float:
    duration = row.get("duration_raw")
    if duration is None or (isinstance(duration, float) and pd.isna(duration)):
        return float("nan")
    try:
        duration_value = float(duration)
    except (TypeError, ValueError):
        return float("nan")

    unit = normalize_text(row.get("duration_unit"))
    if not unit:
        return duration_value
    if "MES" in unit:
        return duration_value * 30
    if "ANO" in unit or "AÑO" in unit or "YEAR" in unit:
        return duration_value * 365
    return duration_value


def resolve_value_per_day(row: pd.Series) -> float:
    value = row.get("contract_value")
    duration_days = row.get("duration_days")
    if not value or not duration_days or pd.isna(duration_days) or duration_days <= 0:
        return float("nan")
    return float(value) / float(duration_days)


def resolve_estimated_vs_awarded_ratio(row: pd.Series) -> float:
    estimated = row.get("value_initial")
    awarded = row.get("value_final_raw")
    if not estimated or not awarded:
        return float("nan")
    if estimated <= 0 or awarded <= 0:
        return float("nan")
    return float(awarded) / float(estimated)


def coalesce_series(frame: pd.DataFrame, candidates: list[str]) -> pd.Series:
    result = pd.Series([None] * len(frame), index=frame.index, dtype="object")
    filled = pd.Series([False] * len(frame), index=frame.index)
    for column in candidates:
        if column not in frame.columns:
            continue
        series = frame[column]
        valid = series.notna()
        if series.dtype == object:
            valid = valid & series.astype(str).str.strip().ne("")
        mask = valid & ~filled
        if mask.any():
            result.loc[mask] = series.loc[mask]
            filled.loc[mask] = True
    return result


def first_text(row: pd.Series, candidates: list[str]) -> str:
    for column in candidates:
        value = row.get(column)
        if value is None or (isinstance(value, float) and pd.isna(value)):
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def first_numeric(row: pd.Series, candidates: list[str]) -> float:
    for column in candidates:
        value = row.get(column)
        number = parse_numeric(value)
        if not math.isnan(number):
            return number
    return float("nan")


def parse_numeric(value: Any) -> float:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return float("nan")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    text = str(value).strip()
    if not text:
        return float("nan")
    text = re.sub(r"[^0-9,.\-]", "", text)
    if not text:
        return float("nan")
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return float("nan")


def normalize_digits_or_empty(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return re.sub(r"\D+", "", str(value))


def extract_url(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("url", "")).strip()
    return str(value).strip() if value is not None and not (isinstance(value, float) and pd.isna(value)) else ""


def count_generic_object_hits(text: Any) -> int:
    norm = normalize_text(text)
    if not norm:
        return 0
    tokens = [
        "SERVICIO",
        "APOYO",
        "SUMINISTRO",
        "GESTION",
        "OPERACION",
        "LOGISTICA",
    ]
    return sum(1 for token in tokens if token in norm)


def is_non_competitive_modality(text: Any) -> bool:
    norm = normalize_text(text)
    if not norm:
        return False
    return "LICITACION PUBLICA" not in norm


def is_direct_contracting(text: Any) -> bool:
    norm = normalize_text(text)
    return "CONTRATACION DIRECTA" in norm


def to_json_number(value: Any) -> float | int | None:
    number = parse_numeric(value)
    if math.isnan(number):
        return None
    if float(number).is_integer():
        return int(number)
    return round(number, 3)


def sanitize_json_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): sanitize_json_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize_json_payload(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize_json_payload(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if value is pd.NA:
        return None
    if hasattr(value, "item") and callable(getattr(value, "item", None)):
        try:
            return sanitize_json_payload(value.item())
        except Exception:
            pass
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def parse_json_field(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return value
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except Exception:
        return value


def unique_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output




if __name__ == "__main__":
    main()
