from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any

import pandas as pd

from ..config import normalize_text


DEFAULT_THRESHOLDS = {
    "short_description_words": 8,
    "generic_description_words": 16,
    "low_competition_participants": 2,
    "repeat_supplier_count": 2,
    "high_value_percentile": 90,
    "very_high_value_percentile": 95,
    "addition_ratio_high": 0.4,
    "similarity_threshold": 0.82,
    "school_start_months": {1, 2},
    "short_duration_days": 90,
}

GENERIC_TERMS = {
    "APOYO",
    "SERVICIO",
    "SUMINISTRO",
    "MANTENIMIENTO",
    "LOGISTICA",
    "OPERACION",
    "GESTION",
    "TECNICO",
    "GENERAL",
    "INTEGRAL",
    "ACTIVIDADES",
    "CONEXAS",
    "SEGUN",
    "NECESIDAD",
    "OTROS",
}

DIRECT_MODALITY_TERMS = {
    "CONTRATACION DIRECTA",
    "CONTRATACION DIRECTA",
    "DIRECTA",
}

SENSITIVE_MODALITY_TERMS = {
    "MINIMA CUANTIA",
    "REGIMEN ESPECIAL",
    "SELECCION ABREVIADA",
    "URGENCIA MANIFIESTA",
}

SANCTION_TERMS = {
    "MULTA",
    "SANCION",
    "RESPONSABILIDAD FISCAL",
    "COLUSION",
}


@dataclass(frozen=True)
class IndicatorBundle:
    frame: pd.DataFrame
    thresholds: dict[str, Any]
    stats: dict[str, Any]


def build_indicator_bundle(frame: pd.DataFrame, thresholds: dict[str, Any] | None = None) -> IndicatorBundle:
    thresholds = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    df = frame.copy()
    df = _prepare_numeric_and_date_columns(df)
    df = _prepare_text_columns(df)
    df = _prepare_derived_columns(df, thresholds)
    stats = _build_stats(df, thresholds)
    df = _attach_group_metrics(df, stats, thresholds)
    df = _attach_text_similarity(df, thresholds)
    return IndicatorBundle(frame=df, thresholds=thresholds, stats=stats)


def _prepare_numeric_and_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    numeric_columns = [
        "amount",
        "estimated_amount",
        "duration_days",
        "addition_value",
        "addition_days",
        "addition_count",
        "participants",
        "offers",
    ]
    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
        else:
            df[column] = 0

    date_columns = ["date", "start_date", "end_date", "addition_last_date"]
    for column in date_columns:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce", utc=False)
        else:
            df[column] = pd.NaT
    return df


def _prepare_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    text_columns = [
        "contract_id",
        "process_id",
        "entity_name",
        "supplier_name",
        "object_text",
        "justification",
        "modality",
        "status",
        "department",
        "municipality",
        "url_process",
        "search_blob",
    ]
    for column in text_columns:
        if column not in df.columns:
            df[column] = ""
        df[column] = df[column].fillna("").astype(str)
    return df


def _prepare_derived_columns(df: pd.DataFrame, thresholds: dict[str, Any]) -> pd.DataFrame:
    df["normalized_object"] = df["object_text"].map(normalize_text)
    df["normalized_justification"] = df["justification"].map(normalize_text)
    df["normalized_modality"] = df["modality"].map(normalize_text)
    df["normalized_entity"] = df["entity_name"].map(normalize_text)
    df["normalized_supplier"] = df["supplier_name"].map(normalize_text)
    df["object_word_count"] = df["normalized_object"].map(_word_count)
    df["generic_term_hits"] = df["normalized_object"].map(_generic_hits)
    df["generic_term_ratio"] = df.apply(
        lambda row: (row["generic_term_hits"] / max(row["object_word_count"], 1)),
        axis=1,
    )
    df["has_url_process"] = df["url_process"].str.strip().ne("")
    df["has_justification"] = df["normalized_justification"].str.strip().ne("")
    df["effective_duration_days"] = df.apply(_effective_duration_days, axis=1)
    df["addition_ratio"] = df.apply(_addition_ratio, axis=1)
    df["value_per_day"] = df.apply(
        lambda row: row["amount"] / max(int(row["effective_duration_days"] or 0), 1) if row["amount"] else 0.0,
        axis=1,
    )
    df["missing_critical_fields_count"] = df.apply(_missing_critical_fields_count, axis=1)
    df["missing_critical_fields"] = df.apply(_missing_critical_fields, axis=1)
    df["text_signature"] = df.apply(_text_signature, axis=1)
    df["modality_is_direct"] = df["normalized_modality"].apply(_contains_any(DIRECT_MODALITY_TERMS))
    df["modality_is_sensitive"] = df["normalized_modality"].apply(_contains_any(SENSITIVE_MODALITY_TERMS))
    df["description_is_generic"] = (
        (df["object_word_count"] < thresholds["generic_description_words"])
        | (df["generic_term_hits"] >= 2)
        | (df["generic_term_ratio"] >= 0.15)
    )
    df["description_is_short"] = df["object_word_count"] < thresholds["short_description_words"]
    df["contract_month"] = df["date"].dt.month.fillna(0).astype(int)
    df["school_start_window"] = df["contract_month"].isin(set(thresholds["school_start_months"]))
    return df


def _build_stats(df: pd.DataFrame, thresholds: dict[str, Any]) -> dict[str, Any]:
    amounts = df["amount"].tolist()
    value_per_day = df["value_per_day"].tolist()
    return {
        "amount_median": _median(amounts),
        "amount_mad": _mad(amounts),
        "amount_iqr": _iqr(amounts),
        "amount_p90": _percentile(amounts, thresholds["high_value_percentile"]),
        "amount_p95": _percentile(amounts, thresholds["very_high_value_percentile"]),
        "value_per_day_median": _median(value_per_day),
        "value_per_day_mad": _mad(value_per_day),
        "value_per_day_p95": _percentile(value_per_day, thresholds["very_high_value_percentile"]),
        "total_amount": float(sum(value for value in amounts if pd.notna(value))),
        "row_count": int(len(df)),
    }


def _attach_group_metrics(df: pd.DataFrame, stats: dict[str, Any], thresholds: dict[str, Any]) -> pd.DataFrame:
    entity_group = df.groupby("normalized_entity", dropna=False)
    df["entity_contract_count"] = entity_group["contract_id"].transform("count")
    df["entity_total_amount"] = entity_group["amount"].transform("sum")
    df["entity_median_amount"] = entity_group["amount"].transform("median")
    df["entity_amount_iqr"] = entity_group["amount"].transform(_series_iqr)
    df["entity_value_per_day_p95"] = entity_group["value_per_day"].transform(lambda series: _percentile(series.tolist(), thresholds["very_high_value_percentile"]))

    supplier_entity = df.groupby(["normalized_entity", "normalized_supplier"], dropna=False)
    df["supplier_contract_count_same_entity"] = supplier_entity["contract_id"].transform("count")
    df["supplier_amount_same_entity"] = supplier_entity["amount"].transform("sum")
    df["supplier_repeat_share_same_entity"] = df["supplier_contract_count_same_entity"] / df["entity_contract_count"].replace(0, pd.NA)
    df["supplier_amount_share_same_entity"] = df["supplier_amount_same_entity"] / df["entity_total_amount"].replace(0, pd.NA)

    supplier_group = df.groupby("normalized_supplier", dropna=False)
    df["supplier_entity_count"] = supplier_group["normalized_entity"].transform("nunique")
    df["supplier_municipality_count"] = supplier_group["municipality"].transform(lambda series: series.fillna("").astype(str).nunique())
    df["supplier_total_amount"] = supplier_group["amount"].transform("sum")
    df["supplier_total_contracts"] = supplier_group["contract_id"].transform("count")
    return df


def _attach_text_similarity(df: pd.DataFrame, thresholds: dict[str, Any]) -> pd.DataFrame:
    similarity_threshold = thresholds["similarity_threshold"]
    max_similarity = [0.0] * len(df)
    similar_count = [0] * len(df)

    for _, group in df.groupby("normalized_entity", dropna=False):
        indices = list(group.index)
        signatures = list(group["text_signature"].tolist())
        if len(indices) <= 1:
            continue

        if len(indices) > 150:
            counts = Counter(signatures)
            for idx, signature in zip(indices, signatures):
                similar_count[idx] = max(0, counts[signature] - 1)
                max_similarity[idx] = 1.0 if counts[signature] > 1 else 0.0
            continue

        for i, idx in enumerate(indices):
            current = signatures[i]
            if not current:
                continue
            best = 0.0
            count = 0
            for j, other_idx in enumerate(indices):
                if idx == other_idx:
                    continue
                other = signatures[j]
                if not other:
                    continue
                ratio = SequenceMatcher(None, current, other).ratio()
                if ratio >= similarity_threshold:
                    count += 1
                if ratio > best:
                    best = ratio
            max_similarity[idx] = round(best, 3)
            similar_count[idx] = count

    df["similar_text_count_same_entity"] = similar_count
    df["similar_text_max_ratio_same_entity"] = max_similarity
    return df


def _missing_critical_fields(row: pd.Series) -> list[str]:
    missing = []
    for column in ["entity_name", "supplier_name", "amount", "date", "modality", "object_text"]:
        value = row.get(column)
        if value is None:
            missing.append(column)
            continue
        if isinstance(value, str) and not value.strip():
            missing.append(column)
        elif column == "amount" and not float(value):
            missing.append(column)
        elif column == "date" and pd.isna(value):
            missing.append(column)
    if not row.get("has_url_process", True):
        missing.append("url_process")
    return missing


def _missing_critical_fields_count(row: pd.Series) -> int:
    return len(_missing_critical_fields(row))


def _effective_duration_days(row: pd.Series) -> int:
    start = row.get("start_date")
    end = row.get("end_date")
    if pd.notna(start) and pd.notna(end):
        duration = (end - start).days
        if duration > 0:
            return int(duration)

    provided = int(row.get("duration_days") or 0)
    if provided > 0:
        return provided
    return 0


def _addition_ratio(row: pd.Series) -> float:
    amount = float(row.get("amount") or 0)
    addition = float(row.get("addition_value") or 0)
    if amount <= 0 or addition <= 0:
        return 0.0
    return round(addition / amount, 4)


def _text_signature(row: pd.Series) -> str:
    parts = " ".join(
        value
        for value in [
            str(row.get("object_text") or ""),
            str(row.get("justification") or ""),
            str(row.get("modality") or ""),
        ]
        if value
    )
    tokens = [token for token in normalize_text(parts).split(" ") if token and token not in GENERIC_TERMS]
    return " ".join(sorted(set(tokens)))


def _word_count(text: str) -> int:
    return len([token for token in text.split(" ") if token])


def _generic_hits(text: str) -> int:
    tokens = set(text.split())
    return len(tokens.intersection(GENERIC_TERMS))


def _contains_any(terms: set[str]):
    def check(text: str) -> bool:
        return any(term in text for term in terms)

    return check


def _median(values: list[Any]) -> float:
    series = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
    if series.empty:
        return 0.0
    return float(series.median())


def _mad(values: list[Any]) -> float:
    series = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
    if series.empty:
        return 0.0
    median = float(series.median())
    return float((series - median).abs().median())


def _iqr(values: list[Any]) -> float:
    series = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
    if series.empty:
        return 0.0
    return float(series.quantile(0.75) - series.quantile(0.25))


def _series_iqr(series: pd.Series) -> float:
    series = pd.to_numeric(series, errors="coerce").dropna()
    if series.empty:
        return 0.0
    return float(series.quantile(0.75) - series.quantile(0.25))


def _percentile(values: list[Any], percent: float) -> float:
    series = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
    if series.empty:
        return 0.0
    return float(series.quantile(percent / 100.0))
