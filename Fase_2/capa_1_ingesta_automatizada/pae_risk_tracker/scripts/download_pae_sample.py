from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline_utils import (
    CACHE_DIR,
    CONFIG_DIR,
    OUTPUT_DIR,
    PROCESSED_DIR,
    RAW_DIR,
    append_app_token,
    build_like_clause,
    combine_clauses,
    dataset_metadata_url,
    fetch_json,
    load_columns_config,
    load_dataset_config,
    load_keywords_config,
    load_schema_cache,
    make_range_clause,
    normalize_digits,
    normalize_text,
    now_iso,
    pick_columns,
    resolve_columns,
    save_json,
    sample_row_query,
    soql_literal,
    top_text_columns,
    write_parquet_frame,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download a low-cost PAE sample from SECOP II.")
    parser.add_argument("--years", nargs="*", type=int, default=list(range(2022, date.today().year + 1)))
    parser.add_argument("--sample-limit", type=int, default=500, help="Maximum number of rows in the final sample.")
    parser.add_argument("--per-year-limit", type=int, default=100, help="Rows to fetch per year before dedupe.")
    parser.add_argument("--output", type=str, default=str(PROCESSED_DIR / "pae_contracts_core.parquet"))
    parser.add_argument("--raw-output", type=str, default=str(RAW_DIR / "pae_contracts_sample.parquet"))
    parser.add_argument("--use-app-token", action="store_true", help="Use SOCRATA_APP_TOKEN if available.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dataset_config = load_dataset_config()
    keywords_config = load_keywords_config()
    columns_config = load_columns_config()
    schema_cache = load_schema_cache()
    core_dataset = next((item for item in dataset_config.get("datasets", []) if item["id"] == dataset_config["core_dataset"]), None)

    if core_dataset is None:
        raise SystemExit("Core dataset configuration not found.")

    dataset_id = core_dataset["id"]
    metadata = schema_cache.get("datasets", {}).get(dataset_id)
    if not metadata:
        metadata = build_runtime_schema(dataset_id, columns_config)

    columns = metadata.get("columns", [])
    resolved = metadata.get("resolved_columns") or resolve_columns(columns, columns_config.get("core", {}))

    search_columns = pick_columns(columns, columns_config.get("search", {}).get("text_columns", []))
    text_columns = search_columns
    if not text_columns:
        text_columns = top_text_columns(columns, limit=6)

    year_field = resolved.get("date") or core_dataset.get("default_year_field")
    query_columns = [
        resolved.get("contract_id"),
        resolved.get("process_id"),
        resolved.get("entity_name"),
        resolved.get("entity_nit"),
        resolved.get("supplier_name"),
        resolved.get("supplier_doc"),
        resolved.get("object_text"),
        resolved.get("modality"),
        resolved.get("status"),
        resolved.get("amount"),
        year_field,
        resolved.get("department"),
        resolved.get("municipality"),
        resolved.get("url_process"),
    ]
    query_columns = [column for column in query_columns if column]

    per_year_limit = max(1, args.per_year_limit)
    token = None
    if args.use_app_token:
        import os

        token = os.getenv("SOCRATA_APP_TOKEN", "").strip() or None

    rows: list[dict[str, Any]] = []
    summary = {"generated_at": now_iso(), "dataset_id": dataset_id, "years": {}, "keywords": keywords_config}

    for year in args.years:
        year_clause = make_range_clause(year_field, year)
        keyword_clause = build_keyword_clause(text_columns, keywords_config)
        exclude_clause = build_exclusion_clause(text_columns, keywords_config)
        where_clause = combine_clauses(year_clause, keyword_clause, exclude_clause)

        params = {
            "$select": ",".join(query_columns),
            "$where": where_clause,
            "$limit": per_year_limit,
        }
        base_url = f"https://www.datos.gov.co/resource/{dataset_id}.json"
        url = append_app_token(build_query_url(base_url, params), token)
        print(f"Fetching year {year}: {url}")
        chunk = fetch_json(url)
        normalized = [normalize_contract_row(row, text_columns, keywords_config) for row in chunk]
        kept = [row for row in normalized if row.get("pae_confidence") in {"alto", "medio", "bajo"}]

        rows.extend(kept)
        summary["years"][str(year)] = {
            "rows_fetched": len(normalized),
            "rows_kept": len(kept),
            "year_clause": year_clause,
            "query_columns": query_columns,
            "text_columns": text_columns,
        }

    unique_rows = dedupe_by_contract(rows)
    selected_rows = unique_rows[: args.sample_limit]

    raw_output = Path(args.raw_output)
    processed_output = Path(args.output)
    write_parquet_frame(selected_rows, raw_output)
    write_parquet_frame(selected_rows, processed_output)
    save_json(RAW_DIR / "pae_contracts_sample.manifest.json", summary)

    print(f"Raw sample written to {raw_output}")
    print(f"Processed core written to {processed_output}")
    print(f"Rows kept: {len(selected_rows)}")


def build_runtime_schema(dataset_id: str, columns_config: dict[str, Any]) -> dict[str, Any]:
    metadata = fetch_json(dataset_metadata_url(dataset_id))
    columns = metadata.get("columns", [])
    resolved = resolve_columns(columns, columns_config.get("core", {}))
    sample = fetch_json(sample_row_query(dataset_id, limit=1))
    return {
        "name": metadata.get("name"),
        "column_count": len(columns),
        "columns": columns,
        "sample_row": sample[0] if isinstance(sample, list) and sample else {},
        "resolved_columns": resolved,
    }


def build_query_builders(columns: list[str], terms: dict[str, Any]) -> tuple[str, str]:
    return build_keyword_clause(columns, terms), build_exclusion_clause(columns, terms)


def build_keyword_clause(columns: list[str], keywords_config: dict[str, Any]) -> str:
    terms = [
        term
        for term in (
            keywords_config.get("high_confidence", [])
            + keywords_config.get("medium_confidence", [])
            + keywords_config.get("low_confidence", [])
        )
        if normalize_text(term) != "PAE"
    ]
    clauses = [build_like_clause(column, terms) for column in columns]
    clauses = [clause for clause in clauses if clause]
    return "(" + " OR ".join(clauses) + ")" if clauses else ""


def build_exclusion_clause(columns: list[str], keywords_config: dict[str, Any]) -> str:
    exclusions = keywords_config.get("exclude", [])
    if not exclusions:
        return ""

    pieces = []
    for column in columns:
        for term in exclusions:
            cleaned = normalize_text(term)
            if not cleaned:
                continue
            pieces.append(f"UPPER(COALESCE({column}, '')) NOT LIKE '%{soql_literal(cleaned)}%'")
    return "(" + " AND ".join(pieces) + ")" if pieces else ""


def build_like_clause(column: str, terms: list[str]) -> str:
    pieces = []
    for term in terms:
        cleaned = normalize_text(term)
        if cleaned:
            pieces.append(f"UPPER(COALESCE({column}, '')) LIKE '%{soql_literal(cleaned)}%'")
    return "(" + " OR ".join(pieces) + ")" if pieces else ""


def build_query_url(base_url: str, params: dict[str, Any]) -> str:
    from urllib.parse import urlencode

    filtered = {key: value for key, value in params.items() if value not in (None, "", [], {})}
    return f"{base_url}?{urlencode(filtered)}"


def normalize_contract_row(row: dict[str, Any], text_columns: list[str], keywords_config: dict[str, Any]) -> dict[str, Any]:
    text_blob = " ".join(str(row.get(column, "")) for column in text_columns)
    norm_text = normalize_text(text_blob)
    high_hits = [term for term in keywords_config.get("high_confidence", []) if normalize_text(term) in norm_text]
    medium_hits = [term for term in keywords_config.get("medium_confidence", []) if normalize_text(term) in norm_text]
    low_hits = [term for term in keywords_config.get("low_confidence", []) if normalize_text(term) in norm_text]
    exclude_hits = [term for term in keywords_config.get("exclude", []) if normalize_text(term) in norm_text]
    school_context_hits = high_hits + medium_hits

    pae_match_score = 0
    pae_confidence = "descartar"
    pae_match_terms: list[str] = []

    if exclude_hits:
        pae_match_score = 0
        pae_confidence = "descartar"
        pae_match_terms = exclude_hits
    elif high_hits:
        pae_match_score = 100
        pae_confidence = "alto"
        pae_match_terms = high_hits
    elif medium_hits:
        pae_match_score = 70
        pae_confidence = "medio"
        pae_match_terms = medium_hits
    elif low_hits and school_context_hits:
        pae_match_score = 45
        pae_confidence = "bajo"
        pae_match_terms = low_hits
    else:
        pae_confidence = "descartar"
        pae_match_score = 0

    normalized = {
        **row,
        "pae_match_score": pae_match_score,
        "pae_match_terms": ", ".join(pae_match_terms),
        "pae_confidence": pae_confidence,
        "entity_norm": normalize_text(row.get("nombre_entidad") or row.get("entidad") or ""),
        "supplier_norm": normalize_text(
            row.get("proveedor_adjudicado")
            or row.get("proveedor")
            or row.get("nombre")
            or row.get("nombre_grupo")
            or ""
        ),
        "supplier_doc_norm": normalize_digits(
            row.get("documento_proveedor")
            or row.get("nit_proveedor")
            or row.get("nit")
            or row.get("nit_grupo")
            or ""
        ),
        "object_norm": normalize_text(
            row.get("descripcion_del_proceso")
            or row.get("nombre_del_procedimiento")
            or row.get("descripcion")
            or row.get("notas")
            or ""
        ),
        "modality_norm": normalize_text(row.get("modalidad_de_contratacion") or row.get("tipo_de_contrato") or row.get("tipo") or ""),
        "department_norm": normalize_text(row.get("departamento") or row.get("departamento_entidad") or row.get("departamento_grupo") or ""),
        "municipality_norm": normalize_text(row.get("ciudad") or row.get("ciudad_entidad") or row.get("municipio") or row.get("ubicacion") or ""),
    }
    return normalized


def dedupe_by_contract(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    ordered: list[dict[str, Any]] = []
    for row in rows:
        key = str(
            row.get("id_contrato")
            or row.get("id_del_contrato")
            or row.get("referencia_del_contrato")
            or row.get("referencia_contrato")
            or row.get("id_procedimiento")
            or row.get("id_del_proceso")
            or row.get("id_proceso")
            or ""
        ).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(row)
    return ordered


if __name__ == "__main__":
    main()
