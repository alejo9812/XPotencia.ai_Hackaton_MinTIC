from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from pipeline_utils import (
    CACHE_DIR,
    PROCESSED_DIR,
    batched,
    build_in_clause,
    build_query_url,
    combine_clauses,
    fetch_json,
    load_columns_config,
    load_dataset_config,
    load_keywords_config,
    load_schema_cache,
    normalize_digits,
    normalize_text,
    save_json,
    soql_literal,
    write_parquet_frame,
)


CORE_SOURCE_PATH = PROCESSED_DIR / "pae_contracts_core.parquet"
PROCESSES_OUTPUT_PATH = PROCESSED_DIR / "pae_processes.parquet"
BIDDERS_OUTPUT_PATH = PROCESSED_DIR / "pae_bidders.parquet"
BIDDERS_SUMMARY_PATH = PROCESSED_DIR / "pae_bidders_summary.parquet"
ENRICHED_CORE_PATH = PROCESSED_DIR / "pae_contracts_enriched.parquet"
MANIFEST_PATH = CACHE_DIR / "traceability_manifest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enrich the PAE core sample with processes and bidder evidence.")
    parser.add_argument("--core", type=str, default=str(CORE_SOURCE_PATH))
    parser.add_argument("--process-batch-size", type=int, default=20)
    parser.add_argument("--bidder-year-limit", type=int, default=250)
    parser.add_argument("--bidder-fetch-limit", type=int, default=200)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dataset_config = load_dataset_config()
    keywords_config = load_keywords_config()
    columns_config = load_columns_config()
    schema_cache = load_schema_cache()

    core = pd.read_parquet(Path(args.core))
    if core.empty:
        raise SystemExit("Core PAE parquet is empty.")

    core = core.copy()
    core["core_year"] = pd.to_datetime(core.get("fecha_de_firma"), errors="coerce").dt.year
    core["contract_key"] = core.get("id_contrato").astype(str)
    core["process_portfolio_id"] = core.get("proceso_de_compra").astype(str)
    core["entity_norm"] = core.get("entity_norm").fillna("").astype(str)
    core["supplier_norm"] = core.get("supplier_norm").fillna("").astype(str)

    process_rows, process_manifest = fetch_process_rows(core, schema_cache, args.process_batch_size)
    process_frame = pd.DataFrame(process_rows)
    if not process_frame.empty:
        process_frame["process_portfolio_norm"] = process_frame.get("id_del_portafolio").astype(str)
        process_frame["process_entity_norm"] = safe_series(process_frame, "entidad").fillna("").map(normalize_text)
        process_frame["process_year"] = pd.to_datetime(process_frame.get("fecha_de_publicacion_del"), errors="coerce").dt.year
        process_frame["num_bidders_proxy"] = process_frame.apply(resolve_num_bidders_proxy, axis=1)
        process_summary = build_process_summary(process_frame)
    else:
        process_frame = pd.DataFrame(
            columns=[
                "id_del_portafolio",
                "id_del_proceso",
                "referencia_del_proceso",
                "entidad",
                "modalidad_de_contratacion",
                "duracion",
                "unidad_de_duracion",
                "fecha_de_publicacion_del",
                "proveedores_invitados",
                "proveedores_que_manifestaron",
                "proveedores_unicos_con",
                "respuestas_al_procedimiento",
                "respuestas_externas",
                "conteo_de_respuestas_a_ofertas",
                "precio_base",
                "valor_total_adjudicacion",
                "nombre_del_procedimiento",
                "nit_entidad",
                "codigo_entidad",
                "urlproceso",
                "process_portfolio_norm",
                "process_entity_norm",
                "process_year",
                "num_bidders_proxy",
            ]
        )
        process_summary = process_frame.copy()

    bidders_rows, bidder_manifest = fetch_bidder_rows(core, keywords_config, args.bidder_year_limit, args.bidder_fetch_limit)
    bidder_frame = pd.DataFrame(bidders_rows)
    if not bidder_frame.empty:
        bidder_frame["entity_norm"] = bidder_frame.get("entidad_compradora").fillna("").map(normalize_text)
        bidder_frame["supplier_norm"] = bidder_frame.get("proveedor").fillna("").map(normalize_text)
        bidder_frame["supplier_doc_norm"] = bidder_frame.get("nit_proveedor").fillna("").map(normalize_digits)
        bidder_frame["bidder_year"] = pd.to_datetime(bidder_frame.get("fecha_publicaci_n"), errors="coerce").dt.year
    else:
        bidder_frame = pd.DataFrame(
            columns=[
                "id_procedimiento",
                "fecha_publicaci_n",
                "nombre_procedimiento",
                "entidad_compradora",
                "proveedor",
                "nit_proveedor",
                "entity_norm",
                "supplier_norm",
                "supplier_doc_norm",
                "bidder_year",
                "pae_match_score",
                "pae_confidence",
                "pae_match_terms",
            ]
        )

    enriched_core = core.merge(
        process_summary[
            [
                "id_del_portafolio",
                "id_del_proceso",
                "referencia_del_proceso",
                "modalidad_de_contratacion",
                "duracion",
                "unidad_de_duracion",
                "fecha_de_publicacion_del",
                "proveedores_invitados",
                "proveedores_que_manifestaron",
                "proveedores_unicos_con",
                "respuestas_al_procedimiento",
                "respuestas_externas",
                "conteo_de_respuestas_a_ofertas",
                "precio_base",
                "valor_total_adjudicacion",
                "nombre_del_procedimiento",
                "urlproceso",
                "num_bidders_proxy",
            ]
        ],
        left_on="process_portfolio_id",
        right_on="id_del_portafolio",
        how="left",
    )
    bidder_proxy = enriched_core.get("proveedores_unicos_con")
    bidder_proxy = bidder_proxy.map(to_number) if bidder_proxy is not None else pd.Series([0] * len(enriched_core), index=enriched_core.index)
    enriched_core["num_oferentes"] = enriched_core["num_bidders_proxy"].fillna(bidder_proxy)
    enriched_core["has_single_bidder"] = enriched_core["num_oferentes"].fillna(0).astype(float).le(1)
    enriched_core["competition_proxy"] = enriched_core["num_oferentes"].fillna(0).map(lambda value: max(0, 100 - min(100, int(value) * 20)))

    bidder_summary = build_bidder_summary(bidder_frame)
    if not bidder_summary.empty:
        enriched_core = enriched_core.merge(
            bidder_summary,
            left_on=["entity_norm", "core_year"],
            right_on=["entity_norm", "bidder_year"],
            how="left",
        )

    write_parquet_frame(process_frame.to_dict(orient="records"), PROCESSES_OUTPUT_PATH)
    write_parquet_frame(bidder_frame.to_dict(orient="records"), BIDDERS_OUTPUT_PATH)
    write_parquet_frame(bidder_summary.to_dict(orient="records"), BIDDERS_SUMMARY_PATH)
    write_parquet_frame(enriched_core.to_dict(orient="records"), ENRICHED_CORE_PATH)

    manifest = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "core_rows": int(len(core)),
        "enriched_rows": int(len(enriched_core)),
        "process_rows": int(len(process_frame)),
        "bidder_rows": int(len(bidder_frame)),
        "bidder_summary_rows": int(len(bidder_summary)),
        "process_fetch": process_manifest,
        "bidder_fetch": bidder_manifest,
    }
    save_json(MANIFEST_PATH, manifest)

    print(f"Processes written to {PROCESSES_OUTPUT_PATH}")
    print(f"Bidders written to {BIDDERS_OUTPUT_PATH}")
    print(f"Bidders summary written to {BIDDERS_SUMMARY_PATH}")
    print(f"Enriched core written to {ENRICHED_CORE_PATH}")


def fetch_process_rows(core: pd.DataFrame, schema_cache: dict[str, Any], batch_size: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    dataset_id = "p6dx-8zbt"
    portfolio_ids = sorted({value for value in core.get("process_portfolio_id").dropna().astype(str).tolist() if value and value != "nan"})
    schema = schema_cache.get("datasets", {}).get(dataset_id, {})
    resolved = schema.get("resolved_columns", {})
    select_fields = [
        "id_del_portafolio",
        "id_del_proceso",
        "referencia_del_proceso",
        "entidad",
                "modalidad_de_contratacion",
                "duracion",
                "unidad_de_duracion",
                "fecha_de_publicacion_del",
                "proveedores_invitados",
        "proveedores_que_manifestaron",
        "proveedores_unicos_con",
        "respuestas_al_procedimiento",
        "respuestas_externas",
        "conteo_de_respuestas_a_ofertas",
        "precio_base",
        "valor_total_adjudicacion",
        "nombre_del_procedimiento",
        "nit_entidad",
        "codigo_entidad",
        "urlproceso",
    ]
    select_fields = [field for field in select_fields if field in [c.get("fieldName") for c in schema.get("columns", [])] or field in resolved.values() or field]

    rows: list[dict[str, Any]] = []
    batches = list(batched(portfolio_ids, batch_size))
    for batch in batches:
        where_clause = build_in_clause("id_del_portafolio", batch)
        if not where_clause:
            continue
        rows.extend(fetch_dataset_rows(dataset_id, select_fields, where_clause, fetch_limit=len(batch) * 2))

    manifest = {"dataset_id": dataset_id, "batches": len(batches), "ids": len(portfolio_ids), "rows": len(rows)}
    return dedupe_rows(rows, key_fields=["id_del_portafolio", "id_del_proceso"]), manifest


def fetch_bidder_rows(core: pd.DataFrame, keywords_config: dict[str, Any], year_limit: int, fetch_limit: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    dataset_id = "hgi6-6wh3"
    years = sorted({int(value) for value in core.get("core_year").dropna().astype(int).tolist()})
    rows: list[dict[str, Any]] = []
    per_year_terms = [
        term
        for term in keywords_config.get("high_confidence", []) + keywords_config.get("medium_confidence", [])
        if normalize_text(term) != "PAE" or "ALIMENTACION ESCOLAR" in normalize_text(term)
    ]

    for year in years:
        year_clause = f"fecha_publicaci_n >= '{year:04d}-01-01T00:00:00' AND fecha_publicaci_n < '{year + 1:04d}-01-01T00:00:00'"
        term_clause = build_text_query_clause(
            ["nombre_procedimiento", "entidad_compradora", "proveedor"],
            per_year_terms,
        )
        where_clause = combine_clauses(year_clause, term_clause)
        if not where_clause:
            continue
        fetched = fetch_dataset_rows(
            dataset_id,
            [
                "id_procedimiento",
                "fecha_publicaci_n",
                "nombre_procedimiento",
                "entidad_compradora",
                "proveedor",
                "nit_proveedor",
                "codigo_entidad",
                "codigo_proveedor",
            ],
            where_clause,
            fetch_limit=min(fetch_limit, year_limit),
        )
        rows.extend([row for row in fetched if is_pae_bidder_row(row, keywords_config)])

    manifest = {"dataset_id": dataset_id, "years": years, "rows": len(rows)}
    return dedupe_rows(rows, key_fields=["id_procedimiento", "nit_proveedor", "proveedor"]), manifest


def fetch_dataset_rows(dataset_id: str, select_fields: list[str], where_clause: str, fetch_limit: int = 500) -> list[dict[str, Any]]:
    url = build_query_url(
        f"https://www.datos.gov.co/resource/{dataset_id}.json",
        {
            "$select": ",".join(select_fields),
            "$where": where_clause,
            "$limit": fetch_limit,
        },
    )
    payload = fetch_json(url)
    return payload if isinstance(payload, list) else []


def build_text_query_clause(fields: list[str], terms: Iterable[str]) -> str:
    clauses: list[str] = []
    for field in fields:
        field_clauses = []
        for term in terms:
            cleaned = normalize_text(term)
            if cleaned:
                field_clauses.append(f"UPPER(COALESCE({field}, '')) LIKE '%{soql_literal(cleaned)}%'")
        if field_clauses:
            clauses.append("(" + " OR ".join(field_clauses) + ")")
    return "(" + " OR ".join(clauses) + ")" if clauses else ""


def is_pae_bidder_row(row: dict[str, Any], keywords_config: dict[str, Any]) -> bool:
    text_blob = " ".join(
        [
            str(row.get("nombre_procedimiento", "")),
            str(row.get("entidad_compradora", "")),
            str(row.get("proveedor", "")),
        ]
    )
    norm_text = normalize_text(text_blob)
    exclude_hits = [term for term in keywords_config.get("exclude", []) if normalize_text(term) in norm_text]
    if exclude_hits:
        return False

    high_hits = [term for term in keywords_config.get("high_confidence", []) if normalize_text(term) in norm_text]
    medium_hits = [term for term in keywords_config.get("medium_confidence", []) if normalize_text(term) in norm_text]
    if high_hits:
        return True
    if medium_hits:
        return True
    if "ALIMENTACION ESCOLAR" in norm_text or "COMPLEMENTO ALIMENTARIO" in norm_text:
        return True
    return False


def build_bidder_summary(bidder_frame: pd.DataFrame) -> pd.DataFrame:
    if bidder_frame.empty:
        return pd.DataFrame(
            columns=[
                "entity_norm",
                "bidder_year",
                "bidder_rows",
                "unique_suppliers",
                "top_supplier",
                "competition_signal",
            ]
        )

    grouped = []
    for (entity_norm, bidder_year), group in bidder_frame.groupby(["entity_norm", "bidder_year"], dropna=False):
        unique_suppliers = int(group["supplier_norm"].replace("", pd.NA).dropna().nunique())
        supplier_counts = group["supplier_norm"].replace("", pd.NA).dropna().value_counts()
        top_supplier = supplier_counts.index[0] if not supplier_counts.empty else ""
        competition_signal = "baja" if unique_suppliers <= 1 else "media" if unique_suppliers <= 3 else "alta"
        grouped.append(
            {
                "entity_norm": entity_norm,
                "bidder_year": int(bidder_year) if pd.notna(bidder_year) else None,
                "bidder_rows": int(len(group)),
                "unique_suppliers": unique_suppliers,
                "top_supplier": top_supplier,
                "competition_signal": competition_signal,
            }
        )

    return pd.DataFrame(grouped)


def build_process_summary(process_frame: pd.DataFrame) -> pd.DataFrame:
    if process_frame.empty:
        return process_frame.copy()

    priority_columns = [
        "num_bidders_proxy",
        "process_year",
        "fecha_de_publicacion_del",
        "precio_base",
        "valor_total_adjudicacion",
    ]
    ordered = process_frame.copy()
    for column in priority_columns:
        if column not in ordered.columns:
            ordered[column] = pd.NA

    ordered = ordered.sort_values(
        by=["id_del_portafolio", "num_bidders_proxy", "process_year", "fecha_de_publicacion_del"],
        ascending=[True, False, False, False],
        na_position="last",
        kind="mergesort",
    )
    return ordered.drop_duplicates(subset=["id_del_portafolio"], keep="first")


def resolve_num_bidders_proxy(row: pd.Series) -> int:
    for field in [
        "proveedores_unicos_con",
        "respuestas_al_procedimiento",
        "respuestas_externas",
        "conteo_de_respuestas_a_ofertas",
        "proveedores_que_manifestaron",
        "proveedores_invitados",
    ]:
        value = to_number(row.get(field))
        if value > 0:
            return value
    return 0


def to_number(value: Any) -> int:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 0
    text = normalize_digits(value)
    try:
        return int(text) if text else 0
    except ValueError:
        return 0


def safe_series(frame: pd.DataFrame, column: str, default: Any = "") -> pd.Series:
    if column in frame.columns:
        return frame[column]
    return pd.Series([default] * len(frame), index=frame.index)


def dedupe_rows(rows: list[dict[str, Any]], key_fields: list[str]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    output: list[dict[str, Any]] = []
    for row in rows:
        key = "||".join(str(row.get(field, "")).strip() for field in key_fields)
        if not key.strip("||"):
            continue
        if key in seen:
            continue
        seen.add(key)
        output.append(row)
    return output


if __name__ == "__main__":
    main()
