from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from ..config import normalize_text
from ..paths import PROCESSED_DIR
from ..storage.duckdb_store import DuckDBStore


SEARCH_INDEX_TABLE = "pae_search_index"
SEARCH_INDEX_PARQUET_NAME = "pae_search_index.parquet"
SEARCH_INDEX_PARQUET = PROCESSED_DIR / SEARCH_INDEX_PARQUET_NAME
SEARCH_INDEX_MANIFEST_NAME = "pae_search_index.manifest.json"


@dataclass(frozen=True)
class SearchIndexSummary:
    row_count: int
    record_type_counts: dict[str, int]
    source_tables: list[str]
    parquet_path: str
    source_snapshot: dict[str, Any] = field(default_factory=dict)
    source_fingerprint: str = ""
    manifest_path: str = ""
    table_name: str = SEARCH_INDEX_TABLE

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def ensure_search_index(store: DuckDBStore, processed_dir: Path | None = None) -> SearchIndexSummary:
    processed_dir = processed_dir or PROCESSED_DIR
    parquet_path = _search_index_parquet_path(processed_dir)
    current_snapshot = build_search_index_source_snapshot(store, processed_dir)
    manifest_path = _search_index_manifest_path(processed_dir)
    manifest = _load_search_index_manifest(manifest_path)

    if int(current_snapshot.get("expected_row_count") or 0) <= 0:
        if manifest is not None:
            return _summary_from_manifest(manifest, manifest_path)
        return SearchIndexSummary(
            row_count=0,
            record_type_counts={},
            source_tables=[],
            parquet_path=str(parquet_path),
            source_snapshot=current_snapshot,
            source_fingerprint=str(current_snapshot.get("signature") or ""),
            manifest_path=str(manifest_path),
        )

    if _should_refresh_search_index(store, current_snapshot, manifest):
        return materialize_search_index(store, processed_dir, source_snapshot=current_snapshot)

    return _summary_from_manifest(manifest, manifest_path)


def build_search_index_source_snapshot(store: DuckDBStore, processed_dir: Path | None = None) -> dict[str, Any]:
    processed_dir = processed_dir or PROCESSED_DIR
    contract_source = _source_snapshot_for_contracts(store, processed_dir)
    process_source = _source_snapshot_for_single_source(store, processed_dir, "pae_processes", "pae_processes.parquet")
    addition_source = _source_snapshot_for_single_source(store, processed_dir, "pae_additions", "pae_additions.parquet")

    snapshot = {
        "contract": contract_source,
        "process": process_source,
        "addition": addition_source,
    }
    snapshot["expected_row_count"] = int(
        contract_source["row_count"] + process_source["row_count"] + addition_source["row_count"]
    )
    snapshot["source_tables"] = [
        contract_source["source"],
        process_source["source"],
        addition_source["source"],
    ]
    snapshot["signature"] = _fingerprint(snapshot)
    return snapshot


def _should_refresh_search_index(
    store: DuckDBStore,
    current_snapshot: dict[str, Any],
    manifest: dict[str, Any] | None,
) -> bool:
    expected_row_count = int(current_snapshot.get("expected_row_count") or 0)
    if expected_row_count <= 0:
        return False

    if manifest is None:
        return True

    if not store.has_table(SEARCH_INDEX_TABLE):
        return True

    try:
        table_rows = store.count(SEARCH_INDEX_TABLE)
    except Exception:
        return True

    if int(table_rows) != int(manifest.get("row_count") or 0):
        return True

    if int(table_rows) != int(current_snapshot.get("expected_row_count") or 0):
        return True

    manifest_signature = str(manifest.get("source_fingerprint") or "")
    current_signature = str(current_snapshot.get("signature") or "")
    return manifest_signature != current_signature


def _summary_from_manifest(manifest: dict[str, Any], manifest_path: Path) -> SearchIndexSummary:
    return SearchIndexSummary(
        row_count=int(manifest.get("row_count") or 0),
        record_type_counts={
            str(key): int(value)
            for key, value in dict(manifest.get("record_type_counts") or {}).items()
        },
        source_tables=[str(value) for value in manifest.get("source_tables") or []],
        parquet_path=str(manifest.get("parquet_path") or SEARCH_INDEX_PARQUET),
        source_snapshot=dict(manifest.get("source_snapshot") or {}),
        source_fingerprint=str(manifest.get("source_fingerprint") or ""),
        manifest_path=str(manifest_path),
    )


def _load_search_index_manifest(manifest_path: Path) -> dict[str, Any] | None:
    if not manifest_path.exists():
        return None
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _search_index_manifest_path(processed_dir: Path) -> Path:
    return processed_dir / SEARCH_INDEX_MANIFEST_NAME


def _search_index_parquet_path(processed_dir: Path) -> Path:
    return processed_dir / SEARCH_INDEX_PARQUET_NAME


def _fingerprint(snapshot: dict[str, Any]) -> str:
    payload = json.dumps(snapshot, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _source_snapshot_for_contracts(store: DuckDBStore, processed_dir: Path) -> dict[str, Any]:
    scored = _source_snapshot_for_single_source(store, processed_dir, "pae_contracts_scored", "pae_contracts_scored.parquet")
    enriched = _source_snapshot_for_single_source(store, processed_dir, "pae_contracts_enriched", "pae_contracts_enriched.parquet")
    core = _source_snapshot_for_single_source(store, processed_dir, "pae_contracts_core", "pae_contracts_core.parquet")

    if scored["row_count"] and enriched["row_count"]:
        source_prefix = "duckdb" if any(
            component["source"].startswith("duckdb:") for component in (scored, enriched)
        ) else "parquet"
        return {
            "source": f"{source_prefix}:pae_contracts_enriched+pae_contracts_scored",
            "row_count": int(enriched["row_count"]),
            "components": [scored, enriched],
        }

    if scored["row_count"]:
        return {
            "source": scored["source"],
            "row_count": int(scored["row_count"]),
            "components": [scored],
        }

    if enriched["row_count"]:
        return {
            "source": enriched["source"],
            "row_count": int(enriched["row_count"]),
            "components": [enriched],
        }

    return {
        "source": core["source"],
        "row_count": int(core["row_count"]),
        "components": [core],
    }


def _source_snapshot_for_single_source(store: DuckDBStore, processed_dir: Path, table_name: str, parquet_name: str) -> dict[str, Any]:
    parquet_path = processed_dir / parquet_name
    if store.has_table(table_name):
        row_count = store.count(table_name)
        source_path = str(store.path)
        stat = store.path.stat() if store.path.exists() else None
        return {
            "source": f"duckdb:{table_name}",
            "table_name": table_name,
            "row_count": int(row_count),
            "path": source_path,
            "mtime_ns": int(stat.st_mtime_ns) if stat else 0,
            "size": int(stat.st_size) if stat else 0,
        }

    if parquet_path.exists():
        frame = pd.read_parquet(parquet_path)
        stat = parquet_path.stat()
        return {
            "source": f"parquet:{parquet_name}",
            "table_name": table_name,
            "row_count": int(len(frame)),
            "path": str(parquet_path),
            "mtime_ns": int(stat.st_mtime_ns),
            "size": int(stat.st_size),
        }

    return {
        "source": f"missing:{table_name}",
        "table_name": table_name,
        "row_count": 0,
        "path": "",
        "mtime_ns": 0,
        "size": 0,
    }


def build_search_index_frame(
    contracts_frame: Optional[pd.DataFrame] = None,
    processes_frame: Optional[pd.DataFrame] = None,
    additions_frame: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    contract_source = contracts_frame if contracts_frame is not None else pd.DataFrame()
    process_source = processes_frame if processes_frame is not None else pd.DataFrame()
    additions_source = additions_frame if additions_frame is not None else pd.DataFrame()
    contract_records = _contract_records(contract_source)
    process_records = _process_records(process_source)
    addition_records = _addition_records(additions_source, contract_records)

    rows = contract_records + process_records + addition_records
    if not rows:
        return pd.DataFrame(
            columns=[
                "record_type",
                "record_id",
                "contract_id",
                "process_id",
                "entity_name",
                "entity_nit",
                "supplier_name",
                "supplier_nit",
                "department",
                "municipality",
                "modality",
                "status",
                "amount",
                "date",
                "start_date",
                "end_date",
                "year",
                "month",
                "risk_score",
                "risk_level",
                "risk_summary",
                "risk_limitations",
                "source_table",
                "url_process",
                "search_text",
            ]
        )

    frame = pd.DataFrame(rows)
    if "search_text" in frame.columns:
        frame["search_text"] = frame["search_text"].fillna("").astype(str).map(normalize_text)
    else:
        frame["search_text"] = ""
    for column in ("amount", "risk_score", "record_rank"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def materialize_search_index(
    store: DuckDBStore,
    processed_dir: Path | None = None,
    *,
    source_snapshot: dict[str, Any] | None = None,
) -> SearchIndexSummary:
    processed_dir = processed_dir or PROCESSED_DIR
    processed_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = _search_index_parquet_path(processed_dir)
    source_snapshot = source_snapshot or build_search_index_source_snapshot(store, processed_dir)
    contracts_frame = _load_contracts_frame(store, processed_dir)
    processes_frame = _load_processes_frame(store, processed_dir)
    additions_frame = _load_additions_frame(store, processed_dir)

    index_frame = build_search_index_frame(contracts_frame, processes_frame, additions_frame)
    store.write_frame(SEARCH_INDEX_TABLE, index_frame, replace=True)
    index_frame.to_parquet(parquet_path, index=False)

    record_type_counts = {
        str(record_type): int(count)
        for record_type, count in index_frame["record_type"].value_counts(dropna=False).sort_index().items()
    } if not index_frame.empty else {}
    source_tables = sorted({str(source) for source in index_frame["source_table"].dropna().astype(str).tolist()}) if not index_frame.empty else []
    manifest_path = _search_index_manifest_path(processed_dir)
    manifest_payload = {
        "table_name": SEARCH_INDEX_TABLE,
        "row_count": int(len(index_frame)),
        "record_type_counts": record_type_counts,
        "source_tables": source_tables,
        "parquet_path": str(parquet_path),
        "source_snapshot": source_snapshot,
        "source_fingerprint": str(source_snapshot.get("signature") or _fingerprint(source_snapshot)),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    manifest_path.write_text(json.dumps(manifest_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return SearchIndexSummary(
        row_count=int(len(index_frame)),
        record_type_counts=record_type_counts,
        source_tables=source_tables,
        parquet_path=str(parquet_path),
        source_snapshot=source_snapshot,
        source_fingerprint=str(manifest_payload["source_fingerprint"]),
        manifest_path=str(manifest_path),
    )


def search_index_where_sql(
    *,
    query: Optional[str] = None,
    entity_name: Optional[str] = None,
    department: Optional[str] = None,
    municipality: Optional[str] = None,
    supplier_name: Optional[str] = None,
    modality: Optional[str] = None,
    state: Optional[str] = None,
    record_type: Optional[str] = None,
    min_amount: Optional[float] = None,
    max_amount: Optional[float] = None,
) -> tuple[str, list[Any]]:
    where: list[str] = ["1 = 1"]
    params: list[Any] = []

    def add_like(column: str, value: Optional[str]) -> None:
        if value:
            where.append(f"LOWER(COALESCE({column}, '')) LIKE ?")
            params.append(f"%{value.lower()}%")

    add_like("entity_name", entity_name)
    add_like("department", department)
    add_like("municipality", municipality)
    add_like("supplier_name", supplier_name)
    add_like("modality", modality)
    add_like("state", state)
    add_like("record_type", record_type)

    if query:
        where.append(
            "("
            "LOWER(COALESCE(search_text, '')) LIKE ? OR "
            "LOWER(COALESCE(record_id, '')) LIKE ? OR "
            "LOWER(COALESCE(contract_id, '')) LIKE ? OR "
            "LOWER(COALESCE(process_id, '')) LIKE ? OR "
            "LOWER(COALESCE(entity_name, '')) LIKE ? OR "
            "LOWER(COALESCE(supplier_name, '')) LIKE ? OR "
            "LOWER(COALESCE(description, '')) LIKE ?"
            ")"
        )
        query_value = f"%{query.lower()}%"
        params.extend([query_value] * 7)

    if min_amount is not None:
        where.append("COALESCE(CAST(amount AS DOUBLE), 0) >= ?")
        params.append(min_amount)

    if max_amount is not None:
        where.append("COALESCE(CAST(amount AS DOUBLE), 0) <= ?")
        params.append(max_amount)

    return " AND ".join(where), params


def search_index_count_sql(
    *,
    query: Optional[str] = None,
    entity_name: Optional[str] = None,
    department: Optional[str] = None,
    municipality: Optional[str] = None,
    supplier_name: Optional[str] = None,
    modality: Optional[str] = None,
    state: Optional[str] = None,
    record_type: Optional[str] = None,
    min_amount: Optional[float] = None,
    max_amount: Optional[float] = None,
) -> tuple[str, list[Any]]:
    where_sql, params = search_index_where_sql(
        query=query,
        entity_name=entity_name,
        department=department,
        municipality=municipality,
        supplier_name=supplier_name,
        modality=modality,
        state=state,
        record_type=record_type,
        min_amount=min_amount,
        max_amount=max_amount,
    )
    return f"SELECT COUNT(*) FROM {SEARCH_INDEX_TABLE} WHERE {where_sql}", params


def search_index_sql(
    *,
    query: Optional[str] = None,
    entity_name: Optional[str] = None,
    department: Optional[str] = None,
    municipality: Optional[str] = None,
    supplier_name: Optional[str] = None,
    modality: Optional[str] = None,
    state: Optional[str] = None,
    record_type: Optional[str] = None,
    min_amount: Optional[float] = None,
    max_amount: Optional[float] = None,
    limit: Optional[int] = 50,
    offset: Optional[int] = 0,
) -> tuple[str, list[Any]]:
    where_sql, params = search_index_where_sql(
        query=query,
        entity_name=entity_name,
        department=department,
        municipality=municipality,
        supplier_name=supplier_name,
        modality=modality,
        state=state,
        record_type=record_type,
        min_amount=min_amount,
        max_amount=max_amount,
    )
    if limit is None:
        sql = f"SELECT * FROM {SEARCH_INDEX_TABLE} WHERE {where_sql} ORDER BY COALESCE(risk_score, 0) DESC, COALESCE(CAST(amount AS DOUBLE), 0) DESC, record_type, record_id"
        return sql, params

    params.append(limit)
    if offset is not None:
        params.append(offset)
        sql = (
            f"SELECT * FROM {SEARCH_INDEX_TABLE} WHERE {where_sql} "
            "ORDER BY COALESCE(risk_score, 0) DESC, COALESCE(CAST(amount AS DOUBLE), 0) DESC, record_type, record_id "
            "LIMIT ? OFFSET ?"
        )
    else:
        sql = (
            f"SELECT * FROM {SEARCH_INDEX_TABLE} WHERE {where_sql} "
            "ORDER BY COALESCE(risk_score, 0) DESC, COALESCE(CAST(amount AS DOUBLE), 0) DESC, record_type, record_id "
            "LIMIT ?"
        )
    return sql, params


def _load_contracts_frame(store: DuckDBStore, processed_dir: Path) -> pd.DataFrame:
    scored = _load_from_store(store, "pae_contracts_scored")
    enriched = _load_from_store(store, "pae_contracts_enriched")
    core = _load_from_store(store, "pae_contracts_core")

    if not scored.empty and not enriched.empty:
        return _merge_contract_risk(enriched, scored)
    if not scored.empty:
        return scored
    if not enriched.empty:
        return enriched
    if not core.empty:
        return core

    scored_disk = _load_from_disk(processed_dir, "pae_contracts_scored.parquet")
    enriched_disk = _load_from_disk(processed_dir, "pae_contracts_enriched.parquet")
    core_disk = _load_from_disk(processed_dir, "pae_contracts_core.parquet")
    if not enriched_disk.empty and not scored_disk.empty:
        return _merge_contract_risk(enriched_disk, scored_disk)
    if not scored_disk.empty:
        return scored_disk
    if not enriched_disk.empty:
        return enriched_disk
    return core_disk


def _load_processes_frame(store: DuckDBStore, processed_dir: Path) -> pd.DataFrame:
    frame = _load_from_store(store, "pae_processes")
    if not frame.empty:
        return frame
    return _load_from_disk(processed_dir, "pae_processes.parquet")


def _load_additions_frame(store: DuckDBStore, processed_dir: Path) -> pd.DataFrame:
    frame = _load_from_store(store, "pae_additions")
    if not frame.empty:
        return frame
    return _load_from_disk(processed_dir, "pae_additions.parquet")


def _load_from_store(store: DuckDBStore, table_name: str) -> pd.DataFrame:
    if store.has_table(table_name):
        frame = store.read_frame(f"SELECT * FROM {table_name}")
        if not frame.empty:
            return frame
    return pd.DataFrame()


def _load_from_disk(processed_dir: Path, filename: str) -> pd.DataFrame:
    path = processed_dir / filename
    if path.exists():
        frame = pd.read_parquet(path)
        if not frame.empty:
            return frame
    return pd.DataFrame()


def _merge_contract_risk(enriched: pd.DataFrame, scored: pd.DataFrame) -> pd.DataFrame:
    left = enriched.copy()
    right = scored.copy()

    if "contract_id" not in left.columns:
        if "id_contrato" in left.columns:
            left["contract_id"] = left["id_contrato"].astype(str)
        elif "referencia_del_contrato" in left.columns:
            left["contract_id"] = left["referencia_del_contrato"].astype(str)
        else:
            left["contract_id"] = ""
    if "contract_id" not in right.columns:
        if "id_contrato" in right.columns:
            right["contract_id"] = right["id_contrato"].astype(str)
        else:
            right["contract_id"] = ""

    risk_columns = [
        column
        for column in ("risk_score", "risk_level", "risk_flags_json", "risk_dimension_scores_json", "risk_summary", "risk_limitations")
        if column in right.columns
    ]
    if not risk_columns:
        return left

    risk_frame = right[["contract_id"] + risk_columns].copy()
    risk_frame["contract_id"] = risk_frame["contract_id"].fillna("").astype(str)
    risk_frame = risk_frame[risk_frame["contract_id"].str.strip().ne("")]
    risk_frame = risk_frame.drop_duplicates(subset=["contract_id"], keep="first")
    merged = left.merge(risk_frame, on="contract_id", how="left", suffixes=("", "_risk"))
    for column in risk_columns:
        risk_column = f"{column}_risk"
        if risk_column in merged.columns:
            if column in merged.columns:
                merged[column] = merged[column].fillna(merged[risk_column])
            else:
                merged[column] = merged[risk_column]
            merged = merged.drop(columns=[risk_column])
    return merged


def _contract_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    records: list[dict[str, Any]] = []
    for row in frame.to_dict(orient="records"):
        contract_id = _first_value(row, "contract_id", "id_contrato", "referencia_del_contrato", "reference_id")
        process_id = _first_value(row, "process_id", "proceso_de_compra", "id_del_proceso")
        entity_name = _first_value(row, "entity_name", "nombre_entidad", "entidad", "process_entity_norm")
        entity_nit = _first_value(row, "entity_nit", "nit_entidad", "entity_doc", "codigo_entidad_creadora", "codigo_entidad")
        supplier_name = _first_value(row, "supplier_name", "proveedor_adjudicado", "proveedor")
        supplier_nit = _first_value(row, "supplier_nit", "supplier_doc_norm", "supplier_doc", "documento_proveedor", "nit_proveedor")
        department = _first_value(row, "department", "departamento", "department_norm")
        municipality = _first_value(row, "municipality", "ciudad", "municipality_norm")
        modality = _first_value(row, "modality", "modalidad_de_contratacion", "modalidad_de_contratacion_x", "modalidad_de_contratacion_y")
        status = _first_value(row, "status", "estado_contrato")
        amount = _first_number(row, "amount", "valor", "valor_total_adjudicacion", "precio_base")
        date = _first_value(row, "date", "fecha_de_firma", "fecha_de_publicacion_del")
        start_date = _first_value(row, "start_date", "date", "fecha_de_firma", "fecha_de_publicacion_del")
        end_date = _first_value(row, "end_date", "fecha_de_terminacion", "fecha_de_fin")
        url_process = _first_value(row, "url_process", "urlproceso", "url_secop")
        description = _first_value(row, "object_text", "descripcion_del_proceso", "nombre_del_procedimiento")
        justification = _first_value(row, "justification", "justificacion_modalidad_de")
        risk_score = _first_number(row, "risk_score", "score")
        risk_level = _first_value(row, "risk_level")
        year = _first_number(row, "year", "core_year", "process_year")
        month = _first_number(row, "month", "signature_month")
        risk_summary = _first_value(row, "risk_summary", "score_explanation")
        risk_limitations = _first_value(row, "risk_limitations", "limitations", "required_manual_checks")
        search_text = _compose_search_text(
            contract_id,
            process_id,
            entity_name,
            entity_nit,
            supplier_name,
            supplier_nit,
            department,
            municipality,
            modality,
            status,
            description,
            risk_summary,
            justification,
            url_process,
        )
        records.append(
            {
                "record_type": "contract",
                "record_id": contract_id or process_id,
                "contract_id": contract_id,
                "process_id": process_id,
                "entity_name": entity_name,
                "entity_nit": entity_nit,
                "supplier_name": supplier_name,
                "supplier_nit": supplier_nit,
                "department": department,
                "municipality": municipality,
                "modality": modality,
                "status": status,
                "amount": amount,
                "date": date,
                "start_date": start_date,
                "end_date": end_date,
                "year": year,
                "month": month,
                "risk_score": risk_score,
                "risk_level": risk_level,
                "risk_summary": risk_summary,
                "risk_limitations": risk_limitations,
                "source_table": _source_table_for_contracts(frame),
                "url_process": _coerce_url(url_process),
                "description": description,
                "justification": justification,
                "search_text": search_text,
            }
        )
    return records


def _process_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    records: list[dict[str, Any]] = []
    for row in frame.to_dict(orient="records"):
        process_id = _first_value(row, "process_id", "id_del_proceso")
        portfolio_id = _first_value(row, "process_portfolio_id", "id_del_portafolio")
        entity_name = _first_value(row, "entity_name", "entidad", "process_entity_norm")
        entity_nit = _first_value(row, "entity_nit", "nit_entidad", "entity_doc")
        department = _first_value(row, "department", "department_norm")
        municipality = _first_value(row, "municipality", "municipality_norm")
        modality = _first_value(row, "modality", "modalidad_de_contratacion", "modalidad_de_contratacion_y", "modalidad_de_contratacion_x")
        status = _first_value(row, "status")
        amount = _first_number(row, "amount", "valor_total_adjudicacion", "precio_base")
        date = _first_value(row, "date", "fecha_de_publicacion_del")
        start_date = _first_value(row, "start_date", "date", "fecha_de_publicacion_del")
        year = _first_number(row, "year", "process_year")
        month = _first_number(row, "month")
        url_process = _first_value(row, "url_process", "urlproceso", "urlproceso_x", "urlproceso_y")
        description = _first_value(row, "description", "nombre_del_procedimiento", "referencia_del_proceso")
        risk_summary = _first_value(row, "risk_summary", "score_explanation")
        search_text = _compose_search_text(
            process_id,
            portfolio_id,
            entity_name,
            entity_nit,
            department,
            municipality,
            modality,
            status,
            description,
            risk_summary,
            url_process,
        )
        records.append(
            {
                "record_type": "process",
                "record_id": process_id or portfolio_id,
                "contract_id": "",
                "process_id": process_id,
                "process_portfolio_id": portfolio_id,
                "entity_name": entity_name,
                "entity_nit": entity_nit,
                "supplier_name": "",
                "supplier_nit": "",
                "department": department,
                "municipality": municipality,
                "modality": modality,
                "status": status,
                "amount": amount,
                "date": date,
                "start_date": start_date,
                "end_date": "",
                "year": year,
                "month": month,
                "risk_score": None,
                "risk_level": None,
                "risk_summary": risk_summary,
                "risk_limitations": _first_value(row, "risk_limitations", "limitations", "required_manual_checks"),
                "source_table": "pae_processes",
                "url_process": _coerce_url(url_process),
                "description": description,
                "justification": "",
                "search_text": search_text,
            }
        )
    return records


def _addition_records(frame: pd.DataFrame, contract_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    contract_lookup = {str(record.get("contract_id") or ""): record for record in contract_records if record.get("contract_id")}
    records: list[dict[str, Any]] = []
    for row in frame.to_dict(orient="records"):
        contract_id = _first_value(row, "contract_id")
        addition_id = _first_value(row, "addition_id")
        addition_type = _first_value(row, "addition_type")
        addition_description = _first_value(row, "addition_description")
        addition_date = _first_value(row, "addition_date")
        linked = contract_lookup.get(str(contract_id or ""))
        entity_name = linked.get("entity_name") if linked else ""
        entity_nit = linked.get("entity_nit") if linked else ""
        supplier_name = linked.get("supplier_name") if linked else ""
        supplier_nit = linked.get("supplier_nit") if linked else ""
        department = linked.get("department") if linked else ""
        municipality = linked.get("municipality") if linked else ""
        modality = linked.get("modality") if linked else ""
        status = linked.get("status") if linked else ""
        risk_score = linked.get("risk_score") if linked else None
        risk_level = linked.get("risk_level") if linked else None
        risk_summary = linked.get("risk_summary") if linked else ""
        linked_description = linked.get("description") if linked else ""
        linked_justification = linked.get("justification") if linked else ""
        search_text = _compose_search_text(
            addition_id,
            contract_id,
            entity_name,
            entity_nit,
            supplier_name,
            supplier_nit,
            department,
            municipality,
            addition_type,
            addition_description,
            linked_description,
            risk_summary,
            linked_justification,
        )
        records.append(
            {
                "record_type": "addition",
                "record_id": addition_id or contract_id,
                "contract_id": contract_id,
                "process_id": linked.get("process_id") if linked else "",
                "entity_name": entity_name,
                "entity_nit": entity_nit,
                "supplier_name": supplier_name,
                "supplier_nit": supplier_nit,
                "department": department,
                "municipality": municipality,
                "modality": modality,
                "status": status,
                "amount": 0.0,
                "date": addition_date,
                "start_date": addition_date,
                "end_date": "",
                "year": _first_number(row, "year"),
                "month": _first_number(row, "month"),
                "risk_score": risk_score,
                "risk_level": risk_level,
                "risk_summary": risk_summary,
                "risk_limitations": linked.get("risk_limitations") if linked else "",
                "source_table": "pae_additions",
                "url_process": linked.get("url_process") if linked else "",
                "description": addition_description,
                "justification": addition_type,
                "addition_type": addition_type,
                "addition_date": addition_date,
                "search_text": search_text,
            }
        )
    return records


def _source_table_for_contracts(frame: pd.DataFrame) -> str:
    columns = set(frame.columns)
    if {"risk_score", "risk_level"}.issubset(columns):
        return "pae_contracts_scored"
    if "bidder_rows" in columns or "competition_signal" in columns:
        return "pae_contracts_enriched"
    return "pae_contracts_core"


def _compose_search_text(*values: Any) -> str:
    return normalize_text(" ".join(str(value) for value in values if value not in (None, "")))


def _first_value(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        if key not in row:
            continue
        value = row.get(key)
        if isinstance(value, dict) and "url" in value:
            value = value.get("url")
        if value in (None, ""):
            continue
        try:
            if pd.isna(value):
                continue
        except Exception:
            pass
        return str(value)
    return ""


def _first_number(row: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        if key not in row:
            continue
        value = row.get(key)
        if value in (None, ""):
            continue
        try:
            if pd.isna(value):
                continue
        except Exception:
            pass
        try:
            return float(value)
        except Exception:
            continue
    return None


def _coerce_url(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("url") or "")
    if value in (None, ""):
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value)
