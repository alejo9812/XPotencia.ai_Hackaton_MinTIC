from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from ..config import DatasetSpec, load_keyword_registry, load_dataset_registry, load_column_registry, load_risk_registry, normalize_text
from ..paths import PROCESSED_DIR, RAW_DIR, ensure_runtime_dirs
from ..storage.duckdb_store import DuckDBStore
from ..connectors.socrata_client import SocrataClient
from .schema_normalizer import (
    classify_pae_record,
    dedupe_addition_records,
    dedupe_records,
    normalize_addition_row,
    normalize_row,
    resolve_addition_columns,
    resolve_core_columns,
    rows_to_frame,
)


@dataclass(frozen=True)
class IngestionSummary:
    dataset_id: str
    years: list[int]
    rows_fetched: int
    rows_kept: int
    cache_hits: int
    raw_parquet: str
    processed_parquet: str
    duckdb_path: str
    manifest_path: str


@dataclass(frozen=True)
class SecondaryLoadSummary:
    dataset_id: str
    source_parquet: str
    rows_fetched: int
    rows_kept: int
    raw_parquet: str
    processed_parquet: str
    duckdb_path: str
    manifest_path: str


class PAEIncrementalLoader:
    def __init__(
        self,
        client: SocrataClient,
        store: DuckDBStore,
        raw_dir: Path | None = None,
        processed_dir: Path | None = None,
    ) -> None:
        self.client = client
        self.store = store
        self.raw_dir = raw_dir or RAW_DIR
        self.processed_dir = processed_dir or PROCESSED_DIR
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    def load_sample(
        self,
        years: list[int],
        sample_limit: int = 500,
        per_year_limit: int = 100,
        dataset_id: str | None = None,
    ) -> IngestionSummary:
        ensure_runtime_dirs()
        dataset = self._core_dataset(dataset_id)
        keyword_catalog = load_keyword_registry()
        column_registry = load_column_registry()
        risk_registry = load_risk_registry()
        columns = column_registry.get("core", {})
        metadata = self.client.get_metadata(dataset.id)
        column_names = [column.get("fieldName") or column.get("name") for column in metadata.get("columns", []) if column.get("fieldName") or column.get("name")]
        resolved = resolve_core_columns(column_names, columns)

        query_columns = [
            resolved.get("contract_id"),
            resolved.get("process_id"),
            resolved.get("entity_name"),
            resolved.get("entity_nit"),
            resolved.get("supplier_name"),
            resolved.get("supplier_doc"),
            resolved.get("object_text"),
            resolved.get("justification"),
            resolved.get("modality"),
            resolved.get("status"),
            resolved.get("amount"),
            resolved.get("estimated_amount"),
            resolved.get("date"),
            resolved.get("department"),
            resolved.get("municipality"),
            resolved.get("url_process"),
        ]
        query_columns = [column for column in query_columns if column]
        text_fields = [
            resolved.get("object_text"),
            resolved.get("modality"),
        ]
        text_fields = [field for field in text_fields if field]

        year_field = dataset.default_year_field or resolved.get("date")
        rows: list[dict[str, Any]] = []
        cache_hits = 0
        fetched = 0

        for year in years:
            where = build_pae_where_clause(year_field, year, text_fields, keyword_catalog)
            for result in self.client.iter_rows(dataset.id, select=query_columns, where=where, page_size=per_year_limit, max_rows=per_year_limit):
                cache_hits += int(result.from_cache)
                fetched += len(result.rows)
                rows.extend(self._normalize_batch(result.rows, resolved, keyword_catalog))

        deduped = dedupe_records(rows)
        kept = deduped[:sample_limit]

        raw_parquet = self.raw_dir / "pae_contracts_sample.parquet"
        processed_parquet = self.processed_dir / "pae_contracts_core.parquet"
        raw_frame = rows_to_frame(kept)
        processed_frame = rows_to_frame(kept)
        raw_frame.to_parquet(raw_parquet, index=False)
        processed_frame.to_parquet(processed_parquet, index=False)
        self.store.write_frame("pae_contracts_core", processed_frame, replace=True)

        manifest = {
            "dataset": asdict(dataset),
            "years": years,
            "rows_fetched": fetched,
            "rows_kept": len(kept),
            "cache_hits": cache_hits,
            "query_columns": query_columns,
            "year_field": year_field,
            "risk_dimensions": risk_registry.get("dimensions", []),
        }
        manifest_path = self.raw_dir / "pae_contracts_sample.manifest.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

        return IngestionSummary(
            dataset_id=dataset.id,
            years=years,
            rows_fetched=fetched,
            rows_kept=len(kept),
            cache_hits=cache_hits,
            raw_parquet=str(raw_parquet),
            processed_parquet=str(processed_parquet),
            duckdb_path=str(self.store.path),
            manifest_path=str(manifest_path),
        )

    def load_additions(
        self,
        core_parquet: Path | None = None,
        dataset_id: str | None = None,
        batch_size: int = 25,
        sample_limit: int | None = None,
    ) -> SecondaryLoadSummary:
        ensure_runtime_dirs()
        dataset = self._secondary_dataset(dataset_id or "cb9c-h8sn", default_name="SECOP II - Adiciones")
        source_parquet = Path(core_parquet or (PROCESSED_DIR / "pae_contracts_core.parquet"))
        if not source_parquet.exists():
            raise FileNotFoundError(f"Core parquet not found at {source_parquet}")

        core_frame = pd.read_parquet(source_parquet)
        contract_column = next((column for column in ("contract_id", "id_contrato", "referencia_del_contrato", "referencia_contrato") if column in core_frame.columns), None)
        contract_ids = sorted(
            {
                str(value).strip()
                for value in core_frame.get(contract_column or "contract_id", pd.Series(dtype="object")).dropna().astype(str).tolist()
                if str(value).strip()
            }
        )
        if not contract_ids:
            raise ValueError("No contract ids available to fetch additions.")

        metadata = self.client.get_metadata(dataset.id)
        column_names = [column.get("fieldName") or column.get("name") for column in metadata.get("columns", []) if column.get("fieldName") or column.get("name")]
        resolved = resolve_addition_columns(column_names)

        query_columns = [
            resolved.get("addition_id"),
            resolved.get("contract_id"),
            resolved.get("addition_type"),
            resolved.get("addition_description"),
            resolved.get("addition_date"),
        ]
        query_columns = [column for column in query_columns if column]

        rows: list[dict[str, Any]] = []
        fetched = 0
        for batch in batch_chunks(contract_ids, batch_size):
            where = build_id_clause(resolved.get("contract_id") or "id_contrato", batch)
            if not where:
                continue
            for result in self.client.iter_rows(dataset.id, select=query_columns, where=where, page_size=batch_size * 5):
                fetched += len(result.rows)
                rows.extend(self._normalize_additions_batch(result.rows, resolved))

        deduped = dedupe_addition_records(rows)
        kept = deduped if sample_limit is None else deduped[:sample_limit]

        raw_parquet = self.raw_dir / "pae_additions_sample.parquet"
        processed_parquet = self.processed_dir / "pae_additions.parquet"
        raw_frame = rows_to_frame(kept)
        processed_frame = rows_to_frame(kept)
        raw_frame.to_parquet(raw_parquet, index=False)
        processed_frame.to_parquet(processed_parquet, index=False)
        self.store.write_frame("pae_additions", processed_frame, replace=True)

        manifest = {
            "dataset": asdict(dataset),
            "source_parquet": str(source_parquet),
            "contract_count": len(contract_ids),
            "rows_fetched": fetched,
            "rows_kept": len(kept),
            "batch_size": batch_size,
        }
        manifest_path = self.raw_dir / "pae_additions_sample.manifest.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

        return SecondaryLoadSummary(
            dataset_id=dataset.id,
            source_parquet=str(source_parquet),
            rows_fetched=fetched,
            rows_kept=len(kept),
            raw_parquet=str(raw_parquet),
            processed_parquet=str(processed_parquet),
            duckdb_path=str(self.store.path),
            manifest_path=str(manifest_path),
        )

    def _normalize_batch(
        self,
        rows: list[dict[str, Any]],
        resolved: dict[str, str | None],
        keyword_catalog: dict[str, Any],
    ) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for row in rows:
            base = normalize_row(row, resolved)
            normalized.append(classify_pae_record(base, keyword_catalog))
        return normalized

    def _normalize_additions_batch(
        self,
        rows: list[dict[str, Any]],
        resolved: dict[str, str | None],
    ) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for row in rows:
            normalized.append(normalize_addition_row(row, resolved))
        return normalized

    def _core_dataset(self, dataset_id: str | None) -> DatasetSpec:
        dataset_id = dataset_id or "jbjy-vk9h"
        for spec in load_dataset_registry().get("datasets", []):
            if spec.get("id") == dataset_id or spec.get("key") == "core_contracts":
                return DatasetSpec(
                    key=str(spec.get("key", "core_contracts")),
                    id=str(spec.get("id", dataset_id)),
                    name=str(spec.get("name", "SECOP II - Contratos Electrónicos")),
                    role=str(spec.get("role", "core")),
                    active=bool(spec.get("active", True)),
                    default_year_field=spec.get("default_year_field"),
                    text_fields=tuple(spec.get("text_fields", []) or ()),
                    id_fields=dict(spec.get("id_fields", {}) or {}),
                )
        raise FileNotFoundError(f"Dataset {dataset_id} not configured.")

    def _secondary_dataset(self, dataset_id: str, default_name: str) -> DatasetSpec:
        for spec in load_dataset_registry().get("datasets", []):
            if spec.get("id") == dataset_id:
                return DatasetSpec(
                    key=str(spec.get("key", dataset_id)),
                    id=str(spec.get("id", dataset_id)),
                    name=str(spec.get("name", default_name)),
                    role=str(spec.get("role", "secondary")),
                    active=bool(spec.get("active", True)),
                    default_year_field=spec.get("default_year_field"),
                    text_fields=tuple(spec.get("text_fields", []) or ()),
                    id_fields=dict(spec.get("id_fields", {}) or {}),
                )
        return DatasetSpec(
            key=dataset_id,
            id=dataset_id,
            name=default_name,
            role="secondary",
            active=True,
        )


def build_pae_where_clause(year_field: str | None, year: int, text_fields: list[str], keyword_catalog: dict[str, Any]) -> str:
    clauses: list[str] = []
    if year_field:
        clauses.append(f"{year_field} >= '{year:04d}-01-01T00:00:00' AND {year_field} < '{year + 1:04d}-01-01T00:00:00'")

    high = keyword_catalog.get("high_confidence", [])
    medium = keyword_catalog.get("medium_confidence", [])
    search_terms = high + medium
    include_clause = build_text_clause(text_fields, search_terms)
    if include_clause:
        clauses.append(include_clause)
    return " AND ".join(f"({clause})" for clause in clauses if clause)


def build_text_clause(fields: list[str], terms: list[str]) -> str:
    pieces: list[str] = []
    for field in fields:
        field_terms = [f"UPPER(COALESCE({field}, '')) LIKE '%{normalize_text(term)}%'" for term in terms if normalize_text(term)]
        if field_terms:
            pieces.append("(" + " OR ".join(field_terms) + ")")
    return "(" + " OR ".join(pieces) + ")" if pieces else ""


def build_exclude_clause(fields: list[str], terms: list[str]) -> str:
    pieces: list[str] = []
    for field in fields:
        field_terms = [f"UPPER(COALESCE({field}, '')) NOT LIKE '%{normalize_text(term)}%'" for term in terms if normalize_text(term)]
        if field_terms:
            pieces.append("(" + " AND ".join(field_terms) + ")")
    return "(" + " AND ".join(pieces) + ")" if pieces else ""


def build_id_clause(field_name: str, values: list[str]) -> str:
    escaped = ["'" + value.replace("'", "''") + "'" for value in values if value]
    if not escaped:
        return ""
    return f"{field_name} IN ({','.join(escaped)})"


def batch_chunks(values: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        size = 1
    return [values[index : index + size] for index in range(0, len(values), size)]
