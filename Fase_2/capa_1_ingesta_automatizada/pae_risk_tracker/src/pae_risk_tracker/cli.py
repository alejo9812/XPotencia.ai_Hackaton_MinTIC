from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .connectors.socrata_client import SocrataClient
from .diagnostics.process_diagnostics import write_process_diagnostic_report
from .ingestion.data_pack_loader import DataPackLoader
from .retrieval.search_index import materialize_search_index
from .risk.scoring import score_contracts_frame, summarize_scores
from .ingestion.incremental_loader import PAEIncrementalLoader
from .paths import CACHE_DIR, DEFAULT_DUCKDB_PATH, OUTPUT_DIR, PROCESSED_DIR, RAW_DIR, ensure_runtime_dirs
from .storage.cache import JsonCache
from .storage.duckdb_store import DuckDBStore
from .validation.catalog import SourceCatalogVerifier
from .validation.service import ValidationService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pae-risk-tracker")
    sub = parser.add_subparsers(dest="command", required=True)

    discover = sub.add_parser("discover-schema", help="Fetch dataset schemas and cache metadata")
    discover.add_argument("--dataset-id", default="jbjy-vk9h")

    ingest = sub.add_parser("ingest", help="Download a PAE sample and persist it in DuckDB")
    ingest.add_argument("--years", nargs="*", type=int, default=[2022, 2023, 2024, 2025, 2026])
    ingest.add_argument("--sample-limit", type=int, default=500)
    ingest.add_argument("--per-year-limit", type=int, default=100)
    ingest.add_argument("--duckdb-path", default=str(DEFAULT_DUCKDB_PATH))
    ingest.add_argument("--dataset-id", default="jbjy-vk9h")
    ingest.add_argument("--no-cache", action="store_true")
    ingest.add_argument("--timeout", type=int, default=int(os.getenv("PAE_TIMEOUT_SECONDS", "30")))
    ingest.add_argument("--retries", type=int, default=int(os.getenv("PAE_MAX_RETRIES", "4")))
    ingest.add_argument("--app-token", default=os.getenv("SOCRATA_APP_TOKEN", ""))
    ingest.add_argument("--domain", default=os.getenv("SOCRATA_DOMAIN", "www.datos.gov.co"))

    additions = sub.add_parser("load-additions", help="Download SECOP II additions for the current PAE sample")
    additions.add_argument("--core-parquet", default=str(PROCESSED_DIR / "pae_contracts_core.parquet"))
    additions.add_argument("--dataset-id", default="cb9c-h8sn")
    additions.add_argument("--batch-size", type=int, default=25)
    additions.add_argument("--sample-limit", type=int, default=500)
    additions.add_argument("--duckdb-path", default=str(DEFAULT_DUCKDB_PATH))
    additions.add_argument("--no-cache", action="store_true")
    additions.add_argument("--timeout", type=int, default=int(os.getenv("PAE_TIMEOUT_SECONDS", "30")))
    additions.add_argument("--retries", type=int, default=int(os.getenv("PAE_MAX_RETRIES", "4")))
    additions.add_argument("--app-token", default=os.getenv("SOCRATA_APP_TOKEN", ""))
    additions.add_argument("--domain", default=os.getenv("SOCRATA_DOMAIN", "www.datos.gov.co"))

    paco = sub.add_parser("load-paco", help="Download PACO sources and materialize a local PACO pack")
    paco.add_argument("--duckdb-path", default=str(DEFAULT_DUCKDB_PATH))
    paco.add_argument("--refresh", action="store_true", help="Redownload PACO files even if cached copies exist")
    paco.add_argument("--include-disabled", action="store_true", help="Include sources marked as disabled in the registry")
    paco.add_argument("--source-keys", nargs="*", default=[], help="Optional PACO source keys to load")

    sync = sub.add_parser("sync-pack", help="Refresh SECOP II sample data and PACO into the local data pack")
    sync.add_argument("--years", nargs="*", type=int, default=[2022, 2023, 2024, 2025, 2026])
    sync.add_argument("--sample-limit", type=int, default=500)
    sync.add_argument("--per-year-limit", type=int, default=100)
    sync.add_argument("--batch-size", type=int, default=25)
    sync.add_argument("--duckdb-path", default=str(DEFAULT_DUCKDB_PATH))
    sync.add_argument("--dataset-id", default="jbjy-vk9h")
    sync.add_argument("--no-cache", action="store_true")
    sync.add_argument("--timeout", type=int, default=int(os.getenv("PAE_TIMEOUT_SECONDS", "30")))
    sync.add_argument("--retries", type=int, default=int(os.getenv("PAE_MAX_RETRIES", "4")))
    sync.add_argument("--app-token", default=os.getenv("SOCRATA_APP_TOKEN", ""))
    sync.add_argument("--domain", default=os.getenv("SOCRATA_DOMAIN", "www.datos.gov.co"))
    sync.add_argument("--refresh-paco", action="store_true")
    sync.add_argument("--include-disabled-paco", action="store_true")
    sync.add_argument("--paco-source-keys", nargs="*", default=[], help="Optional PACO source keys to load")
    sync.add_argument("--skip-source-verify", action="store_true", help="Skip the catalog verification step after refreshing the pack")
    sync.add_argument("--report-dir", default=str(OUTPUT_DIR.parent / "validation" / "reports"))
    sync.add_argument("--manifest-path", default=str(CACHE_DIR / "paco" / "paco_pack_manifest.json"))

    score = sub.add_parser("score", help="Score the ingested PAE contracts stored in DuckDB")
    score.add_argument("--duckdb-path", default=str(DEFAULT_DUCKDB_PATH))
    score.add_argument("--input-table", default="pae_contracts_core")
    score.add_argument("--output-table", default="pae_contracts_scored")
    score.add_argument("--output-json", default=str(OUTPUT_DIR / "pae_risk_scores.json"))
    score.add_argument("--top-k", type=int, default=20)
    score.add_argument("--with-context", nargs="*", default=[], help="Optional secondary DuckDB tables to load if present")

    index_cmd = sub.add_parser("materialize-index", help="Build the unified DuckDB search index from processes, contracts and additions")
    index_cmd.add_argument("--duckdb-path", default=str(DEFAULT_DUCKDB_PATH))
    index_cmd.add_argument("--processed-dir", default=str(PROCESSED_DIR))

    validate = sub.add_parser("validate-sources", help="Profile PACO first, then review SECOP and external public sources")
    validate.add_argument("--duckdb-path", default=str(DEFAULT_DUCKDB_PATH))
    validate.add_argument("--source-table", default="pae_search_index")
    validate.add_argument("--limit", type=int, default=25)
    validate.add_argument("--deep", action="store_true", help="Force external source review even if earlier stages are conclusive")
    validate.add_argument("--no-registry-sources", action="store_true", help="Skip registry monitoring sources and only validate record-specific evidence")

    diagnose = sub.add_parser("diagnose-process", help="Build a process diagnostic pack with real and synthetic cases")
    diagnose.add_argument("--duckdb-path", default=str(DEFAULT_DUCKDB_PATH))
    diagnose.add_argument("--processed-dir", default=str(PROCESSED_DIR))
    diagnose.add_argument("--output-json", default=str(OUTPUT_DIR / "process_diagnostics.json"))
    diagnose.add_argument("--output-csv", default=str(OUTPUT_DIR / "process_diagnostic_cases.csv"))
    diagnose.add_argument("--limit", type=int, default=8)
    diagnose.add_argument("--synthetic-count", type=int, default=4)

    verify = sub.add_parser("verify-sources", help="Compare the official SECOP II and PACO sources against the local catalog and cache")
    verify.add_argument("--duckdb-path", default=str(DEFAULT_DUCKDB_PATH))
    verify.add_argument("--report-dir", default=str(OUTPUT_DIR.parent / "validation" / "reports"))
    verify.add_argument("--manifest-path", default=str(CACHE_DIR / "paco" / "paco_pack_manifest.json"))
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    ensure_runtime_dirs()

    if args.command == "discover-schema":
        client = _make_client(no_cache=False, timeout=int(os.getenv("PAE_TIMEOUT_SECONDS", "30")), retries=int(os.getenv("PAE_MAX_RETRIES", "4")), app_token=os.getenv("SOCRATA_APP_TOKEN", ""), domain=os.getenv("SOCRATA_DOMAIN", "www.datos.gov.co"))
        metadata = client.get_metadata(args.dataset_id)
        output = CACHE_DIR / "schema_cache.json"
        output.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Schema cached at {output}")
        return

    if args.command == "ingest":
        client = _make_client(
            no_cache=args.no_cache,
            timeout=args.timeout,
            retries=args.retries,
            app_token=args.app_token,
            domain=args.domain,
        )
        store = DuckDBStore(Path(args.duckdb_path))
        loader = PAEIncrementalLoader(client, store, raw_dir=RAW_DIR, processed_dir=PROCESSED_DIR)
        summary = loader.load_sample(args.years, sample_limit=args.sample_limit, per_year_limit=args.per_year_limit, dataset_id=args.dataset_id)
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return

    if args.command == "load-additions":
        client = _make_client(
            no_cache=args.no_cache,
            timeout=args.timeout,
            retries=args.retries,
            app_token=args.app_token,
            domain=args.domain,
        )
        store = DuckDBStore(Path(args.duckdb_path))
        loader = PAEIncrementalLoader(client, store, raw_dir=RAW_DIR, processed_dir=PROCESSED_DIR)
        summary = loader.load_additions(
            core_parquet=Path(args.core_parquet),
            dataset_id=args.dataset_id,
            batch_size=args.batch_size,
            sample_limit=args.sample_limit,
        )
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return

    if args.command == "load-paco":
        store = DuckDBStore(Path(args.duckdb_path))
        loader = DataPackLoader(
            store,
            raw_dir=RAW_DIR / "paco",
            processed_dir=PROCESSED_DIR / "paco",
            cache_dir=CACHE_DIR / "paco",
        )
        summary = loader.sync_paco(
            refresh=args.refresh,
            include_disabled=args.include_disabled,
            source_keys=args.source_keys,
        )
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return

    if args.command == "sync-pack":
        client = _make_client(
            no_cache=args.no_cache,
            timeout=args.timeout,
            retries=args.retries,
            app_token=args.app_token,
            domain=args.domain,
        )
        store = DuckDBStore(Path(args.duckdb_path))
        secop_loader = PAEIncrementalLoader(client, store, raw_dir=RAW_DIR, processed_dir=PROCESSED_DIR)
        core_summary = secop_loader.load_sample(
            args.years,
            sample_limit=args.sample_limit,
            per_year_limit=args.per_year_limit,
            dataset_id=args.dataset_id,
        )
        additions_summary = secop_loader.load_additions(
            core_parquet=Path(core_summary.processed_parquet),
            dataset_id="cb9c-h8sn",
            batch_size=args.batch_size,
            sample_limit=args.sample_limit,
        )
        paco_loader = DataPackLoader(
            store,
            raw_dir=RAW_DIR / "paco",
            processed_dir=PROCESSED_DIR / "paco",
            cache_dir=CACHE_DIR / "paco",
        )
        paco_summary = paco_loader.sync_paco(
            refresh=args.refresh_paco,
            include_disabled=args.include_disabled_paco,
            source_keys=args.paco_source_keys,
        )
        verification_summary = None
        if not args.skip_source_verify:
            verifier = SourceCatalogVerifier(
                store,
                report_dir=Path(args.report_dir),
                manifest_path=Path(args.manifest_path),
            )
            verification_summary = verifier.run().summary
        payload = {
            "secop_core": core_summary.__dict__,
            "secop_additions": additions_summary.__dict__,
            "paco": paco_summary.__dict__,
        }
        if verification_summary is not None:
            payload["source_catalog"] = verification_summary.to_dict()
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    if args.command == "score":
        store = DuckDBStore(Path(args.duckdb_path))
        frame = store.read_frame(f"SELECT * FROM {args.input_table}")
        external_tables = _load_optional_tables(store, args.with_context)
        scored, summary = score_contracts_frame(frame, external_tables=external_tables)
        store.write_frame(args.output_table, scored, replace=True)
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        top_rows = scored.sort_values(["risk_score", "amount"], ascending=[False, False]).head(args.top_k)
        payload = {
            "summary": summarize_scores(scored),
            "top_k": [
                {
                    "contract_id": str(row.get("contract_id") or ""),
                    "risk_score": int(row.get("risk_score") or 0),
                    "risk_level": str(row.get("risk_level") or "bajo"),
                    "summary": str(row.get("risk_summary") or ""),
                    "limitations": str(row.get("risk_limitations") or ""),
                    "flags": json.loads(row.get("risk_flags_json") or "[]"),
                    "dimension_scores": json.loads(row.get("risk_dimension_scores_json") or "{}"),
                }
                for _, row in top_rows.iterrows()
            ],
            "row_count": int(len(scored)),
        }
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))
        print(f"Scored table written to {args.output_table}")
        print(f"Report written to {output_path}")
        return

    if args.command == "materialize-index":
        store = DuckDBStore(Path(args.duckdb_path))
        summary = materialize_search_index(store, Path(args.processed_dir))
        print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))
        return

    if args.command == "validate-sources":
        store = DuckDBStore(Path(args.duckdb_path))
        service = ValidationService(store)
        summary = service.run(
            args.source_table,
            limit=args.limit,
            include_registry_sources=not args.no_registry_sources,
            deep=args.deep,
        )
        print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))
        return

    if args.command == "diagnose-process":
        store = DuckDBStore(Path(args.duckdb_path))
        report = write_process_diagnostic_report(
            store,
            processed_dir=Path(args.processed_dir),
            output_json=Path(args.output_json),
            output_csv=Path(args.output_csv),
            limit=args.limit,
            synthetic_count=args.synthetic_count,
        )
        print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
        print(f"Diagnostic report written to {args.output_json}")
        print(f"Diagnostic cases written to {args.output_csv}")
        return

    if args.command == "verify-sources":
        store = DuckDBStore(Path(args.duckdb_path))
        verifier = SourceCatalogVerifier(
            store,
            report_dir=Path(args.report_dir),
            manifest_path=Path(args.manifest_path),
        )
        report = verifier.run()
        print(json.dumps(report.summary.to_dict(), ensure_ascii=False, indent=2))
        print(f"Report written to {report.summary.report_path}")
        return


def _make_client(no_cache: bool, timeout: int, retries: int, app_token: str, domain: str) -> SocrataClient:
    cache = None if no_cache else JsonCache(CACHE_DIR / "socrata")
    return SocrataClient(
        domain=domain,
        app_token=app_token or None,
        timeout_seconds=timeout,
        max_retries=retries,
        cache=cache,
    )


def _load_optional_tables(store: DuckDBStore, table_names: list[str]) -> dict[str, object]:
    mapping: dict[str, object] = {}
    default_candidates = [
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
    names = list(dict.fromkeys([*table_names, *default_candidates])) if table_names else default_candidates
    for table_name in names:
        if not store.has_table(table_name):
            continue
        mapping[table_name] = store.read_frame(f"SELECT * FROM {table_name}")
    return mapping


if __name__ == "__main__":  # pragma: no cover
    main()
