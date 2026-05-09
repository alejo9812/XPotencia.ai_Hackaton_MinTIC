from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from typing import Any

from ..paths import PROCESSED_DIR
from ..risk.opacity_criteria import build_criteria_knowledge_rows, build_opacity_criteria_report
from ..storage.duckdb_store import DuckDBStore


@dataclass(frozen=True)
class KnowledgeSearchResult:
    source_table: str
    total_rows: int
    returned_rows: int
    rows: list[dict[str, Any]]
    evidence_rows: list[dict[str, Any]]
    validation: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def search_criteria_knowledge(
    store: DuckDBStore,
    query: str,
    limit: int,
    *,
    processed_dir: Any | None = None,
) -> KnowledgeSearchResult:
    processed_dir = processed_dir or PROCESSED_DIR
    with ThreadPoolExecutor(max_workers=2) as executor:
        report_future = executor.submit(build_opacity_criteria_report, store, processed_dir=processed_dir)
        latest_future = executor.submit(_latest_validation_run, store)
        report = report_future.result()
        latest_run = latest_future.result()

    rows = build_criteria_knowledge_rows(report, query=query)
    selected = rows[: max(1, int(limit))]
    validation = {
        "latest_run": latest_run,
        "observations": [],
        "criteria": report.to_dict(),
        "search_mode": "criteria",
    }
    return KnowledgeSearchResult(
        source_table="opacity_criteria_knowledge",
        total_rows=int(len(rows)),
        returned_rows=int(len(selected)),
        rows=selected,
        evidence_rows=selected,
        validation=validation,
    )


def _latest_validation_run(store: DuckDBStore) -> dict[str, Any]:
    if not store.has_table("validation_runs"):
        return {}
    frame = store.query_frame("SELECT * FROM validation_runs ORDER BY created_at DESC LIMIT 1")
    if frame.empty:
        return {}
    return frame.iloc[0].to_dict()

