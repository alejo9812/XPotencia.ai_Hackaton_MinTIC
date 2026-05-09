from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote_plus

import pandas as pd

from ..config import normalize_digits, normalize_text
from ..paths import VALIDATION_REPORT_DIR, VALIDATION_SNAPSHOT_DIR, ensure_runtime_dirs
from ..storage.duckdb_store import DuckDBStore
from .fetcher import ValidationFetchResult, ValidationFetcher
from .registry import ValidationRegistry, ValidationSourceSpec, load_validation_registry_spec


VALIDATION_RUNS_TABLE = "validation_runs"
VALIDATION_OBSERVATIONS_TABLE = "validation_observations"
VALIDATION_REGISTRY_TABLE = "validation_registry"


@dataclass(frozen=True)
class ValidationObservation:
    run_id: str
    stage: str
    status: str
    source_key: str
    source_name: str
    source_kind: str
    scope: str
    url: str
    domain: str
    record_type: str
    record_id: str
    contract_id: str
    process_id: str
    entity_name: str
    supplier_name: str
    department: str
    municipality: str
    evidence: str
    confidence: int
    http_status: int | None
    content_type: str
    robots_status: str
    content_hash: str
    byte_count: int
    title: str
    description: str
    text_excerpt: str
    snapshot_path: str
    error_message: str
    inspected_at: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["http_status"] = self.http_status
        return payload


@dataclass(frozen=True)
class ValidationRunSummary:
    run_id: str
    created_at: str
    source_table: str
    candidate_count: int
    paco_count: int
    secop_count: int
    external_count: int
    hit_count: int
    clear_count: int
    blocked_count: int
    error_count: int
    observation_count: int
    registry_source_count: int
    snapshot_count: int
    overall_status: str
    report_path: str
    snapshot_dir: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ValidationService:
    def __init__(
        self,
        store: DuckDBStore,
        registry: ValidationRegistry | None = None,
        fetcher: ValidationFetcher | None = None,
        *,
        snapshot_dir: Path | None = None,
        report_dir: Path | None = None,
    ) -> None:
        ensure_runtime_dirs()
        self.store = store
        self.registry = registry or load_validation_registry_spec()
        self.fetcher = fetcher or ValidationFetcher(
            self.registry.allowed_domain_set(),
            timeout_seconds=self.registry.default_timeout_seconds,
            user_agent=self.registry.default_user_agent,
        )
        self.snapshot_dir = Path(snapshot_dir or VALIDATION_SNAPSHOT_DIR)
        self.report_dir = Path(report_dir or VALIDATION_REPORT_DIR)
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self._paco_frame: pd.DataFrame | None = None
        self._run_id: str | None = None

    def run(
        self,
        source_table: str = "pae_search_index",
        *,
        limit: int = 25,
        include_registry_sources: bool = True,
        deep: bool = False,
    ) -> ValidationRunSummary:
        self._run_id = self._new_run_id()
        try:
            candidate_frame = self._load_candidate_frame(source_table, limit)
            observations: list[ValidationObservation] = []

            for row in candidate_frame.to_dict(orient="records"):
                observations.extend(self._validate_record(row, deep=deep))

            if include_registry_sources:
                observations.extend(self._validate_registry_sources(candidate_frame, deep=deep))

            summary = self._build_summary(self._require_run_id(), source_table, candidate_frame, observations)
            self._persist_run(summary, observations)
            return summary
        finally:
            self._run_id = None

    def _validate_record(self, row: dict[str, Any], *, deep: bool) -> list[ValidationObservation]:
        record_context = self._record_context(row)
        paco_result, paco_matches = self._profile_paco(record_context)
        observations: list[ValidationObservation] = [
            self._observation_from_paco(record_context, paco_result, paco_matches)
        ]

        secop_result = self._review_secop(record_context, paco_result)
        observations.append(secop_result)

        if deep or self._should_run_external(paco_result, secop_result):
            observations.extend(self._review_external(record_context, paco_result, secop_result))

        return observations

    def _validate_registry_sources(self, candidate_frame: pd.DataFrame, *, deep: bool) -> list[ValidationObservation]:
        observations: list[ValidationObservation] = []
        for source in self.registry.active_sources():
            fetch_result = self.fetcher.fetch(source.url)
            observations.append(
                self._observation_from_fetch(
                    run_id=self._require_run_id(),
                    stage="external",
                    source=source,
                    fetch_result=fetch_result,
                    scope="registry",
                    record_context={},
                    evidence_prefix="Registry source",
                )
            )
        return observations

    def _profile_paco(self, record: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        frame = self._load_paco_frame()
        if frame.empty:
            return {"status": "clear", "confidence": 0, "evidence": "No PACO tables loaded."}, []

        supplier_doc = normalize_digits(record.get("supplier_doc") or "")
        entity_doc = normalize_digits(record.get("entity_doc") or "")
        supplier_name = normalize_text(record.get("supplier_name") or "")
        entity_name = normalize_text(record.get("entity_name") or "")
        contract_id = normalize_text(record.get("contract_id") or record.get("record_id") or "")
        process_id = normalize_text(record.get("process_id") or "")
        search_terms = normalize_text(" ".join(value for value in [supplier_name, entity_name, contract_id, process_id] if value))

        matches: list[dict[str, Any]] = []
        for paco_row in frame.to_dict(orient="records"):
            score = 0
            evidence_bits: list[str] = []
            subject_doc = normalize_digits(paco_row.get("subject_doc") or "")
            entity_doc_match = normalize_digits(paco_row.get("entity_doc") or "")
            subject_name = normalize_text(paco_row.get("subject_name") or "")
            paco_entity_name = normalize_text(paco_row.get("entity_name") or "")
            paco_search = normalize_text(paco_row.get("search_text") or "")
            if supplier_doc and supplier_doc == subject_doc:
                score += 60
                evidence_bits.append("supplier_doc exact match")
            if entity_doc and entity_doc == entity_doc_match:
                score += 55
                evidence_bits.append("entity_doc exact match")
            if supplier_name and supplier_name == subject_name:
                score += 25
                evidence_bits.append("supplier_name exact match")
            if entity_name and entity_name == paco_entity_name:
                score += 20
                evidence_bits.append("entity_name exact match")
            if supplier_name and supplier_name in subject_name and supplier_name:
                score += 15
                evidence_bits.append("supplier_name contained in PACO subject")
            if entity_name and entity_name in paco_entity_name and entity_name:
                score += 12
                evidence_bits.append("entity_name contained in PACO entity")
            if search_terms and any(token and token in paco_search for token in search_terms.split()[:8]):
                score += 8
                evidence_bits.append("search text overlap")
            if contract_id and contract_id in normalize_text(paco_row.get("reference") or ""):
                score += 10
                evidence_bits.append("contract reference overlap")
            if process_id and process_id in normalize_text(paco_row.get("reference") or ""):
                score += 8
                evidence_bits.append("process reference overlap")
            if score <= 0:
                continue
            matches.append(
                {
                    "score": score,
                    "evidence": " | ".join(evidence_bits),
                    "source_key": str(paco_row.get("source_key") or ""),
                    "source_name": str(paco_row.get("source_name") or ""),
                    "event_type": str(paco_row.get("event_type") or ""),
                    "reference": str(paco_row.get("reference") or ""),
                    "status": str(paco_row.get("status") or ""),
                    "description": str(paco_row.get("description") or ""),
                    "event_date": str(paco_row.get("event_date") or ""),
                    "subject_name": str(paco_row.get("subject_name") or ""),
                    "entity_name": str(paco_row.get("entity_name") or ""),
                }
            )

        matches.sort(key=lambda item: item["score"], reverse=True)
        top_matches = matches[:3]
        max_score = top_matches[0]["score"] if top_matches else 0
        if max_score >= 55:
            status = "hit"
        elif max_score > 0:
            status = "possible_match"
        else:
            status = "clear"
        evidence = "; ".join(
            f'{match["source_name"]} ({match["event_type"]}): {match["subject_name"]} / {match["entity_name"]} | {match["evidence"]} [{match["status"]}]'
            for match in top_matches
        )
        confidence = min(100, max_score)
        return {"status": status, "confidence": confidence, "evidence": evidence or "No PACO evidence found."}, top_matches

    def _review_secop(self, record: dict[str, Any], paco_result: dict[str, Any]) -> ValidationObservation:
        candidate_url = str(record.get("url_process") or record.get("url") or "").strip()
        source = ValidationSourceSpec(
            key="secop_public",
            name="SECOP public record",
            kind="public_record",
            url=candidate_url,
            domain=self._domain_from_url(candidate_url),
            enabled=True,
        )
        if not candidate_url:
            return ValidationObservation(
                run_id=self._require_run_id(),
                stage="secop",
                status="no_url",
                source_key=source.key,
                source_name=source.name,
                source_kind=source.kind,
                scope="record",
                url="",
                domain="",
                record_type=record.get("record_type", ""),
                record_id=record.get("record_id", ""),
                contract_id=record.get("contract_id", ""),
                process_id=record.get("process_id", ""),
                entity_name=record.get("entity_name", ""),
                supplier_name=record.get("supplier_name", ""),
                department=record.get("department", ""),
                municipality=record.get("municipality", ""),
                evidence="No SECOP URL available in the local record.",
                confidence=0,
                http_status=None,
                content_type="",
                robots_status="unknown",
                content_hash="",
                byte_count=0,
                title="",
                description="",
                text_excerpt="",
                snapshot_path="",
                error_message="",
                inspected_at=self._now(),
            )

        fetch_result = self.fetcher.fetch(candidate_url)
        status = "validated" if fetch_result.status == "fetched" and fetch_result.byte_count > 0 else fetch_result.status
        evidence = fetch_result.description or fetch_result.title or fetch_result.text_excerpt or "SECOP URL inspected."
        confidence = 45 if status == "validated" else 10 if status == "fetched" else 0
        snapshot_path = self._write_snapshot_file("secop", source.key, fetch_result)
        if paco_result.get("status") == "hit" and confidence < 30:
            confidence = 30
        return ValidationObservation(
            run_id=self._require_run_id(),
            stage="secop",
            status=status,
            source_key=source.key,
            source_name=source.name,
            source_kind=source.kind,
            scope="record",
            url=candidate_url,
            domain=source.domain,
            record_type=record.get("record_type", ""),
            record_id=record.get("record_id", ""),
            contract_id=record.get("contract_id", ""),
            process_id=record.get("process_id", ""),
            entity_name=record.get("entity_name", ""),
            supplier_name=record.get("supplier_name", ""),
            department=record.get("department", ""),
            municipality=record.get("municipality", ""),
            evidence=evidence,
            confidence=confidence,
            http_status=fetch_result.http_status,
            content_type=fetch_result.content_type,
            robots_status=fetch_result.robots_status,
            content_hash=fetch_result.content_hash,
            byte_count=fetch_result.byte_count,
            title=fetch_result.title,
            description=fetch_result.description,
            text_excerpt=fetch_result.text_excerpt,
            snapshot_path=snapshot_path,
            error_message=fetch_result.error_message,
            inspected_at=fetch_result.fetched_at,
        )

    def _observation_from_paco(
        self,
        record: dict[str, Any],
        paco_result: dict[str, Any],
        matches: list[dict[str, Any]],
    ) -> ValidationObservation:
        status = str(paco_result.get("status") or "clear")
        confidence = int(paco_result.get("confidence") or 0)
        evidence = str(paco_result.get("evidence") or "")
        if matches:
            best = matches[0]
            source = ValidationSourceSpec(
                key=str(best.get("source_key") or "paco_events"),
                name=str(best.get("source_name") or "PACO"),
                kind="local_paco",
                url="",
                domain="",
                enabled=True,
            )
            title = best.get("description") or best.get("reference") or best.get("subject_name") or ""
            snapshot_path = ""
        else:
            source = ValidationSourceSpec(
                key="paco_events",
                name="PACO profile",
                kind="local_paco",
                url="",
                domain="",
                enabled=True,
            )
            title = ""
            snapshot_path = ""
        return ValidationObservation(
            run_id=self._require_run_id(),
            stage="paco",
            status=status,
            source_key=source.key,
            source_name=source.name,
            source_kind=source.kind,
            scope="local",
            url="",
            domain="",
            record_type=record.get("record_type", ""),
            record_id=record.get("record_id", ""),
            contract_id=record.get("contract_id", ""),
            process_id=record.get("process_id", ""),
            entity_name=record.get("entity_name", ""),
            supplier_name=record.get("supplier_name", ""),
            department=record.get("department", ""),
            municipality=record.get("municipality", ""),
            evidence=evidence or "No PACO evidence found.",
            confidence=confidence,
            http_status=None,
            content_type="",
            robots_status="unknown",
            content_hash="",
            byte_count=0,
            title=str(title or ""),
            description="",
            text_excerpt="",
            snapshot_path=snapshot_path,
            error_message="",
            inspected_at=self._now(),
        )

    def _observation_from_fetch(
        self,
        *,
        run_id: str,
        stage: str,
        source: ValidationSourceSpec,
        fetch_result: ValidationFetchResult,
        scope: str,
        record_context: dict[str, Any],
        evidence_prefix: str,
        override_status: str | None = None,
        override_confidence: int | None = None,
    ) -> ValidationObservation:
        snapshot_path = self._write_snapshot_file(stage, source.key, fetch_result)
        status = override_status or fetch_result.status
        confidence = override_confidence if override_confidence is not None else (40 if fetch_result.status == "fetched" else 0)
        evidence = fetch_result.description or fetch_result.title or fetch_result.text_excerpt or evidence_prefix
        return ValidationObservation(
            run_id=run_id,
            stage=stage,
            status=status,
            source_key=source.key,
            source_name=source.name,
            source_kind=source.kind,
            scope=scope,
            url=fetch_result.url,
            domain=fetch_result.domain,
            record_type=str(record_context.get("record_type", "")),
            record_id=str(record_context.get("record_id", "")),
            contract_id=str(record_context.get("contract_id", "")),
            process_id=str(record_context.get("process_id", "")),
            entity_name=str(record_context.get("entity_name", "")),
            supplier_name=str(record_context.get("supplier_name", "")),
            department=str(record_context.get("department", "")),
            municipality=str(record_context.get("municipality", "")),
            evidence=evidence,
            confidence=confidence,
            http_status=fetch_result.http_status,
            content_type=fetch_result.content_type,
            robots_status=fetch_result.robots_status,
            content_hash=fetch_result.content_hash,
            byte_count=fetch_result.byte_count,
            title=fetch_result.title,
            description=fetch_result.description,
            text_excerpt=fetch_result.text_excerpt,
            snapshot_path=snapshot_path,
            error_message=fetch_result.error_message,
            inspected_at=fetch_result.fetched_at,
        )

    def _load_candidate_frame(self, source_table: str, limit: int) -> pd.DataFrame:
        if not self.store.has_table(source_table):
            return pd.DataFrame()
        sql = f"SELECT * FROM {source_table} LIMIT ?"
        frame = self.store.query_frame(sql, [limit])
        if frame.empty:
            return frame
        return frame

    def _load_paco_frame(self) -> pd.DataFrame:
        if self._paco_frame is not None:
            return self._paco_frame
        frames: list[pd.DataFrame] = []
        candidate_tables = [
            "paco_events",
            "paco_disciplinary",
            "paco_penal",
            "paco_fiscal",
            "paco_contractual",
            "paco_collusion",
        ]
        for table_name in candidate_tables:
            if not self.store.has_table(table_name):
                continue
            frame = self.store.read_frame(f"SELECT * FROM {table_name}")
            if not frame.empty:
                frames.append(frame)
        if not frames:
            self._paco_frame = pd.DataFrame()
            return self._paco_frame
        combined = pd.concat(frames, ignore_index=True, sort=False)
        if "source_key" in combined.columns and "source_row_hash" in combined.columns:
            combined = combined.drop_duplicates(subset=["source_key", "source_row_hash"], keep="first")
        self._paco_frame = combined.reset_index(drop=True)
        return self._paco_frame

    def _record_context(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "record_type": self._string(row, "record_type"),
            "record_id": self._string(row, "record_id", "contract_id", "process_id"),
            "contract_id": self._string(row, "contract_id", "id_contrato", "referencia_del_contrato"),
            "process_id": self._string(row, "process_id", "proceso_de_compra", "id_del_proceso"),
            "entity_name": self._string(row, "entity_name", "nombre_entidad", "entidad"),
            "supplier_name": self._string(row, "supplier_name", "proveedor_adjudicado", "proveedor"),
            "supplier_doc": self._digits(row, "supplier_doc", "documento_proveedor", "nit_proveedor"),
            "entity_doc": self._digits(row, "entity_doc", "nit_entidad", "codigo_entidad"),
            "department": self._string(row, "department", "departamento"),
            "municipality": self._string(row, "municipality", "ciudad"),
            "url_process": self._string(row, "url_process", "url"),
        }

    def _review_external_source(
        self,
        source: ValidationSourceSpec,
        record: dict[str, Any],
        search_query: str,
    ) -> ValidationObservation:
        candidate_url = source.url
        if source.search_url_template and search_query:
            candidate_url = source.search_url_template.format(query=quote_plus(search_query), terms=quote_plus(search_query))
        fetch_result = self.fetcher.fetch(candidate_url)
        return self._observation_from_fetch(
            run_id=self._require_run_id(),
            stage="external",
            source=source,
            fetch_result=fetch_result,
            scope="registry",
            record_context=record,
            evidence_prefix=f"External source query: {search_query or 'registry monitoring'}",
            override_status="observed" if fetch_result.status == "fetched" else fetch_result.status,
            override_confidence=20 if fetch_result.status == "fetched" else 0,
        )

    def _review_external(
        self,
        record: dict[str, Any],
        paco_result: dict[str, Any],
        secop_result: ValidationObservation,
    ) -> list[ValidationObservation]:
        observations: list[ValidationObservation] = []
        search_query = self._build_external_query(record)
        for source in self.registry.active_sources():
            if not source.search_url_template:
                continue
            observations.append(self._review_external_source(source, record, search_query))
        return observations

    def _should_run_external(self, paco_result: dict[str, Any], secop_result: ValidationObservation) -> bool:
        if secop_result.status in {"inconclusive", "blocked_robots", "blocked_domain", "error", "no_url"}:
            return True
        return paco_result.get("status") in {"clear", "possible_match"}

    def _build_external_query(self, record: dict[str, Any]) -> str:
        terms = [
            record.get("entity_name", ""),
            record.get("supplier_name", ""),
            record.get("contract_id", ""),
            record.get("process_id", ""),
            record.get("department", ""),
        ]
        cleaned = [normalize_text(term) for term in terms if str(term).strip()]
        return " ".join(term for term in cleaned if term).strip()

    def _observation_record_to_frame(self, observations: list[ValidationObservation]) -> pd.DataFrame:
        if not observations:
            return pd.DataFrame(
                columns=[
                    "run_id",
                    "stage",
                    "status",
                    "source_key",
                    "source_name",
                    "source_kind",
                    "scope",
                    "url",
                    "domain",
                    "record_type",
                    "record_id",
                    "contract_id",
                    "process_id",
                    "entity_name",
                    "supplier_name",
                    "department",
                    "municipality",
                    "evidence",
                    "confidence",
                    "http_status",
                    "content_type",
                    "robots_status",
                    "content_hash",
                    "byte_count",
                    "title",
                    "description",
                    "text_excerpt",
                    "snapshot_path",
                    "error_message",
                    "inspected_at",
                ]
            )
        return pd.DataFrame([observation.to_dict() for observation in observations])

    def _build_summary(self, run_id: str, source_table: str, candidate_frame: pd.DataFrame, observations: list[ValidationObservation]) -> ValidationRunSummary:
        paco_count = sum(1 for obs in observations if obs.stage == "paco")
        secop_count = sum(1 for obs in observations if obs.stage == "secop")
        external_count = sum(1 for obs in observations if obs.stage == "external")
        hit_count = sum(1 for obs in observations if obs.status in {"hit", "validated"})
        clear_count = sum(1 for obs in observations if obs.status in {"clear", "observed"})
        blocked_count = sum(1 for obs in observations if obs.status in {"blocked_domain", "blocked_robots", "blocked"})
        error_count = sum(1 for obs in observations if obs.status == "error")
        overall_status = "review_needed" if hit_count else ("manual_review" if blocked_count or error_count else "clear")
        report_path = self.report_dir / f"{run_id}.json"
        snapshot_count = sum(1 for obs in observations if obs.snapshot_path)
        return ValidationRunSummary(
            run_id=run_id,
            created_at=self._now(),
            source_table=source_table,
            candidate_count=int(len(candidate_frame)),
            paco_count=int(paco_count),
            secop_count=int(secop_count),
            external_count=int(external_count),
            hit_count=int(hit_count),
            clear_count=int(clear_count),
            blocked_count=int(blocked_count),
            error_count=int(error_count),
            observation_count=int(len(observations)),
            registry_source_count=int(len(self.registry.active_sources())),
            snapshot_count=int(snapshot_count),
            overall_status=overall_status,
            report_path=str(report_path),
            snapshot_dir=str(self.snapshot_dir),
        )

    def _persist_run(self, summary: ValidationRunSummary, observations: list[ValidationObservation]) -> None:
        registry_frame = pd.DataFrame([source.to_dict() for source in self.registry.sources])
        if not registry_frame.empty:
            self.store.write_frame(VALIDATION_REGISTRY_TABLE, registry_frame, replace=True)

        summary_frame = pd.DataFrame([summary.to_dict()])
        if self.store.has_table(VALIDATION_RUNS_TABLE):
            self.store.append_frame(VALIDATION_RUNS_TABLE, summary_frame)
        else:
            self.store.write_frame(VALIDATION_RUNS_TABLE, summary_frame, replace=True)

        observation_frame = self._observation_record_to_frame(observations)
        if not observation_frame.empty:
            if self.store.has_table(VALIDATION_OBSERVATIONS_TABLE):
                self.store.append_frame(VALIDATION_OBSERVATIONS_TABLE, observation_frame)
            else:
                self.store.write_frame(VALIDATION_OBSERVATIONS_TABLE, observation_frame, replace=True)

        report_payload = {
            "summary": summary.to_dict(),
            "observations": [observation.to_dict() for observation in observations],
        }
        summary_path = Path(summary.report_path)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(report_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _write_snapshot_file(self, stage: str, source_key: str, fetch_result: ValidationFetchResult) -> str:
        if not fetch_result.payload:
            return ""
        stage_dir = self.snapshot_dir / stage / source_key
        stage_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{fetch_result.content_hash or self._hash_url(fetch_result.url)}{fetch_result.snapshot_suffix}"
        path = stage_dir / filename
        if not path.exists():
            path.write_bytes(fetch_result.payload)
        return str(path)

    def _hash_url(self, url: str) -> str:
        import hashlib

        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    def _require_run_id(self) -> str:
        if not self._run_id:
            self._run_id = self._new_run_id()
        return self._run_id

    def _new_run_id(self) -> str:
        return datetime.now(timezone.utc).strftime("validation-%Y%m%d-%H%M%S-%f")

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _domain_from_url(self, url: str) -> str:
        from urllib.parse import urlparse

        parsed = urlparse(url)
        return (parsed.hostname or "").lower().strip(".")

    def _string(self, row: dict[str, Any], *keys: str) -> str:
        for key in keys:
            if key not in row:
                continue
            value = row.get(key)
            if value is None:
                continue
            try:
                if pd.isna(value):
                    continue
            except Exception:
                pass
            text = str(value).strip()
            if text:
                return text
        return ""

    def _digits(self, row: dict[str, Any], *keys: str) -> str:
        for key in keys:
            if key not in row:
                continue
            value = row.get(key)
            if value is None:
                continue
            digits = normalize_digits(value)
            if digits:
                return digits
        return ""
