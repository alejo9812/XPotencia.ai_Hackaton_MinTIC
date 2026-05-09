from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pae_risk_tracker.storage.duckdb_store import DuckDBStore
from pae_risk_tracker.validation.fetcher import ValidationFetchResult
from pae_risk_tracker.validation.registry import ValidationRegistry, ValidationSourceSpec
from pae_risk_tracker.validation.service import ValidationService


class StaticFetcher:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def fetch(self, url: str) -> ValidationFetchResult:
        self.calls.append(url)
        if "search" in url:
            payload = b"<html><head><title>External Search</title><meta name='description' content='Search result'></head><body>External Search</body></html>"
            return ValidationFetchResult(
                url=url,
                domain="example.com",
                status="fetched",
                robots_status="allowed",
                http_status=200,
                content_type="text/html",
                fetched_at="2026-05-09T00:00:00+00:00",
                byte_count=len(payload),
                content_hash="hash-external",
                title="External Search",
                description="Search result",
                text_excerpt="External Search",
                snapshot_suffix=".html",
                payload=payload,
            )
        if "oecd.org" in url:
            payload = b"<html><head><title>OECD Integrity</title><meta name='description' content='Integrity in public procurement'></head><body>OECD public procurement</body></html>"
            return ValidationFetchResult(
                url=url,
                domain="oecd.org",
                status="fetched",
                robots_status="allowed",
                http_status=200,
                content_type="text/html",
                fetched_at="2026-05-09T00:00:00+00:00",
                byte_count=len(payload),
                content_hash="hash-oecd",
                title="OECD Integrity",
                description="Integrity in public procurement",
                text_excerpt="OECD public procurement",
                snapshot_suffix=".html",
                payload=payload,
            )
        if "open-contracting.org" in url:
            payload = b"<html><head><title>OCP Red Flags</title><meta name='description' content='Red flags guide'></head><body>Open Contracting red flags</body></html>"
            return ValidationFetchResult(
                url=url,
                domain="open-contracting.org",
                status="fetched",
                robots_status="allowed",
                http_status=200,
                content_type="text/html",
                fetched_at="2026-05-09T00:00:00+00:00",
                byte_count=len(payload),
                content_hash="hash-ocp",
                title="OCP Red Flags",
                description="Red flags guide",
                text_excerpt="Open Contracting red flags",
                snapshot_suffix=".html",
                payload=payload,
            )
        if "worldbank.org" in url:
            payload = b"<html><head><title>World Bank Warning Signs</title><meta name='description' content='Warning signs of fraud and corruption'></head><body>World Bank warning signs</body></html>"
            return ValidationFetchResult(
                url=url,
                domain="worldbank.org",
                status="fetched",
                robots_status="allowed",
                http_status=200,
                content_type="text/html",
                fetched_at="2026-05-09T00:00:00+00:00",
                byte_count=len(payload),
                content_hash="hash-wb",
                title="World Bank Warning Signs",
                description="Warning signs of fraud and corruption",
                text_excerpt="World Bank warning signs",
                snapshot_suffix=".html",
                payload=payload,
            )
        payload = b"<html><head><title>SECOP Record</title><meta name='description' content='SECOP record'></head><body>SECOP Record</body></html>"
        return ValidationFetchResult(
            url=url,
            domain="example.com",
            status="fetched",
            robots_status="allowed",
            http_status=200,
            content_type="text/html",
            fetched_at="2026-05-09T00:00:00+00:00",
            byte_count=len(payload),
            content_hash="hash-secop",
            title="SECOP Record",
            description="SECOP record",
            text_excerpt="SECOP Record",
            snapshot_suffix=".html",
            payload=payload,
        )


def test_validation_service_runs_paco_then_secop_then_external(tmp_path):
    store = DuckDBStore(tmp_path / "tracker.duckdb")
    store.write_frame(
        "paco_events",
        pd.DataFrame(
            [
                {
                    "source_key": "disciplinary",
                    "source_name": "PACO disciplinary",
                    "source_url": "https://example.com/paco",
                    "family": "paco",
                    "event_type": "disciplinary_sanction",
                    "record_id": "PAC-1",
                    "subject_name": "Proveedor Uno",
                    "subject_doc": "900123456",
                    "entity_name": "Alcaldia de Ibague",
                    "entity_doc": "890101234",
                    "department": "Tolima",
                    "municipality": "Ibague",
                    "reference": "R-1",
                    "description": "Sancion disciplinaria",
                    "event_date": "2025-01-10",
                    "amount": 1000000,
                    "status": "vigente",
                    "search_text": "PROVEEDOR UNO ALCALDIA DE IBAGUE",
                    "source_row_hash": "hash-1",
                }
            ]
        ),
    )
    store.write_frame(
        "pae_search_index",
        pd.DataFrame(
            [
                {
                    "record_type": "contract",
                    "record_id": "C-1",
                    "contract_id": "C-1",
                    "process_id": "P-1",
                    "entity_name": "Alcaldia de Ibague",
                    "supplier_name": "Proveedor Uno",
                    "department": "Tolima",
                    "municipality": "Ibague",
                    "modality": "Contratacion Directa",
                    "status": "Adjudicado",
                    "amount": 150000000,
                    "date": "2025-01-05",
                    "risk_score": 75,
                    "risk_level": "alto",
                    "source_table": "pae_contracts_scored",
                    "url_process": "https://example.com/secop/contract/1",
                    "description": "PAE alimentacion escolar",
                    "justification": "",
                    "search_text": "PAE ALIMENTACION ESCOLAR",
                }
            ]
        ),
    )

    registry = ValidationRegistry(
        project="Test",
        allow_domains=("example.com",),
        default_timeout_seconds=1,
        default_user_agent="test-agent",
        sources=(
            ValidationSourceSpec(
                key="external_search",
                name="External search",
                kind="search",
                url="https://example.com/external",
                domain="example.com",
                enabled=True,
                search_url_template="https://example.com/search?q={query}",
            ),
        ),
    )
    fetcher = StaticFetcher()
    service = ValidationService(store, registry=registry, fetcher=fetcher, snapshot_dir=tmp_path / "snapshots", report_dir=tmp_path / "reports")

    summary = service.run("pae_search_index", limit=10, include_registry_sources=False, deep=True)

    assert summary.run_id.startswith("validation-")
    assert summary.paco_count == 1
    assert summary.secop_count == 1
    assert summary.external_count == 1
    assert summary.overall_status == "review_needed"
    assert Path(summary.report_path).exists()
    assert (tmp_path / "snapshots" / "secop" / "secop_public").exists()
    assert (tmp_path / "snapshots" / "external" / "external_search").exists()

    runs = store.read_frame("SELECT * FROM validation_runs")
    observations = store.read_frame("SELECT * FROM validation_observations ORDER BY stage")
    assert len(runs) == 1
    assert len(observations) == 3
    assert list(observations["stage"]) == ["external", "paco", "secop"] or list(observations["stage"]) == ["paco", "secop", "external"]
    assert "Proveedor Uno" in str(observations.loc[observations["stage"] == "paco", "evidence"].iloc[0])
    assert fetcher.calls[0] == "https://example.com/secop/contract/1"


def test_validation_service_includes_registry_web_sources(tmp_path):
    store = DuckDBStore(tmp_path / "tracker.duckdb")
    store.write_frame(
        "pae_search_index",
        pd.DataFrame(
            [
                {
                    "record_type": "contract",
                    "record_id": "C-1",
                    "contract_id": "C-1",
                    "process_id": "P-1",
                    "entity_name": "Alcaldia de Ibague",
                    "supplier_name": "Proveedor Uno",
                    "department": "Tolima",
                    "municipality": "Ibague",
                    "modality": "Contratacion Directa",
                    "status": "Adjudicado",
                    "amount": 150000000,
                    "date": "2025-01-05",
                    "risk_score": 75,
                    "risk_level": "alto",
                    "source_table": "pae_contracts_scored",
                    "url_process": "https://www.oecd.org/en/topics/sub-issues/integrity-in-public-procurement.html",
                    "description": "PAE alimentacion escolar",
                    "justification": "",
                    "search_text": "PAE ALIMENTACION ESCOLAR",
                }
            ]
        ),
    )
    registry = ValidationRegistry(
        project="Test",
        allow_domains=("oecd.org", "open-contracting.org", "worldbank.org"),
        default_timeout_seconds=1,
        default_user_agent="test-agent",
        sources=(
            ValidationSourceSpec(
                key="oecd_integrity",
                name="OECD integrity in public procurement",
                kind="study",
                url="https://www.oecd.org/en/topics/sub-issues/integrity-in-public-procurement.html",
                domain="oecd.org",
                enabled=True,
                search_url_template="https://www.oecd.org/en/topics/sub-issues/integrity-in-public-procurement.html?q={query}",
            ),
            ValidationSourceSpec(
                key="ocp_red_flags",
                name="OCP red flags guide",
                kind="guide",
                url="https://www.open-contracting.org/resources/red-flags-in-public-procurement-a-guide-to-using-data-to-detect-and-mitigate-risks/",
                domain="open-contracting.org",
                enabled=True,
                search_url_template="https://www.open-contracting.org/resources/red-flags-in-public-procurement-a-guide-to-using-data-to-detect-and-mitigate-risks/?q={query}",
            ),
        ),
    )
    fetcher = StaticFetcher()
    service = ValidationService(store, registry=registry, fetcher=fetcher, snapshot_dir=tmp_path / "snapshots", report_dir=tmp_path / "reports")

    summary = service.run("pae_search_index", limit=5, include_registry_sources=True, deep=True)

    assert summary.registry_source_count == 2
    assert summary.external_count >= 2
    assert summary.snapshot_count >= 2
    assert summary.overall_status == "review_needed"
    assert any("OECD Integrity" in call or "open-contracting.org" in call for call in fetcher.calls)
    report = json.loads(Path(summary.report_path).read_text(encoding="utf-8"))
    assert report["summary"]["registry_source_count"] == 2
