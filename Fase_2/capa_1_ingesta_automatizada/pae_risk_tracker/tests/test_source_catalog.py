from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from pae_risk_tracker.storage.duckdb_store import DuckDBStore
from pae_risk_tracker.validation.catalog import PACO_PAGE_URL, SourceCatalogVerifier


def test_source_catalog_verifier_matches_official_catalog_and_local_pack(tmp_path, monkeypatch):
    store = DuckDBStore(tmp_path / "tracker.duckdb")

    raw_path = tmp_path / "disciplinary.zip"
    raw_payload = b"raw paco payload"
    raw_path.write_bytes(raw_payload)
    processed_path = tmp_path / "paco_disciplinary.parquet"
    pd.DataFrame([{"a": 1}]).to_parquet(processed_path, index=False)

    manifest_path = tmp_path / "paco_pack_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "pack_name": "pae_data_pack",
                "sources": [
                    {
                        "key": "disciplinary",
                        "name": "PACO - Antecedentes SIRI sanciones",
                        "url": "https://paco7public7info7prod.blob.core.windows.net/paco-pulic-info/antecedentes_SIRI_sanciones_Cleaned.zip",
                        "raw_path": str(raw_path),
                        "processed_parquet": str(processed_path),
                        "rows_downloaded": 1,
                        "rows_normalized": 1,
                        "sha256": hashlib.sha256(raw_payload).hexdigest(),
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    dataset_registry = {
        "datasets": [
            {
                "key": "core_contracts",
                "id": "jbjy-vk9h",
                "name": "SECOP II - Contratos Electrónicos",
                "active": True,
            },
            {
                "key": "processes",
                "id": "p6dx-8zbt",
                "name": "SECOP II - Procesos de Contratación",
                "active": True,
            },
        ]
    }
    pack_registry = {
        "pack_name": "pae_data_pack",
        "sources": [
            {
                "key": "disciplinary",
                "name": "PACO - Antecedentes SIRI sanciones",
                "url": "https://paco7public7info7prod.blob.core.windows.net/paco-pulic-info/antecedentes_SIRI_sanciones_Cleaned.zip",
                "enabled": True,
            }
        ],
    }

    verifier = SourceCatalogVerifier(
        store,
        dataset_registry=dataset_registry,
        pack_registry=pack_registry,
        manifest_path=manifest_path,
        report_dir=tmp_path / "reports",
    )

    def fake_fetch(url: str) -> dict[str, object]:
        if url == "https://www.datos.gov.co/api/views/jbjy-vk9h.json":
            return {
                "ok": True,
                "http_status": 200,
                "content_type": "application/json",
                "payload": json.dumps(
                    {
                        "id": "jbjy-vk9h",
                        "name": "SECOP II - Contratos Electrónicos",
                        "permalink": "https://www.datos.gov.co/Gastos-Gubernamentales/SECOP-II-Contratos-Electr-nicos/jbjy-vk9h",
                    }
                ).encode("utf-8"),
                "content_hash": "hash-1",
                "error": "",
            }
        if url == "https://www.datos.gov.co/api/views/p6dx-8zbt.json":
            return {
                "ok": True,
                "http_status": 200,
                "content_type": "application/json",
                "payload": json.dumps(
                    {
                        "id": "p6dx-8zbt",
                        "name": "SECOP II - Procesos de Contratación",
                        "permalink": "https://www.datos.gov.co/Gastos-Gubernamentales/SECOP-II-Procesos-de-Contrataci-n/p6dx-8zbt",
                    }
                ).encode("utf-8"),
                "content_hash": "hash-2",
                "error": "",
            }
        if url == PACO_PAGE_URL:
            return {
                "ok": True,
                "http_status": 200,
                "content_type": "text/html",
                "payload": (
                    "<html><body>"
                    "<a href='https://paco7public7info7prod.blob.core.windows.net/paco-pulic-info/antecedentes_SIRI_sanciones_Cleaned.zip'>descarga</a>"
                    "</body></html>"
                ).encode("utf-8"),
                "content_hash": "hash-3",
                "error": "",
            }
        raise AssertionError(f"Unexpected URL fetched: {url}")

    monkeypatch.setattr(verifier, "_fetch", fake_fetch)

    report = verifier.run()

    assert report.summary.secop_source_count == 2
    assert report.summary.paco_source_count == 1
    assert report.summary.local_pack_source_count == 1
    assert report.summary.matched_count >= 4
    assert report.summary.review_count == 0
    assert report.summary.missing_count == 0
    assert report.summary.error_count == 0
    assert Path(report.summary.report_path).exists()

    assert store.has_table("source_catalog_runs")
    assert store.has_table("source_catalog_checks")

    checks = store.read_frame("SELECT * FROM source_catalog_checks ORDER BY family, source_key")
    assert set(checks["status"]) == {"matched"}
