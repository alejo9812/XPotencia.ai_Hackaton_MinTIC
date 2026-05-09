from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from pae_risk_tracker.connectors.socrata_client import SocrataClient
from pae_risk_tracker.ingestion.incremental_loader import PAEIncrementalLoader
from pae_risk_tracker.storage.duckdb_store import DuckDBStore


@dataclass
class DummyResult:
    rows: list[dict]
    from_cache: bool = False


class FakeClient(SocrataClient):
    def __init__(self):
        pass

    def get_metadata(self, dataset_id):
        return {
            "columns": [
                {"fieldName": "id_contrato"},
                {"fieldName": "proceso_de_compra"},
                {"fieldName": "nombre_entidad"},
                {"fieldName": "nit_entidad"},
                {"fieldName": "proveedor_adjudicado"},
                {"fieldName": "documento_proveedor"},
                {"fieldName": "descripcion_del_proceso"},
                {"fieldName": "modalidad_de_contratacion"},
                {"fieldName": "estado_contrato"},
                {"fieldName": "valor"},
                {"fieldName": "fecha_de_firma"},
                {"fieldName": "departamento"},
                {"fieldName": "ciudad"},
                {"fieldName": "urlproceso"},
            ]
        }

    def iter_rows(self, dataset_id, select=None, where=None, order=None, page_size=1000, max_rows=None):
        yield DummyResult(
            rows=[
                {
                    "id_contrato": "C-1",
                    "proceso_de_compra": "P-1",
                    "nombre_entidad": "Alcaldia",
                    "nit_entidad": "123",
                    "proveedor_adjudicado": "Proveedor Uno",
                    "documento_proveedor": "456",
                    "descripcion_del_proceso": "PAE alimentacion escolar",
                    "modalidad_de_contratacion": "Contratacion Directa",
                    "estado_contrato": "Adjudicado",
                    "valor": "1000",
                    "fecha_de_firma": "2025-01-01",
                    "departamento": "Tolima",
                    "ciudad": "Ibague",
                    "urlproceso": "https://example.com",
                },
                {
                    "id_contrato": "C-1",
                    "proceso_de_compra": "P-1",
                    "nombre_entidad": "Alcaldia",
                    "nit_entidad": "123",
                    "proveedor_adjudicado": "Proveedor Uno",
                    "documento_proveedor": "456",
                    "descripcion_del_proceso": "PAE alimentacion escolar",
                    "modalidad_de_contratacion": "Contratacion Directa",
                    "estado_contrato": "Adjudicado",
                    "valor": "1000",
                    "fecha_de_firma": "2025-01-01",
                    "departamento": "Tolima",
                    "ciudad": "Ibague",
                    "urlproceso": "https://example.com",
                },
            ],
            from_cache=True,
        )


class FakeAdditionsClient(SocrataClient):
    def __init__(self):
        self.rows = [
            {
                "identificador": "ADD-1",
                "id_contrato": "C-1",
                "tipo": "Adicion",
                "descripcion": "Adicion de valor",
                "fecharegistro": "2025-01-05",
            },
            {
                "identificador": "ADD-2",
                "id_contrato": "C-1",
                "tipo": "Prorroga",
                "descripcion": "Prorroga del plazo",
                "fecharegistro": "2025-02-10",
            },
            {
                "identificador": "ADD-3",
                "id_contrato": "C-2",
                "tipo": "Modificacion",
                "descripcion": "Otrosi contractual",
                "fecharegistro": "2025-03-01",
            },
        ]

    def get_metadata(self, dataset_id):
        return {
            "columns": [
                {"fieldName": "identificador"},
                {"fieldName": "id_contrato"},
                {"fieldName": "tipo"},
                {"fieldName": "descripcion"},
                {"fieldName": "fecharegistro"},
            ]
        }

    def iter_rows(self, dataset_id, select=None, where=None, order=None, page_size=1000, max_rows=None):
        selected_ids = _extract_ids(where or "")
        rows = [row for row in self.rows if row["id_contrato"] in selected_ids]
        yield DummyResult(rows=rows, from_cache=False)


def test_loader_builds_sample(tmp_path):
    store = DuckDBStore(tmp_path / "tracker.duckdb")
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    loader = PAEIncrementalLoader(FakeClient(), store, raw_dir=raw_dir, processed_dir=processed_dir)
    summary = loader.load_sample([2025], sample_limit=10, per_year_limit=5)

    assert summary.rows_fetched == 2
    assert summary.rows_kept == 1
    assert store.count("pae_contracts_core") == 1
    manifest = json.loads((raw_dir / "pae_contracts_sample.manifest.json").read_text(encoding="utf-8"))
    assert manifest["rows_kept"] == 1
    assert summary.dataset_id == "jbjy-vk9h"


def test_loader_builds_additions_sample(tmp_path):
    core_path = tmp_path / "pae_contracts_core.parquet"
    pd.DataFrame(
        [
            {"contract_id": "C-1", "process_id": "P-1", "amount": 1000},
            {"contract_id": "C-2", "process_id": "P-2", "amount": 2000},
        ]
    ).to_parquet(core_path, index=False)

    store = DuckDBStore(tmp_path / "tracker.duckdb")
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    loader = PAEIncrementalLoader(FakeAdditionsClient(), store, raw_dir=raw_dir, processed_dir=processed_dir)
    summary = loader.load_additions(core_parquet=core_path, batch_size=2, sample_limit=10)

    assert summary.dataset_id == "cb9c-h8sn"
    assert summary.rows_kept == 3
    assert store.count("pae_additions") == 3
    manifest = json.loads((raw_dir / "pae_additions_sample.manifest.json").read_text(encoding="utf-8"))
    assert manifest["contract_count"] == 2
    assert manifest["rows_kept"] == 3


def _extract_ids(where: str) -> set[str]:
    return {part.strip("'") for part in where.split("IN (")[-1].rstrip(")").split(",") if part.strip()}
