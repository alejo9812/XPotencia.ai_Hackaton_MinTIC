from __future__ import annotations

import pandas as pd
from fastapi.testclient import TestClient

from pae_risk_tracker.api.server import create_app
from pae_risk_tracker.retrieval.search_index import (
    SEARCH_INDEX_TABLE,
    ensure_search_index,
    materialize_search_index,
    search_index_count_sql,
    search_index_sql,
)
from pae_risk_tracker.storage.duckdb_store import DuckDBStore


def test_materialize_search_index_and_search_api(tmp_path):
    store = DuckDBStore(tmp_path / "tracker.duckdb")
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    contracts = pd.DataFrame(
        [
            {
                "contract_id": "C-1",
                "process_id": "P-1",
                "entity_name": "Alcaldia de Ibague",
                "supplier_name": "Proveedor Uno",
                "department": "Tolima",
                "municipality": "Ibague",
                "modality": "Contratacion Directa",
                "status": "Adjudicado",
                "amount": 150_000_000,
                "date": "2025-01-05",
                "risk_score": 75,
                "risk_level": "alto",
                "object_text": "PAE alimentacion escolar",
                "justification": "",
                "url_process": "https://example.com/process",
            }
        ]
    )
    processes = pd.DataFrame(
        [
            {
                "id_del_proceso": "P-1",
                "id_del_portafolio": "PF-1",
                "entidad": "Alcaldia de Ibague",
                "modalidad_de_contratacion": "Licitacion publica",
                "fecha_de_publicacion_del": "2025-01-01T00:00:00.000",
                "proveedores_unicos_con": 3,
                "respuestas_al_procedimiento": 3,
                "precio_base": 200_000_000,
                "valor_total_adjudicacion": 180_000_000,
                "nombre_del_procedimiento": "Programa de Alimentacion Escolar PAE",
                "urlproceso": {"url": "https://example.com/process"},
            }
        ]
    )
    additions = pd.DataFrame(
        [
            {
                "addition_id": "A-1",
                "contract_id": "C-1",
                "addition_type": "ADICION",
                "addition_description": "Adicion de valor",
                "addition_date": "2025-02-01",
            }
        ]
    )

    store.write_frame("pae_contracts_scored", contracts, replace=True)
    store.write_frame("pae_processes", processes, replace=True)
    store.write_frame("pae_additions", additions, replace=True)

    summary = materialize_search_index(store, processed_dir)
    assert summary.table_name == SEARCH_INDEX_TABLE
    assert summary.row_count == 3
    assert summary.record_type_counts["contract"] == 1
    assert summary.record_type_counts["process"] == 1
    assert summary.record_type_counts["addition"] == 1

    sql, params = search_index_sql(query="PAE", limit=10)
    result = store.query_frame(sql, params)
    assert len(result) == 3

    app = create_app()
    app.state.store = store
    client = TestClient(app)
    response = client.get("/records/search", params={"query": "PAE", "limit": 10})
    payload = response.json()
    assert payload["source_table"] == SEARCH_INDEX_TABLE
    assert payload["returned_rows"] == 3
    assert {row["record_type"] for row in payload["rows"]} == {"contract", "process", "addition"}


def test_ensure_search_index_refreshes_stale_table(tmp_path):
    store = DuckDBStore(tmp_path / "tracker.duckdb")
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    contracts = pd.DataFrame(
        [
            {
                "contract_id": "C-1",
                "process_id": "P-1",
                "entity_name": "Alcaldia de Ibague",
                "supplier_name": "Proveedor Uno",
                "department": "Tolima",
                "municipality": "Ibague",
                "modality": "Contratacion Directa",
                "status": "Adjudicado",
                "amount": 150_000_000,
                "date": "2025-01-05",
                "risk_score": 75,
                "risk_level": "alto",
                "object_text": "PAE alimentacion escolar",
                "justification": "",
                "url_process": "https://example.com/process",
            }
        ]
    )
    processes = pd.DataFrame(
        [
            {
                "id_del_proceso": "P-1",
                "id_del_portafolio": "PF-1",
                "entidad": "Alcaldia de Ibague",
                "modalidad_de_contratacion": "Licitacion publica",
                "fecha_de_publicacion_del": "2025-01-01T00:00:00.000",
                "precio_base": 200_000_000,
                "valor_total_adjudicacion": 180_000_000,
                "nombre_del_procedimiento": "Programa de Alimentacion Escolar PAE",
                "urlproceso": {"url": "https://example.com/process"},
            }
        ]
    )
    additions = pd.DataFrame(
        [
            {
                "addition_id": "A-1",
                "contract_id": "C-1",
                "addition_type": "ADICION",
                "addition_description": "Adicion de valor",
                "addition_date": "2025-02-01",
            }
        ]
    )

    store.write_frame("pae_contracts_scored", contracts, replace=True)
    store.write_frame("pae_processes", processes, replace=True)
    store.write_frame("pae_additions", additions, replace=True)
    store.write_frame(
        SEARCH_INDEX_TABLE,
        pd.DataFrame([{"record_type": "contract", "record_id": "old", "contract_id": "old", "process_id": "", "entity_name": "", "supplier_name": "", "department": "", "municipality": "", "modality": "", "status": "", "amount": 0, "date": "", "risk_score": 0, "risk_level": "", "source_table": "stale", "url_process": "", "search_text": ""}]),
        replace=True,
    )

    summary = ensure_search_index(store, processed_dir)

    assert summary.row_count == 3
    assert store.count(SEARCH_INDEX_TABLE) == 3
    count_sql, count_params = search_index_count_sql(query="PAE")
    count_frame = store.query_frame(count_sql, count_params)
    assert int(count_frame.iloc[0, 0]) == 3
    assert summary.manifest_path.endswith("pae_search_index.manifest.json")
