from __future__ import annotations

from pae_risk_tracker.storage.duckdb_store import DuckDBStore


def test_duckdb_store_roundtrip(tmp_path):
    store = DuckDBStore(tmp_path / "tracker.duckdb")
    rows = [{"contract_id": "A", "amount": 100}, {"contract_id": "B", "amount": 200}]

    result = store.write_rows("contracts", rows)
    frame = store.read_frame("SELECT * FROM contracts ORDER BY contract_id")

    assert result.row_count == 2
    assert store.count("contracts") == 2
    assert list(frame["contract_id"]) == ["A", "B"]
    assert list(frame["amount"]) == [100, 200]

