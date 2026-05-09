from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


@dataclass(frozen=True)
class TableWriteResult:
    table_name: str
    row_count: int
    path: Path


class DuckDBStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.path))

    def write_rows(self, table_name: str, rows: list[dict[str, Any]], replace: bool = True) -> TableWriteResult:
        frame = pd.DataFrame(rows)
        return self.write_frame(table_name, frame, replace=replace)

    def write_frame(self, table_name: str, frame: pd.DataFrame, replace: bool = True) -> TableWriteResult:
        with self.connect() as con:
            con.register("frame_view", frame)
            clause = "OR REPLACE " if replace else ""
            con.execute(f"CREATE {clause}TABLE {table_name} AS SELECT * FROM frame_view")
        return TableWriteResult(table_name=table_name, row_count=len(frame), path=self.path)

    def append_frame(self, table_name: str, frame: pd.DataFrame) -> TableWriteResult:
        with self.connect() as con:
            con.register("frame_view", frame)
            if self.has_table(table_name):
                con.execute(f"INSERT INTO {table_name} SELECT * FROM frame_view")
            else:
                con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM frame_view")
        return TableWriteResult(table_name=table_name, row_count=len(frame), path=self.path)

    def read_frame(self, sql: str) -> pd.DataFrame:
        return self.query_frame(sql)

    def query_frame(self, sql: str, params: list[Any] | tuple[Any, ...] | None = None) -> pd.DataFrame:
        with self.connect() as con:
            if params is None:
                return con.execute(sql).fetchdf()
            return con.execute(sql, params).fetchdf()

    def count(self, table_name: str) -> int:
        with self.connect() as con:
            return int(con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])

    def has_table(self, table_name: str) -> bool:
        with self.connect() as con:
            value = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()[0]
            return int(value) > 0

    def list_tables(self) -> list[str]:
        with self.connect() as con:
            rows = con.execute("SELECT table_name FROM information_schema.tables ORDER BY table_name").fetchall()
        return [str(row[0]) for row in rows]
