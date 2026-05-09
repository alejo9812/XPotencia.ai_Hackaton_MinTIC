from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from ..paths import DATA_DIR, DEFAULT_DUCKDB_PATH, OUTPUT_DIR, PROCESSED_DIR


@dataclass(frozen=True)
class ApiSettings:
    duckdb_path: Path = DEFAULT_DUCKDB_PATH
    data_dir: Path = DATA_DIR
    processed_dir: Path = PROCESSED_DIR
    default_limit: int = 50
    high_risk_threshold: int = 61
    output_dir: Path = OUTPUT_DIR


def load_api_settings() -> ApiSettings:
    duckdb_value = os.getenv("PAE_DUCKDB_PATH", str(DEFAULT_DUCKDB_PATH))
    default_limit = int(os.getenv("PAE_API_DEFAULT_LIMIT", "50"))
    high_risk_threshold = int(os.getenv("PAE_API_HIGH_RISK_THRESHOLD", "61"))
    return ApiSettings(
        duckdb_path=Path(duckdb_value),
        data_dir=DATA_DIR,
        processed_dir=PROCESSED_DIR,
        default_limit=default_limit,
        high_risk_threshold=high_risk_threshold,
    )
