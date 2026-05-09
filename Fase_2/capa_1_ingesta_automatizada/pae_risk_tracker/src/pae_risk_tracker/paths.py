from __future__ import annotations

from pathlib import Path


PACKAGE_DIR = Path(__file__).resolve().parent
SRC_DIR = PACKAGE_DIR.parent
PROJECT_DIR = SRC_DIR.parent
CONFIG_DIR = PROJECT_DIR / "config"
DATA_DIR = PROJECT_DIR / "data"
CACHE_DIR = DATA_DIR / "cache"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUT_DIR = DATA_DIR / "outputs"
VALIDATION_DIR = DATA_DIR / "validation"
VALIDATION_SNAPSHOT_DIR = VALIDATION_DIR / "snapshots"
VALIDATION_REPORT_DIR = VALIDATION_DIR / "reports"
DUCKDB_DIR = DATA_DIR / "duckdb"
DEFAULT_DUCKDB_PATH = DUCKDB_DIR / "pae_risk_tracker.duckdb"


def ensure_runtime_dirs() -> None:
    for path in (CACHE_DIR, RAW_DIR, PROCESSED_DIR, OUTPUT_DIR, VALIDATION_DIR, VALIDATION_SNAPSHOT_DIR, VALIDATION_REPORT_DIR, DUCKDB_DIR):
        path.mkdir(parents=True, exist_ok=True)
