from __future__ import annotations

import json
import re
import unicodedata
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


ROOT_DIR = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT_DIR / "config"
DATA_DIR = ROOT_DIR / "data"
CACHE_DIR = DATA_DIR / "cache"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUT_DIR = DATA_DIR / "outputs"


def load_json_config(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.loads(handle.read())


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = text.upper()
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_lower(value: Any) -> str:
    return normalize_text(value).lower()


def normalize_digits(value: Any) -> str:
    return re.sub(r"\D+", "", "" if value is None else str(value))


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def fetch_json(url: str, timeout: int = 60) -> Any:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "PAE-Risk-Tracker/0.1",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def build_query_url(base_url: str, params: dict[str, Any]) -> str:
    filtered = {key: value for key, value in params.items() if value not in (None, "", [], {})}
    return f"{base_url}?{urllib.parse.urlencode(filtered)}"


def append_app_token(url: str, token: str | None) -> str:
    if not token:
        return url

    parsed = urllib.parse.urlparse(url)
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    query.append(("$$app_token", token))
    return urllib.parse.urlunparse(parsed._replace(query=urllib.parse.urlencode(query)))


def soql_literal(value: str) -> str:
    return value.replace("'", "''")


def make_range_clause(field_name: str | None, year: int | None) -> str:
    if not field_name or year is None:
        return ""

    next_year = year + 1
    start = f"{year:04d}-01-01T00:00:00"
    end = f"{next_year:04d}-01-01T00:00:00"
    return f"{field_name} >= '{start}' AND {field_name} < '{end}'"


def build_like_clause(field_name: str, terms: Iterable[str]) -> str:
    pieces = []
    for term in terms:
        cleaned = normalize_text(term)
        if not cleaned:
            continue
        pieces.append(f"UPPER(COALESCE({field_name}, '')) LIKE '%{soql_literal(cleaned)}%'")
    return "(" + " OR ".join(pieces) + ")" if pieces else ""


def combine_clauses(*clauses: str) -> str:
    active = [clause for clause in clauses if clause]
    if not active:
        return ""
    if len(active) == 1:
        return active[0]
    return "(" + " AND ".join(active) + ")"


def build_in_clause(field_name: str, values: Iterable[Any]) -> str:
    escaped = []
    for value in values:
        text = "" if value is None else str(value).strip()
        if not text:
            continue
        escaped.append(f"'{soql_literal(text)}'")
    if not escaped:
        return ""
    return f"{field_name} IN ({', '.join(escaped)})"


def batched(values: list[Any], size: int) -> Iterable[list[Any]]:
    if size <= 0:
        size = 1
    for index in range(0, len(values), size):
        yield values[index : index + size]


def resolve_columns(columns: list[dict[str, Any]], alias_map: dict[str, list[str]]) -> dict[str, str | None]:
    normalized = {
        normalize_text(column.get("fieldName") or column.get("name") or ""): column
        for column in columns
    }

    resolved: dict[str, str | None] = {}
    for role, aliases in alias_map.items():
        resolved[role] = None
        for alias in aliases:
            match_key = normalize_text(alias)
            if match_key in normalized:
                resolved[role] = normalized[match_key].get("fieldName") or normalized[match_key].get("name")
                break
    return resolved


def pick_columns(columns: list[dict[str, Any]], names: Iterable[str | None]) -> list[str]:
    wanted = {normalize_text(name) for name in names if name}
    picked: list[str] = []
    for column in columns:
        field_name = column.get("fieldName") or column.get("name")
        if normalize_text(field_name) in wanted:
            picked.append(field_name)
    return picked


def top_text_columns(columns: list[dict[str, Any]], limit: int = 5) -> list[str]:
    result: list[str] = []
    for column in columns:
        if column.get("dataTypeName") in {"text", "calendar_date"}:
            field_name = column.get("fieldName") or column.get("name")
            if field_name:
                result.append(field_name)
        if len(result) >= limit:
            break
    return result


def sample_row_query(dataset_id: str, limit: int = 1, select: str | None = None) -> str:
    base_url = f"https://www.datos.gov.co/resource/{dataset_id}.json"
    params: dict[str, Any] = {"$limit": limit}
    if select:
        params["$select"] = select
    return build_query_url(base_url, params)


def dataset_metadata_url(dataset_id: str) -> str:
    return f"https://www.datos.gov.co/api/views/{dataset_id}.json"


def load_dataset_config() -> dict[str, Any]:
    return load_json_config(CONFIG_DIR / "datasets.yml")


def load_keywords_config() -> dict[str, Any]:
    return load_json_config(CONFIG_DIR / "pae_keywords.yml")


def load_columns_config() -> dict[str, Any]:
    return load_json_config(CONFIG_DIR / "columns.yml")


def load_schema_cache() -> dict[str, Any]:
    schema_path = CACHE_DIR / "schema_cache.json"
    if not schema_path.exists():
        return {}
    return load_json_config(schema_path)


def write_parquet_frame(rows: list[dict[str, Any]], path: Path) -> None:
    import pandas as pd

    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows)
    frame.to_parquet(path, index=False)
