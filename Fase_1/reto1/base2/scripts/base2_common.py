from __future__ import annotations

import json
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path


API_BASE_URL = "https://www.datos.gov.co"
DATASET_ID = "dmgg-8hin"
META_URL = f"{API_BASE_URL}/api/views/{DATASET_ID}.json"
RESOURCE_URL = f"{API_BASE_URL}/resource/{DATASET_ID}.json"
BASE2_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EDA_OUTPUT = BASE2_ROOT / "EDA_base2.html"
DEFAULT_QUALITY_OUTPUT = BASE2_ROOT / "calidad_datos_base2.html"

MISSING_TOKENS = {
    "",
    "na",
    "n/a",
    "null",
    "none",
    "no definido",
    "no definida",
    "sin descripcion",
    "sin descripcin",
}


def fetch_json(url: str):
    with urllib.request.urlopen(url) as response:
        return json.loads(response.read().decode("utf-8"))


def load_metadata() -> dict:
    return fetch_json(META_URL)


def query_resource(params: dict[str, str]) -> list[dict]:
    url = RESOURCE_URL + "?" + urllib.parse.urlencode(params, safe="(),* <>/=:'\"")
    return fetch_json(url)


def format_int_es(value: int) -> str:
    return f"{value:,}".replace(",", ".")


def format_float_es(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}".replace(".", ",")


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    cleaned = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(cleaned)
    except ValueError:
        return None


def format_iso_datetime(value: str | None) -> str:
    parsed = parse_iso_datetime(value)
    if parsed is None:
        return "N/D"
    if parsed.hour == 0 and parsed.minute == 0 and parsed.second == 0:
        return parsed.strftime("%Y-%m-%d")
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def column_names_by_type(metadata: dict, type_name: str) -> list[tuple[str, str]]:
    return [
        (column["fieldName"], column["name"])
        for column in metadata["columns"]
        if column["dataTypeName"] == type_name
    ]


def column_names_by_types(metadata: dict, type_names: set[str]) -> list[tuple[str, str]]:
    return [
        (column["fieldName"], column["name"])
        for column in metadata["columns"]
        if column["dataTypeName"] in type_names
    ]


def build_missing_count_expr(field_name: str, treat_blank_as_missing: bool = False) -> str:
    if treat_blank_as_missing:
        return (
            f"sum(case when {field_name} is null or trim({field_name}) = '' "
            f"or lower(trim({field_name})) in ('null','na','n/a','none','no definido','no definida','sin descripcion','sin descripcin') "
            f"then 1 else 0 end)"
        )
    return f"count(*) - count({field_name})"


