from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .paths import CONFIG_DIR


def _load_structured_text(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        try:
            import yaml  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise ValueError("The configuration file is not valid JSON and PyYAML is not installed.") from exc
        return yaml.safe_load(text)


def load_structured_file(path: Path) -> Any:
    return _load_structured_text(path.read_text(encoding="utf-8"))


def load_first_existing(stem: str) -> Any:
    for suffix in (".yaml", ".yml", ".json"):
        candidate = CONFIG_DIR / f"{stem}{suffix}"
        if candidate.exists():
            return load_structured_file(candidate)
    raise FileNotFoundError(f"Configuration file not found for stem: {stem}")


def normalize_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = re.sub(r"[^A-Z0-9]+", " ", text.upper())
    return re.sub(r"\s+", " ", text).strip()


def normalize_digits(value: Any) -> str:
    return re.sub(r"\D+", "", "" if value is None else str(value))


@dataclass(frozen=True)
class DatasetSpec:
    key: str
    id: str
    name: str
    role: str
    active: bool = True
    default_year_field: str | None = None
    text_fields: tuple[str, ...] = ()
    id_fields: dict[str, str] | None = None


def load_dataset_registry() -> dict[str, Any]:
    return load_first_existing("datasets")


def load_column_registry() -> dict[str, Any]:
    return load_first_existing("columns")


def load_keyword_registry() -> dict[str, Any]:
    return load_first_existing("pae_keywords")


def load_risk_registry() -> dict[str, Any]:
    return load_first_existing("risk_flags")


def load_pack_registry() -> dict[str, Any]:
    return load_first_existing("data_pack")


def load_scoring_registry() -> dict[str, Any]:
    return load_first_existing("scoring")


def load_llm_registry() -> dict[str, Any]:
    return load_first_existing("llm")


def load_validation_registry() -> dict[str, Any]:
    return load_first_existing("validation_sources")


def dataset_specs() -> list[DatasetSpec]:
    payload = load_dataset_registry()
    specs: list[DatasetSpec] = []
    for entry in payload.get("datasets", []):
        specs.append(
            DatasetSpec(
                key=str(entry.get("key", entry.get("id", ""))),
                id=str(entry.get("id", "")),
                name=str(entry.get("name", "")),
                role=str(entry.get("role", "secondary")),
                active=bool(entry.get("active", True)),
                default_year_field=entry.get("default_year_field"),
                text_fields=tuple(entry.get("text_fields", []) or ()),
                id_fields=dict(entry.get("id_fields", {}) or {}),
            )
        )
    return specs


def active_dataset_specs() -> list[DatasetSpec]:
    return [spec for spec in dataset_specs() if spec.active]


def find_dataset_spec(key_or_id: str) -> DatasetSpec | None:
    needle = normalize_text(key_or_id)
    for spec in dataset_specs():
        if normalize_text(spec.key) == needle or normalize_text(spec.id) == needle:
            return spec
    return None


def alias_lookup(column_names: list[str], aliases: dict[str, list[str]]) -> dict[str, str | None]:
    normalized = {normalize_text(name): name for name in column_names}
    resolved: dict[str, str | None] = {}
    for role, candidates in aliases.items():
        resolved[role] = next((normalized.get(normalize_text(alias)) for alias in candidates if normalize_text(alias) in normalized), None)
    return resolved
