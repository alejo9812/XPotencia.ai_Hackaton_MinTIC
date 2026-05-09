from __future__ import annotations

from pathlib import Path
from typing import Any

from pipeline_utils import (
    CACHE_DIR,
    CONFIG_DIR,
    dataset_metadata_url,
    fetch_json,
    load_columns_config,
    load_dataset_config,
    load_schema_cache,
    now_iso,
    pick_columns,
    save_json,
    sample_row_query,
    resolve_columns,
)


def main() -> None:
    dataset_config = load_dataset_config()
    columns_config = load_columns_config()
    cached = load_schema_cache()
    schema: dict[str, Any] = {
        "generated_at": now_iso(),
        "project": dataset_config.get("project", "PAE Risk Tracker"),
        "datasets": {},
    }

    core_aliases = columns_config.get("core", {})

    for entry in dataset_config.get("datasets", []):
        if not entry.get("active", True):
            continue

        dataset_id = entry["id"]
        print(f"Inspecting {dataset_id} - {entry['name']}")
        metadata = fetch_json(dataset_metadata_url(dataset_id))
        columns = metadata.get("columns", [])
        sample = fetch_json(sample_row_query(dataset_id, limit=1))
        resolved = resolve_columns(columns, core_aliases)
        selected = pick_columns(columns, list(resolved.values()))

        schema["datasets"][dataset_id] = {
            "key": entry["key"],
            "name": entry["name"],
            "role": entry["role"],
            "column_count": len(columns),
            "columns": [
                {
                    "name": col.get("name"),
                    "fieldName": col.get("fieldName"),
                    "dataTypeName": col.get("dataTypeName"),
                    "description": col.get("description"),
                }
                for col in columns
            ],
            "sample_row": sample[0] if isinstance(sample, list) and sample else {},
            "resolved_columns": resolved,
            "resolved_column_names": selected,
            "cached": cached.get("datasets", {}).get(dataset_id, {}),
            "default_year_field": entry.get("default_year_field"),
            "text_fields": entry.get("text_fields", []),
            "id_fields": entry.get("id_fields", {}),
        }

    output_path = CACHE_DIR / "schema_cache.json"
    save_json(output_path, schema)
    print(f"Schema cache written to {output_path}")


if __name__ == "__main__":
    main()
