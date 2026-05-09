from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..config import load_risk_registry, load_scoring_registry, normalize_text
from ..paths import PROCESSED_DIR
from ..storage.duckdb_store import DuckDBStore
from ..validation.registry import ValidationRegistry, load_validation_registry_spec


@dataclass(frozen=True)
class CriteriaReference:
    origin: str
    title: str
    locator: str
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CriteriaFamilyReport:
    id: str
    label: str
    flag_codes: tuple[str, ...]
    missing_flag_codes: tuple[str, ...]
    local_references: tuple[CriteriaReference, ...]
    study_references: tuple[CriteriaReference, ...]
    validation_sources: tuple[CriteriaReference, ...]
    status: str
    summary: str
    notes: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["local_references"] = [item.to_dict() for item in self.local_references]
        payload["study_references"] = [item.to_dict() for item in self.study_references]
        payload["validation_sources"] = [item.to_dict() for item in self.validation_sources]
        payload["flag_codes"] = list(self.flag_codes)
        payload["missing_flag_codes"] = list(self.missing_flag_codes)
        return payload


@dataclass(frozen=True)
class OpacityCriteriaReport:
    generated_at: str
    overall_status: str
    criteria_count: int
    covered_count: int
    coverage_ratio: float
    families: tuple[CriteriaFamilyReport, ...]
    repository_references: tuple[CriteriaReference, ...]
    study_references: tuple[CriteriaReference, ...]
    validation_sources: tuple[CriteriaReference, ...]
    data_snapshot: dict[str, Any]
    gaps: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["families"] = [item.to_dict() for item in self.families]
        payload["repository_references"] = [item.to_dict() for item in self.repository_references]
        payload["study_references"] = [item.to_dict() for item in self.study_references]
        payload["validation_sources"] = [item.to_dict() for item in self.validation_sources]
        payload["gaps"] = list(self.gaps)
        return payload


OECD_INTEGRITY = CriteriaReference(
    origin="study",
    title="OECD Principles for Integrity in Public Procurement",
    locator="https://www.oecd.org/en/publications/2009/03/oecd-principles-for-integrity-in-public-procurement_g1gh9fbe.html",
    note="Cubre integridad durante todo el ciclo de compra y la gestion de riesgos.",
)
OCP_RED_FLAGS = CriteriaReference(
    origin="study",
    title="Red Flags in Public Procurement",
    locator="https://www.open-contracting.org/resources/red-flags-in-public-procurement-a-guide-to-using-data-to-detect-and-mitigate-risks/",
    note="Muestra metodologia de red flags desde planeacion hasta implementacion.",
)
WB_WARNING_SIGNS = CriteriaReference(
    origin="study",
    title="Warning Signs of Fraud and Corruption in Procurement",
    locator="https://documents1.worldbank.org/curated/en/223241573576857116/pdf/Warning-Signs-of-Fraud-and-Corruption-in-Procurement.pdf",
    note="Resume senales como contratos pequenos, sobreprecio y cambios de valor.",
)
WB_INDICATORS = CriteriaReference(
    origin="study",
    title="Public procurement corruption risk indicators validation study",
    locator="https://documents1.worldbank.org/curated/en/099354012192440312/pdf/IDU-e8b27e8d-a15e-4c77-8145-f021dc9840d4.pdf",
    note="Valida indicadores de riesgo y usa single bidding como proxy principal.",
)

LOCAL_RISK_FILES = (
    CriteriaReference(
        origin="local_repository",
        title="risk_flags.yml",
        locator="Fase_2/capa_1_ingesta_automatizada/pae_risk_tracker/config/risk_flags.yml",
        note="Catalogo principal de senales de opacidad y pesos por dimension.",
    ),
    CriteriaReference(
        origin="local_repository",
        title="scoring.yml",
        locator="Fase_2/capa_1_ingesta_automatizada/pae_risk_tracker/config/scoring.yml",
        note="Bandas de score y ponderacion de dimensiones.",
    ),
    CriteriaReference(
        origin="local_repository",
        title="rules_engine.py",
        locator="Fase_2/capa_2_motor_reglas_cuantitativas/pae_risk_tracker/src/pae_risk_tracker/risk/rules_engine.py",
        note="Implementa las reglas deterministicas y la trazabilidad de cada flag.",
    ),
)

LOCAL_TRACEABILITY_FILES = (
    CriteriaReference(
        origin="local_repository",
        title="validation_sources.json",
        locator="Fase_2/capa_1_ingesta_automatizada/pae_risk_tracker/config/validation_sources.json",
        note="Lista las fuentes web permitidas para validacion documental.",
    ),
    CriteriaReference(
        origin="local_repository",
        title="validation/service.py",
        locator="Fase_2/capa_1_ingesta_automatizada/pae_risk_tracker/src/pae_risk_tracker/validation/service.py",
        note="Cruza PACO, SECOP y fuentes externas para revisar trazabilidad.",
    ),
    CriteriaReference(
        origin="local_repository",
        title="data_pack.json",
        locator="Fase_2/capa_1_ingesta_automatizada/pae_risk_tracker/config/data_pack.json",
        note="Declara el pack local de PACO y SECOP usado en la validacion.",
    ),
    CriteriaReference(
        origin="local_repository",
        title="search_index.py",
        locator="Fase_2/capa_2_motor_reglas_cuantitativas/pae_risk_tracker/src/pae_risk_tracker/retrieval/search_index.py",
        note="Materializa el indice unificado y su manifiesto de trazabilidad.",
    ),
)


_FAMILY_SPECS: tuple[dict[str, Any], ...] = (
    {
        "id": "planning",
        "label": "Planeacion / modalidad",
        "flag_codes": ("RF-01", "RF-03", "RF-04", "RF-05", "RF-06", "RF-07"),
        "local_references": (
            LOCAL_RISK_FILES[0],
            LOCAL_RISK_FILES[1],
            LOCAL_RISK_FILES[2],
        ),
        "study_references": (OECD_INTEGRITY, OCP_RED_FLAGS, WB_WARNING_SIGNS),
        "validation_sources": (LOCAL_TRACEABILITY_FILES[0],),
    },
    {
        "id": "competition",
        "label": "Competencia",
        "flag_codes": ("RF-08", "RF-09", "RF-11", "RF-12", "RF-13", "RF-23"),
        "local_references": (
            LOCAL_RISK_FILES[0],
            LOCAL_RISK_FILES[2],
            LOCAL_TRACEABILITY_FILES[3],
        ),
        "study_references": (OCP_RED_FLAGS, WB_INDICATORS, WB_WARNING_SIGNS),
        "validation_sources": (LOCAL_TRACEABILITY_FILES[0],),
    },
    {
        "id": "supplier_concentration",
        "label": "Concentracion del proveedor",
        "flag_codes": ("RF-10", "RF-14", "RF-15", "RF-16"),
        "local_references": (
            LOCAL_RISK_FILES[0],
            LOCAL_RISK_FILES[2],
            LOCAL_TRACEABILITY_FILES[3],
        ),
        "study_references": (WB_INDICATORS, OCP_RED_FLAGS, WB_WARNING_SIGNS),
        "validation_sources": (LOCAL_TRACEABILITY_FILES[0],),
    },
    {
        "id": "value_outlier",
        "label": "Valor / sobrecosto relativo",
        "flag_codes": ("RF-20", "RF-21"),
        "local_references": (
            LOCAL_RISK_FILES[0],
            LOCAL_RISK_FILES[1],
            LOCAL_RISK_FILES[2],
        ),
        "study_references": (WB_WARNING_SIGNS, OCP_RED_FLAGS, OECD_INTEGRITY),
        "validation_sources": (LOCAL_TRACEABILITY_FILES[0],),
    },
    {
        "id": "execution",
        "label": "Adiciones y prorrogas",
        "flag_codes": ("RF-22", "RF-24", "RF-25", "RF-26", "RF-27", "RF-28", "RF-29"),
        "local_references": (
            LOCAL_RISK_FILES[0],
            LOCAL_RISK_FILES[2],
            LOCAL_TRACEABILITY_FILES[2],
        ),
        "study_references": (WB_WARNING_SIGNS, OCP_RED_FLAGS, OECD_INTEGRITY),
        "validation_sources": (LOCAL_TRACEABILITY_FILES[0],),
    },
    {
        "id": "traceability",
        "label": "Opacidad documental",
        "flag_codes": ("RF-30", "RF-31", "RF-32", "RF-33", "RF-34", "RF-35", "RF-36", "RF-37"),
        "local_references": (
            LOCAL_TRACEABILITY_FILES[0],
            LOCAL_TRACEABILITY_FILES[1],
            LOCAL_TRACEABILITY_FILES[2],
            LOCAL_TRACEABILITY_FILES[3],
        ),
        "study_references": (OECD_INTEGRITY, WB_WARNING_SIGNS, OCP_RED_FLAGS),
        "validation_sources": (LOCAL_TRACEABILITY_FILES[0], LOCAL_TRACEABILITY_FILES[1]),
    },
    {
        "id": "paco_context",
        "label": "Sanciones / PACO",
        "flag_codes": ("RF-17", "RF-18", "RF-19"),
        "local_references": (
            LOCAL_TRACEABILITY_FILES[0],
            LOCAL_TRACEABILITY_FILES[2],
            CriteriaReference(
                origin="local_repository",
                title="paco_events table",
                locator="Fase_2/capa_1_ingesta_automatizada/pae_risk_tracker/data/processed/paco/paco_events.parquet",
                note="Consolidado local de antecedentes PACO y validacion documental.",
            ),
        ),
        "study_references": (WB_INDICATORS, OECD_INTEGRITY, OCP_RED_FLAGS),
        "validation_sources": (LOCAL_TRACEABILITY_FILES[0], LOCAL_TRACEABILITY_FILES[1]),
    },
)


def build_opacity_criteria_report(
    store: DuckDBStore | None = None,
    *,
    processed_dir: Path | None = None,
    validation_registry: ValidationRegistry | None = None,
) -> OpacityCriteriaReport:
    processed_dir = processed_dir or PROCESSED_DIR
    validation_registry = validation_registry or load_validation_registry_spec()
    risk_registry = load_risk_registry()
    scoring_registry = load_scoring_registry()
    flags = dict(risk_registry.get("flags", {}) or {})
    dimensions = dict(scoring_registry.get("dimension_weights", {}) or {})
    families = [
        _build_family_report(spec, flags=flags, dimensions=dimensions, validation_registry=validation_registry)
        for spec in _FAMILY_SPECS
    ]
    repository_references = _unique_references(
        ref
        for family in families
        for ref in (*family.local_references,)
    )
    study_references = _unique_references(
        ref
        for family in families
        for ref in (*family.study_references,)
    )
    validation_sources = _unique_references(
        ref
        for family in families
        for ref in (*family.validation_sources,)
    )
    covered_count = sum(1 for family in families if family.status == "covered")
    gaps = tuple(
        note
        for family in families
        for note in family.notes
        if family.status != "covered"
    )
    data_snapshot = _build_data_snapshot(store, processed_dir, validation_registry)
    overall_status = "verified" if covered_count == len(families) and not gaps else "partial" if covered_count else "needs_review"
    coverage_ratio = round(covered_count / len(families), 3) if families else 0.0
    return OpacityCriteriaReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        overall_status=overall_status,
        criteria_count=len(families),
        covered_count=covered_count,
        coverage_ratio=coverage_ratio,
        families=tuple(families),
        repository_references=tuple(repository_references),
        study_references=tuple(study_references),
        validation_sources=tuple(validation_sources),
        data_snapshot=data_snapshot,
        gaps=gaps,
    )


def build_criteria_knowledge_rows(report: OpacityCriteriaReport, query: str = "") -> list[dict[str, Any]]:
    query_text = normalize_text(query)
    rows: list[dict[str, Any]] = []
    for family in report.families:
        for reference in family.local_references:
            rows.append(_knowledge_row(family, reference, query_text))
        for reference in family.study_references:
            rows.append(_knowledge_row(family, reference, query_text))
        for reference in family.validation_sources:
            rows.append(_knowledge_row(family, reference, query_text))

    for row in rows:
        row["search_text"] = normalize_text(
            " ".join(
                str(value)
                for value in (
                    row.get("criteria_id"),
                    row.get("criteria_label"),
                    row.get("reference_kind"),
                    row.get("reference_title"),
                    row.get("reference_locator"),
                    row.get("note"),
                    " ".join(row.get("flag_codes") or []),
                )
                if value
            )
        )

    if query_text:
        rows.sort(key=lambda row: (_knowledge_score(query_text, row), row["criteria_id"], row["reference_title"]), reverse=True)
    else:
        rows.sort(key=lambda row: (row["criteria_id"], row["reference_kind"], row["reference_title"]))
    return rows


def _build_family_report(
    spec: dict[str, Any],
    *,
    flags: dict[str, Any],
    dimensions: dict[str, Any],
    validation_registry: ValidationRegistry,
) -> CriteriaFamilyReport:
    flag_codes = tuple(str(code) for code in spec.get("flag_codes", ()) if str(code).strip())
    missing_flag_codes = tuple(code for code in flag_codes if code not in flags)
    local_references = tuple(spec.get("local_references", ()))
    study_references = tuple(spec.get("study_references", ()))
    validation_sources = tuple(spec.get("validation_sources", ()))
    dimension_weight = int(dimensions.get(spec["id"], 0))
    notes: list[str] = []
    if missing_flag_codes:
        notes.append(f"Faltan flags configurados: {', '.join(missing_flag_codes)}.")
    if not local_references:
        notes.append("No hay referencias locales para esta familia.")
    if not study_references:
        notes.append("No hay estudios o guias oficiales asignados.")
    if validation_sources:
        notes.append(f"Cruza {len(validation_sources)} fuente(s) de validacion web permitidas.")

    if missing_flag_codes:
        status = "gap"
    elif local_references and study_references:
        status = "covered"
    else:
        status = "partial"

    summary = (
        f"{spec['label']} cubre {len(flag_codes)} flags, peso configurado {dimension_weight} y "
        f"{len(local_references)} referencia(s) locales + {len(study_references)} estudio(s)."
    )
    return CriteriaFamilyReport(
        id=str(spec["id"]),
        label=str(spec["label"]),
        flag_codes=flag_codes,
        missing_flag_codes=missing_flag_codes,
        local_references=local_references,
        study_references=study_references,
        validation_sources=validation_sources,
        status=status,
        summary=summary,
        notes=tuple(notes),
    )


def _knowledge_row(family: CriteriaFamilyReport, reference: CriteriaReference, query_text: str) -> dict[str, Any]:
    return {
        "criteria_id": family.id,
        "criteria_label": family.label,
        "reference_kind": reference.origin,
        "reference_title": reference.title,
        "reference_locator": reference.locator,
        "note": reference.note,
        "flag_codes": list(family.flag_codes),
        "criteria_status": family.status,
        "criteria_summary": family.summary,
        "match_hint": _knowledge_score(query_text, {
            "criteria_id": family.id,
            "criteria_label": family.label,
            "reference_kind": reference.origin,
            "reference_title": reference.title,
            "reference_locator": reference.locator,
            "note": reference.note,
            "flag_codes": list(family.flag_codes),
        }),
    }


def _knowledge_score(query_text: str, row: dict[str, Any]) -> int:
    if not query_text:
        return 0
    haystack = normalize_text(
        " ".join(
            str(value)
            for value in (
                row.get("criteria_id"),
                row.get("criteria_label"),
                row.get("reference_kind"),
                row.get("reference_title"),
                row.get("reference_locator"),
                row.get("note"),
                " ".join(row.get("flag_codes") or []),
            )
            if value
        )
    )
    score = 0
    for token in query_text.split():
        if token and token in haystack:
            score += 4
    if row.get("reference_kind") == "study" and any(token in query_text for token in ("ESTUDIO", "FUENTE", "REFERENCIA", "BIBLIOGRAFIA")):
        score += 3
    if row.get("reference_kind") == "local_repository" and any(token in query_text for token in ("REPOSITORIO", "REPO", "CODIGO", "CONFIG", "CONFIGURACION")):
        score += 3
    if row.get("criteria_status") == "covered":
        score += 1
    return score


def _unique_references(references: Any) -> list[CriteriaReference]:
    seen: set[tuple[str, str]] = set()
    unique: list[CriteriaReference] = []
    for reference in references:
        if not isinstance(reference, CriteriaReference):
            continue
        key = (reference.origin, reference.locator)
        if key in seen:
            continue
        seen.add(key)
        unique.append(reference)
    return unique


def _build_data_snapshot(
    store: DuckDBStore | None,
    processed_dir: Path,
    validation_registry: ValidationRegistry,
) -> dict[str, Any]:
    tables = [
        "pae_contracts_core",
        "pae_contracts_scored",
        "pae_contracts_enriched",
        "pae_processes",
        "pae_additions",
        "paco_events",
        "validation_runs",
        "validation_observations",
        "pae_search_index",
    ]
    duckdb_counts: dict[str, int] = {}
    if store is not None:
        for table_name in tables:
            try:
                duckdb_counts[table_name] = int(store.count(table_name)) if store.has_table(table_name) else 0
            except Exception:
                duckdb_counts[table_name] = 0
    else:
        duckdb_counts = {table_name: 0 for table_name in tables}

    manifest_path = processed_dir / "pae_search_index.manifest.json"
    manifest = _read_json(manifest_path)
    return {
        "duckdb_tables": duckdb_counts,
        "search_index_manifest": manifest or {},
        "active_validation_sources": [source.to_dict() for source in validation_registry.active_sources()],
        "active_validation_source_count": len(validation_registry.active_sources()),
        "processed_dir": str(processed_dir),
    }


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        import json

        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
