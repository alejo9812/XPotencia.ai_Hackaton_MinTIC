from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import re
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd

from ..config import load_pack_registry, normalize_digits, normalize_text
from ..paths import CACHE_DIR, PROCESSED_DIR, RAW_DIR, ensure_runtime_dirs
from ..storage.duckdb_store import DuckDBStore


logger = logging.getLogger(__name__)

PACO_PACK_TABLE = "paco_events"


@dataclass(frozen=True)
class PacoSourceSpec:
    key: str
    name: str
    url: str
    family: str = "paco"
    event_type: str = ""
    enabled: bool = True


@dataclass(frozen=True)
class PacoSourceSummary:
    key: str
    name: str
    url: str
    raw_path: str
    processed_parquet: str
    rows_downloaded: int
    rows_normalized: int
    sha256: str
    columns: list[str]


@dataclass(frozen=True)
class PacoPackSummary:
    pack_name: str
    source_count: int
    rows_downloaded: int
    rows_normalized: int
    table_name: str
    raw_dir: str
    processed_dir: str
    duckdb_path: str
    manifest_path: str
    source_summaries: list[dict[str, Any]]


class DataPackLoader:
    def __init__(
        self,
        store: DuckDBStore,
        raw_dir: Path | None = None,
        processed_dir: Path | None = None,
        cache_dir: Path | None = None,
    ) -> None:
        ensure_runtime_dirs()
        self.store = store
        self.raw_dir = Path(raw_dir or (RAW_DIR / "paco"))
        self.processed_dir = Path(processed_dir or (PROCESSED_DIR / "paco"))
        self.cache_dir = Path(cache_dir or (CACHE_DIR / "paco"))
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.sources = self._load_source_specs()

    def sync_paco(self, *, refresh: bool = False, include_disabled: bool = False, source_keys: list[str] | None = None) -> PacoPackSummary:
        selected = self._select_sources(include_disabled=include_disabled, source_keys=source_keys)
        source_summaries: list[PacoSourceSummary] = []
        rows_downloaded = 0
        rows_normalized = 0

        for spec in selected:
            raw_path, sha256 = self._ensure_source_file(spec, refresh=refresh)
            frame = self._read_tabular_frame(raw_path, spec)
            normalized = self._normalize_source_frame(frame, spec)
            rows_downloaded += int(len(frame))
            rows_normalized += int(len(normalized))

            processed_path = self.processed_dir / f"paco_{spec.key}.parquet"
            normalized.to_parquet(processed_path, index=False)
            self.store.write_frame(f"paco_{spec.key}", normalized, replace=True)

            source_summaries.append(
                PacoSourceSummary(
                    key=spec.key,
                    name=spec.name,
                    url=spec.url,
                    raw_path=str(raw_path),
                    processed_parquet=str(processed_path),
                    rows_downloaded=int(len(frame)),
                    rows_normalized=int(len(normalized)),
                    sha256=sha256,
                    columns=list(frame.columns),
                )
            )

        consolidated = self._combine_cached_frames()
        consolidated_path = self.processed_dir / f"{PACO_PACK_TABLE}.parquet"
        consolidated.to_parquet(consolidated_path, index=False)
        self.store.write_frame(PACO_PACK_TABLE, consolidated, replace=True)

        manifest = {
            "pack_name": self._pack_name(),
            "table_name": PACO_PACK_TABLE,
            "source_count": len(selected),
            "rows_downloaded": rows_downloaded,
            "rows_normalized": rows_normalized,
            "sources": [asdict(summary) for summary in source_summaries],
            "consolidated_parquet": str(consolidated_path),
        }
        manifest_path = self.cache_dir / "paco_pack_manifest.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

        return PacoPackSummary(
            pack_name=self._pack_name(),
            source_count=len(selected),
            rows_downloaded=rows_downloaded,
            rows_normalized=rows_normalized,
            table_name=PACO_PACK_TABLE,
            raw_dir=str(self.raw_dir),
            processed_dir=str(self.processed_dir),
            duckdb_path=str(self.store.path),
            manifest_path=str(manifest_path),
            source_summaries=[asdict(summary) for summary in source_summaries],
        )

    def _load_source_specs(self) -> list[PacoSourceSpec]:
        payload = load_pack_registry()
        specs: list[PacoSourceSpec] = []
        for entry in payload.get("sources", []):
            url = str(entry.get("url", "")).strip()
            if not url:
                continue
            specs.append(
                PacoSourceSpec(
                    key=str(entry.get("key", "")) or self._slug_from_url(url),
                    name=str(entry.get("name", entry.get("key", url))),
                    url=url,
                    family=str(entry.get("family", "paco")),
                    event_type=str(entry.get("event_type", "")),
                    enabled=bool(entry.get("enabled", True)),
                )
            )
        if specs:
            return specs
        return self._default_source_specs()

    def _default_source_specs(self) -> list[PacoSourceSpec]:
        return [
            PacoSourceSpec(
                key="disciplinary",
                name="PACO - Antecedentes SIRI sanciones",
                url="https://paco7public7info7prod.blob.core.windows.net/paco-pulic-info/antecedentes_SIRI_sanciones_Cleaned.zip",
                event_type="disciplinary_sanction",
            ),
            PacoSourceSpec(
                key="penal",
                name="PACO - Sanciones penales FGN",
                url="https://paco7public7info7prod.blob.core.windows.net/paco-pulic-info/sanciones_penales_FGN.csv",
                event_type="penal_sanction",
            ),
            PacoSourceSpec(
                key="fiscal",
                name="PACO - Responsabilidades fiscales",
                url="https://paco7public7info7prod.blob.core.windows.net/paco-pulic-info/responsabilidades_fiscales.csv",
                event_type="fiscal_responsibility",
            ),
            PacoSourceSpec(
                key="contractual",
                name="PACO - Multas SECOP",
                url="https://paco7public7info7prod.blob.core.windows.net/paco-pulic-info/multas_SECOP_Cleaned.zip",
                event_type="contractual_sanction",
            ),
            PacoSourceSpec(
                key="collusion",
                name="PACO - Colusiones en contratacion",
                url="https://paco7public7info7prod.blob.core.windows.net/paco-pulic-info/colusiones_en_contratacion_SIC.csv",
                event_type="collusion_case",
            ),
        ]

    def _slug_from_url(self, url: str) -> str:
        stem = Path(url).stem
        slug = normalize_text(stem).lower()
        slug = re.sub(r"[^a-z0-9]+", "_", slug).strip("_")
        return slug or "paco_source"

    def _select_sources(self, *, include_disabled: bool, source_keys: list[str] | None) -> list[PacoSourceSpec]:
        candidates = self.sources if include_disabled else [spec for spec in self.sources if spec.enabled]
        if not source_keys:
            return candidates
        wanted = {normalize_text(key) for key in source_keys if key}
        return [spec for spec in candidates if normalize_text(spec.key) in wanted or normalize_text(spec.name) in wanted]

    def _ensure_source_file(self, spec: PacoSourceSpec, refresh: bool) -> tuple[Path, str]:
        suffix = ".zip" if spec.url.lower().endswith(".zip") else Path(spec.url).suffix or ".csv"
        raw_path = self.raw_dir / f"{spec.key}{suffix}"
        if raw_path.exists() and not refresh:
            return raw_path, self._sha256_file(raw_path)

        request = Request(spec.url, headers={"User-Agent": "PAE-Risk-Tracker/0.1", "Accept": "*/*"})
        try:
            with urlopen(request, timeout=120) as response:
                payload = response.read()
        except (HTTPError, URLError) as exc:
            if raw_path.exists():
                logger.warning("Using cached PACO source for %s after download error: %s", spec.key, exc)
                return raw_path, self._sha256_file(raw_path)
            raise

        raw_path.write_bytes(payload)
        return raw_path, hashlib.sha256(payload).hexdigest()

    def _read_tabular_frame(self, path: Path, spec: PacoSourceSpec | None = None) -> pd.DataFrame:
        if path.suffix.lower() == ".zip":
            return self._read_zipped_frame(path, spec=spec)
        return self._read_delimited_bytes(path.read_bytes(), source_name=path.name)

    def _read_zipped_frame(self, path: Path, spec: PacoSourceSpec | None = None) -> pd.DataFrame:
        with zipfile.ZipFile(path) as archive:
            names = [name for name in archive.namelist() if not name.endswith("/")]
            if not names:
                return pd.DataFrame()
            candidate = next((name for name in names if Path(name).suffix.lower() in {".csv", ".txt", ".tsv", ".dat"}), names[0])
            with archive.open(candidate) as handle:
                payload = handle.read()
        header = None if spec and spec.key in {"disciplinary", "contractual"} else "infer"
        return self._read_delimited_bytes(payload, source_name=path.name, header=header)

    def _read_delimited_bytes(self, payload: bytes, source_name: str, header: str | int | None = "infer") -> pd.DataFrame:
        text = self._decode_text(payload)
        if not text.strip():
            return pd.DataFrame()

        delimiter = self._detect_delimiter(text)
        candidates = [delimiter, ",", ";", "|", "\t"]
        seen: set[str] = set()
        for candidate in candidates:
            if candidate in seen:
                continue
            seen.add(candidate)
            try:
                read_kwargs = dict(
                    sep=candidate,
                    dtype=str,
                    keep_default_na=False,
                    engine="python",
                    on_bad_lines="skip",
                )
                if header != "infer":
                    read_kwargs["header"] = header
                frame = pd.read_csv(io.StringIO(text), **read_kwargs)
            except Exception:
                continue
            if frame.shape[1] > 1 or candidate == candidates[-1]:
                return frame
        try:
            read_kwargs = dict(
                dtype=str,
                keep_default_na=False,
                engine="python",
                on_bad_lines="skip",
            )
            if header != "infer":
                read_kwargs["header"] = header
            return pd.read_csv(io.StringIO(text), **read_kwargs)
        except Exception:
            logger.warning("Could not parse PACO source %s as a tabular file.", source_name)
            return pd.DataFrame()

    def _normalize_source_frame(self, frame: pd.DataFrame, spec: PacoSourceSpec) -> pd.DataFrame:
        if frame.empty:
            return self._empty_pack_frame()

        if spec.key == "disciplinary":
            return self._normalize_disciplinary_frame(frame, spec)
        if spec.key == "contractual":
            return self._normalize_contractual_frame(frame, spec)

        records: list[dict[str, Any]] = []
        for idx, row in frame.reset_index(drop=True).iterrows():
            row_dict = self._row_to_dict(row)
            subject_name = self._first_non_empty(row_dict, self._subject_name_candidates(spec))
            subject_doc = normalize_digits(self._first_non_empty(row_dict, self._subject_doc_candidates(spec)))
            entity_name = self._first_non_empty(row_dict, self._entity_name_candidates(spec))
            entity_doc = normalize_digits(self._first_non_empty(row_dict, self._entity_doc_candidates(spec)))
            department = self._first_non_empty(row_dict, self._department_candidates(spec))
            municipality = self._first_non_empty(row_dict, self._municipality_candidates(spec))
            reference = self._first_non_empty(row_dict, self._reference_candidates(spec))
            status = self._first_non_empty(row_dict, self._status_candidates(spec))
            description = self._build_description(row_dict, spec)
            event_date = self._resolve_date(row_dict, spec)
            amount = self._resolve_amount(row_dict)
            record_id = self._first_non_empty(
                row_dict,
                [
                    "id",
                    "ID",
                    "No.",
                    "No",
                    "RADICADO",
                    "radicado",
                    "CASO",
                    "caso",
                    "IDENTIFICADOR",
                    "identificador",
                    "NUMERO",
                ],
            )
            search_text = normalize_text(
                " ".join(
                    str(value)
                    for value in [
                        spec.key,
                        spec.name,
                        spec.event_type,
                        subject_name,
                        subject_doc,
                        entity_name,
                        entity_doc,
                        department,
                        municipality,
                        reference,
                        description,
                        status,
                    ]
                    if value
                )
            )
            source_row_hash = hashlib.sha256(json.dumps(row_dict, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
            records.append(
                {
                    "source_key": spec.key,
                    "source_name": spec.name,
                    "source_url": spec.url,
                    "family": spec.family,
                    "event_type": spec.event_type,
                    "record_id": record_id or f"{spec.key}-{idx + 1}",
                    "subject_name": subject_name,
                    "subject_doc": subject_doc,
                    "entity_name": entity_name,
                    "entity_doc": entity_doc,
                    "department": department,
                    "municipality": municipality,
                    "reference": reference,
                    "description": description,
                    "event_date": event_date,
                    "amount": amount,
                    "status": status,
                    "search_text": search_text,
                    "source_row_hash": source_row_hash,
                }
            )

        normalized = pd.DataFrame(records)
        normalized = normalized.drop_duplicates(subset=["source_key", "source_row_hash"], keep="first").reset_index(drop=True)
        normalized["event_date"] = pd.to_datetime(normalized["event_date"], errors="coerce")
        normalized["amount"] = pd.to_numeric(normalized["amount"], errors="coerce")
        return normalized

    def _normalize_disciplinary_frame(self, frame: pd.DataFrame, spec: PacoSourceSpec) -> pd.DataFrame:
        df = frame.reset_index(drop=True).copy()
        if df.shape[1] < 20:
            return self._normalize_source_frame(df, PacoSourceSpec(key="fallback", name=spec.name, url=spec.url, family=spec.family, event_type=spec.event_type, enabled=spec.enabled))

        subject_name = (
            self._series_at(df, 6)
            .fillna("")
            .astype(str)
            .str.cat(self._series_at(df, 7).fillna("").astype(str), sep=" ")
            .str.cat(self._series_at(df, 8).fillna("").astype(str), sep=" ")
            .str.cat(self._series_at(df, 9).fillna("").astype(str), sep=" ")
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
        subject_doc = self._series_at(df, 5).fillna("").astype(str).map(normalize_digits)
        entity_name = self._series_at(df, 21).fillna("").astype(str)
        department = self._series_at(df, 11).fillna("").astype(str)
        municipality = self._series_at(df, 12).fillna("").astype(str)
        reference = self._series_at(df, 20).fillna("").astype(str)
        status = self._series_at(df, 13).fillna("").astype(str)
        description = (
            self._series_at(df, 13).fillna("").astype(str)
            .str.cat(self._series_at(df, 17).fillna("").astype(str), sep=" ")
            .str.cat(self._series_at(df, 18).fillna("").astype(str), sep=" ")
            .str.cat(self._series_at(df, 19).fillna("").astype(str), sep=" ")
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
        event_date = pd.to_datetime(self._series_at(df, 19), errors="coerce")
        amount = pd.Series([pd.NA] * len(df), index=df.index, dtype="object")
        record_id = self._series_at(df, 0).fillna("").astype(str)
        search_text = (
            spec.key
            + " "
            + spec.name
        )
        pack = pd.DataFrame(
            {
                "source_key": spec.key,
                "source_name": spec.name,
                "source_url": spec.url,
                "family": spec.family,
                "event_type": spec.event_type,
                "record_id": record_id.where(record_id.str.strip().ne(""), other=pd.Series(record_id.index.astype(str), index=record_id.index)),
                "subject_name": subject_name,
                "subject_doc": subject_doc,
                "entity_name": entity_name,
                "entity_doc": "",
                "department": department,
                "municipality": municipality,
                "reference": reference,
                "description": description,
                "event_date": event_date,
                "amount": amount,
                "status": status,
            }
        )
        pack["search_text"] = (
            pack["source_key"].fillna("").astype(str)
            + " "
            + pack["source_name"].fillna("").astype(str)
            + " "
            + pack["subject_name"].fillna("").astype(str)
            + " "
            + pack["subject_doc"].fillna("").astype(str)
            + " "
            + pack["entity_name"].fillna("").astype(str)
            + " "
            + pack["department"].fillna("").astype(str)
            + " "
            + pack["municipality"].fillna("").astype(str)
            + " "
            + pack["reference"].fillna("").astype(str)
            + " "
            + pack["description"].fillna("").astype(str)
            + " "
            + pack["status"].fillna("").astype(str)
        ).map(normalize_text)
        pack["source_row_hash"] = pd.util.hash_pandas_object(df, index=False).astype("uint64").astype(str)
        pack = pack.drop_duplicates(subset=["source_key", "source_row_hash"], keep="first").reset_index(drop=True)
        return pack[self._empty_pack_frame().columns]

    def _normalize_contractual_frame(self, frame: pd.DataFrame, spec: PacoSourceSpec) -> pd.DataFrame:
        df = frame.reset_index(drop=True).copy()
        if df.shape[1] < 11:
            return self._normalize_source_frame(df, PacoSourceSpec(key="fallback", name=spec.name, url=spec.url, family=spec.family, event_type=spec.event_type, enabled=spec.enabled))

        entity_name = self._series_at(df, 0).fillna("").astype(str)
        entity_doc = self._series_at(df, 1).fillna("").astype(str).map(normalize_digits)
        department = self._series_at(df, 2).fillna("").astype(str)
        municipality = self._series_at(df, 3).fillna("").astype(str)
        description = (
            self._series_at(df, 4).fillna("").astype(str)
            .str.cat(self._series_at(df, 13).fillna("").astype(str), sep=" ")
            .str.cat(self._series_at(df, 14).fillna("").astype(str), sep=" ")
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
        subject_doc = self._series_at(df, 5).fillna("").astype(str).map(normalize_digits)
        subject_name = self._series_at(df, 6).fillna("").astype(str)
        reference = self._series_at(df, 7).fillna("").astype(str)
        amount = pd.to_numeric(self._series_at(df, 8), errors="coerce")
        event_date = pd.to_datetime(self._series_at(df, 9), errors="coerce")
        status = self._series_at(df, 13).fillna("").astype(str)
        record_id = reference.where(reference.str.strip().ne(""), other=self._series_at(df, 10).fillna("").astype(str))

        pack = pd.DataFrame(
            {
                "source_key": spec.key,
                "source_name": spec.name,
                "source_url": spec.url,
                "family": spec.family,
                "event_type": spec.event_type,
                "record_id": record_id.where(record_id.str.strip().ne(""), other=pd.Series(record_id.index.astype(str), index=record_id.index)),
                "subject_name": subject_name,
                "subject_doc": subject_doc,
                "entity_name": entity_name,
                "entity_doc": entity_doc,
                "department": department,
                "municipality": municipality,
                "reference": reference,
                "description": description,
                "event_date": event_date,
                "amount": amount,
                "status": status,
            }
        )
        pack["search_text"] = (
            pack["source_key"].fillna("").astype(str)
            + " "
            + pack["source_name"].fillna("").astype(str)
            + " "
            + pack["subject_name"].fillna("").astype(str)
            + " "
            + pack["subject_doc"].fillna("").astype(str)
            + " "
            + pack["entity_name"].fillna("").astype(str)
            + " "
            + pack["entity_doc"].fillna("").astype(str)
            + " "
            + pack["department"].fillna("").astype(str)
            + " "
            + pack["municipality"].fillna("").astype(str)
            + " "
            + pack["reference"].fillna("").astype(str)
            + " "
            + pack["description"].fillna("").astype(str)
            + " "
            + pack["status"].fillna("").astype(str)
        ).map(normalize_text)
        pack["source_row_hash"] = pd.util.hash_pandas_object(df, index=False).astype("uint64").astype(str)
        pack = pack.drop_duplicates(subset=["source_key", "source_row_hash"], keep="first").reset_index(drop=True)
        return pack[self._empty_pack_frame().columns]

    def _combine_frames(self, frames: list[pd.DataFrame]) -> pd.DataFrame:
        if not frames:
            return self._empty_pack_frame()
        combined = pd.concat(frames, ignore_index=True, sort=False)
        if combined.empty:
            return self._empty_pack_frame()
        combined = combined.drop_duplicates(subset=["source_key", "source_row_hash"], keep="first").reset_index(drop=True)
        return combined

    def _series_at(self, frame: pd.DataFrame, index: int) -> pd.Series:
        if index >= frame.shape[1]:
            return pd.Series([""] * len(frame), index=frame.index)
        return frame.iloc[:, index]

    def _combine_cached_frames(self) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for path in sorted(self.processed_dir.glob("paco_*.parquet")):
            if path.name == f"{PACO_PACK_TABLE}.parquet":
                continue
            try:
                frame = pd.read_parquet(path)
            except Exception:
                continue
            if frame.empty:
                continue
            frames.append(frame)
        return self._combine_frames(frames)

    def _empty_pack_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "source_key",
                "source_name",
                "source_url",
                "family",
                "event_type",
                "record_id",
                "subject_name",
                "subject_doc",
                "entity_name",
                "entity_doc",
                "department",
                "municipality",
                "reference",
                "description",
                "event_date",
                "amount",
                "status",
                "search_text",
                "source_row_hash",
            ]
        )

    def _pack_name(self) -> str:
        payload = load_pack_registry()
        return str(payload.get("pack_name", "pae_data_pack"))

    def _decode_text(self, payload: bytes) -> str:
        for encoding in ("utf-8-sig", "utf-8", "latin-1"):
            try:
                return payload.decode(encoding)
            except UnicodeDecodeError:
                continue
        return payload.decode("utf-8", errors="replace")

    def _detect_delimiter(self, text: str) -> str:
        sample = "\n".join(text.splitlines()[:10])
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "|", "\t"])
            return str(dialect.delimiter)
        except Exception:
            if "\t" in sample:
                return "\t"
            if sample.count(";") > sample.count(","):
                return ";"
            return ","

    def _sha256_file(self, path: Path) -> str:
        hasher = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _row_to_dict(self, row: pd.Series) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in row.items():
            if value is None:
                result[key] = None
                continue
            try:
                if pd.isna(value):
                    result[key] = None
                    continue
            except Exception:
                pass
            result[key] = value
        return result

    def _first_non_empty(self, row: dict[str, Any], candidates: Iterable[str]) -> str:
        for candidate in candidates:
            for key, value in row.items():
                if normalize_text(key) != normalize_text(candidate):
                    continue
                text = "" if value is None else str(value).strip()
                if text:
                    return text
        return ""

    def _subject_name_candidates(self, spec: PacoSourceSpec) -> list[str]:
        candidates = [
            "NOMBRES Y APELLIDOS",
            "NOMBRES Y APELLIDOS DEL SANCIONADO",
            "NOMBRES",
            "NOMBRE",
            "APELLIDOS",
            "PERSONA SANCIONADA",
            "PERSONAS SANCIONADAS",
            "RESPONSABLE FISCAL",
            "CONTRATISTA",
            "PROVEEDOR",
            "NOMBRE DEL SANCIONADO",
            "NOMBRE COMPLETO",
            "TITULO",
        ]
        if spec.key == "collusion":
            candidates = ["PERSONAS SANCIONADAS", "IDENTIFICACION", "CASO"] + candidates
        if spec.key == "fiscal":
            candidates = ["RESPONSABLE FISCAL"] + candidates
        if spec.key == "contractual":
            candidates = ["CONTRATISTA", "PROVEEDOR", "NOMBRE PROVEEDOR"] + candidates
        return candidates

    def _subject_doc_candidates(self, spec: PacoSourceSpec) -> list[str]:
        candidates = [
            "NUMERO DOCUMENTO",
            "NUMERO DE DOCUMENTO",
            "TIPO Y NUM DOCUEMENTO",
            "TIPO Y NUM DOCUMENTO",
            "IDENTIFICACION",
            "CEDULA",
            "NIT",
            "DOCUMENTO",
            "AS_CODIGO_PROVEEDOR_OBJETO",
        ]
        if spec.key == "fiscal":
            candidates = ["TIPO Y NUM DOCUEMENTO", "NIT", "IDENTIFICACION"] + candidates
        return candidates

    def _entity_name_candidates(self, spec: PacoSourceSpec) -> list[str]:
        candidates = [
            "ENTIDAD",
            "ENTIDAD AFECTADA",
            "ENTIDAD COMPRADORA",
            "NOMBRE ENTIDAD",
            "NOMBRE_ENTIDAD",
            "ENTIDAD CREADORA",
            "NOMBRE ENTIDAD CREADORA",
            "ENTE QUE REPORTA",
            "MUNICIPIO",
            "MPIO",
        ]
        if spec.key == "penal":
            candidates = ["MUNICIPIO", "MPIO", "DEPARTAMENTO"] + candidates
        return candidates

    def _entity_doc_candidates(self, spec: PacoSourceSpec) -> list[str]:
        candidates = [
            "NIT ENTIDAD",
            "NIT",
            "CODIGO ENTIDAD",
            "CODIGO ENTIDAD CREADORA",
            "ENTIDAD_AFECTADA_NIT",
        ]
        if spec.key == "contractual":
            candidates = ["NIT", "CODIGO ENTIDAD CREADORA"] + candidates
        return candidates

    def _department_candidates(self, spec: PacoSourceSpec) -> list[str]:
        return [
            "DEPARTAMENTO",
            "DEPARTAMENTO_AFECTADO",
            "DEPARTAMENTO CREADOR",
            "DEPARTAMENTO_ENTIDAD",
            "UBICACION",
        ]

    def _municipality_candidates(self, spec: PacoSourceSpec) -> list[str]:
        return [
            "MUNICIPIO",
            "MUNICIPIO_ID",
            "MPIO",
            "CIUDAD",
            "LOCALIDAD",
        ]

    def _reference_candidates(self, spec: PacoSourceSpec) -> list[str]:
        candidates = [
            "RADICADO",
            "CASO",
            "FALTA QUE ORIGINA LA SANCION",
            "RESOLUCION DE APERTURA",
            "RESOLUCION DE SANCION",
            "TR",
            "R",
            "REFERENCIA",
            "REFERENCIA PROCESO",
            "CONTRATO",
            "ID",
            "TITULO",
            "ARTICULO",
            "CAPITULO",
        ]
        if spec.key == "contractual":
            candidates = ["REFERENCIA", "CONTRATO", "RESOLUCION", "PROVEEDOR"] + candidates
        return candidates

    def _status_candidates(self, spec: PacoSourceSpec) -> list[str]:
        return [
            "TIPO DE SANCION",
            "TIPO SANCION",
            "TIPO",
            "ESTADO",
            "CLASE",
            "CATEGORIA",
        ]

    def _build_description(self, row: dict[str, Any], spec: PacoSourceSpec) -> str:
        if spec.key == "penal":
            parts = [
                self._first_non_empty(row, ["TITULO"]),
                self._first_non_empty(row, ["CAPITULO"]),
                self._first_non_empty(row, ["ARTICULO"]),
                self._first_non_empty(row, ["ANO_ACTUACION"]),
            ]
            return " | ".join(part for part in parts if part)

        if spec.key == "collusion":
            parts = [
                self._first_non_empty(row, ["FALTA QUE ORIGINA LA SANCION"]),
                self._first_non_empty(row, ["CASO"]),
                self._first_non_empty(row, ["RESOLUCION DE SANCION"]),
            ]
            return " | ".join(part for part in parts if part)

        if spec.key == "fiscal":
            parts = [
                self._first_non_empty(row, ["RESPONSABLE FISCAL"]),
                self._first_non_empty(row, ["ENTIDAD AFECTADA"]),
                self._first_non_empty(row, ["ENTE QUE REPORTA"]),
            ]
            return " | ".join(part for part in parts if part)

        if spec.key == "contractual":
            parts = [
                self._first_non_empty(row, ["TIPO DE SANCION"]),
                self._first_non_empty(row, ["CONTRATISTA"]),
                self._first_non_empty(row, ["REFERENCIA"]),
                self._first_non_empty(row, ["DESCRIPCION"]),
            ]
            return " | ".join(part for part in parts if part)

        parts = [
            self._first_non_empty(row, ["TITULO"]),
            self._first_non_empty(row, ["CAPITULO"]),
            self._first_non_empty(row, ["ARTICULO"]),
            self._first_non_empty(row, ["DESCRIPCION"]),
        ]
        return " | ".join(part for part in parts if part)

    def _resolve_date(self, row: dict[str, Any], spec: PacoSourceSpec) -> Any:
        date_candidates = [
            "FECHA",
            "FECHA_ACTUACION",
            "FECHA DE ACTUACION",
            "FECHA REGISTRO",
            "FECHAREGISTRO",
            "FECHA_RADICACION",
            "FECHA DE RADICACION",
            "FECHA DE SANCION",
            "FECHASANCION",
            "FECHA DE CREACION",
            "FECHA_CREACION",
            "FECHA FACTURA",
            "FECHA",
        ]
        for candidate in date_candidates:
            value = self._first_non_empty(row, [candidate])
            if value:
                parsed = pd.to_datetime(value, errors="coerce")
                if pd.notna(parsed):
                    return parsed

        year_value = self._first_non_empty(row, ["ANO_ACTUACION", "ANO", "YEAR", "VIGENCIA"])
        if year_value:
            year_digits = normalize_digits(year_value)
            if year_digits:
                try:
                    return pd.Timestamp(year=int(year_digits[:4]), month=1, day=1)
                except Exception:
                    return pd.NaT
        return pd.NaT

    def _resolve_amount(self, row: dict[str, Any]) -> Any:
        amount_candidates = [
            "VALOR",
            "VALOR INICIAL",
            "MULTA INICIAL",
            "MONTO",
            "CUANTIA",
            "VALOR DE LA MULTA",
            "VALOR SANCION",
        ]
        for candidate in amount_candidates:
            value = self._first_non_empty(row, [candidate])
            if value:
                digits = normalize_digits(value)
                if digits:
                    try:
                        return float(digits)
                    except Exception:
                        continue
        return pd.NA


def load_paco_pack(store: DuckDBStore, *, refresh: bool = False, include_disabled: bool = False, source_keys: list[str] | None = None) -> PacoPackSummary:
    loader = DataPackLoader(store)
    return loader.sync_paco(refresh=refresh, include_disabled=include_disabled, source_keys=source_keys)
