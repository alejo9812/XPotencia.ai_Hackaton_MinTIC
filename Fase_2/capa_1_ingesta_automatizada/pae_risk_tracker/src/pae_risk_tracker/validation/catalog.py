from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

import pandas as pd
import pyarrow.parquet as pq

from ..config import load_dataset_registry, load_pack_registry, normalize_text
from ..paths import CACHE_DIR, VALIDATION_REPORT_DIR, ensure_runtime_dirs
from ..storage.duckdb_store import DuckDBStore


SOURCE_CATALOG_RUNS_TABLE = "source_catalog_runs"
SOURCE_CATALOG_CHECKS_TABLE = "source_catalog_checks"
PACO_MANIFEST_PATH = CACHE_DIR / "paco" / "paco_pack_manifest.json"
PACO_PAGE_URL = "https://portal.paco.gov.co/index.php?pagina=descargarDatos"


@dataclass(frozen=True)
class SourceCatalogCheck:
    family: str
    source_key: str
    source_name: str
    status: str
    local_reference: str
    official_reference: str
    local_url: str
    official_url: str
    http_status: int | None
    content_hash: str
    local_hash: str
    rows_local: int | None
    rows_manifest: int | None
    raw_path: str
    processed_path: str
    checked_at: str
    notes: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SourceCatalogRunSummary:
    run_id: str
    created_at: str
    secop_source_count: int
    paco_source_count: int
    local_pack_source_count: int
    matched_count: int
    review_count: int
    missing_count: int
    error_count: int
    report_path: str
    manifest_path: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SourceCatalogReport:
    summary: SourceCatalogRunSummary
    checks: list[SourceCatalogCheck]

    def to_dict(self) -> dict[str, Any]:
        return {
            "summary": self.summary.to_dict(),
            "checks": [check.to_dict() for check in self.checks],
        }


class _HrefParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []
        self.text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for key, value in attrs:
            if key.lower() == "href" and value:
                self.hrefs.append(value)

    def handle_data(self, data: str) -> None:
        text = data.strip()
        if text:
            self.text_parts.append(text)


class SourceCatalogVerifier:
    def __init__(
        self,
        store: DuckDBStore,
        *,
        dataset_registry: dict[str, Any] | None = None,
        pack_registry: dict[str, Any] | None = None,
        report_dir: Path | None = None,
        manifest_path: Path | None = None,
        timeout_seconds: int = 30,
        user_agent: str = "PAE-Risk-Tracker/0.1 (+source-verification)",
    ) -> None:
        ensure_runtime_dirs()
        self.store = store
        self.dataset_registry = dataset_registry or load_dataset_registry()
        self.pack_registry = pack_registry or load_pack_registry()
        self.report_dir = Path(report_dir or VALIDATION_REPORT_DIR)
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_path = Path(manifest_path or PACO_MANIFEST_PATH)
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent

    def run(self) -> SourceCatalogReport:
        run_id = self._new_run_id()
        paco_source_count = len([entry for entry in self.pack_registry.get("sources", []) if bool(entry.get("enabled", True))])
        secop_checks = self._verify_secop_sources()
        paco_checks = self._verify_paco_sources()
        local_checks = self._verify_local_paco_manifest()
        checks = [*secop_checks, *paco_checks, *local_checks]
        summary = self._build_summary(run_id, checks, len(secop_checks), paco_source_count, len(local_checks))
        report = SourceCatalogReport(summary=summary, checks=checks)
        self._persist(report)
        return report

    def _verify_secop_sources(self) -> list[SourceCatalogCheck]:
        checks: list[SourceCatalogCheck] = []
        for entry in self.dataset_registry.get("datasets", []):
            if not bool(entry.get("active", True)):
                continue
            dataset_id = str(entry.get("id", "")).strip()
            if not dataset_id:
                continue
            local_name = str(entry.get("name", "")).strip()
            local_url = f"https://www.datos.gov.co/{dataset_id}"
            official_url = f"https://www.datos.gov.co/api/views/{dataset_id}.json"
            fetch_result = self._fetch(official_url)
            status = "error"
            official_reference = ""
            notes: list[str] = []
            rows_local: int | None = None
            rows_manifest: int | None = None
            if fetch_result.get("ok") and fetch_result.get("payload"):
                payload = self._decode_json(fetch_result["payload"])
                official_reference = str(payload.get("permalink") or payload.get("url") or official_url)
                official_id = str(payload.get("id") or "").strip()
                official_name = str(payload.get("name") or "").strip()
                id_match = official_id == dataset_id
                name_match = normalize_text(official_name) == normalize_text(local_name)
                if id_match:
                    status = "matched"
                else:
                    status = "mismatch"
                    notes.append(f"official id {official_id or 'missing'} differs from local id {dataset_id}")
                if not name_match:
                    notes.append(f"official name='{official_name}' local name='{local_name}'")
                if not official_reference:
                    official_reference = official_url
            else:
                notes.append(str(fetch_result.get("error") or "No se pudo leer la metadata oficial"))
            checks.append(
                SourceCatalogCheck(
                    family="secop",
                    source_key=str(entry.get("key", dataset_id)),
                    source_name=local_name,
                    status=status,
                    local_reference=f"{dataset_id} | {local_name}",
                    official_reference=official_reference,
                    local_url=local_url,
                    official_url=official_url,
                    http_status=fetch_result.get("http_status"),
                    content_hash=str(fetch_result.get("content_hash") or ""),
                    local_hash="",
                    rows_local=rows_local,
                    rows_manifest=rows_manifest,
                    raw_path="",
                    processed_path="",
                    checked_at=self._now(),
                    notes="; ".join(notes).strip(),
                )
            )
        return checks

    def _verify_paco_sources(self) -> list[SourceCatalogCheck]:
        pack_sources = [entry for entry in self.pack_registry.get("sources", []) if bool(entry.get("enabled", True))]
        if not pack_sources:
            return []

        fetch_result = self._fetch(PACO_PAGE_URL)
        page_hrefs: set[str] = set()
        page_text = ""
        if fetch_result.get("ok") and fetch_result.get("payload"):
            parsed_hrefs, page_text = self._extract_hrefs_and_text(fetch_result["payload"], PACO_PAGE_URL)
            page_hrefs = parsed_hrefs

        checks: list[SourceCatalogCheck] = []
        for entry in pack_sources:
            source_url = str(entry.get("url", "")).strip()
            source_key = str(entry.get("key", source_url)).strip()
            source_name = str(entry.get("name", source_key)).strip()
            source_basename = Path(urlparse(source_url).path).name
            normalized_page_hrefs = {self._normalize_url(href) for href in page_hrefs}
            normalized_source_url = self._normalize_url(source_url)
            found_on_page = normalized_source_url in normalized_page_hrefs or any(
                source_basename and source_basename in href for href in page_hrefs
            )
            notes: list[str] = []
            if not fetch_result.get("ok"):
                status = "error"
                notes.append(str(fetch_result.get("error") or "No se pudo leer la pagina PACO"))
            elif found_on_page:
                status = "matched"
                notes.append("URL oficial encontrada en el portal PACO")
            else:
                status = "missing"
                notes.append("La URL configurada no aparecio en los enlaces del portal PACO")

            checks.append(
                SourceCatalogCheck(
                    family="paco",
                    source_key=source_key,
                    source_name=source_name,
                    status=status,
                    local_reference=source_url,
                    official_reference=PACO_PAGE_URL,
                    local_url=source_url,
                    official_url=PACO_PAGE_URL,
                    http_status=fetch_result.get("http_status"),
                    content_hash=str(fetch_result.get("content_hash") or ""),
                    local_hash="",
                    rows_local=None,
                    rows_manifest=None,
                    raw_path="",
                    processed_path="",
                    checked_at=self._now(),
                    notes="; ".join(notes).strip(),
                )
            )
        if page_text:
            checks.append(
                SourceCatalogCheck(
                    family="paco_page",
                    source_key="download_page",
                    source_name="PACO - Bases de datos",
                    status="matched" if fetch_result.get("ok") else "error",
                    local_reference=PACO_PAGE_URL,
                    official_reference=PACO_PAGE_URL,
                    local_url=PACO_PAGE_URL,
                    official_url=PACO_PAGE_URL,
                    http_status=fetch_result.get("http_status"),
                    content_hash=str(fetch_result.get("content_hash") or ""),
                    local_hash="",
                    rows_local=None,
                    rows_manifest=None,
                    raw_path="",
                    processed_path="",
                    checked_at=self._now(),
                    notes=page_text[:240],
                )
            )
        return checks

    def _verify_local_paco_manifest(self) -> list[SourceCatalogCheck]:
        if not self.manifest_path.exists():
            return [
                SourceCatalogCheck(
                    family="local_pack",
                    source_key="paco_manifest",
                    source_name="PACO manifest",
                    status="missing",
                    local_reference=str(self.manifest_path),
                    official_reference=str(self.manifest_path),
                    local_url="",
                    official_url="",
                    http_status=None,
                    content_hash="",
                    local_hash="",
                    rows_local=None,
                    rows_manifest=None,
                    raw_path="",
                    processed_path="",
                    checked_at=self._now(),
                    notes="No existe el manifiesto local del pack PACO.",
                )
            ]

        payload = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        checks: list[SourceCatalogCheck] = []
        for entry in payload.get("sources", []):
            raw_path = Path(str(entry.get("raw_path", "")))
            processed_path = Path(str(entry.get("processed_parquet", "")))
            expected_hash = str(entry.get("sha256", ""))
            expected_rows = int(entry.get("rows_normalized") or 0)
            local_hash = self._sha256_file(raw_path) if raw_path.exists() else ""
            rows_local = self._parquet_rows(processed_path) if processed_path.exists() else None
            hash_match = bool(expected_hash and local_hash and expected_hash == local_hash)
            rows_match = rows_local == expected_rows if rows_local is not None else False
            status = "matched" if hash_match and rows_match else "review"
            notes: list[str] = []
            if not raw_path.exists():
                status = "missing"
                notes.append("Falta el archivo crudo local.")
            elif not hash_match:
                notes.append("El hash local no coincide con el del manifiesto.")
            if processed_path.exists():
                if rows_local is not None and rows_local != expected_rows:
                    notes.append(f"Filas parquet={rows_local} vs manifiesto={expected_rows}.")
            else:
                status = "missing"
                notes.append("Falta el parquet procesado local.")
            checks.append(
                SourceCatalogCheck(
                    family="local_pack",
                    source_key=str(entry.get("key", raw_path.stem)),
                    source_name=str(entry.get("name", raw_path.stem)),
                    status=status,
                    local_reference=str(raw_path),
                    official_reference=str(entry.get("url", "")),
                    local_url=str(entry.get("url", "")),
                    official_url=str(entry.get("url", "")),
                    http_status=None,
                    content_hash="",
                    local_hash=local_hash,
                    rows_local=rows_local,
                    rows_manifest=expected_rows,
                    raw_path=str(raw_path),
                    processed_path=str(processed_path),
                    checked_at=self._now(),
                    notes="; ".join(notes).strip(),
                )
            )
        return checks

    def _build_summary(
        self,
        run_id: str,
        checks: list[SourceCatalogCheck],
        secop_count: int,
        paco_count: int,
        local_pack_count: int,
    ) -> SourceCatalogRunSummary:
        matched_count = sum(1 for check in checks if check.status == "matched")
        review_count = sum(1 for check in checks if check.status == "review")
        missing_count = sum(1 for check in checks if check.status == "missing")
        error_count = sum(1 for check in checks if check.status == "error")
        report_path = self.report_dir / f"{run_id}.json"
        return SourceCatalogRunSummary(
            run_id=run_id,
            created_at=self._now(),
            secop_source_count=secop_count,
            paco_source_count=paco_count,
            local_pack_source_count=local_pack_count,
            matched_count=matched_count,
            review_count=review_count,
            missing_count=missing_count,
            error_count=error_count,
            report_path=str(report_path),
            manifest_path=str(self.manifest_path),
        )

    def _persist(self, report: SourceCatalogReport) -> None:
        summary_frame = pd.DataFrame([report.summary.to_dict()])
        checks_frame = pd.DataFrame([check.to_dict() for check in report.checks])
        if self.store.has_table(SOURCE_CATALOG_RUNS_TABLE):
            self.store.append_frame(SOURCE_CATALOG_RUNS_TABLE, summary_frame)
        else:
            self.store.write_frame(SOURCE_CATALOG_RUNS_TABLE, summary_frame, replace=True)
        if self.store.has_table(SOURCE_CATALOG_CHECKS_TABLE):
            self.store.append_frame(SOURCE_CATALOG_CHECKS_TABLE, checks_frame)
        else:
            self.store.write_frame(SOURCE_CATALOG_CHECKS_TABLE, checks_frame, replace=True)
        Path(report.summary.report_path).write_text(json.dumps(report.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")

    def _fetch(self, url: str) -> dict[str, Any]:
        request = Request(url, headers={"User-Agent": self.user_agent, "Accept": "application/json,text/html,*/*;q=0.8"})
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                payload = response.read()
                content_type = str(response.headers.get_content_type() or "")
                return {
                    "ok": True,
                    "http_status": int(getattr(response, "status", 200) or 200),
                    "content_type": content_type,
                    "payload": payload,
                    "content_hash": hashlib.sha256(payload).hexdigest() if payload else "",
                    "error": "",
                }
        except HTTPError as exc:
            payload = b""
            try:
                payload = exc.read() or b""
            except Exception:
                payload = b""
            return {
                "ok": False,
                "http_status": int(getattr(exc, "code", 0) or 0) or None,
                "content_type": str(getattr(exc.headers, "get_content_type", lambda: "")() if exc.headers else ""),
                "payload": payload,
                "content_hash": hashlib.sha256(payload).hexdigest() if payload else "",
                "error": f"HTTP {exc.code}: {exc.reason}",
            }
        except URLError as exc:
            return {
                "ok": False,
                "http_status": None,
                "content_type": "",
                "payload": b"",
                "content_hash": "",
                "error": str(exc.reason or exc),
            }
        except Exception as exc:
            return {
                "ok": False,
                "http_status": None,
                "content_type": "",
                "payload": b"",
                "content_hash": "",
                "error": str(exc),
            }

    def _decode_json(self, payload: bytes) -> dict[str, Any]:
        for encoding in ("utf-8-sig", "utf-8", "latin-1"):
            try:
                return json.loads(payload.decode(encoding))
            except Exception:
                continue
        return json.loads(payload.decode("utf-8", errors="replace"))

    def _extract_hrefs_and_text(self, payload: bytes, base_url: str) -> tuple[set[str], str]:
        parser = _HrefParser()
        text = payload.decode("utf-8", errors="replace")
        try:
            parser.feed(text)
        except Exception:
            pass
        hrefs = {self._normalize_url(urljoin(base_url, href)) for href in parser.hrefs if href}
        page_text = " ".join(parser.text_parts)
        return hrefs, page_text

    def _parquet_rows(self, path: Path) -> int | None:
        try:
            return int(pq.ParquetFile(path).metadata.num_rows)
        except Exception:
            try:
                return int(len(pd.read_parquet(path)))
            except Exception:
                return None

    def _sha256_file(self, path: Path) -> str:
        hasher = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _normalize_url(self, url: str) -> str:
        parsed = urlparse(str(url).strip())
        netloc = (parsed.netloc or "").lower().strip(".")
        path = parsed.path.rstrip("/")
        query = parsed.query.strip()
        normalized = f"{parsed.scheme.lower()}://{netloc}{path}"
        if query:
            normalized = f"{normalized}?{query}"
        return normalized

    def _new_run_id(self) -> str:
        return datetime.now(timezone.utc).strftime("source-catalog-%Y%m%d-%H%M%S-%f")

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()
