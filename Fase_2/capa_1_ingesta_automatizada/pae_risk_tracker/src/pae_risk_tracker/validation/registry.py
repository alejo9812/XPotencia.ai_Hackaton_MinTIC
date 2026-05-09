from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from ..config import load_validation_registry
from ..paths import CONFIG_DIR


def _normalize_domain(value: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if "://" not in text:
        text = f"https://{text}"
    parsed = urlparse(text)
    hostname = (parsed.hostname or "").lower().strip(".")
    return hostname


@dataclass(frozen=True)
class ValidationSourceSpec:
    key: str
    name: str
    kind: str
    url: str
    domain: str
    enabled: bool = True
    notes: str = ""
    search_url_template: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ValidationRegistry:
    project: str
    allow_domains: tuple[str, ...]
    default_timeout_seconds: int
    default_user_agent: str
    sources: tuple[ValidationSourceSpec, ...]

    def active_sources(self) -> list[ValidationSourceSpec]:
        return [source for source in self.sources if source.enabled]

    def allowed_domain_set(self) -> set[str]:
        return {domain for domain in self.allow_domains if domain}

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["sources"] = [source.to_dict() for source in self.sources]
        return payload


def load_validation_registry_spec() -> ValidationRegistry:
    payload = load_validation_registry()
    sources: list[ValidationSourceSpec] = []
    for entry in payload.get("sources", []):
        url = str(entry.get("url", "")).strip()
        domain = str(entry.get("domain", "")).strip() or _normalize_domain(url)
        if not domain and url:
            domain = _normalize_domain(url)
        sources.append(
            ValidationSourceSpec(
                key=str(entry.get("key", "")) or _slug_from_url(url),
                name=str(entry.get("name", entry.get("key", url))) or url,
                kind=str(entry.get("kind", "portal_page")),
                url=url,
                domain=domain,
                enabled=bool(entry.get("enabled", True)),
                notes=str(entry.get("notes", "")),
                search_url_template=str(entry.get("search_url_template", "")),
            )
        )

    allow_domains = tuple(
        _normalize_domain(domain)
        for domain in payload.get("allow_domains", [])
        if _normalize_domain(domain)
    )
    default_timeout_seconds = int(payload.get("default_timeout_seconds", 20))
    default_user_agent = str(payload.get("default_user_agent", "PAE-Risk-Tracker/0.1 (+validation)"))
    project = str(payload.get("project", "PAE Risk Tracker"))
    return ValidationRegistry(
        project=project,
        allow_domains=allow_domains,
        default_timeout_seconds=default_timeout_seconds,
        default_user_agent=default_user_agent,
        sources=tuple(sources),
    )


def _slug_from_url(url: str) -> str:
    parsed = urlparse(url)
    stem = Path(parsed.path).stem or parsed.netloc or "validation_source"
    return "".join(char.lower() if char.isalnum() else "_" for char in stem).strip("_") or "validation_source"
