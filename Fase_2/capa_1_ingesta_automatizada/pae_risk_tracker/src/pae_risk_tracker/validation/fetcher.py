from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.robotparser import RobotFileParser

def _normalize_domain(value: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if "://" not in text:
        text = f"https://{text}"
    parsed = urlparse(text)
    return (parsed.hostname or "").lower().strip(".")


def _is_html_content(content_type: str, payload: bytes) -> bool:
    if "html" in (content_type or "").lower():
        return True
    preview = payload[:200].lstrip().lower()
    return preview.startswith(b"<!doctype html") or preview.startswith(b"<html") or b"<body" in preview


def _content_suffix(content_type: str, payload: bytes) -> str:
    content_type = (content_type or "").lower()
    if "json" in content_type:
        return ".json"
    if "html" in content_type or _is_html_content(content_type, payload):
        return ".html"
    if "text/plain" in content_type or content_type.startswith("text/"):
        return ".txt"
    return ".bin"


class _MetadataParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_title = False
        self.title_parts: list[str] = []
        self.description = ""
        self.h1_parts: list[str] = []
        self._in_h1 = False
        self._meta_seen = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag == "title":
            self.in_title = True
            return
        if tag == "h1":
            self._in_h1 = True
            return
        if tag != "meta" or self._meta_seen:
            return
        attr_map = {key.lower(): (value or "") for key, value in attrs}
        name = attr_map.get("name", "").lower()
        if name in {"description", "og:description"} and attr_map.get("content"):
            self.description = attr_map["content"]
            self._meta_seen = True

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag == "title":
            self.in_title = False
        if tag == "h1":
            self._in_h1 = False

    def handle_data(self, data: str) -> None:
        text = data.strip()
        if not text:
            return
        if self.in_title:
            self.title_parts.append(text)
        elif self._in_h1 and not self.h1_parts:
            self.h1_parts.append(text)


def _decode_payload(payload: bytes, content_type: str) -> str:
    charset = ""
    if "charset=" in content_type.lower():
        charset = content_type.lower().split("charset=", 1)[1].split(";", 1)[0].strip()
    encodings = [charset] if charset else []
    encodings.extend(["utf-8-sig", "utf-8", "latin-1"])
    for encoding in encodings:
        if not encoding:
            continue
        try:
            return payload.decode(encoding, errors="replace")
        except Exception:
            continue
    return payload.decode("utf-8", errors="replace")


def _extract_excerpt(text: str, limit: int = 1000) -> str:
    compact = re.sub(r"\s+", " ", text).strip()
    return compact[:limit]


def _compact_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _extract_html_metadata(payload: bytes, content_type: str) -> tuple[str, str, str]:
    text = _decode_payload(payload, content_type)
    parser = _MetadataParser()
    try:
        parser.feed(text)
    except Exception:
        pass
    title = " ".join(parser.title_parts).strip()
    description = parser.description.strip()
    if not description and parser.h1_parts:
        description = " ".join(parser.h1_parts).strip()
    stripped = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", text)
    stripped = re.sub(r"(?s)<[^>]+>", " ", stripped)
    excerpt = _extract_excerpt(unescape(stripped))
    return title, description, excerpt


@dataclass(frozen=True)
class ValidationFetchResult:
    url: str
    domain: str
    status: str
    robots_status: str
    http_status: int | None
    content_type: str
    fetched_at: str
    byte_count: int
    content_hash: str
    title: str
    description: str
    text_excerpt: str
    snapshot_suffix: str
    error_message: str = ""
    payload: bytes = b""

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record.pop("payload", None)
        return record


class ValidationFetcher:
    def __init__(
        self,
        allow_domains: set[str] | list[str] | tuple[str, ...],
        *,
        timeout_seconds: int = 20,
        user_agent: str = "PAE-Risk-Tracker/0.1 (+validation)",
    ) -> None:
        self.allow_domains = {_normalize_domain(domain) for domain in allow_domains if _normalize_domain(domain)}
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent
        self._robots_cache: dict[str, RobotFileParser | None] = {}

    def is_allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False
        domain = (parsed.hostname or "").lower().strip(".")
        return any(domain == allowed or domain.endswith(f".{allowed}") for allowed in self.allow_domains)

    def fetch(self, url: str) -> ValidationFetchResult:
        parsed = urlparse(url)
        domain = (parsed.hostname or "").lower().strip(".")
        if parsed.scheme not in {"http", "https"} or not domain or not self.is_allowed(url):
            return ValidationFetchResult(
                url=url,
                domain=domain,
                status="blocked_domain",
                robots_status="unknown",
                http_status=None,
                content_type="",
                fetched_at=self._now(),
                byte_count=0,
                content_hash="",
                title="",
                description="",
                text_excerpt="",
                snapshot_suffix=".txt",
                error_message="Domain or scheme not allowed by validation policy.",
            )

        robots_status = self._robots_status(url)
        if robots_status == "disallowed":
            return ValidationFetchResult(
                url=url,
                domain=domain,
                status="blocked_robots",
                robots_status=robots_status,
                http_status=None,
                content_type="",
                fetched_at=self._now(),
                byte_count=0,
                content_hash="",
                title="",
                description="",
                text_excerpt="",
                snapshot_suffix=".txt",
                error_message="robots.txt disallows fetching this URL.",
            )

        request = Request(
            url,
            headers={
                "User-Agent": self.user_agent,
                "Accept": "text/html,application/json,text/plain,*/*;q=0.8",
            },
        )
        payload = b""
        http_status: int | None = None
        content_type = ""
        error_message = ""
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                http_status = int(getattr(response, "status", 200) or 200)
                content_type = str(response.headers.get_content_type() or "")
                payload = response.read()
        except HTTPError as exc:
            http_status = int(getattr(exc, "code", 0) or 0) or None
            content_type = str(getattr(exc.headers, "get_content_type", lambda: "")() if exc.headers else "")
            try:
                payload = exc.read() or b""
            except Exception:
                payload = b""
            error_message = f"HTTP {exc.code}: {exc.reason}"
        except URLError as exc:
            error_message = str(exc.reason or exc)
            return ValidationFetchResult(
                url=url,
                domain=domain,
                status="error",
                robots_status=robots_status,
                http_status=http_status,
                content_type=content_type,
                fetched_at=self._now(),
                byte_count=0,
                content_hash="",
                title="",
                description="",
                text_excerpt="",
                snapshot_suffix=".txt",
                error_message=error_message,
            )
        except Exception as exc:
            return ValidationFetchResult(
                url=url,
                domain=domain,
                status="error",
                robots_status=robots_status,
                http_status=http_status,
                content_type=content_type,
                fetched_at=self._now(),
                byte_count=0,
                content_hash="",
                title="",
                description="",
                text_excerpt="",
                snapshot_suffix=".txt",
                error_message=str(exc),
            )

        content_hash = hashlib.sha256(payload).hexdigest() if payload else ""
        suffix = _content_suffix(content_type, payload)
        title = ""
        description = ""
        excerpt = ""
        if payload:
            if _is_html_content(content_type, payload):
                title, description, excerpt = _extract_html_metadata(payload, content_type)
            else:
                decoded = _decode_payload(payload, content_type)
                excerpt = _extract_excerpt(decoded)
                if not description and content_type.lower().startswith("application/json"):
                    description = "JSON endpoint fetched for validation."

        status = "fetched" if http_status and 200 <= http_status < 400 else "error"
        return ValidationFetchResult(
            url=url,
            domain=domain,
            status=status,
            robots_status=robots_status,
            http_status=http_status,
            content_type=content_type,
            fetched_at=self._now(),
            byte_count=len(payload),
            content_hash=content_hash,
            title=_compact_text(title),
            description=_compact_text(description),
            text_excerpt=_compact_text(excerpt),
            snapshot_suffix=suffix,
            error_message=error_message,
            payload=payload,
        )

    def _robots_status(self, url: str) -> str:
        parsed = urlparse(url)
        domain = (parsed.hostname or "").lower().strip(".")
        if not domain:
            return "unknown"
        if domain in self._robots_cache:
            parser = self._robots_cache[domain]
            if parser is None:
                return "unknown"
            return "allowed" if parser.can_fetch(self.user_agent, url) else "disallowed"

        robots_url = f"{parsed.scheme}://{domain}/robots.txt"
        parser = RobotFileParser()
        parser.set_url(robots_url)
        try:
            request = Request(robots_url, headers={"User-Agent": self.user_agent, "Accept": "text/plain,*/*;q=0.5"})
            with urlopen(request, timeout=self.timeout_seconds) as response:
                payload = response.read()
            text = _decode_payload(payload, str(getattr(response.headers, "get_content_type", lambda: "text/plain")()))
            parser.parse(text.splitlines())
        except Exception:
            self._robots_cache[domain] = None
            return "unknown"
        self._robots_cache[domain] = parser
        try:
            return "allowed" if parser.can_fetch(self.user_agent, url) else "disallowed"
        except Exception:
            return "unknown"

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()
