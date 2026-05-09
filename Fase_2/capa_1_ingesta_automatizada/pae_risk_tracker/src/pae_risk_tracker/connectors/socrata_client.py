from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import quote_plus, urljoin
from urllib.request import Request, build_opener

from ..storage.cache import JsonCache


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SocrataQueryResult:
    url: str
    rows: list[dict[str, Any]]
    status_code: int
    from_cache: bool


class SocrataClient:
    def __init__(
        self,
        domain: str = "www.datos.gov.co",
        app_token: str | None = None,
        timeout_seconds: int = 30,
        max_retries: int = 4,
        backoff_seconds: float = 0.75,
        cache: JsonCache | None = None,
        opener: Any | None = None,
    ) -> None:
        self.domain = domain.strip() or "www.datos.gov.co"
        self.app_token = (app_token or "").strip() or None
        self.timeout_seconds = timeout_seconds
        self.max_retries = max(1, max_retries)
        self.backoff_seconds = backoff_seconds
        self.cache = cache
        self.opener = opener or build_opener()

    def build_url(self, dataset_id: str, params: dict[str, Any] | None = None, path: str = "resource") -> str:
        base = f"https://{self.domain}/"
        endpoint = f"{path.rstrip('/')}/{dataset_id}.json"
        filtered = {key: value for key, value in (params or {}).items() if value not in (None, "", [], {})}
        if self.app_token:
            filtered.setdefault("$$app_token", self.app_token)
        if not filtered:
            return urljoin(base, endpoint)
        query = "&".join(
            f"{quote_plus(str(key), safe='$')}={quote_plus(str(value))}"
            for key, value in filtered.items()
        )
        return urljoin(base, endpoint) + f"?{query}"

    def get_metadata(self, dataset_id: str) -> dict[str, Any]:
        return self._get_json(f"https://{self.domain}/api/views/{dataset_id}.json", cache_key=f"metadata:{dataset_id}")

    def query_rows(
        self,
        dataset_id: str,
        select: str | Iterable[str] | None = None,
        where: str | None = None,
        order: str | None = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> SocrataQueryResult:
        params: dict[str, Any] = {"$limit": int(limit), "$offset": int(offset)}
        if select:
            params["$select"] = ",".join(select) if not isinstance(select, str) else select
        if where:
            params["$where"] = where
        if order:
            params["$order"] = order

        url = self.build_url(dataset_id, params)
        cache_key = self.cache.key_for(url) if self.cache else None
        cached = self.cache.get(cache_key) if cache_key else None
        if cached is not None:
            return SocrataQueryResult(url=url, rows=cached, status_code=200, from_cache=True)

        rows, status_code = self._fetch_json(url)
        if self.cache and cache_key:
            self.cache.set(cache_key, rows)
        return SocrataQueryResult(url=url, rows=rows, status_code=status_code, from_cache=False)

    def iter_rows(
        self,
        dataset_id: str,
        select: str | Iterable[str] | None = None,
        where: str | None = None,
        order: str | None = None,
        page_size: int = 1000,
        max_rows: int | None = None,
    ) -> Iterable[SocrataQueryResult]:
        offset = 0
        seen = 0
        while True:
            limit = page_size if max_rows is None else min(page_size, max_rows - seen)
            if limit <= 0:
                break

            result = self.query_rows(dataset_id, select=select, where=where, order=order, limit=limit, offset=offset)
            yield result
            rows = result.rows
            batch = len(rows)
            if batch == 0:
                break
            offset += batch
            seen += batch
            if batch < limit:
                break

    def _get_json(self, url: str, cache_key: str | None = None) -> Any:
        if self.cache and cache_key:
            cached = self.cache.get(cache_key)
            if cached is not None:
                return cached
        rows, _ = self._fetch_json(url)
        if self.cache and cache_key:
            self.cache.set(cache_key, rows)
        return rows

    def _fetch_json(self, url: str) -> tuple[Any, int]:
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            request = Request(url, headers={"Accept": "application/json", "User-Agent": "PAE-Risk-Tracker/0.1"})
            try:
                if self.app_token:
                    request.add_header("X-App-Token", self.app_token)
                logger.info("Socrata request %s", url)
                with self.opener.open(request, timeout=self.timeout_seconds) as response:
                    raw = response.read().decode("utf-8")
                    payload = json.loads(raw)
                    status_code = int(getattr(response, "status", 200) or 200)
                    return self._normalize_payload(payload), status_code
            except HTTPError as exc:
                last_error = exc
                if exc.code == 429 and attempt < self.max_retries:
                    retry_after = int(exc.headers.get("Retry-After", "0") or 0)
                    sleep_for = retry_after if retry_after > 0 else self.backoff_seconds * attempt
                    time.sleep(sleep_for)
                    continue
                if 500 <= exc.code < 600 and attempt < self.max_retries:
                    time.sleep(self.backoff_seconds * attempt)
                    continue
                raise
            except (URLError, TimeoutError, json.JSONDecodeError) as exc:
                last_error = exc
                if attempt < self.max_retries:
                    time.sleep(self.backoff_seconds * attempt)
                    continue
                break
        if last_error:
            raise last_error
        raise RuntimeError("Socrata request failed without a captured error.")

    def _normalize_payload(self, payload: Any) -> Any:
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            if isinstance(payload.get("data"), list):
                return payload["data"]
            if isinstance(payload.get("results"), list):
                return payload["results"]
        return payload
