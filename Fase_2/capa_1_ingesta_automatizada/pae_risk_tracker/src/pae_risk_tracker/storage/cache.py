from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CacheEntry:
    key: str
    path: Path
    exists: bool


class JsonCache:
    def __init__(self, root: Path, ttl_seconds: int | None = None) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.ttl_seconds = ttl_seconds

    def key_for(self, value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def entry(self, key: str) -> CacheEntry:
        path = self.root / f"{key}.json"
        return CacheEntry(key=key, path=path, exists=path.exists())

    def get(self, key: str) -> Any | None:
        entry = self.entry(key)
        if not entry.exists:
            return None

        payload = json.loads(entry.path.read_text(encoding="utf-8"))
        if self.ttl_seconds is not None:
            created_at = float(payload.get("_cached_at", 0))
            if created_at and (self._now() - created_at) > self.ttl_seconds:
                return None
        return payload.get("value")

    def set(self, key: str, value: Any) -> Path:
        entry = self.entry(key)
        entry.path.write_text(
            json.dumps({"_cached_at": self._now(), "value": value}, ensure_ascii=False),
            encoding="utf-8",
        )
        return entry.path

    def _now(self) -> float:
        import time

        return time.time()

