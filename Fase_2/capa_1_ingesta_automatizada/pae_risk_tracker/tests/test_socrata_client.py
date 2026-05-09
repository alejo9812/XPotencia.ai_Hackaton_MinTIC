from __future__ import annotations

import json
from dataclasses import dataclass

from pae_risk_tracker.connectors.socrata_client import SocrataClient
from pae_risk_tracker.storage.cache import JsonCache


@dataclass
class DummyResponse:
    payload: str
    status: int = 200

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return self.payload.encode("utf-8")


class DummyOpener:
    def __init__(self, response: DummyResponse):
        self.response = response
        self.requests = []

    def open(self, request, timeout=None):
        self.requests.append((request.full_url, timeout, dict(request.header_items())))
        return self.response


def test_build_url_and_query_rows(tmp_path):
    opener = DummyOpener(DummyResponse(json.dumps([{"id": 1}]), status=200))
    cache = JsonCache(tmp_path / "cache")
    client = SocrataClient(domain="www.datos.gov.co", app_token="token-123", cache=cache, opener=opener)

    result = client.query_rows("jbjy-vk9h", select=["id_contrato", "valor"], where="valor > 0", limit=5, offset=10, order="valor DESC")

    assert result.rows == [{"id": 1}]
    assert result.from_cache is False
    assert "jbjy-vk9h.json" in result.url
    assert "$select=id_contrato%2Cvalor" in result.url
    assert "$where=valor+%3E+0" in result.url
    assert "$offset=10" in result.url
    assert "$limit=5" in result.url
    assert any(key.lower() == "x-app-token" for key in opener.requests[0][2])


def test_client_uses_cache(tmp_path):
    opener = DummyOpener(DummyResponse(json.dumps([{"id": 1}]), status=200))
    cache = JsonCache(tmp_path / "cache")
    client = SocrataClient(cache=cache, opener=opener)

    first = client.query_rows("jbjy-vk9h", select="id_contrato", limit=1)
    second = client.query_rows("jbjy-vk9h", select="id_contrato", limit=1)

    assert first.from_cache is False
    assert second.from_cache is True
    assert len(opener.requests) == 1
