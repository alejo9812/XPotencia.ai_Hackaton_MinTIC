from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any


@dataclass(frozen=True)
class RiskFlag:
    code: str
    dimension: str
    label: str
    weight: int
    evidence: str
    source: str = "SECOP II"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RiskAssessment:
    contract_id: str
    risk_score: int
    risk_level: str
    flags: list[RiskFlag]
    summary: str
    limitations: str
    dimension_scores: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["flags"] = [flag.to_dict() for flag in self.flags]
        return payload

