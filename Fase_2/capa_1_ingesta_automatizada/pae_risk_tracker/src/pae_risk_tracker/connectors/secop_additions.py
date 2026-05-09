from __future__ import annotations

from .secop_contracts import SocrataDataset


def additions_dataset() -> SocrataDataset:
    return SocrataDataset("cb9c-h8sn", "SECOP II - Adiciones")

