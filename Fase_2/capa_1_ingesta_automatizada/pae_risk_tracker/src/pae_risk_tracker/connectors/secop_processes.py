from __future__ import annotations

from .secop_contracts import SocrataDataset


def processes_dataset() -> SocrataDataset:
    return SocrataDataset("p6dx-8zbt", "SECOP II - Procesos de Contratación")

