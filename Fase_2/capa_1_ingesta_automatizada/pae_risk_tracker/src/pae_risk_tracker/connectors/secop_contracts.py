from __future__ import annotations

from dataclasses import dataclass

from ..config import DatasetSpec


CONTRACTS_DATASET = DatasetSpec(
    key="core_contracts",
    id="jbjy-vk9h",
    name="SECOP II - Contratos Electrónicos",
    role="core",
    active=True,
    default_year_field="fecha_de_firma",
    text_fields=(
        "descripcion_del_proceso",
        "tipo_de_contrato",
        "modalidad_de_contratacion",
        "justificacion_modalidad_de",
        "condiciones_de_entrega",
        "proveedor_adjudicado",
        "nombre_entidad",
    ),
    id_fields={
        "contract": "id_contrato",
        "process": "proceso_de_compra",
        "entity_nit": "nit_entidad",
        "supplier_name": "proveedor_adjudicado",
        "supplier_doc": "documento_proveedor",
        "url_process": "urlproceso",
    },
)


@dataclass(frozen=True)
class SocrataDataset:
    dataset_id: str
    name: str


def core_dataset() -> SocrataDataset:
    return SocrataDataset(CONTRACTS_DATASET.id, CONTRACTS_DATASET.name)

