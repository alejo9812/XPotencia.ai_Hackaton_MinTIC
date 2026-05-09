# PAE Risk Tracker

Pipeline tecnico para detectar senales de alerta y priorizar revision documental en contratos PAE.

Este subproyecto contiene la base Python del MVP:

- cliente Socrata y carga incremental;
- normalizacion y persistencia local en DuckDB / Parquet;
- cruces con adiciones y contexto PACO;
- motor de scoring deterministico;
- ranking explicable;
- fichas de auditoria;
- indice de busqueda;
- API FastAPI;
- agente de consulta.
- servicio de chat-first con memoria minima de sesion;
- vistas dinamicas para contratos, red flags, reportes y seguimiento.

La guia de arquitectura por capas vive en `../README.md`. Desde ahi se llega a las 4 carpetas del primer nivel de `Fase_2/`, una por cada capa, con su propio `README.md` y PDF.

## Contrato canonico de salida

La integracion de cierre usa como contrato compartido:

- `config/export_contract.json`
- `data/outputs/pae_risk_ranking.csv`
- `data/outputs/pae_risk_ranking.json`
- `data/outputs/pae_audit_cards.json`

## Campos principales

- `contract_id`
- `process_id`
- `entity`
- `entity_nit`
- `supplier`
- `supplier_nit`
- `department`
- `municipality`
- `object`
- `modality`
- `status`
- `initial_value`
- `final_value`
- `start_date`
- `end_date`
- `year`
- `month`
- `risk_score`
- `risk_level`
- `red_flags`
- `evidence`
- `secop_url`
- `recommended_action`
- `limitations`

## API

La API interna expone al menos:

- `GET /health`
- `GET /contracts`
- `GET /contracts/{contract_id}`
- `GET /contracts/{contract_id}/risk`
- `GET /reports/high-risk`
- `POST /agent/query`
- `GET /chat/bootstrap`
- `POST /chat/respond`
- `GET /records/search`
- `GET /validation/latest`
- `GET /validation/contracts/{contract_id}`
- `GET /diagnostics/process`

## Chat-first y memoria

El modulo `src/pae_risk_tracker/chat_service.py` actua como la capa de
orquestacion del chat dinamico. Sus responsabilidades son:

- clasificar la intencion del usuario;
- extraer contrato, proveedor, entidad y filtros relevantes;
- consultar caches, indices y la base DuckDB;
- construir una respuesta estructurada con `intent`, `message`, `view_type`,
  `data`, `suggested_actions` y `limitations`;
- actualizar la memoria minima de sesion.

La memoria conserva:

- ultimo contrato consultado;
- ultimo proveedor consultado;
- ultima entidad consultada;
- ultimos filtros aplicados;
- ultimo tipo de reporte solicitado;
- contratos marcados para seguimiento.

Cuando no hay contexto suficiente, el chat pide el dato faltante en vez de
inventarlo.

## Como ejecutar

Recomendado: Python 3.10 o superior.

```powershell
cd Fase_2\pae_risk_tracker
$env:PYTHONPATH="src"
py -3 -m pae_risk_tracker.cli discover-schema
py -3 -m pae_risk_tracker.cli ingest --years 2023 2024 2025 --sample-limit 500 --per-year-limit 100
py -3 -m pae_risk_tracker.cli load-paco
py -3 -m pae_risk_tracker.cli score
py -3 -m pae_risk_tracker.cli materialize-index
py -3 -m pae_risk_tracker.cli diagnose-process
py -3 -m uvicorn pae_risk_tracker.api.server:app --reload --port 8000
```

En paralelo, puedes levantar la experiencia chat-first del frontend con:

```powershell
node ..\Info\server.mjs
```

Ese servidor expone `http://localhost:4175` y sirve tambien la vista clasica
`dashboard-opacidad-pae.html`.

El comando `diagnose-process` genera un paquete de diagnostico con casos reales priorizados y casos sinteticos de guia en `data/outputs/process_diagnostics.json` y `data/outputs/process_diagnostic_cases.csv`.

Si ya tienes el parquet enriquecido, puedes regenerar solo los outputs canonicos con:

```powershell
py -3 Fase_2\pae_risk_tracker\scripts\score_contracts.py
```

## Pruebas

```powershell
cd Fase_2\pae_risk_tracker
$env:PYTHONPATH="src"
py -3 -m pytest
```

## Nota de integracion

El sistema solo marca riesgo, red flags y limitaciones de trazabilidad.
No afirma corrupcion ni reemplaza la revision documental humana.
