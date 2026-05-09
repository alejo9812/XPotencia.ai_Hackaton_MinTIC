# Capa 1 - Ingesta Automatizada

Consume SECOP II, normaliza 67 campos al estandar OCDS y enriquece con el historial PACO.

Esta capa trae datos publicos, los limpia, los normaliza y los deja listos para analisis, trazabilidad y enriquecimiento.

## Proposito

La capa 1 convierte datos dispersos de SECOP II en un contrato canonico que pueda ser consultado por reglas, LLM y dashboard.

Tambien agrega contexto historico de PACO para que el equipo tenga una vista mas completa del contratista y de la trazabilidad documental.

- Consumir API publica SECOP II en tiempo real o por lotes controlados.
- Normalizar los campos relevantes al mapa canonico del proyecto y al estandar OCDS.
- Enriquecer cada contrato con antecedentes, packs y tablas de contexto PACO.

## Entradas

- SECOP II - contratos, procesos y adiciones desde Socrata.
- Paquetes PACO con contexto contractual, fiscal, disciplinario, penal y colusion.
- Archivos raw de muestra y manifests para trabajo local sin depender siempre de la red.
- Catalogos de columnas, datasets y keywords para reconocer sinonimos y variantes de nombre.

## Salidas y artefactos

- data/raw/ -> insumos descargados y muestras de trabajo.
- data/processed/pae_contracts_core.parquet -> contrato base normalizado.
- data/processed/pae_contracts_enriched.parquet -> contrato con contexto PACO y trazabilidad.
- data/processed/paco/*.parquet -> tablas PACO listas para cruces.
- data/cache/ -> cache tecnico, esquemas y manifests de trazabilidad.
- data/duckdb/pae_risk_tracker.duckdb -> base local compartida por toda la solucion.

## Piezas tecnicas

- connectors/socrata_client.py -> cliente de consultas a Datos Abiertos.
- connectors/secop_contracts.py, secop_processes.py y secop_additions.py -> descarga y mapeo de las fuentes.
- ingestion/schema_normalizer.py -> resuelve alias de columnas, convierte fechas y numeros, y arma search_blob.
- ingestion/incremental_loader.py -> carga incremental con deduplicacion y control de lote.
- ingestion/data_pack_loader.py -> integra packs locales y contexto PACO.
- storage/duckdb_store.py -> persistencia analitica local.

## Reglas de normalizacion

- Resolver sinonimos de columnas antes de escribir el contrato canonico.
- Coercionar numeros, fechas y IDs para evitar tipos inconsistentes.
- Construir un search_blob que permita busqueda textual y clasificacion PAE.
- Eliminar duplicados por contract_id o process_id antes de persistir.
- Clasificar coincidencias PAE por confianza alta, media o baja.
- Cruzar adiciones con el contrato principal para enriquecer el historial.

## Como extenderla

- Agregar o ajustar datasets en config/datasets.yml.
- Afinar aliases y columnas en config/columns.yml.
- Revisar keywords de PAE si cambia el nicho o el lenguaje contractual.
- Reejecutar discover-schema, ingest, load-paco y score para refrescar los artefactos.
