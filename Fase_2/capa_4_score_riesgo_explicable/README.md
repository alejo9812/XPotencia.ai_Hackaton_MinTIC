# Fase 2 - Mapa General y Capa 4

`Fase_2/` queda organizado solamente en cuatro carpetas visibles:

- `capa_1_ingesta_automatizada/`
- `capa_2_motor_reglas_cuantitativas/`
- `capa_3_analisis_semantico_llm/`
- `capa_4_score_riesgo_explicable/`

Este README funciona como puerta de entrada general y, al mismo tiempo, documenta la capa 4. Si alguien nuevo llega al proyecto, este es el orden recomendado:

1. Leer la capa 1 para entender de donde salen los datos.
2. Leer la capa 2 para entender como se calcula el riesgo numerico.
3. Leer la capa 3 para entender como se explica el contrato con LLM.
4. Leer esta capa para ver el score final, el dashboard y la salida explicable.

## Entrada principal del proyecto

- HTML unico para abrir la experiencia completa: `Fase_2/capa_4_score_riesgo_explicable/Info/index.html`
- Servidor local del dashboard: `Fase_2/capa_4_score_riesgo_explicable/Info/server.mjs`
- Script para levantar el flujo completo: `Fase_2/capa_4_score_riesgo_explicable/run_phase2.ps1`

## Capa 4 - Score de Riesgo Explicable

Combina la senal numerica de la capa 2 con la explicacion de la capa 3 para producir un score interpretable.

Esta capa entrega el numero final, la banda de riesgo, la evidencia y la recomendacion que consume el API, el dashboard y los reportes.

## Proposito

La capa 4 es la capa de consumo: recibe el vector de riesgo numerico, lo contextualiza con la explicacion LLM y lo convierte en una ficha clara para priorizacion.

El objetivo no es castigar contratos sino ordenar la revision y dejar claro por que un caso quedo arriba en la lista.

## Formula de trabajo

El score final se apoya en la suma de pesos, caps por dimension y ajustes estadisticos. La implementacion actual lo limita a 100 y clasifica en bandas de lectura.

```text
score_base = suma de pesos por flags activados
score_final = min(100, score_base + ajustes_estadisticos)
bandas = bajo | medio | alto | critico
```

## Entradas

- risk_flags y risk_dimension_scores_json desde la capa 2.
- analysis.summary, analysis.explanation y audit_questions desde la capa 3.
- validation context, trazabilidad y evidencias de contrato.
- Parametros de exportacion y contrato canonico de salida.

## Salidas

- RiskAssessment por contrato con score, nivel, flags y limitaciones.
- pae_risk_ranking.csv / json para ranking y exportacion.
- pae_audit_cards.json con fichas de revision.
- Respuesta de API para dashboard, chat y reportes.
- Indicadores agregados para vistas ejecutivas y diagnosticos.

## Piezas tecnicas

- risk/scoring.py -> expone el score para frames y registros.
- risk/evidence.py -> estructura la ficha explicable.
- response_builder.py -> estandariza la respuesta para chat y API.
- api/routes_contracts.py -> publica contratos, detalle y riesgo.
- api/routes_diagnostics.py -> expone diagnosticos y validacion.
- data/outputs/ -> guarda ranking, cards y diagnosticos finales.

## No objetivos

- No es una sentencia legal ni una prueba de corrupcion.
- No sustituye revision humana ni auditoria juridica.
- No debe ocultar la evidencia que explica el score.

## Como extenderla

- Cambiar bandas de riesgo en config/scoring.yml si el equipo necesita otra lectura.
- Agregar nuevos campos de salida en el contrato canonico si el dashboard lo pide.
- Actualizar export_contract.json cuando cambie la forma de entregar resultados.
- Revisar las pruebas de scoring y de API cuando se mueva la formula.
