# Capa 2 - Motor de Reglas Cuantitativas

Agrupa 12 reglas estructurales basadas en tipologias UNODC/OEA y produce vectores de riesgo numericos.

Esta capa toma el contrato normalizado y lo convierte en senales cuantitativas que ayudan a priorizar revision documental.

## Proposito

El motor cuantitativo no decide culpabilidad ni reemplaza una auditoria. Su trabajo es detectar patrones estructurales y convertirlos en valores numericos comparables.

La version actual del codigo contiene una familia amplia de flags RF-01 a RF-37; esta documentacion los agrupa en 12 reglas de negocio para que el equipo las lea mas facil.

## Las 12 reglas

- Fraccionamiento de contratos.
- Sobreprecio o valor atipico frente a comparables.
- Baja competencia: pocos oferentes o pocas propuestas.
- Concentracion de proveedor o repeticion inusual en la misma entidad.
- Adiciones excesivas, prorrogas o incrementos desproporcionados.
- Objeto contractual generico o demasiado corto.
- Campos criticos incompletos o trazabilidad documental debil.
- Consistencia temporal dudosa entre firma, publicacion, inicio y fin.
- Valor por dia o intensidad de ejecucion atipicos.
- Polizas o garantias vencidas o ausentes, cuando se crucen fuentes complementarias.
- Mallas empresariales, consorcios repetidos o redes de proveedores relacionadas.
- Contexto PACO o antecedentes que refuerzan la alerta.

## Entradas

- Contrato normalizado desde la capa 1.
- Tablas de adiciones y contexto de PACO para enriquecer el analisis.
- Umbrales y pesos definidos en config/risk_flags.yml y config/scoring.yml.
- Estadisticos robustos como mediana, MAD, IQR y percentiles.

## Salidas

La salida principal es un frame puntuado con score, nivel y evidencias por contrato.

- risk_score -> valor numerico de 0 a 100.
- risk_level -> bajo, medio, alto o critico.
- risk_flags -> lista de flags activados con evidencia.
- risk_dimension_scores_json -> vector numerico por dimension.
- risk_summary y risk_limitations -> lectura corta para API y dashboard.

```text
score_base = suma de pesos por flags activados
score_final = min(100, score_base + ajustes_estadisticos)
```

## Piezas tecnicas

- risk/indicators.py -> calcula indicadores de apoyo y estadisticos base.
- risk/rules_engine.py -> evalua las reglas, arma flags y construye el score.
- risk/scoring.py -> expone helpers de score para frames y registros.
- risk/evidence.py -> define estructuras RiskFlag y RiskAssessment.
- config/risk_flags.yml -> catalogo de flags, pesos y dimensiones.
- config/scoring.yml -> bandas, caps y reglas de clasificacion.

## Como modificarla y probarla

- Cambiar pesos o caps en config/scoring.yml.
- Ajustar umbrales o familias de senales en config/risk_flags.yml.
- Agregar pruebas en tests/test_scoring.py y tests/test_opacity_criteria.py.
- Reejecutar el pipeline de scoring para validar el impacto de los cambios.
