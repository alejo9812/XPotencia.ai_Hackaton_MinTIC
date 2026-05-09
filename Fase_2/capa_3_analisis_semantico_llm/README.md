# Capa 3 - Analisis Semantico LLM

Claude Sonnet analiza campos textuales, encuentra inconsistencias y devuelve explicaciones trazables en lenguaje natural.

Esta capa interpreta el texto del contrato para explicar por que el caso merece revision, sin inventar datos ni reemplazar la evidencia estructurada.

## Proposito

La capa semantica lee el objeto contractual, las justificaciones, las condiciones y la evidencia recuperada para producir una explicacion clara y auditable.

Su rol es traducir hallazgos tecnicos a lenguaje natural, no redefinir el score ni emitir conclusiones legales.

## Que analiza

- Objeto contractual y descripciones extensas.
- Justificaciones de modalidad y condiciones de entrega.
- Textos genericos, repeticiones y frases sospechosamente vagas.
- Inconsistencias entre el relato textual y los campos estructurados.
- Preguntas de auditoria que ayuden a revisar el contrato con criterio humano.

## Contrato de salida

- analysis.summary -> resumen corto para el usuario.
- analysis.explanation -> explicacion trazable en lenguaje natural.
- analysis.recommendations -> acciones sugeridas.
- analysis.audit_questions -> preguntas para revision documental.
- analysis.limitations -> advertencias de cobertura o calidad de datos.
- provider, model y prompt_version -> trazabilidad del proveedor LLM usado.

## Arquitectura de ejecucion

El flujo clasico es: consulta del usuario -> plan estructurado -> recuperacion de evidencia -> validacion local -> analisis LLM -> respuesta final.

```text
user query -> plan -> evidence rows -> validation context -> Claude Sonnet / mock -> structured analysis
```

## Reglas de uso del LLM

- No inventar hechos fuera de la evidencia recuperada.
- No reemplazar el score cuantitativo ni la regla deterministica.
- No emitir juicios legales; solo priorizar y explicar.
- Preferir una respuesta corta, clara y con soporte documental.
- Usar el proveedor configurado o el modo mock para mantener el contrato estable.

## Piezas tecnicas

- agent/orchestrator.py -> arma la corrida del agente y conecta evidencia con LLM.
- agent/tools.py -> planifica la consulta y selecciona filas de evidencia.
- agent/prompts.py -> define el contrato de prompting y la politica de salida.
- agent/knowledge.py -> recupera conocimiento de reglas y criterios.
- agent/llm_client.py -> abstrae Claude Sonnet o el proveedor configurado.
- response_builder.py -> construye respuestas estables para chat y API.

## Como extenderla

- Ajustar el prompt si cambia el tono, el idioma o el nivel de detalle esperado.
- Agregar herramientas nuevas en agent/tools.py si hace falta mas contexto.
- Ampliar knowledge.py con criterios o explicaciones reutilizables.
- Mantener pruebas para no perder trazabilidad al cambiar el proveedor LLM.
