# Agent Integration Guide

Guía para que otros agentes de Codex, chats o módulos futuros se integren con la Fase 2 sin romper la ruta principal.

La información del proyecto es de **Alejandro Montes**.

## Regla principal

La interfaz principal `Fase_2/capa_4_score_riesgo_explicable/Info/index.html` es la ruta prioritaria de integración de la Fase 2.

Todos los agentes deben entregar resultados compatibles con esa interfaz. No deben inventar una segunda ruta de navegación ni mover el flujo fuera de la selección de contrato y su informe.

## Contrato de datos esperado

Los módulos futuros deben producir datos en estos bloques:

```json
{
  "contracts": [],
  "selected_contract": {},
  "report": {},
  "dashboard_metrics": {},
  "red_flags": [],
  "risk_patterns": [],
  "connections": [],
  "chatbot_context": {}
}
```

## Responsabilidades por bloque

### `contracts`

- Lista priorizada de contratos PAE.
- Debe incluir campos suficientes para búsqueda, filtros y orden por riesgo.

### `selected_contract`

- Contrato actualmente seleccionado.
- Fuente única para el panel de detalle y el reporte.

### `report`

- Resumen ejecutivo.
- Análisis técnico.
- Interpretación ciudadana.
- Recomendaciones.

### `dashboard_metrics`

- Score de riesgo.
- Número de red flags.
- Comparaciones.
- Concentración.
- Frecuencias.

### `red_flags`

- Alertas preliminares.
- Evidencia o dato detonante.
- Severidad.
- Recomendación.

### `risk_patterns`

- Patrones repetitivos o sospechosos.
- Explicación.
- Acción sugerida.

### `connections`

- Relaciones con otros contratos.
- Relaciones con proveedor, entidad o territorio.
- Fuentes externas sugeridas.

### `chatbot_context`

- Resumen del contrato.
- Hallazgos clave.
- Limitaciones.
- Preguntas sugeridas.

## Reglas de seguridad y calidad

- No afirmar corrupción.
- No inventar evidencia.
- No completar datos faltantes con supuestos.
- Si hay poca información, decirlo explícitamente.
- Mantener tono de alerta preliminar, no acusatorio.

## Cómo debe responder el chatbot

El chatbot solo debe usar:

- El contrato seleccionado.
- El reporte visible.
- Las red flags detectadas.
- Los patrones mostrados.
- Las conexiones disponibles.
- Las limitaciones del análisis.

Si falta el contrato seleccionado, la respuesta debe ser:

- `Primero selecciona un contrato para que pueda responder preguntas sobre su informe.`

Si la información es insuficiente, la respuesta debe ser:

- `No hay información suficiente en los datos cargados para responder con precisión.`

## Formato recomendado de salida para agentes

```json
{
  "selected_contract": {
    "id": "PAE-001",
    "department": "Cundinamarca",
    "municipality": "Soacha",
    "entity": "Entidad contratante de ejemplo",
    "supplier": "Proveedor de ejemplo",
    "object": "Prestación del servicio de alimentación escolar",
    "value": 1200000000,
    "date": "2025-03-15",
    "modality": "Licitación pública",
    "status": "En ejecución",
    "risk_score": 84,
    "risk_level": "Alto"
  },
  "report": {
    "executive_summary": "",
    "technical_analysis": "",
    "citizen_interpretation": "",
    "recommendations": []
  },
  "dashboard_metrics": {
    "risk_score": 84,
    "red_flag_count": 3,
    "value": 1200000000,
    "supplier_frequency": 4
  },
  "red_flags": [
    {
      "name": "Baja pluralidad de oferentes",
      "severity": "Alta",
      "evidence": "Solo se identificó un oferente en el proceso.",
      "recommendation": "Revisar documentos del proceso y actas de evaluación."
    }
  ],
  "risk_patterns": [
    {
      "name": "Proveedor repetido",
      "description": "El proveedor aparece en varios contratos similares.",
      "importance": "Media",
      "action": "Cruzar historial del proveedor."
    }
  ],
  "connections": [
    {
      "type": "Proveedor",
      "description": "Otros contratos asociados al mismo proveedor.",
      "source": "SECOP II",
      "url": ""
    }
  ],
  "chatbot_context": {
    "summary": "",
    "limitations": "",
    "questions": []
  }
}
```

## Lo que no debe hacer un agente

- No reordenar el flujo principal.
- No esconder el filtro obligatorio de departamento.
- No convertir el chatbot en la pantalla de inicio.
- No reemplazar el score de riesgo por texto libre.
- No devolver datos incompatibles con la tabla o el reporte.

## Integración futura

Cuando exista backend real, el contrato esperado es compatible con:

- `GET /api/contracts`
- `GET /api/contracts/{id}`
- `GET /api/contracts/{id}/report`
- `GET /api/contracts/{id}/risk`
- `GET /api/contracts/{id}/connections`
- `POST /api/chat`

## Resumen operativo para otros agentes

Si vas a trabajar sobre la Fase 2, tu salida debe poder enchufarse en la interfaz principal sin cambiar la navegación ni el orden del usuario.
