# Roadmap Fase 2

Este documento define la secuencia prioritaria para la Fase 2 del MVP.

La información del proyecto es de **Alejandro Montes**.

## Principio rector

No dispersar la solución en múltiples rutas de navegación. La página principal `Fase_2/capa_4_score_riesgo_explicable/Info/index.html` es la columna vertebral del MVP y todos los módulos futuros deben adaptarse a su contrato de datos.

## Orden de implementación

### 1. Interfaz principal

- Crear y mantener una sola entrada visual.
- Mostrar encabezado, búsqueda, filtros, tabla, panel de contrato, reporte, dashboard, red flags, conexiones y chatbot.
- Usar mock data cuando no exista backend.

### 2. Priorización de contratos

- Ordenar por score de riesgo descendente.
- Desempatar por red flags, adiciones, valor y pluralidad de oferentes.
- Mantener visible el filtro obligatorio de departamento.

### 3. Selección y detalle

- Permitir seleccionar un contrato desde la tabla.
- Generar automáticamente el reporte estructurado.
- Renderizar métricas, red flags, patrones y conexiones.

### 4. Chatbot contextual

- Activar el asistente solo cuando haya contrato seleccionado.
- Responder únicamente con base en el contrato visible y el informe generado.
- Devolver límites y faltantes cuando no haya suficiente información.

### 5. Conexión a backend

- Reemplazar mock data por API cuando exista.
- Mantener el mismo contrato de salida para no romper la interfaz.
- Cargar bajo demanda los detalles pesados, no al inicio.

### 6. Enriquecimiento y agentes

- Conectar un agente LLM real para preguntas sobre el informe.
- Integrar fuentes públicas externas.
- Agregar trazabilidad avanzada sin cambiar el flujo principal.

## Dependencias

### Puede hacerse con mock data

- Búsqueda.
- Filtros.
- Tabla priorizada.
- Panel de contrato.
- Reporte estructurado.
- Dashboard básico.
- Red flags.
- Patrones.
- Conexiones de ejemplo.
- Chatbot local.

### Requiere datos reales

- Comparaciones confiables entre contratos similares.
- Frecuencia real de proveedor y entidad.
- Conexiones externas verificables.
- Valores atípicos con contexto histórico.

### Requiere backend

- `GET /api/contracts`
- `GET /api/contracts/{id}`
- `GET /api/contracts/{id}/report`
- `GET /api/contracts/{id}/risk`
- `GET /api/contracts/{id}/connections`
- `POST /api/chat`

### Requiere agente LLM

- Explicación sencilla del informe.
- Respuestas sobre red flags y patrones.
- Síntesis ejecutiva.
- Sugerencias de revisión.
- Detección de faltantes sin inventar datos.

## Reglas de integración

- No crear una segunda interfaz principal.
- No mover la conversación a una vista inicial por fuera del reporte.
- No inventar evidencia.
- No afirmar corrupción.
- No romper el contrato de salida del módulo principal.

## Entregables esperados por fase

1. Base visual navegable.
2. Mock data temporal.
3. Reporte estructurado.
4. Dashboard por contrato.
5. Chatbot contextual.
6. API real.
7. Enlaces externos.
8. Agente de IA más robusto.
