# README Fase 2

La Fase 2 del proyecto **Agente de IA para Detección de Opacidad en Contratos Públicos PAE** concentra la experiencia principal en una sola interfaz HTML tipo dashboard.

La información del proyecto es de **Alejandro Montes**.

## Ruta prioritaria del MVP

La ruta que manda el diseño de esta fase es:

1. Abrir la interfaz principal.
2. Buscar contratos PAE por ID, objeto, entidad, proveedor, municipio, departamento o palabra clave.
3. Filtrar obligatoriamente por departamento.
4. Ver una tabla de contratos sugeridos ordenada por score de riesgo.
5. Seleccionar un contrato.
6. Cargar el reporte, el dashboard y las alertas del contrato.
7. Revisar red flags, patrones riesgosos y conexiones.
8. Consultar al chatbot contextual al final de la página.

La entrada canónica del MVP es:

- `Fase_2/capa_4_score_riesgo_explicable/Info/index.html`
- Servida en local por `Fase_2/capa_4_score_riesgo_explicable/Info/server.mjs`

La vista clásica de soporte sigue existiendo en:

- `Fase_2/capa_4_score_riesgo_explicable/Info/dashboard-opacidad-pae.html`

## Qué resuelve esta interfaz

- Busca contratos públicos PAE.
- Prioriza contratos con mayor score de riesgo.
- Permite filtrar por departamento, municipio, entidad, proveedor, modalidad, año, score y valor.
- Muestra un panel del contrato seleccionado.
- Construye un reporte estructurado.
- Muestra un dashboard liviano y extensible.
- Expone red flags, patrones riesgosos y conexiones.
- Cierra con un chatbot contextual que solo responde sobre el contrato seleccionado y los datos visibles.

## Estados y comportamiento

- Si no hay contrato seleccionado, la vista muestra estados vacíos explícitos.
- Si no hay backend conectado, la interfaz usa mock data temporal.
- Si falta información, el chatbot lo dice de forma directa y no inventa.
- Si un módulo futuro todavía no existe, la interfaz deja placeholders listos para integrarlo.

## Bloques de integración obligatorios

Los demás agentes o módulos deben entregar su información en estas estructuras:

1. `contracts`
2. `selected_contract`
3. `report`
4. `dashboard_metrics`
5. `red_flags`
6. `risk_patterns`
7. `connections`
8. `chatbot_context`

## Archivos clave

- `README_FASE_2.md`: resumen operativo de la fase.
- `ROADMAP_FASE_2.md`: orden de implementación y dependencias.
- `INTERFACE_SPEC.md`: contrato de la interfaz principal.
- `AGENT_INTEGRATION_GUIDE.md`: guía para otros agentes.
- `Info/index.html`: interfaz principal.
- `Info/css/dashboard-opacidad-pae.css`: estilos del dashboard.
- `Info/js/dashboard-opacidad-pae.js`: lógica principal.
- `Info/js/contractDetail.js`: construcción del reporte, dashboard y conexiones.
- `Info/js/table.js`: tabla priorizada de contratos.
- `Info/js/filters.js`: filtros, incluyendo departamento.
- `Info/data/contracts_pae_mock.json`: datos temporales.

## Qué está listo hoy

- Página principal unificada.
- Búsqueda y filtros.
- Tabla priorizada.
- Selección de contrato.
- Reporte estructurado.
- Red flags.
- Patrones de riesgo.
- Conexiones y fuentes externas preparadas.
- Chatbot local contextual.
- Documentación para otros agentes.

## Qué sigue conectado por fuera

- Backend real de contratos.
- Endpoints de detalle, reporte y conexiones.
- Enriquecimiento territorial o documental.
- Agente LLM remoto.
- Fuentes externas públicas integradas desde API.
