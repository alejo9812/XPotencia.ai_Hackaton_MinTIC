# Interface Spec

Especificación de la interfaz principal de la Fase 2.

La información del proyecto es de **Alejandro Montes**.

## Propósito

La página principal de Fase 2 funciona como una sola interfaz tipo dashboard para buscar contratos PAE, priorizarlos por riesgo, abrir su reporte y consultar un chatbot contextual.

## Secciones

### 1. Header del proyecto

- Título del proyecto.
- Subtítulo explicativo.
- Nota de autoría.
- Advertencia de riesgo preliminar.

### 2. Panel de búsqueda

- Input principal de búsqueda.
- Botón Buscar.
- Búsqueda por contrato, proveedor, entidad, municipio, departamento y palabra clave.

### 3. Filtros

- Departamento.
- Municipio.
- Entidad contratante.
- Proveedor.
- Rango de valor.
- Nivel de riesgo.
- Modalidad de contratación.
- Año.

Estado vacío:

- Si no hay datos, mostrar opciones de ejemplo y placeholders claros.

### 4. Tabla de contratos sugeridos

- Orden descendente por score de riesgo.
- Columnas: ID, departamento, municipio, entidad, proveedor, valor, score, red flags, acción.
- Acciones: ver análisis, generar reporte, consultar conexiones.

### 5. Panel de contrato seleccionado

- ID del contrato.
- Entidad contratante.
- Proveedor.
- Objeto contractual.
- Valor.
- Departamento.
- Municipio.
- Fecha.
- Modalidad.
- Estado.
- Score de riesgo.
- Resumen del análisis.

Estado vacío:

- `Selecciona un contrato de la tabla para generar su análisis.`

### 6. Reporte del contrato

Bloques:

- Resumen ejecutivo.
- Análisis técnico.
- Interpretación ciudadana.
- Recomendaciones.

Estado vacío:

- `Reporte en preparación. Esta sección recibirá datos del módulo de análisis y reportes.`

### 7. Dashboard del contrato

- Score de riesgo.
- Número de red flags.
- Valor del contrato.
- Comparación con contratos similares.
- Frecuencia del proveedor.
- Nivel de concentración.
- Estado del contrato.
- Alertas documentales.

Fallback:

- Tarjetas HTML/CSS.
- Espacio reservado para Chart.js, Plotly u otra librería.

### 8. Red flags

Campos:

- Nombre.
- Descripción.
- Evidencia.
- Severidad.
- Recomendación.

### 9. Patrones riesgosos

Campos:

- Nombre.
- Explicación.
- Datos relacionados.
- Nivel de importancia.
- Acción sugerida.

### 10. Conexiones y fuentes externas

- Otros contratos del mismo proveedor.
- Otros contratos de la misma entidad.
- Contratos del mismo departamento.
- Contratos con objetos similares.
- Fuentes externas sugeridas.

Fallback:

- `Conexión pendiente con fuentes externas.`
- `Este espacio será alimentado por el módulo de trazabilidad y datos.`

### 11. Chatbot contextual

- Va al final de la página.
- Solo responde sobre el contrato seleccionado, el reporte y los datos visibles.
- Debe bloquearse si no hay contrato seleccionado.

Estado vacío:

- `Primero selecciona un contrato para que pueda responder preguntas sobre su informe.`

## Entradas y salidas

### Entradas

- Query de búsqueda.
- Filtros de la interfaz.
- Selección de contrato.
- Preguntas del chatbot.

### Salidas

- Tabla filtrada.
- Contrato seleccionado.
- Reporte estructurado.
- Métricas del dashboard.
- Red flags.
- Patrones.
- Conexiones.
- Respuesta contextual del chatbot.

## Estados vacíos

- Carga inicial.
- Sin resultados.
- Sin contrato seleccionado.
- Reporte no disponible.
- Conexiones pendientes.
- Chatbot bloqueado.

## Eventos

- `submit` de búsqueda.
- `change` de filtros.
- `click` en fila de contrato.
- `click` en acciones de reporte.
- `submit` del chatbot.

## Datos requeridos por sección

| Sección | Datos mínimos |
|---|---|
| Header | título, subtítulo, autoría, advertencia |
| Búsqueda | texto de búsqueda |
| Filtros | catálogo de departamentos, municipios, entidades, proveedores, modalidades, años |
| Tabla | lista priorizada de contratos |
| Panel de contrato | contrato seleccionado |
| Reporte | resumen, hallazgos, recomendaciones |
| Dashboard | métricas numéricas y comparativas |
| Red flags | lista de alertas con evidencia |
| Patrones | patrones y acciones sugeridas |
| Conexiones | relaciones y fuentes externas |
| Chatbot | contexto del contrato e informe visible |

