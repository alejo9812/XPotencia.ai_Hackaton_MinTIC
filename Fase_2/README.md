# Fase 2

GobIA Auditor vive aqui como un proyecto independiente de Fase 1.

## Acceso rapido

- [Abrir la pagina de Fase 2](./Info/index.html)
- Servidor local estatico: `http://localhost:4175`
- Servidor local con proxy Dify: `http://localhost:4175`

Para verla en el navegador desde la raiz del proyecto:

```powershell
py -3 -m http.server 4175 --directory Fase_2\Info
```

## Estado actual

La carpeta `Fase_2/Info/` ya tiene un MVP funcional en modo estatico con:

- busqueda y seleccion de fuente SECOP II
- panel de filtros y carga manual de casos
- score deterministico de riesgo
- tabla y ranking de contratos
- panel de señales explicables
- informe ejecutivo listo para imprimir en PDF o descargar en HTML/MD
- panel de Dify Chatflow embebido con URL editable desde la UI
- panel de Dify API segura con proxy local y respuesta visible

## Como abrirlo localmente

Para la version con proxy de Dify:

```powershell
$env:DIFY_API_KEY="tu_api_key_de_dify"
node Fase_2\Info\server.mjs
```

Despues abre:

```text
http://localhost:4175
```

Si solo quieres ver la UI estatica sin la API:

```powershell
py -3 -m http.server 4175 --directory Fase_2\Info
```

## Estructura de la fase

- `Fase_2/Info/index.html`: pagina principal del MVP
- `Fase_2/Info/app.js`: estado, render, filtros y seleccion de contratos
- `Fase_2/Info/risk-engine.js`: score local y señales explicables
- `Fase_2/Info/secop-api.js`: carga de fuentes, normalizacion y utilidades
- `Fase_2/Info/report.js`: resumen ejecutivo e informe imprimible
- `Fase_2/Info/report-export.js`: exportacion descargable a HTML y Markdown
- `Fase_2/Info/dify-widget.js`: espacio para Dify Chatflow embebido
- `Fase_2/Info/dify-api.js`: payload y llamada segura a Dify API
- `Fase_2/Info/server.mjs`: servidor local con proxy hacia Dify
- `Fase_2/Info/data/`: catalogos y datos mock

## Ruta de los siguientes 3 pasos

1. Integrar Dify Chatflow embebido en el panel de IA.
2. Conectar Dify por API o serverless para enviar contrato, score y señales.
3. Pulir el informe ejecutivo y dejar salida PDF mas formal.

## Variables de entorno para Dify API

- `DIFY_API_KEY`: clave privada de tu app de Dify
- `DIFY_BASE_URL`: opcional, por defecto `https://api.dify.ai`
- `PORT`: opcional, por defecto `4175`

Nota:
- La clave nunca se guarda en el navegador.
- Si `DIFY_API_KEY` no esta definida, el panel de API sigue mostrando el payload y el error de forma controlada.

## Regla de trabajo

- Fase 1 y Fase 2 no comparten variables ni logica.
- Fase 2 se desarrolla solo en `Fase_2/Info/`.
- El MVP debe seguir funcionando aunque falle la API publica.
- El resumen ciudadano nunca debe acusar corrupcion; solo prioriza revision humana.
