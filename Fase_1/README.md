# Fase 1 - Open Data Sprint

Este directorio contiene el borrador local de la Fase 1 dentro de `info/`.
La idea es tener un MVP simple, ordenado y facil de extender para analizar datos abiertos del Estado colombiano, generar listados, explicar indicadores y exportar un informe en PDF desde el navegador.

## Como abrirlo en local

Desde la raiz del proyecto:

```powershell
py -3 -m http.server 4174 --directory Fase_1\info
```

Luego abre:

```text
http://localhost:4174
```

## Que resuelve este MVP

- Elegir una fuente de datos desde una lista.
- Elegir el indicador que se quiere analizar.
- Ver listados, rankings y tarjetas de analisis.
- Agregar nueva informacion manualmente sin tocar el codigo.
- Imprimir el informe desde el navegador y guardarlo como PDF.
- Revisar un resumen ejecutivo y una explicacion por indicador.

## Estructura editable

- `info/index.html`: pagina principal del MVP.
- El script incluye los catalogos de fuentes, indicadores y filas de ejemplo.
- El panel lateral permite cambiar fuente, indicador, agrupacion, busqueda y `Top N`.
- El formulario lateral permite agregar registros nuevos en localStorage.
- El boton `Imprimir informe PDF` usa la impresion del navegador.

## Flujo recomendado de analisis

1. Elegir la fuente de datos.
2. Filtrar o buscar registros.
3. Seleccionar el indicador.
4. Agrupar por entidad, proveedor, modalidad o estado.
5. Revisar el ranking y las senales de alerta.
6. Leer el analisis ejecutivo.
7. Imprimir el informe para entregar o guardar como PDF.

## Settings que conviene poder editar rapido

- Fuente activa.
- Indicador activo.
- Agrupar por.
- Buscar en la lista.
- `Top N`.
- Rango de datos o periodo, si se conecta una fuente real.
- Texto del informe ejecutivo.
- Registros manuales nuevos.

## Fuentes de datos recomendadas

Prioridad inicial para la Fase 1:

| Fuente | Dataset | Uso principal |
| --- | --- | --- |
| SECOP II - Contratos | `jbjy-vk9h` | Riesgo, valor y concentracion |
| SECOP II - Procesos | `p6dx-8zbt` | Competencia y volumen |
| SECOP II - Proponentes | `hgi6-6wh3` | Pluralidad y competencia |
| SECOP II - Ofertas | `wi7w-2nvm` | Competencia y mercado |
| SECOP II - Multas y sanciones | `it5q-hg94` | Riesgo reputacional |

Endpoints utiles para probar:

- JSON: `https://www.datos.gov.co/resource/jbjy-vk9h.json?$limit=5000`
- CSV: `https://www.datos.gov.co/resource/jbjy-vk9h.csv?$limit=5000`

La misma logica se puede replicar para las demas fuentes.

## Como organizar indicadores y estudios

La pagina esta pensada para que cada indicador tenga:

- Un nombre claro.
- Una descripcion corta.
- Una grafica principal.
- Un ranking o listado.
- Una explicacion de por que ese resultado importa.
- Una seccion de hallazgos o estudios para lectura rapida.

Recomendacion de grupos de analisis:

- Valor contratado.
- Cantidad de registros.
- Calidad de datos.
- Competencia o pluralidad.
- Riesgo u opacidad.
- Cambios por tiempo o territorio.

## Repositorios y librerias de referencia

Estos repos ayudan a decidir como leer datos, analizarlos, sacar reportes y extender el MVP.

### Ingesta de datos

| Repositorio | Aporta |
| --- | --- |
| [pandas-dev/pandas](https://github.com/pandas-dev/pandas) | Lectura y transformacion de CSV, Excel y HTML |
| [duckdb/duckdb](https://github.com/duckdb/duckdb) | Analitica local rapida sobre CSV y otros formatos |
| [pola-rs/polars](https://github.com/pola-rs/polars) | DataFrames muy rapidos para pipelines de datos |
| [jsvine/pdfplumber](https://github.com/jsvine/pdfplumber) | Extraccion de texto y tablas desde PDF |
| [camelot-dev/camelot](https://github.com/camelot-dev/camelot) | Extraccion de tablas desde PDF |
| [tabulapdf/tabula-java](https://github.com/tabulapdf/tabula-java) | Extraccion de tablas de PDFs con soporte multiplataforma |
| [py-pdf/pypdf](https://github.com/py-pdf/pypdf) | Lectura, mezcla y extraccion basica de PDF |
| [scrapy/scrapy](https://github.com/scrapy/scrapy) | Scraping web estructurado |
| [microsoft/playwright](https://github.com/microsoft/playwright) | Automatizacion web para capturar datos dinamicos |
| [SeleniumHQ/selenium](https://github.com/SeleniumHQ/selenium) | Automatizacion de navegadores para scraping y pruebas |

### Analisis y calidad

| Repositorio | Aporta |
| --- | --- |
| [ydataai/ydata-profiling](https://github.com/ydataai/ydata-profiling) | Perfilamiento automatico y reportes HTML |
| [evidentlyai/evidently](https://github.com/evidentlyai/evidently) | Calidad de datos, drift y reportes de seguimiento |
| [adamerose/PandasGUI](https://github.com/adamerose/PandasGUI) | Exploracion visual de DataFrames |
| [sqlfluff/sqlfluff](https://github.com/sqlfluff/sqlfluff) | Lint y orden para SQL si luego se agregan consultas |

### Reportes y PDF

| Repositorio | Aporta |
| --- | --- |
| [xhtml2pdf/xhtml2pdf](https://github.com/xhtml2pdf/xhtml2pdf) | Convertir HTML a PDF de forma directa |
| [jupyter/nbconvert](https://github.com/jupyter/nbconvert) | Exportar notebooks a HTML y PDF |
| [streamlit/streamlit-pdf](https://github.com/streamlit/streamlit-pdf) | Visualizar PDF dentro de apps Streamlit |
| [nsxydis/streamlit_report](https://github.com/nsxydis/streamlit_report) | Crear reportes HTML con estructura tipo dashboard |
| [Jaspersoft/jasperreports](https://github.com/Jaspersoft/jasperreports) | Reportes de alto nivel con salida PDF, Excel y mas |
| [streamlit/example-app-pdf-report](https://github.com/streamlit/example-app-pdf-report) | Ejemplo de generacion de reporte PDF |
| [plotly/dash](https://github.com/plotly/dash) | Dashboards interactivos para analisis web |
| [metabase/metabase](https://github.com/metabase/metabase) | BI y analitica compartible con paneles |

### Repositorio de referencia del proyecto

| Repositorio | Aporta |
| --- | --- |
| [alejo9812/AGP](https://github.com/alejo9812/AGP) | Patron de settings, dashboard y limpieza de datos reutilizable como guia |

## Que conviene dejar listo para evolucionar la Fase 1

- Selector de fuente de datos.
- Selector de indicador.
- Panel de filtros.
- Vista de ranking.
- Vista de hallazgos.
- Boton de imprimir PDF.
- Formulario para agregar datos manualmente.
- Espacio para conectar CSV, Excel, PDF o scraping mas adelante.

## Como pensar el sistema

1. Ingerir: CSV, Excel, PDF o scraping.
2. Normalizar: limpiar campos, fechas y valores.
3. Analizar: calcular indicadores por entidad, proveedor, modalidad o territorio.
4. Explicar: redactar un hallazgo por grafica o indicador.
5. Listar: mostrar rankings y tablas filtrables.
6. Exportar: imprimir el informe a PDF.

## Propuesta para la presentacion

La vista debe responder rapido a preguntas como:

- Que fuente estamos analizando.
- Que indicador se quiere revisar.
- Que categoria concentra mas valor, riesgo o volumen.
- Que registros tienen senales de alerta.
- Que evidencia queda para el informe final.

