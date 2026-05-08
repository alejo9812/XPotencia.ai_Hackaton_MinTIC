# Fase 1 - Open Data Sprint MVP

Fase 1 es el punto de partida del proyecto: un prototipo local, autocontenible y facil de extender para explorar datos abiertos del Estado colombiano, explicar indicadores, construir rankings y exportar un informe listo para imprimir a PDF.

La idea de esta fase no es solo "mostrar datos", sino dejar montada la base de trabajo para que despues podamos conectar fuentes reales, validar indicadores y convertir esto en un MVP serio para analisis y presentacion.

## Acceso rapido

- [Abrir la pagina de Fase 1](./info/index.html)
- Servidor local: `http://localhost:4174`

Para verla en el navegador desde la raiz del proyecto:

```powershell
py -3 -m http.server 4174 --directory Fase_1\info
```

## Objetivo de la fase

- Explorar una fuente de datos desde una interfaz clara.
- Cambiar el indicador que se quiere analizar sin tocar el codigo.
- Leer listas, rankings, graficas y un resumen ejecutivo en la misma pantalla.
- Cargar registros manuales para pruebas o demostraciones.
- Imprimir el informe desde el navegador y guardarlo como PDF.
- Dejar una base tecnica limpia para seguir desarrollando la Fase 1.

## Vista general de la pagina

La pagina principal vive en `Fase_1/info/index.html` y esta pensada como un dashboard de analisis con dos zonas grandes:

- una columna lateral para elegir fuente, ajustar filtros y registrar informacion;
- una zona principal para ver KPIs, graficas, informe ejecutivo, ranking e indicadores.

### Wireframe rapido

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│ Badge de fase + titulo + descripcion + acciones                              │
│ [Imprimir informe PDF] [Restaurar muestra]                                   │
├──────────────────────────────────────────────────────────────────────────────┤
│ Sidebar                                                                      │
│ ┌ Fuentes de datos ────────────────────────────────────────────────────────┐ │
│ │ Tarjetas de fuentes SECOP II y otras bases                                │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
│ ┌ Settings ────────────────────────────────────────────────────────────────┐ │
│ │ Fuente activa | Indicador | Agrupar por | Buscar | Top N                 │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
│ ┌ Agregar informacion ─────────────────────────────────────────────────────┐ │
│ │ Formulario para cargar registros nuevos en localStorage                  │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
│ ┌ Ruta tecnica ────────────────────────────────────────────────────────────┐ │
│ │ Referencias para pandas, PDF, scraping, calidad y dashboards            │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
├──────────────────────────────────────────────────────────────────────────────┤
│ Contenido principal                                                          │
│ ┌ KPI de analisis ────────────────────────────────────────────────────────┐  │
│ │ Resumen rapido del dataset filtrado                                     │  │
│ └─────────────────────────────────────────────────────────────────────────┘  │
│ ┌ Top categorias ────────────────┐  ┌ Informe ejecutivo ──────────────────┐  │
│ │ Grafica principal               │  │ Texto listo para copiar o imprimir  │  │
│ └────────────────────────────────┘  └─────────────────────────────────────┘  │
│ ┌ Ranking de registros ───────────────────────────────────────────────────┐  │
│ │ Tabla con score, valor, entidad, proveedor y senales                    │  │
│ └─────────────────────────────────────────────────────────────────────────┘  │
│ ┌ Indicadores y estudios ──────────────────────────────────────────────────┐  │
│ │ Tarjetas con lectura por indicador y notas de interpretacion            │  │
│ └─────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Descripcion de cada bloque

### Hero superior

La franja superior presenta el contexto de la fase:

- `Badge` con el estado del proyecto: `Fase 1 - borrador local en info/`.
- Titulo principal: `Open Data Sprint MVP`.
- Texto explicativo con el proposito del prototipo.
- Boton `Imprimir informe PDF`.
- Boton `Restaurar muestra`.
- Banda de estado dinamica con etiquetas de la fuente, el indicador y el top actual.

### Sidebar: Fuentes de datos

Este bloque sirve para seleccionar rapidamente la base que se quiere estudiar.

En la implementacion actual se contemplan fuentes SECOP II como:

- Contratos.
- Procesos.
- Proponentes.
- Ofertas.
- Multas y sanciones.

Cada fuente muestra metadatos que ayudan a entenderla de inmediato:

- nombre visible;
- dataset ID;
- tipo de carga;
- descripcion corta;
- foco analitico principal.

### Sidebar: Settings

Este panel agrupa los controles que conviene editar rapido en una demostracion o en una exploracion inicial.

Los controles actuales son:

- `Fuente activa`
- `Indicador`
- `Agrupar por`
- `Buscar en la lista`
- `Top N`

El selector de agrupacion permite revisar la informacion por:

- entidad;
- proveedor;
- departamento;
- modalidad;
- estado.

El rango de `Top N` va de 3 a 12 para mantener la lectura rapida.

### Sidebar: Agregar informacion

Este formulario permite cargar nuevas observaciones sin tocar el codigo.

Campos disponibles:

- fuente;
- ID;
- entidad;
- proveedor;
- departamento;
- modalidad;
- valor;
- fecha;
- participantes;
- estado;
- descripcion.

El objetivo de este bloque es facilitar pruebas, ejemplos de jurado y enriquecimiento manual de la muestra.

La persistencia local usa `localStorage`, asi que sirve para simulacion de datos mientras conectamos una fuente real.

### Sidebar: Ruta tecnica

Este bloque documenta la ruta de evolucion tecnica de la fase.

La pagina ya sugiere herramientas utiles para pasar del prototipo a un flujo real:

- `pandas` para leer CSV, Excel y HTML.
- `pdfplumber` y `Camelot` para tablas desde PDF.
- `Scrapy` para scraping estructurado.
- `ydata-profiling` y `Evidently` para calidad y seguimiento.
- `Metabase`, `Dash` o `Streamlit` para compartir resultados.

### Contenido principal: KPI de analisis

Este panel resume el dataset filtrado en tarjetas de KPI.

La intencion es responder, de un solo vistazo, preguntas como:

- cuantos registros entraron en la vista;
- cuanto valor total hay;
- cual es el promedio o la mediana relevante;
- que nivel de riesgo o concentracion hay;
- como va la participacion o la competencia.

### Contenido principal: Top categorias

Aqui se muestra la grafica principal del indicador seleccionado.

La grafica cambia segun el foco analitico:

- riesgo;
- valor;
- volumen;
- calidad;
- competencia.

La idea es que la pantalla siempre tenga una lectura prioritaria y no se quede solo en tablas.

### Contenido principal: Informe ejecutivo

Este bloque redacta un resumen listo para copiar o imprimir.

Debe contestar de forma corta:

- que se esta viendo;
- cual es el hallazgo principal;
- por que importa;
- que categorias concentran el resultado;
- que accion de revision sugiere.

### Contenido principal: Ranking de registros

Esta tabla permite revisar casos individuales.

Columnas visibles:

- registro;
- entidad;
- proveedor;
- valor;
- score;
- senales.

La tabla esta pensada para apoyar trazabilidad: primero se ve el resultado agregado, luego se baja a los registros concretos que explican el hallazgo.

### Contenido principal: Indicadores y estudios

Este bloque define la lectura analitica de la pagina.

Cada indicador debe tener:

- nombre claro;
- descripcion corta;
- grafica principal;
- resultado clave;
- interpretacion;
- estudio o nota de apoyo.

La logica es que la pantalla no sea solo visual, sino que tambien explique por que la grafica importa.

## Flujo de uso recomendado

1. Abrir la pagina en local.
2. Elegir la fuente de datos.
3. Definir el indicador que se quiere explorar.
4. Agrupar por una dimension util.
5. Buscar un registro concreto si hace falta.
6. Ajustar `Top N` para limpiar la lectura.
7. Revisar KPI, grafica, informe ejecutivo y ranking.
8. Agregar registros manuales para pruebas.
9. Imprimir el informe y guardarlo como PDF.

## Como levantarlo en local

Desde la raiz del proyecto:

```powershell
py -3 -m http.server 4174 --directory Fase_1\info
```

Luego abre:

```text
http://localhost:4174
```

## Datos y fuentes recomendadas

La primera version de la Fase 1 se apoya en fuentes abiertas de SECOP II.

Prioridad inicial:

| Fuente | Dataset | Uso principal |
| --- | --- | --- |
| SECOP II - Contratos | `jbjy-vk9h` | Riesgo, valor y concentracion |
| SECOP II - Procesos | `p6dx-8zbt` | Competencia y volumen |
| SECOP II - Proponentes | `hgi6-6wh3` | Pluralidad y competencia |
| SECOP II - Ofertas | `wi7w-2nvm` | Competencia y mercado |
| SECOP II - Multas y sanciones | `it5q-hg94` | Riesgo reputacional |

Endpoints utiles para prueba rapida:

- JSON: `https://www.datos.gov.co/resource/jbjy-vk9h.json?$limit=5000`
- CSV: `https://www.datos.gov.co/resource/jbjy-vk9h.csv?$limit=5000`

La misma logica se puede aplicar a las demas fuentes cambiando el dataset ID.

## Estructura tecnica actual

- `Fase_1/info/index.html`: pagina unica del MVP.
- La vista se renderiza desde un solo archivo HTML autocontenible.
- El CSS, la data base de ejemplo y la logica de render viven dentro del mismo archivo.
- `localStorage` guarda los registros manuales.
- `printBtn` dispara la impresion del navegador para exportar a PDF.
- `resetBtn` restaura la muestra inicial.

## Configuraciones que conviene poder editar rapido

- fuente activa;
- indicador activo;
- agrupacion;
- busqueda;
- `Top N`;
- texto del informe ejecutivo;
- registros manuales;
- lista de categorias priorizadas;
- rango temporal cuando conectemos una fuente real.

## Como pensar la evolucion de la fase

1. Ingerir: CSV, Excel, PDF o scraping.
2. Normalizar: limpiar campos, fechas y valores.
3. Analizar: calcular indicadores por entidad, proveedor, modalidad o territorio.
4. Explicar: redactar hallazgos por grafica o indicador.
5. Listar: mostrar rankings y tablas filtrables.
6. Exportar: imprimir el informe a PDF.

## Alcance de desarrollo de la Fase 1

La Fase 1 debe dejar listo:

- un selector de fuente de datos;
- un selector de indicador;
- un panel de filtros;
- una grafica principal;
- un informe ejecutivo editable;
- un ranking de registros;
- una seccion de indicadores y estudios;
- un formulario para agregar datos manuales;
- la salida de impresion a PDF;
- la base para conectar datos reales despues.

## Propuesta de entregables

Para presentar esta fase de forma clara, la pantalla debe responder rapido a estas preguntas:

- que fuente estamos analizando;
- que indicador se quiere revisar;
- que categoria concentra mas valor, riesgo o volumen;
- que registros muestran senales de alerta;
- que evidencia queda para el informe final.

## Referencias utiles

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
| [sqlfluff/sqlfluff](https://github.com/sqlfluff/sqlfluff) | Orden y lint para SQL si luego se agregan consultas |

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

## Nota de trabajo

Este README esta pensado para ser la base documental de la Fase 1. Si despues cambiamos la estructura de la pagina, conviene actualizar tambien esta descripcion para que el documento siga reflejando la UI real.
