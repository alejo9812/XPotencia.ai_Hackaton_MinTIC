# SECOP download helper

Carpeta independiente de `Fase_1` y `Fase_2` para descargar datos de SECOP II desde Datos Abiertos Colombia.

## Que hace

- Baja el dataset `jbjy-vk9h` desde el endpoint publico de Socrata.
- Genera CSV, JSONL o Parquet por lotes.
- Permite limitar la descarga para no traer los 5.6M de filas de una sola vez.
- Soporta `X-App-Token` si despues quieres subir el limite de solicitudes.

## Uso rapido

```powershell
node .\Fase_1\reto1\tools\secop-download\download-secop.mjs
node .\Fase_1\reto1\tools\secop-download\download-secop.mjs --format parquet --max-rows 1000
node .\Fase_1\reto1\tools\secop-download\download-secop.mjs --format parquet --max-rows 10000 --limit 500
```

Por defecto descarga las filas mas recientes dentro de `Fase_1/reto1/tools/secop-download/output/`.

## Opciones utiles

```powershell
node .\Fase_1\reto1\tools\secop-download\download-secop.mjs --max-rows 10000 --limit 1000
node .\Fase_1\reto1\tools\secop-download\download-secop.mjs --where "ultima_actualizacion >= '2026-01-01T00:00:00'"
node .\Fase_1\reto1\tools\secop-download\download-secop.mjs --output .\Fase_1\reto1\tools\secop-download\output\secop_2026.csv
```

Tambien puedes usar `--format parquet` para generar un archivo `.parquet` listo para analisis en herramientas como DuckDB, Python, Spark o Power BI.

## Variables de entorno

- `SOCRATA_APP_TOKEN`: token opcional de Socrata.
- `SOCRATA_BASE_URL`: base URL alternativa si quieres apuntar a otro portal.

## Nota

El dataset completo es muy grande. Si quieres trabajar analisis de fase 1, conviene empezar con una descarga filtrada por fecha o por una ventana de registros reciente.
