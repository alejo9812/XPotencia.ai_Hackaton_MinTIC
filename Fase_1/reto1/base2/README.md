# Reto 1 - Base 2

Carpeta de trabajo para la base 2 del reto 1 de Fase 1.

## Fuente

- Dataset: `SECOP II - Archivos Descarga Desde 2025`
- API metadata: `https://www.datos.gov.co/api/views/dmgg-8hin.json`
- API de consulta: `https://www.datos.gov.co/resource/dmgg-8hin.json`

## Estructura

- `parquet/`: archivos descargados para análisis local
- `scripts/`: scripts de EDA, calidad y descarga
- `meta/`: manifiestos, logs y notas de descarga

## Scripts de análisis

- `scripts/eda_base2.py`: genera el HTML con las respuestas a las preguntas 15 a 26.
- `scripts/calidad_datos_base2.py`: genera un reporte de calidad de datos aparte.

## Ejecución

```powershell
py -3 Fase_1\reto1\base2\scripts\eda_base2.py
py -3 Fase_1\reto1\base2\scripts\calidad_datos_base2.py
```

## Nota

Los scripts consultan la API oficial de datos.gov.co para trabajar con el dataset completo de 17.3 millones de registros y generar los HTML sin depender de un parquet local en descarga.

