# Base 1 - Reto 1

Esta carpeta concentra la primera base analizada del reto 1.

## Contenido

- `meta/`: muestra de 1000 registros y sus manifiestos.
- `reports/`: HTML generados para exploracion y calidad.
- `scripts/`: scripts de EDA y calidad de datos.

## Archivos clave

- `meta/muestra_1000.csv`
- `meta/muestra_1000.parquet`
- `reports/EDA_respuestas.html`
- `reports/calidad_datos.html`

## Uso

Los scripts de esta carpeta estan pensados para correr sobre la muestra de la base 1 y dejar resultados dentro de `reports/`.

## Scripts disponibles

### `scripts/EDA.py`

Genera un informe HTML con respuestas automaticas sobre el dataset publicado en `datos.gov.co`.

- Lee metadatos del dataset por API.
- Cuenta variables, tipos de columnas y nulos.
- Calcula algunos maximos y minimos relevantes.
- Guarda el resultado en `reports/EDA_respuestas.html`.

Ejemplo de ejecucion:

```powershell
python .\Fase_1\reto1\base1\scripts\EDA.py --dataset-id jbjy-vk9h
```

### `scripts/calidad_datos.py`

Analiza un archivo local CSV o Parquet y genera un reporte HTML de calidad de datos.

- Sirve para probar con un archivo de un amigo o con una base local.
- Detecta nulos, valores unicos y tipos de columna.
- Guarda el resultado en `reports/calidad_datos.html`.

Ejemplos de ejecucion:

```powershell
python .\Fase_1\reto1\base1\scripts\calidad_datos.py --input "D:\ruta\archivo.csv"
```

```powershell
python .\Fase_1\reto1\base1\scripts\calidad_datos.py --input "D:\ruta\archivo.parquet"
```

## Flujo recomendado para pruebas

1. Usa `calidad_datos.py` con el archivo local que te comparta tu amigo.
2. Revisa el HTML generado en `reports/`.
3. Si el dataset tambien esta publicado en `datos.gov.co`, ejecuta `EDA.py` para contrastar resultados con la API.
