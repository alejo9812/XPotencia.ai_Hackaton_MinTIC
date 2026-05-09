# Reto 2

Carpeta de trabajo para el dataset grande de SECOP II usado en el reto 2.

## Fuente local

- CSV principal: `SECOP_II_-_Archivos_Descarga_Desde_2025_20260508.csv`

## Recomendacion de formato

Para este volumen de datos, el formato recomendado es **Parquet** porque:

- conserva todas las filas y columnas
- comprime mejor que CSV
- se lee mucho mas rapido con DuckDB o Python
- permite hacer EDA y calidad de datos sin cargar todo el archivo en memoria

## Flujo propuesto

1. Convertir el CSV a Parquet.
2. Calcular EDA y calidad de datos sobre la base usada para el analisis.
3. Generar dos HTML separados, igual que en el reto 1.

## Script principal

- `scripts/generate_reports_reto2.py`
- `scripts/generate_dashboard_reto2.py`

## Ejecucion

```powershell
py -3 Fase_1\reto2\scripts\generate_reports_reto2.py
py -3 Fase_1\reto2\scripts\generate_dashboard_reto2.py
```

## Salidas esperadas

- `parquet/`: copia en Parquet del CSV
- `reports/reto2_eda.html`: reporte HTML de EDA con respuestas
- `reports/reto2_calidad.html`: reporte HTML de calidad de datos
- `dashboard/index.html`: dashboard visual aparte

## Nota importante

Si el CSV esta abierto en Excel, la conversion puede fallar por bloqueo del archivo. En ese caso, cierra Excel y vuelve a ejecutar el script.

## Acceso rapido

- [EDA](./reports/reto2_eda.html)
- [Calidad](./reports/reto2_calidad.html)
- [Dashboard](./dashboard/index.html)
