from __future__ import annotations

import argparse
from html import escape
from pathlib import Path

from base2_common import (
    DEFAULT_EDA_OUTPUT,
    build_missing_count_expr,
    column_names_by_type,
    column_names_by_types,
    format_float_es,
    format_int_es,
    format_iso_datetime,
    load_metadata,
    parse_iso_datetime,
    query_resource,
)


TARGET_ID_DOCUMENTO = 756926574


def get_total_rows() -> int:
    return int(query_resource({"$select": "count(*) as total", "$limit": "1"})[0]["total"])


def build_html() -> str:
    metadata = load_metadata()
    total_rows = get_total_rows()
    total_columns = len(metadata["columns"])

    date_columns = column_names_by_type(metadata, "calendar_date")
    numeric_columns = column_names_by_type(metadata, "number")
    text_columns = column_names_by_types(metadata, {"text", "url"})

    stats = query_resource(
        {
            "$select": ",".join(
                [
                    f"{build_missing_count_expr('descripci_n', treat_blank_as_missing=True)} as descripcion_nulls",
                    f"{build_missing_count_expr('proceso', treat_blank_as_missing=True)} as proceso_nulls",
                    "min(id_documento) as min_id_documento",
                    "max(id_documento) as max_id_documento",
                    "avg(id_documento) as avg_id_documento",
                    "median(id_documento) as median_id_documento",
                    "min(tamanno_archivo) as min_tamano_archivo",
                    "max(tamanno_archivo) as max_tamano_archivo",
                    "avg(tamanno_archivo) as avg_tamano_archivo",
                    "median(tamanno_archivo) as median_tamano_archivo",
                    "min(nit_entidad) as min_nit_entidad",
                    "max(nit_entidad) as max_nit_entidad",
                    "avg(nit_entidad) as avg_nit_entidad",
                    "median(nit_entidad) as median_nit_entidad",
                    "min(fecha_carga) as min_fecha_carga",
                    "max(fecha_carga) as max_fecha_carga",
                ]
            ),
            "$limit": "1",
        }
    )[0]

    lookup_rows = query_resource(
        {
            "$select": "nombre_archivo,fecha_carga",
            "$where": f"id_documento = {TARGET_ID_DOCUMENTO}",
            "$order": "fecha_carga ASC",
            "$limit": "20",
        }
    )

    min_fecha = parse_iso_datetime(stats["min_fecha_carga"])
    max_fecha = parse_iso_datetime(stats["max_fecha_carga"])
    rango_dias = (max_fecha - min_fecha).days if min_fecha and max_fecha else None

    def join_names(items):
        return ", ".join(escape(name) for _, name in items) if items else "N/D"

    answer_rows = [
        ("15", "¿Cuál es el total de registros en el dataset de documentos electrónicos dmgg-8hin?", format_int_es(total_rows)),
        ("16", "¿Cuántas columnas tiene el dataset?", format_int_es(total_columns)),
        ("17", "¿Cuántos valores nulos tiene el campo descripción?", format_int_es(int(stats["descripcion_nulls"]))),
        ("18", "¿Cuántos valores nulos tiene el campo proceso?", format_int_es(int(stats["proceso_nulls"]))),
        ("19", "¿Cuáles columnas tienen tipo de dato int64?", join_names(numeric_columns)),
        ("20", "¿Cuáles columnas tienen tipo de dato str?", join_names(text_columns)),
        (
            "21",
            "Máximo, mínimo, media y mediana de la columna ID documento.",
            " | ".join(
                [
                    f"Máximo: {format_int_es(int(stats['max_id_documento']))}",
                    f"Mínimo: {format_int_es(int(stats['min_id_documento']))}",
                    f"Media: {format_float_es(float(stats['avg_id_documento']))}",
                    f"Mediana: {format_int_es(int(round(float(stats['median_id_documento']))))}",
                ]
            ),
        ),
        (
            "22",
            "Máximo, mínimo, media y mediana de la columna tamaño archivo.",
            " | ".join(
                [
                    f"Máximo: {format_int_es(int(stats['max_tamano_archivo']))}",
                    f"Mínimo: {format_int_es(int(stats['min_tamano_archivo']))}",
                    f"Media: {format_float_es(float(stats['avg_tamano_archivo']))}",
                    f"Mediana: {format_int_es(int(round(float(stats['median_tamano_archivo']))))}",
                ]
            ),
        ),
        (
            "23",
            "Máximo, mínimo, media y mediana de la columna nit_entidad.",
            " | ".join(
                [
                    f"Máximo: {format_int_es(int(stats['max_nit_entidad']))}",
                    f"Mínimo: {format_int_es(int(stats['min_nit_entidad']))}",
                    f"Media: {format_float_es(float(stats['avg_nit_entidad']))}",
                    f"Mediana: {format_int_es(int(round(float(stats['median_nit_entidad']))))}",
                ]
            ),
        ),
        (
            "24",
            "Máximos y mínimos de fecha carga.",
            f"Mínimo: {format_iso_datetime(stats['min_fecha_carga'])} | Máximo: {format_iso_datetime(stats['max_fecha_carga'])}",
        ),
        (
            "25",
            "Rango de fecha o diferencia de fecha de fecha carga.",
            f"{format_int_es(rango_dias) if rango_dias is not None else 'N/D'} días",
        ),
        (
            "26",
            f"¿Cuál es el nombre_archivo y fecha_carga para el ID Documento = {TARGET_ID_DOCUMENTO}?",
            "; ".join(
                f"{row['nombre_archivo']} ({format_iso_datetime(row['fecha_carga'])})"
                for row in lookup_rows
            )
            if lookup_rows
            else "No se encontraron registros para ese ID",
        ),
    ]

    qa_rows_html = "".join(
        f"""
        <tr>
          <td>{num}</td>
          <td>{escape(question)}</td>
          <td>{escape(answer)}</td>
        </tr>
        """
        for num, question, answer in answer_rows
    )

    lookup_html = (
        "".join(
            f"<tr><td>{escape(row['nombre_archivo'])}</td><td>{format_iso_datetime(row['fecha_carga'])}</td></tr>"
            for row in lookup_rows
        )
        if lookup_rows
        else "<tr><td colspan='2'>No se encontraron registros para el ID solicitado.</td></tr>"
    )

    min_fecha_display = format_iso_datetime(stats["min_fecha_carga"])
    max_fecha_display = format_iso_datetime(stats["max_fecha_carga"])

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>EDA Base 2 - SECOP II</title>
  <style>
    body {{
      margin: 0;
      font-family: Arial, Helvetica, sans-serif;
      background: linear-gradient(180deg, #0b1120 0%, #111827 100%);
      color: #e5e7eb;
    }}
    .wrap {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }}
    .hero, .section, .card {{
      background: rgba(17,24,39,.84);
      border: 1px solid rgba(148,163,184,.16);
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 18px 50px rgba(0,0,0,.18);
    }}
    .hero {{
      background: linear-gradient(135deg, rgba(56,189,248,.18), rgba(52,211,153,.14));
      margin-bottom: 18px;
    }}
    h1, h2 {{ margin: 0 0 12px; }}
    .subtle, .muted {{ color: #9ca3af; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 14px;
      margin: 18px 0;
    }}
    .metric {{
      font-size: 30px;
      font-weight: 700;
      color: #38bdf8;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 10px;
    }}
    th, td {{
      border-bottom: 1px solid #334155;
      padding: 10px 8px;
      text-align: left;
      vertical-align: top;
    }}
    th {{ color: #cbd5e1; }}
    code {{
      background: rgba(15,23,42,.8);
      padding: 2px 5px;
      border-radius: 6px;
    }}
    .section {{ margin-top: 18px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div class="subtle">Base 2 · SECOP II - Archivos Descarga Desde 2025</div>
      <h1>EDA de la base 2</h1>
      <p class="subtle">Análisis generado sobre el dataset completo vía API oficial de datos.gov.co.</p>
    </div>

    <div class="grid">
      <div class="card"><div class="subtle">15. Registros totales</div><div class="metric">{format_int_es(total_rows)}</div></div>
      <div class="card"><div class="subtle">16. Columnas totales</div><div class="metric">{format_int_es(total_columns)}</div></div>
      <div class="card"><div class="subtle">17. Nulos en descripción</div><div class="metric">{format_int_es(int(stats['descripcion_nulls']))}</div></div>
      <div class="card"><div class="subtle">18. Nulos en proceso</div><div class="metric">{format_int_es(int(stats['proceso_nulls']))}</div></div>
    </div>

    <div class="section">
      <h2>Tipos de columnas</h2>
      <p><strong>int64 / number:</strong> {join_names(numeric_columns)}</p>
      <p><strong>str / text:</strong> {join_names(text_columns)}</p>
      <p><strong>fecha / calendar_date:</strong> {join_names(date_columns)}</p>
    </div>

    <div class="section">
      <h2>Preguntas y respuestas</h2>
      <table>
        <thead>
          <tr>
            <th>#</th>
            <th>Pregunta</th>
            <th>Respuesta</th>
          </tr>
        </thead>
        <tbody>
          {qa_rows_html}
        </tbody>
      </table>
    </div>

    <div class="section">
      <h2>Estadísticas numéricas</h2>
      <table>
        <thead>
          <tr>
            <th>Columna</th>
            <th>Mínimo</th>
            <th>Máximo</th>
            <th>Media</th>
            <th>Mediana</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>ID Documento</td>
            <td>{format_int_es(int(stats['min_id_documento']))}</td>
            <td>{format_int_es(int(stats['max_id_documento']))}</td>
            <td>{format_float_es(float(stats['avg_id_documento']))}</td>
            <td>{format_int_es(int(round(float(stats['median_id_documento']))))}</td>
          </tr>
          <tr>
            <td>Tamaño archivo</td>
            <td>{format_int_es(int(stats['min_tamano_archivo']))}</td>
            <td>{format_int_es(int(stats['max_tamano_archivo']))}</td>
            <td>{format_float_es(float(stats['avg_tamano_archivo']))}</td>
            <td>{format_int_es(int(round(float(stats['median_tamano_archivo']))))}</td>
          </tr>
          <tr>
            <td>NIT Entidad</td>
            <td>{format_int_es(int(stats['min_nit_entidad']))}</td>
            <td>{format_int_es(int(stats['max_nit_entidad']))}</td>
            <td>{format_float_es(float(stats['avg_nit_entidad']))}</td>
            <td>{format_int_es(int(round(float(stats['median_nit_entidad']))))}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div class="section">
      <h2>Fecha carga</h2>
      <p><strong>Mínimo:</strong> {min_fecha_display}</p>
      <p><strong>Máximo:</strong> {max_fecha_display}</p>
      <p><strong>25. Rango:</strong> {format_int_es(rango_dias) if rango_dias is not None else 'N/D'} días</p>
    </div>

    <div class="section">
      <h2>Consulta puntual</h2>
      <p><strong>26.</strong> ¿Cuál es el nombre_archivo y fecha_carga para el ID Documento = {TARGET_ID_DOCUMENTO}?</p>
      <table>
        <thead>
          <tr>
            <th>nombre_archivo</th>
            <th>fecha_carga</th>
          </tr>
        </thead>
        <tbody>
          {lookup_html}
        </tbody>
      </table>
    </div>
  </div>
</body>
</html>
"""
    return html


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Genera la EDA de la base 2.")
    parser.add_argument("--output", default=str(DEFAULT_EDA_OUTPUT), help="Archivo HTML de salida.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = Path(args.output)
    output_path.write_text(build_html(), encoding="utf-8")
    print(f"Reporte EDA base 2 generado en: {output_path}")


if __name__ == "__main__":
    main()
