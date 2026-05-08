from __future__ import annotations

import argparse
from html import escape
from pathlib import Path

from base2_common import (
    DEFAULT_QUALITY_OUTPUT,
    build_missing_count_expr,
    column_names_by_type,
    format_float_es,
    format_int_es,
    load_metadata,
    query_resource,
)


def count_total_rows() -> int:
    return int(query_resource({"$select": "count(*) as total", "$limit": "1"})[0]["total"])


def build_html() -> str:
    metadata = load_metadata()
    total_rows = count_total_rows()
    columns = metadata["columns"]

    type_by_field = {column["fieldName"]: column["dataTypeName"] for column in columns}
    null_exprs = {
        "n_mero_de_contrato": build_missing_count_expr("n_mero_de_contrato", treat_blank_as_missing=True),
        "id_documento": build_missing_count_expr("id_documento"),
        "proceso": build_missing_count_expr("proceso", treat_blank_as_missing=True),
        "nombre_archivo": build_missing_count_expr("nombre_archivo", treat_blank_as_missing=True),
        "tamanno_archivo": build_missing_count_expr("tamanno_archivo"),
        "extensi_n": build_missing_count_expr("extensi_n", treat_blank_as_missing=True),
        "descripci_n": build_missing_count_expr("descripci_n", treat_blank_as_missing=True),
        "fecha_carga": build_missing_count_expr("fecha_carga"),
        "entidad": build_missing_count_expr("entidad", treat_blank_as_missing=True),
        "nit_entidad": build_missing_count_expr("nit_entidad"),
        "url_descarga_documento": build_missing_count_expr("url_descarga_documento"),
    }

    stats = query_resource(
        {
            "$select": ",".join(
                [
                    "count(*) as total_rows",
                    f"{null_exprs['descripci_n']} as descripcion_nulls",
                    f"{null_exprs['proceso']} as proceso_nulls",
                    f"{null_exprs['fecha_carga']} as fecha_carga_nulls",
                    f"count(*) - count(id_documento) as id_documento_parse_fail",
                    f"count(*) - count(tamanno_archivo) as tamanno_archivo_parse_fail",
                    f"count(*) - count(nit_entidad) as nit_entidad_parse_fail",
                    f"count(*) - count(fecha_carga) as fecha_carga_parse_fail",
                    "count(*) - count(distinct id_documento) as duplicate_id_documento_rows",
                ]
            ),
            "$limit": "1",
        }
    )[0]

    critical_missing_rows = query_resource(
        {
            "$select": "count(*) as total",
            "$where": "id_documento is null or proceso is null or nombre_archivo is null or fecha_carga is null",
            "$limit": "1",
        }
    )[0]["total"]

    def fetch_missing_count(expr: str, alias: str) -> int:
        return int(query_resource({"$select": f"{expr} as {alias}", "$limit": "1"})[0][alias])

    missing_rows = {
        "n_mero_de_contrato": fetch_missing_count(null_exprs["n_mero_de_contrato"], "n_mero_de_contrato"),
        "id_documento": fetch_missing_count(null_exprs["id_documento"], "id_documento"),
        "proceso": fetch_missing_count(null_exprs["proceso"], "proceso"),
        "nombre_archivo": fetch_missing_count(null_exprs["nombre_archivo"], "nombre_archivo"),
        "tamanno_archivo": fetch_missing_count(null_exprs["tamanno_archivo"], "tamanno_archivo"),
        "extensi_n": fetch_missing_count(null_exprs["extensi_n"], "extensi_n"),
        "descripci_n": fetch_missing_count(null_exprs["descripci_n"], "descripci_n"),
        "fecha_carga": fetch_missing_count(null_exprs["fecha_carga"], "fecha_carga"),
        "entidad": fetch_missing_count(null_exprs["entidad"], "entidad"),
        "nit_entidad": fetch_missing_count(null_exprs["nit_entidad"], "nit_entidad"),
        "url_descarga_documento": fetch_missing_count(null_exprs["url_descarga_documento"], "url_descarga_documento"),
    }

    columns_with_nulls = sum(1 for column in columns if int(missing_rows[column["fieldName"]]) > 0)
    max_null_column = max(columns, key=lambda column: int(missing_rows[column["fieldName"]]))
    max_null_count = int(missing_rows[max_null_column["fieldName"]])
    max_null_pct = (max_null_count / total_rows) * 100 if total_rows else 0.0

    numeric_columns = column_names_by_type(metadata, "number")
    date_columns = column_names_by_type(metadata, "calendar_date")

    types_html = "".join(
        f"<tr><td>{escape(column['name'])}</td><td>{escape(type_by_field[column['fieldName']])}</td></tr>"
        for column in columns
    )

    nulls_html = "".join(
        f"<tr><td>{escape(column['name'])}</td><td>{escape(type_by_field[column['fieldName']])}</td><td>{format_int_es(int(missing_rows[column['fieldName']]))}</td><td>{format_float_es((int(missing_rows[column['fieldName']]) / total_rows) * 100 if total_rows else 0.0)}%</td></tr>"
        for column in columns
    )

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Calidad Base 2 - SECOP II</title>
  <style>
    body {{
      margin: 0;
      font-family: Arial, Helvetica, sans-serif;
      background: linear-gradient(180deg, #0b1120 0%, #111827 100%);
      color: #e5e7eb;
    }}
    .wrap {{ max-width: 1200px; margin: 0 auto; padding: 32px 20px 48px; }}
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
    .metric {{ font-size: 30px; font-weight: 700; color: #34d399; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
    th, td {{ border-bottom: 1px solid #334155; padding: 10px 8px; text-align: left; vertical-align: top; }}
    th {{ color: #cbd5e1; }}
    code {{ background: rgba(15,23,42,.8); padding: 2px 5px; border-radius: 6px; }}
    .section {{ margin-top: 18px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div class="subtle">Base 2 · comprobación de calidad</div>
      <h1>Reporte de calidad de datos</h1>
      <p class="subtle">Reporte generado sobre el dataset completo vía API oficial de datos.gov.co.</p>
    </div>

    <div class="grid">
      <div class="card"><div class="subtle">Registros</div><div class="metric">{format_int_es(total_rows)}</div></div>
      <div class="card"><div class="subtle">Columnas</div><div class="metric">{format_int_es(len(columns))}</div></div>
      <div class="card"><div class="subtle">Columnas con nulos</div><div class="metric">{format_int_es(columns_with_nulls)}</div></div>
      <div class="card"><div class="subtle">Fila crítica incompleta</div><div class="metric">{format_int_es(int(critical_missing_rows))}</div></div>
      <div class="card"><div class="subtle">ID duplicados</div><div class="metric">{format_int_es(int(stats['duplicate_id_documento_rows']))}</div></div>
      <div class="card"><div class="subtle">Mayor nulidad</div><div class="metric">{format_float_es(max_null_pct)}%</div></div>
    </div>

    <div class="section">
      <h2>Tipos detectados</h2>
      <table>
        <thead><tr><th>Columna</th><th>Tipo</th></tr></thead>
        <tbody>{types_html}</tbody>
      </table>
    </div>

    <div class="section">
      <h2>Resumen de nulos</h2>
      <table>
        <thead><tr><th>Columna</th><th>Tipo</th><th>Nulos</th><th>% nulos</th></tr></thead>
        <tbody>{nulls_html}</tbody>
      </table>
    </div>

    <div class="section">
      <h2>Problemas de conversión</h2>
      <table>
        <thead><tr><th>Campo</th><th>Filas con problema</th></tr></thead>
        <tbody>
          <tr><td>ID Documento</td><td>{format_int_es(int(stats['id_documento_parse_fail']))}</td></tr>
          <tr><td>Tamaño Documento</td><td>{format_int_es(int(stats['tamanno_archivo_parse_fail']))}</td></tr>
          <tr><td>NIT Entidad</td><td>{format_int_es(int(stats['nit_entidad_parse_fail']))}</td></tr>
          <tr><td>Fecha Carga</td><td>{format_int_es(int(stats['fecha_carga_parse_fail']))}</td></tr>
        </tbody>
      </table>
    </div>

    <div class="section">
      <h2>Campos clave</h2>
      <p><strong>Numéricos:</strong> {escape(", ".join(name for _, name in numeric_columns))}</p>
      <p><strong>Fecha:</strong> {escape(", ".join(name for _, name in date_columns))}</p>
    </div>
  </div>
</body>
</html>
"""
    return html


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Genera el reporte de calidad de la base 2.")
    parser.add_argument("--output", default=str(DEFAULT_QUALITY_OUTPUT), help="Archivo HTML de salida.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = Path(args.output)
    html = build_html()
    output_path.write_text(html, encoding="utf-8")
    print(f"Reporte de calidad base 2 generado en: {output_path}")


if __name__ == "__main__":
    main()
