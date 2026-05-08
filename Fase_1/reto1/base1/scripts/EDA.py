from __future__ import annotations

import argparse
import json
import urllib.parse
import urllib.request
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Iterable


API_BASE_URL = "https://www.datos.gov.co"
DEFAULT_DATASET_ID = "jbjy-vk9h"
BASE1_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = BASE1_ROOT / "reports" / "EDA_respuestas.html"
BATCH_SIZE = 20


@dataclass
class ReportData:
    dataset_id: str
    dataset_name: str
    total_records: int
    total_variables: int
    date_fields: list[tuple[str, str]]
    numeric_fields: list[tuple[str, str]]
    text_fields: list[tuple[str, str]]
    null_counts: dict[str, int]
    max_null_fields: list[tuple[str, str]]
    fecha_de_firma_null_pct: float
    fecha_inicio_liquidacion_nulls: int
    max_dias_adicionados: int
    max_valor_del_contrato: int
    septimo_valor_del_contrato: int
    fecha_de_firma_min: str
    fecha_de_firma_max: str


def fetch_json(url: str):
    with urllib.request.urlopen(url) as response:
        return json.loads(response.read().decode("utf-8"))


def build_url(path: str, params: dict[str, str] | None = None) -> str:
    url = f"{API_BASE_URL}{path}"
    if params:
        query = urllib.parse.urlencode(params, safe="(),*<> =")
        url = f"{url}?{query}"
    return url


def load_metadata(dataset_id: str) -> dict:
    return fetch_json(build_url(f"/api/views/{dataset_id}.json"))


def query_rows(dataset_id: str, params: dict[str, str]) -> list[dict]:
    return fetch_json(build_url(f"/resource/{dataset_id}.json", params))


def format_int_es(value: int) -> str:
    return f"{value:,}".replace(",", ".")


def format_decimal_es(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}".replace(".", ",")


def format_date_only(value: str) -> str:
    return value[:10] if value else "N/D"


def count_non_nulls(dataset_id: str, fields: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for start in range(0, len(fields), BATCH_SIZE):
        batch = fields[start : start + BATCH_SIZE]
        select_expr = ",".join(f"count({field}) as {field}" for field in batch)
        rows = query_rows(dataset_id, {"$select": select_expr})
        if not rows:
            raise RuntimeError(f"Sin resultados para el lote: {batch}")
        row = rows[0]
        for field in batch:
            counts[field] = int(row[field])
    return counts


def classify_fields(columns: list[dict]) -> tuple[list[tuple[str, str]], list[tuple[str, str]], list[tuple[str, str]]]:
    date_fields: list[tuple[str, str]] = []
    numeric_fields: list[tuple[str, str]] = []
    text_fields: list[tuple[str, str]] = []

    for column in columns:
        field_name = column["fieldName"]
        display_name = column["name"]
        data_type = column["dataTypeName"]

        if data_type == "calendar_date":
            date_fields.append((field_name, display_name))
        elif data_type == "number":
            numeric_fields.append((field_name, display_name))
        elif data_type in {"text", "url"}:
            text_fields.append((field_name, display_name))

    return date_fields, numeric_fields, text_fields


def query_scalar(dataset_id: str, select_expr: str, where_expr: str | None = None, order_expr: str | None = None, limit: int | None = None) -> list[dict]:
    params: dict[str, str] = {"$select": select_expr}
    if where_expr:
        params["$where"] = where_expr
    if order_expr:
        params["$order"] = order_expr
    if limit is not None:
        params["$limit"] = str(limit)
    return query_rows(dataset_id, params)


def build_report_data(dataset_id: str) -> ReportData:
    metadata = load_metadata(dataset_id)
    columns = metadata["columns"]
    date_fields, numeric_fields, text_fields = classify_fields(columns)

    total_records = int(
        query_scalar(dataset_id, "count(*) as total")[0]["total"]
    )
    total_variables = len(columns)

    all_fields = [column["fieldName"] for column in columns]
    non_null_counts = count_non_nulls(dataset_id, all_fields)
    null_counts = {field: total_records - non_null_counts[field] for field in all_fields}

    max_null_value = max(null_counts.values())
    max_null_fields = [
        (column["fieldName"], column["name"])
        for column in columns
        if null_counts[column["fieldName"]] == max_null_value
    ]

    fecha_de_firma_nulls = null_counts["fecha_de_firma"]
    fecha_inicio_liquidacion_nulls = null_counts["fecha_inicio_liquidacion"]

    max_dias_adicionados = int(
        query_scalar(
            dataset_id,
            "max(dias_adicionados) as max_dias",
            where_expr="dias_adicionados is not null",
        )[0]["max_dias"]
    )
    max_valor_del_contrato = int(
        query_scalar(
            dataset_id,
            "max(valor_del_contrato) as max_valor",
            where_expr="valor_del_contrato is not null",
        )[0]["max_valor"]
    )
    septimo_valor_del_contrato = int(
        query_scalar(
            dataset_id,
            "valor_del_contrato",
            where_expr="valor_del_contrato is not null",
            order_expr="valor_del_contrato DESC",
            limit=7,
        )[6]["valor_del_contrato"]
    )

    firma_min_max = query_scalar(
        dataset_id,
        "min(fecha_de_firma) as min_firma, max(fecha_de_firma) as max_firma",
        where_expr="fecha_de_firma is not null",
    )[0]

    return ReportData(
        dataset_id=dataset_id,
        dataset_name=metadata.get("name", dataset_id),
        total_records=total_records,
        total_variables=total_variables,
        date_fields=date_fields,
        numeric_fields=numeric_fields,
        text_fields=text_fields,
        null_counts=null_counts,
        max_null_fields=max_null_fields,
        fecha_de_firma_null_pct=(fecha_de_firma_nulls / total_records) * 100 if total_records else 0.0,
        fecha_inicio_liquidacion_nulls=fecha_inicio_liquidacion_nulls,
        max_dias_adicionados=max_dias_adicionados,
        max_valor_del_contrato=max_valor_del_contrato,
        septimo_valor_del_contrato=septimo_valor_del_contrato,
        fecha_de_firma_min=format_date_only(firma_min_max["min_firma"]),
        fecha_de_firma_max=format_date_only(firma_min_max["max_firma"]),
    )


def render_variable_list(items: Iterable[tuple[str, str]]) -> str:
    return "\n".join(
        f"<li><strong>{escape(display_name)}</strong> <span class=\"muted\">({escape(field_name)})</span></li>"
        for field_name, display_name in items
    )


def render_null_ties(items: list[tuple[str, str]], null_value: int) -> str:
    joined = ", ".join(
        f"{escape(display_name)} <span class=\"muted\">({escape(field_name)})</span>"
        for field_name, display_name in items
    )
    return f"{joined} <span class=\"muted\">({format_int_es(null_value)} nulos cada una)</span>"


def build_html(report: ReportData) -> str:
    date_names = [display_name for _, display_name in report.date_fields]
    numeric_names = [display_name for _, display_name in report.numeric_fields]
    text_names = [display_name for _, display_name in report.text_fields]
    max_null_count = report.null_counts[report.max_null_fields[0][0]] if report.max_null_fields else 0

    html_doc = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Respuestas EDA - {escape(report.dataset_id)}</title>
  <style>
    :root {{
      --bg: #0f172a;
      --panel: #111827;
      --card: #1f2937;
      --text: #e5e7eb;
      --muted: #9ca3af;
      --accent: #38bdf8;
      --accent-2: #34d399;
      --line: #334155;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Arial, Helvetica, sans-serif;
      background: linear-gradient(180deg, #0b1120 0%, #111827 100%);
      color: var(--text);
      line-height: 1.5;
    }}
    .wrap {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }}
    .hero {{
      background: linear-gradient(135deg, rgba(56,189,248,.18), rgba(52,211,153,.14));
      border: 1px solid rgba(148,163,184,.22);
      border-radius: 20px;
      padding: 28px;
      margin-bottom: 20px;
      box-shadow: 0 18px 50px rgba(0,0,0,.22);
    }}
    .badge {{
      display: inline-block;
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(56,189,248,.15);
      color: #bae6fd;
      font-size: 12px;
      letter-spacing: .02em;
      margin-bottom: 12px;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 32px;
    }}
    .subtle {{
      color: var(--muted);
      margin: 0;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 14px;
      margin: 20px 0;
    }}
    .card {{
      background: rgba(17,24,39,.85);
      border: 1px solid rgba(148,163,184,.16);
      border-radius: 16px;
      padding: 16px;
    }}
    .card h2, .section h2 {{
      margin: 0 0 10px;
      font-size: 20px;
    }}
    .metric {{
      font-size: 30px;
      font-weight: 700;
      color: var(--accent);
      margin: 2px 0 4px;
    }}
    .label {{
      color: var(--muted);
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: .05em;
    }}
    .section {{
      background: rgba(17,24,39,.72);
      border: 1px solid rgba(148,163,184,.14);
      border-radius: 18px;
      padding: 20px;
      margin-top: 18px;
    }}
    .section p, .section li {{
      margin-top: 6px;
      margin-bottom: 6px;
    }}
    ul {{
      padding-left: 20px;
      margin: 10px 0 0;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 12px;
    }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 10px 8px;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      color: #cbd5e1;
      font-weight: 700;
      font-size: 14px;
    }}
    .muted {{
      color: var(--muted);
    }}
    .kicker {{
      color: var(--accent-2);
      font-weight: 700;
    }}
    .callout {{
      background: rgba(52,211,153,.09);
      border-left: 4px solid var(--accent-2);
      padding: 12px 14px;
      border-radius: 10px;
      margin-top: 14px;
    }}
    .footer {{
      color: var(--muted);
      font-size: 12px;
      margin-top: 20px;
      text-align: right;
    }}
    @media print {{
      body {{ background: white; color: #111827; }}
      .hero, .card, .section {{ box-shadow: none; background: white; color: #111827; }}
      .muted {{ color: #4b5563; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div class="badge">Reporte EDA automatizado</div>
      <h1>Respuestas a las preguntas 3 a 14</h1>
      <p class="subtle">Fuente: {escape(report.dataset_name)} <span class="muted">({escape(report.dataset_id)})</span></p>
      <p class="subtle">Este informe se calculó contra la API oficial de datos.gov.co, que contiene el dataset completo de SECOP II. El parquet incluido en el repositorio es solo una muestra.</p>
    </div>

    <div class="grid">
      <div class="card">
        <div class="label">3. Registros totales</div>
        <div class="metric">{format_int_es(report.total_records)}</div>
        <div class="muted">Cantidad total de registros</div>
      </div>
      <div class="card">
        <div class="label">4. Variables totales</div>
        <div class="metric">{format_int_es(report.total_variables)}</div>
        <div class="muted">Cantidad total de variables</div>
      </div>
      <div class="card">
        <div class="label">5. Fechas</div>
        <div class="metric">{format_int_es(len(report.date_fields))}</div>
        <div class="muted">Variables de tipo fecha</div>
      </div>
      <div class="card">
        <div class="label">6. Numéricas</div>
        <div class="metric">{format_int_es(len(report.numeric_fields))}</div>
        <div class="muted">Variables de tipo numérico</div>
      </div>
      <div class="card">
        <div class="label">7. Texto</div>
        <div class="metric">{format_int_es(len(report.text_fields))}</div>
        <div class="muted">Variables de tipo texto</div>
      </div>
    </div>

    <div class="section">
      <h2>Respuestas puntuales</h2>
      <table>
        <thead>
          <tr>
            <th>Pregunta</th>
            <th>Respuesta</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>5. ¿Cuántas y cuáles variables de tipo fecha existen?</td>
            <td><strong>{format_int_es(len(report.date_fields))}</strong>: {escape(", ".join(date_names))}</td>
          </tr>
          <tr>
            <td>6. ¿Cuántas y cuáles variables de tipo numérico existen?</td>
            <td><strong>{format_int_es(len(report.numeric_fields))}</strong>: {escape(", ".join(numeric_names))}</td>
          </tr>
          <tr>
            <td>7. ¿Cuántas y cuáles variables de tipo texto existen?</td>
            <td><strong>{format_int_es(len(report.text_fields))}</strong>: {escape(", ".join(text_names))}</td>
          </tr>
          <tr>
            <td>8. ¿Qué variable tiene la mayor cantidad de registros nulos?</td>
            <td>{render_null_ties(report.max_null_fields, max_null_count)}</td>
          </tr>
          <tr>
            <td>9. ¿Qué porcentaje de registros nulos tiene la variable Fecha de Firma?</td>
            <td><strong>{format_decimal_es(report.fecha_de_firma_null_pct)}%</strong></td>
          </tr>
          <tr>
            <td>10. ¿Cuántos registros nulos tiene la variable Fecha Inicio Liquidación?</td>
            <td><strong>{format_int_es(report.fecha_inicio_liquidacion_nulls)}</strong></td>
          </tr>
          <tr>
            <td>11. ¿Cuál es el valor máximo de la variable Días adicionados?</td>
            <td><strong>{format_int_es(report.max_dias_adicionados)}</strong></td>
          </tr>
          <tr>
            <td>12. ¿Cuál es el valor más alto de la variable Valor del Contrato?</td>
            <td><strong>{format_int_es(report.max_valor_del_contrato)}</strong></td>
          </tr>
          <tr>
            <td>13. ¿Cuál es el séptimo valor más alto de la variable Valor del Contrato?</td>
            <td><strong>{format_int_es(report.septimo_valor_del_contrato)}</strong></td>
          </tr>
          <tr>
            <td>14. ¿Cuáles son los valores mínimo y máximo de la variable Fecha de Firma?</td>
            <td><strong>{escape(report.fecha_de_firma_min)}</strong> y <strong>{escape(report.fecha_de_firma_max)}</strong></td>
          </tr>
        </tbody>
      </table>
    </div>

    <div class="section">
      <h2>Resumen de clasificación</h2>
      <div class="callout">
        <div class="kicker">Nota metodológica</div>
        <p>Para este reporte se usaron los tipos oficiales del esquema: <code>calendar_date</code> para fechas, <code>number</code> para numéricos y <code>text</code> + <code>url</code> para texto. Así las 84 variables quedan clasificadas en 7 fechas, 19 numéricas y 58 de texto.</p>
      </div>
      <p><strong>Fechas:</strong> {escape(", ".join(date_names))}</p>
      <p><strong>Numéricas:</strong> {escape(", ".join(numeric_names))}</p>
      <p><strong>Texto:</strong> {escape(", ".join(text_names))}</p>
    </div>

    <div class="section">
      <h2>Contexto de nulos</h2>
      <p>La(s) variable(s) con más nulos son: {render_null_ties(report.max_null_fields, max_null_count)}.</p>
      <p>Esto indica que hay columnas completamente vacías en el dataset, lo cual es normal en algunos campos opcionales del formulario SECOP.</p>
    </div>

    <div class="footer">
      Generado automáticamente por <code>Fase_1/EDA.py</code>
    </div>
  </div>
</body>
</html>
"""
    return html_doc


def save_report(report: ReportData, output_path: Path) -> Path:
    html_doc = build_html(report)
    output_path.write_text(html_doc, encoding="utf-8")
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Genera un informe HTML con las respuestas EDA del dataset SECOP II."
    )
    parser.add_argument(
        "--dataset-id",
        default=DEFAULT_DATASET_ID,
        help="ID del dataset de datos.gov.co (por defecto: jbjy-vk9h).",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Ruta del archivo HTML de salida.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = build_report_data(args.dataset_id)
    output_path = save_report(report, Path(args.output))

    print(f"Reporte generado en: {output_path}")
    print(f"Registros: {format_int_es(report.total_records)}")
    print(f"Variables: {format_int_es(report.total_variables)}")
    print(f"Fechas: {format_int_es(len(report.date_fields))}")
    print(f"Numéricas: {format_int_es(len(report.numeric_fields))}")
    print(f"Texto: {format_int_es(len(report.text_fields))}")


if __name__ == "__main__":
    main()
