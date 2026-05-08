from __future__ import annotations

import argparse
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Iterable

import pandas as pd


BASE1_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_CANDIDATES = (
    BASE1_ROOT / "meta" / "muestra_1000.csv",
    BASE1_ROOT / "meta" / "muestra_1000.parquet",
)
DEFAULT_OUTPUT = BASE1_ROOT / "reports" / "calidad_datos.html"
DEFAULT_NA_TOKENS = (
    "",
    "NA",
    "N/A",
    "None",
    "null",
    "No definido",
    "No Definido",
    "No aplica",
    "Sin Descripcion",
    "Sin Descripcin",
    "Sin Descripcion",
)


@dataclass
class FieldQuality:
    column_name: str
    data_type: str
    non_null_count: int
    null_count: int
    null_pct: float
    unique_count: int


@dataclass
class QualityReport:
    source_name: str
    total_records: int
    total_variables: int
    fields: list[FieldQuality]


def format_int_es(value: int) -> str:
    return f"{value:,}".replace(",", ".")


def format_decimal_es(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}".replace(".", ",")


def resolve_input_path(raw_path: str | None) -> Path:
    if raw_path:
        input_path = Path(raw_path)
        if not input_path.exists():
            raise FileNotFoundError(f"No existe el archivo de entrada: {input_path}")
        return input_path

    for candidate in DEFAULT_INPUT_CANDIDATES:
        if candidate.exists():
            return candidate

    raise FileNotFoundError(
        "No encontre un archivo de entrada. Usa --input con un CSV o Parquet valido."
    )


def normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    na_tokens = {token.strip().lower() for token in DEFAULT_NA_TOKENS if token}

    def normalize_value(value):
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned.lower() in na_tokens:
                return pd.NA
            return cleaned
        return value

    object_columns = [
        column
        for column in df.columns
        if pd.api.types.is_object_dtype(df[column]) or pd.api.types.is_string_dtype(df[column])
    ]

    for column in object_columns:
        df[column] = df[column].map(normalize_value)

    return df


def read_dataset(input_path: Path) -> pd.DataFrame:
    if input_path.suffix.lower() == ".csv":
        df = pd.read_csv(
            input_path,
            encoding="utf-8-sig",
            low_memory=False,
            na_values=[token for token in DEFAULT_NA_TOKENS if token],
            keep_default_na=True,
        )
    elif input_path.suffix.lower() == ".parquet":
        df = pd.read_parquet(input_path)
    else:
        raise ValueError(f"Formato no soportado: {input_path.suffix}")

    return normalize_frame(df)


def detect_data_type(series: pd.Series) -> str:
    if pd.api.types.is_datetime64_any_dtype(series):
        return "fecha"
    if pd.api.types.is_bool_dtype(series):
        return "booleana"
    if pd.api.types.is_numeric_dtype(series):
        return "numerica"
    return "texto"


def build_quality_report(df: pd.DataFrame, source_name: str) -> QualityReport:
    total_records = len(df)
    fields: list[FieldQuality] = []

    for column_name in df.columns:
        series = df[column_name]
        non_null_count = int(series.notna().sum())
        null_count = int(series.isna().sum())
        null_pct = (null_count / total_records) * 100 if total_records else 0.0
        unique_count = int(series.nunique(dropna=True))

        fields.append(
            FieldQuality(
                column_name=column_name,
                data_type=detect_data_type(series),
                non_null_count=non_null_count,
                null_count=null_count,
                null_pct=null_pct,
                unique_count=unique_count,
            )
        )

    fields.sort(key=lambda item: (-item.null_pct, -item.null_count, item.column_name.lower()))

    return QualityReport(
        source_name=source_name,
        total_records=total_records,
        total_variables=len(df.columns),
        fields=fields,
    )


def render_summary_cards(report: QualityReport) -> str:
    columns_with_nulls = sum(1 for field in report.fields if field.null_count > 0)
    columns_without_nulls = report.total_variables - columns_with_nulls
    average_null_pct = (
        sum(field.null_pct for field in report.fields) / report.total_variables
        if report.total_variables
        else 0.0
    )
    highest_null_field = report.fields[0] if report.fields else None

    return f"""
    <div class="grid">
      <div class="card">
        <div class="label">Registros</div>
        <div class="metric">{format_int_es(report.total_records)}</div>
        <div class="muted">Filas analizadas</div>
      </div>
      <div class="card">
        <div class="label">Variables</div>
        <div class="metric">{format_int_es(report.total_variables)}</div>
        <div class="muted">Columnas analizadas</div>
      </div>
      <div class="card">
        <div class="label">Con nulos</div>
        <div class="metric">{format_int_es(columns_with_nulls)}</div>
        <div class="muted">Columnas con al menos un nulo</div>
      </div>
      <div class="card">
        <div class="label">Sin nulos</div>
        <div class="metric">{format_int_es(columns_without_nulls)}</div>
        <div class="muted">Columnas limpias</div>
      </div>
      <div class="card">
        <div class="label">Nulidad promedio</div>
        <div class="metric">{format_decimal_es(average_null_pct)}%</div>
        <div class="muted">Promedio por variable</div>
      </div>
      <div class="card">
        <div class="label">Mayor nulidad</div>
        <div class="metric">{format_decimal_es(highest_null_field.null_pct if highest_null_field else 0.0)}%</div>
        <div class="muted">{escape(highest_null_field.column_name if highest_null_field else "N/D")}</div>
      </div>
    </div>
    """


def render_top_nulls(report: QualityReport, top_n: int) -> str:
    items = report.fields[:top_n]
    if not items:
        return "<p>No hay variables para mostrar.</p>"

    return "".join(
        f"<li><strong>{escape(item.column_name)}</strong> <span class=\"muted\">({format_decimal_es(item.null_pct)}% nulos, {format_int_es(item.unique_count)} unicos)</span></li>"
        for item in items
    )


def render_rows(report: QualityReport) -> str:
    rows: list[str] = []
    for field in report.fields:
        rows.append(
            f"""
            <tr>
              <td><strong>{escape(field.column_name)}</strong></td>
              <td>{escape(field.data_type)}</td>
              <td>{format_int_es(field.non_null_count)}</td>
              <td>{format_int_es(field.null_count)}</td>
              <td>{format_decimal_es(field.null_pct)}%</td>
              <td>{format_int_es(field.unique_count)}</td>
            </tr>
            """
        )
    return "\n".join(rows)


def build_html(report: QualityReport, top_n: int) -> str:
    top_nulls_html = render_top_nulls(report, top_n)

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Calidad de Datos - {escape(report.source_name)}</title>
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
      max-width: 1280px;
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
    .metric {{
      font-size: 28px;
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
      font-size: 14px;
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
      font-size: 13px;
      position: sticky;
      top: 0;
      background: #111827;
    }}
    .muted {{
      color: var(--muted);
      font-size: 12px;
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
      th {{ background: white; color: #111827; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div class="badge">Reporte de calidad de datos</div>
      <h1>Analisis de nulidad y unicidad</h1>
      <p class="subtle">Fuente: {escape(report.source_name)}</p>
      <p class="subtle">Este informe calcula nulos por variable, porcentaje de nulidad y conteo de valores unicos sobre el archivo local seleccionado.</p>
    </div>

    {render_summary_cards(report)}

    <div class="section">
      <h2>Variables con mayor nulidad</h2>
      <div class="callout">
        <p>Top {format_int_es(top_n)} variables ordenadas por porcentaje de nulidad:</p>
        <ul>
          {top_nulls_html}
        </ul>
      </div>
    </div>

    <div class="section">
      <h2>Detalle por variable</h2>
      <table>
        <thead>
          <tr>
            <th>Variable</th>
            <th>Tipo</th>
            <th>No nulos</th>
            <th>Nulos</th>
            <th>% Nulos</th>
            <th>Unicos</th>
          </tr>
        </thead>
        <tbody>
          {render_rows(report)}
        </tbody>
      </table>
    </div>

    <div class="footer">
      Generado automaticamente por <code>Fase_1/calidad_datos.py</code>
    </div>
  </div>
</body>
</html>
"""


def save_report(report: QualityReport, output_path: Path, top_n: int) -> Path:
    output_path.write_text(build_html(report, top_n), encoding="utf-8")
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Genera un reporte HTML de calidad de datos a partir de un archivo local CSV o Parquet."
    )
    parser.add_argument(
        "--input",
        help="Ruta del archivo CSV o Parquet a analizar. Si se omite, usa la muestra del repositorio.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Ruta del archivo HTML de salida.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Cantidad de variables a mostrar en el resumen de mayor nulidad.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = resolve_input_path(args.input)
    df = read_dataset(input_path)
    report = build_quality_report(df, input_path.name)
    output_path = save_report(report, Path(args.output), max(1, args.top_n))

    print(f"Archivo analizado: {input_path}")
    print(f"Reporte generado en: {output_path}")
    print(f"Registros: {format_int_es(report.total_records)}")
    print(f"Variables: {format_int_es(report.total_variables)}")
    print("Top 5 variables con mayor nulidad:")
    for field in report.fields[:5]:
        print(
            f"- {field.column_name}: {format_int_es(field.null_count)} nulos, "
            f"{format_decimal_es(field.null_pct)}%, {format_int_es(field.unique_count)} unicos"
        )


if __name__ == "__main__":
    main()
