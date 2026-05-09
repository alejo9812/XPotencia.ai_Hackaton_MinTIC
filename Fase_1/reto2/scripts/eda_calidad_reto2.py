from __future__ import annotations

import argparse
import json
import re
import unicodedata
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "SECOP_II_-_Archivos_Descarga_Desde_2025_20260508.csv"
DEFAULT_PARQUET_DIR = ROOT / "parquet"
DEFAULT_REPORT_DIR = ROOT / "reports"
DEFAULT_HTML_OUTPUT = DEFAULT_REPORT_DIR / "reto2_eda_calidad.html"
DEFAULT_PARQUET_OUTPUT = DEFAULT_PARQUET_DIR / "SECOP_II_-_Archivos_Descarga_Desde_2025_20260508.parquet"

SPECIAL_ALIASES = {
    "id_documento": ["id_documento", "id documento", "documento", "id"],
    "n_mero_de_contrato": ["numero de contrato", "numero contrato", "n_mero_de_contrato"],
    "proceso": ["proceso"],
    "nombre_archivo": ["nombre archivo", "nombre_archivo"],
    "tamanno_archivo": ["tamano archivo", "tamanno_archivo", "tamano", "peso"],
    "extensi_n": ["extension", "extensi_n"],
    "descripci_n": ["descripcion", "descripci_n", "objeto"],
    "fecha_carga": ["fecha carga", "fecha_carga"],
    "entidad": ["entidad"],
    "nit_entidad": ["nit entidad", "nit_entidad", "nit"],
    "url_descarga_documento": ["url descarga documento", "url_descarga_documento", "url"],
}


@dataclass
class ColumnMeta:
    name: str
    data_type: str


def sql_literal(value: Path | str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def format_int_es(value: int) -> str:
    return f"{value:,}".replace(",", ".")


def format_float_es(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}".replace(".", ",")


def normalize_text(value: str) -> str:
    text = unicodedata.normalize("NFKD", value)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def ensure_output_dirs() -> None:
    DEFAULT_PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    DEFAULT_REPORT_DIR.mkdir(parents=True, exist_ok=True)


def load_questions(path: Path | None) -> list[str]:
    if path is None:
        return []

    raw = json.loads(path.read_text(encoding="utf-8"))
    questions: list[str] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, str):
                questions.append(item)
            elif isinstance(item, dict) and "question" in item:
                questions.append(str(item["question"]))
    return questions


def extract_json_from_questions(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []

    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        return []

    items: list[dict[str, Any]] = []
    for item in raw:
        if isinstance(item, dict) and "question" in item:
            items.append(item)
        elif isinstance(item, str):
            items.append({"question": item})
    return items


def resolve_column(question: str, columns: list[ColumnMeta]) -> ColumnMeta | None:
    normalized_question = normalize_text(question)

    for column in columns:
        normalized_column = normalize_text(column.name)
        if normalized_column and normalized_column in normalized_question:
            return column

    for canonical, aliases in SPECIAL_ALIASES.items():
        if canonical not in {column.name for column in columns}:
            continue
        for alias in aliases:
            if normalize_text(alias) in normalized_question:
                return next(column for column in columns if column.name == canonical)

    return None


def classify_type(data_type: str) -> str:
    normalized = data_type.lower()
    if any(token in normalized for token in ["int", "double", "decimal", "real", "float", "number", "bigint", "smallint", "tinyint"]):
        return "numerica"
    if any(token in normalized for token in ["date", "time", "timestamp"]):
        return "fecha"
    if "bool" in normalized:
        return "booleana"
    return "texto"


def list_columns_by_kind(columns: list[ColumnMeta], kind: str) -> list[str]:
    return [column.name for column in columns if classify_type(column.data_type) == kind]


def describe_schema(con: duckdb.DuckDBPyConnection, parquet_path: Path) -> list[ColumnMeta]:
    rows = con.execute(f"DESCRIBE SELECT * FROM parquet_scan({sql_literal(parquet_path)})").fetchall()
    return [ColumnMeta(name=row[0], data_type=row[1]) for row in rows]


def convert_csv_to_parquet(con: duckdb.DuckDBPyConnection, csv_path: Path, parquet_path: Path) -> bool:
    if parquet_path.exists() and parquet_path.stat().st_mtime >= csv_path.stat().st_mtime:
        return False

    query = f"""
        COPY (
            SELECT *
            FROM read_csv_auto(
                {sql_literal(csv_path)},
                HEADER=true,
                SAMPLE_SIZE=-1
            )
        )
        TO {sql_literal(parquet_path)}
        (FORMAT PARQUET, COMPRESSION ZSTD);
    """
    con.execute(query)
    return True


def build_column_stats(con: duckdb.DuckDBPyConnection, parquet_path: Path, columns: list[ColumnMeta]) -> dict[str, Any]:
    table = f"parquet_scan({sql_literal(parquet_path)})"
    total_rows = int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
    total_columns = len(columns)

    null_counts: list[tuple[str, int, float]] = []
    for column in columns:
        nulls = int(con.execute(f'SELECT count(*) - count("{column.name}") FROM {table}').fetchone()[0])
        pct = (nulls / total_rows) * 100 if total_rows else 0.0
        null_counts.append((column.name, nulls, pct))

    null_counts.sort(key=lambda item: (-item[2], -item[1], item[0].lower()))

    numeric_summaries: list[dict[str, Any]] = []
    text_summaries: list[dict[str, Any]] = []
    date_summaries: list[dict[str, Any]] = []

    for column in columns:
        kind = classify_type(column.data_type)
        quoted = f'"{column.name}"'

        if kind == "numerica":
            row = con.execute(
                f"""
                SELECT
                    min({quoted}) AS min_value,
                    max({quoted}) AS max_value,
                    avg({quoted}) AS avg_value,
                    median({quoted}) AS median_value
                FROM {table}
                WHERE {quoted} IS NOT NULL
                """
            ).fetchone()
            numeric_summaries.append(
                {
                    "column": column.name,
                    "min": row[0],
                    "max": row[1],
                    "avg": row[2],
                    "median": row[3],
                }
            )
        elif kind == "texto":
            row = con.execute(
                f"""
                SELECT
                    count(DISTINCT {quoted}) AS distinct_count
                FROM {table}
                WHERE {quoted} IS NOT NULL
                """
            ).fetchone()
            text_summaries.append({"column": column.name, "distinct_count": int(row[0])})
        elif kind == "fecha":
            row = con.execute(
                f"""
                SELECT
                    min({quoted}) AS min_value,
                    max({quoted}) AS max_value
                FROM {table}
                WHERE {quoted} IS NOT NULL
                """
            ).fetchone()
            date_summaries.append({"column": column.name, "min": row[0], "max": row[1]})

    critical_missing_rows = None
    critical_columns = [
        "id_documento",
        "proceso",
        "nombre_archivo",
        "fecha_carga",
    ]
    if all(any(column.name == required for column in columns) for required in critical_columns):
        where_clause = " OR ".join(f'"{name}" IS NULL' for name in critical_columns)
        critical_missing_rows = int(con.execute(f"SELECT count(*) FROM {table} WHERE {where_clause}").fetchone()[0])

    duplicate_id_rows = None
    if any(column.name == "id_documento" for column in columns):
        duplicate_id_rows = int(
            con.execute(
                f"""
                SELECT count(*) - count(DISTINCT "id_documento")
                FROM {table}
                WHERE "id_documento" IS NOT NULL
                """
            ).fetchone()[0]
        )

    top_null_column = null_counts[0] if null_counts else None
    return {
        "total_rows": total_rows,
        "total_columns": total_columns,
        "null_counts": null_counts,
        "numeric_summaries": numeric_summaries,
        "text_summaries": text_summaries,
        "date_summaries": date_summaries,
        "critical_missing_rows": critical_missing_rows,
        "duplicate_id_rows": duplicate_id_rows,
        "top_null_column": top_null_column,
    }


def answer_question(
    question: str,
    con: duckdb.DuckDBPyConnection,
    parquet_path: Path,
    columns: list[ColumnMeta],
    stats: dict[str, Any],
) -> str:
    q = normalize_text(question)
    table = f"parquet_scan({sql_literal(parquet_path)})"

    if any(phrase in q for phrase in ["total de registros", "numero de registros", "cantidad de registros", "total de filas"]):
        return format_int_es(stats["total_rows"])

    if any(phrase in q for phrase in ["cuantas columnas", "numero de columnas", "total de columnas"]):
        return format_int_es(stats["total_columns"])

    if "columnas" in q and any(word in q for word in ["tipo", "dato", "int", "texto", "fecha", "numerica"]):
        if any(word in q for word in ["int", "numerica", "numero", "numerico"]):
            return ", ".join(list_columns_by_kind(columns, "numerica")) or "No se detectaron columnas numericas."
        if any(word in q for word in ["fecha", "date", "time"]):
            return ", ".join(list_columns_by_kind(columns, "fecha")) or "No se detectaron columnas de fecha."
        return ", ".join(list_columns_by_kind(columns, "texto")) or "No se detectaron columnas de texto."

    if "nulos" in q:
        column = resolve_column(question, columns)
        if column:
            nulls = int(con.execute(f'SELECT count(*) - count("{column.name}") FROM {table}').fetchone()[0])
            return f"{format_int_es(nulls)} valores nulos en {column.name}"

    if any(word in q for word in ["maximo", "minimo", "media", "mediana", "rango"]) and columns:
        column = resolve_column(question, columns)
        if column:
            quoted = f'"{column.name}"'
            kind = classify_type(column.data_type)
            if kind == "numerica":
                row = con.execute(
                    f"""
                    SELECT
                        min({quoted}),
                        max({quoted}),
                        avg({quoted}),
                        median({quoted})
                    FROM {table}
                    WHERE {quoted} IS NOT NULL
                    """
                ).fetchone()
                parts = [
                    f"Minimo: {row[0]}",
                    f"Maximo: {row[1]}",
                    f"Media: {format_float_es(float(row[2])) if row[2] is not None else 'N/D'}",
                    f"Mediana: {format_float_es(float(row[3])) if row[3] is not None else 'N/D'}",
                ]
                return " | ".join(parts)
            if kind == "fecha":
                row = con.execute(
                    f"""
                    SELECT min({quoted}), max({quoted})
                    FROM {table}
                    WHERE {quoted} IS NOT NULL
                    """
                ).fetchone()
                return f"Minimo: {row[0]} | Maximo: {row[1]}"

    if "fecha carga" in q or "rango de fecha" in q:
        if any(column.name == "fecha_carga" for column in columns):
            row = con.execute(
                f"""
                SELECT min("fecha_carga"), max("fecha_carga")
                FROM {table}
                WHERE "fecha_carga" IS NOT NULL
                """
            ).fetchone()
            return f"Minimo: {row[0]} | Maximo: {row[1]}"

    if "id documento" in q and any(ch.isdigit() for ch in question):
        match = re.search(r"\b\d{4,}\b", question)
        if match and any(column.name == "id_documento" for column in columns):
            id_value = match.group(0)
            rows = con.execute(
                f"""
                SELECT nombre_archivo, fecha_carga
                FROM {table}
                WHERE "id_documento" = {id_value}
                ORDER BY fecha_carga ASC
                LIMIT 20
                """
            ).fetchall()
            if not rows:
                return "No se encontraron registros para ese ID."
            return "; ".join(f"{row[0]} ({row[1]})" for row in rows)

    return "No pude inferir una respuesta exacta para esa pregunta."


def render_rows(rows: list[tuple[Any, ...]]) -> str:
    return "\n".join(
        f"""
        <tr>
          <td>{escape(str(row[0]))}</td>
          <td>{escape(str(row[1]))}</td>
          <td>{escape(str(row[2]))}</td>
        </tr>
        """
        for row in rows
    )


def render_numeric_rows(items: list[dict[str, Any]]) -> str:
    if not items:
        return "<tr><td colspan='5'>No se detectaron columnas numericas.</td></tr>"

    return "\n".join(
        f"""
        <tr>
          <td>{escape(str(item["column"]))}</td>
          <td>{escape(str(item["min"]))}</td>
          <td>{escape(str(item["max"]))}</td>
          <td>{escape(format_float_es(float(item["avg"]))) if item["avg"] is not None else "N/D"}</td>
          <td>{escape(format_float_es(float(item["median"]))) if item["median"] is not None else "N/D"}</td>
        </tr>
        """
        for item in items
    )


def render_text_rows(items: list[dict[str, Any]]) -> str:
    if not items:
        return "<tr><td colspan='2'>No se detectaron columnas de texto.</td></tr>"
    return "\n".join(
        f"""
        <tr>
          <td>{escape(str(item["column"]))}</td>
          <td>{format_int_es(int(item["distinct_count"]))}</td>
        </tr>
        """
        for item in items
    )


def render_date_rows(items: list[dict[str, Any]]) -> str:
    if not items:
        return "<tr><td colspan='3'>No se detectaron columnas de fecha.</td></tr>"
    return "\n".join(
        f"""
        <tr>
          <td>{escape(str(item["column"]))}</td>
          <td>{escape(str(item["min"]))}</td>
          <td>{escape(str(item["max"]))}</td>
        </tr>
        """
        for item in items
    )


def build_question_rows(
    question_items: list[dict[str, Any]],
    con: duckdb.DuckDBPyConnection,
    parquet_path: Path,
    columns: list[ColumnMeta],
    stats: dict[str, Any],
) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    for item in question_items:
        question = str(item["question"])
        answer = answer_question(question, con, parquet_path, columns, stats)
        rows.append((question, answer))
    return rows


def build_html(
    parquet_path: Path,
    source_path: Path,
    columns: list[ColumnMeta],
    stats: dict[str, Any],
    question_rows: list[tuple[str, str]],
) -> str:
    top_null = stats["top_null_column"]
    top_null_text = "N/D"
    if top_null:
        top_null_text = f"{top_null[0]} ({format_float_es(top_null[2])}%)"

    critical_rows = stats["critical_missing_rows"]
    duplicate_rows = stats["duplicate_id_rows"]

    question_html = ""
    if question_rows:
        question_html = f"""
        <section class="section">
          <h2>Respuestas a preguntas</h2>
          <table>
            <thead>
              <tr>
                <th>Pregunta</th>
                <th>Respuesta</th>
              </tr>
            </thead>
            <tbody>
              {''.join(f'<tr><td>{escape(q)}</td><td>{escape(a)}</td></tr>' for q, a in question_rows)}
            </tbody>
          </table>
        </section>
        """

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Reto 2 - EDA y calidad de datos</title>
  <style>
    :root {{
      --bg: #0b1120;
      --panel: rgba(17, 24, 39, .85);
      --line: rgba(148, 163, 184, .18);
      --text: #e5e7eb;
      --muted: #9ca3af;
      --accent: #38bdf8;
      --accent-2: #34d399;
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
    .hero, .section, .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 18px 50px rgba(0, 0, 0, .2);
    }}
    .hero {{
      background: linear-gradient(135deg, rgba(56, 189, 248, .18), rgba(52, 211, 153, .14));
      margin-bottom: 18px;
    }}
    h1, h2 {{ margin: 0 0 12px; }}
    .subtle, .muted {{ color: var(--muted); }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 14px;
      margin: 18px 0;
    }}
    .metric {{ font-size: 30px; font-weight: 700; color: var(--accent-2); }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 10px;
    }}
    th, td {{
      border-bottom: 1px solid rgba(148, 163, 184, .16);
      padding: 10px 8px;
      text-align: left;
      vertical-align: top;
    }}
    th {{ color: #cbd5e1; }}
    .section {{ margin-top: 18px; }}
    .footer {{
      color: var(--muted);
      font-size: 12px;
      margin-top: 20px;
      text-align: right;
    }}
    code {{
      background: rgba(15, 23, 42, .82);
      padding: 2px 6px;
      border-radius: 6px;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div class="subtle">Reto 2 | SECOP II</div>
      <h1>EDA y calidad de datos</h1>
      <p class="subtle">Origen: {escape(str(source_path))}</p>
      <p class="subtle">Parquet generado: {escape(str(parquet_path))}</p>
      <p class="subtle">El analisis se hace con DuckDB sobre el formato columnar para no cargar todo el CSV en memoria.</p>
    </div>

    <div class="grid">
      <div class="card">
        <div class="muted">Registros</div>
        <div class="metric">{format_int_es(stats["total_rows"])}</div>
      </div>
      <div class="card">
        <div class="muted">Columnas</div>
        <div class="metric">{format_int_es(stats["total_columns"])}</div>
      </div>
      <div class="card">
        <div class="muted">Mayor nulidad</div>
        <div class="metric">{escape(top_null_text)}</div>
      </div>
      <div class="card">
        <div class="muted">Fila critica incompleta</div>
        <div class="metric">{escape("N/D" if critical_rows is None else format_int_es(int(critical_rows)))}</div>
      </div>
      <div class="card">
        <div class="muted">Duplicados por ID</div>
        <div class="metric">{escape("N/D" if duplicate_rows is None else format_int_es(int(duplicate_rows)))}</div>
      </div>
      <div class="card">
        <div class="muted">Columnas con nulos</div>
        <div class="metric">{format_int_es(sum(1 for _, nulls, _ in stats["null_counts"] if nulls > 0))}</div>
      </div>
    </div>

    <section class="section">
      <h2>Esquema detectado</h2>
      <table>
        <thead>
          <tr>
            <th>Columna</th>
            <th>Tipo</th>
          </tr>
        </thead>
        <tbody>
          {''.join(f'<tr><td>{escape(column.name)}</td><td>{escape(column.data_type)}</td></tr>' for column in columns)}
        </tbody>
      </table>
    </section>

    <section class="section">
      <h2>Calidad de datos</h2>
      <table>
        <thead>
          <tr>
            <th>Columna</th>
            <th>Nulos</th>
            <th>% nulos</th>
          </tr>
        </thead>
        <tbody>
          {''.join(f'<tr><td>{escape(name)}</td><td>{format_int_es(nulls)}</td><td>{format_float_es(pct)}%</td></tr>' for name, nulls, pct in stats["null_counts"])}
        </tbody>
      </table>
    </section>

    <section class="section">
      <h2>Resumen numerico</h2>
      <table>
        <thead>
          <tr>
            <th>Columna</th>
            <th>Minimo</th>
            <th>Maximo</th>
            <th>Media</th>
            <th>Mediana</th>
          </tr>
        </thead>
        <tbody>
          {render_numeric_rows(stats["numeric_summaries"])}
        </tbody>
      </table>
    </section>

    <section class="section">
      <h2>Resumen de texto</h2>
      <table>
        <thead>
          <tr>
            <th>Columna</th>
            <th>Valores distintos</th>
          </tr>
        </thead>
        <tbody>
          {render_text_rows(stats["text_summaries"])}
        </tbody>
      </table>
    </section>

    <section class="section">
      <h2>Resumen de fechas</h2>
      <table>
        <thead>
          <tr>
            <th>Columna</th>
            <th>Minimo</th>
            <th>Maximo</th>
          </tr>
        </thead>
        <tbody>
          {render_date_rows(stats["date_summaries"])}
        </tbody>
      </table>
    </section>

    {question_html}

    <div class="footer">
      Generado automaticamente por <code>Fase_1/reto2/scripts/eda_calidad_reto2.py</code>
    </div>
  </div>
</body>
</html>
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convierte el CSV a Parquet y genera un reporte HTML de EDA y calidad de datos.")
    parser.add_argument("--input", help="Ruta del CSV a analizar. Por defecto usa el archivo local del reto 2.")
    parser.add_argument("--parquet-output", help="Ruta del Parquet de salida.")
    parser.add_argument("--output", help="Ruta del HTML de salida.")
    parser.add_argument("--questions-file", help="JSON con preguntas a responder. Puede ser una lista de textos o de objetos con la llave question.")
    parser.add_argument("--question", action="append", dest="questions", help="Pregunta individual para responder. Se puede repetir.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_output_dirs()

    input_path = Path(args.input) if args.input else DEFAULT_INPUT
    parquet_path = Path(args.parquet_output) if args.parquet_output else DEFAULT_PARQUET_OUTPUT
    output_path = Path(args.output) if args.output else DEFAULT_HTML_OUTPUT

    if not input_path.exists():
        raise FileNotFoundError(f"No existe el archivo de entrada: {input_path}")

    con = duckdb.connect(database=":memory:")
    try:
        converted = convert_csv_to_parquet(con, input_path, parquet_path)
    except Exception as exc:  # pragma: no cover - friendly runtime message
        raise RuntimeError(
            f"No pude leer o convertir el CSV. Ahora mismo parece estar bloqueado o en uso.\n"
            f"Archivo: {input_path}\n"
            f"Detalle: {exc}\n"
            f"Cierra Excel u otro proceso que lo tenga abierto y vuelve a ejecutar el script."
        ) from exc

    columns = describe_schema(con, parquet_path)
    stats = build_column_stats(con, parquet_path, columns)

    question_items = extract_json_from_questions(Path(args.questions_file)) if args.questions_file else []
    for question in args.questions or []:
        question_items.append({"question": question})
    question_rows = build_question_rows(question_items, con, parquet_path, columns, stats) if question_items else []

    output_path.write_text(build_html(parquet_path, input_path, columns, stats, question_rows), encoding="utf-8")

    print(f"Archivo de entrada: {input_path}")
    print(f"Parquet generado: {parquet_path}")
    print(f"Reporte HTML: {output_path}")
    print(f"Registros: {format_int_es(stats['total_rows'])}")
    print(f"Columnas: {format_int_es(stats['total_columns'])}")
    if converted:
        print("Conversion: completada")
    else:
        print("Conversion: reutilizada desde el Parquet existente")
    if question_rows:
        print("Preguntas resueltas:")
        for question, answer in question_rows:
            print(f"- {question}: {answer}")


if __name__ == "__main__":
    main()
