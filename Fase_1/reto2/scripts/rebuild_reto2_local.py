from __future__ import annotations

import json
import math
import subprocess
import sys
import unicodedata
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
REPORTS_SCRIPT = ROOT / "scripts" / "generate_reports_reto2.py"
CSV_GLOB = "SECOP_II_-_Contratos_*.csv"
PARQUET_DIR = ROOT / "parquet"
DASHBOARD_API_SUMMARY = ROOT / "dashboard" / "api" / "summary.json"
DASHBOARD_DATA_SUMMARY = ROOT / "dashboard" / "data" / "summary.json"


@dataclass
class RankedItem:
    name: str
    count: int | None = None
    total: int | None = None


def normalize_text(value: str) -> str:
    return unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii").lower()


def find_column(columns: list[str], needle: str) -> str:
    needle_norm = normalize_text(needle)
    for column in columns:
        if needle_norm in normalize_text(column):
            return column
    raise KeyError(f"No se encontro una columna para: {needle}")


def sql_quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def to_int(value) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        return int(value)
    return int(Decimal(str(value)))


def format_int_es(value: int) -> str:
    return f"{value:,}".replace(",", ".")


def format_pct_es(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}".replace(".", ",")


def format_float_es(value: float, digits: int = 2) -> str:
    return f"{value:,.{digits}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def main() -> None:
    csv_path = next(ROOT.glob(CSV_GLOB))
    parquet_path = PARQUET_DIR / f"{csv_path.stem}.parquet"
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW src AS
        SELECT *
        FROM read_csv_auto('{csv_path.as_posix()}', header=true, sample_size=2000, all_varchar=true)
        """
    )

    columns = [row[0] for row in con.execute("DESCRIBE SELECT * FROM src").fetchall()]
    entity_col = find_column(columns, "Nombre Entidad")
    dept_col = find_column(columns, "Departamento")
    tipo_col = find_column(columns, "Tipo de Contrato")
    modalidad_col = find_column(columns, "Modalidad de Contratacion")
    fecha_firma_col = find_column(columns, "Fecha de Firma")
    pyme_col = find_column(columns, "Es Pyme")
    pago_adelantado_col = find_column(columns, "Habilita Pago Adelantado")
    obligacion_ambiental_col = find_column(columns, "Obligacion Ambiental")
    gender_col = find_column(columns, "Genero Representante Legal")
    valor_col = find_column(columns, "Valor del Contrato")
    valor_adelantado_col = find_column(columns, "Valor de pago adelantado")
    valor_facturado_col = find_column(columns, "Valor Facturado")
    fecha_inicio_liq_col = find_column(columns, "Fecha Inicio Liquidacion")
    fecha_fin_liq_col = find_column(columns, "Fecha Fin Liquidacion")
    fecha_notif_prorroga_col = find_column(columns, "Fecha de notificacion de prorrogacion")
    objeto_col = find_column(columns, "Objeto del Contrato")

    entity_ident = sql_quote_identifier(entity_col)
    dept_ident = sql_quote_identifier(dept_col)
    tipo_ident = sql_quote_identifier(tipo_col)
    modalidad_ident = sql_quote_identifier(modalidad_col)
    fecha_firma_ident = sql_quote_identifier(fecha_firma_col)
    pyme_ident = sql_quote_identifier(pyme_col)
    pago_adelantado_ident = sql_quote_identifier(pago_adelantado_col)
    obligacion_ambiental_ident = sql_quote_identifier(obligacion_ambiental_col)
    gender_ident = sql_quote_identifier(gender_col)
    valor_ident = sql_quote_identifier(valor_col)
    valor_adelantado_ident = sql_quote_identifier(valor_adelantado_col)
    valor_facturado_ident = sql_quote_identifier(valor_facturado_col)
    fecha_inicio_liq_ident = sql_quote_identifier(fecha_inicio_liq_col)
    fecha_fin_liq_ident = sql_quote_identifier(fecha_fin_liq_col)
    fecha_notif_prorroga_ident = sql_quote_identifier(fecha_notif_prorroga_col)
    objeto_ident = sql_quote_identifier(objeto_col)

    amount_expr = (
        f"COALESCE(TRY_CAST(NULLIF(REGEXP_REPLACE({valor_ident}, '[^0-9]', '', 'g'), '') AS DECIMAL(38,0)), 0)"
    )

    con.execute(
        f"""
        COPY (
            SELECT *
            FROM src
        )
        TO '{parquet_path.as_posix()}'
        (FORMAT PARQUET)
        """
    )

    total_records = to_int(con.execute("SELECT COUNT(*) FROM src").fetchone()[0])
    total_variables = len(columns)

    records_2025 = to_int(
        con.execute(
            f"""
            SELECT COUNT(*)
            FROM src
            WHERE TRY_STRPTIME({fecha_firma_ident}, '%m/%d/%Y') >= DATE '2025-01-01'
              AND TRY_STRPTIME({fecha_firma_ident}, '%m/%d/%Y') < DATE '2026-01-01'
            """
        ).fetchone()[0]
    )

    pyme_yes = to_int(
        con.execute(
            f"""
            SELECT COUNT(*)
            FROM src
            WHERE lower(trim(coalesce({pyme_ident}, ''))) IN ('si', 'sí', 's', 'yes', 'true', '1')
            """
        ).fetchone()[0]
    )
    pyme_pct = (pyme_yes / total_records) * 100 if total_records else 0.0

    departments = [
        RankedItem(name=row[0], count=to_int(row[1]))
        for row in con.execute(
            f"""
            SELECT {dept_ident} AS department, COUNT(*) AS n
            FROM src
            WHERE trim(coalesce({dept_ident}, '')) <> ''
            GROUP BY 1
            ORDER BY n DESC, department ASC
            LIMIT 10
            """
        ).fetchall()
    ]
    top_department_6 = departments[5] if len(departments) >= 6 else RankedItem(name="", count=0)

    preferred_modality_row = con.execute(
        f"""
        SELECT {modalidad_ident} AS modality, COUNT(*) AS n
        FROM src
        WHERE trim(coalesce({modalidad_ident}, '')) <> ''
        GROUP BY 1
        ORDER BY n DESC, modality ASC
        LIMIT 1
        """
    ).fetchone()
    preferred_modality = (
        RankedItem(name=preferred_modality_row[0], count=to_int(preferred_modality_row[1]))
        if preferred_modality_row
        else RankedItem(name="", count=0)
    )

    contract_types = [
        RankedItem(name=row[0], count=to_int(row[1]))
        for row in con.execute(
            f"""
            SELECT {tipo_ident} AS contract_type, COUNT(*) AS n
            FROM src
            WHERE trim(coalesce({tipo_ident}, '')) <> ''
            GROUP BY 1
            ORDER BY n DESC, contract_type ASC
            LIMIT 5
            """
        ).fetchall()
    ]
    top_type_pct = (contract_types[0].count / total_records) * 100 if total_records and contract_types else 0.0

    top_entities = [
        RankedItem(name=row[0], total=to_int(row[1]))
        for row in con.execute(
            f"""
            WITH base AS (
              SELECT
                {entity_ident} AS entity_name,
                {amount_expr} AS amount
              FROM src
            )
            SELECT entity_name, SUM(amount) AS total_amount
            FROM base
            WHERE trim(coalesce(entity_name, '')) <> ''
            GROUP BY 1
            ORDER BY total_amount DESC, entity_name ASC
            LIMIT 3
            """
        ).fetchall()
    ]

    top_values = [
        {
            "entidad": row[0],
            "monto": to_int(row[1]),
            "tipo_de_contrato": row[2],
            "modalidad": row[3],
            "objeto": row[4],
            "veredicto": "Anomalo / por verificar",
            "sustento": (
                "El monto esta muy por encima del comportamiento general de la base y conviene cruzarlo "
                "con el documento origen antes de asumir que es correcto."
            ),
        }
        for row in con.execute(
            f"""
            SELECT
              {entity_ident} AS entity_name,
              {amount_expr} AS amount,
              {tipo_ident} AS contract_type,
              {modalidad_ident} AS modality,
              {objeto_ident} AS object_text
            FROM src
            WHERE trim(coalesce({entity_ident}, '')) <> ''
            ORDER BY amount DESC, entity_name ASC
            LIMIT 3
            """
        ).fetchall()
    ]

    advance_yes = to_int(
        con.execute(
            f"""
            SELECT COUNT(*)
            FROM src
            WHERE lower(trim(coalesce({pago_adelantado_ident}, ''))) IN ('si', 'sí', 's', 'yes', 'true', '1')
            """
        ).fetchone()[0]
    )
    advance_pct = (advance_yes / total_records) * 100 if total_records else 0.0

    env_yes = to_int(
        con.execute(
            f"""
            SELECT COUNT(*)
            FROM src
            WHERE lower(trim(coalesce({obligacion_ambiental_ident}, ''))) IN ('si', 'sí', 's', 'yes', 'true', '1')
            """
        ).fetchone()[0]
    )

    gender_rows = con.execute(
        f"""
        SELECT
          {gender_ident} AS gender,
          COUNT(*) AS n,
          SUM({amount_expr}) AS total_value,
          AVG({amount_expr}) AS avg_value
        FROM src
        WHERE trim(coalesce({gender_ident}, '')) <> ''
        GROUP BY 1
        ORDER BY total_value DESC NULLS LAST, n DESC
        """
    ).fetchall()
    gender_map = {
        row[0]: {
            "count": to_int(row[1]),
            "total": to_int(row[2]),
            "avg": float(row[3]) if row[3] is not None else 0.0,
        }
        for row in gender_rows
    }

    total_value = Decimal(
        str(
            con.execute(
                f"""
                SELECT COALESCE(SUM({amount_expr}), 0)
                FROM src
                """
            ).fetchone()[0]
        )
    )

    unique_entities = to_int(
        con.execute(
            f"""
            SELECT COUNT(DISTINCT {entity_ident})
            FROM src
            WHERE trim(coalesce({entity_ident}, '')) <> ''
            """
        ).fetchone()[0]
    )
    top20_entities = math.ceil(unique_entities * 0.2) if unique_entities else 0
    top20_sum = Decimal("0")
    if top20_entities:
        top20_sum = Decimal(
            str(
                con.execute(
                    f"""
                    WITH base AS (
                      SELECT
                        {entity_ident} AS entity_name,
                        {amount_expr} AS amount
                      FROM src
                    ),
                    ranked AS (
                      SELECT entity_name, SUM(amount) AS total_amount
                      FROM base
                      WHERE trim(coalesce(entity_name, '')) <> ''
                      GROUP BY 1
                      ORDER BY total_amount DESC, entity_name ASC
                      LIMIT {top20_entities}
                    )
                    SELECT COALESCE(SUM(total_amount), 0)
                    FROM ranked
                    """
                ).fetchone()[0]
            )
        )
    pareto_share_pct = float((top20_sum / total_value) * 100) if total_value else 0.0

    female = gender_map.get("Mujer", {"count": 0, "total": 0, "avg": 0.0})
    male = gender_map.get("Hombre", {"count": 0, "total": 0, "avg": 0.0})
    other = gender_map.get("Otro", {"count": 0, "total": 0, "avg": 0.0})
    undefined = gender_map.get("No Definido", {"count": 0, "total": 0, "avg": 0.0})

    gender_answer = (
        "Sí existe una brecha de género financiera en la representación legal. "
        f"Las mujeres concentran {format_int_es(female['count'])} contratos y {format_int_es(female['total'])} "
        f"en valor total, con un promedio aproximado de {format_int_es(int(female['avg']))} por contrato; "
        f"los hombres concentran {format_int_es(male['count'])} contratos y {format_int_es(male['total'])} "
        f"en valor total, con un promedio aproximado de {format_int_es(int(male['avg']))} por contrato. "
        f"La categoría Otro también aparece con {format_int_es(other['count'])} contratos y "
        f"{format_int_es(other['total'])} en valor total. "
        f"Además, la categoría No Definido es alta ({format_int_es(undefined['count'])} contratos), "
        f"con un valor total de {format_int_es(undefined['total'])} y un promedio de "
        f"{format_float_es(undefined['avg'])} por contrato, así que la lectura debe incluir esa limitación."
    )

    type_anomalies = [
        {
            "variable": pyme_col,
            "tipo_esperado": "BOOLEAN",
            "motivo": "Es una variable binaria de si/no y conviene normalizarla a booleano.",
        },
        {
            "variable": obligacion_ambiental_col,
            "tipo_esperado": "BOOLEAN",
            "motivo": "Tambien se interpreta como indicador binario de cumplimiento ambiental.",
        },
        {
            "variable": valor_col,
            "tipo_esperado": "DECIMAL(38,0)",
            "motivo": "Es un monto monetario y no deberia quedarse como texto libre.",
        },
        {
            "variable": valor_adelantado_col,
            "tipo_esperado": "DECIMAL(38,0)",
            "motivo": "Es un valor financiero; lo correcto es tratarlo como numerico.",
        },
        {
            "variable": valor_facturado_col,
            "tipo_esperado": "DECIMAL(38,0)",
            "motivo": "Tambien representa dinero y debe modelarse como numerico.",
        },
        {
            "variable": fecha_inicio_liq_col,
            "tipo_esperado": "DATE",
            "motivo": "Es una fecha de proceso contractual y deberia poder compararse como fecha.",
        },
        {
            "variable": fecha_fin_liq_col,
            "tipo_esperado": "DATE",
            "motivo": "Es otra fecha de cierre y no deberia manejarse como texto.",
        },
        {
            "variable": fecha_notif_prorroga_col,
            "tipo_esperado": "DATE",
            "motivo": "La notificacion de prorroga corresponde a un campo de fecha.",
        },
    ]

    q_responses = [
        {"n": 1, "pregunta": "Seleccione su equipo", "respuesta": "Pendiente de tu dato personal", "estado": "manual"},
        {"n": 2, "pregunta": "Ingrese la CC del capitan", "respuesta": "Pendiente de tu dato personal", "estado": "manual"},
        {"n": 3, "pregunta": "Numero de registros en la nueva base de datos", "respuesta": format_int_es(total_records), "estado": "ok"},
        {"n": 4, "pregunta": "Numero de variables en la nueva base de datos", "respuesta": format_int_es(total_variables), "estado": "ok"},
        {"n": 5, "pregunta": "Numero de registros que corresponden al 2025", "respuesta": format_int_es(records_2025), "estado": "ok"},
        {"n": 6, "pregunta": "Proporcion de contratos asignados a Pymes", "respuesta": format_pct_es(pyme_pct), "estado": "ok"},
        {"n": 7, "pregunta": "Numero de contratos asignados a Pymes", "respuesta": format_int_es(pyme_yes), "estado": "ok"},
        {
            "n": 8,
            "pregunta": "Top 10 clasificado por departamentos del numero de contratos celebrados",
            "respuesta": ", ".join(item.name.lower() for item in departments),
            "estado": "ok",
        },
        {
            "n": 9,
            "pregunta": "Contratos ejecutados por el departamento en posicion 6",
            "respuesta": format_int_es(top_department_6.count or 0),
            "estado": "ok",
        },
        {
            "n": 10,
            "pregunta": "Modalidad de contratacion preferida por las entidades publicas",
            "respuesta": preferred_modality.name,
            "estado": "ok",
        },
        {
            "n": 11,
            "pregunta": "Numero de contratos de la modalidad preferida",
            "respuesta": format_int_es(preferred_modality.count or 0),
            "estado": "ok",
        },
        {
            "n": 12,
            "pregunta": "Top 3 de entidades que mas ejecutaron dinero",
            "respuesta": ", ".join(
                f"{idx + 1}. {item.name} ({format_int_es(item.total or 0)})"
                for idx, item in enumerate(top_entities)
            ),
            "estado": "ok",
        },
        {
            "n": 13,
            "pregunta": "Top 5 de tipos de contrato y cuantos registros tiene cada tipo",
            "respuesta": ", ".join(
                f"{idx + 1}. {item.name} ({format_int_es(item.count or 0)})"
                for idx, item in enumerate(contract_types)
            ),
            "estado": "ok",
        },
        {
            "n": 14,
            "pregunta": "Porcentaje del tipo de contrato con mayor resultado",
            "respuesta": format_pct_es(top_type_pct),
            "estado": "ok",
        },
        {
            "n": 15,
            "pregunta": "Top 3 de valores anomalos financieros",
            "respuesta": top_values,
            "estado": "ok",
        },
        {
            "n": 16,
            "pregunta": "Porcentaje de contratos que contempla pagos adelantados o anticipos",
            "respuesta": format_pct_es(advance_pct),
            "estado": "ok",
        },
        {
            "n": 17,
            "pregunta": "Contratos que incluyen obligaciones o clausulas ambientales",
            "respuesta": format_int_es(env_yes),
            "estado": "ok",
        },
        {
            "n": 18,
            "pregunta": "Se cumple el principio de Pareto en la contratacion estatal",
            "respuesta": (
                f"Si. El 20% superior de entidades concentra {format_pct_es(pareto_share_pct)}% "
                "del valor total, lo que muestra una concentracion muy alta."
            ),
            "estado": "analysis",
        },
        {
            "n": 19,
            "pregunta": "Existe una brecha de genero financiera en la representacion legal",
            "respuesta": gender_answer,
            "estado": "analysis",
        },
        {
            "n": 20,
            "pregunta": "Revision del tipo de dato en las variables reportadas",
            "respuesta": [
                {"variable": item["variable"], "tipo_esperado": item["tipo_esperado"], "motivo": item["motivo"]}
                for item in type_anomalies
            ],
            "estado": "analysis",
        },
    ]

    summary = {
        "dataset": {
            "id": csv_path.stem,
            "name": "SECOP II - Contratos Electronicos",
            "source_url": csv_path.as_uri(),
            "metadata_url": csv_path.as_uri(),
        },
        "kpis": {
            "total_records": total_records,
            "total_variables": total_variables,
            "records_2025": records_2025,
            "pyme_yes": pyme_yes,
            "pyme_pct": pyme_pct,
            "advance_yes": advance_yes,
            "advance_pct": advance_pct,
            "environmental_yes": env_yes,
            "pareto_share_pct": pareto_share_pct,
            "unique_entities": unique_entities,
        },
        "top_entities": [{"name": item.name, "count": item.count, "total": item.total} for item in top_entities],
        "departments": [{"name": item.name, "count": item.count, "total": item.total} for item in departments],
        "contract_types": [{"name": item.name, "count": item.count, "total": item.total} for item in contract_types],
        "preferred_modality": {
            "name": preferred_modality.name,
            "count": preferred_modality.count,
            "total": preferred_modality.total,
        },
        "top_values": top_values,
        "type_anomalies": type_anomalies,
        "notes": [
            "Las variables de si/no se deberian convertir a booleanos para analisis mas limpios.",
            "La base si contiene el campo de genero del representante legal y permite analizar brechas financieras.",
            "La concentracion de valor es muy alta, por lo que la revision humana sigue siendo clave.",
        ],
        "questions": q_responses,
    }

    payload = json.dumps(summary, ensure_ascii=False, indent=2)
    DASHBOARD_API_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    DASHBOARD_DATA_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    DASHBOARD_API_SUMMARY.write_text(payload, encoding="utf-8")
    DASHBOARD_DATA_SUMMARY.write_text(payload, encoding="utf-8")

    subprocess.run([sys.executable, str(REPORTS_SCRIPT)], check=True)

    print(f"CSV: {csv_path}")
    print(f"Parquet: {parquet_path}")
    print(f"API summary: {DASHBOARD_API_SUMMARY}")
    print(f"Data summary: {DASHBOARD_DATA_SUMMARY}")
    print(f"EDA: {ROOT / 'reports' / 'reto2_eda.html'}")
    print(f"Calidad: {ROOT / 'reports' / 'reto2_calidad.html'}")
    print(f"Registros: {format_int_es(total_records)}")
    print(f"Variables: {format_int_es(total_variables)}")
    print(f"Registros 2025: {format_int_es(records_2025)}")
    print(f"Pymes: {format_int_es(pyme_yes)} ({format_pct_es(pyme_pct)}%)")
    print(f"Anticipos: {format_int_es(advance_yes)} ({format_pct_es(advance_pct)}%)")
    print(f"Ambientales: {format_int_es(env_yes)}")
    print(f"Pareto: {format_pct_es(pareto_share_pct)}%")


if __name__ == "__main__":
    main()
