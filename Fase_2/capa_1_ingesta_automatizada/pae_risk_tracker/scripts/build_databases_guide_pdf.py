from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Iterable

import duckdb
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
DUCKDB_PATH = DATA_DIR / "duckdb" / "pae_risk_tracker.duckdb"
OUTPUT_PATH = ROOT_DIR.parent / "docs" / "bases_datos_proyecto_fase2.pdf"


def fmt_bytes(value: int) -> str:
    size = float(value)
    units = ["B", "KB", "MB", "GB"]
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{int(value)} B"


def summarize_folder(path: Path) -> tuple[int, int]:
    files = [item for item in path.rglob("*") if item.is_file()]
    total_size = sum(item.stat().st_size for item in files)
    return len(files), total_size


def list_relative_files(path: Path, limit: int | None = None) -> list[str]:
    files = [item for item in path.rglob("*") if item.is_file()]
    rel = [str(item.relative_to(path)).replace("\\", "/") for item in files]
    rel.sort()
    if limit is not None:
        return rel[:limit]
    return rel


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    value = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()[0]
    return int(value) > 0


def table_count(con: duckdb.DuckDBPyConnection, table_name: str) -> int | None:
    if not table_exists(con, table_name):
        return None
    return int(con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def build_styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "DocTitle",
            parent=base["Title"],
            fontName="Helvetica-Bold",
            fontSize=24,
            leading=28,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#102A43"),
            spaceAfter=10,
        ),
        "subtitle": ParagraphStyle(
            "DocSubtitle",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=11,
            leading=14,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#334E68"),
            spaceAfter=12,
        ),
        "h1": ParagraphStyle(
            "Heading1Custom",
            parent=base["Heading1"],
            fontName="Helvetica-Bold",
            fontSize=15,
            leading=18,
            textColor=colors.HexColor("#102A43"),
            spaceBefore=10,
            spaceAfter=6,
        ),
        "h2": ParagraphStyle(
            "Heading2Custom",
            parent=base["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=12,
            leading=15,
            textColor=colors.HexColor("#243B53"),
            spaceBefore=8,
            spaceAfter=4,
        ),
        "body": ParagraphStyle(
            "BodyCustom",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=9.5,
            leading=12.5,
            alignment=TA_LEFT,
            spaceAfter=4,
        ),
        "small": ParagraphStyle(
            "SmallCustom",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=8.5,
            leading=11,
            alignment=TA_LEFT,
            textColor=colors.HexColor("#486581"),
        ),
        "mono": ParagraphStyle(
            "MonoCustom",
            parent=base["Code"],
            fontName="Courier",
            fontSize=8.5,
            leading=10.5,
            textColor=colors.HexColor("#102A43"),
        ),
    }


def paragraph(text: str, style: ParagraphStyle) -> Paragraph:
    return Paragraph(text, style)


def bullet_block(items: Iterable[str], style: ParagraphStyle) -> list[Paragraph]:
    return [Paragraph(f"&bull; {item}", style) for item in items]


def build_table(headers: list[str], rows: list[list[str]], col_widths: list[float]) -> Table:
    data = [headers] + rows
    table = Table(data, colWidths=col_widths, repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#D9E2EC")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#102A43")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 8.5),
                ("LEADING", (0, 0), (-1, -1), 10.5),
                ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#BCCCDC")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    return table


def make_layer_summary(con: duckdb.DuckDBPyConnection) -> list[list[str]]:
    folders = [
        ("data/raw", DATA_DIR / "raw", "Origenes publicos y muestras"),
        ("data/processed", DATA_DIR / "processed", "Capas normalizadas y derivadas"),
        ("data/duckdb", DATA_DIR / "duckdb", "Base analitica local"),
        ("data/cache", DATA_DIR / "cache", "Caches tecnicos y manifests"),
        ("data/outputs", DATA_DIR / "outputs", "Ranking, auditoria y diagnosticos"),
        ("data/validation", DATA_DIR / "validation", "Reportes y snapshots de validacion"),
    ]
    rows: list[list[str]] = []
    for label, path, purpose in folders:
        if path.exists():
            file_count, total_size = summarize_folder(path)
            size_text = fmt_bytes(total_size)
        else:
            file_count, size_text = 0, "0 B"
        rows.append([label, str(file_count), size_text, purpose])
    return rows


def make_db_table_rows(con: duckdb.DuckDBPyConnection) -> list[list[str]]:
    grouped = [
        ("PAE base", "pae_contracts_core", "Contrato base unificado"),
        ("PAE base", "pae_contracts_enriched", "Contrato enriquecido con contexto"),
        ("PAE base", "pae_contracts_scored", "Contrato puntuado para riesgo"),
        ("PAE busqueda", "pae_search_index", "Indice unificado para chat y agente"),
        ("PAE base", "pae_additions", "Adiciones y modificaciones"),
        ("PACO", "paco_events", "Eventos de contexto y antecedentes"),
        ("PACO", "paco_contractual", "Subconjunto contractual"),
        ("PACO", "paco_collusion", "Alertas por colusion"),
        ("PACO", "paco_disciplinary", "Alertas disciplinarias"),
        ("PACO", "paco_fiscal", "Alertas fiscales"),
        ("PACO", "paco_penal", "Alertas penales"),
        ("Trazabilidad", "source_catalog_runs", "Ejecuciones del catalogo de fuentes"),
        ("Trazabilidad", "source_catalog_checks", "Verificaciones por fuente"),
        ("Validacion", "validation_runs", "No presente en la snapshot actual"),
        ("Validacion", "validation_observations", "No presente en la snapshot actual"),
        ("Validacion", "validation_registry", "No presente en la snapshot actual"),
    ]
    rows: list[list[str]] = []
    for group, table, purpose in grouped:
        count = table_count(con, table)
        count_text = "N/D" if count is None else f"{count:,}".replace(",", ".")
        rows.append([group, table, count_text, purpose])
    return rows


def write_pdf() -> Path:
    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(f"No existe la base DuckDB: {DUCKDB_PATH}")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DUCKDB_PATH))
    styles = build_styles()

    doc = SimpleDocTemplate(
        str(OUTPUT_PATH),
        pagesize=A4,
        leftMargin=40,
        rightMargin=40,
        topMargin=40,
        bottomMargin=40,
        pageCompression=0,
        title="Bases de datos del proyecto Fase 2",
        author="Codex",
    )

    story: list = []

    story.append(paragraph("Bases de datos del proyecto Fase 2", styles["title"]))
    story.append(
        paragraph(
            "Documento tecnico para entender que datos usa el proyecto, el chat y el agente. "
            "La version actual no usa bases separadas por componente: todos leen la misma capa local DuckDB "
            "y los mismos artefactos de trazabilidad.",
            styles["subtitle"],
        )
    )
    story.append(
        paragraph(
            f"Generado el {date.today().isoformat()} a partir de la snapshot local del repositorio.",
            styles["small"],
        )
    )
    story.append(Spacer(1, 10))

    story.append(paragraph("1. Resumen ejecutivo", styles["h1"]))
    summary_bullets = [
        "El proyecto centraliza la persistencia en una base analitica local DuckDB.",
        "Las fuentes originales llegan desde SECOP II, PACO y archivos de muestra del repositorio.",
        "La capa procesada materializa contratos, adiciones, procesos, antecedentes y el indice de busqueda.",
        "El chat y el agente consumen el mismo indice unificado para responder con evidencia y trazabilidad.",
        "La validacion documental y los reportes de auditoria salen del mismo entorno de datos.",
    ]
    story.extend(bullet_block(summary_bullets, styles["body"]))
    story.append(Spacer(1, 6))
    story.append(paragraph("Flujo resumido:", styles["h2"]))
    story.append(paragraph("Fuentes web y muestras locales -> raw -> processed -> DuckDB -> search index -> API / chat / agente -> outputs", styles["mono"]))
    story.append(PageBreak())

    story.append(paragraph("2. Capas de almacenamiento", styles["h1"]))
    layer_headers = ["Capa", "Archivos", "Tamano total", "Para que se usa"]
    layer_rows = make_layer_summary(con)
    layer_table = build_table(layer_headers, layer_rows, [80, 52, 74, 236])
    story.append(layer_table)
    story.append(Spacer(1, 10))
    story.append(paragraph("Detalle por capa", styles["h2"]))
    story.extend(
        bullet_block(
            [
                "data/raw: contiene los insumos de origen, por ejemplo los archivos PACO descargados y las muestras PAE con sus manifestos.",
                "data/processed: contiene los parquet ya normalizados y el indice de busqueda materializado.",
                "data/duckdb: contiene el archivo unico de consulta analitica que usan la API, el chat y el agente.",
                "data/cache: guarda metadatos, manifests y respuestas cacheadas de Socrata / trazabilidad.",
                "data/outputs: guarda rankings, fichas de auditoria, diagnosticos y checkpoints de integracion.",
                "data/validation: guarda reportes de catalogo de fuentes y deja listo el espacio para snapshots de validacion.",
            ],
            styles["body"],
        )
    )
    story.append(PageBreak())

    story.append(paragraph("3. DuckDB principal y tablas", styles["h1"]))
    story.append(
        paragraph(
            "El archivo principal es data/duckdb/pae_risk_tracker.duckdb. "
            "No hay una base separada para el chat ni otra para el agente: ambos consultan la misma base local.",
            styles["body"],
        )
    )
    db_headers = ["Grupo", "Tabla", "Filas", "Rol dentro del proyecto"]
    db_rows = make_db_table_rows(con)
    db_table = build_table(db_headers, db_rows, [72, 130, 52, 182])
    story.append(db_table)
    story.append(Spacer(1, 8))
    story.extend(
        bullet_block(
            [
                "Las tablas PAE concentran el contrato base, el contrato puntuado, las adiciones y el indice unificado de busqueda.",
                "Las tablas PACO aportan contexto de antecedentes y alertas por tipos de hallazgo.",
                "source_catalog_runs y source_catalog_checks registran la trazabilidad de las fuentes ya revisadas.",
                "validation_runs, validation_observations y validation_registry no aparecen en la snapshot actual; el servicio de validacion las crea cuando ejecuta una corrida persistida.",
            ],
            styles["body"],
        )
    )
    story.append(PageBreak())

    story.append(paragraph("4. Como lo usa el chat", styles["h1"]))
    story.extend(
        bullet_block(
            [
                "El chat no mantiene una base propia. Consulta la misma base DuckDB y el mismo indice de busqueda.",
                "Primero intenta responder con pae_search_index porque alli estan unificados contratos, procesos y adiciones.",
                "Si necesita mas contexto, cae a pae_contracts_scored o pae_contracts_core para ver el detalle del contrato.",
                "Para trazabilidad, combina la respuesta con los reportes de validacion y los manifests de los datos procesados.",
                "La salida del chat debe quedar explicable: la respuesta trae evidencia y no solo un texto generado.",
            ],
            styles["body"],
        )
    )
    story.append(Spacer(1, 6))
    story.append(paragraph("En terminos practicos:", styles["h2"]))
    story.append(
        paragraph(
            "Pregunta del usuario -> busqueda sobre pae_search_index -> recuperacion de evidencia -> validacion contextual -> respuesta final con soporte.",
            styles["mono"],
        )
    )

    story.append(Spacer(1, 10))
    story.append(paragraph("5. Como lo usa el agente", styles["h1"]))
    story.extend(
        bullet_block(
            [
                "El agente construye un plan de consulta y luego decide si usa el indice unificado o el contrato base.",
                "Cuando la consulta es sobre opacidad o criterios, primero busca conocimiento en la capa local del repositorio y luego cruza con la evidencia guardada.",
                "Para preguntas generales, asegura el indice de busqueda y consulta la misma base DuckDB.",
                "El agente usa observaciones de validacion, criterios de opacidad y evidencia de contrato para priorizar la respuesta.",
                "Si el indice no esta listo, el agente puede caer al contrato base para no quedarse sin resultados.",
            ],
            styles["body"],
        )
    )
    story.append(Spacer(1, 6))
    story.append(paragraph("En terminos practicos:", styles["h2"]))
    story.append(
        paragraph(
            "Pregunta del interventor -> plan -> indice unificado o contrato base -> evidencia -> resumen explicable -> preguntas de auditoria.",
            styles["mono"],
        )
    )

    story.append(PageBreak())
    story.append(paragraph("6. Validacion, cache y trazabilidad", styles["h1"]))
    story.extend(
        bullet_block(
            [
                "data/cache/schema_cache.json guarda metadatos de esquemas y ayuda a no recalcular descubrimientos ya conocidos.",
                "data/cache/traceability_manifest.json resume la huella de trazabilidad del indice y de los orígenes.",
                "data/cache/socrata/ contiene respuestas cacheadas por hash para consultas repetidas a fuentes publicas.",
                "data/validation/reports/ contiene reportes de catalogo de fuentes; la snapshot actual ya tiene dos JSON de control.",
                "data/validation/snapshots/ esta preparada para guardar instantaneas, pero en esta snapshot esta vacia.",
            ],
            styles["body"],
        )
    )
    story.append(Spacer(1, 8))
    story.append(
        paragraph(
            "La regla operativa es simple: si un dato entra al analisis, debe dejar huella en el cache, en el indice o en la salida de validacion.",
            styles["body"],
        )
    )

    story.append(Spacer(1, 10))
    story.append(paragraph("7. Archivos clave del repositorio", styles["h1"]))
    story.extend(
        bullet_block(
            [
                "src/pae_risk_tracker/paths.py -> define data/raw, data/processed, data/cache, data/outputs, data/validation y data/duckdb.",
                "src/pae_risk_tracker/storage/duckdb_store.py -> wrapper para leer y escribir DuckDB.",
                "src/pae_risk_tracker/retrieval/search_index.py -> materializa pae_search_index y su manifest.",
                "src/pae_risk_tracker/api/server.py -> monta la API con el mismo DuckDBStore compartido.",
                "src/pae_risk_tracker/agent/orchestrator.py -> consulta DuckDB, el indice y la validacion para responder.",
                "src/pae_risk_tracker/validation/service.py -> persiste reportes de validacion y snapshots de trazabilidad.",
            ],
            styles["body"],
        )
    )
    story.append(Spacer(1, 8))
    story.append(
        paragraph(
            "Con esto un interventor puede saber donde vive cada dato, quien lo usa y como se traza de extremo a extremo.",
            styles["body"],
        )
    )

    def add_page_number(canvas, doc):  # type: ignore[no-untyped-def]
        canvas.saveState()
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.HexColor("#486581"))
        canvas.drawString(40, 22, "PAE Risk Tracker - Bases de datos del proyecto Fase 2")
        canvas.drawRightString(A4[0] - 40, 22, f"Pagina {doc.page}")
        canvas.restoreState()

    doc.build(story, onFirstPage=add_page_number, onLaterPages=add_page_number)
    con.close()
    return OUTPUT_PATH


def main() -> None:
    path = write_pdf()
    print(path)


if __name__ == "__main__":
    main()
