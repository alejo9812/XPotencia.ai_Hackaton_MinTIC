from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from html import escape
from pathlib import Path
from typing import Iterable

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import Paragraph, Preformatted, SimpleDocTemplate, Spacer


FASE2_DIR = Path(__file__).resolve().parents[2]
DOCS_DIR = FASE2_DIR


@dataclass(frozen=True)
class DocSection:
    heading: str
    paragraphs: tuple[str, ...] = ()
    bullets: tuple[str, ...] = ()
    code: tuple[str, ...] = ()


@dataclass(frozen=True)
class LayerDoc:
    slug: str
    title: str
    subtitle: str
    intro: str
    sections: tuple[DocSection, ...]


def build_styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "TitleCustom",
            parent=base["Title"],
            fontName="Helvetica-Bold",
            fontSize=22,
            leading=26,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#102A43"),
            spaceAfter=6,
        ),
        "subtitle": ParagraphStyle(
            "SubtitleCustom",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=10.5,
            leading=13,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#334E68"),
            spaceAfter=10,
        ),
        "meta": ParagraphStyle(
            "MetaCustom",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=8.5,
            leading=10.5,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#627D98"),
            spaceAfter=12,
        ),
        "section": ParagraphStyle(
            "SectionCustom",
            parent=base["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=12.5,
            leading=15,
            textColor=colors.HexColor("#102A43"),
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
            textColor=colors.HexColor("#243B53"),
            spaceAfter=4,
        ),
        "bullet": ParagraphStyle(
            "BulletCustom",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=9.2,
            leading=12,
            leftIndent=14,
            firstLineIndent=-8,
            spaceAfter=2,
            textColor=colors.HexColor("#243B53"),
        ),
        "code": ParagraphStyle(
            "CodeCustom",
            parent=base["Code"],
            fontName="Courier",
            fontSize=8.2,
            leading=10.2,
            leftIndent=10,
            rightIndent=10,
            spaceBefore=4,
            spaceAfter=8,
            textColor=colors.HexColor("#102A43"),
        ),
        "note": ParagraphStyle(
            "NoteCustom",
            parent=base["BodyText"],
            fontName="Helvetica-Oblique",
            fontSize=8.7,
            leading=11,
            textColor=colors.HexColor("#486581"),
            spaceAfter=4,
        ),
    }


def paragraph(text: str, style: ParagraphStyle) -> Paragraph:
    return Paragraph(escape(text), style)


def bullet_lines(items: Iterable[str], style: ParagraphStyle) -> list[Paragraph]:
    return [Paragraph(f"&bull; {escape(item)}", style) for item in items]


def render_markdown(doc: LayerDoc) -> str:
    lines: list[str] = [f"# {doc.title}", "", doc.subtitle, "", doc.intro, ""]
    for section in doc.sections:
        lines.extend([f"## {section.heading}", ""])
        for text in section.paragraphs:
            lines.extend([text, ""])
        if section.bullets:
            lines.extend([f"- {item}" for item in section.bullets])
            lines.append("")
        if section.code:
            lines.append("```text")
            lines.extend(section.code)
            lines.append("```")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def build_pdf(doc: LayerDoc, output_path: Path) -> None:
    styles = build_styles()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    story: list = []
    story.append(paragraph(doc.title, styles["title"]))
    story.append(paragraph(doc.subtitle, styles["subtitle"]))
    story.append(
        paragraph(
            f"Generado el {date.today().isoformat()} para documentar la arquitectura funcional del proyecto.",
            styles["meta"],
        )
    )
    story.append(paragraph(doc.intro, styles["body"]))
    story.append(Spacer(1, 6))

    for section in doc.sections:
        story.append(paragraph(section.heading, styles["section"]))
        for text in section.paragraphs:
            story.append(paragraph(text, styles["body"]))
        if section.bullets:
            story.extend(bullet_lines(section.bullets, styles["bullet"]))
        if section.code:
            story.append(Preformatted("\n".join(section.code), styles["code"]))

    def footer(canvas, doc_obj):  # type: ignore[no-untyped-def]
        canvas.saveState()
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.HexColor("#627D98"))
        canvas.drawString(40, 22, "PAE Risk Tracker - Arquitectura por capas")
        canvas.drawRightString(A4[0] - 40, 22, f"Pagina {doc_obj.page}")
        canvas.restoreState()

    pdf = SimpleDocTemplate(
        str(output_path),
        pagesize=A4,
        leftMargin=40,
        rightMargin=40,
        topMargin=40,
        bottomMargin=40,
        title=doc.title,
        author="Codex",
    )
    pdf.build(story, onFirstPage=footer, onLaterPages=footer)


def write_doc(doc: LayerDoc) -> None:
    layer_dir = DOCS_DIR / doc.slug
    layer_dir.mkdir(parents=True, exist_ok=True)
    markdown_path = layer_dir / "README.md"
    pdf_path = layer_dir / f"{doc.slug}.pdf"
    markdown_path.write_text(render_markdown(doc), encoding="utf-8")
    build_pdf(doc, pdf_path)


def layer_docs() -> list[LayerDoc]:
    return [
        LayerDoc(
            slug="capa_1_ingesta_automatizada",
            title="Capa 1 - Ingesta Automatizada",
            subtitle="Consume SECOP II, normaliza 67 campos al estandar OCDS y enriquece con el historial PACO.",
            intro="Esta capa trae datos publicos, los limpia, los normaliza y los deja listos para analisis, trazabilidad y enriquecimiento.",
            sections=(
                DocSection(
                    heading="Proposito",
                    paragraphs=(
                        "La capa 1 convierte datos dispersos de SECOP II en un contrato canonico que pueda ser consultado por reglas, LLM y dashboard.",
                        "Tambien agrega contexto historico de PACO para que el equipo tenga una vista mas completa del contratista y de la trazabilidad documental.",
                    ),
                    bullets=(
                        "Consumir API publica SECOP II en tiempo real o por lotes controlados.",
                        "Normalizar los campos relevantes al mapa canonico del proyecto y al estandar OCDS.",
                        "Enriquecer cada contrato con antecedentes, packs y tablas de contexto PACO.",
                    ),
                ),
                DocSection(
                    heading="Entradas",
                    bullets=(
                        "SECOP II - contratos, procesos y adiciones desde Socrata.",
                        "Paquetes PACO con contexto contractual, fiscal, disciplinario, penal y colusion.",
                        "Archivos raw de muestra y manifests para trabajo local sin depender siempre de la red.",
                        "Catalogos de columnas, datasets y keywords para reconocer sinonimos y variantes de nombre.",
                    ),
                ),
                DocSection(
                    heading="Salidas y artefactos",
                    bullets=(
                        "data/raw/ -> insumos descargados y muestras de trabajo.",
                        "data/processed/pae_contracts_core.parquet -> contrato base normalizado.",
                        "data/processed/pae_contracts_enriched.parquet -> contrato con contexto PACO y trazabilidad.",
                        "data/processed/paco/*.parquet -> tablas PACO listas para cruces.",
                        "data/cache/ -> cache tecnico, esquemas y manifests de trazabilidad.",
                        "data/duckdb/pae_risk_tracker.duckdb -> base local compartida por toda la solucion.",
                    ),
                ),
                DocSection(
                    heading="Piezas tecnicas",
                    bullets=(
                        "connectors/socrata_client.py -> cliente de consultas a Datos Abiertos.",
                        "connectors/secop_contracts.py, secop_processes.py y secop_additions.py -> descarga y mapeo de las fuentes.",
                        "ingestion/schema_normalizer.py -> resuelve alias de columnas, convierte fechas y numeros, y arma search_blob.",
                        "ingestion/incremental_loader.py -> carga incremental con deduplicacion y control de lote.",
                        "ingestion/data_pack_loader.py -> integra packs locales y contexto PACO.",
                        "storage/duckdb_store.py -> persistencia analitica local.",
                    ),
                ),
                DocSection(
                    heading="Reglas de normalizacion",
                    bullets=(
                        "Resolver sinonimos de columnas antes de escribir el contrato canonico.",
                        "Coercionar numeros, fechas y IDs para evitar tipos inconsistentes.",
                        "Construir un search_blob que permita busqueda textual y clasificacion PAE.",
                        "Eliminar duplicados por contract_id o process_id antes de persistir.",
                        "Clasificar coincidencias PAE por confianza alta, media o baja.",
                        "Cruzar adiciones con el contrato principal para enriquecer el historial.",
                    ),
                ),
                DocSection(
                    heading="Como extenderla",
                    bullets=(
                        "Agregar o ajustar datasets en config/datasets.yml.",
                        "Afinar aliases y columnas en config/columns.yml.",
                        "Revisar keywords de PAE si cambia el nicho o el lenguaje contractual.",
                        "Reejecutar discover-schema, ingest, load-paco y score para refrescar los artefactos.",
                    ),
                ),
            ),
        ),
        LayerDoc(
            slug="capa_2_motor_reglas_cuantitativas",
            title="Capa 2 - Motor de Reglas Cuantitativas",
            subtitle="Agrupa 12 reglas estructurales basadas en tipologias UNODC/OEA y produce vectores de riesgo numericos.",
            intro="Esta capa toma el contrato normalizado y lo convierte en senales cuantitativas que ayudan a priorizar revision documental.",
            sections=(
                DocSection(
                    heading="Proposito",
                    paragraphs=(
                        "El motor cuantitativo no decide culpabilidad ni reemplaza una auditoria. Su trabajo es detectar patrones estructurales y convertirlos en valores numericos comparables.",
                        "La version actual del codigo contiene una familia amplia de flags RF-01 a RF-37; esta documentacion los agrupa en 12 reglas de negocio para que el equipo las lea mas facil.",
                    ),
                ),
                DocSection(
                    heading="Las 12 reglas",
                    bullets=(
                        "Fraccionamiento de contratos.",
                        "Sobreprecio o valor atipico frente a comparables.",
                        "Baja competencia: pocos oferentes o pocas propuestas.",
                        "Concentracion de proveedor o repeticion inusual en la misma entidad.",
                        "Adiciones excesivas, prorrogas o incrementos desproporcionados.",
                        "Objeto contractual generico o demasiado corto.",
                        "Campos criticos incompletos o trazabilidad documental debil.",
                        "Consistencia temporal dudosa entre firma, publicacion, inicio y fin.",
                        "Valor por dia o intensidad de ejecucion atipicos.",
                        "Polizas o garantias vencidas o ausentes, cuando se crucen fuentes complementarias.",
                        "Mallas empresariales, consorcios repetidos o redes de proveedores relacionadas.",
                        "Contexto PACO o antecedentes que refuerzan la alerta.",
                    ),
                ),
                DocSection(
                    heading="Entradas",
                    bullets=(
                        "Contrato normalizado desde la capa 1.",
                        "Tablas de adiciones y contexto de PACO para enriquecer el analisis.",
                        "Umbrales y pesos definidos en config/risk_flags.yml y config/scoring.yml.",
                        "Estadisticos robustos como mediana, MAD, IQR y percentiles.",
                    ),
                ),
                DocSection(
                    heading="Salidas",
                    paragraphs=(
                        "La salida principal es un frame puntuado con score, nivel y evidencias por contrato.",
                    ),
                    bullets=(
                        "risk_score -> valor numerico de 0 a 100.",
                        "risk_level -> bajo, medio, alto o critico.",
                        "risk_flags -> lista de flags activados con evidencia.",
                        "risk_dimension_scores_json -> vector numerico por dimension.",
                        "risk_summary y risk_limitations -> lectura corta para API y dashboard.",
                    ),
                    code=(
                        "score_base = suma de pesos por flags activados",
                        "score_final = min(100, score_base + ajustes_estadisticos)",
                    ),
                ),
                DocSection(
                    heading="Piezas tecnicas",
                    bullets=(
                        "risk/indicators.py -> calcula indicadores de apoyo y estadisticos base.",
                        "risk/rules_engine.py -> evalua las reglas, arma flags y construye el score.",
                        "risk/scoring.py -> expone helpers de score para frames y registros.",
                        "risk/evidence.py -> define estructuras RiskFlag y RiskAssessment.",
                        "config/risk_flags.yml -> catalogo de flags, pesos y dimensiones.",
                        "config/scoring.yml -> bandas, caps y reglas de clasificacion.",
                    ),
                ),
                DocSection(
                    heading="Como modificarla y probarla",
                    bullets=(
                        "Cambiar pesos o caps en config/scoring.yml.",
                        "Ajustar umbrales o familias de senales en config/risk_flags.yml.",
                        "Agregar pruebas en tests/test_scoring.py y tests/test_opacity_criteria.py.",
                        "Reejecutar el pipeline de scoring para validar el impacto de los cambios.",
                    ),
                ),
            ),
        ),
        LayerDoc(
            slug="capa_3_analisis_semantico_llm",
            title="Capa 3 - Analisis Semantico LLM",
            subtitle="Claude Sonnet analiza campos textuales, encuentra inconsistencias y devuelve explicaciones trazables en lenguaje natural.",
            intro="Esta capa interpreta el texto del contrato para explicar por que el caso merece revision, sin inventar datos ni reemplazar la evidencia estructurada.",
            sections=(
                DocSection(
                    heading="Proposito",
                    paragraphs=(
                        "La capa semantica lee el objeto contractual, las justificaciones, las condiciones y la evidencia recuperada para producir una explicacion clara y auditable.",
                        "Su rol es traducir hallazgos tecnicos a lenguaje natural, no redefinir el score ni emitir conclusiones legales.",
                    ),
                ),
                DocSection(
                    heading="Que analiza",
                    bullets=(
                        "Objeto contractual y descripciones extensas.",
                        "Justificaciones de modalidad y condiciones de entrega.",
                        "Textos genericos, repeticiones y frases sospechosamente vagas.",
                        "Inconsistencias entre el relato textual y los campos estructurados.",
                        "Preguntas de auditoria que ayuden a revisar el contrato con criterio humano.",
                    ),
                ),
                DocSection(
                    heading="Contrato de salida",
                    bullets=(
                        "analysis.summary -> resumen corto para el usuario.",
                        "analysis.explanation -> explicacion trazable en lenguaje natural.",
                        "analysis.recommendations -> acciones sugeridas.",
                        "analysis.audit_questions -> preguntas para revision documental.",
                        "analysis.limitations -> advertencias de cobertura o calidad de datos.",
                        "provider, model y prompt_version -> trazabilidad del proveedor LLM usado.",
                    ),
                ),
                DocSection(
                    heading="Arquitectura de ejecucion",
                    paragraphs=(
                        "El flujo clasico es: consulta del usuario -> plan estructurado -> recuperacion de evidencia -> validacion local -> analisis LLM -> respuesta final.",
                    ),
                    code=(
                        "user query -> plan -> evidence rows -> validation context -> Claude Sonnet / mock -> structured analysis",
                    ),
                ),
                DocSection(
                    heading="Reglas de uso del LLM",
                    bullets=(
                        "No inventar hechos fuera de la evidencia recuperada.",
                        "No reemplazar el score cuantitativo ni la regla deterministica.",
                        "No emitir juicios legales; solo priorizar y explicar.",
                        "Preferir una respuesta corta, clara y con soporte documental.",
                        "Usar el proveedor configurado o el modo mock para mantener el contrato estable.",
                    ),
                ),
                DocSection(
                    heading="Piezas tecnicas",
                    bullets=(
                        "agent/orchestrator.py -> arma la corrida del agente y conecta evidencia con LLM.",
                        "agent/tools.py -> planifica la consulta y selecciona filas de evidencia.",
                        "agent/prompts.py -> define el contrato de prompting y la politica de salida.",
                        "agent/knowledge.py -> recupera conocimiento de reglas y criterios.",
                        "agent/llm_client.py -> abstrae Claude Sonnet o el proveedor configurado.",
                        "response_builder.py -> construye respuestas estables para chat y API.",
                    ),
                ),
                DocSection(
                    heading="Como extenderla",
                    bullets=(
                        "Ajustar el prompt si cambia el tono, el idioma o el nivel de detalle esperado.",
                        "Agregar herramientas nuevas en agent/tools.py si hace falta mas contexto.",
                        "Ampliar knowledge.py con criterios o explicaciones reutilizables.",
                        "Mantener pruebas para no perder trazabilidad al cambiar el proveedor LLM.",
                    ),
                ),
            ),
        ),
        LayerDoc(
            slug="capa_4_score_riesgo_explicable",
            title="Capa 4 - Score de Riesgo Explicable",
            subtitle="Combina la senal numerica de la capa 2 con la explicacion de la capa 3 para producir un score interpretable.",
            intro="Esta capa entrega el numero final, la banda de riesgo, la evidencia y la recomendacion que consume el API, el dashboard y los reportes.",
            sections=(
                DocSection(
                    heading="Proposito",
                    paragraphs=(
                        "La capa 4 es la capa de consumo: recibe el vector de riesgo numerico, lo contextualiza con la explicacion LLM y lo convierte en una ficha clara para priorizacion.",
                        "El objetivo no es castigar contratos sino ordenar la revision y dejar claro por que un caso quedo arriba en la lista.",
                    ),
                ),
                DocSection(
                    heading="Formula de trabajo",
                    paragraphs=(
                        "El score final se apoya en la suma de pesos, caps por dimension y ajustes estadisticos. La implementacion actual lo limita a 100 y clasifica en bandas de lectura.",
                    ),
                    code=(
                        "score_base = suma de pesos por flags activados",
                        "score_final = min(100, score_base + ajustes_estadisticos)",
                        "bandas = bajo | medio | alto | critico",
                    ),
                ),
                DocSection(
                    heading="Entradas",
                    bullets=(
                        "risk_flags y risk_dimension_scores_json desde la capa 2.",
                        "analysis.summary, analysis.explanation y audit_questions desde la capa 3.",
                        "validation context, trazabilidad y evidencias de contrato.",
                        "Parametros de exportacion y contrato canonico de salida.",
                    ),
                ),
                DocSection(
                    heading="Salidas",
                    bullets=(
                        "RiskAssessment por contrato con score, nivel, flags y limitaciones.",
                        "pae_risk_ranking.csv / json para ranking y exportacion.",
                        "pae_audit_cards.json con fichas de revision.",
                        "Respuesta de API para dashboard, chat y reportes.",
                        "Indicadores agregados para vistas ejecutivas y diagnosticos.",
                    ),
                ),
                DocSection(
                    heading="Piezas tecnicas",
                    bullets=(
                        "risk/scoring.py -> expone el score para frames y registros.",
                        "risk/evidence.py -> estructura la ficha explicable.",
                        "response_builder.py -> estandariza la respuesta para chat y API.",
                        "api/routes_contracts.py -> publica contratos, detalle y riesgo.",
                        "api/routes_diagnostics.py -> expone diagnosticos y validacion.",
                        "data/outputs/ -> guarda ranking, cards y diagnosticos finales.",
                    ),
                ),
                DocSection(
                    heading="No objetivos",
                    bullets=(
                        "No es una sentencia legal ni una prueba de corrupcion.",
                        "No sustituye revision humana ni auditoria juridica.",
                        "No debe ocultar la evidencia que explica el score.",
                    ),
                ),
                DocSection(
                    heading="Como extenderla",
                    bullets=(
                        "Cambiar bandas de riesgo en config/scoring.yml si el equipo necesita otra lectura.",
                        "Agregar nuevos campos de salida en el contrato canonico si el dashboard lo pide.",
                        "Actualizar export_contract.json cuando cambie la forma de entregar resultados.",
                        "Revisar las pruebas de scoring y de API cuando se mueva la formula.",
                    ),
                ),
            ),
        ),
    ]


def main() -> None:
    docs = layer_docs()
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    for doc in docs:
        write_doc(doc)

    print(f"Generated documentation in {DOCS_DIR}")


if __name__ == "__main__":
    main()
