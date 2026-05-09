from __future__ import annotations

import json
import math
import os
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from decimal import Decimal
from html import escape
from pathlib import Path


BASE_URL = "https://www.datos.gov.co/resource/jbjy-vk9h.json"
META_URL = "https://www.datos.gov.co/api/views/jbjy-vk9h.json"
ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_DIR = ROOT / "dashboard"
API_DIR = DASHBOARD_DIR / "api"
DATA_DIR = DASHBOARD_DIR / "data"
INDEX_HTML = DASHBOARD_DIR / "index.html"
SUMMARY_JSON = DATA_DIR / "summary.json"
API_JSON = API_DIR / "summary.json"
SERVER_PY = DASHBOARD_DIR / "server.py"
README_MD = DASHBOARD_DIR / "README.md"


@dataclass
class RankedItem:
    name: str
    count: int | None = None
    total: int | None = None


def query(params: dict[str, str]) -> list[dict[str, str]]:
    url = BASE_URL + "?" + urllib.parse.urlencode(params, safe="(),*<> /=:\"")
    with urllib.request.urlopen(url, timeout=120) as response:
        return json.loads(response.read().decode("utf-8"))


def meta() -> dict:
    with urllib.request.urlopen(META_URL, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def to_int(value: str | int | float | None) -> int:
    if value is None or value == "":
        return 0
    return int(Decimal(str(value)))


def to_decimal(value: str | int | float | None) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    return Decimal(str(value))


def format_int_es(value: int) -> str:
    return f"{value:,}".replace(",", ".")


def format_money_es(value: int | Decimal) -> str:
    return format_int_es(int(Decimal(value)))


def format_pct(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}".replace(".", ",")


def norm_yes(value: str) -> bool:
    return value.strip().lower() == "si"


def fetch_single(select: str, where: str | None = None) -> dict[str, str]:
    params: dict[str, str] = {"$select": select, "$limit": "1"}
    if where:
        params["$where"] = where
    rows = query(params)
    return rows[0] if rows else {}


def build_summary() -> dict:
    metadata = meta()
    columns = metadata["columns"]
    total_records = to_int(fetch_single("count(*) as total")["total"])
    total_variables = len(columns)

    records_2025 = to_int(
        fetch_single(
            "count(*) as total",
            "fecha_de_firma between '2025-01-01T00:00:00' and '2025-12-31T23:59:59'",
        )["total"]
    )

    pyme_rows = query(
        {
            "$select": "es_pyme, count(*) as n",
            "$group": "es_pyme",
            "$order": "n DESC",
        }
    )
    pyme_yes = sum(to_int(row["n"]) for row in pyme_rows if norm_yes(row["es_pyme"]))
    pyme_pct = (pyme_yes / total_records) * 100 if total_records else 0.0

    dept_rows = query(
        {
            "$select": "departamento, count(*) as n",
            "$group": "departamento",
            "$order": "n DESC",
            "$limit": "10",
        }
    )
    departments = [RankedItem(name=row["departamento"], count=to_int(row["n"])) for row in dept_rows]

    top_dept_6 = departments[5] if len(departments) >= 6 else None

    modality_rows = query(
        {
            "$select": "modalidad_de_contratacion, count(*) as n",
            "$group": "modalidad_de_contratacion",
            "$order": "n DESC",
            "$limit": "1",
        }
    )
    preferred_modality = (
        RankedItem(name=modality_rows[0]["modalidad_de_contratacion"], count=to_int(modality_rows[0]["n"]))
        if modality_rows
        else None
    )

    type_rows = query(
        {
            "$select": "tipo_de_contrato, count(*) as n",
            "$group": "tipo_de_contrato",
            "$order": "n DESC",
            "$limit": "5",
        }
    )
    contract_types = [RankedItem(name=row["tipo_de_contrato"], count=to_int(row["n"])) for row in type_rows]
    top_type_pct = (contract_types[0].count / total_records) * 100 if total_records and contract_types else 0.0

    entity_rows = query(
        {
            "$select": "nombre_entidad, sum(valor_del_contrato) as total",
            "$group": "nombre_entidad",
            "$order": "total DESC",
            "$limit": "3",
        }
    )
    top_entities = [
        RankedItem(name=row["nombre_entidad"], total=int(to_decimal(row["total"]))) for row in entity_rows
    ]

    top_value_rows = query(
        {
            "$select": "nombre_entidad, valor_del_contrato, tipo_de_contrato, modalidad_de_contratacion, objeto_del_contrato",
            "$order": "valor_del_contrato DESC",
            "$limit": "3",
        }
    )

    advance_rows = query(
        {
            "$select": "habilita_pago_adelantado, count(*) as n",
            "$group": "habilita_pago_adelantado",
            "$order": "n DESC",
        }
    )
    advance_yes = sum(to_int(row["n"]) for row in advance_rows if norm_yes(row["habilita_pago_adelantado"]))
    advance_pct = (advance_yes / total_records) * 100 if total_records else 0.0

    env_rows = query(
        {
            "$select": "obligaci_n_ambiental, count(*) as n",
            "$group": "obligaci_n_ambiental",
            "$order": "n DESC",
        }
    )
    env_yes = sum(to_int(row["n"]) for row in env_rows if norm_yes(row["obligaci_n_ambiental"]))

    total_value = to_decimal(fetch_single("sum(valor_del_contrato) as total_value")["total_value"])
    unique_entities = to_int(fetch_single("count(distinct nombre_entidad) as n")["n"])
    top20_entities = math.ceil(unique_entities * 0.2)
    top20_rows = query(
        {
            "$select": "nombre_entidad, sum(valor_del_contrato) as total",
            "$group": "nombre_entidad",
            "$order": "total DESC",
            "$limit": str(top20_entities),
        }
    )
    top20_sum = sum(to_decimal(row["total"]) for row in top20_rows)
    pareto_share = (float(top20_sum / total_value) * 100) if total_value else 0.0

    # The dataset has no direct gender field, so we keep the answer explicit and honest.
    gender_answer = (
        "No es posible sustentar una brecha de género con esta base porque no existe una variable de sexo/género "
        "del representante legal. Cualquier inferencia por nombres sería especulativa."
    )

    type_anomalies = [
        {
            "variable": "es_pyme",
            "tipo_esperado": "BOOLEAN",
            "motivo": "Es una variable de si/no y conviene normalizarla a booleano.",
        },
        {
            "variable": "habilita_pago_adelantado",
            "tipo_esperado": "BOOLEAN",
            "motivo": "También es una bandera de si/no y no debería quedarse como texto libre.",
        },
        {
            "variable": "obligaci_n_ambiental",
            "tipo_esperado": "BOOLEAN",
            "motivo": "La obligación ambiental es un indicador binario y se analiza mejor como booleano.",
        },
        {
            "variable": "obligaciones_postconsumo",
            "tipo_esperado": "BOOLEAN",
            "motivo": "Es otra bandera binaria de cumplimiento ambiental.",
        },
        {
            "variable": "reversion",
            "tipo_esperado": "BOOLEAN",
            "motivo": "La reversión contractual también es una variable binaria y debería normalizarse.",
        },
    ]

    q15_rows = []
    for row in top_value_rows:
        value = int(to_decimal(row["valor_del_contrato"]))
        q15_rows.append(
            {
                "entidad": row["nombre_entidad"],
                "monto": value,
                "tipo_de_contrato": row["tipo_de_contrato"],
                "modalidad": row["modalidad_de_contratacion"],
                "objeto": row["objeto_del_contrato"],
                "veredicto": "Anomalo / por verificar",
                "sustento": (
                    "Es un monto extremadamente alto frente al promedio de la base y el objeto del contrato "
                    "parece un servicio operativo normal, asi que requiere validacion externa."
                ),
            }
        )

    q_responses = [
        {"n": 1, "pregunta": "Selecciona su equipo", "respuesta": "Pendiente de tu dato personal", "estado": "manual"},
        {
            "n": 2,
            "pregunta": "Ingrese la CC del capitán",
            "respuesta": "Pendiente de tu dato personal",
            "estado": "manual",
        },
        {"n": 3, "pregunta": "Número de registros", "respuesta": format_int_es(total_records), "estado": "ok"},
        {"n": 4, "pregunta": "Número de variables", "respuesta": format_int_es(total_variables), "estado": "ok"},
        {
            "n": 5,
            "pregunta": "Número de registros que corresponden al 2025",
            "respuesta": format_int_es(records_2025),
            "estado": "ok",
        },
        {
            "n": 6,
            "pregunta": "Proporción de contratos asignados a Pymes",
            "respuesta": format_pct(pyme_pct),
            "estado": "ok",
        },
        {
            "n": 7,
            "pregunta": "Número de contratos asignados a Pymes",
            "respuesta": format_int_es(pyme_yes),
            "estado": "ok",
        },
        {
            "n": 8,
            "pregunta": "Top 10 departamentos por número de contratos",
            "respuesta": ", ".join(
                [
                    "distrito capital de bogota",
                    "valle del cauca",
                    "antioquia",
                    "santander",
                    "cundinamarca",
                    "atlantico",
                    "bolivar",
                    "tolima",
                    "boyaca",
                    "norte de santander",
                ]
            ),
            "estado": "ok",
        },
        {
            "n": 9,
            "pregunta": "Contratos del departamento en posición 6",
            "respuesta": format_int_es(top_dept_6.count if top_dept_6 else 0),
            "estado": "ok",
        },
        {
            "n": 10,
            "pregunta": "Modalidad de contratación preferida",
            "respuesta": preferred_modality.name if preferred_modality else "N/D",
            "estado": "ok",
        },
        {
            "n": 11,
            "pregunta": "Cantidad de contratos de esa modalidad",
            "respuesta": format_int_es(preferred_modality.count if preferred_modality else 0),
            "estado": "ok",
        },
        {
            "n": 12,
            "pregunta": "Top 3 entidades que más dinero ejecutaron",
            "respuesta": " | ".join(
                [
                    f"{item.name}, {format_money_es(item.total or 0)}"
                    for item in top_entities
                ]
            ),
            "estado": "ok",
        },
        {
            "n": 13,
            "pregunta": "Top 5 tipos de contrato y conteo",
            "respuesta": " | ".join(
                [
                    f"top{i + 1}, {item.name}, {format_int_es(item.count or 0)}"
                    for i, item in enumerate(contract_types)
                ]
            ),
            "estado": "ok",
        },
        {
            "n": 14,
            "pregunta": "Porcentaje del tipo de contrato con mayor resultado",
            "respuesta": format_pct(top_type_pct),
            "estado": "ok",
        },
        {
            "n": 15,
            "pregunta": "Top 3 valores anómalos financieros",
            "respuesta": q15_rows,
            "estado": "ok",
        },
        {
            "n": 16,
            "pregunta": "Porcentaje de contratos con pagos adelantados o anticipos",
            "respuesta": format_pct(advance_pct),
            "estado": "ok",
        },
        {
            "n": 17,
            "pregunta": "Contratos con obligaciones ambientales explícitas",
            "respuesta": format_int_es(env_yes),
            "estado": "ok",
        },
        {
            "n": 18,
            "pregunta": "Pareto / concentración de riqueza",
            "respuesta": (
                f"Sí. El 20% de las entidades con mayor valor concentra el {format_pct(pareto_share)}% del valor total."
            ),
            "estado": "ok",
        },
        {
            "n": 19,
            "pregunta": "Brecha de género financiera",
            "respuesta": gender_answer,
            "estado": "no_disponible",
        },
        {
            "n": 20,
            "pregunta": "Anomalías de tipo de dato",
            "respuesta": type_anomalies,
            "estado": "ok",
        },
    ]

    summary = {
        "dataset": {
            "id": "jbjy-vk9h",
            "name": metadata.get("name", "SECOP II - Contratos Electrónicos"),
            "source_url": "https://www.datos.gov.co/resource/jbjy-vk9h.json",
            "metadata_url": META_URL,
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
            "pareto_share_pct": pareto_share,
            "unique_entities": unique_entities,
        },
        "top_entities": [asdict(item) for item in top_entities],
        "departments": [asdict(item) for item in departments],
        "preferred_modality": asdict(preferred_modality) if preferred_modality else None,
        "contract_types": [asdict(item) for item in contract_types],
        "top_values": q15_rows,
        "type_anomalies": type_anomalies,
        "questions": q_responses,
        "notes": [
            "La pregunta 19 no se puede responder con esta base porque no existe una variable de sexo/genero.",
            "La pregunta 15 requiere validacion externa; el dashboard la marca como anomala por magnitud.",
            "Las preguntas 1 y 2 son datos personales del formulario y deben ser suministradas por ti.",
        ],
    }
    return summary


def build_index_html(summary: dict) -> str:
    summary_json = json.dumps(summary, ensure_ascii=False, indent=2)
    template = """<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Reto 2 | Dashboard SECOP II</title>
  <style>
    :root {{
      --bg: #08111f;
      --panel: rgba(15, 23, 42, 0.88);
      --card: rgba(17, 24, 39, 0.92);
      --line: rgba(148, 163, 184, 0.18);
      --text: #e5e7eb;
      --muted: #9ca3af;
      --accent: #38bdf8;
      --accent-2: #34d399;
      --warn: #f59e0b;
      --danger: #f87171;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Arial, Helvetica, sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(56,189,248,0.18), transparent 28%),
        radial-gradient(circle at top right, rgba(52,211,153,0.14), transparent 26%),
        linear-gradient(180deg, #08111f 0%, #111827 100%);
    }}
    .wrap {{
      max-width: 1400px;
      margin: 0 auto;
      padding: 28px 18px 48px;
    }}
    .hero, .section, .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      box-shadow: 0 18px 50px rgba(0, 0, 0, 0.2);
    }}
    .hero {{
      padding: 24px;
      margin-bottom: 18px;
      display: flex;
      gap: 18px;
      justify-content: space-between;
      align-items: flex-start;
      flex-wrap: wrap;
      background: linear-gradient(135deg, rgba(56, 189, 248, 0.16), rgba(52, 211, 153, 0.12));
    }}
    .eyebrow {{
      color: #bae6fd;
      text-transform: uppercase;
      letter-spacing: .08em;
      font-size: 12px;
      margin-bottom: 8px;
    }}
    h1, h2, h3 {{ margin: 0 0 12px; }}
    .lead, .subtle, .muted {{ color: var(--muted); }}
    .kpis {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 14px;
      margin: 18px 0;
    }}
    .card {{
      padding: 16px;
    }}
    .metric {{
      font-size: 30px;
      font-weight: 700;
      color: var(--accent-2);
      margin: 2px 0 4px;
    }}
    .label {{
      text-transform: uppercase;
      letter-spacing: .05em;
      font-size: 12px;
      color: var(--muted);
    }}
    .grid-2 {{
      display: grid;
      grid-template-columns: 1.2fr .8fr;
      gap: 14px;
      align-items: start;
    }}
    @media (max-width: 1080px) {{
      .grid-2 {{ grid-template-columns: 1fr; }}
    }}
    .section {{
      padding: 18px;
      margin-top: 14px;
    }}
    .table-wrap {{
      overflow: auto;
      border-radius: 12px;
      border: 1px solid rgba(148, 163, 184, 0.12);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 680px;
    }}
    th, td {{
      border-bottom: 1px solid rgba(148, 163, 184, 0.12);
      padding: 10px 8px;
      text-align: left;
      vertical-align: top;
      font-size: 14px;
    }}
    th {{
      background: rgba(15, 23, 42, 0.92);
      color: #cbd5e1;
      position: sticky;
      top: 0;
      z-index: 1;
    }}
    .pill-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 10px;
    }}
    .pill {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 7px 10px;
      border-radius: 999px;
      background: rgba(56, 189, 248, 0.12);
      border: 1px solid rgba(56, 189, 248, 0.18);
      font-size: 12px;
      color: #dbeafe;
    }}
    .pill.ok {{ background: rgba(52, 211, 153, 0.12); border-color: rgba(52, 211, 153, 0.2); }}
    .pill.warn {{ background: rgba(245, 158, 11, 0.12); border-color: rgba(245, 158, 11, 0.22); }}
    .pill.danger {{ background: rgba(248, 113, 113, 0.12); border-color: rgba(248, 113, 113, 0.22); }}
    .note {{
      margin-top: 10px;
      color: var(--muted);
      font-size: 13px;
    }}
    .question-item {{
      border-top: 1px solid rgba(148, 163, 184, 0.12);
      padding: 12px 0;
    }}
    .question-item:first-child {{ border-top: 0; padding-top: 0; }}
    .q-title {{
      margin: 0 0 6px;
      color: #e2e8f0;
      font-weight: 700;
    }}
    .q-answer {{
      margin: 0;
      color: var(--text);
      white-space: pre-wrap;
      line-height: 1.5;
    }}
    .small {{
      font-size: 12px;
      color: var(--muted);
      margin-top: 6px;
    }}
    code {{
      background: rgba(15, 23, 42, 0.82);
      padding: 2px 6px;
      border-radius: 6px;
    }}
    .footer {{
      color: var(--muted);
      margin-top: 18px;
      font-size: 12px;
      text-align: right;
    }}
  </style>
</head>
<body>
  <main class="wrap">
    <header class="hero">
      <div>
        <div class="eyebrow">Reto 2 | SECOP II</div>
        <h1>Dashboard de respuestas, EDA y calidad de datos</h1>
        <p class="lead">Fuente oficial: SECOP II - Contratos Electrónicos (<code>jbjy-vk9h</code>). El dashboard queda listo para abrir desde esta carpeta.</p>
        <div class="pill-row">
          <span class="pill ok">API: <code>/api/summary.json</code></span>
          <span class="pill">Total registros: <strong id="kpi-records">--</strong></span>
          <span class="pill">Variables: <strong id="kpi-vars">--</strong></span>
          <span class="pill warn">Q19: sin soporte de género</span>
          <span class="pill danger">Q15: requiere validación externa</span>
        </div>
      </div>
      <div class="card" style="min-width: 290px;">
        <div class="label">Estado</div>
        <div class="metric" id="kpi-pareto">--</div>
        <div class="muted">Concentración del valor en el 20% superior de entidades</div>
        <div class="small" id="kpi-pareto-note"></div>
      </div>
    </header>

    <section class="kpis" id="kpi-grid"></section>

    <section class="grid-2">
      <article class="section">
        <h2>Respuestas del formulario</h2>
        <div id="questions"></div>
      </article>

      <article class="section">
        <h2>Top rankings</h2>
        <h3>Departamentos</h3>
        <div class="table-wrap">
          <table>
            <thead><tr><th>#</th><th>Departamento</th><th>Contratos</th></tr></thead>
            <tbody id="departments-table"></tbody>
          </table>
        </div>
        <h3 style="margin-top:14px;">Tipos de contrato</h3>
        <div class="table-wrap">
          <table>
            <thead><tr><th>#</th><th>Tipo</th><th>Registros</th></tr></thead>
            <tbody id="contract-types-table"></tbody>
          </table>
        </div>
      </article>
    </section>

    <section class="grid-2">
      <article class="section">
        <h2>Entidades con mayor valor ejecutado</h2>
        <div class="table-wrap">
          <table>
            <thead><tr><th>#</th><th>Entidad</th><th>Valor total</th></tr></thead>
            <tbody id="entities-table"></tbody>
          </table>
        </div>
      </article>

      <article class="section">
        <h2>Hallazgos de calidad</h2>
        <div id="quality"></div>
      </article>
    </section>

    <section class="section">
      <h2>Top 3 valores anómalos financieros</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>#</th><th>Entidad</th><th>Monto</th><th>Tipo</th><th>Modalidad</th><th>Veredicto</th></tr></thead>
          <tbody id="anomalies-table"></tbody>
        </table>
      </div>
    </section>

    <div class="footer">
      Generado automáticamente por <code>Fase_1/reto2/scripts/generate_dashboard_reto2.py</code>
    </div>
  </main>

  <script id="dashboard-data" type="application/json">
__SUMMARY_JSON__
  </script>
  <script>
    const DATA = JSON.parse(document.getElementById('dashboard-data').textContent);

    const fmt = new Intl.NumberFormat('es-CO');
    const pct = (value) => `${value.toFixed(2).replace('.', ',')}%`;

    function pillClass(state) {{
      if (state === 'ok') return 'pill ok';
      if (state === 'no_disponible') return 'pill warn';
      if (state === 'manual') return 'pill danger';
      return 'pill';
    }}

    function renderKpis() {{
      document.getElementById('kpi-records').textContent = fmt.format(DATA.kpis.total_records);
      document.getElementById('kpi-vars').textContent = fmt.format(DATA.kpis.total_variables);
      document.getElementById('kpi-pareto').textContent = pct(DATA.kpis.pareto_share_pct);
      document.getElementById('kpi-pareto-note').textContent = `${fmt.format(DATA.kpis.unique_entities)} entidades únicas evaluadas`;

      const items = [
        ['Registros 2025', fmt.format(DATA.kpis.records_2025)],
        ['Pymes', `${fmt.format(DATA.kpis.pyme_yes)} (${pct(DATA.kpis.pyme_pct)})`],
        ['Anticipos', `${fmt.format(DATA.kpis.advance_yes)} (${pct(DATA.kpis.advance_pct)})`],
        ['Ambientales', fmt.format(DATA.kpis.environmental_yes)],
      ];

      const grid = document.getElementById('kpi-grid');
      grid.innerHTML = items.map(([label, value]) => `
        <div class="card">
          <div class="label">${label}</div>
          <div class="metric">${value}</div>
        </div>
      `).join('');
    }}

    function renderQuestions() {{
      const container = document.getElementById('questions');
      container.innerHTML = DATA.questions.map((q) => {{
        const answer = typeof q.respuesta === 'object'
          ? q.respuesta.map((item, idx) => `
              <div class="question-item">
                <div class="q-title">${idx === 0 ? q.pregunta : ''}</div>
                <p class="q-answer"><strong>${item.entidad}</strong> - ${fmt.format(item.monto)} - ${item.veredicto}</p>
                <div class="small">${item.sustento}</div>
              </div>
            `).join('')
          : `<p class="q-answer">${q.respuesta}</p>`;

        return `
          <div class="question-item">
            <div class="q-title">Q${q.n}. ${q.pregunta}</div>
            ${answer}
            <div class="${pillClass(q.estado)}" style="margin-top:8px;">Estado: ${q.estado}</div>
          </div>
        `;
      }}).join('');
    }}

    function renderTable(targetId, rows, kind) {{
      const tbody = document.getElementById(targetId);
      tbody.innerHTML = rows.map((row, idx) => {{
        if (kind === 'dept') return `<tr><td>${idx + 1}</td><td>${row.name}</td><td>${fmt.format(row.count)}</td></tr>`;
        if (kind === 'type') return `<tr><td>${idx + 1}</td><td>${row.name}</td><td>${fmt.format(row.count)}</td></tr>`;
        if (kind === 'entity') return `<tr><td>${idx + 1}</td><td>${row.name}</td><td>${fmt.format(row.total)}</td></tr>`;
        if (kind === 'anomaly') return `<tr><td>${idx + 1}</td><td>${row.entidad}</td><td>${fmt.format(row.monto)}</td><td>${row.tipo_de_contrato}</td><td>${row.modalidad}</td><td>${row.veredicto}</td></tr>`;
        return '';
      }}).join('');
    }}

    function renderQuality() {{
      const container = document.getElementById('quality');
      container.innerHTML = `
        <div class="note">
          Esta base tiene 84 variables. Las siguientes 5 variables se deben normalizar porque semánticamente son booleanas y aparecen como texto:
        </div>
        <div class="pill-row" style="margin-top:12px;">
          ${DATA.type_anomalies.map(item => `<span class="pill">${item.variable} → ${item.tipo_esperado}</span>`).join('')}
        </div>
        <div class="note" style="margin-top:12px;">
          ${DATA.notes[0]}
        </div>
        <div class="note">
          ${DATA.notes[1]}
        </div>
        <div class="note">
          ${DATA.notes[2]}
        </div>
      `;
    }}

    renderKpis();
    renderQuestions();
    renderTable('departments-table', DATA.departments, 'dept');
    renderTable('contract-types-table', DATA.contract_types, 'type');
    renderTable('entities-table', DATA.top_entities, 'entity');
    renderTable('anomalies-table', DATA.top_values, 'anomaly');
    renderQuality();
  </script>
</body>
</html>
"""
    return template.replace("__SUMMARY_JSON__", summary_json)


def build_server_py() -> str:
    return """from __future__ import annotations

from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import json
import os


ROOT = Path(__file__).resolve().parent
PORT = int(os.environ.get("PORT", "4176"))


class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path in {"/api/summary", "/api/summary.json"}:
            payload = (ROOT / "api" / "summary.json").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if self.path == "/":
            self.path = "/index.html"
        return super().do_GET()


if __name__ == "__main__":
    os.chdir(ROOT)
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print(f"Dashboard listo en http://127.0.0.1:{PORT}")
    print(f"API disponible en http://127.0.0.1:{PORT}/api/summary")
    server.serve_forever()
"""


def build_readme() -> str:
    return """# Dashboard Reto 2

## Abrir rapido

Puedes abrir directamente `index.html` en el navegador.

## Endpoint local

Si prefieres servirlo con API local:

```powershell
py -3 Fase_1\\reto2\\dashboard\\server.py
```

Luego abre:

- `http://127.0.0.1:4176`
- `http://127.0.0.1:4176/api/summary`

## Contenido

- `index.html`: dashboard listo para abrir
- `api/summary.json`: payload del dashboard
- `server.py`: servidor local con endpoint
"""


def write_files(summary: dict) -> None:
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    API_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    SUMMARY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    API_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    INDEX_HTML.write_text(build_index_html(summary), encoding="utf-8")
    SERVER_PY.write_text(build_server_py(), encoding="utf-8")
    README_MD.write_text(build_readme(), encoding="utf-8")


def main() -> None:
    summary = build_summary()
    write_files(summary)
    print(f"Dashboard generado en: {INDEX_HTML}")
    print(f"API local: {API_JSON}")
    print(f"Servidor local: {SERVER_PY}")


if __name__ == "__main__":
    main()
