from __future__ import annotations

import json
from html import escape
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SUMMARY_PATH = ROOT / "dashboard" / "api" / "summary.json"
REPORTS_DIR = ROOT / "reports"
EDA_PATH = REPORTS_DIR / "reto2_eda.html"
QUALITY_PATH = REPORTS_DIR / "reto2_calidad.html"


def format_int_es(value: int) -> str:
    return f"{value:,}".replace(",", ".")


def format_pct(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}".replace(".", ",")


def format_money(value: int) -> str:
    return format_int_es(int(value))


def load_summary() -> dict:
    return json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))


def pill(text: str, kind: str = "") -> str:
    cls = f"pill {kind}" if kind else "pill"
    return f'<span class="{cls}">{escape(text)}</span>'


def bar_rows(items: list[dict], label_key: str, value_key: str, value_label: str) -> str:
    max_value = max(int(item[value_key]) for item in items) if items else 1
    rows = []
    for item in items:
        value = int(item[value_key])
        width = max(6, round((value / max_value) * 100))
        rows.append(
            f"""
            <div class="bar-row">
              <div class="bar-topline">
                <strong>{escape(str(item[label_key]))}</strong>
                <span class="bar-meta">{value_label}: {format_int_es(value)}</span>
              </div>
              <div class="bar-track"><div class="bar-fill" style="width: {width}%"></div></div>
            </div>
            """
        )
    return "\n".join(rows)


def render_eda_html(summary: dict) -> str:
    q = summary["questions"]
    top_departments = summary["departments"]
    top_types = summary["contract_types"]
    top_entities = summary["top_entities"]
    top_values = summary["top_values"]
    preferred_modality = summary["preferred_modality"]
    k = summary["kpis"]

    question_rows = []
    for item in q:
        if item["n"] in (1, 2):
            answer_html = f'<span class="muted">Pendiente de tu dato personal.</span>'
        elif item["n"] == 15:
            answer_html = "".join(
                f"""
                <div class="answer-card">
                  <strong>{escape(row['entidad'])}</strong>
                  <span>Monto: {format_money(row['monto'])}</span>
                  <span>Tipo: {escape(row['tipo_de_contrato'])}</span>
                  <span>Modalidad: {escape(row['modalidad'])}</span>
                  <span>Veredicto: {escape(row['veredicto'])}</span>
                  <span>{escape(row['sustento'])}</span>
                </div>
                """
                for row in item["respuesta"]
            )
        elif item["n"] == 20:
            answer_html = "".join(
                f"""
                <tr>
                  <td>{escape(row['variable'])}</td>
                  <td>{escape(row['tipo_esperado'])}</td>
                  <td>{escape(row['motivo'])}</td>
                </tr>
                """
                for row in item["respuesta"]
            )
        else:
            answer_html = escape(str(item["respuesta"]))

        question_rows.append(
            f"""
            <article class="qa-card">
              <div class="qa-title">
                <span class="qa-number">{item['n']}</span>
                <span>{escape(item['pregunta'])}</span>
              </div>
              <div class="qa-body">{answer_html}</div>
              <div class="qa-status">{escape(item['estado'])}</div>
            </article>
            """
        )

    question_cards_html = "".join(question_rows)
    top_values_rows_html = "".join(
        f"<tr><td>{i + 1}</td><td>{escape(row['entidad'])}</td><td>{format_money(row['monto'])}</td><td>{escape(row['tipo_de_contrato'])}</td><td>{escape(row['modalidad'])}</td><td>{escape(row['veredicto'])}</td></tr>"
        for i, row in enumerate(top_values)
    )

    html = f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>EDA | Reto 2</title>
  <style>
    :root {{
      --bg: #08111f;
      --panel: #111a2e;
      --panel-2: #16213a;
      --text: #ecf2ff;
      --muted: #98a9cb;
      --accent: #6ee7ff;
      --accent-2: #8b5cf6;
      --good: #35d07f;
      --warn: #fbbf24;
      --bad: #fb7185;
      --border: rgba(160, 180, 220, 0.18);
      --shadow: 0 24px 80px rgba(0, 0, 0, 0.28);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Arial, Helvetica, sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(110, 231, 255, 0.16), transparent 30%),
        radial-gradient(circle at top right, rgba(139, 92, 246, 0.18), transparent 28%),
        linear-gradient(180deg, #06101b 0%, var(--bg) 100%);
    }}
    main {{
      max-width: 1440px;
      margin: 0 auto;
      padding: 32px 20px 56px;
    }}
    .hero {{
      display: grid;
      gap: 18px;
      padding: 28px;
      border: 1px solid var(--border);
      border-radius: 24px;
      background: rgba(17, 26, 46, 0.9);
      box-shadow: var(--shadow);
    }}
    .hero-top {{
      display: flex;
      justify-content: space-between;
      gap: 20px;
      flex-wrap: wrap;
    }}
    .badge {{
      display: inline-flex;
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(110, 231, 255, 0.12);
      color: var(--accent);
      font-size: 0.88rem;
      font-weight: 700;
    }}
    h1, h2, h3 {{ margin: 0; }}
    h1 {{
      margin-top: 14px;
      font-size: clamp(2rem, 5vw, 3.4rem);
      line-height: 1.04;
    }}
    .lead, .subtle, .muted {{ color: var(--muted); }}
    .hero-actions {{ display: flex; flex-wrap: wrap; gap: 10px; align-items: start; }}
    .button {{
      appearance: none;
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 12px 16px;
      background: var(--panel-2);
      color: var(--text);
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      justify-content: center;
    }}
    .button.primary {{
      background: linear-gradient(90deg, var(--accent), var(--accent-2));
      color: #04111b;
      font-weight: 700;
      border-color: transparent;
    }}
    .status-row {{ display: flex; flex-wrap: wrap; gap: 10px; }}
    .pill {{
      padding: 8px 12px;
      border-radius: 999px;
      border: 1px solid var(--border);
      background: rgba(255,255,255,.05);
      color: var(--text);
      font-size: .9rem;
    }}
    .pill.ok {{ background: rgba(52, 211, 153, .12); border-color: rgba(52,211,153,.2); }}
    .pill.warn {{ background: rgba(245, 158, 11, .12); border-color: rgba(245,158,11,.22); }}
    .pill.bad {{ background: rgba(248, 113, 113, .12); border-color: rgba(248,113,113,.22); }}
    .shell {{
      display: grid;
      gap: 18px;
      grid-template-columns: 340px minmax(0, 1fr);
      margin-top: 18px;
    }}
    .sidebar, .content {{ display: grid; gap: 18px; }}
    .panel {{
      border: 1px solid var(--border);
      border-radius: 22px;
      background: rgba(17, 26, 46, 0.84);
      padding: 18px;
      box-shadow: 0 18px 50px rgba(0,0,0,.16);
    }}
    .panel-head {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 14px;
    }}
    .section-kicker {{
      margin: 0 0 6px;
      color: #bae6fd;
      text-transform: uppercase;
      letter-spacing: .08em;
      font-size: .78rem;
      font-weight: 700;
    }}
    .kpi-grid {{
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    }}
    .kpi-card {{
      background: var(--panel-2);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 16px;
    }}
    .kpi-label {{
      color: var(--muted);
      font-size: .8rem;
      text-transform: uppercase;
      letter-spacing: .08em;
    }}
    .kpi-value {{
      margin-top: 10px;
      font-size: 1.5rem;
      font-weight: 700;
    }}
    .kpi-hint {{
      margin-top: 8px;
      color: var(--muted);
      line-height: 1.5;
      font-size: .9rem;
    }}
    .bars {{ display: grid; gap: 12px; }}
    .bar-row {{ display: grid; gap: 6px; }}
    .bar-topline {{ display: flex; justify-content: space-between; gap: 10px; align-items: baseline; }}
    .bar-meta {{ color: var(--muted); font-size: .86rem; }}
    .bar-track {{ height: 14px; border-radius: 999px; background: rgba(255,255,255,.08); overflow: hidden; }}
    .bar-fill {{ height: 100%; border-radius: 999px; background: linear-gradient(90deg, var(--accent), var(--accent-2)); }}
    .analysis-grid {{ display: grid; gap: 18px; grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    .question-grid {{ display: grid; gap: 12px; }}
    .qa-card {{
      border: 1px solid var(--border);
      border-radius: 18px;
      background: rgba(255,255,255,.03);
      padding: 14px;
      display: grid;
      gap: 8px;
    }}
    .qa-title {{
      display: flex;
      gap: 10px;
      align-items: flex-start;
      font-weight: 700;
      line-height: 1.35;
    }}
    .qa-number {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 30px;
      height: 30px;
      padding: 0 8px;
      border-radius: 999px;
      background: rgba(110, 231, 255, 0.14);
      color: var(--accent);
      font-size: 0.9rem;
      font-weight: 700;
      flex: 0 0 auto;
    }}
    .qa-status {{
      width: fit-content;
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(56,189,248,.12);
      border: 1px solid rgba(56,189,248,.18);
      color: #dbeafe;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .04em;
    }}
    .answer-card {{
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 12px;
      background: rgba(22,33,58,.88);
      display: grid;
      gap: 4px;
    }}
    .table-wrap {{ overflow: auto; border-radius: 16px; border: 1px solid rgba(148,163,184,.12); }}
    table {{ width: 100%; border-collapse: collapse; min-width: 780px; }}
    th, td {{
      border-bottom: 1px solid rgba(148,163,184,.12);
      padding: 10px 8px;
      text-align: left;
      vertical-align: top;
      font-size: 14px;
    }}
    th {{
      background: rgba(15,23,42,.92);
      color: #cbd5e1;
      position: sticky;
      top: 0;
      z-index: 1;
      text-transform: uppercase;
      letter-spacing: .06em;
      font-size: .78rem;
    }}
    .footer-note {{
      color: var(--muted);
      text-align: right;
      padding-top: 6px;
    }}
    @media (max-width: 1180px) {{
      .shell, .analysis-grid {{ grid-template-columns: 1fr; }}
    }}
    @media (max-width: 760px) {{
      main {{ padding: 20px 12px 36px; }}
      .hero, .panel {{ padding: 16px; border-radius: 18px; }}
    }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <div class="hero-top">
        <div>
          <span class="badge">Fase 2 | EDA</span>
          <h1>EDA con respuestas numeradas</h1>
          <p class="lead">
            Este HTML separa la parte de EDA, como en el reto 1: preguntas respondidas, rankings,
            Pymes, modalidades, contratos y observaciones para la sustentación.
          </p>
        </div>
        <div class="hero-actions">
          <a class="button primary" href="../dashboard/index.html">Abrir dashboard</a>
          <a class="button" href="./reto2_calidad.html">Ver calidad de datos</a>
          <a class="button" href="./../dashboard/api/summary.json" target="_blank" rel="noreferrer">API local</a>
        </div>
      </div>
      <div class="status-row">
        {pill(f"Registros: {format_int_es(k['total_records'])}", "ok")}
        {pill(f"Variables: {format_int_es(k['total_variables'])}")}
        {pill(f"Registros 2025: {format_int_es(k['records_2025'])}", "ok")}
        {pill(f"Pymes: {format_pct(k['pyme_pct'])}", "warn")}
        {pill(f"Anticipos: {format_pct(k['advance_pct'])}", "warn")}
        {pill(f"Ambientales: {format_int_es(k['environmental_yes'])}", "ok")}
      </div>
    </section>

    <section class="shell">
      <aside class="sidebar">
        <section class="panel">
          <div class="panel-head">
            <div>
              <p class="section-kicker">Fuente</p>
              <h2>{escape(summary['dataset']['name'])}</h2>
            </div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label">Dataset</div>
            <div class="kpi-value">{escape(summary['dataset']['id'])}</div>
            <div class="kpi-hint">API oficial: {escape(summary['dataset']['source_url'])}</div>
          </div>
          <div class="kpi-card" style="margin-top:12px;">
            <div class="kpi-label">Modalidad preferida</div>
            <div class="kpi-value">{escape(preferred_modality['name'])}</div>
            <div class="kpi-hint">Con {format_int_es(preferred_modality['count'])} contratos.</div>
          </div>
        </section>

        <section class="panel">
          <div class="panel-head">
            <div>
              <p class="section-kicker">Notas</p>
              <h2>Lo importante para defender</h2>
            </div>
          </div>
          <ul class="muted" style="margin:0; padding-left:18px; line-height:1.65;">
            <li>La base sí soporta el análisis de Pymes, anticipos, modalidad, tipo de contrato y cláusulas ambientales.</li>
            <li>La pregunta de género no se puede sustentar porque no hay un campo directo de sexo o género.</li>
            <li>Los montos más altos son tan grandes que deben validarse con la fuente original antes de afirmarlos como correctos.</li>
          </ul>
        </section>
      </aside>

      <section class="content">
        <section class="panel">
          <div class="panel-head">
            <div>
              <p class="section-kicker">Indicadores</p>
              <h2>Resumen ejecutivo</h2>
            </div>
          </div>
          <div class="kpi-grid">
            <div class="kpi-card">
              <div class="kpi-label">Registros</div>
              <div class="kpi-value">{format_int_es(k['total_records'])}</div>
              <div class="kpi-hint">Volumen total de contratos.</div>
            </div>
            <div class="kpi-card">
              <div class="kpi-label">Registros 2025</div>
              <div class="kpi-value">{format_int_es(k['records_2025'])}</div>
              <div class="kpi-hint">Casos ubicados en 2025.</div>
            </div>
            <div class="kpi-card">
              <div class="kpi-label">Pymes</div>
              <div class="kpi-value">{format_pct(k['pyme_pct'])}</div>
              <div class="kpi-hint">{format_int_es(k['pyme_yes'])} contratos adjudicados a Pymes.</div>
            </div>
            <div class="kpi-card">
              <div class="kpi-label">Top modalidad</div>
              <div class="kpi-value">{escape(preferred_modality['name'])}</div>
              <div class="kpi-hint">{format_int_es(preferred_modality['count'])} contratos.</div>
            </div>
          </div>
        </section>

        <section class="analysis-grid">
          <article class="panel">
            <div class="panel-head">
              <div>
                <p class="section-kicker">Ranking</p>
                <h2>Top 10 departamentos</h2>
              </div>
            </div>
            <div class="bars">
              {bar_rows(top_departments, 'name', 'count', 'Contratos')}
            </div>
          </article>

          <article class="panel">
            <div class="panel-head">
              <div>
                <p class="section-kicker">Ranking</p>
                <h2>Top 3 entidades por valor</h2>
              </div>
            </div>
            <div class="bars">
              {bar_rows(top_entities, 'name', 'total', 'Valor')}
            </div>
          </article>
        </section>

        <section class="analysis-grid">
          <article class="panel">
            <div class="panel-head">
              <div>
                <p class="section-kicker">Clasificación</p>
                <h2>Top 5 tipos de contrato</h2>
              </div>
            </div>
            <div class="bars">
              {bar_rows(top_types, 'name', 'count', 'Registros')}
            </div>
          </article>

          <article class="panel">
            <div class="panel-head">
              <div>
                <p class="section-kicker">Hallazgos</p>
                <h2>Calidad del análisis</h2>
              </div>
            </div>
            <div class="qa-card">
              <div class="qa-title">Q18. Pareto / concentración</div>
              <div class="qa-body">Sí. El 20% de las entidades con mayor valor concentra el {format_pct(k['pareto_share_pct'])} del valor total.</div>
            </div>
            <div class="qa-card">
              <div class="qa-title">Q19. Brecha de género</div>
              <div class="qa-body">{escape(summary['questions'][18]['respuesta'])}</div>
            </div>
            <div class="qa-card">
              <div class="qa-title">Q20. Anomalías de tipo de dato</div>
              <div class="qa-body">Se identificaron 5 variables que conviene normalizar a booleano.</div>
            </div>
          </article>
        </section>

        <section class="panel">
          <div class="panel-head">
            <div>
              <p class="section-kicker">Preguntas</p>
              <h2>Respuestas numeradas del formulario</h2>
            </div>
          </div>
          <div class="question-grid">
            {question_cards_html}
          </div>
        </section>

        <section class="panel">
          <div class="panel-head">
            <div>
              <p class="section-kicker">Top 3</p>
              <h2>Valores anómalos financieros</h2>
            </div>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>#</th>
                  <th>Entidad</th>
                  <th>Monto</th>
                  <th>Tipo</th>
                  <th>Modalidad</th>
                  <th>Veredicto</th>
                </tr>
              </thead>
              <tbody>
                {top_values_rows_html}
              </tbody>
            </table>
          </div>
        </section>

        <footer class="footer-note">
          EDA separado del dashboard, como en el reto 1.
        </footer>
      </section>
    </section>
  </main>
</body>
</html>
"""
    return html


def render_quality_html(summary: dict) -> str:
    k = summary["kpis"]
    type_anomalies = summary["type_anomalies"]
    notes = summary["notes"]
    rows = summary["questions"][14]["respuesta"]

    type_anomalies_html = "".join(
        f"""
        <div class="quality-card">
          <strong>{escape(item['variable'])}</strong>
          <span>Tipo esperado: {escape(item['tipo_esperado'])}</span>
          <span>{escape(item['motivo'])}</span>
        </div>
        """
        for item in type_anomalies
    )
    top_values_rows_html = "".join(
        f"<tr><td>{i + 1}</td><td>{escape(row['entidad'])}</td><td>{format_money(row['monto'])}</td><td>{escape(row['tipo_de_contrato'])}</td><td>{escape(row['modalidad'])}</td><td>{escape(row['veredicto'])}</td></tr>"
        for i, row in enumerate(rows)
    )

    html = f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Reto 2 | Calidad de datos</title>
  <style>
    :root {{
      --bg: #08111f;
      --panel: #111a2e;
      --panel-2: #16213a;
      --text: #ecf2ff;
      --muted: #98a9cb;
      --accent: #6ee7ff;
      --accent-2: #8b5cf6;
      --good: #35d07f;
      --warn: #fbbf24;
      --bad: #fb7185;
      --border: rgba(160, 180, 220, 0.18);
      --shadow: 0 24px 80px rgba(0, 0, 0, 0.28);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Arial, Helvetica, sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(110, 231, 255, 0.16), transparent 30%),
        radial-gradient(circle at top right, rgba(139, 92, 246, 0.18), transparent 28%),
        linear-gradient(180deg, #06101b 0%, var(--bg) 100%);
    }}
    main {{
      max-width: 1440px;
      margin: 0 auto;
      padding: 32px 20px 56px;
    }}
    .hero {{
      display: grid;
      gap: 18px;
      padding: 28px;
      border: 1px solid var(--border);
      border-radius: 24px;
      background: rgba(17, 26, 46, 0.9);
      box-shadow: var(--shadow);
    }}
    .hero-top {{
      display: flex;
      justify-content: space-between;
      gap: 20px;
      flex-wrap: wrap;
    }}
    .badge {{
      display: inline-flex;
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(52, 211, 153, 0.12);
      color: #bbf7d0;
      font-size: 0.88rem;
      font-weight: 700;
    }}
    h1, h2, h3 {{ margin: 0; }}
    h1 {{
      margin-top: 14px;
      font-size: clamp(2rem, 5vw, 3.4rem);
      line-height: 1.04;
    }}
    .lead, .subtle, .muted {{ color: var(--muted); }}
    .hero-actions {{ display: flex; flex-wrap: wrap; gap: 10px; }}
    .button {{
      appearance: none;
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 12px 16px;
      background: var(--panel-2);
      color: var(--text);
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      justify-content: center;
    }}
    .button.primary {{
      background: linear-gradient(90deg, var(--accent), var(--accent-2));
      color: #04111b;
      font-weight: 700;
      border-color: transparent;
    }}
    .status-row {{ display: flex; flex-wrap: wrap; gap: 10px; }}
    .pill {{
      padding: 8px 12px;
      border-radius: 999px;
      border: 1px solid var(--border);
      background: rgba(255,255,255,.05);
      color: var(--text);
      font-size: .9rem;
    }}
    .pill.ok {{ background: rgba(52, 211, 153, .12); border-color: rgba(52,211,153,.2); }}
    .pill.warn {{ background: rgba(245, 158, 11, .12); border-color: rgba(245,158,11,.22); }}
    .pill.bad {{ background: rgba(248, 113, 113, .12); border-color: rgba(248,113,113,.22); }}
    .shell {{
      display: grid;
      gap: 18px;
      grid-template-columns: 340px minmax(0, 1fr);
      margin-top: 18px;
    }}
    .sidebar, .content {{ display: grid; gap: 18px; }}
    .panel {{
      border: 1px solid var(--border);
      border-radius: 22px;
      background: rgba(17, 26, 46, 0.84);
      padding: 18px;
      box-shadow: 0 18px 50px rgba(0,0,0,.16);
    }}
    .panel-head {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 14px;
    }}
    .section-kicker {{
      margin: 0 0 6px;
      color: #bae6fd;
      text-transform: uppercase;
      letter-spacing: .08em;
      font-size: .78rem;
      font-weight: 700;
    }}
    .kpi-grid {{
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    }}
    .kpi-card {{
      background: var(--panel-2);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 16px;
    }}
    .kpi-label {{
      color: var(--muted);
      font-size: .8rem;
      text-transform: uppercase;
      letter-spacing: .08em;
    }}
    .kpi-value {{
      margin-top: 10px;
      font-size: 1.5rem;
      font-weight: 700;
    }}
    .kpi-hint {{
      margin-top: 8px;
      color: var(--muted);
      line-height: 1.5;
      font-size: .9rem;
    }}
    .quality-grid {{
      display: grid;
      gap: 12px;
    }}
    .quality-card {{
      border: 1px solid var(--border);
      border-radius: 18px;
      background: rgba(255,255,255,.03);
      padding: 14px;
      display: grid;
      gap: 8px;
    }}
    .table-wrap {{
      overflow: auto;
      border-radius: 16px;
      border: 1px solid rgba(148,163,184,.12);
    }}
    table {{ width: 100%; border-collapse: collapse; min-width: 720px; }}
    th, td {{
      border-bottom: 1px solid rgba(148,163,184,.12);
      padding: 10px 8px;
      text-align: left;
      vertical-align: top;
      font-size: 14px;
    }}
    th {{
      background: rgba(15,23,42,.92);
      color: #cbd5e1;
      position: sticky;
      top: 0;
      z-index: 1;
      text-transform: uppercase;
      letter-spacing: .06em;
      font-size: .78rem;
    }}
    .bullet-list {{
      margin: 0;
      padding-left: 18px;
      line-height: 1.65;
    }}
    .footer-note {{
      color: var(--muted);
      text-align: right;
      padding-top: 6px;
    }}
    @media (max-width: 1180px) {{
      .shell {{ grid-template-columns: 1fr; }}
    }}
    @media (max-width: 760px) {{
      main {{ padding: 20px 12px 36px; }}
      .hero, .panel {{ padding: 16px; border-radius: 18px; }}
    }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <div class="hero-top">
        <div>
          <span class="badge">Fase 2 | Calidad de datos</span>
          <h1>Validación semántica y calidad de la base</h1>
          <p class="lead">
            Este HTML separa la calidad de datos del EDA, igual que en el reto 1.
            Aquí se muestran los puntos débiles del dataset y las variables que conviene normalizar.
          </p>
        </div>
        <div class="hero-actions">
          <a class="button primary" href="./reto2_eda.html">Abrir EDA</a>
          <a class="button" href="../dashboard/index.html">Abrir dashboard</a>
          <a class="button" href="./../dashboard/api/summary.json" target="_blank" rel="noreferrer">API local</a>
        </div>
      </div>
      <div class="status-row">
        {pill(f"Registros: {format_int_es(k['total_records'])}", "ok")}
        {pill(f"Variables: {format_int_es(k['total_variables'])}")}
        {pill("Brecha de género: no sustentable", "warn")}
        {pill("Anomalías financieras: revisar", "bad")}
      </div>
    </section>

    <section class="shell">
      <aside class="sidebar">
        <section class="panel">
          <div class="panel-head">
            <div>
              <p class="section-kicker">Hallazgos</p>
              <h2>Resumen de calidad</h2>
            </div>
          </div>
          <div class="kpi-grid">
            <div class="kpi-card">
              <div class="kpi-label">Pymes</div>
              <div class="kpi-value">{format_pct(k['pyme_pct'])}</div>
              <div class="kpi-hint">{format_int_es(k['pyme_yes'])} contratos marcados como Pyme.</div>
            </div>
            <div class="kpi-card">
              <div class="kpi-label">Anticipos</div>
              <div class="kpi-value">{format_pct(k['advance_pct'])}</div>
              <div class="kpi-hint">{format_int_es(k['advance_yes'])} contratos con pago adelantado.</div>
            </div>
            <div class="kpi-card">
              <div class="kpi-label">Ambientales</div>
              <div class="kpi-value">{format_int_es(k['environmental_yes'])}</div>
              <div class="kpi-hint">Contratos con cláusulas ambientales explícitas.</div>
            </div>
            <div class="kpi-card">
              <div class="kpi-label">Pareto</div>
              <div class="kpi-value">{format_pct(k['pareto_share_pct'])}</div>
              <div class="kpi-hint">Concentración del 20% superior de entidades.</div>
            </div>
          </div>
        </section>

        <section class="panel">
          <div class="panel-head">
            <div>
              <p class="section-kicker">Notas</p>
              <h2>Interpretación</h2>
            </div>
          </div>
          <ul class="bullet-list">
            <li>Las variables de tipo sí/no se deberían convertir a booleanos para análisis más limpios.</li>
            <li>La pregunta de género no tiene sustento en esta base y debe reportarse como no disponible.</li>
            <li>La concentración de valor es muy alta, por lo que la revisión humana sigue siendo clave.</li>
          </ul>
        </section>
      </aside>

      <section class="content">
        <section class="panel">
          <div class="panel-head">
            <div>
              <p class="section-kicker">Variables</p>
              <h2>Anomalías de tipo de dato</h2>
            </div>
          </div>
          <div class="quality-grid">
            {type_anomalies_html}
          </div>
        </section>

        <section class="analysis-grid">
          <article class="panel">
            <div class="panel-head">
              <div>
                <p class="section-kicker">Consistencia</p>
                <h2>Campos críticos del análisis</h2>
              </div>
            </div>
            <div class="quality-grid">
              <div class="quality-card">
                <strong>es_pyme</strong>
                <span>Se usa como indicador binario de adjudicación a Pymes.</span>
              </div>
              <div class="quality-card">
                <strong>habilita_pago_adelantado</strong>
                <span>Conviene normalizarlo a booleano y no mantenerlo como texto libre.</span>
              </div>
              <div class="quality-card">
                <strong>obligaci_n_ambiental</strong>
                <span>Es un indicador binario de cumplimiento contractual ambiental.</span>
              </div>
              <div class="quality-card">
                <strong>reversion</strong>
                <span>También debería ser booleano por su semántica de regla contractual.</span>
              </div>
            </div>
          </article>

          <article class="panel">
            <div class="panel-head">
              <div>
                <p class="section-kicker">Respaldo</p>
                <h2>Por qué Q19 no se responde</h2>
              </div>
            </div>
            <div class="quality-card">
              <strong>Brecha de género</strong>
              <span>{escape(summary['questions'][18]['respuesta'])}</span>
            </div>
            <div class="quality-card" style="margin-top:12px;">
              <strong>Anomalías financieras</strong>
              <span>
                Los tres contratos más altos son atípicos por magnitud y conviene revisarlos con la fuente original o con el expediente contractual.
              </span>
            </div>
          </article>
        </section>

        <section class="panel">
          <div class="panel-head">
            <div>
              <p class="section-kicker">Top 3</p>
              <h2>Valores atípicos financieros</h2>
            </div>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>#</th>
                  <th>Entidad</th>
                  <th>Monto</th>
                  <th>Tipo</th>
                  <th>Modalidad</th>
                  <th>Veredicto</th>
                </tr>
              </thead>
              <tbody>
                {top_values_rows_html}
              </tbody>
            </table>
          </div>
        </section>

        <footer class="footer-note">
          Calidad de datos separada del EDA, como en el reto 1.
        </footer>
      </section>
    </section>
  </main>
</body>
</html>
"""
    return html


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    summary = load_summary()
    EDA_PATH.write_text(render_eda_html(summary), encoding="utf-8")
    QUALITY_PATH.write_text(render_quality_html(summary), encoding="utf-8")
    print(f"EDA: {EDA_PATH}")
    print(f"Calidad: {QUALITY_PATH}")


if __name__ == "__main__":
    main()
