import { escapeHtml, normalizeText } from "./secop-api.js";

export function buildReportFilename(report, extension = "html") {
  const base = normalizeText(
    [report?.title, report?.subtitle, report?.meta?.[0]?.value].filter(Boolean).join(" "),
  )
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 88);

  return `${base || "gobia-auditor"}-informe.${extension}`;
}

export function buildMarkdownReport(report) {
  const lines = [];
  lines.push(`# ${report.title}`);
  lines.push("");
  lines.push(`**${report.subtitle}**`);
  lines.push("");
  lines.push("## Datos clave");
  lines.push("");
  for (const item of report.meta) {
    lines.push(`- **${item.label}:** ${item.value}`);
  }
  lines.push("");
  lines.push("## Resumen ejecutivo");
  lines.push("");
  lines.push(report.overview);
  lines.push("");
  lines.push(report.indicatorNote);
  lines.push("");
  lines.push(report.signalNarrative);
  lines.push("");
  lines.push("## Ficha del contrato");
  lines.push("");
  for (const item of report.selection) {
    lines.push(`- **${item.label}:** ${item.value}`);
  }
  lines.push("");
  lines.push("## Señales detectadas");
  lines.push("");
  if (report.signals.length) {
    for (const signal of report.signals) {
      lines.push(`- **${signal.label}:** ${signal.detail}`);
    }
  } else {
    lines.push("- No se detectaron señales dominantes en el caso seleccionado.");
  }
  lines.push("");
  lines.push("## Preguntas para auditor humano");
  lines.push("");
  for (const item of report.auditQuestions) {
    lines.push(`- ${item}`);
  }
  lines.push("");
  lines.push("## Metodología y contexto");
  lines.push("");
  for (const item of report.methodology) {
    lines.push(`- ${item}`);
  }
  for (const item of report.auditChecklist) {
    lines.push(`- ${item}`);
  }
  lines.push("");
  lines.push("## Lectura de Dify");
  lines.push("");
  lines.push(report.difyStatus);
  lines.push("");
  lines.push(report.difyAnswer || "La respuesta de Dify aparecerá cuando se use la API segura.");
  lines.push("");
  lines.push("## Recomendación final");
  lines.push("");
  lines.push(report.recommendation);
  lines.push("");
  lines.push(`Actualizado: ${report.meta.find((item) => item.label === "Actualizado")?.value || "--"}`);
  return `${lines.join("\n")}\n`;
}

export function buildHtmlReport(report) {
  const listItems = (items) =>
    items
      .map((item) => `<li><strong>${escapeHtml(item.label)}:</strong> ${escapeHtml(item.detail)}</li>`)
      .join("");

  const reportSelection = report.selection
    .map(
      (item) => `
        <div class="fact">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(item.value)}</strong>
        </div>
      `,
    )
    .join("");

  const reportStats = report.stats
    .map(
      (item) => `
        <div class="stat">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(item.value)}</strong>
        </div>
      `,
    )
    .join("");

  const reportMeta = report.meta
    .map(
      (item) => `
        <div class="meta-chip">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(item.value)}</strong>
        </div>
      `,
    )
    .join("");

  const auditQuestions = report.auditQuestions.map((item) => `<li>${escapeHtml(item)}</li>`).join("");
  const methodology = report.methodology.map((item) => `<li>${escapeHtml(item)}</li>`).join("");
  const auditChecklist = report.auditChecklist.map((item) => `<li>${escapeHtml(item)}</li>`).join("");

  return `<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${escapeHtml(report.title)}</title>
    <style>
      :root { color-scheme: light; }
      body {
        margin: 0;
        font-family: Inter, Arial, Helvetica, sans-serif;
        color: #10212b;
        background: #f5f8fb;
      }
      .page {
        max-width: 1120px;
        margin: 0 auto;
        padding: 28px;
      }
      .hero {
        display: grid;
        gap: 16px;
        padding: 24px;
        border: 1px solid #d8e1e8;
        border-radius: 20px;
        background: #ffffff;
      }
      .eyebrow {
        font-size: 12px;
        font-weight: 800;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: #0f766e;
      }
      h1, h2, h3, p { margin: 0; }
      h1 { font-size: 30px; line-height: 1.1; }
      .subtitle { color: #4d6575; line-height: 1.6; }
      .meta-grid {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 10px;
      }
      .meta-chip, .stat, .fact, .section, .callout {
        border: 1px solid #d8e1e8;
        border-radius: 16px;
        background: #ffffff;
      }
      .meta-chip, .stat, .fact {
        padding: 12px 14px;
      }
      .meta-chip span, .stat span, .fact span {
        display: block;
        margin-bottom: 4px;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #5f7483;
      }
      .meta-chip strong, .stat strong, .fact strong {
        display: block;
        font-size: 14px;
        line-height: 1.4;
      }
      .stats-grid {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 10px;
      }
      .content {
        display: grid;
        gap: 14px;
        margin-top: 14px;
      }
      .section {
        padding: 18px;
        display: grid;
        gap: 12px;
      }
      .section h2 {
        font-size: 18px;
      }
      .section p, .section li {
        color: #4d6575;
        line-height: 1.7;
      }
      .facts-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 10px;
      }
      ul {
        margin: 0;
        padding-left: 20px;
        display: grid;
        gap: 8px;
      }
      .callout {
        padding: 16px;
        background: #eefbf8;
        border-color: #b7ebe2;
      }
      .callout strong { display: block; margin-bottom: 6px; }
      .callout p { color: #175e53; }
      .report-wide {
        grid-column: 1 / -1;
      }
      .footer {
        margin-top: 18px;
        color: #5f7483;
        font-size: 12px;
      }
      @media print {
        body { background: #fff; }
        .page { padding: 0; max-width: none; }
      }
    </style>
  </head>
  <body>
    <main class="page">
      <section class="hero">
        <div>
          <div class="eyebrow">GobIA Auditor | Informe ejecutivo</div>
          <h1>${escapeHtml(report.title)}</h1>
          <p class="subtitle">${escapeHtml(report.subtitle)}</p>
        </div>
        <div class="meta-grid">
          ${reportMeta}
        </div>
      </section>

      <section class="content">
        <div class="stats-grid">
          ${reportStats}
        </div>

        <article class="section report-wide">
          <h2>Resumen ejecutivo</h2>
          <p>${escapeHtml(report.overview)}</p>
          <p>${escapeHtml(report.indicatorNote)}</p>
          <p>${escapeHtml(report.signalNarrative)}</p>
        </article>

        <article class="section">
          <h2>Ficha del contrato</h2>
          <div class="facts-grid">${reportSelection}</div>
        </article>

        <article class="section">
          <h2>Señales detectadas</h2>
          <ul>${report.signals.length ? listItems(report.signals) : "<li>No se detectaron señales dominantes en el caso seleccionado.</li>"}</ul>
        </article>

        <article class="section">
          <h2>Preguntas para auditor humano</h2>
          <ul>${auditQuestions}</ul>
        </article>

        <article class="section">
          <h2>Metodología y contexto</h2>
          <ul>${methodology}${auditChecklist}</ul>
          <div class="callout">
            <strong>Señal dominante</strong>
            <p>${escapeHtml(report.topSignal)}</p>
          </div>
        </article>

        <article class="section">
          <h2>Lectura de Dify</h2>
          <p>${escapeHtml(report.difyStatus)}</p>
          <p>${escapeHtml(report.difyAnswer || "La respuesta de Dify aparecerá cuando se use la API segura.")}</p>
          <div class="callout">
            <strong>Conversación</strong>
            <p>${escapeHtml(report.meta.find((item) => item.label === "Conversacion")?.value || "nueva")}</p>
          </div>
        </article>

        <article class="section report-wide callout">
          <strong>Recomendación final</strong>
          <p>${escapeHtml(report.recommendation)}</p>
          <p>${escapeHtml(`Actualizado: ${report.meta.find((item) => item.label === "Actualizado")?.value || "--"}`)}</p>
        </article>
      </section>

      <p class="footer">GobIA Auditor - Fase 2. Documento generado localmente para revisión y PDF.</p>
    </main>
  </body>
</html>`;
}

export function downloadTextFile(filename, content, mimeType = "text/plain;charset=utf-8") {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.rel = "noreferrer";
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}
