import { escapeHtml, truncateText } from "./secop-api.js";
import { buildPromptPreview, buildSelectionContext } from "./report.js";

export function buildIframeSnippet(config = {}) {
  const url = normalizeEmbedUrl(config);
  const height = Number(config?.iframeHeight) || 760;
  const title = config?.title || "GobIA Auditor";

  return `<iframe src="${url}" title="${title}" width="100%" height="${height}" frameborder="0"></iframe>`;
}

export function renderDifyWidget(container, config, record, analysis, indicatorLabel) {
  if (!container) {
    return;
  }

  const contextPreview = buildPromptPreview(record, analysis, indicatorLabel);
  const selection = buildSelectionContext(record);

  if (config?.enabled && config?.webAppUrl) {
    container.innerHTML = `
      <div class="dify-frame-shell">
        <div class="dify-frame-head">
          <div class="panel-tag">Dify Chatflow embebido</div>
          <a class="dify-frame-link" href="${escapeHtml(config.webAppUrl)}" target="_blank" rel="noreferrer">Abrir en nueva pestaña</a>
        </div>
        <iframe
          class="dify-frame"
          title="${escapeHtml(config.title || "GobIA Auditor")}"
          src="${escapeHtml(config.webAppUrl)}"
          loading="lazy"
          referrerpolicy="no-referrer"
          style="min-height: ${Number(config.iframeHeight) || 760}px;"
        ></iframe>
      </div>
    `;
    return;
  }

  container.innerHTML = `
    <div class="dify-placeholder">
      <div class="panel-tag">Dify Chatflow listo para conectar</div>
      <h3>${escapeHtml(config?.title || "GobIA Auditor")}</h3>
      <p>${escapeHtml(config?.intro || "Explicacion automatica del caso seleccionado.")}</p>
      <div class="dify-callout">
        <strong>Proximo paso:</strong> pega el URL publico del chatflow y este panel se convierte en un iframe vivo.
      </div>
      <div class="dify-context-grid">
        ${selection
          .map(
            (item) => `
              <div class="dify-context-item">
                <span>${escapeHtml(item.label)}</span>
                <strong>${escapeHtml(item.value)}</strong>
              </div>
            `,
          )
          .join("")}
      </div>
      <div class="dify-context-snippet">
        <span>Contexto para el agente</span>
        <pre>${escapeHtml(JSON.stringify(contextPreview, null, 2))}</pre>
      </div>
      <div class="dify-snippet">
        <span>Snippet de iframe</span>
        <pre>${escapeHtml(buildIframeSnippet(config))}</pre>
      </div>
      <p class="dify-note">${escapeHtml(
        config?.cta ||
          "Cuando el chatflow este publicado, la siguiente iteracion solo cambiara esta configuracion, no toda la pagina.",
      )}</p>
    </div>
  `;
}

export function buildConversationStarters(record, analysis) {
  return [
    `Explica por que ${record.id} tiene nivel ${record.level.toUpperCase()}.`,
    `Que documentos deberia revisar un auditor en ${record.entity || "esta entidad"}?`,
    `Resume las señales mas importantes sin acusar corrupcion.`,
    `Que preguntas faltan para validar este contrato?`,
  ].slice(0, Math.max(2, Math.min(4, analysis?.signals?.length + 1)));
}

function normalizeEmbedUrl(config) {
  const rawUrl = String(config?.webAppUrl || "").trim();
  if (rawUrl) {
    return rawUrl;
  }

  return "https://udify.app/chatbot/YOUR_APP_TOKEN";
}
