import { buildPromptPreview, buildSelectionContext } from "./report.js";

const DEFAULT_API_ENDPOINT = "/api/dify/chat";
const DEFAULT_API_USER = "gobia-auditor-local";

export function buildDifyApiRequest(record, analysis, indicatorLabel, sourceLabel, config = {}, conversationId = "") {
  const request = {
    query:
      config.apiQuery ||
      "Analiza el contrato seleccionado y responde con la estructura de GobIA Auditor.",
    user: config.apiUserId || DEFAULT_API_USER,
    response_mode: config.apiResponseMode || "blocking",
    inputs: {
      source_label: sourceLabel,
      indicator_label: indicatorLabel,
      contract_context: JSON.stringify(buildSelectionContext(record), null, 2),
      analysis_context: JSON.stringify(
        {
          source: sourceLabel,
          indicator: indicatorLabel,
          contractId: record.id,
          score: record.score,
          level: record.level,
          signals: analysis.signals.map((signal) => ({
            id: signal.id,
            label: signal.label,
            detail: signal.detail,
          })),
          auditQuestions: analysis.auditQuestions,
          summary: analysis.summary,
        },
        null,
        2,
      ),
      methodology_context: JSON.stringify(
        {
          purpose: "Explicacion ciudadana de posible opacidad en contratos publicos",
          warnings: [
            "No acusar corrupcion.",
            "No afirmar delitos.",
            "Usar lenguaje de priorizacion para revision humana.",
          ],
          tone: "claro, prudente y verificable",
        },
        null,
        2,
      ),
      prompt_preview: JSON.stringify(buildPromptPreview(record, analysis, indicatorLabel), null, 2),
    },
  };

  if (conversationId) {
    request.conversation_id = conversationId;
  }

  return {
    endpoint: config.apiProxyPath || DEFAULT_API_ENDPOINT,
    request,
  };
}

export async function sendDifyApiRequest(endpoint, request) {
  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify(request),
  });

  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json")
    ? await response.json()
    : { raw: await response.text() };

  if (!response.ok) {
    const message = payload?.error || payload?.message || `HTTP ${response.status}`;
    throw new Error(message);
  }

  return payload;
}

export function formatDifyApiResponse(payload = {}) {
  return {
    answer:
      payload.answer ||
      payload?.data?.answer ||
      payload?.outputs?.answer ||
      payload?.outputs?.result ||
      payload?.result ||
      "",
    conversationId: payload.conversation_id || payload.conversationId || "",
    taskId: payload.task_id || payload.taskId || "",
    messageId: payload.message_id || payload.messageId || "",
    metadata: payload.metadata || null,
    raw: payload,
  };
}

export function buildDifyApiStatusText(config, conversationId = "") {
  const endpoint = config?.apiProxyPath || DEFAULT_API_ENDPOINT;
  const hasConversation = Boolean(conversationId);
  const mode = config?.enabled && config?.webAppUrl ? "Chatflow embebido activo" : "Chatflow listo para conectar";
  const conversationText = hasConversation ? `Conversacion: ${conversationId}` : "Conversacion nueva";
  return `${mode}. API segura en ${endpoint}. ${conversationText}.`;
}
