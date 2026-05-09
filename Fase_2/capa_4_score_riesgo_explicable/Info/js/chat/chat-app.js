import { bootstrapChat, sendChatMessage } from "./api.js";
import { getViewTitle, renderLoadingView, renderMainView } from "./views.js";
import { downloadTextFile, escapeHtml, formatDateTime } from "../utils.js";

const STORAGE_KEY_SESSION = "pae.chat.session_id.v1";

const DEFAULT_PLACEHOLDER =
  "Pregunta por contratos, reportes, seguimiento, red flags o comparaciones entre proveedores y entidades...";

const SUGGESTED_QUERY_MAP = {
  "Ver contratos con mayor riesgo": "Muestra los contratos PAE con mayor riesgo",
  "Buscar contrato por ID": "Busca el contrato",
  "Comparar proveedores": "Compara proveedores PAE",
  "Generar reporte": "Genera un reporte ejecutivo",
  "Ver red flags": "Muestra las red flags más frecuentes",
  "Crear seguimiento": "Crear seguimiento del contrato más riesgoso",
  "Ver resumen del dashboard": "Muestra el resumen del dashboard",
  "Generar versión técnica": "Genera un reporte técnico",
  "Volver al contrato": "Analiza el contrato",
};

const state = {
  sessionId: loadSessionId(),
  messages: [],
  currentResponse: null,
  busy: false,
  placeholder: DEFAULT_PLACEHOLDER,
};

const dom = {};

document.addEventListener("DOMContentLoaded", bootstrap);

async function bootstrap() {
  cacheDom();
  bindEvents();
  renderShellState();
  setLoading(true);
  try {
    const response = await bootstrapChat(state.sessionId);
    applyResponse(response, { resetTranscript: true });
  } catch (error) {
    console.error("Chat bootstrap failed:", error);
    applyResponse(buildBootstrapFallbackResponse(state.sessionId), { resetTranscript: true });
  } finally {
    setLoading(false);
  }
}

function cacheDom() {
  const ids = [
    "chatForm",
    "chatInput",
    "chatSendBtn",
    "chatTranscript",
    "viewHost",
    "viewTitle",
    "viewSubtitle",
    "viewMeta",
    "statusSource",
    "statusIntent",
    "statusView",
    "statusSession",
    "statusUpdated",
    "composerHelper",
  ];

  for (const id of ids) {
    dom[id] = document.getElementById(id);
  }
}

function bindEvents() {
  dom.chatForm?.addEventListener("submit", handleSubmit);
  dom.viewHost?.addEventListener("click", handleViewAction);
  document.body.addEventListener("click", handleGlobalAction);
  dom.chatInput?.addEventListener("input", autoResizeComposer);
  dom.chatInput?.addEventListener("keydown", handleComposerKeydown);
}

async function handleSubmit(event) {
  event.preventDefault();
  const query = dom.chatInput?.value.trim() || "";
  if (!query || state.busy) {
    return;
  }

  await sendQuery(query);
}

function handleComposerKeydown(event) {
  if (event.key !== "Enter" || event.shiftKey) {
    return;
  }

  event.preventDefault();
  dom.chatForm?.requestSubmit();
}

async function sendQuery(query) {
  if (state.busy) {
    return;
  }

  pushMessage({
    role: "user",
    text: query,
    meta: {
      role: "Usuario",
    },
  });

  dom.chatInput.value = "";
  autoResizeComposer();
  setLoading(true);

  try {
    const response = await sendChatMessage({
      sessionId: state.sessionId,
      query,
      limit: 10,
    });

    applyResponse(response, { appendAssistantMessage: true });
  } catch (error) {
    console.error("Chat request failed:", error);
    applyResponse(buildQueryFallbackResponse(query, state.sessionId), { appendAssistantMessage: true });
  } finally {
    setLoading(false);
    dom.chatInput?.focus();
  }
}

function applyResponse(response, { resetTranscript = false, appendAssistantMessage = false } = {}) {
  state.currentResponse = response;
  state.sessionId = response.session_id || state.sessionId;
  persistSessionId(state.sessionId);

  if (resetTranscript) {
    state.messages = [];
  }

  if (appendAssistantMessage || resetTranscript) {
    pushMessage({
      role: "assistant",
      text: response.message || "Sin respuesta disponible.",
      meta: {
        intent: response.intent || "unknown_query",
        view_type: response.view_type || "project_overview",
        confidence: response.meta?.confidence,
        analysis_mode: response.meta?.analysis_mode || response.meta?.depth,
        generated_at: response.meta?.generated_at || new Date().toISOString(),
      },
    });
  }

  renderTranscript();
  renderView(response);
  updateStatus(response);
}

function renderShellState() {
  if (dom.viewHost) {
    dom.viewHost.innerHTML = renderLoadingView("Esperando la primera respuesta...");
  }
  if (dom.viewTitle) {
    dom.viewTitle.textContent = "Resumen del proyecto";
  }
  if (dom.viewSubtitle) {
    dom.viewSubtitle.textContent = "La vista principal se actualizará automáticamente según la intención del usuario.";
  }
}

function renderTranscript() {
  if (!dom.chatTranscript) {
    return;
  }

  if (!state.messages.length) {
    dom.chatTranscript.innerHTML = `
      <div class="empty-state compact">
        <strong>Bienvenido al chat PAE.</strong>
        <p>Escribe una pregunta para comenzar. Puedes buscar contratos, comparar proveedores o pedir reportes.</p>
      </div>
    `;
    return;
  }

  dom.chatTranscript.innerHTML = state.messages.map(renderMessage).join("");
  dom.chatTranscript.scrollTop = dom.chatTranscript.scrollHeight;
}

function renderMessage(message) {
  const isUser = message.role === "user";
  const metaItems = [];
  if (message.meta?.role) {
    metaItems.push(message.meta.role);
  }
  if (message.meta?.intent) {
    metaItems.push(`Intento: ${message.meta.intent}`);
  }
  if (message.meta?.view_type) {
    metaItems.push(`Vista: ${message.meta.view_type}`);
  }
  if (message.meta?.analysis_mode) {
    metaItems.push(`Modo: ${message.meta.analysis_mode}`);
  }
  if (message.meta?.confidence !== undefined && message.role === "assistant") {
    metaItems.push(`Confianza: ${Math.round(Number(message.meta.confidence || 0) * 100)}%`);
  }
  if (message.meta?.generated_at) {
    metaItems.push(formatDateTime(message.meta.generated_at));
  }

  return `
    <article class="chat-message ${isUser ? "chat-message--user" : "chat-message--assistant"}">
      <div class="chat-message__bubble">${escapeHtml(message.text || "")}</div>
      <div class="chat-message__meta">
        ${metaItems.map((item) => `<span class="chat-tag">${escapeHtml(item)}</span>`).join("")}
      </div>
    </article>
  `;
}

function pushMessage(message) {
  state.messages.push({
    text: String(message.text || ""),
    role: message.role || "assistant",
    meta: message.meta || {},
  });
}

function renderView(response) {
  if (!dom.viewHost) {
    return;
  }

  dom.viewHost.innerHTML = renderMainView(response);
  if (dom.viewTitle) {
    dom.viewTitle.textContent = getViewTitle(response.view_type, response);
  }
  if (dom.viewSubtitle) {
    dom.viewSubtitle.textContent = response.message || "";
  }
  if (dom.viewMeta) {
    dom.viewMeta.innerHTML = buildViewMeta(response);
  }
}

function buildViewMeta(response) {
  const meta = response.meta || {};
  const pills = [
    response.intent || "unknown_query",
    response.view_type || "project_overview",
    meta.depth ? `depth: ${meta.depth}` : "",
    meta.analysis_mode ? `LLM: ${meta.analysis_mode}` : "",
    meta.confidence !== undefined ? `confianza ${Math.round(Number(meta.confidence || 0) * 100)}%` : "",
  ].filter(Boolean);

  if (!pills.length) {
    return '<span class="status-pill">Vista lista</span>';
  }

  return pills.map((item) => `<span class="status-pill">${escapeHtml(item)}</span>`).join("");
}

function updateStatus(response) {
  const data = response.data || {};
  const sourceLabel = data.source_label || "SECOP II preprocesado (tracker PAE)";
  if (dom.statusSource) {
    dom.statusSource.textContent = `Fuente: ${sourceLabel}`;
  }
  if (dom.statusIntent) {
    dom.statusIntent.textContent = `Intento: ${response.intent || "unknown_query"}`;
  }
  if (dom.statusView) {
    dom.statusView.textContent = `Vista: ${response.view_type || "project_overview"}`;
  }
  if (dom.statusSession) {
    dom.statusSession.textContent = `Sesión: ${shortSessionId(state.sessionId)}`;
  }
  if (dom.statusUpdated) {
    dom.statusUpdated.textContent = `Actualizado: ${formatDateTime(response.meta?.generated_at || new Date().toISOString())}`;
  }
}

function setLoading(isBusy) {
  state.busy = isBusy;
  if (dom.chatSendBtn) {
    dom.chatSendBtn.disabled = isBusy;
    dom.chatSendBtn.textContent = isBusy ? "Analizando..." : "Enviar";
  }
  if (dom.chatTranscript) {
    dom.chatTranscript.setAttribute("aria-busy", String(isBusy));
    if (isBusy) {
      dom.chatTranscript.insertAdjacentHTML("beforeend", renderTypingState());
      dom.chatTranscript.scrollTop = dom.chatTranscript.scrollHeight;
    } else {
      removeTypingState();
    }
  }
}

function renderTypingState() {
  return `
    <div class="chat-message chat-message--assistant" data-typing="true">
      <div class="chat-message__bubble chat-loading">
        <span class="typing-indicator" aria-hidden="true">
          <span></span>
          <span></span>
          <span></span>
        </span>
        Analizando la consulta...
      </div>
    </div>
  `;
}

function removeTypingState() {
  const typing = dom.chatTranscript?.querySelector("[data-typing='true']");
  if (typing) {
    typing.remove();
  }
}

function handleViewAction(event) {
  const button = event.target.closest("[data-chat-action]");
  if (!button) {
    return;
  }

  handleActionButton(button);
}

function handleGlobalAction(event) {
  const button = event.target.closest("[data-chat-action]");
  if (!button || button.closest("#viewHost")) {
    return;
  }

  handleActionButton(button);
}

function handleActionButton(button) {
  const action = button.dataset.chatAction;
  if (action === "send-query") {
    const query = button.dataset.chatQuery || button.textContent || "";
    if (query.trim()) {
      sendQuery(query.trim());
    }
    return;
  }

  if (action === "suggested") {
    const label = button.dataset.suggestedLabel || button.textContent || "";
    const query = resolveSuggestedQuery(label);
    if (query) {
      sendQuery(query);
    } else {
      focusComposer(`Intenta reformular: ${label}`);
    }
    return;
  }

  if (action === "focus-input") {
    focusComposer(button.dataset.focusPlaceholder || DEFAULT_PLACEHOLDER);
    return;
  }

  if (action === "export-report") {
    exportCurrentReport();
  }
}

function focusComposer(placeholder) {
  state.placeholder = placeholder || DEFAULT_PLACEHOLDER;
  if (dom.chatInput) {
    dom.chatInput.placeholder = state.placeholder;
    dom.chatInput.focus();
  }
  if (dom.composerHelper) {
    dom.composerHelper.textContent = state.placeholder;
  }
}

function resolveSuggestedQuery(label) {
  const normalized = String(label || "").trim();
  if (!normalized) {
    return "";
  }

  if (normalized === "Volver al contrato" && state.currentResponse?.session_state?.last_contract_id) {
    return `Analiza el contrato ${state.currentResponse.session_state.last_contract_id}`;
  }

  if (normalized === "Crear seguimiento" && state.currentResponse?.session_state?.last_contract_id) {
    return `Crear seguimiento del contrato ${state.currentResponse.session_state.last_contract_id}`;
  }

  if (normalized === "Generar versión técnica" && state.currentResponse?.session_state?.last_contract_id) {
    return `Genera un reporte técnico del contrato ${state.currentResponse.session_state.last_contract_id}`;
  }

  return SUGGESTED_QUERY_MAP[normalized] || "";
}

function exportCurrentReport() {
  const response = state.currentResponse;
  const markdown = response?.data?.export?.markdown;
  if (!markdown) {
    focusComposer("No hay un reporte exportable en la vista actual.");
    return;
  }

  const filename = response?.data?.export?.filename || "reporte_pae.md";
  downloadTextFile(filename, markdown, "text/markdown;charset=utf-8");
}

function autoResizeComposer() {
  if (!dom.chatInput) {
    return;
  }

  dom.chatInput.style.height = "auto";
  dom.chatInput.style.height = `${Math.min(dom.chatInput.scrollHeight, 180)}px`;
}

function loadSessionId() {
  try {
    const stored = window.localStorage.getItem(STORAGE_KEY_SESSION);
    if (stored) {
      return stored;
    }
    const generated = createSessionId();
    window.localStorage.setItem(STORAGE_KEY_SESSION, generated);
    return generated;
  } catch {
    return createSessionId();
  }
}

function persistSessionId(sessionId) {
  try {
    window.localStorage.setItem(STORAGE_KEY_SESSION, sessionId);
  } catch {
    // No-op.
  }
}

function createSessionId() {
  if (window.crypto?.randomUUID) {
    return `chat-${window.crypto.randomUUID()}`;
  }
  return `chat-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function shortSessionId(sessionId) {
  return String(sessionId || "default").slice(-8);
}

function buildBootstrapFallbackResponse(sessionId) {
  const generatedAt = new Date().toISOString();
  return {
    session_id: sessionId,
    intent: "project_overview",
    message:
      "Hola. Soy el asistente de analisis de contratos publicos PAE. Puedo ayudarte a consultar contratos, detectar senales preliminares de opacidad, generar reportes y hacer seguimiento a contratos especificos. La informacion del proyecto es de Alejandro Montes. Que deseas analizar?",
    view_type: "project_overview",
    data: {
      project_name: "Agente de IA para Deteccion de Opacidad en Contratos Publicos PAE",
      author: "Alejandro Montes",
      source_label: "SECOP II preprocesado (tracker PAE)",
      loaded_at: generatedAt,
      metrics: {
        total_contracts: 0,
        average_risk: 0,
        high_risk_contracts: 0,
        medium_risk_contracts: 0,
        low_risk_contracts: 0,
        total_red_flags: 0,
        total_value: 0,
      },
      risk_distribution: [],
      top_flags: [],
      top_contracts: [],
      quick_actions: [
        { label: "Ver contratos con mayor riesgo", query: "Muestra los contratos PAE con mayor riesgo" },
        { label: "Buscar contrato por ID", query: "Busca el contrato" },
        { label: "Comparar proveedores", query: "Compara proveedores PAE" },
        { label: "Generar reporte", query: "Genera un reporte ejecutivo" },
        { label: "Ver red flags", query: "Muestra las red flags mas frecuentes" },
        { label: "Crear seguimiento", query: "Crear seguimiento del contrato mas riesgoso" },
        { label: "Ver resumen del dashboard", query: "Muestra el resumen del dashboard" },
      ],
      methodology: [
        "Primero se consulta la base estructurada y los indicadores precalculados.",
        "Despues se usan caches e indices por contrato, proveedor, entidad y territorio.",
        "Solo si hace falta se usa busqueda semantica o LLM para sintetizar la evidencia.",
      ],
      data_sources: [
        "Contratos PAE precalculados desde SECOP II.",
        "Scores de riesgo y red flags ya calculadas.",
        "Indices por contrato, proveedor, entidad y territorio.",
      ],
      warnings: [
        "El analisis es preliminar y depende de la calidad de los datos publicos.",
        "No se deben inventar datos cuando la evidencia no esta disponible.",
        "La salida prioriza revision humana, no conclusiones legales.",
      ],
    },
    suggested_actions: [
      "Ver contratos con mayor riesgo",
      "Buscar contrato por ID",
      "Comparar proveedores",
      "Generar reporte",
      "Ver red flags",
      "Crear seguimiento",
      "Ver resumen del dashboard",
    ],
    limitations: "La API no respondio, asi que se muestra un resumen local construido desde la cache publica disponible.",
    session_state: {
      session_id: sessionId,
      last_intent: "project_overview",
      last_view_type: "project_overview",
    },
    meta: {
      bootstrap: true,
      offline: true,
      generated_at: generatedAt,
    },
  };
}

function buildQueryFallbackResponse(query, sessionId) {
  const generatedAt = new Date().toISOString();
  return {
    session_id: sessionId,
    intent: "unknown_query",
    message:
      "No pude conectar con el backend de analisis en este momento. El chat sigue disponible, pero las vistas dinamicas necesitan la API de Fase 2 para responder con datos completos.",
    view_type: "project_overview",
    data: {
      project_name: "Agente de IA para Deteccion de Opacidad en Contratos Publicos PAE",
      author: "Alejandro Montes",
      query,
      quick_actions: [
        { label: "Ver contratos con mayor riesgo", query: "Muestra los contratos PAE con mayor riesgo" },
        { label: "Buscar contrato por ID", query: "Busca el contrato" },
        { label: "Comparar proveedores", query: "Compara proveedores PAE" },
        { label: "Generar reporte", query: "Genera un reporte ejecutivo" },
      ],
      warnings: [
        "La conexion al backend esta temporalmente indisponible.",
        "La sesion se puede reintentar sin perder el contexto local del navegador.",
      ],
    },
    suggested_actions: [
      "Ver contratos con mayor riesgo",
      "Buscar contrato por ID",
      "Comparar proveedores",
      "Generar reporte",
    ],
    limitations: "La capa de respuesta profunda no esta disponible temporalmente. Intenta de nuevo cuando el backend de Fase 2 este en linea.",
    session_state: {
      session_id: sessionId,
      last_intent: "unknown_query",
      last_view_type: "project_overview",
    },
    meta: {
      bootstrap: false,
      offline: true,
      generated_at: generatedAt,
    },
  };
}
