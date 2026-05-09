const CHAT_BOOTSTRAP_URL = "/api/chat/bootstrap";
const CHAT_RESPOND_URL = "/api/chat/respond";
const PAE_SUMMARY_URL = "/api/pae/summary";
const PAE_DATASET_URL = "/api/pae/dataset";

const WELCOME_MESSAGE =
  "Hola. Soy el asistente de análisis de contratos públicos PAE. Puedo ayudarte a consultar contratos, detectar señales preliminares de opacidad, generar reportes y hacer seguimiento a contratos específicos. La información del proyecto es de Alejandro Montes. ¿Qué deseas analizar?";

export async function bootstrapChat(sessionId) {
  try {
    return await fetchJson(`${CHAT_BOOTSTRAP_URL}?session_id=${encodeURIComponent(sessionId)}`);
  } catch {
    return buildLocalBootstrap(sessionId);
  }
}

export async function sendChatMessage({ sessionId, query, limit = 10 }) {
  try {
    return await fetchJson(CHAT_RESPOND_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        session_id: sessionId,
        query,
        limit,
      }),
    });
  } catch {
    return buildOfflineResponse(sessionId, query);
  }
}

async function fetchJson(url, init = {}) {
  const response = await fetch(url, {
    cache: "no-store",
    ...init,
  });

  if (!response.ok) {
    const payload = await safeJson(response);
    throw new Error(payload?.detail || payload?.error || `Request failed with ${response.status}`);
  }

  return response.json();
}

async function safeJson(response) {
  try {
    return await response.clone().json();
  } catch {
    return null;
  }
}

async function buildLocalBootstrap(sessionId) {
  const dataset = await fetchJsonOptional(PAE_DATASET_URL);
  const summary = await fetchJsonOptional(PAE_SUMMARY_URL);
  const contracts = Array.isArray(dataset?.contracts) ? dataset.contracts : [];
  const sourceLabel = dataset?.source_label || summary?.source_label || "SECOP II preprocesado (tracker PAE)";
  const topContracts = buildTopContracts(contracts, summary?.top_k || []);
  const metrics = buildMetrics(contracts, summary);

  return {
    session_id: sessionId,
    intent: "project_overview",
    message: WELCOME_MESSAGE,
    view_type: "project_overview",
    data: {
      project_name: "Agente de IA para Detección de Opacidad en Contratos Públicos PAE",
      author: "Alejandro Montes",
      source_label: sourceLabel,
      loaded_at: dataset?.last_updated || summary?.last_updated || new Date().toISOString(),
      metrics,
      risk_distribution: buildRiskDistribution(contracts),
      top_flags: buildTopFlags(summary?.summary?.top_flags || []),
      top_contracts: topContracts,
      quick_actions: buildQuickActions(),
      methodology: [
        "Primero se consulta la base estructurada y los indicadores precalculados.",
        "Después se usan cachés e índices por contrato, proveedor, entidad y territorio.",
        "Solo si hace falta se usa búsqueda semántica o LLM para sintetizar la evidencia.",
      ],
      data_sources: [
        "Contratos PAE precalculados desde SECOP II.",
        "Scores de riesgo y red flags ya calculadas.",
        "Índices por contrato, proveedor, entidad y territorio.",
      ],
      warnings: [
        "El análisis es preliminar y depende de la calidad de los datos públicos.",
        "No se deben inventar datos cuando la evidencia no está disponible.",
        "La salida prioriza revisión humana, no conclusiones legales.",
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
    limitations:
      "El backend no respondió, así que se muestra un resumen local construido desde la caché pública disponible.",
    session_state: {
      session_id: sessionId,
      last_intent: "project_overview",
      last_view_type: "project_overview",
    },
    meta: {
      bootstrap: true,
      offline: true,
      generated_at: new Date().toISOString(),
    },
  };
}

function buildOfflineResponse(sessionId, query) {
  return {
    session_id: sessionId,
    intent: "unknown_query",
    message:
      "No pude conectar con el backend de análisis en este momento. El chat sigue disponible, pero las vistas dinámicas necesitan la API de Fase 2 para responder con datos completos.",
    view_type: "project_overview",
    data: {
      project_name: "Agente de IA para Detección de Opacidad en Contratos Públicos PAE",
      author: "Alejandro Montes",
      quick_actions: buildQuickActions(),
      warnings: [
        "La conexión al backend está temporalmente indisponible.",
        "La sesión se puede reintentar sin perder el contexto local del navegador.",
      ],
      query,
    },
    suggested_actions: [
      "Ver contratos con mayor riesgo",
      "Buscar contrato por ID",
      "Comparar proveedores",
      "Generar reporte",
    ],
    limitations:
      "La capa de respuesta profunda no está disponible temporalmente. Intenta de nuevo cuando el backend de Fase 2 esté en línea.",
    session_state: {
      session_id: sessionId,
      last_intent: "unknown_query",
      last_view_type: "project_overview",
    },
    meta: {
      bootstrap: false,
      offline: true,
      generated_at: new Date().toISOString(),
    },
  };
}

async function fetchJsonOptional(url) {
  try {
    const response = await fetch(url, { cache: "no-store" });
    if (!response.ok) {
      return null;
    }
    return response.json();
  } catch {
    return null;
  }
}

function buildMetrics(contracts, summary) {
  const metrics = summary?.summary || {};
  const totalContracts = Number(metrics.total_records || contracts.length || 0);
  const levelCounts = metrics.level_counts || buildLevelCounts(contracts);
  const averageScore = Number(metrics.average_score || average(contracts.map((item) => item.risk_score)));
  const totalRedFlags = Number(
    contracts.reduce((total, item) => total + Number(item.red_flag_count || (item.red_flags || []).length || 0), 0),
  );

  return {
    total_contracts: totalContracts,
    average_risk: Number.isFinite(averageScore) ? Number(averageScore.toFixed(2)) : 0,
    high_risk_contracts: Number(levelCounts.alto || 0) + Number(levelCounts.critico || 0),
    medium_risk_contracts: Number(levelCounts.medio || 0),
    low_risk_contracts: Number(levelCounts.bajo || 0),
    total_red_flags: totalRedFlags,
    top_entity: "Sin dato",
    top_supplier: "Sin dato",
    top_region: "Sin dato",
    total_value: Number(contracts.reduce((total, item) => total + Number(item.final_value || 0), 0)),
  };
}

function buildRiskDistribution(contracts) {
  const counts = buildLevelCounts(contracts);
  const total = Math.max(1, Object.values(counts).reduce((accumulator, value) => accumulator + Number(value || 0), 0));
  return [
    { label: "Bajo", key: "bajo", count: counts.bajo || 0, percent: ((counts.bajo || 0) / total) * 100, color: "#16a34a" },
    { label: "Medio", key: "medio", count: counts.medio || 0, percent: ((counts.medio || 0) / total) * 100, color: "#f59e0b" },
    { label: "Alto", key: "alto", count: counts.alto || 0, percent: ((counts.alto || 0) / total) * 100, color: "#ef4444" },
    {
      label: "Crítico",
      key: "critico",
      count: counts.critico || 0,
      percent: ((counts.critico || 0) / total) * 100,
      color: "#b91c1c",
    },
  ];
}

function buildLevelCounts(contracts) {
  return contracts.reduce(
    (accumulator, item) => {
      const key = normalizeRiskLevel(item.risk_level);
      accumulator[key] = (accumulator[key] || 0) + 1;
      return accumulator;
    },
    { bajo: 0, medio: 0, alto: 0, critico: 0 },
  );
}

function buildTopContracts(contracts, topK) {
  const fromContracts = [...contracts]
    .sort((left, right) => Number(right.risk_score || 0) - Number(left.risk_score || 0))
    .slice(0, 5)
    .map((item) => normalizeContract(item));

  if (fromContracts.length) {
    return fromContracts;
  }

  return (topK || []).slice(0, 5).map((item) => ({
    contract_id: item.contract_id,
    risk_score: Number(item.risk_score || 0),
    risk_level: normalizeRiskLevel(item.risk_level),
    summary_line: item.summary || item.summary_line || `Contrato ${item.contract_id}`,
    limitations: item.limitations || "",
    red_flags: (item.flags || []).map((flag) => flag.code || flag.label || "").filter(Boolean),
    red_flag_count: Array.isArray(item.flags) ? item.flags.length : 0,
  }));
}

function normalizeContract(record) {
  return {
    contract_id: record.contract_id,
    entity: record.entity || record.entity_name || "",
    supplier: record.supplier || record.supplier_name || "",
    department: record.department || record.departamento || "",
    municipality: record.municipality || record.municipio || "",
    final_value: Number(record.final_value || record.amount || 0),
    initial_value: Number(record.initial_value || 0),
    risk_score: Number(record.risk_score || 0),
    risk_level: normalizeRiskLevel(record.risk_level),
    red_flags: Array.isArray(record.red_flags) ? record.red_flags : Array.isArray(record.audit_red_flags_activadas) ? record.audit_red_flags_activadas : [],
    red_flag_count: Number(record.red_flag_count || (record.red_flags || []).length || 0),
    summary_line: record.summary_line || record.risk_summary || `Contrato ${record.contract_id}`,
    limitations: record.limitations || record.risk_limitations || "",
  };
}

function buildTopFlags(flags) {
  return (flags || []).slice(0, 10).map((item) => ({
    code: item.code || item.label || "",
    label: item.label || item.name || item.code || "",
    severity: item.severity || "Media",
    count: Number(item.count || 0),
    color: item.color || "#2dd4bf",
  }));
}

function buildQuickActions() {
  return [
    { label: "Ver contratos con mayor riesgo", query: "Muestra los contratos PAE con mayor riesgo" },
    { label: "Buscar contrato por ID", query: "Busca el contrato" },
    { label: "Comparar proveedores", query: "Compara proveedores PAE" },
    { label: "Generar reporte", query: "Genera un reporte ejecutivo" },
    { label: "Ver red flags", query: "Muestra las red flags más frecuentes" },
    { label: "Crear seguimiento", query: "Crear seguimiento del contrato más riesgoso" },
    { label: "Ver resumen del dashboard", query: "Muestra el resumen del dashboard" },
  ];
}

function normalizeRiskLevel(level) {
  const value = String(level || "").toLowerCase();
  if (value.startsWith("crit")) {
    return "critico";
  }
  if (value.startsWith("alto")) {
    return "alto";
  }
  if (value.startsWith("medio")) {
    return "medio";
  }
  return "bajo";
}

function average(values) {
  const numbers = values.map((value) => Number(value || 0)).filter((value) => Number.isFinite(value));
  if (!numbers.length) {
    return 0;
  }
  return numbers.reduce((accumulator, value) => accumulator + value, 0) / numbers.length;
}

