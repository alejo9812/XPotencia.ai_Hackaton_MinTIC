import http from "node:http";
import { readFile, stat } from "node:fs/promises";
import { extname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT_DIR = resolve(fileURLToPath(new URL(".", import.meta.url)));
const PAE_TRACKER_DIR = resolve(ROOT_DIR, "..", "pae_risk_tracker");
const PAE_OUTPUTS_DIR = resolve(PAE_TRACKER_DIR, "data", "outputs");
const PAE_CONFIG_PATH = resolve(ROOT_DIR, "data", "dashboard_config.json");
const PAE_RANKING_PATH = resolve(PAE_OUTPUTS_DIR, "pae_risk_ranking.csv");
const PAE_RANKING_JSON_PATH = resolve(PAE_OUTPUTS_DIR, "pae_risk_ranking.json");
const PAE_SUMMARY_PATH = resolve(PAE_OUTPUTS_DIR, "pae_risk_scores.json");
const PAE_AUDIT_CARDS_PATH = resolve(PAE_OUTPUTS_DIR, "pae_audit_cards.json");
const PAE_EXPORT_CONTRACT_PATH = resolve(PAE_TRACKER_DIR, "config", "export_contract.json");
const PAE_TRACEABILITY_PATH = resolve(PAE_TRACKER_DIR, "data", "processed", "pae_search_index.manifest.json");
const PAE_FLAGS_PATH = resolve(PAE_TRACKER_DIR, "config", "risk_flags.yml");
const PORT = Number(process.env.PORT || 4175);
const DIFY_BASE_URL = (process.env.DIFY_BASE_URL || "https://api.dify.ai").replace(/\/+$/, "");
const DIFY_API_KEY = String(process.env.DIFY_API_KEY || "").trim();
const PAE_CHAT_BACKEND_URL = (process.env.PAE_CHAT_BACKEND_URL || "http://127.0.0.1:8000").replace(/\/+$/, "");
const PAE_SOURCE_LABEL = "SECOP II preprocesado (tracker PAE)";

let cachedPaeSummaryKey = "";
let cachedPaeSummaryValue = null;
let cachedPaeDatasetKey = "";
let cachedPaeDatasetValue = null;

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
  ".ico": "image/x-icon",
  ".txt": "text/plain; charset=utf-8",
};

const server = http.createServer(async (req, res) => {
  try {
    if (!req.url) {
      respondText(res, 400, "Solicitud invalida.");
      return;
    }

    const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);

    if (req.method === "OPTIONS") {
      respondEmpty(res, 204);
      return;
    }

    if (url.pathname === "/api/health") {
      respondJson(res, 200, {
        ok: true,
        difyConfigured: Boolean(DIFY_API_KEY),
        baseUrl: DIFY_BASE_URL,
        apiRoute: "/api/dify/chat",
        chatApiRoute: "/api/chat/bootstrap",
        chatBackend: PAE_CHAT_BACKEND_URL,
      });
      return;
    }

    if (url.pathname === "/api/dify/chat") {
      await handleDifyProxy(req, res);
      return;
    }

    if (url.pathname === "/api/chat/bootstrap") {
      await handleChatBootstrap(req, url, res);
      return;
    }

    if (url.pathname === "/api/chat/respond") {
      await handleChatRespond(req, res);
      return;
    }

    if (url.pathname === "/api/pae/summary") {
      const payload = await loadPaeSummary();
      respondJson(res, 200, payload);
      return;
    }

    if (url.pathname === "/api/pae/dataset") {
      const payload = await loadPaeDataset();
      respondJson(res, 200, payload);
      return;
    }

    await serveStatic(url.pathname, res);
  } catch (error) {
    respondJson(res, 500, {
      ok: false,
      error: error instanceof Error ? error.message : "Error interno del servidor.",
    });
  }
});

server.listen(PORT, () => {
  console.log(`GobIA Auditor Fase 2 running at http://localhost:${PORT}`);
});

async function handleDifyProxy(req, res) {
  if (req.method !== "POST") {
    respondJson(res, 405, {
      ok: false,
      error: "Solo se permite POST en /api/dify/chat.",
    });
    return;
  }

  if (!DIFY_API_KEY) {
    respondJson(res, 503, {
      ok: false,
      error: "Falta configurar DIFY_API_KEY en el entorno del servidor.",
    });
    return;
  }

  const body = await readJsonBody(req);
  const upstreamResponse = await fetch(`${DIFY_BASE_URL}/v1/chat-messages`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${DIFY_API_KEY}`,
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify({
      query:
        body.query || "Analiza el contrato seleccionado y responde con la estructura de GobIA Auditor.",
      inputs: body.inputs || {},
      user: body.user || "gobia-auditor-local",
      response_mode: body.response_mode || "blocking",
      conversation_id: body.conversation_id || undefined,
      auto_generate_name: Boolean(body.auto_generate_name),
    }),
  });

  const contentType = upstreamResponse.headers.get("content-type") || "";
  const payload = contentType.includes("application/json")
    ? await upstreamResponse.json()
    : { raw: await upstreamResponse.text() };

  if (!upstreamResponse.ok) {
    respondJson(res, upstreamResponse.status, {
      ok: false,
      error: payload?.message || payload?.error || "Dify devolvio un error.",
      upstream: payload,
    });
    return;
  }

  respondJson(res, 200, {
    ok: true,
    ...payload,
  });
}

async function handleChatBootstrap(req, url, res) {
  if (req.method !== "GET") {
    respondJson(res, 405, {
      ok: false,
      error: "Solo se permite GET en /api/chat/bootstrap.",
    });
    return;
  }

  const upstreamUrl = new URL(`${PAE_CHAT_BACKEND_URL}/chat/bootstrap`);
  const sessionId = url.searchParams.get("session_id");
  if (sessionId) {
    upstreamUrl.searchParams.set("session_id", sessionId);
  }

  await proxyJsonResponse({
    upstreamUrl,
    method: "GET",
    res,
    failureMessage: "No se pudo conectar con la API de chat del tracker PAE.",
  });
}

async function handleChatRespond(req, res) {
  if (req.method !== "POST") {
    respondJson(res, 405, {
      ok: false,
      error: "Solo se permite POST en /api/chat/respond.",
    });
    return;
  }

  const body = await readJsonBody(req);
  const upstreamUrl = new URL(`${PAE_CHAT_BACKEND_URL}/chat/respond`);
  await proxyJsonResponse({
    upstreamUrl,
    method: "POST",
    body,
    res,
    failureMessage: "No se pudo conectar con la API de chat del tracker PAE.",
  });
}

async function proxyJsonResponse({ upstreamUrl, method, body, res, failureMessage }) {
  try {
    const upstreamResponse = await fetch(upstreamUrl, {
      method,
      headers: {
        Accept: "application/json",
        ...(method === "POST"
          ? {
              "Content-Type": "application/json",
            }
          : {}),
      },
      body: method === "POST" ? JSON.stringify(body || {}) : undefined,
    });

    const payload = await readMaybeJsonPayload(upstreamResponse);
    const responsePayload = isPlainObject(payload) ? payload : { raw: payload };

    if (!upstreamResponse.ok) {
      respondJson(res, upstreamResponse.status, {
        ok: false,
        ...responsePayload,
      });
      return;
    }

    respondJson(res, upstreamResponse.status, responsePayload);
  } catch (error) {
    respondJson(res, 503, {
      ok: false,
      error: failureMessage,
      detail: error instanceof Error ? error.message : "Error de red al consultar la API de chat.",
    });
  }
}

async function readMaybeJsonPayload(response) {
  const rawText = await response.text();
  if (!rawText) {
    return {};
  }

  try {
    return JSON.parse(rawText);
  } catch {
    return { raw: rawText };
  }
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

async function loadPaeSummary() {
  const cacheKey = await buildSummaryCacheKey();
  if (cachedPaeSummaryValue && cachedPaeSummaryKey === cacheKey) {
    return cachedPaeSummaryValue;
  }

  const payload = await readPaeSummary();
  cachedPaeSummaryKey = cacheKey;
  cachedPaeSummaryValue = payload;
  return payload;
}

async function loadPaeDataset() {
  const cacheKey = await buildDatasetCacheKey();
  if (cachedPaeDatasetValue && cachedPaeDatasetKey === cacheKey) {
    return cachedPaeDatasetValue;
  }

  const payload = await readPaeDataset();
  cachedPaeDatasetKey = cacheKey;
  cachedPaeDatasetValue = payload;
  return payload;
}

async function readPaeSummary() {
  const [summaryRaw, summaryStats, traceabilityRaw] = await Promise.all([
    readJsonFile(PAE_SUMMARY_PATH),
    stat(PAE_SUMMARY_PATH),
    readJsonFileIfExists(PAE_TRACEABILITY_PATH),
  ]);
  const summary = summaryRaw?.summary || {};

  return {
    ok: true,
    source: "tracker",
    source_label: PAE_SOURCE_LABEL,
    last_updated: summaryStats.mtime.toISOString(),
    row_count: Number(summaryRaw?.row_count || summary?.total_records || 0),
    summary,
    top_k: Array.isArray(summaryRaw?.top_k) ? summaryRaw.top_k : [],
    traceability: buildTraceabilityPayload(traceabilityRaw, summaryStats),
  };
}

async function readPaeDataset() {
  const [configRaw, trackerFlagsRaw, exportContractRaw, summaryRaw, rankingJsonRaw, rankingText, auditCardsRaw, summaryStats, traceabilityRaw] = await Promise.all([
    readJsonFile(PAE_CONFIG_PATH),
    readJsonFile(PAE_FLAGS_PATH),
    readJsonFileIfExists(PAE_EXPORT_CONTRACT_PATH),
    readJsonFile(PAE_SUMMARY_PATH),
    readJsonFileIfExists(PAE_RANKING_JSON_PATH),
    readFile(PAE_RANKING_PATH, "utf8"),
    readLooseJsonFile(PAE_AUDIT_CARDS_PATH),
    stat(PAE_SUMMARY_PATH),
    readJsonFileIfExists(PAE_TRACEABILITY_PATH),
  ]);

  const config = buildPaeConfig(configRaw, trackerFlagsRaw, exportContractRaw);
  const flagCatalog = buildTrackerFlagCatalog(trackerFlagsRaw);
  const auditCards = Array.isArray(auditCardsRaw) ? auditCardsRaw : [];
  const auditCardsById = new Map(
    auditCards
      .filter((card) => card && typeof card === "object")
      .map((card) => [cleanText(card.contract_id), card]),
  );
  const rankingContracts = Array.isArray(rankingJsonRaw) && rankingJsonRaw.length
    ? rankingJsonRaw
    : parseCsv(rankingText)
        .slice(1)
        .map((row, index) => {
          const headers = parseCsv(rankingText)[0] || [];
          const record = {};
          headers.forEach((header, headerIndex) => {
            record[header] = row[headerIndex] ?? "";
          });
          return record;
        });
  const contracts = rankingContracts
    .filter((record) => record && typeof record === "object" && Object.keys(record).length > 0)
    .map((record, index) => {
      const headers = Object.keys(record);
      const row = headers.map((header) => record[header]);
      const baseContract = normalizeTrackerContract(headers, row, flagCatalog, index);
      const auditCard = auditCardsById.get(baseContract.contract_id) || null;
      return mergeAuditCardData(baseContract, auditCard);
    });

  return {
    ok: true,
    source: "tracker",
    source_label: PAE_SOURCE_LABEL,
    last_updated: summaryStats.mtime.toISOString(),
    row_count: contracts.length,
    summary: summaryRaw?.summary || {},
    top_k: Array.isArray(summaryRaw?.top_k) ? summaryRaw.top_k : [],
    config,
    contracts,
    traceability: buildTraceabilityPayload(traceabilityRaw, summaryStats),
  };
}

async function readJsonFile(pathname) {
  const text = await readFile(pathname, "utf8");
  return JSON.parse(text);
}

async function readJsonFileIfExists(pathname) {
  try {
    return await readJsonFile(pathname);
  } catch (error) {
    if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") {
      return null;
    }
    throw error;
  }
}

async function readLooseJsonFile(pathname) {
  const text = await readFile(pathname, "utf8");
  return JSON.parse(normalizeLooseJsonText(text));
}

async function buildSummaryCacheKey() {
  const [summarySig, traceabilitySig] = await Promise.all([
    fileSignature(PAE_SUMMARY_PATH),
    fileSignature(PAE_TRACEABILITY_PATH),
  ]);
  return [summarySig, traceabilitySig].join("|");
}

async function buildDatasetCacheKey() {
  const [configSig, flagsSig, summarySig, rankingSig, auditSig, traceabilitySig] = await Promise.all([
    fileSignature(PAE_CONFIG_PATH),
    fileSignature(PAE_FLAGS_PATH),
    fileSignature(PAE_SUMMARY_PATH),
    fileSignature(PAE_RANKING_PATH),
    fileSignature(PAE_AUDIT_CARDS_PATH),
    fileSignature(PAE_TRACEABILITY_PATH),
  ]);
  return [configSig, flagsSig, summarySig, rankingSig, auditSig, traceabilitySig].join("|");
}

async function fileSignature(pathname) {
  try {
    const fileStats = await stat(pathname);
    return `${fileStats.mtimeMs}:${fileStats.size}`;
  } catch {
    return "missing";
  }
}

function buildTraceabilityPayload(rawTraceability, summaryStats) {
  if (!rawTraceability || typeof rawTraceability !== "object") {
    return null;
  }

  const sourceSnapshot = rawTraceability.source_snapshot && typeof rawTraceability.source_snapshot === "object"
    ? rawTraceability.source_snapshot
    : {};
  const expectedRowCount = Number(sourceSnapshot.expected_row_count ?? rawTraceability.expected_row_count ?? 0);
  const rowCount = Number(rawTraceability.row_count || 0);
  const complete = expectedRowCount > 0 && rowCount === expectedRowCount;
  const statusLabel = complete
    ? `Trazabilidad completa (${rowCount}/${expectedRowCount})`
    : expectedRowCount > 0
      ? `Trazabilidad parcial (${rowCount}/${expectedRowCount})`
      : "Trazabilidad no disponible";

  return {
    table_name: String(rawTraceability.table_name || "pae_search_index"),
    row_count: rowCount,
    expected_row_count: expectedRowCount,
    status: complete ? "complete" : expectedRowCount > 0 ? "pending_review" : "unknown",
    status_label: statusLabel,
    source_tables: Array.isArray(rawTraceability.source_tables) ? rawTraceability.source_tables : [],
    source_fingerprint: String(rawTraceability.source_fingerprint || ""),
    generated_at: String(rawTraceability.generated_at || summaryStats.mtime.toISOString()),
    source_snapshot: sourceSnapshot,
  };
}

function buildPaeConfig(rawConfig, trackerFlagsRaw, exportContractRaw) {
  const trackerCatalog = buildTrackerFlagCatalog(trackerFlagsRaw);
  const localFlags = Array.isArray(rawConfig?.red_flags) ? rawConfig.red_flags : [];
  const mergedFlags = mergeFlags(localFlags, trackerCatalog.flags);

  return {
    ...rawConfig,
    source_label: PAE_SOURCE_LABEL,
    export_contract: exportContractRaw || rawConfig?.export_contract || null,
    red_flags: mergedFlags,
  };
}

function buildTrackerFlagCatalog(rawCatalog) {
  const dimensions = Array.isArray(rawCatalog?.dimensions) ? rawCatalog.dimensions : [];
  const dimensionsById = Object.fromEntries(
    dimensions.map((dimension) => [String(dimension.id || "").trim(), String(dimension.label || dimension.id || "").trim()]),
  );
  const flags = Object.entries(rawCatalog?.flags || {})
    .map(([code, flag]) => {
      const weight = Number(flag?.weight || 0);
      const severity = weight >= 8 ? "Alta" : weight >= 6 ? "Media" : "Baja";
      const dimensionLabel = dimensionsById[String(flag?.dimension || "").trim()] || String(flag?.dimension || "general");

      return {
        code: String(code || "").trim(),
        name: String(flag?.label || code || "").trim(),
        severity,
        category: String(flag?.dimension || "general").trim(),
        description: `${String(flag?.label || code || "").trim()} · ${dimensionLabel}`.trim(),
        evidence_hint: String(flag?.label || code || "").trim(),
        color: severityColor(severity),
      };
    })
    .filter((flag) => flag.code);

  return {
    dimensions,
    flags,
    flagsByCode: Object.fromEntries(flags.map((flag) => [flag.code, flag])),
  };
}

function mergeFlags(localFlags, trackerFlags) {
  const merged = [];
  const seen = new Set();

  for (const flag of [...localFlags, ...trackerFlags]) {
    const code = String(flag?.code || "").trim();
    if (!code || seen.has(code)) {
      continue;
    }

    seen.add(code);
    merged.push({
      code,
      name: String(flag?.name || code).trim(),
      severity: String(flag?.severity || "Media").trim(),
      category: String(flag?.category || "general").trim(),
      description: String(flag?.description || flag?.label || "").trim(),
      evidence_hint: String(flag?.evidence_hint || "").trim(),
      color: String(flag?.color || severityColor(flag?.severity || "Media")).trim(),
    });
  }

  return merged;
}

function normalizeTrackerContract(headers, row, flagCatalog, index) {
  const record = {};
  headers.forEach((header, headerIndex) => {
    record[header] = row[headerIndex] ?? "";
  });

  const contractId = cleanText(record.contract_id || record.contractId || record.id || `PAE-${index + 1}`);
  const entity = cleanText(
    record.entity ||
    record.entity_name ||
    record.entidad ||
    record.nombre_entidad ||
    record.process_entity_norm,
  );
  const entityNit = cleanText(
    record.entity_nit ||
    record.nit_entidad ||
    record.entity_doc ||
    record.codigo_entidad_creadora ||
    record.codigo_entidad,
  );
  const supplier = cleanText(record.supplier || record.supplier_name || record.proveedor || record.proveedor_adjudicado);
  const supplierNit = cleanText(
    record.supplier_nit ||
    record.supplier_doc_norm ||
    record.supplier_doc ||
    record.documento_proveedor ||
    record.nit_proveedor,
  );
  const department = cleanText(record.department || record.departamento || record.department_name);
  const municipality = cleanText(record.municipality || record.municipio || record.municipality_name);
  const object = cleanText(record.object || record.object_text || record.objeto || record.descripcion_del_proceso);
  const modality = cleanText(record.modality || record.modality_text || record.modalidad || record.modalidad_de_contratacion);
  const status = deriveStatus(record);
  const initialValue = parseNumber(record.initial_value || record.valor_inicial || record.value_initial || record.estimated_amount || record.precio_base);
  const finalValue = parseNumber(record.final_value || record.valor_final || record.value_final || record.amount || record.valor_total_adjudicacion);
  const additionValue = Math.max(0, finalValue - initialValue);
  const additionPercentage = initialValue > 0 ? (additionValue / initialValue) * 100 : 0;
  const riskScore = clampNumber(parseNumber(record.risk_score || record.score), 0, 100);
  const riskLevel = normalizeRiskLevel(record.risk_level, riskScore);
  const redFlags = parseRedFlags(record.red_flags || record.red_flags_activadas || record.activated_flags || record.risk_flags);
  const scoreExplanation = cleanText(record.risk_summary || record.score_explanation || record.scoreExplanation || record.explanation);
  const manualChecks = cleanText(record.required_manual_checks || record.requiredManualChecks || record.limitations || record.risk_limitations);
  const recommendedAction = manualChecks.split(/\s*\|\s*/).find(Boolean) || "";
  const canonicalEvidence = parseJsonValue(record.evidence || record.audit_evidence || record.flag_evidence);
  const flagEvidence = canonicalEvidence && !Array.isArray(canonicalEvidence) && typeof canonicalEvidence === "object"
    ? canonicalEvidence
    : buildFlagEvidence(redFlags, scoreExplanation, flagCatalog);
  const startDate = cleanText(record.start_date || record.date || record.fecha_de_firma || record.fecha_de_inicio || record.fecha_de_publicacion_del);
  const endDate = cleanText(record.end_date || record.fecha_de_fin || record.fecha_de_terminacion);
  const year = parseNumber(record.year || record.core_year || record.process_year);
  const month = parseNumber(record.month || record.signature_month || record.contract_month);
  const limitations = cleanText(record.limitations || record.risk_limitations || record.riskLimitations || manualChecks);
  const searchBlob = normalizeTextForSearch(
    [
      contractId,
      entity,
      entityNit,
      supplier,
      supplierNit,
      department,
      municipality,
      modality,
      object,
      status,
      redFlags.join(" "),
      scoreExplanation,
      manualChecks,
      limitations,
    ]
      .filter(Boolean)
      .join(" "),
  );

  return {
    contract_id: contractId,
    secop_url: cleanText(
      record.secop_url ||
      record.url_secop ||
      record.url_process ||
      (record.urlproceso && typeof record.urlproceso === "object" ? record.urlproceso.url : record.urlproceso),
    ),
    entity,
    entity_nit: entityNit,
    supplier,
    supplier_nit: supplierNit,
    department,
    municipality,
    object,
    modality,
    status,
    initial_value: initialValue,
    final_value: finalValue,
    addition_value: additionValue,
    addition_percentage: roundNumber(additionPercentage, 2),
    start_date: startDate,
    end_date: endDate,
    risk_score: riskScore,
    risk_level: riskLevel,
    red_flags: redFlags,
    red_flag_details: redFlags.map((code) => {
      const catalog = flagCatalog.flagsByCode[code] || {};
      return {
        code,
        name: String(catalog.name || code),
        severity: String(catalog.severity || "Media"),
        category: String(catalog.category || "general"),
        description: String(catalog.description || catalog.name || code),
        evidence: String(flagEvidence[code] || catalog.evidence_hint || scoreExplanation || catalog.name || code),
        color: String(catalog.color || severityColor(catalog.severity || "Media")),
      };
    }),
    red_flag_count: redFlags.length,
    flag_evidence: flagEvidence,
    bidder_count: parseNumber(record.num_oferentes),
    offer_count: parseNumber(record.bidder_rows || record.num_oferentes),
    additions_count: parseBoolean(record.has_additions) ? 1 : 0,
    modifications_count: parseBoolean(record.has_additions) ? 1 : 0,
    process_id: cleanText(record.process_id),
    year,
    month,
    duration_days: 0,
    value_gap: finalValue - initialValue,
    score_explanation: scoreExplanation,
    required_manual_checks: manualChecks,
    recommended_action: cleanText(record.recommended_action || record.audit_recommendation || record.recomendacion || recommendedAction),
    limitations,
    risk_summary: scoreExplanation,
    risk_limitations: limitations,
    evidence: canonicalEvidence && Object.keys(canonicalEvidence).length ? canonicalEvidence : flagEvidence,
    audit_summary: {
      entity,
      entity_nit: entityNit,
      supplier,
      supplier_nit: supplierNit,
      department,
      municipality,
      object,
      modality,
      value_initial: initialValue,
      value_final: finalValue,
      num_oferentes: parseNumber(record.num_oferentes),
      duration_days: 0,
      secop_url: cleanText(
        record.secop_url ||
        record.url_secop ||
        record.url_process ||
        (record.urlproceso && typeof record.urlproceso === "object" ? record.urlproceso.url : record.urlproceso),
      ),
      process_id: cleanText(record.process_id),
    },
    audit_evidence: {
      process_id: cleanText(record.process_id),
      competition_signal: cleanText(record.competition_signal),
      num_oferentes_reported: parseNumber(record.num_oferentes),
      bidder_rows: parseNumber(record.bidder_rows || record.num_oferentes),
      unique_suppliers: parseNumber(record.unique_suppliers),
      supplier_total_contracts: parseNumber(record.supplier_total_contracts),
      supplier_share_in_entity: 0,
      value_percentile_year: 0,
      estimated_vs_awarded_ratio: null,
      url_secop: cleanText(
        record.secop_url ||
        record.url_secop ||
        record.url_process ||
        (record.urlproceso && typeof record.urlproceso === "object" ? record.urlproceso.url : record.urlproceso),
      ),
    },
    huecos_de_informacion: [],
    documentos_a_revisar: [],
    audit_recommendation: cleanText(record.audit_recommendation || record.auditRecommendation || record.recomendacion || recommendedAction),
    audit_score_explanation: scoreExplanation,
    audit_red_flags_activadas: redFlags,
    audit_dimension_scores: parseDimensionScores(record.dimension_scores),
    explanation: scoreExplanation,
    search_blob: searchBlob,
    raw: record,
  };
}

function mergeAuditCardData(contract, auditCard) {
  if (!auditCard || typeof auditCard !== "object") {
    return contract;
  }

  const summary = auditCard.summary && typeof auditCard.summary === "object" ? auditCard.summary : {};
  const evidence = auditCard.evidence && typeof auditCard.evidence === "object" ? auditCard.evidence : {};
  const huecos = Array.isArray(auditCard.huecos_de_informacion) ? auditCard.huecos_de_informacion : [];
  const documentos = Array.isArray(auditCard.documentos_a_revisar) ? auditCard.documentos_a_revisar : [];
  const redFlags = Array.isArray(auditCard.red_flags_activadas) ? auditCard.red_flags_activadas : [];
  const auditSummaryText = typeof auditCard.summary === "string" ? auditCard.summary : "";
  const auditRecommendation = cleanText(
    auditCard.recomendacion ||
    auditCard.audit_recommendation ||
    contract.audit_recommendation ||
    contract.recommended_action,
  );
  const auditScoreExplanation = cleanText(
    auditCard.score_explanation ||
    auditCard.audit_score_explanation ||
    contract.audit_score_explanation ||
    contract.risk_summary,
  );
  const auditLimitations = cleanText(auditCard.limitations || auditCard.risk_limitations || contract.limitations);
  const auditRiskLevel = normalizeRiskLevel(auditCard.risk_level || contract.risk_level, auditCard.risk_score ?? contract.risk_score);

  return {
    ...contract,
    entity: cleanText(auditCard.entity || contract.entity),
    entity_nit: cleanText(auditCard.entity_nit || contract.entity_nit),
    supplier: cleanText(auditCard.supplier || contract.supplier),
    supplier_nit: cleanText(auditCard.supplier_nit || contract.supplier_nit),
    department: cleanText(auditCard.department || contract.department),
    municipality: cleanText(auditCard.municipality || contract.municipality),
    object: cleanText(auditCard.object || contract.object),
    modality: cleanText(auditCard.modality || contract.modality),
    status: cleanText(auditCard.status || contract.status),
    initial_value: Number.isFinite(Number(auditCard.initial_value)) ? Number(auditCard.initial_value) : contract.initial_value,
    final_value: Number.isFinite(Number(auditCard.final_value)) ? Number(auditCard.final_value) : contract.final_value,
    start_date: cleanText(auditCard.start_date || contract.start_date),
    end_date: cleanText(auditCard.end_date || contract.end_date),
    year: Number.isFinite(Number(auditCard.year)) ? Number(auditCard.year) : contract.year,
    month: Number.isFinite(Number(auditCard.month)) ? Number(auditCard.month) : contract.month,
    risk_score: Number.isFinite(Number(auditCard.risk_score)) ? Number(auditCard.risk_score) : contract.risk_score,
    risk_level: auditRiskLevel,
    red_flags: redFlags.length ? redFlags : contract.red_flags,
    secop_url: cleanText(auditCard.secop_url || contract.secop_url),
    recommended_action: auditRecommendation,
    limitations: auditLimitations,
    risk_summary: cleanText(auditCard.risk_summary || auditSummaryText || contract.risk_summary),
    risk_limitations: cleanText(auditCard.risk_limitations || auditLimitations || contract.risk_limitations),
    evidence: auditCard.evidence && typeof auditCard.evidence === "object" ? auditCard.evidence : contract.evidence,
    audit_summary: {
      ...contract.audit_summary,
      ...summary,
    },
    audit_evidence: {
      ...contract.audit_evidence,
      ...evidence,
    },
    huecos_de_informacion: huecos,
    documentos_a_revisar: documentos,
    audit_recommendation: auditRecommendation,
    audit_score_explanation: auditScoreExplanation,
    audit_red_flags_activadas: redFlags.length ? redFlags : contract.audit_red_flags_activadas,
    audit_dimension_scores:
      auditCard.dimension_scores && typeof auditCard.dimension_scores === "object"
        ? auditCard.dimension_scores
        : contract.audit_dimension_scores,
  };
}

function buildFlagEvidence(redFlags, scoreExplanation, flagCatalog) {
  const signals = extractSignals(scoreExplanation);
  const evidence = {};

  for (const code of redFlags) {
    const catalog = flagCatalog.flagsByCode[code] || {};
    const label = normalizeTextForSearch(catalog.name || code);
    const match = signals.find((signal) => normalizeTextForSearch(signal).includes(label) || label.includes(normalizeTextForSearch(signal)));
    evidence[code] = match || scoreExplanation || catalog.name || code;
  }

  return evidence;
}

function extractSignals(scoreExplanation) {
  const text = String(scoreExplanation || '').trim();
  if (!text) {
    return [];
  }

  const marker = ['Se�ales:', 'Señales:'].find((candidate) => text.includes(candidate));
  if (!marker) {
    return text
      .split('|')
      .map((part) => part.trim())
      .filter(Boolean);
  }

  return text
    .slice(text.indexOf(marker) + marker.length)
    .split(';')
    .map((part) => part.trim())
    .filter(Boolean);
}

function parseCsv(text) {
  const rows = [];
  let currentRow = [];
  let currentField = "";
  let inQuotes = false;

  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    const nextChar = text[index + 1];

    if (inQuotes) {
      if (char === '"') {
        if (nextChar === '"') {
          currentField += '"';
          index += 1;
        } else {
          inQuotes = false;
        }
      } else {
        currentField += char;
      }
      continue;
    }

    if (char === '"') {
      inQuotes = true;
      continue;
    }

    if (char === ",") {
      currentRow.push(currentField);
      currentField = "";
      continue;
    }

    if (char === "\r") {
      continue;
    }

    if (char === "\n") {
      currentRow.push(currentField);
      rows.push(currentRow);
      currentRow = [];
      currentField = "";
      continue;
    }

    currentField += char;
  }

  if (currentField.length > 0 || currentRow.length > 0) {
    currentRow.push(currentField);
    rows.push(currentRow);
  }

  return rows;
}

function parseRedFlags(value) {
  const text = String(value || "").trim();
  if (!text) {
    return [];
  }

  try {
    const parsed = JSON.parse(text);
    if (Array.isArray(parsed)) {
      return parsed
        .map((item) => {
          if (item && typeof item === "object") {
            return String(item.code || item.name || item.value || "").trim();
          }
          return String(item || "").trim();
        })
        .filter(Boolean);
    }
  } catch {
    // Fallback below.
  }

  return text
    .replace(/^\[|\]$/g, "")
    .split(/[,|;]/)
    .map((item) => item.replace(/^["'\s]+|["'\s]+$/g, "").trim())
    .filter(Boolean);
}

function parseDimensionScores(value) {
  if (value && typeof value === "object") {
    return value;
  }

  const text = String(value || "").trim();
  if (!text) {
    return {};
  }

  try {
    return JSON.parse(text);
  } catch {
    return {};
  }
}

function deriveStatus(record) {
  if (parseBoolean(record.tiene_sanciones)) {
    return "Con sanciones";
  }

  if (parseBoolean(record.has_additions)) {
    return "Con adiciones";
  }

  return "Analizado";
}

function parseBoolean(value) {
  const text = String(value || "").trim().toLowerCase();
  return text === "true" || text === "1" || text === "yes";
}

function parseNumber(value) {
  const normalized = String(value ?? "").trim().replace(/\s+/g, "").replace(/,/g, ".");
  if (!normalized) {
    return 0;
  }

  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : 0;
}

function cleanText(value) {
  return String(value || "").trim();
}

function normalizeRiskLevel(value, score = null) {
  const text = String(value || "").trim().toLowerCase();
  if (text === "alto") {
    return "Alto";
  }
  if (text === "medio") {
    return "Medio";
  }
  if (text === "critico" || text === "crÃ­tico") {
    return "Critico";
  }
  const numericScore = Number(score);
  if (Number.isFinite(numericScore)) {
    if (numericScore >= 85) {
      return "Critico";
    }
    if (numericScore >= 56) {
      return "Alto";
    }
    if (numericScore >= 31) {
      return "Medio";
    }
  }
  return "Bajo";
}

function parseJsonValue(value) {
  if (value === null || value === undefined || value === "" || (Array.isArray(value) && value.length === 0)) {
    return null;
  }
  if (Array.isArray(value) || typeof value === "object") {
    return value;
  }
  if (typeof value !== "string") {
    return value;
  }
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function roundNumber(value, decimals = 2) {
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}

function clampNumber(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function severityColor(severity) {
  switch (String(severity || "").trim()) {
    case "Alta":
      return "#dc2626";
    case "Media":
      return "#f59e0b";
    default:
      return "#16a34a";
  }
}

function normalizeTextForSearch(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeLooseJsonText(text) {
  return String(text || "")
    .replace(/\b-Infinity\b/g, "null")
    .replace(/\bInfinity\b/g, "null")
    .replace(/\bNaN\b/g, "null");
}

async function serveStatic(pathname, res) {
  const cleanPath = pathname === "/" ? "/index.html" : pathname;
  const filePath = resolve(join(ROOT_DIR, `.${cleanPath}`));

  if (!filePath.startsWith(ROOT_DIR)) {
    respondText(res, 403, "Acceso denegado.");
    return;
  }

  try {
    const data = await readFile(filePath);
    const contentType = MIME_TYPES[extname(filePath)] || "application/octet-stream";
    res.writeHead(200, {
      "Content-Type": contentType,
      "Cache-Control": "no-store",
    });
    res.end(data);
  } catch {
    respondText(res, 404, "Archivo no encontrado.");
  }
}

function respondJson(res, statusCode, payload) {
  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
  });
  res.end(JSON.stringify(payload));
}

function respondText(res, statusCode, text) {
  res.writeHead(statusCode, {
    "Content-Type": "text/plain; charset=utf-8",
    "Cache-Control": "no-store",
  });
  res.end(text);
}

function respondEmpty(res, statusCode) {
  res.writeHead(statusCode, {
    "Cache-Control": "no-store",
  });
  res.end();
}

async function readJsonBody(req) {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(chunk);
  }

  const raw = Buffer.concat(chunks).toString("utf8").trim();
  if (!raw) {
    return {};
  }

  try {
    return JSON.parse(raw);
  } catch {
    return {};
  }
}
