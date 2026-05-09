import {
  clamp,
  monthKey,
  normalizeText,
  parseDate,
  toNumber,
} from "./utils.js";

const CONTRACTS_URL = new URL("../data/contracts_pae_mock.json", import.meta.url);
const CONFIG_URL = new URL("../data/dashboard_config.json", import.meta.url);
const LIVE_DATASET_PATH = "/api/pae/dataset";

let cachedDatasetPromise = null;

export async function loadDashboardDataset(forceReload = false) {
  if (forceReload) {
    cachedDatasetPromise = null;
  }

  if (!cachedDatasetPromise) {
    cachedDatasetPromise = readDashboardDataset();
  }

  return cachedDatasetPromise;
}

async function readDashboardDataset() {
  const configPromise = fetchJson(CONFIG_URL);
  const liveUrl = resolveBrowserApiUrl(LIVE_DATASET_PATH);
  const livePromise = liveUrl ? fetchJsonOptional(liveUrl) : Promise.resolve(null);
  const [configRaw, liveRaw] = await Promise.all([configPromise, livePromise]);

  if (liveRaw?.ok && Array.isArray(liveRaw.contracts) && liveRaw.contracts.length) {
    const config = normalizeConfig(liveRaw.config || configRaw);
    if (liveRaw.source_label) {
      config.source_label = String(liveRaw.source_label);
    }

    const contracts = liveRaw.contracts.map((record, index) => normalizeContract(record, config, index));

    return {
      contracts,
      config,
      summary: liveRaw.summary || null,
      traceability: liveRaw.traceability || null,
      source: String(liveRaw.source_label || config.source_label || "Datos reales preprocesados"),
      lastUpdated: liveRaw.last_updated ? new Date(liveRaw.last_updated) : new Date(),
    };
  }

  const contractsRaw = await fetchJson(CONTRACTS_URL);
  const config = normalizeConfig(configRaw);
  const contracts = contractsRaw.map((record, index) => normalizeContract(record, config, index));

  return {
    contracts,
    config,
    summary: null,
    traceability: null,
    source: config.source_label,
    lastUpdated: new Date(),
  };
}

async function fetchJson(url) {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`No se pudo cargar ${url.pathname}.`);
  }

  return response.json();
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

function resolveBrowserApiUrl(pathname) {
  if (typeof window === "undefined") {
    return null;
  }

  const origin = window.location?.origin;
  if (!origin || origin === "null") {
    return null;
  }

  return new URL(pathname, origin);
}

export function normalizeConfig(raw = {}) {
  const defaultThresholds = {
    low: [0, 39],
    medium: [40, 69],
    high: [70, 100],
  };

  const thresholds = raw.risk_thresholds || raw.thresholds || defaultThresholds;
  const redFlags = Array.isArray(raw.red_flags) ? raw.red_flags : [];
  const redFlagsByCode = Object.fromEntries(redFlags.map((flag) => [flag.code, flag]));
  const pageSize = Math.max(1, toNumber(raw.page_size ?? raw.pageSize ?? 8) || 8);

  return {
    project_name: String(raw.project_name || "Dashboard de Opacidad Contractual - Alimentacion Escolar"),
    subtitle: String(raw.subtitle || "Analisis de contratos publicos SECOP II / PAE"),
    source_label: String(raw.source_label || "Datos mock preprocesados"),
    export_contract: raw.export_contract || raw.exportContract || null,
    period_default: String(raw.period_default || "all"),
    page_size: pageSize,
    default_sort: {
      key: String(raw.default_sort?.key || "risk_score"),
      direction: raw.default_sort?.direction === "asc" ? "asc" : "desc",
    },
    risk_thresholds: {
      low: Array.isArray(thresholds.low) ? thresholds.low : defaultThresholds.low,
      medium: Array.isArray(thresholds.medium) ? thresholds.medium : defaultThresholds.medium,
      high: Array.isArray(thresholds.high) ? thresholds.high : defaultThresholds.high,
    },
    risk_colors: {
      low: String(raw.risk_colors?.low || "#16a34a"),
      medium: String(raw.risk_colors?.medium || "#f59e0b"),
      high: String(raw.risk_colors?.high || "#dc2626"),
    },
    chart_palette: Array.isArray(raw.chart_palette) && raw.chart_palette.length
      ? raw.chart_palette
      : ["#1d4ed8", "#0ea5e9", "#14b8a6", "#16a34a", "#f59e0b", "#dc2626", "#8b5cf6", "#64748b"],
    weights: {
      amountHigh: 12,
      amountVeryHigh: 20,
      shortDescription: 14,
      genericDescription: 10,
      missingEntity: 9,
      missingProvider: 9,
      missingAmount: 9,
      missingDate: 7,
      missingModality: 8,
      repeatProvider: 12,
      lowCompetition: 10,
      sensitiveModality: 8,
      invalidDate: 8,
      sanctionSignal: 15,
      ...(raw.weights || {}),
    },
    red_flags: redFlags.map((flag) => ({
      code: String(flag.code || "").trim(),
      name: String(flag.name || flag.code || "").trim(),
      severity: String(flag.severity || "Media").trim(),
      category: String(flag.category || "general").trim(),
      description: String(flag.description || "").trim(),
      evidence_hint: String(flag.evidence_hint || flag.evidence || "").trim(),
      color: String(flag.color || "").trim(),
    })),
    red_flags_by_code: redFlagsByCode,
  };
}

export function normalizeContract(raw = {}, config, index = 0) {
  const initialValue = toNumber(raw.initial_value ?? raw.initialValue);
  const finalValue = toNumber(raw.final_value ?? raw.finalValue ?? initialValue);
  const additionValue = toNumber(raw.addition_value ?? raw.additionValue);
  const additionPercentage = toNumber(raw.addition_percentage ?? raw.additionPercentage);
  const score = clamp(toNumber(raw.risk_score ?? raw.riskScore), 0, 100);
  const startDate = String(raw.start_date || raw.startDate || "").trim();
  const endDate = String(raw.end_date || raw.endDate || "").trim();
  const scoreExplanation = String(raw.score_explanation || raw.scoreExplanation || "").trim();
  const auditScoreExplanation = String(raw.audit_score_explanation || raw.auditScoreExplanation || "").trim();
  const riskSummary = String(raw.risk_summary || raw.riskSummary || raw.explanation || raw.analysis_summary || scoreExplanation || "").trim();
  const requiredManualChecks = String(raw.required_manual_checks || raw.requiredManualChecks || "").trim();
  const recommendedAction = String(
    raw.recommended_action ||
    raw.recommendedAction ||
      raw.recomendacion ||
      raw.audit_recommendation ||
      requiredManualChecks.split(/\s*\|\s*/).find(Boolean) ||
      "",
  ).trim();
  const riskLimitations = String(raw.risk_limitations || raw.riskLimitations || raw.limitations || requiredManualChecks || "").trim();
  const flagEvidence = raw.flag_evidence && typeof raw.flag_evidence === "object"
    ? raw.flag_evidence
    : raw.evidence && typeof raw.evidence === "object"
      ? raw.evidence
      : {};
  const rawFlags = Array.isArray(raw.red_flags) ? raw.red_flags : Array.isArray(raw.red_flags_activadas) ? raw.red_flags_activadas : [];
  const auditRedFlags = normalizeStringArray(raw.audit_red_flags_activadas ?? raw.auditRedFlagsActivadas ?? raw.red_flags_activadas ?? raw.red_flags);
  const auditHuecos = normalizeStringArray(raw.huecos_de_informacion ?? raw.huecosDeInformacion);
  const auditDocumentos = normalizeStringArray(raw.documentos_a_revisar ?? raw.documentosARevisar);
  const auditSummary = raw.audit_summary && typeof raw.audit_summary === "object"
    ? raw.audit_summary
    : raw.summary && typeof raw.summary === "object"
      ? raw.summary
      : null;
  const auditEvidence = raw.audit_evidence && typeof raw.audit_evidence === "object"
    ? raw.audit_evidence
    : raw.evidence && typeof raw.evidence === "object"
      ? raw.evidence
      : null;
  const auditDimensionScores = raw.audit_dimension_scores && typeof raw.audit_dimension_scores === "object"
    ? raw.audit_dimension_scores
    : null;
  const redFlagCodes = rawFlags
    .map((item) => (typeof item === "string" ? item : item?.code))
    .filter(Boolean);

  const redFlagDetails = redFlagCodes.map((code) => {
    const catalog = config.red_flags_by_code?.[code] || config.red_flags?.find((flag) => flag.code === code) || {};
    const evidence =
      flagEvidence[code] ||
      rawFlags.find((item) => typeof item === "object" && item?.code === code)?.evidence ||
      catalog.evidence_hint ||
      "";

    return {
      code,
      name: String(catalog.name || code),
      severity: String(catalog.severity || "Media"),
      category: String(catalog.category || "general"),
      description: String(catalog.description || ""),
      evidence: String(evidence || ""),
      color: String(catalog.color || ""),
    };
  });

  const year = startDate ? parseDate(startDate)?.getFullYear() ?? null : null;
  const month = startDate ? monthKey(startDate) : "";
  const durationDays = calculateDurationDays(startDate, endDate);
  const level = normalizeRiskLevelLabel(raw.risk_level, score, config.risk_thresholds);
  const searchableText = normalizeText(
    [
      raw.contract_id || raw.id || `PAE-${index + 1}`,
      raw.entity,
      raw.entity_nit,
      raw.supplier,
      raw.supplier_nit,
      raw.department,
      raw.municipality,
      raw.modality,
      raw.status,
      raw.object,
      redFlagDetails.map((flag) => flag.name).join(" "),
      redFlagDetails.map((flag) => flag.description).join(" "),
      redFlagDetails.map((flag) => flag.evidence).join(" "),
      riskSummary,
      riskLimitations,
    ]
      .filter(Boolean)
      .join(" "),
  );

  return {
    contract_id: String(raw.contract_id || raw.id || `PAE-${index + 1}`),
    secop_url: String(raw.secop_url || ""),
    entity: String(raw.entity || "").trim(),
    entity_nit: String(raw.entity_nit || raw.entityNit || "").trim(),
    supplier: String(raw.supplier || "").trim(),
    supplier_nit: String(raw.supplier_nit || "").trim(),
    department: String(raw.department || "").trim(),
    municipality: String(raw.municipality || "").trim(),
    object: String(raw.object || "").trim(),
    modality: String(raw.modality || "").trim(),
    status: String(raw.status || "").trim(),
    initial_value: initialValue,
    final_value: finalValue,
    addition_value: additionValue,
    addition_percentage: additionPercentage,
    start_date: startDate,
    end_date: endDate,
    risk_score: score,
    risk_level: level,
    red_flags: redFlagCodes,
    red_flag_details: redFlagDetails,
    red_flag_count: redFlagDetails.length,
    flag_evidence: flagEvidence,
    bidder_count: toNumber(raw.bidder_count ?? raw.bidderCount),
    offer_count: toNumber(raw.offer_count ?? raw.offerCount),
    additions_count: toNumber(raw.additions_count ?? raw.additionsCount),
    modifications_count: toNumber(raw.modifications_count ?? raw.modificationsCount),
    process_id: String(raw.process_id || "").trim(),
    year,
    month,
    duration_days: durationDays,
    value_gap: finalValue - initialValue,
    search_blob: searchableText,
    score_explanation: scoreExplanation,
    risk_summary: riskSummary,
    required_manual_checks: requiredManualChecks,
    recommended_action: recommendedAction,
    limitations: riskLimitations,
    risk_limitations: riskLimitations,
    audit_summary: auditSummary,
    audit_evidence: auditEvidence,
    huecos_de_informacion: auditHuecos,
    documentos_a_revisar: auditDocumentos,
    audit_recommendation: String(raw.audit_recommendation || raw.auditRecommendation || raw.recomendacion || "").trim(),
    audit_score_explanation: auditScoreExplanation,
    audit_red_flags_activadas: auditRedFlags,
    audit_dimension_scores: auditDimensionScores,
    red_flags_activadas: auditRedFlags,
    evidence: raw.evidence,
    raw,
  };
}

function normalizeRiskLevelLabel(value, score, thresholds) {
  const text = String(value || "").trim().toLowerCase();
  if (text === "alto") {
    return "Alto";
  }
  if (text === "medio") {
    return "Medio";
  }
  if (text === "critico" || text === "crítico") {
    return "Critico";
  }
  if (text === "bajo") {
    return "Bajo";
  }

  const normalized = score >= 0 && score <= 100 ? score : 0;
  const [lowMin, lowMax] = thresholds.low;
  const [mediumMin, mediumMax] = thresholds.medium;
  const [highMin] = thresholds.high;

  if (normalized >= 85) {
    return "Critico";
  }

  if (normalized >= highMin && normalized <= thresholds.high[1]) {
    return "Alto";
  }

  if (normalized >= mediumMin && normalized <= mediumMax) {
    return "Medio";
  }

  if (normalized >= lowMin && normalized <= lowMax) {
    return "Bajo";
  }

  return "Bajo";
}

function calculateDurationDays(startDate, endDate) {
  const start = parseDate(startDate);
  const end = parseDate(endDate);

  if (!start || !end) {
    return 0;
  }

  const diff = Math.round((end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24));
  return diff > 0 ? diff : 0;
}

function normalizeStringArray(value) {
  if (Array.isArray(value)) {
    return value.map((item) => String(item || "").trim()).filter(Boolean);
  }

  if (typeof value === "string") {
    return value
      .split(/\s*\|\s*|\s*;\s*|\s*,\s*/)
      .map((item) => item.trim())
      .filter(Boolean);
  }

  return [];
}
