const CATALOG_URLS = {
  sources: new URL("./data/source-catalog.json", import.meta.url),
  indicators: new URL("./data/indicator-catalog.json", import.meta.url),
  riskRules: new URL("./data/risk-rules.json", import.meta.url),
  dify: new URL("./data/dify.config.json", import.meta.url),
  mockRecords: new URL("./data/secop.mock.json", import.meta.url),
};

const DEFAULT_SOURCE_CATALOG = [
  {
    id: "all",
    label: "Todos los datos",
    datasetId: "multi-source",
    jsonUrl: "",
    csvUrl: "",
    description: "Vista combinada para revisar el universo completo de la demo.",
    focus: ["risk", "value", "competition", "quality"],
  },
  {
    id: "secop_contracts",
    label: "SECOP II - Contratos Electronicos",
    datasetId: "jbjy-vk9h",
    jsonUrl: "https://www.datos.gov.co/resource/jbjy-vk9h.json?$limit=5000",
    csvUrl: "https://www.datos.gov.co/resource/jbjy-vk9h.csv?$limit=5000",
    description: "Contrato, valor, proveedor, entidad y trazabilidad basica.",
    focus: ["risk", "value", "quality"],
  },
  {
    id: "secop_processes",
    label: "SECOP II - Procesos de Contratacion",
    datasetId: "p6dx-8zbt",
    jsonUrl: "https://www.datos.gov.co/resource/p6dx-8zbt.json?$limit=5000",
    csvUrl: "https://www.datos.gov.co/resource/p6dx-8zbt.csv?$limit=5000",
    description: "Procesos, competencia, modalidad y etapa de compra.",
    focus: ["competition", "quality"],
  },
  {
    id: "secop_bidders",
    label: "SECOP II - Proponentes por Proceso",
    datasetId: "hgi6-6wh3",
    jsonUrl: "https://www.datos.gov.co/resource/hgi6-6wh3.json?$limit=5000",
    csvUrl: "https://www.datos.gov.co/resource/hgi6-6wh3.csv?$limit=5000",
    description: "Pluralidad de participantes y comportamiento de oferentes.",
    focus: ["competition", "risk"],
  },
  {
    id: "secop_offers",
    label: "SECOP II - Ofertas por Proceso",
    datasetId: "wi7w-2nvm",
    jsonUrl: "https://www.datos.gov.co/resource/wi7w-2nvm.json?$limit=5000",
    csvUrl: "https://www.datos.gov.co/resource/wi7w-2nvm.csv?$limit=5000",
    description: "Cantidad de ofertas y dinamica de participacion.",
    focus: ["competition", "value"],
  },
  {
    id: "secop_sanctions",
    label: "SECOP II - Multas y Sanciones",
    datasetId: "it5q-hg94",
    jsonUrl: "https://www.datos.gov.co/resource/it5q-hg94.json?$limit=5000",
    csvUrl: "https://www.datos.gov.co/resource/it5q-hg94.csv?$limit=5000",
    description: "Señales reputacionales para priorizar revision humana.",
    focus: ["risk", "quality"],
  },
];

const DEFAULT_INDICATOR_CATALOG = [
  {
    id: "risk",
    label: "Riesgo",
    description: "Score deterministico y señales de opacidad.",
    groupHint: "entity",
    chartLabel: "Ranking de riesgo",
    study: "Prioriza contratos con montos altos, descripciones debiles o campos incompletos.",
    question: "Que contratos requieren revision humana primero?",
  },
  {
    id: "value",
    label: "Valor",
    description: "Concentracion economica y montos mas altos.",
    groupHint: "provider",
    chartLabel: "Concentracion por valor",
    study: "Muestra quien concentra mas recursos y si el valor se concentra en pocas manos.",
    question: "Donde esta el mayor monto contratado?",
  },
  {
    id: "competition",
    label: "Competencia",
    description: "Pluralidad de participantes y oportunidades de oferta.",
    groupHint: "modality",
    chartLabel: "Competencia por categoria",
    study: "Ayuda a ver procesos con poca pluralidad o baja participacion.",
    question: "Que procesos tienen menos competencia?",
  },
  {
    id: "quality",
    label: "Calidad",
    description: "Completitud de datos y limpieza para auditoria.",
    groupHint: "entity",
    chartLabel: "Calidad de datos",
    study: "Detecta registros con campos vacios, fechas dudosas o textos genericos.",
    question: "Que registros necesitan limpieza o verificacion?",
  },
  {
    id: "timeline",
    label: "Linea temporal",
    description: "Distribucion por fecha para encontrar picos de carga o concentracion.",
    groupHint: "month",
    chartLabel: "Distribucion temporal",
    study: "Sirve para detectar periodos con mucha actividad, repeticion o concentracion.",
    question: "En que periodo se concentran los casos?",
  },
];

const DEFAULT_RISK_RULES = {
  thresholds: {
    shortDescriptionWords: 8,
    genericDescriptionWords: 16,
    lowCompetitionParticipants: 2,
    highPercentile: 90,
    veryHighPercentile: 95,
    repeatProviderCount: 2,
    futureDateToleranceDays: 14,
  },
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
  },
  genericTerms: [
    "apoyo",
    "servicio",
    "suministro",
    "mantenimiento",
    "logistica",
    "operacion",
    "gestion",
    "tecnico",
    "general",
    "integral",
  ],
  sensitiveModalities: [
    "contratacion directa",
    "minima cuantia",
    "seleccion abreviada",
  ],
};

const DEFAULT_DIFY_CONFIG = {
  enabled: false,
  title: "GobIA Auditor",
  webAppUrl: "",
  iframeHeight: 760,
  intro: "Chatflow embebido para explicar contratos, señales de opacidad y preguntas de auditoria.",
  cta: "Cuando tengas el URL del chatflow, lo conectamos aqui sin tocar la logica local.",
};

const DEFAULT_MOCK_RECORDS = [
  {
    sourceId: "secop_contracts",
    sourceLabel: "SECOP II - Contratos Electronicos",
    id: "CD-2026-001",
    entity: "Ministerio de Salud y Proteccion Social",
    provider: "Soluciones Integrales del Caribe SAS",
    modality: "Contratacion Directa",
    department: "Bogota D.C.",
    municipality: "Bogota",
    amount: "980.000.000",
    currency: "COP",
    date: "2026-03-22",
    description: "Servicio integral de apoyo tecnologico para operacion de plataforma de atencion",
    status: "Adjudicado",
    participants: 1,
    offers: 1,
    durationDays: 180,
    origin: "mock",
  },
  {
    sourceId: "secop_contracts",
    sourceLabel: "SECOP II - Contratos Electronicos",
    id: "CD-2026-002",
    entity: "Alcaldia de Medellin",
    provider: "DataGov Labs SAS",
    modality: "Licitacion Publica",
    department: "Antioquia",
    municipality: "Medellin",
    amount: "420000000",
    currency: "COP",
    date: "2026-02-14",
    description: "Implementacion de tablero ciudadano para analisis de datos abiertos",
    status: "Adjudicado",
    participants: 6,
    offers: 4,
    durationDays: 150,
    origin: "mock",
  },
];

const DEFAULT_ALIASES = {
  id: ["id", "contractid", "procesoid", "proceso", "consecutivo", "recordid"],
  entity: [
    "entity",
    "entidad",
    "nombre_entidad",
    "entidad_compradora",
    "proceso_entidad",
    "dependencia",
  ],
  provider: [
    "provider",
    "proveedor",
    "contratista",
    "adjudicatario",
    "supplier",
    "tercero",
  ],
  modality: ["modality", "modalidad", "modalidad_de_contratacion", "tipo_modalidad"],
  department: ["department", "departamento", "depto", "departamento_entidad"],
  municipality: ["municipality", "municipio", "ciudad", "localidad"],
  amount: ["amount", "monto", "valor", "cuantia", "valor_contrato", "valor_total"],
  date: [
    "date",
    "fecha",
    "fecha_de_firma",
    "fecha_publicacion",
    "fecha_inicio",
    "fecha_fin",
    "created_at",
  ],
  description: ["description", "objeto", "descripcion", "detalle", "objeto_del_proceso"],
  status: ["status", "estado", "estado_del_proceso", "fase"],
  participants: [
    "participants",
    "numero_participantes",
    "numero_proponentes",
    "num_participants",
    "oferentes",
  ],
  offers: ["offers", "numero_ofertas", "num_ofertas", "ofertas", "bids"],
  durationDays: ["durationdays", "duracion_dias", "dias", "plazo_dias"],
};

export async function loadCatalogs() {
  const [sources, indicators, riskRules, difyConfig] = await Promise.all([
    loadJson(CATALOG_URLS.sources, DEFAULT_SOURCE_CATALOG),
    loadJson(CATALOG_URLS.indicators, DEFAULT_INDICATOR_CATALOG),
    loadJson(CATALOG_URLS.riskRules, DEFAULT_RISK_RULES),
    loadJson(CATALOG_URLS.dify, DEFAULT_DIFY_CONFIG),
  ]);

  return {
    sources: normalizeArray(sources, DEFAULT_SOURCE_CATALOG),
    indicators: normalizeArray(indicators, DEFAULT_INDICATOR_CATALOG),
    riskRules: { ...DEFAULT_RISK_RULES, ...(riskRules ?? {}) },
    difyConfig: { ...DEFAULT_DIFY_CONFIG, ...(difyConfig ?? {}) },
  };
}

export async function loadMockRecords() {
  const records = await loadJson(CATALOG_URLS.mockRecords, DEFAULT_MOCK_RECORDS);
  return normalizeArray(records, DEFAULT_MOCK_RECORDS);
}

export async function loadSourceRecords(source, mode = "mock") {
  const baseRecords = await loadMockRecords();
  const sourceId = source?.id ?? "secop_contracts";

  if (mode === "live" && sourceId !== "all" && source?.jsonUrl) {
    const remoteRows = await loadRemoteRecords(source.jsonUrl, baseRecords);
    if (remoteRows.length > 0) {
      return normalizeRecords(remoteRows, source);
    }
  }

  if (sourceId === "all") {
    return normalizeRecords(baseRecords, source);
  }

  return normalizeRecords(
    baseRecords.filter((record) => getRawValue(record, "sourceid", "sourceId") === sourceId),
    source,
  );
}

export async function loadRemoteRecords(url, fallback = []) {
  try {
    const response = await fetch(url, { headers: { Accept: "application/json" } });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const payload = await response.json();
    if (Array.isArray(payload)) {
      return payload;
    }

    if (Array.isArray(payload?.data)) {
      return payload.data;
    }

    return fallback;
  } catch {
    return fallback;
  }
}

export async function loadJson(url, fallback) {
  try {
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    return await response.json();
  } catch {
    return fallback;
  }
}

export function normalizeRecords(records, source = {}) {
  return records.map((record, index) => normalizeContractRecord(record, source, index));
}

export function normalizeContractRecord(record, source = {}, index = 0) {
  const raw = isObject(record) ? record : {};
  const sourceId = toText(getRawValue(raw, "sourceid", "sourceId")) || source.id || "secop_contracts";
  const sourceLabel = toText(getRawValue(raw, "sourcelabel", "sourceLabel")) || source.label || "";

  return {
    id:
      toText(getRawValue(raw, ...DEFAULT_ALIASES.id)) ||
      `${sourceId}-${index + 1}`,
    sourceId,
    sourceLabel: sourceLabel || source.id || sourceId,
    entity: toText(getRawValue(raw, ...DEFAULT_ALIASES.entity)),
    provider: toText(getRawValue(raw, ...DEFAULT_ALIASES.provider)),
    modality: toText(getRawValue(raw, ...DEFAULT_ALIASES.modality)),
    department: toText(getRawValue(raw, ...DEFAULT_ALIASES.department)),
    municipality: toText(getRawValue(raw, ...DEFAULT_ALIASES.municipality)),
    amount: parseAmount(getRawValue(raw, ...DEFAULT_ALIASES.amount)),
    currency: toText(raw.currency) || "COP",
    date: parseDate(getRawValue(raw, ...DEFAULT_ALIASES.date)),
    description: toText(getRawValue(raw, ...DEFAULT_ALIASES.description)),
    status: toText(getRawValue(raw, ...DEFAULT_ALIASES.status)) || "Sin estado",
    participants: parseInteger(getRawValue(raw, ...DEFAULT_ALIASES.participants)),
    offers: parseInteger(getRawValue(raw, ...DEFAULT_ALIASES.offers)),
    durationDays: parseInteger(getRawValue(raw, ...DEFAULT_ALIASES.durationDays)),
    origin: toText(raw.origin) || "mock",
    raw,
  };
}

export function parseAmount(value) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  const text = toText(value)
    .replace(/\$/g, "")
    .replace(/\s/g, "")
    .replace(/[.](?=\d{3}(\D|$))/g, "")
    .replace(/,/g, ".");

  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed : 0;
}

export function parseInteger(value) {
  const parsed = Number.parseInt(toText(value).replace(/[^\d-]/g, ""), 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

export function parseDate(value) {
  const text = toText(value);
  if (!text) {
    return "";
  }

  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }

  return parsed.toISOString().slice(0, 10);
}

export function normalizeText(value) {
  return toText(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

export function formatMoney(value, locale = "es-CO") {
  const amount = Number(value) || 0;
  return new Intl.NumberFormat(locale, {
    style: "currency",
    currency: "COP",
    maximumFractionDigits: 0,
  }).format(amount);
}

export function formatNumber(value, locale = "es-CO") {
  return new Intl.NumberFormat(locale, {
    maximumFractionDigits: 0,
  }).format(Number(value) || 0);
}

export function formatPercent(value, locale = "es-CO") {
  return new Intl.NumberFormat(locale, {
    style: "percent",
    maximumFractionDigits: 0,
  }).format(Number(value) || 0);
}

export function formatDate(value, locale = "es-CO") {
  const parsed = parseDate(value);
  if (!parsed) {
    return "Sin fecha";
  }

  const date = new Date(parsed);
  return new Intl.DateTimeFormat(locale, {
    day: "2-digit",
    month: "short",
    year: "numeric",
  }).format(date);
}

export function monthKey(value) {
  const parsed = parseDate(value);
  return parsed ? parsed.slice(0, 7) : "sin-fecha";
}

export function percentile(values, percent) {
  const numbers = values
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value))
    .sort((a, b) => a - b);

  if (numbers.length === 0) {
    return 0;
  }

  if (numbers.length === 1) {
    return numbers[0];
  }

  const position = (percent / 100) * (numbers.length - 1);
  const lower = Math.floor(position);
  const upper = Math.ceil(position);

  if (lower === upper) {
    return numbers[lower];
  }

  return numbers[lower] + (numbers[upper] - numbers[lower]) * (position - lower);
}

export function average(values) {
  const numbers = values
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value));

  if (numbers.length === 0) {
    return 0;
  }

  const total = numbers.reduce((sum, value) => sum + value, 0);
  return total / numbers.length;
}

export function escapeHtml(value) {
  return toText(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

export function truncateText(value, maxLength = 120) {
  const text = toText(value);
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, Math.max(0, maxLength - 1)).trim()}…`;
}

export function getRawValue(record, ...keys) {
  if (!isObject(record)) {
    return "";
  }

  const normalizedKeys = keys.map((key) => normalizeText(key).replace(/\s+/g, ""));
  const entries = Object.entries(record);

  for (const [key, value] of entries) {
    const normalizedKey = normalizeText(key).replace(/\s+/g, "");
    if (normalizedKeys.includes(normalizedKey)) {
      return value;
    }
  }

  return "";
}

function normalizeArray(value, fallback) {
  if (!Array.isArray(value) || value.length === 0) {
    return fallback;
  }

  return value;
}

function isObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function toText(value) {
  if (value == null) {
    return "";
  }

  return String(value).trim();
}
