import { escapeHtml, formatDate, formatMoney, formatNumber, truncateText } from "./secop-api.js";

export function buildExecutiveSummary({ sourceLabel, indicatorLabel, stats, selectedRecord, topGroup }) {
  const lines = [];
  lines.push(`La demo de GobIA Auditor analiza ${stats.totalRecords} registros de ${sourceLabel}.`);
  lines.push(
    `El foco principal es ${indicatorLabel}. El contrato seleccionado es ${selectedRecord.id} con ${formatMoney(selectedRecord.amount)} y nivel ${selectedRecord.level.toUpperCase()}.`,
  );
  lines.push(
    `Se observan ${stats.highRiskCount} contratos de riesgo alto, ${stats.mediumRiskCount} de riesgo medio y ${stats.lowRiskCount} de riesgo bajo.`,
  );

  if (topGroup) {
    lines.push(`La categoria que mas destaca es ${topGroup.label} con ${topGroup.labelDetail}.`);
  }

  if (stats.topSignal && stats.topSignal !== "Sin señales dominantes") {
    lines.push(`La señal mas repetida en el universo cargado es ${stats.topSignal}.`);
  }

  return lines.join(" ");
}

export function buildMethodologyCopy() {
  return [
    "El score local no acusa delitos.",
    "Solo prioriza revision humana con reglas explicables y datos publicos.",
    "Dify se usa para generar explicaciones en lenguaje ciudadano.",
    "La API de Dify quedara para una siguiente iteracion si el MVP ya esta estable.",
  ];
}

export function buildSelectionContext(record) {
  return [
    {
      label: "Contrato",
      value: record.id,
    },
    {
      label: "Entidad",
      value: record.entity || "Sin entidad",
    },
    {
      label: "Proveedor",
      value: record.provider || "Sin proveedor",
    },
    {
      label: "Monto",
      value: formatMoney(record.amount),
    },
    {
      label: "Fecha",
      value: formatDate(record.date),
    },
    {
      label: "Modalidad",
      value: record.modality || "Sin modalidad",
    },
    {
      label: "Descripcion",
      value: truncateText(record.description || "Sin descripcion", 90),
    },
  ];
}

export function buildPromptPreview(record, analysis, indicatorLabel) {
  return {
    contract: {
      id: record.id,
      entity: record.entity,
      provider: record.provider,
      modality: record.modality,
      amount: record.amount,
      date: record.date,
      description: record.description,
      status: record.status,
    },
    analysis: {
      score: record.score,
      level: record.level,
      indicator: indicatorLabel,
      signals: analysis.signals.map((signal) => signal.label),
      auditQuestions: analysis.auditQuestions,
    },
    prompt:
      "Resume el caso, explica el nivel de riesgo, enumera señales y sugiere preguntas para un auditor humano.",
  };
}

export function buildAuditChecklist(signals, record) {
  const base = [
    `Verificar el expediente de ${record.id}.`,
    `Revisar los soportes del objeto contractual.`,
    `Cruzar la modalidad con el valor y la competencia reportada.`,
  ];

  const signalItems = signals.map((signal) => `${signal.label}: ${signal.detail}`);
  return [...base, ...signalItems];
}

export function buildIndicatorNote(indicatorLabel, topGroup) {
  if (!topGroup) {
    return `No hay categorias suficientes para explicar ${indicatorLabel.toLowerCase()}.`;
  }

  return `${indicatorLabel} concentra su lectura en ${topGroup.label} y permite revisar ${topGroup.labelDetail}.`;
}

export function buildPrintableTitle(sourceLabel, indicatorLabel) {
  return `GobIA Auditor - ${sourceLabel} - ${indicatorLabel}`;
}

export function toHtmlList(items) {
  return items.map((item) => `<li>${escapeHtml(item)}</li>`).join("");
}

export function buildPdfReportModel({
  auditChecklist = [],
  auditQuestions = [],
  conversationId = "",
  difyAnswer = "",
  difyStatus = "",
  indicatorLabel,
  lastUpdated = "",
  methodology = [],
  traceability = null,
  selectedRecord,
  signalNarrative = "",
  sourceLabel,
  stats,
  topGroup = null,
}) {
  const reviewPriority = buildReviewPriority(selectedRecord);
  const actionPlan = buildActionPlan(selectedRecord);
  const signalHighlights = selectedRecord.signals.slice(0, 3).map((signal) => signal.label);
  const selection = buildSelectionContext(selectedRecord);
  const reportSummary = buildExecutiveSummary({
    sourceLabel,
    indicatorLabel,
    stats,
    selectedRecord,
    topGroup,
  });
  const traceabilitySummary = buildTraceabilitySummary(traceability, sourceLabel, stats, lastUpdated);

  return {
    title: buildPrintableTitle(sourceLabel, indicatorLabel),
    subtitle: `${selectedRecord.id} | ${selectedRecord.level.toUpperCase()} | ${formatMoney(selectedRecord.amount)}`,
    scoreLabel: `${selectedRecord.score}/100`,
    riskLabel: selectedRecord.riskLabel,
    reviewPriority,
    actionPlan,
    signalHighlights,
    meta: [
      { label: "Fuente", value: sourceLabel },
      { label: "Indicador", value: indicatorLabel },
      { label: "Trazabilidad", value: traceabilitySummary.statusLabel },
      { label: "Actualizado", value: formatDate(lastUpdated) },
      { label: "Conversacion", value: conversationId || "nueva" },
    ],
    selection,
    signals: selectedRecord.signals.map((signal) => ({
      label: signal.label,
      detail: signal.detail,
    })),
    stats: [
      { label: "Registros", value: formatNumber(stats.totalRecords) },
      { label: "Riesgo medio", value: `${Math.round(stats.averageRisk)}/100` },
      { label: "Completitud", value: `${Math.round(stats.completeness)}%` },
      { label: "Altos", value: formatNumber(stats.highRiskCount) },
    ],
    overview: reportSummary,
    indicatorNote: buildIndicatorNote(indicatorLabel, topGroup),
    signalNarrative,
    auditQuestions,
    auditChecklist,
    methodology,
    difyAnswer,
    difyStatus,
    recommendation: buildRecommendationText(selectedRecord),
    topSignal: stats.topSignal,
    traceability: traceabilitySummary,
  };
}

function buildTraceabilitySummary(traceability, sourceLabel, stats, lastUpdated) {
  const fallbackItems = [
    { label: "Fuente", value: sourceLabel },
    { label: "Registros", value: formatNumber(stats.totalRecords) },
    { label: "Riesgo medio", value: `${Math.round(stats.averageRisk)}/100` },
    { label: "Completitud", value: `${Math.round(stats.completeness)}%` },
    { label: "Actualizado", value: formatDate(lastUpdated) },
  ];

  const items = Array.isArray(traceability?.items) && traceability.items.length > 0 ? traceability.items : fallbackItems;

  return {
    label: String(traceability?.label || "Trazabilidad operativa"),
    detail: String(traceability?.detail || `Universo consolidado desde ${sourceLabel}.`),
    statusLabel: String(traceability?.statusLabel || "Verificada"),
    statusDetail: String(
      traceability?.statusDetail ||
        `Se revisan fuente, modo y cobertura para ${formatNumber(stats.totalRecords)} registros.`,
    ),
    items,
  };
}

function buildRecommendationText(record) {
  if (record.level === "high") {
    return "Priorizar revision humana inmediata. Verificar expediente, soportes, modalidad y competencia sin afirmar corrupcion.";
  }

  if (record.level === "medium") {
    return "Revisar el contexto, contrastar soportes y documentar la justificacion antes de sacar conclusiones.";
  }

  return "Mantener monitoreo rutinario y conservar el caso como referencia comparativa.";
}

function buildReviewPriority(record) {
  if (record.level === "high") {
    return {
      label: "Revision inmediata",
      detail:
        "El caso acumula suficientes señales para priorizar verificacion humana con trazabilidad completa.",
      action:
        "Solicitar expediente, soportes tecnicos, justificacion de modalidad y evidencia de competencia.",
    };
  }

  if (record.level === "medium") {
    return {
      label: "Revision contextual",
      detail:
        "El score sugiere revisar soportes y contexto antes de sacar conclusiones o escalar el caso.",
      action:
        "Contrastar la necesidad, la modalidad y la documentacion de respaldo con el area responsable.",
    };
  }

  return {
    label: "Seguimiento rutinario",
    detail:
      "El contrato se mantiene dentro de una lectura conservadora y sirve como referencia de monitoreo.",
    action:
      "Conservar el caso bajo seguimiento normal y registrar cualquier cambio relevante del contexto.",
  };
}

function buildActionPlan(record) {
  const plan = [
    "Revisar el objeto contractual y confirmar que la descripcion tenga suficiente detalle verificable.",
    "Cruzar el monto, la modalidad y la competencia reportada contra el resto del universo analizado.",
    "Validar que existan soportes suficientes para explicar la decision de contratacion.",
  ];

  if (record.signals.some((signal) => signal.id === "low-competition")) {
    plan.push("Analizar la pluralidad de oferentes y documentar por que la competencia fue limitada.");
  }

  if (record.signals.some((signal) => signal.id === "sensitive-modality")) {
    plan.push("Revisar la justificacion de la modalidad usada frente al objeto y al valor observado.");
  }

  if (record.signals.some((signal) => signal.id === "sanction-signal")) {
    plan.push("Cruzar antecedentes reputacionales o sanciones antes de emitir una lectura final.");
  }

  if (record.level === "high") {
    plan.push("Escalar el caso a una revision prioritaria con evidencia documental completa.");
  }

  return plan.slice(0, 5);
}
