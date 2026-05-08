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
  selectedRecord,
  signalNarrative = "",
  sourceLabel,
  stats,
  topGroup = null,
}) {
  const selection = buildSelectionContext(selectedRecord);
  const reportSummary = buildExecutiveSummary({
    sourceLabel,
    indicatorLabel,
    stats,
    selectedRecord,
    topGroup,
  });

  return {
    title: buildPrintableTitle(sourceLabel, indicatorLabel),
    subtitle: `${selectedRecord.id} | ${selectedRecord.level.toUpperCase()} | ${formatMoney(selectedRecord.amount)}`,
    meta: [
      { label: "Fuente", value: sourceLabel },
      { label: "Indicador", value: indicatorLabel },
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
