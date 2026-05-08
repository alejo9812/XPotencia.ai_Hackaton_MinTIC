import { average, formatMoney, monthKey, normalizeText, percentile, truncateText } from "./secop-api.js";

const DEFAULT_RULES = {
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

export function scoreContracts(records, riskRules = DEFAULT_RULES) {
  const rules = mergeRules(riskRules);
  const amounts = records.map((record) => record.amount);
  const p90 = percentile(amounts, rules.thresholds.highPercentile);
  const p95 = percentile(amounts, rules.thresholds.veryHighPercentile);
  const repeatMap = buildRepeatMap(records);

  const scoredRecords = records.map((record) =>
    scoreRecord(record, {
      p90,
      p95,
      repeatMap,
      rules,
    }),
  );

  const summary = buildSummary(scoredRecords);
  const rankedByRisk = [...scoredRecords].sort((left, right) => {
    if (right.score !== left.score) {
      return right.score - left.score;
    }

    return right.amount - left.amount;
  });

  return {
    records: scoredRecords,
    rankedByRisk,
    summary,
  };
}

export function sortRecords(records, indicatorId = "risk") {
  const dataset = [...records];

  if (indicatorId === "value") {
    return dataset.sort((left, right) => right.amount - left.amount);
  }

  if (indicatorId === "competition") {
    return dataset.sort((left, right) => {
      const leftScore = competitionRiskScore(left);
      const rightScore = competitionRiskScore(right);
      return rightScore - leftScore;
    });
  }

  if (indicatorId === "quality") {
    return dataset.sort((left, right) => left.completeness - right.completeness);
  }

  if (indicatorId === "timeline") {
    return dataset.sort((left, right) => {
      const leftDate = left.date ? new Date(left.date).getTime() : 0;
      const rightDate = right.date ? new Date(right.date).getTime() : 0;
      return rightDate - leftDate;
    });
  }

  return dataset.sort((left, right) => {
    if (right.score !== left.score) {
      return right.score - left.score;
    }

    return right.amount - left.amount;
  });
}

export function buildGroupedSeries(records, groupBy, indicatorId = "risk", topN = 8) {
  const buckets = new Map();

  for (const record of records) {
    const key = resolveGroupKey(record, groupBy);
    const bucket = buckets.get(key) ?? createBucket(key);
    bucket.count += 1;
    bucket.amount += record.amount;
    bucket.risk += record.score;
    bucket.completeness += record.completeness;
    bucket.participants += record.participants || 0;
    bucket.offers += record.offers || 0;
    bucket.lowCompetition += record.participants <= 1 ? 1 : 0;
    buckets.set(key, bucket);
  }

  const series = [...buckets.values()].map((bucket) => {
    const averageRisk = bucket.risk / bucket.count;
    const averageCompleteness = bucket.completeness / bucket.count;
    const averageParticipants = bucket.participants / bucket.count;
    const lowCompetitionRate = bucket.lowCompetition / bucket.count;

    const value =
      indicatorId === "value"
        ? bucket.amount
        : indicatorId === "competition"
          ? lowCompetitionRate * 100
          : indicatorId === "quality"
            ? averageCompleteness
            : indicatorId === "timeline"
              ? bucket.amount
              : averageRisk;

    const label =
      indicatorId === "competition"
        ? `${Math.round(lowCompetitionRate * 100)}% baja competencia`
        : indicatorId === "quality"
          ? `${Math.round(averageCompleteness)}% completo`
          : indicatorId === "value"
            ? formatMoney(bucket.amount)
            : indicatorId === "timeline"
              ? `${bucket.count} casos`
              : `${Math.round(averageRisk)}/100`;

    return {
      key: bucket.key,
      label: bucket.key,
      count: bucket.count,
      amount: bucket.amount,
      score: averageRisk,
      completeness: averageCompleteness,
      participants: averageParticipants,
      lowCompetitionRate,
      value,
      labelDetail: label,
    };
  });

  return series
    .sort((left, right) => right.value - left.value)
    .slice(0, topN);
}

export function buildSummary(scoredRecords) {
  const totalRecords = scoredRecords.length;
  const totalAmount = scoredRecords.reduce((sum, record) => sum + record.amount, 0);
  const averageAmount = average(scoredRecords.map((record) => record.amount));
  const averageRisk = average(scoredRecords.map((record) => record.score));
  const completeness = average(scoredRecords.map((record) => record.completeness));
  const highRiskCount = scoredRecords.filter((record) => record.level === "high").length;
  const mediumRiskCount = scoredRecords.filter((record) => record.level === "medium").length;
  const lowRiskCount = scoredRecords.filter((record) => record.level === "low").length;
  const topSignal = buildTopSignal(scoredRecords);

  return {
    totalRecords,
    totalAmount,
    averageAmount,
    averageRisk,
    completeness,
    highRiskCount,
    mediumRiskCount,
    lowRiskCount,
    topSignal,
  };
}

export function buildSelectionSignals(record, riskRules = DEFAULT_RULES) {
  const rules = mergeRules(riskRules);
  const signals = [];

  if (record.score >= 95) {
    signals.push({
      id: "risk-very-high",
      label: "Riesgo muy alto",
      detail: `Score de ${record.score}/100, por encima de la banda critica.`,
      points: 0,
      question: "Que soporte adicional justifica esta concentracion de alertas?",
    });
  } else if (record.score >= 70) {
    signals.push({
      id: "risk-high",
      label: "Riesgo alto",
      detail: `Score de ${record.score}/100, requiere revision humana.`,
      points: 0,
      question: "Puede revisarse el expediente con prioridad?",
    });
  } else if (record.score >= 35) {
    signals.push({
      id: "risk-medium",
      label: "Riesgo medio",
      detail: `Score de ${record.score}/100, conviene revisar contexto y soportes.`,
      points: 0,
      question: "Hay evidencia adicional que cambie la lectura del caso?",
    });
  } else {
    signals.push({
      id: "risk-low",
      label: "Riesgo bajo",
      detail: `Score de ${record.score}/100, no se observan alertas dominantes.`,
      points: 0,
      question: "Se mantienen controles de rutina?",
    });
  }

  if ((record.percentiles?.p95 ?? 0) > 0 && record.amount >= record.percentiles.p95) {
    signals.push({
      id: "amount-p95",
      label: "Monto por encima del percentil 95",
      detail: `El monto ${formatMoney(record.amount)} supera el tramo mas alto del universo cargado.`,
      points: rules.weights.amountVeryHigh,
      question: "Por que este monto supera con tanta diferencia a otros casos?",
    });
  }

  if (
    (record.percentiles?.p90 ?? 0) > 0 &&
    record.amount >= record.percentiles.p90 &&
    record.amount < (record.percentiles?.p95 ?? Number.POSITIVE_INFINITY)
  ) {
    signals.push({
      id: "amount-p90",
      label: "Monto por encima del percentil 90",
      detail: `El monto ${formatMoney(record.amount)} queda dentro del grupo mas alto del analisis.`,
      points: rules.weights.amountHigh,
      question: "Que caracteriza este contrato frente al resto del grupo?",
    });
  }

  if (record.signals?.some((signal) => signal.id === "short-description")) {
    signals.push({
      id: "short-description",
      label: "Objeto contractual corto",
      detail: `La descripcion solo tiene ${record.descriptionWords} palabras.`,
      points: rules.weights.shortDescription,
      question: "El objeto contractual tiene suficiente detalle tecnico?",
    });
  }

  if (record.signals?.some((signal) => signal.id === "generic-description")) {
    signals.push({
      id: "generic-description",
      label: "Descripcion generica",
      detail: "El texto del contrato usa terminos muy generales y pocos elementos verificables.",
      points: rules.weights.genericDescription,
      question: "La descripcion permite saber exactamente que se entregara?",
    });
  }

  if (record.signals?.some((signal) => signal.id === "low-competition")) {
    signals.push({
      id: "low-competition",
      label: "Baja competencia",
      detail: "Hay pocos participantes u ofertas frente al universo analizado.",
      points: rules.weights.lowCompetition,
      question: "La pluralidad de oferentes fue suficiente?",
    });
  }

  if (record.signals?.some((signal) => signal.id === "sensitive-modality")) {
    signals.push({
      id: "sensitive-modality",
      label: "Modalidad sensible",
      detail: "La modalidad observada suele requerir una revision contextual mas cuidadosa.",
      points: rules.weights.sensitiveModality,
      question: "La modalidad elegida es la mas justificada para este objeto?",
    });
  }

  if (record.signals?.some((signal) => signal.id === "sanction-signal")) {
    signals.push({
      id: "sanction-signal",
      label: "Referencia reputacional",
      detail: "La fuente de sanciones o multas sugiere priorizar revision adicional.",
      points: rules.weights.sanctionSignal,
      question: "Conviene cruzar este caso con antecedentes o sanciones?",
    });
  }

  return dedupeSignals(signals);
}

export function buildSignalNarrative(signals) {
  if (!signals.length) {
    return "No se detectaron señales dominantes en el contrato seleccionado.";
  }

  return signals
    .slice(0, 3)
    .map((signal) => `${signal.label}: ${signal.detail}`)
    .join(" ");
}

export function buildIndicatorSummary(indicatorId, stats, selectedRecord) {
  if (indicatorId === "value") {
    return `La vista enfatiza concentracion por valor. El contrato seleccionado tiene ${formatMoney(selectedRecord.amount)} y la cartera analizada suma ${formatMoney(stats.totalAmount)}.`;
  }

  if (indicatorId === "competition") {
    return `La vista enfatiza competencia. El contrato seleccionado tiene ${selectedRecord.participants} participantes y ${selectedRecord.offers} ofertas reportadas.`;
  }

  if (indicatorId === "quality") {
    return `La vista enfatiza calidad de datos. El caso seleccionado muestra ${Math.round(selectedRecord.completeness)}% de completitud.`;
  }

  if (indicatorId === "timeline") {
    return `La vista enfatiza distribucion temporal. El caso seleccionado fue registrado el ${selectedRecord.date || "sin fecha"} y sirve para detectar concentraciones por periodo.`;
  }

  return `La vista enfatiza riesgo. El contrato seleccionado tiene un score de ${selectedRecord.score}/100 con ${selectedRecord.signals.length} señales activas.`;
}

export function buildAuditQuestions(signals, fallback = []) {
  const questions = signals
    .map((signal) => signal.question)
    .filter(Boolean);

  return dedupeList([...questions, ...fallback]);
}

export function buildDifyPayload(record, analysis, indicatorLabel, sourceLabel) {
  return {
    contract: {
      id: record.id,
      source: sourceLabel,
      entity: record.entity,
      provider: record.provider,
      modality: record.modality,
      amount: record.amount,
      date: record.date,
      department: record.department,
      municipality: record.municipality,
      status: record.status,
      description: record.description,
      participants: record.participants,
      offers: record.offers,
      durationDays: record.durationDays,
    },
    analysis: {
      score: record.score,
      level: record.level,
      signals: record.signals,
      indicator: indicatorLabel,
      summary: analysis.summary.topSignal,
      auditQuestions: analysis.auditQuestions,
    },
    methodology: {
      message:
        "El score local no acusa delitos. Solo prioriza revision humana con reglas explicables y datos publicos.",
    },
  };
}

function scoreRecord(record, context) {
  const { p90, p95, repeatMap, rules } = context;
  const signals = [];
  let score = 0;

  const descriptionWords = normalizeText(record.description).split(" ").filter(Boolean).length;
  const normalizedDescription = normalizeText(record.description);
  const modality = normalizeText(record.modality);
  const providerKey = normalizeText(record.provider);
  const entityKey = normalizeText(record.entity);
  const pairKey = `${entityKey}::${providerKey}`;
  const repeatCount = repeatMap.get(pairKey) ?? 0;
  const completeness = calculateCompleteness(record);
  const isSanctionSource = record.sourceId === "secop_sanctions" || modality.includes("sancion");
  const isFutureDate = isDateInFuture(record.date, rules.thresholds.futureDateToleranceDays);
  const hasMissingCriticalFields = [];

  if (!record.entity) {
    score += rules.weights.missingEntity;
    hasMissingCriticalFields.push("entity");
    signals.push(createSignal("missing-entity", "Entidad faltante", "No se pudo identificar la entidad contratante."));
  }

  if (!record.provider) {
    score += rules.weights.missingProvider;
    hasMissingCriticalFields.push("provider");
    signals.push(createSignal("missing-provider", "Proveedor faltante", "No se pudo identificar el proveedor o contratista."));
  }

  if (!record.amount) {
    score += rules.weights.missingAmount;
    hasMissingCriticalFields.push("amount");
    signals.push(createSignal("missing-amount", "Valor faltante", "El monto no aparece o llega en cero."));
  }

  if (!record.date) {
    score += rules.weights.missingDate;
    hasMissingCriticalFields.push("date");
    signals.push(createSignal("missing-date", "Fecha faltante", "No hay fecha util para verificar la secuencia temporal."));
  }

  if (!record.modality) {
    score += rules.weights.missingModality;
    hasMissingCriticalFields.push("modality");
    signals.push(createSignal("missing-modality", "Modalidad faltante", "No hay modalidad suficiente para entender la contratacion."));
  }

  if (record.amount >= p95) {
    score += rules.weights.amountVeryHigh;
    signals.push(
      createSignal(
        "amount-p95",
        "Monto por encima del percentil 95",
        `El monto ${formatMoney(record.amount)} supera el tramo mas alto del universo cargado.`,
      ),
    );
  } else if (record.amount >= p90) {
    score += rules.weights.amountHigh;
    signals.push(
      createSignal(
        "amount-p90",
        "Monto por encima del percentil 90",
        `El monto ${formatMoney(record.amount)} se ubica entre los mas altos del conjunto.`,
      ),
    );
  }

  if (descriptionWords > 0 && descriptionWords < rules.thresholds.shortDescriptionWords) {
    score += rules.weights.shortDescription;
    signals.push(
      createSignal(
        "short-description",
        "Objeto contractual corto",
        `La descripcion solo tiene ${descriptionWords} palabras.`,
      ),
    );
  }

  if (rules.genericTerms.some((term) => normalizedDescription.includes(term))) {
    const genericHits = rules.genericTerms.filter((term) => normalizedDescription.includes(term)).length;
    if (descriptionWords < rules.thresholds.genericDescriptionWords || genericHits >= 2) {
      score += rules.weights.genericDescription;
      signals.push(
        createSignal(
          "generic-description",
          "Descripcion generica",
          "El texto usa terminos amplios y poco verificables.",
        ),
      );
    }
  }

  if (repeatCount >= rules.thresholds.repeatProviderCount) {
    score += rules.weights.repeatProvider;
    signals.push(
      createSignal(
        "repeat-provider",
        "Proveedor repetido con la misma entidad",
        `El proveedor aparece ${repeatCount} veces junto a la misma entidad en el universo cargado.`,
      ),
    );
  }

  if (record.participants > 0 && record.participants <= rules.thresholds.lowCompetitionParticipants) {
    score += rules.weights.lowCompetition;
    signals.push(
      createSignal(
        "low-competition",
        "Baja competencia",
        `Solo se registran ${record.participants} participantes para este proceso.`,
      ),
    );
  }

  if (rules.sensitiveModalities.some((item) => modality.includes(item))) {
    score += rules.weights.sensitiveModality;
    signals.push(
      createSignal(
        "sensitive-modality",
        "Modalidad sensible",
        "La modalidad observada suele requerir una revision contextual mas cuidadosa.",
      ),
    );
  }

  if (record.date && isFutureDate) {
    score += rules.weights.invalidDate;
    signals.push(
      createSignal(
        "invalid-date",
        "Fecha inconsistente",
        "La fecha calculada queda por fuera de la ventana temporal esperada.",
      ),
    );
  }

  if (isSanctionSource) {
    score += rules.weights.sanctionSignal;
    signals.push(
      createSignal(
        "sanction-signal",
        "Referencia reputacional",
        "La fuente sugiere que conviene cruzar el caso con sanciones o multas.",
      ),
    );
  }

  const finalScore = clampScore(score);
  const level = resolveLevel(finalScore);

  return {
    ...record,
    score: finalScore,
    level,
    completeness,
    descriptionWords,
    signals: dedupeSignals(signals),
    percentiles: { p90, p95 },
    missingCriticalFields: hasMissingCriticalFields,
    riskLabel: resolveRiskLabel(finalScore),
    auditQuestions: buildAuditQuestions(signals),
    summary: buildRecordSummary(record, finalScore, level),
  };
}

function buildRecordSummary(record, score, level) {
  const descriptor = {
    high: "requiere revision prioritaria",
    medium: "merece verificacion contextual",
    low: "se mantiene dentro de una lectura rutinaria",
  }[level];

  return `${record.id} con ${formatMoney(record.amount)} y nivel ${level.toUpperCase()} (${score}/100) ${descriptor}.`;
}

function buildTopSignal(records) {
  const counter = new Map();
  for (const record of records) {
    for (const signal of record.signals) {
      counter.set(signal.label, (counter.get(signal.label) ?? 0) + 1);
    }
  }

  const top = [...counter.entries()].sort((left, right) => right[1] - left[1])[0];
  return top ? `${top[0]} (${top[1]} casos)` : "Sin señales dominantes";
}

function buildRepeatMap(records) {
  const map = new Map();
  for (const record of records) {
    const key = `${normalizeText(record.entity)}::${normalizeText(record.provider)}`;
    map.set(key, (map.get(key) ?? 0) + 1);
  }
  return map;
}

function calculateCompleteness(record) {
  const fields = ["entity", "provider", "modality", "amount", "date", "description", "status"];
  const filled = fields.filter((field) => {
    const value = record[field];
    return value != null && String(value).trim() !== "" && String(value).trim() !== "0";
  }).length;
  return Math.round((filled / fields.length) * 100);
}

function resolveGroupKey(record, groupBy) {
  if (groupBy === "month") {
    return monthKey(record.date);
  }

  if (groupBy === "provider") {
    return record.provider || "Sin proveedor";
  }

  if (groupBy === "entity") {
    return record.entity || "Sin entidad";
  }

  if (groupBy === "department") {
    return record.department || "Sin departamento";
  }

  if (groupBy === "status") {
    return record.status || "Sin estado";
  }

  if (groupBy === "modality") {
    return record.modality || "Sin modalidad";
  }

  return record.entity || "Sin categoria";
}

function competitionRiskScore(record) {
  const participants = Number(record.participants) || 0;
  const offers = Number(record.offers) || 0;
  const scarcity = participants <= 1 ? 100 : participants <= 2 ? 80 : participants <= 3 ? 60 : participants <= 5 ? 40 : 20;
  const offerPenalty = offers <= 1 ? 15 : offers <= 2 ? 8 : 0;
  return scarcity + offerPenalty;
}

function createBucket(key) {
  return {
    key,
    count: 0,
    amount: 0,
    risk: 0,
    completeness: 0,
    participants: 0,
    offers: 0,
    lowCompetition: 0,
  };
}

function mergeRules(rules) {
  return {
    ...DEFAULT_RULES,
    ...rules,
    thresholds: {
      ...DEFAULT_RULES.thresholds,
      ...(rules?.thresholds ?? {}),
    },
    weights: {
      ...DEFAULT_RULES.weights,
      ...(rules?.weights ?? {}),
    },
    genericTerms: rules?.genericTerms?.length ? rules.genericTerms : DEFAULT_RULES.genericTerms,
    sensitiveModalities:
      rules?.sensitiveModalities?.length ? rules.sensitiveModalities : DEFAULT_RULES.sensitiveModalities,
  };
}

function resolveLevel(score) {
  if (score >= 70) {
    return "high";
  }
  if (score >= 35) {
    return "medium";
  }
  return "low";
}

function resolveRiskLabel(score) {
  if (score >= 85) {
    return "Prioridad maxima";
  }
  if (score >= 70) {
    return "Revision prioritaria";
  }
  if (score >= 35) {
    return "Verificacion contextual";
  }
  return "Seguimiento rutinario";
}

function isDateInFuture(value, toleranceDays = 14) {
  if (!value) {
    return false;
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return false;
  }

  const now = new Date();
  const diffDays = (parsed.getTime() - now.getTime()) / (1000 * 60 * 60 * 24);
  return diffDays > toleranceDays;
}

function createSignal(id, label, detail) {
  return {
    id,
    label,
    detail,
  };
}

function dedupeSignals(signals) {
  const seen = new Set();
  const list = [];

  for (const signal of signals) {
    const key = signal.id || signal.label;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    list.push(signal);
  }

  return list;
}

function dedupeList(values) {
  return [...new Set(values.filter(Boolean))];
}

function clampScore(value) {
  return Math.max(0, Math.min(100, Math.round(value)));
}
