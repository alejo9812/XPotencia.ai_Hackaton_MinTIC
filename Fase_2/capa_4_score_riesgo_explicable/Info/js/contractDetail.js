import {
  escapeHtml,
  formatDate,
  formatMoney,
  formatNumber,
  formatPercent,
  mean,
  median,
  normalizeText,
} from "./utils.js";

const RISK_TONES = {
  Bajo: "low",
  Medio: "medium",
  Alto: "high",
  Critico: "high",
};

export function createDetailModel(contract, records, config) {
  if (!contract) {
    return null;
  }

  const peerGroup = records
    .filter(
      (record) =>
        record.contract_id !== contract.contract_id &&
        (record.department === contract.department || record.modality === contract.modality),
    )
    .sort((left, right) => Math.abs(left.risk_score - contract.risk_score) - Math.abs(right.risk_score - contract.risk_score))
    .slice(0, 6);

  const peerAverageRisk = peerGroup.length ? mean(peerGroup.map((item) => item.risk_score)) : contract.risk_score;
  const peerAverageValue = peerGroup.length ? mean(peerGroup.map((item) => item.final_value)) : contract.final_value;
  const peerMedianValue = peerGroup.length ? median(peerGroup.map((item) => item.final_value)) : contract.final_value;
  const riskDelta = contract.risk_score - peerAverageRisk;
  const valueDeltaPct = peerAverageValue ? ((contract.final_value - peerAverageValue) / peerAverageValue) * 100 : 0;
  const comparableCount = peerGroup.length;
  const similarContracts = peerGroup.slice(0, 3);
  const supplierMatches = records.filter(
    (record) => normalizeText(record.supplier) === normalizeText(contract.supplier),
  );
  const entityMatches = records.filter((record) => normalizeText(record.entity) === normalizeText(contract.entity));
  const departmentMatches = records.filter(
    (record) => normalizeText(record.department) === normalizeText(contract.department),
  );
  const similarObjectContracts = buildSimilarObjectContracts(contract, records, peerGroup);
  const dashboardCards = buildDashboardCards(contract, {
    comparableCount,
    departmentMatches,
    entityMatches,
    peerAverageRisk,
    peerAverageValue,
    peerMedianValue,
    riskDelta,
    similarObjectContracts,
    supplierMatches,
    valueDeltaPct,
  });
  const riskPatterns = buildRiskPatterns(contract, {
    comparableCount,
    departmentMatches,
    entityMatches,
    peerAverageRisk,
    riskDelta,
    similarObjectContracts,
    supplierMatches,
    valueDeltaPct,
  });
  const reportSections = buildReportSections(contract, {
    comparableCount,
    departmentMatches,
    entityMatches,
    peerAverageRisk,
    peerAverageValue,
    peerMedianValue,
    riskDelta,
    similarObjectContracts,
    supplierMatches,
    valueDeltaPct,
    patternCount: riskPatterns.length,
  });
  const connections = buildConnections(contract, {
    departmentMatches,
    entityMatches,
    similarObjectContracts,
    supplierMatches,
  });

  const interpretation = buildInterpretation(contract, {
    peerAverageRisk,
    peerAverageValue,
    peerMedianValue,
    comparableCount,
    riskDelta,
    valueDeltaPct,
  });

  return {
    peerAverageRisk,
    peerAverageValue,
    peerMedianValue,
    comparableCount,
    riskDelta,
    valueDeltaPct,
    similarContracts,
    dashboardCards,
    reportSections,
    riskPatterns,
    connections,
    interpretation,
    recommendation: buildRecommendation(contract),
    summaryText: buildSummaryText(contract),
    chatbotContext: buildChatbotContext(contract, {
      comparableCount,
      riskDelta,
      valueDeltaPct,
      peerAverageRisk,
      peerAverageValue,
      peerMedianValue,
      similarObjectContracts,
      supplierMatches,
      departmentMatches,
      entityMatches,
      reportSections,
      riskPatterns,
      connections,
    }),
  };
}

export function renderContractDetail(container, contract, detailModel, config) {
  if (!container) {
    return;
  }

  if (!contract) {
    container.innerHTML = `
      <div class="empty-state">
        <strong>Selecciona un contrato de la tabla para generar su análisis.</strong>
        <p>Esta vista recibirá el reporte, el dashboard y el chatbot contextual del contrato seleccionado.</p>
      </div>
    `;
    return;
  }

  const flags = contract.red_flag_details || [];
  const levelClass = RISK_TONES[contract.risk_level] || "medium";
  const secopLink = contract.secop_url
    ? `<a class="button-link" href="${escapeHtml(contract.secop_url)}" target="_blank" rel="noopener">Abrir SECOP</a>`
    : `<span class="button-link disabled">SECOP no disponible</span>`;

  container.innerHTML = `
    <article class="detail-card">
      <div class="detail-header">
        <div>
          <span class="eyebrow eyebrow--soft">Detalle del contrato</span>
          <h3>${escapeHtml(contract.contract_id)}</h3>
          <p class="detail-lead">${escapeHtml(contract.entity || "Sin entidad")} · ${escapeHtml(contract.supplier || "Sin proveedor")}</p>
        </div>
        <div class="detail-actions">
          <button type="button" class="button secondary" data-detail-action="copy-summary">Copiar resumen</button>
          ${secopLink}
        </div>
      </div>

      <div class="detail-score">
        <div class="detail-score__main">
          <span>Score de riesgo</span>
          <strong>${escapeHtml(formatNumber(contract.risk_score))}/100</strong>
        </div>
        <div class="risk-badge ${levelClass}">${escapeHtml(contract.risk_level)}</div>
      </div>

      ${renderDashboardCards(detailModel?.dashboardCards)}

      <div class="detail-grid">
        ${buildField("Entidad", contract.entity || "Sin entidad")}
        ${buildField("Entidad NIT", contract.entity_nit || "Sin NIT")}
        ${buildField("Proveedor", contract.supplier || "Sin proveedor")}
        ${buildField("Proveedor NIT", contract.supplier_nit || "Sin NIT")}
        ${buildField("Modalidad", contract.modality || "Sin modalidad")}
        ${buildField("Departamento", contract.department || "Sin departamento")}
        ${buildField("Municipio", contract.municipality || "Sin municipio")}
        ${buildField("Estado", contract.status || "Sin estado")}
        ${buildField("Fecha", formatDate(contract.start_date))}
        ${buildField("Valor inicial", formatMoney(contract.initial_value))}
        ${buildField("Valor final", formatMoney(contract.final_value))}
        ${buildField("Adicion", formatMoney(contract.addition_value))}
        ${buildField("Plazo", `${formatNumber(contract.duration_days)} dias`)}
      </div>

      <section class="detail-section">
        <h4>Objeto contractual</h4>
        <p>${escapeHtml(contract.object || "Sin descripcion disponible")}</p>
      </section>

      ${renderReportSections(detailModel?.reportSections)}

      ${(contract.audit_score_explanation || contract.score_explanation || contract.explanation)
        ? `
          <section class="detail-section">
            <h4>Explicación del score</h4>
            <p>${escapeHtml(contract.audit_score_explanation || contract.score_explanation || contract.explanation)}</p>
          </section>
        `
        : ""}

      ${contract.required_manual_checks
        ? `
          <section class="detail-section">
            <h4>Checklist recomendado</h4>
            <p>${escapeHtml(contract.required_manual_checks)}</p>
          </section>
        `
        : ""}

      ${contract.limitations || contract.risk_limitations
        ? `
          <section class="detail-section">
            <h4>Limitaciones</h4>
            <p>${escapeHtml(contract.limitations || contract.risk_limitations)}</p>
          </section>
        `
        : ""}

      ${renderAuditEvidence(contract.audit_evidence)}

      ${renderDetailList("Huecos de información", contract.huecos_de_informacion, "No hay huecos de información adicionales en el audit card.")}

      ${renderDetailChips("Documentos a revisar", contract.documentos_a_revisar, "Sin documentos adicionales sugeridos.")}

      <section class="detail-section">
        <h4>Red flags detectadas</h4>
        <div class="flag-list">
          ${flags.length
            ? flags
                .map(
                  (flag) => `
                    <article class="flag-card flag-card--${normalizeText(flag.severity)}">
                      <div class="flag-card__head">
                        <strong>${escapeHtml(flag.name)}</strong>
                        <span>${escapeHtml(flag.severity)}</span>
                      </div>
                      <p>${escapeHtml(flag.description || "Sin descripcion adicional.")}</p>
                      <small><strong>Evidencia:</strong> ${escapeHtml(flag.evidence || "Sin evidencia adicional.")}</small>
                      <small><strong>Recomendación:</strong> ${escapeHtml(buildFlagRecommendation(flag.name, contract))}</small>
                    </article>
                  `,
                )
                .join("")
            : `<div class="empty-state compact"><strong>No se detectaron red flags en este contrato.</strong></div>`}
        </div>
      </section>

      <section class="detail-section">
        <h4>Patrones riesgosos</h4>
        <div class="pattern-list">
          ${detailModel?.riskPatterns?.length
            ? detailModel.riskPatterns
                .map(
                  (pattern) => `
                    <article class="pattern-card pattern-card--${normalizeText(pattern.importance)}">
                      <div class="flag-card__head">
                        <strong>${escapeHtml(pattern.name)}</strong>
                        <span>${escapeHtml(pattern.importance)}</span>
                      </div>
                      <p>${escapeHtml(pattern.description || "Sin descripcion adicional.")}</p>
                      <small><strong>Dato relacionado:</strong> ${escapeHtml(pattern.related_data || "Sin dato")}</small>
                      <small><strong>Acción sugerida:</strong> ${escapeHtml(pattern.action || "Revisar contexto y soportes.")}</small>
                    </article>
                  `,
                )
                .join("")
            : `<div class="empty-state compact"><strong>No hay patrones riesgosos adicionales en el universo filtrado.</strong></div>`}
        </div>
      </section>

      <section class="detail-section">
        <h4>Conexiones y fuentes externas</h4>
        <div class="connection-list">
          ${detailModel?.connections?.length
            ? detailModel.connections
                .map(
                  (connection) => `
                    <article class="connection-card">
                      <div class="flag-card__head">
                        <strong>${escapeHtml(connection.type)}</strong>
                        <span>${escapeHtml(connection.source || "Base local")}</span>
                      </div>
                      <p>${escapeHtml(connection.description || "Conexión pendiente con fuentes externas.")}</p>
                      <small><strong>Estado:</strong> ${escapeHtml(connection.url ? "Disponible" : "Pendiente")}</small>
                      ${connection.url ? `<a class="button-link" href="${escapeHtml(connection.url)}" target="_blank" rel="noopener">Abrir fuente</a>` : ""}
                    </article>
                  `,
                )
                .join("")
            : `<div class="empty-state compact"><strong>Conexión pendiente con fuentes externas.</strong><p>Este espacio será alimentado por el módulo de trazabilidad y datos.</p></div>`}
        </div>
      </section>

      <section class="detail-section">
        <h4>Comparación con contratos similares</h4>
        <div class="peer-grid">
          ${buildField("Promedio score pares", formatNumber(detailModel?.peerAverageRisk || contract.risk_score))}
          ${buildField("Promedio valor pares", formatMoney(detailModel?.peerAverageValue || contract.final_value))}
          ${buildField("Valor mediano pares", formatMoney(detailModel?.peerMedianValue || contract.final_value))}
          ${buildField("Contratos comparables", formatNumber(detailModel?.comparableCount || 0))}
          ${buildField("Delta score", `${detailModel ? formatNumber(detailModel.riskDelta) : "0"}`)}
          ${buildField("Delta valor", detailModel ? `${formatNumber(detailModel.valueDeltaPct)}%` : "0%")}
        </div>
      </section>

      <section class="detail-section">
        <h4>Interpretación para usuario no técnico</h4>
        <p>${escapeHtml(detailModel?.interpretation || buildInterpretation(contract, null))}</p>
      </section>

      <section class="detail-section">
        <h4>Recomendación</h4>
        <div class="recommendation recommendation--${levelClass}">
          <strong>${escapeHtml(detailModel?.recommendation || buildRecommendation(contract))}</strong>
        </div>
      </section>

      <section class="detail-section">
        <h4>Contratos similares</h4>
        <div class="similar-list">
          ${detailModel?.similarContracts?.length
            ? detailModel.similarContracts
                .map(
                  (item) => `
                    <article class="similar-card">
                      <strong>${escapeHtml(item.contract_id)}</strong>
                      <span>${escapeHtml(item.entity || "Sin entidad")}</span>
                      <small>${escapeHtml(formatNumber(item.risk_score))}/100 · ${escapeHtml(item.risk_level)}</small>
                    </article>
                  `,
                )
                .join("")
            : `<div class="empty-state compact"><strong>No hay contratos comparables suficientes.</strong></div>`}
        </div>
      </section>

      <footer class="detail-footer">
        <span class="detail-footer__text">${escapeHtml(detailModel?.summaryText || buildSummaryText(contract))}</span>
      </footer>
    </article>
  `;
}

function buildInterpretation(contract, peers) {
  const flags = contract.red_flag_details.map((flag) => flag.name);
  const flagPhrase = flags.length ? flags.slice(0, 3).join(", ") : "sin red flags relevantes";
  const peerRisk = peers?.peerAverageRisk ?? contract.risk_score;
  const peerValue = peers?.peerAverageValue ?? contract.final_value;
  const valueDelta = peers?.valueDeltaPct ?? 0;
  const riskDelta = peers?.riskDelta ?? 0;
  const peerPhrase = peers?.comparableCount
    ? `En comparacion con ${formatNumber(peers.comparableCount)} contratos similares`
    : "No se encontraron pares suficientes";

  return [
    `Este contrato presenta un score ${contract.risk_level.toLowerCase()} porque combina ${flagPhrase}.`,
    `${peerPhrase}, su score esta ${riskDelta >= 0 ? "por encima" : "por debajo"} del promedio de pares en ${formatNumber(Math.abs(riskDelta))} puntos y su valor final ${valueDelta >= 0 ? "supera" : "esta por debajo de"} el promedio comparativo en ${formatNumber(Math.abs(valueDelta))}%.`,
    "Esto no implica corrupción; solo indica una revisión documental prioritaria para verificar soportes, competencia y modificaciones contractuales.",
  ].join(" ");
}

function buildRecommendation(contract) {
  if (contract.audit_recommendation) {
    return contract.audit_recommendation;
  }

  if (contract.recommended_action) {
    return contract.recommended_action;
  }

  if (contract.risk_score >= 85) {
    return "Prioridad critica para revision documental inmediata.";
  }

  if (contract.risk_score >= 70) {
    return "Revisar manualmente con prioridad alta.";
  }

  if (contract.risk_score >= 40) {
    return "Revisar manualmente y validar documentos de soporte.";
  }

  return "Sin alerta critica, mantener seguimiento normal.";
}

function buildSummaryText(contract) {
  const flags = contract.red_flag_details.map((flag) => flag.name).join(", ") || "sin red flags";
  return `${contract.contract_id} | ${contract.entity || "Sin entidad"} | score ${formatNumber(contract.risk_score)}/100 | ${contract.risk_level} | ${flags}`;
}

function renderDashboardCards(cards) {
  const items = Array.isArray(cards) ? cards : [];
  if (!items.length) {
    return "";
  }

  return `
    <section class="detail-section">
      <h4>Dashboard del contrato</h4>
      <div class="contract-dashboard-grid">
        ${items
          .map(
            (card) => `
              <article class="contract-dashboard-card">
                <span>${escapeHtml(card.label || "Indicador")}</span>
                <strong>${escapeHtml(card.value || "Sin dato")}</strong>
                <p>${escapeHtml(card.note || "")}</p>
              </article>
            `,
          )
          .join("")}
      </div>
    </section>
  `;
}

function renderReportSections(sections) {
  const items = Array.isArray(sections) ? sections : [];
  if (!items.length) {
    return `
      <section class="detail-section">
        <h4>Reporte del contrato</h4>
        <div class="empty-state compact">
          <strong>Reporte en preparación.</strong>
          <p>Esta sección recibirá datos del módulo de análisis y reportes.</p>
        </div>
      </section>
    `;
  }

  return `
    <section class="detail-section">
      <h4>Reporte del contrato</h4>
      <div class="report-section-list">
        ${items
          .map(
            (section) => `
              <article class="report-section">
                <div class="comparison-card__title">${escapeHtml(section.title || "Seccion")}</div>
                <div class="report-section__body">
                  ${(section.paragraphs || [])
                    .map((paragraph) => `<p>${escapeHtml(paragraph)}</p>`)
                    .join("")}
                  ${(section.bullets || []).length
                    ? `<ul class="detail-note-list">${section.bullets.map((bullet) => `<li>${escapeHtml(bullet)}</li>`).join("")}</ul>`
                    : ""}
                </div>
              </article>
            `,
          )
          .join("")}
      </div>
    </section>
  `;
}

function buildDashboardCards(contract, stats) {
  const documentAlerts = normalizeStringValues(contract.huecos_de_informacion).length +
    normalizeStringValues(contract.documentos_a_revisar).length;
  const supplierFrequency = stats.supplierMatches?.length || 0;
  const concentrationLabel = supplierFrequency >= 4 ? "Alta" : supplierFrequency >= 2 ? "Media" : "Baja";
  const redFlagPreview = (contract.red_flag_details || []).map((flag) => flag.name).slice(0, 2).join(" · ");

  return [
    {
      label: "Score de riesgo",
      value: `${formatNumber(contract.risk_score)}/100`,
      note: `Nivel ${contract.risk_level || "Sin nivel"}`,
    },
    {
      label: "Red flags",
      value: formatNumber(contract.red_flag_count || 0),
      note: redFlagPreview || "Sin alertas destacadas",
    },
    {
      label: "Valor del contrato",
      value: formatMoney(contract.final_value),
      note: `Inicial ${formatMoney(contract.initial_value)}`,
    },
    {
      label: "Comparación con similares",
      value: `${formatNumber(stats.riskDelta || 0)} pts`,
      note: `Promedio pares ${formatNumber(stats.peerAverageRisk || contract.risk_score)}/100`,
    },
    {
      label: "Frecuencia del proveedor",
      value: formatNumber(supplierFrequency),
      note: supplierFrequency > 1 ? "Proveedor repetido" : "Sin repeticion visible",
    },
    {
      label: "Nivel de concentración",
      value: concentrationLabel,
      note: `${formatNumber(supplierFrequency)} contratos del mismo proveedor`,
    },
    {
      label: "Estado del contrato",
      value: contract.status || "Sin estado",
      note: contract.modality || "Sin modalidad",
    },
    {
      label: "Alertas documentales",
      value: formatNumber(documentAlerts),
      note: documentAlerts ? "Datos a completar" : "Sin huecos visibles",
    },
  ];
}

function buildReportSections(contract, stats) {
  const redFlagNames = (contract.red_flag_details || []).map((flag) => flag.name).filter(Boolean);
  const redFlagSummary = redFlagNames.length ? redFlagNames.join(", ") : "Sin red flags destacadas";
  const questions = [
    "¿El objeto contractual esta suficientemente soportado en el expediente?",
    "¿La modalidad y el valor son coherentes con el contexto del contrato?",
    "¿La competencia y los oferentes son compatibles con el nivel de riesgo?",
  ];

  return [
    {
      title: "A. Resumen ejecutivo",
      paragraphs: [
        `Que se contrato: ${contract.object || "Sin objeto disponible"}.`,
        `Quien contrato: ${contract.entity || "Sin entidad"} | Proveedor: ${contract.supplier || "Sin proveedor"}.`,
        `Valor del contrato: ${formatMoney(contract.final_value)} | Nivel de riesgo preliminar: ${contract.risk_level || "Sin nivel"} (${formatNumber(contract.risk_score)}/100).`,
      ],
      bullets: [
        `Principales hallazgos: ${redFlagSummary}.`,
        `Departamento y municipio: ${contract.department || "Sin dato"} / ${contract.municipality || "Sin dato"}.`,
        `Modalidad contractual: ${contract.modality || "Sin dato"}.`,
      ],
    },
    {
      title: "B. Analisis tecnico",
      paragraphs: [
        "Indicadores analizados: score de riesgo, red flags, competencia, adiciones, valor comparado, patron del proveedor y conexiones del contrato.",
      ],
      bullets: [
        `Red flags detectadas: ${redFlagSummary}.`,
        `Patrones riesgosos: ${formatNumber(stats.patternCount || 0)} identificados en el universo visible.`,
        `Comparaciones relevantes: promedio de pares ${formatNumber(stats.peerAverageRisk || contract.risk_score)}/100 y delta ${formatNumber(stats.riskDelta || 0)} puntos.`,
        `Limitaciones de los datos: ${contract.limitations || contract.risk_limitations || "No hay limitaciones adicionales registradas."}`,
      ],
    },
    {
      title: "C. Interpretación ciudadana",
      paragraphs: [
        `En palabras sencillas, este contrato merece atencion porque combina un nivel de riesgo ${String(contract.risk_level || "sin nivel").toLowerCase()} con senales preliminares que conviene revisar antes de sacar conclusiones.`,
      ],
      bullets: [
        "Por que puede ser relevante revisarlo: la alerta es preventiva y ayuda a priorizar la revision humana.",
        `Que deberia preguntarse un auditor o ciudadano: ${questions.join(" ")}`,
      ],
    },
    {
      title: "D. Recomendaciones",
      paragraphs: [
        "La salida es preliminar y no afirma corrupción. La revisión debe enfocarse en soportes, competencia, modificaciones y trazabilidad documental.",
      ],
      bullets: [
        "Revisar documentos soporte y anexos del proceso.",
        "Comparar con contratos similares del mismo proveedor, entidad o departamento.",
        "Validar historial del proveedor y posibles repeticiones territoriales.",
        "Revisar adiciones o modificaciones contractuales.",
        "Consultar fuentes externas publicas cuando la integracion este disponible.",
      ],
    },
  ];
}

function buildRiskPatterns(contract, stats) {
  const patterns = [];
  const supplierFrequency = stats.supplierMatches?.length || 0;
  const bidderCount = Number(contract.bidder_count || 0);
  const offerCount = Number(contract.offer_count || 0);
  const additionPercentage = Number(contract.addition_percentage || 0);
  const modificationsCount = Number(contract.modifications_count || 0);
  const additionsCount = Number(contract.additions_count || 0);
  const genericObject = normalizeText(contract.object || "").split(" ").filter(Boolean).length <= 10;
  const sensitiveModality = /contratacion directa|minima cuantia|seleccion abreviada/i.test(contract.modality || "");

  if (bidderCount <= 1 || offerCount <= 1) {
    patterns.push({
      name: "Baja pluralidad de oferentes",
      description: `Solo se observan ${formatNumber(bidderCount || offerCount || 0)} oferentes y ${formatNumber(offerCount)} ofertas en el contrato.`,
      related_data: `Oferentes reportados: ${formatNumber(bidderCount)} | Ofertas: ${formatNumber(offerCount)}`,
      importance: "Alta",
      action: "Revisar actas, competencia y justificacion de la seleccion.",
    });
  }

  if (supplierFrequency > 1) {
    patterns.push({
      name: "Proveedor repetido en la misma region",
      description: `El proveedor aparece en ${formatNumber(supplierFrequency)} contratos del universo visible.`,
      related_data: `Proveedor: ${contract.supplier || "Sin proveedor"}`,
      importance: supplierFrequency >= 4 ? "Alta" : "Media",
      action: "Cruzar historial del proveedor y contratos relacionados.",
    });
  }

  if (Math.abs(stats.valueDeltaPct || 0) >= 20 || Math.abs(stats.riskDelta || 0) >= 10) {
    patterns.push({
      name: "Valor atipico frente a contratos comparables",
      description: `El valor final difiere ${formatNumber(Math.abs(stats.valueDeltaPct || 0))}% del promedio de pares comparables.`,
      related_data: `Promedio pares: ${formatMoney(stats.peerAverageValue || contract.final_value)} | Mediana: ${formatMoney(stats.peerMedianValue || contract.final_value)}`,
      importance: "Media",
      action: "Comparar con contratos homólogos por objeto, modalidad y territorio.",
    });
  }

  if (additionsCount >= 3 || modificationsCount >= 2 || additionPercentage >= 20) {
    patterns.push({
      name: "Modificaciones o adiciones frecuentes",
      description: `Se identifican ${formatNumber(additionsCount)} adiciones y ${formatNumber(modificationsCount)} modificaciones con una variación de ${formatNumber(additionPercentage)}%.`,
      related_data: `Adicion acumulada: ${formatMoney(contract.addition_value)} | Variacion: ${formatNumber(additionPercentage)}%`,
      importance: "Alta",
      action: "Revisar adiciones, prorrogas y documentos de ejecucion.",
    });
  }

  if (genericObject) {
    patterns.push({
      name: "Objeto contractual ambiguo",
      description: "El objeto usa una descripcion corta o generica que puede dificultar la trazabilidad.",
      related_data: `Objeto: ${contract.object || "Sin objeto disponible"}`,
      importance: "Media",
      action: "Solicitar mayor precision sobre entregables, alcance y productos esperados.",
    });
  }

  if (sensitiveModality) {
    patterns.push({
      name: "Modalidad contractual sensible",
      description: `La modalidad ${contract.modality || "sin modalidad"} amerita revision reforzada por su sensibilidad comparativa.`,
      related_data: `Modalidad: ${contract.modality || "Sin modalidad"}`,
      importance: "Media",
      action: "Validar la justificacion juridica y tecnica de la modalidad.",
    });
  }

  const departmentFrequency = stats.departmentMatches?.length || 0;
  if (departmentFrequency > 2) {
    patterns.push({
      name: "Concentracion territorial",
      description: `El mismo departamento acumula ${formatNumber(departmentFrequency)} contratos dentro del universo filtrado.`,
      related_data: `Departamento: ${contract.department || "Sin departamento"}`,
      importance: "Media",
      action: "Revisar acumulacion regional y eventuales patrones de repeticion.",
    });
  }

  return patterns.slice(0, 6);
}

function buildConnections(contract, stats) {
  const supplierCount = Math.max(0, (stats.supplierMatches?.length || 0) - 1);
  const entityCount = Math.max(0, (stats.entityMatches?.length || 0) - 1);
  const departmentCount = Math.max(0, (stats.departmentMatches?.length || 0) - 1);
  const similarCount = Math.max(0, stats.similarObjectContracts?.length || 0);
  const externalSources = contract.secop_url
    ? contract.secop_url
    : "";

  return [
    {
      type: "Otros contratos del mismo proveedor",
      description:
        supplierCount > 0
          ? `${formatNumber(supplierCount)} contratos adicionales del mismo proveedor aparecen en el universo filtrado.`
          : "No se detectaron otros contratos del mismo proveedor en el corte actual.",
      source: "Base local / SECOP II",
      url: externalSources,
    },
    {
      type: "Contratos de la misma entidad",
      description:
        entityCount > 0
          ? `${formatNumber(entityCount)} contratos adicionales de la misma entidad estan disponibles para comparacion.`
          : "No hay contratos adicionales de la misma entidad en el corte actual.",
      source: "Base local / SECOP II",
      url: "",
    },
    {
      type: "Contratos del mismo departamento",
      description:
        departmentCount > 0
          ? `${formatNumber(departmentCount)} contratos adicionales del mismo departamento sirven como referencia.`
          : "No hay suficientes contratos del mismo departamento en el corte actual.",
      source: "Base local / SECOP II",
      url: "",
    },
    {
      type: "Objetos similares",
      description:
        similarCount > 0
          ? `${formatNumber(similarCount)} contratos similares apoyan la comparacion de valor, competencia y red flags.`
          : "No se identificaron objetos comparables suficientes.",
      source: "Base local",
      url: "",
    },
    {
      type: "Fuentes externas",
      description:
        "SECOP II, Datos Abiertos Colombia, Colombia Compra Eficiente, RUES, Contraloria y Procuraduria quedan listados para integracion progresiva.",
      source: "Pendiente de integracion",
      url: "",
    },
  ];
}

function buildChatbotContext(contract, stats) {
  return {
    summary: `Contrato ${contract.contract_id}: ${contract.entity || "Sin entidad"} contrató ${contract.object || "sin objeto disponible"} con ${contract.supplier || "sin proveedor"}. Score ${formatNumber(contract.risk_score)}/100 (${contract.risk_level || "sin nivel"}).`,
    limitations: contract.limitations || contract.risk_limitations || "No hay limitaciones adicionales registradas en los datos cargados.",
    key_points: [
      `Red flags: ${(contract.red_flag_details || []).map((flag) => flag.name).join(", ") || "sin alertas destacadas"}.`,
      `Comparación: promedio de pares ${formatNumber(stats.peerAverageRisk || contract.risk_score)}/100 y delta ${formatNumber(stats.riskDelta || 0)} puntos.`,
      `Conexiones: ${(stats.connections || []).length || 0} elementos visibles en la sección de trazabilidad.`,
    ],
    questions: [
      "Explícame este informe en palabras sencillas.",
      "¿Por qué este contrato tiene este score?",
      "¿Qué red flag es más importante?",
      "¿Qué debería revisar primero?",
      "¿Este proveedor aparece en otros contratos?",
      "¿Qué datos faltan para mejorar el análisis?",
    ],
  };
}

function buildSimilarObjectContracts(contract, records, peerGroup) {
  if (Array.isArray(peerGroup) && peerGroup.length) {
    return peerGroup.slice(0, 3);
  }

  const baseTokens = tokenizeText(contract.object || "");
  if (!baseTokens.length) {
    return [];
  }

  return records
    .filter((record) => {
      if (record.contract_id === contract.contract_id) {
        return false;
      }

      const candidateTokens = tokenizeText(record.object || "");
      if (!candidateTokens.length) {
        return false;
      }

      return candidateTokens.some((token) => baseTokens.includes(token));
    })
    .slice(0, 3);
}

function buildFlagRecommendation(flagName, contract) {
  const normalized = normalizeText(flagName);

  if (normalized.includes("pluralidad") || normalized.includes("oferente")) {
    return "Revisar documentos del proceso, actas de evaluacion y justificacion de la competencia.";
  }

  if (normalized.includes("proveedor")) {
    return "Cruzar historial del proveedor, contratos relacionados y concentración regional.";
  }

  if (normalized.includes("valor")) {
    return "Comparar el valor con contratos homólogos y verificar la razón de la variación.";
  }

  if (normalized.includes("modificacion") || normalized.includes("adicion")) {
    return "Revisar adiciones, prorrogas, modificaciones y soportes de ejecucion.";
  }

  if (normalized.includes("modalidad")) {
    return "Validar la justificacion juridica y tecnica de la modalidad usada.";
  }

  if (normalized.includes("plazo")) {
    return "Verificar cronograma, entregables y consistencia del plazo con el objeto contractual.";
  }

  if (normalized.includes("documental") || normalized.includes("incompleta")) {
    return "Solicitar anexos, soportes y documentos faltantes antes de concluir.";
  }

  if (normalized.includes("objeto")) {
    return "Solicitar mayor precision del objeto contractual y sus entregables.";
  }

  if (normalized.includes("conexion")) {
    return "Cruzar contratos, proveedores y fuentes externas antes de cerrar la revision.";
  }

  return contract.limitations || contract.risk_limitations || "Revisar el caso con soporte documental completo.";
}

function buildField(label, value) {
  return `
    <div class="detail-field">
      <span>${escapeHtml(label)}</span>
      <strong>${escapeHtml(value)}</strong>
    </div>
  `;
}

function renderAuditEvidence(evidence) {
  if (!evidence || typeof evidence !== "object") {
    return "";
  }

  const fields = [
    ["Proceso SECOP", evidence.process_id],
    ["Oferentes reportados", evidence.num_oferentes_reported],
    ["Filas de oferentes", evidence.bidder_rows],
    ["Proveedores unicos", evidence.unique_suppliers],
    ["Contratos proveedor/entidad", evidence.supplier_total_contracts],
    ["Participacion proveedor", formatPercent(Number(evidence.supplier_share_in_entity || 0) * 100)],
    ["Percentil de valor", formatPercent(Number(evidence.value_percentile_year || 0) * 100)],
    ["Ratio estimado/adjudicado", evidence.estimated_vs_awarded_ratio === null || evidence.estimated_vs_awarded_ratio === undefined ? "Sin dato" : formatNumber(evidence.estimated_vs_awarded_ratio)],
  ].filter(([, value]) => value !== undefined && value !== null && value !== "");

  if (!fields.length) {
    return "";
  }

  return `
    <section class="detail-section">
      <h4>Evidencia estructurada</h4>
      <div class="evidence-grid">
        ${fields.map(([label, value]) => buildEvidenceField(label, value)).join("")}
      </div>
    </section>
  `;
}

function renderDetailList(title, values, emptyText) {
  const items = normalizeStringValues(values);
  if (!items.length) {
    return `
      <section class="detail-section">
        <h4>${escapeHtml(title)}</h4>
        <div class="empty-state compact"><strong>${escapeHtml(emptyText)}</strong></div>
      </section>
    `;
  }

  return `
    <section class="detail-section">
      <h4>${escapeHtml(title)}</h4>
      <ul class="detail-note-list">
        ${items.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}
      </ul>
    </section>
  `;
}

function renderDetailChips(title, values, emptyText) {
  const items = normalizeStringValues(values);
  if (!items.length) {
    return `
      <section class="detail-section">
        <h4>${escapeHtml(title)}</h4>
        <div class="empty-state compact"><strong>${escapeHtml(emptyText)}</strong></div>
      </section>
    `;
  }

  return `
    <section class="detail-section">
      <h4>${escapeHtml(title)}</h4>
      <div class="detail-chip-list">
        ${items.map((item) => `<span class="detail-chip">${escapeHtml(item)}</span>`).join("")}
      </div>
    </section>
  `;
}

function buildEvidenceField(label, value) {
  return `
    <div class="evidence-card">
      <span>${escapeHtml(label)}</span>
      <strong>${escapeHtml(formatEvidenceValue(value))}</strong>
    </div>
  `;
}

function formatEvidenceValue(value) {
  if (value === null || value === undefined || value === "") {
    return "Sin dato";
  }

  if (typeof value === "number") {
    if (Number.isInteger(value)) {
      return formatNumber(value);
    }

    return new Intl.NumberFormat("es-CO", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value);
  }

  return String(value);
}

function normalizeStringValues(values) {
  if (Array.isArray(values)) {
    return values.map((item) => String(item || "").trim()).filter(Boolean);
  }

  if (typeof values === "string") {
    return values
      .split(/\s*\|\s*|\s*;\s*|\s*,\s*/)
      .map((item) => String(item || "").trim())
      .filter(Boolean);
  }

  return [];
}
