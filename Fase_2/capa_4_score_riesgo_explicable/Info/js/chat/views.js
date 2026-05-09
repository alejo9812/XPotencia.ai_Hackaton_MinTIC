import {
  escapeHtml,
  formatDate,
  formatDateTime,
  formatMoney,
  formatNumber,
  formatPercent,
  normalizeText,
} from "../utils.js";

const VIEW_TITLES = {
  project_overview: "Resumen del proyecto",
  risk_contracts_table: "Contratos con mayor riesgo",
  contract_detail: "Detalle del contrato",
  red_flags_panel: "Red flags",
  supplier_comparison: "Comparación de proveedores",
  entity_comparison: "Comparación de entidades",
  region_summary: "Resumen territorial",
  report_preview: "Vista previa del reporte",
  followup_panel: "Panel de seguimiento",
  dashboard_summary: "Resumen del dashboard",
  unknown_query: "Ayuda inicial",
};

export function getViewTitle(viewType, response = {}) {
  return response?.data?.title || response?.data?.report_title || VIEW_TITLES[viewType] || "Vista dinámica";
}

export function renderMainView(response) {
  if (!response) {
    return renderEmptyView("No hay respuesta para renderizar todavía.");
  }

  const viewType = response.view_type || "project_overview";
  const title = getViewTitle(viewType, response);
  const content = renderBody(viewType, response);
  const actions = renderSuggestedActions(response.suggested_actions || []);
  const footer = response.limitations
    ? `
      <div class="view-footer">
        <strong>Limitaciones:</strong> ${escapeHtml(response.limitations)}
      </div>
    `
    : "";

  return `
    <div class="view-shell" data-view-type="${escapeHtml(viewType)}">
      <div class="view-header">
        <div>
          <h3 class="view-title">${escapeHtml(title)}</h3>
          <p class="view-subtitle">${escapeHtml(response.message || "")}</p>
        </div>
        <div class="view-meta">
          ${buildMetaPills(response)}
        </div>
      </div>
      ${content}
      ${actions}
      ${footer}
    </div>
  `;
}

export function renderLoadingView(message = "Procesando consulta...") {
  return `
    <div class="view-shell">
      <div class="view-header">
        <div>
          <h3 class="view-title">${escapeHtml(message)}</h3>
          <p class="view-subtitle">El asistente está leyendo la intención, consultando caché y preparando la vista más útil.</p>
        </div>
        <div class="view-meta">
          <span class="status-pill">Analizando</span>
        </div>
      </div>
      <div class="summary-grid">
        ${Array.from({ length: 3 }, () => '<div class="skeleton skeleton--card"></div>').join("")}
      </div>
      <div class="skeleton skeleton--line"></div>
      <div class="skeleton skeleton--line"></div>
    </div>
  `;
}

function renderBody(viewType, response) {
  switch (viewType) {
    case "project_overview":
    case "unknown_query":
      return renderProjectOverview(response.data || {}, response);
    case "risk_contracts_table":
      return renderRiskContractsTable(response.data || {});
    case "contract_detail":
      return renderContractDetail(response.data || {});
    case "red_flags_panel":
      return renderRedFlagsPanel(response.data || {});
    case "supplier_comparison":
    case "entity_comparison":
    case "region_summary":
      return renderComparisonView(response.data || {}, viewType);
    case "report_preview":
      return renderReportPreview(response.data || {});
    case "followup_panel":
      return renderFollowupPanel(response.data || {});
    case "dashboard_summary":
      return renderDashboardSummary(response.data || {});
    default:
      return renderEmptyView("No hay una plantilla disponible para esta vista.");
  }
}

function renderProjectOverview(data, response) {
  const metrics = data.metrics || {};
  const quickActions = Array.isArray(data.quick_actions) ? data.quick_actions : [];
  const topContracts = Array.isArray(data.top_contracts) ? data.top_contracts : [];
  const riskDistribution = Array.isArray(data.risk_distribution) ? data.risk_distribution : [];

  return `
    <div class="view-grid">
      <section class="section">
        <div class="section__title">
          <div>
            <span class="badge badge--muted">Inicio</span>
            <h4 class="panel-title">Chat-first para el análisis de opacidad contractual</h4>
          </div>
          <div class="panel-meta">
            <span class="status-pill">${escapeHtml(data.author || "Alejandro Montes")}</span>
            <span class="status-pill">${escapeHtml(data.source_label || "Fuente local")}</span>
          </div>
        </div>
        <div class="section__body">
          ${renderMetricsGrid([
            { label: "Contratos", value: formatNumber(metrics.total_contracts || 0), note: "Universo visible y precalculado" },
            { label: "Riesgo promedio", value: `${formatNumber(metrics.average_risk || 0)}/100`, note: "Score promedio de la sesión" },
            { label: "Riesgo alto", value: formatNumber(metrics.high_risk_contracts || 0), note: "Prioridad de revisión" },
            { label: "Red flags", value: formatNumber(metrics.total_red_flags || 0), note: "Señales activas en el universo" },
            { label: "Valor total", value: formatMoney(metrics.total_value || 0), note: "Suma agregada de contratos" },
          ])}
        </div>
      </section>

      ${renderDecisionSupportPanel(data.decision_support)}

      <section class="section">
        <div class="section__title">
          <div>
            <h4 class="panel-title">Acciones rápidas</h4>
            <p class="panel-subtitle">Botones listos para iniciar una conversación útil.</p>
          </div>
        </div>
        <div class="chip-row">
          ${quickActions
            .map(
              (action) => `
                <button
                  class="chip chip--primary"
                  type="button"
                  data-chat-action="send-query"
                  data-chat-query="${escapeHtml(action.query || action.label || "")}"
                >
                  ${escapeHtml(action.label || action.query || "")}
                </button>
              `,
            )
            .join("")}
        </div>
      </section>

      <section class="section">
        <div class="section__title">
          <div>
            <h4 class="panel-title">Contratos más riesgosos</h4>
            <p class="panel-subtitle">Top 5 de la caché precalculada.</p>
          </div>
        </div>
        ${renderTopContractsTable(topContracts, { compact: true })}
      </section>

      <section class="section">
        <div class="section__title">
          <div>
            <h4 class="panel-title">Distribución preliminar</h4>
            <p class="panel-subtitle">La barra visual resume la concentración por nivel de riesgo.</p>
          </div>
        </div>
        <div class="summary-stack">
          ${riskDistribution
            .map(
              (item) => `
                <div class="comparison-card">
                  <div class="comparison-card__head">
                    <span class="comparison-card__title">${escapeHtml(item.label)}</span>
                    <span class="badge badge--muted">${formatNumber(item.count || 0)} · ${formatPercent(item.percent || 0, 1)}</span>
                  </div>
                  <div class="bar-track" aria-hidden="true">
                    <div class="bar-fill" style="width:${Math.max(4, Number(item.percent || 0))}%; background:${escapeHtml(item.color || "#2dd4bf")}"></div>
                  </div>
                </div>
              `,
            )
            .join("")}
        </div>
      </section>

      <section class="section">
        <div class="section__title">
          <div>
            <h4 class="panel-title">Metodología y cautelas</h4>
            <p class="panel-subtitle">La explicación evita inventar información cuando los datos no están completos.</p>
          </div>
        </div>
        <div class="section__body">
          ${renderBulletPanel("Fuentes de datos", data.data_sources || [])}
          ${renderBulletPanel("Cómo responde el sistema", data.methodology || [])}
          ${renderBulletPanel("Advertencias", data.warnings || [])}
        </div>
      </section>
    </div>
  `;
}

function renderRiskContractsTable(data) {
  const contracts = Array.isArray(data.contracts) ? data.contracts : [];
  if (!contracts.length) {
    return renderEmptyView(data.query ? `No encontré contratos para “${data.query}”.` : "No hay contratos disponibles con esos filtros.");
  }

  return `
    <div class="view-grid">
      <section class="section">
        <div class="section__title">
          <div>
            <span class="badge badge--muted">${escapeHtml(data.title || "Resultados de búsqueda")}</span>
            <h4 class="panel-title">Tabla de contratos ordenada por riesgo</h4>
            <p class="panel-subtitle">${escapeHtml(data.query || "Consulta alimentada por caché y filtros estructurados.")}</p>
          </div>
          <div class="panel-meta">
            <span class="status-pill">${formatNumber(contracts.length)} resultados</span>
            ${renderFilterPillList(data.filters || {})}
          </div>
        </div>
        ${renderDecisionSupportPanel(data.decision_support)}
        ${renderContractsTable(contracts)}
      </section>
    </div>
  `;
}

function renderContractDetail(data) {
  const contract = data.contract || {};
  if (!contract.contract_id) {
    return renderEmptyView("No tengo el contrato suficiente para construir la ficha.");
  }

  const redFlags = Array.isArray(data.red_flags) ? data.red_flags : [];
  const peerSummary = data.peer_summary || {};
  const analysis = data.analysis || {};
  const score = Number(contract.risk_score || 0);
  const riskClass = riskLevelClass(contract.risk_level || score);
  const riskLabel = formatRiskLabel(contract.risk_level || score);

  return `
    <div class="view-grid">
      <section class="detail-card">
        <div class="section__title">
          <div>
            <span class="badge badge--muted">Contrato</span>
            <h4 class="panel-title">${escapeHtml(contract.contract_id)}</h4>
            <p class="panel-subtitle">${escapeHtml([contract.entity, contract.supplier, contract.department, contract.municipality].filter(Boolean).join(" · "))}</p>
          </div>
          <div class="score-box">
            <span class="score-box__label">Score de riesgo</span>
            <strong class="score-box__value">${formatNumber(score)}</strong>
            <span class="badge ${riskClass}">${escapeHtml(riskLabel)}</span>
          </div>
        </div>

        <div class="key-value-grid">
          ${renderKeyValue("Entidad", contract.entity || "Sin dato")}
          ${renderKeyValue("Proveedor", contract.supplier || "Sin dato")}
          ${renderKeyValue("Departamento", contract.department || "Sin dato")}
          ${renderKeyValue("Municipio", contract.municipality || "Sin dato")}
          ${renderKeyValue("Modalidad", contract.modality || "Sin dato")}
          ${renderKeyValue("Estado", contract.status || "Sin dato")}
          ${renderKeyValue("Valor inicial", formatMoney(contract.initial_value || 0))}
          ${renderKeyValue("Valor final", formatMoney(contract.final_value || 0))}
          ${renderKeyValue("Fecha inicio", formatDate(contract.start_date))}
          ${renderKeyValue("Fecha fin", formatDate(contract.end_date))}
          ${renderKeyValue("Red flags", formatNumber(contract.red_flag_count || redFlags.length || 0))}
          ${renderKeyValue("Limitaciones", contract.limitations || "Sin limitaciones adicionales")}
        </div>

        <div class="callout">
          <strong>Objeto contractual</strong>
          <p class="panel-subtitle">${escapeHtml(contract.object || "Sin descripción disponible.")}</p>
        </div>

        ${analysis.explanation ? renderAnalysisBlock("Explicación del riesgo", analysis.explanation) : ""}
        ${analysis.summary ? renderAnalysisBlock("Resumen del análisis", analysis.summary) : ""}
        ${analysis.recommendations?.length ? renderBulletPanel("Recomendaciones", analysis.recommendations) : ""}
        ${analysis.audit_questions?.length ? renderBulletPanel("Preguntas de auditoría", analysis.audit_questions) : ""}
        ${renderDecisionSupportPanel(data.decision_support)}

        <div class="summary-grid">
          <div class="comparison-card">
            <div class="comparison-card__head">
              <span class="comparison-card__title">Contexto de pares</span>
              <span class="badge badge--muted">${formatNumber(peerSummary.peer_count || 0)} comparables</span>
            </div>
            <div class="comparison-card__body">
              Promedio de riesgo: <strong>${formatNumber(peerSummary.peer_average_risk || 0)}/100</strong><br />
              Promedio de valor: <strong>${formatMoney(peerSummary.peer_average_value || 0)}</strong>
            </div>
          </div>
          <div class="comparison-card">
            <div class="comparison-card__head">
              <span class="comparison-card__title">Evidencia y soporte</span>
            </div>
            <div class="comparison-card__body">
              ${escapeHtml(contract.audit_recommendation || contract.recommended_action || "Sin recomendación adicional.")}
            </div>
          </div>
        </div>
      </section>

      <section class="section">
        <div class="section__title">
          <div>
            <h4 class="panel-title">Red flags detectadas</h4>
            <p class="panel-subtitle">Cada señal se muestra con su evidencia asociada.</p>
          </div>
        </div>
        ${renderDecisionSupportPanel(data.decision_support)}
        ${renderFlagsList(redFlags, contract)}
      </section>

      <section class="section">
        <div class="section__title">
          <div>
            <h4 class="panel-title">Contratos comparables</h4>
            <p class="panel-subtitle">Pares cercanos por entidad, proveedor o territorio.</p>
          </div>
        </div>
        ${renderTopContractsTable(peerSummary.peer_top_contracts || [], { compact: true, allowContractActions: true })}
      </section>
    </div>
  `;
}

function renderRedFlagsPanel(data) {
  const flags = Array.isArray(data.flags) ? data.flags : [];
  if (!flags.length) {
    return renderEmptyView(data.headline || "No hay red flags activas para mostrar.");
  }

  const isContractScope = data.scope === "contract";
  return `
    <div class="view-grid">
      <section class="section">
        <div class="section__title">
          <div>
            <span class="badge badge--muted">${escapeHtml(isContractScope ? "Contrato" : "Universo PAE")}</span>
            <h4 class="panel-title">${escapeHtml(data.headline || "Red flags")}</h4>
          </div>
          ${isContractScope && data.contract?.contract_id ? `<div class="panel-meta"><span class="status-pill">${escapeHtml(data.contract.contract_id)}</span></div>` : ""}
        </div>
        ${isContractScope ? renderFlagsList(flags, data.contract || {}) : renderFlagSummaryTable(flags)}
      </section>
    </div>
  `;
}

function renderComparisonView(data, viewType) {
  const rows = Array.isArray(data.rows) ? data.rows : [];
  const mode = data.mode || viewType.replace("_comparison", "");
  if (!rows.length) {
    return renderEmptyView("No hay suficientes grupos para una comparación útil.");
  }

  const maxCount = Math.max(...rows.map((row) => Number(row.contract_count || 0)), 1);
  const maxRisk = Math.max(...rows.map((row) => Number(row.average_risk || 0)), 1);

  return `
    <div class="view-grid">
      <section class="section">
        <div class="section__title">
          <div>
            <span class="badge badge--muted">${escapeHtml(labelForMode(mode))}</span>
            <h4 class="panel-title">${escapeHtml(data.headline || "Comparación")}</h4>
            <p class="panel-subtitle">Se priorizan métricas agregadas para evitar cargar detalles pesados.</p>
          </div>
          <div class="panel-meta">
            ${renderFilterPillList(data.filters || {})}
          </div>
        </div>
        ${renderDecisionSupportPanel(data.decision_support)}

        <div class="comparison-list">
          ${rows
            .map((row) => {
              const width = Math.max(4, Math.round((Number(row.contract_count || 0) / maxCount) * 100));
              const riskWidth = Math.max(4, Math.round((Number(row.average_risk || 0) / maxRisk) * 100));
              return `
                <article class="comparison-card">
                  <div class="comparison-card__head">
                    <div>
                      <div class="comparison-card__title">${escapeHtml(row.name || "Sin dato")}</div>
                      <div class="data-table__subtle">${escapeHtml((row.entities || row.suppliers || row.departments || []).join(", ") || "Sin detalles complementarios")}</div>
                    </div>
                    <span class="badge badge--muted">${formatNumber(row.contract_count || 0)} contratos</span>
                  </div>
                  <div class="comparison-card__body">
                    <div class="key-value-grid">
                      ${renderKeyValue("Valor total", formatMoney(row.total_value || 0))}
                      ${renderKeyValue("Riesgo promedio", `${formatNumber(row.average_risk || 0)}/100`)}
                      ${renderKeyValue("Red flags", formatNumber(row.red_flag_count || 0))}
                      ${renderKeyValue("Entidades", formatNumber((row.entities || []).length || 0))}
                    </div>
                    <div style="margin-top:12px" class="summary-stack">
                      <div>
                        <div class="data-table__subtle">Contratos</div>
                        <div class="bar-track"><div class="bar-fill" style="width:${width}%;"></div></div>
                      </div>
                      <div>
                        <div class="data-table__subtle">Riesgo promedio</div>
                        <div class="bar-track"><div class="bar-fill bar-fill--warn" style="width:${riskWidth}%;"></div></div>
                      </div>
                    </div>
                    ${row.top_flags?.length ? renderBulletPanel("Red flags frecuentes", row.top_flags.map((flag) => `${flag.code || flag.label} · ${flag.count || 0}`)) : ""}
                    ${row.top_contracts?.length ? renderTopContractsTable(row.top_contracts.slice(0, 3), { compact: true, allowContractActions: true }) : ""}
                  </div>
                </article>
              `;
            })
            .join("")}
        </div>
      </section>
    </div>
  `;
}

function renderReportPreview(data) {
  const sections = Array.isArray(data.sections) ? data.sections : [];
  const exportPayload = data.export || {};
  if (!sections.length && !data.summary) {
    return renderEmptyView("No hay información suficiente para la vista previa del reporte.");
  }

  return `
    <div class="view-grid">
      <section class="report-card">
        <div class="section__title">
          <div>
            <span class="badge badge--muted">${escapeHtml(data.report_type || "executive")}</span>
            <h4 class="panel-title">${escapeHtml(data.title || "Vista previa del reporte")}</h4>
            <p class="panel-subtitle">${escapeHtml(data.scope_label || "Reporte provisional basado en la sesión actual.")}</p>
          </div>
          <div class="report-export">
            ${
              exportPayload.available
                ? `
                  <button class="button primary" type="button" data-chat-action="export-report" data-export-format="markdown">
                    Exportar reporte
                  </button>
                `
                : ""
            }
          </div>
        </div>

        <div class="summary-grid">
          ${data.highlights?.length ? data.highlights.slice(0, 3).map((item) => renderMiniHighlight(item)).join("") : ""}
        </div>

        ${renderDecisionSupportPanel(data.decision_support)}

        ${data.summary ? renderAnalysisBlock("Resumen del reporte", data.summary) : ""}

        <div class="report-sections">
          ${sections
            .map((section) => {
              const paragraphs = Array.isArray(section.paragraphs) ? section.paragraphs : [];
              const bullets = Array.isArray(section.bullets) ? section.bullets : [];
              return `
                <article class="report-section">
                  <div class="comparison-card__title">${escapeHtml(section.title || "Sección")}</div>
                  <div class="report-section__body">
                    ${paragraphs.map((item) => `<p>${escapeHtml(item)}</p>`).join("")}
                    ${bullets.length ? `<ul class="bullet-list">${bullets.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>` : ""}
                  </div>
                </article>
              `;
            })
            .join("")}
        </div>
      </section>
    </div>
  `;
}

function renderFollowupPanel(data) {
  const items = Array.isArray(data.followups) ? data.followups : [];
  if (!items.length) {
    return renderEmptyView(data.headline || "Todavía no hay seguimientos guardados en esta sesión.");
  }

  return `
    <div class="view-grid">
      <section class="section">
        <div class="section__title">
          <div>
            <span class="badge badge--muted">Seguimiento</span>
            <h4 class="panel-title">${escapeHtml(data.headline || "Panel de seguimiento")}</h4>
            <p class="panel-subtitle">Los contratos marcados quedan en memoria mínima de sesión.</p>
          </div>
          <div class="panel-meta">
            <span class="status-pill">Total: ${formatNumber(data.summary?.total || items.length)}</span>
            <span class="status-pill">Abiertos: ${formatNumber(data.summary?.open || 0)}</span>
            <span class="status-pill">Cerrados: ${formatNumber(data.summary?.closed || 0)}</span>
          </div>
        </div>

        <div class="followup-list">
          ${items
            .map(
              (item) => `
                <article class="followup-item">
                  <div class="followup-item__head">
                    <div>
                      <div class="timeline-item__title">${escapeHtml(item.contract_id || "Sin contrato")}</div>
                      <div class="timeline-item__meta">${escapeHtml([item.entity, item.supplier, item.department].filter(Boolean).join(" · ") || "Sin contexto adicional")}</div>
                    </div>
                    <span class="badge ${statusClass(item.status)}">${escapeHtml(item.status || "Pendiente")}</span>
                  </div>
                  <div class="followup-item__body">
                    <p>${escapeHtml(item.notes || "Sin observaciones adicionales.")}</p>
                    <p><strong>Próxima acción:</strong> ${escapeHtml(item.next_action || data.next_action || "Revisar soportes y trazabilidad.")}</p>
                    ${item.created_at ? `<p class="data-table__subtle">Creado: ${escapeHtml(formatDateTime(item.created_at))}</p>` : ""}
                  </div>
                  <div class="data-table__actions">
                    ${
                      item.contract_id
                        ? `
                          <button class="button secondary" type="button" data-chat-action="send-query" data-chat-query="Analiza el contrato ${escapeHtml(item.contract_id)}">
                            Ver detalle
                          </button>
                          <button class="button secondary" type="button" data-chat-action="send-query" data-chat-query="Genera un reporte ejecutivo del contrato ${escapeHtml(item.contract_id)}">
                            Generar reporte
                          </button>
                        `
                        : ""
                    }
                  </div>
                </article>
              `,
            )
            .join("")}
        </div>
      </section>
    </div>
  `;
}

function renderDashboardSummary(data) {
  const metrics = data.metrics || {};
  const riskDistribution = Array.isArray(data.risk_distribution) ? data.risk_distribution : [];
  const topFlags = Array.isArray(data.top_flags) ? data.top_flags : [];
  const topContracts = Array.isArray(data.top_contracts) ? data.top_contracts : [];
  const topSuppliers = Array.isArray(data.top_suppliers) ? data.top_suppliers : [];
  const topEntities = Array.isArray(data.top_entities) ? data.top_entities : [];
  const topTerritories = Array.isArray(data.top_territories) ? data.top_territories : [];

  return `
    <div class="view-grid">
      <section class="section">
        <div class="section__title">
          <div>
            <span class="badge badge--muted">Dashboard</span>
            <h4 class="panel-title">Resumen ejecutivo del universo visible</h4>
            <p class="panel-subtitle">${escapeHtml(data.interpretation || "Resumen analítico preliminar.")}</p>
          </div>
          <div class="panel-meta">
            ${renderFilterPillList(data.filters || {})}
          </div>
        </div>
        ${renderMetricsGrid([
          { label: "Contratos", value: formatNumber(metrics.total_contracts || 0), note: "Base visible del dashboard" },
          { label: "Riesgo promedio", value: `${formatNumber(metrics.average_risk || 0)}/100`, note: "Promedio de score" },
          { label: "Riesgo alto", value: formatNumber(metrics.high_risk_contracts || 0), note: "Contratos de prioridad alta" },
          { label: "Red flags", value: formatNumber(metrics.total_red_flags || 0), note: "Señales activas" },
          { label: "Valor total", value: formatMoney(metrics.total_value || 0), note: "Suma agregada" },
        ])}
        ${renderDecisionSupportPanel(data.decision_support)}
      </section>

      <section class="section">
        <div class="section__title">
          <div>
            <h4 class="panel-title">Distribución de riesgo</h4>
          </div>
        </div>
        <div class="summary-stack">
          ${riskDistribution
            .map(
              (item) => `
                <div class="comparison-card">
                  <div class="comparison-card__head">
                    <span class="comparison-card__title">${escapeHtml(item.label)}</span>
                    <span class="badge badge--muted">${formatNumber(item.count || 0)} contratos</span>
                  </div>
                  <div class="bar-track"><div class="bar-fill" style="width:${Math.max(4, Number(item.percent || 0))}%; background:${escapeHtml(item.color || "#2dd4bf")}"></div></div>
                </div>
              `,
            )
            .join("")}
        </div>
      </section>

      <section class="section">
        <div class="section__title"><div><h4 class="panel-title">Top flags</h4></div></div>
        ${renderFlagSummaryTable(topFlags)}
      </section>

      <section class="section">
        <div class="section__title"><div><h4 class="panel-title">Contratos más riesgosos</h4></div></div>
        ${renderTopContractsTable(topContracts)}
      </section>

      <section class="section">
        <div class="section__title"><div><h4 class="panel-title">Top proveedores, entidades y territorios</h4></div></div>
        <div class="comparison-list">
          ${renderComparisonMiniCard("Proveedores", topSuppliers)}
          ${renderComparisonMiniCard("Entidades", topEntities)}
          ${renderComparisonMiniCard("Territorios", topTerritories)}
        </div>
      </section>
    </div>
  `;
}

function renderComparisonMiniCard(title, rows) {
  if (!rows || !rows.length) {
    return `
      <article class="comparison-card">
        <div class="comparison-card__title">${escapeHtml(title)}</div>
        <div class="comparison-card__body">Sin datos suficientes.</div>
      </article>
    `;
  }

  return `
    <article class="comparison-card">
      <div class="comparison-card__head">
        <span class="comparison-card__title">${escapeHtml(title)}</span>
        <span class="badge badge--muted">${formatNumber(rows.length)} grupos</span>
      </div>
      <div class="comparison-card__body">
        ${rows
          .slice(0, 3)
          .map(
            (row) => `
              <div class="summary-stack" style="margin-bottom:10px">
                <div class="comparison-card__head">
                  <span>${escapeHtml(row.name || row.label || "Sin dato")}</span>
                  <span class="badge badge--muted">${formatNumber(row.contract_count || row.count || 0)}</span>
                </div>
                <div class="bar-track"><div class="bar-fill bar-fill--warn" style="width:${Math.max(4, Number(row.average_risk || 0) || Number(row.value || 0))}%"></div></div>
              </div>
            `,
          )
          .join("")}
      </div>
    </article>
  `;
}

function renderContractsTable(contracts, options = {}) {
  const compact = Boolean(options.compact);
  return `
    <div class="table-shell">
      <table class="data-table" aria-label="Tabla de contratos">
        <thead>
          <tr>
            <th>ID</th>
            <th>Entidad</th>
            <th>Proveedor</th>
            <th>Valor</th>
            <th>Región</th>
            <th>Score</th>
            <th>Red flags</th>
            <th>Acciones</th>
          </tr>
        </thead>
        <tbody>
          ${contracts
            .map((contract) => {
              const queryId = `Analiza el contrato ${contract.contract_id}`;
              const queryReport = `Genera un reporte ejecutivo del contrato ${contract.contract_id}`;
              const queryFollowup = `Crear seguimiento del contrato ${contract.contract_id}`;
              return `
                <tr>
                  <td>
                    <strong>${escapeHtml(contract.contract_id || "Sin ID")}</strong>
                    <div class="data-table__subtle">${escapeHtml(contract.summary_line || contract.summary || "")}</div>
                  </td>
                  <td>
                    <strong>${escapeHtml(contract.entity || "Sin entidad")}</strong>
                    <div class="data-table__subtle">${escapeHtml(contract.status || "Sin estado")}</div>
                  </td>
                  <td>
                    <strong>${escapeHtml(contract.supplier || "Sin proveedor")}</strong>
                    <div class="data-table__subtle">${escapeHtml(contract.modality || "Sin modalidad")}</div>
                  </td>
                  <td>
                    <strong>${formatMoney(contract.final_value || 0)}</strong>
                    <div class="data-table__subtle">Inicial ${formatMoney(contract.initial_value || 0)}</div>
                  </td>
                  <td>
                    <strong>${escapeHtml(contract.department || "Sin departamento")}</strong>
                    <div class="data-table__subtle">${escapeHtml(contract.municipality || "Sin municipio")}</div>
                  </td>
                  <td><span class="badge ${riskLevelClass(contract.risk_level || contract.risk_score)}">${formatNumber(contract.risk_score || 0)}</span></td>
                  <td>${renderFlagChips(contract.red_flags || [])}</td>
                  <td>
                    <div class="data-table__actions">
                      <button class="button secondary" type="button" data-chat-action="send-query" data-chat-query="${escapeHtml(queryId)}">Ver detalle</button>
                      <button class="button secondary" type="button" data-chat-action="send-query" data-chat-query="${escapeHtml(queryReport)}">Reporte</button>
                      <button class="button secondary" type="button" data-chat-action="send-query" data-chat-query="${escapeHtml(queryFollowup)}">Seguimiento</button>
                    </div>
                  </td>
                </tr>
              `;
            })
            .join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderTopContractsTable(contracts, options = {}) {
  const items = Array.isArray(contracts) ? contracts : [];
  if (!items.length) {
    return renderEmptyView("No hay contratos comparables en este corte.");
  }
  return renderContractsTable(items, options);
}

function renderFlagsList(flags, contract = {}) {
  if (!flags.length) {
    return renderEmptyView("No se detectaron red flags en este contrato.");
  }

  return `
    <div class="flag-list">
      ${flags
        .map(
          (flag) => `
            <article class="flag-card">
              <div class="flag-card__head">
                <div>
                  <div class="comparison-card__title">${escapeHtml(flag.label || flag.name || flag.code || "Red flag")}</div>
                  <div class="data-table__subtle">${escapeHtml(flag.code || "")}</div>
                </div>
                <span class="badge ${severityClass(flag.severity)}">${escapeHtml(flag.severity || "Media")}</span>
              </div>
              <div class="flag-card__body">
                <p>${escapeHtml(flag.description || "Sin descripción adicional.")}</p>
                ${flag.evidence ? `<p><strong>Evidencia:</strong> ${escapeHtml(flag.evidence)}</p>` : ""}
                ${flag.weight !== undefined ? `<p><strong>Peso:</strong> ${formatNumber(flag.weight)}</p>` : ""}
              </div>
            </article>
          `,
        )
        .join("")}
    </div>
  `;
}

function renderFlagSummaryTable(flags) {
  if (!flags.length) {
    return renderEmptyView("Sin red flags agregadas para mostrar.");
  }

  return `
    <div class="table-shell">
      <table class="data-table">
        <thead>
          <tr>
            <th>Código</th>
            <th>Etiqueta</th>
            <th>Severidad</th>
            <th>Casos</th>
          </tr>
        </thead>
        <tbody>
          ${flags
            .map(
              (flag) => `
                <tr>
                  <td><strong>${escapeHtml(flag.code || "")}</strong></td>
                  <td>${escapeHtml(flag.label || flag.name || "")}</td>
                  <td><span class="badge ${severityClass(flag.severity)}">${escapeHtml(flag.severity || "Media")}</span></td>
                  <td>${formatNumber(flag.count || 0)}</td>
                </tr>
              `,
            )
            .join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderAnalysisBlock(title, text) {
  return `
    <div class="callout">
      <strong>${escapeHtml(title)}</strong>
      <p class="panel-subtitle">${escapeHtml(text)}</p>
    </div>
  `;
}

function renderDecisionSupportPanel(decisionSupport) {
  const support = decisionSupport || {};
  const patterns = Array.isArray(support.patterns) ? support.patterns : [];
  const graphSuggestions = Array.isArray(support.graph_suggestions) ? support.graph_suggestions : [];
  const focusAreas = Array.isArray(support.focus_areas) ? support.focus_areas : [];
  const hasContent = Boolean(support.guidance || support.why_now || patterns.length || graphSuggestions.length || focusAreas.length);

  if (!hasContent) {
    return "";
  }

  return `
    <section class="section section--decision">
      <div class="section__title">
        <div>
          <span class="badge badge--muted">${escapeHtml(support.title || "Guía de decisión")}</span>
          <h4 class="panel-title">Qué conviene revisar antes de decidir</h4>
          <p class="panel-subtitle">${escapeHtml(support.guidance || "La lectura del panel ayuda a priorizar revisión, contrastar evidencia y decidir si vale la pena profundizar.")}</p>
        </div>
        <div class="panel-meta">
          ${support.why_now ? `<span class="status-pill">${escapeHtml(support.why_now)}</span>` : ""}
        </div>
      </div>

      <div class="callout">
        <strong>Por qué importa</strong>
        <p class="panel-subtitle">${escapeHtml(support.guidance || "La señal no cierra una conclusión por sí sola: sirve para decidir dónde revisar primero.")}</p>
      </div>

      <div class="summary-grid">
        <article class="comparison-card">
          <div class="comparison-card__head">
            <span class="comparison-card__title">Patrones detectados</span>
          </div>
          <div class="comparison-card__body">
            ${
              patterns.length
                ? `<ul class="bullet-list">${patterns.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>`
                : `<p>Sin patrones suficientes para resaltar una tendencia estable.</p>`
            }
          </div>
        </article>
        <article class="comparison-card">
          <div class="comparison-card__head">
            <span class="comparison-card__title">Gráficas sugeridas</span>
          </div>
          <div class="comparison-card__body">
            ${
              graphSuggestions.length
                ? `<div class="chip-row">${graphSuggestions.map((item) => `<span class="badge badge--muted">${escapeHtml(item)}</span>`).join("")}</div>`
                : `<p>Sin gráficas específicas sugeridas para esta consulta.</p>`
            }
          </div>
        </article>
        <article class="comparison-card">
          <div class="comparison-card__head">
            <span class="comparison-card__title">En qué profundizar</span>
          </div>
          <div class="comparison-card__body">
            ${
              focusAreas.length
                ? `<ul class="bullet-list">${focusAreas.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>`
                : `<p>Prioriza score, red flags, competencia y trazabilidad documental.</p>`
            }
          </div>
        </article>
      </div>
    </section>
  `;
}

function renderBulletPanel(title, items) {
  const list = Array.isArray(items) ? items : [];
  if (!list.length) {
    return "";
  }

  return `
    <div class="section">
      <div class="section__title">
        <div><h4 class="panel-title">${escapeHtml(title)}</h4></div>
      </div>
      <ul class="bullet-list">${list.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>
    </div>
  `;
}

function renderKeyValue(label, value) {
  return `
    <div class="key-value">
      <span class="key-value__label">${escapeHtml(label)}</span>
      <span class="key-value__value">${escapeHtml(value)}</span>
    </div>
  `;
}

function renderMetricsGrid(cards) {
  return `
    <div class="metrics-grid">
      ${cards
        .map(
          (card) => `
            <article class="metric-card">
              <span class="metric-card__label">${escapeHtml(card.label)}</span>
              <strong class="metric-card__value">${escapeHtml(card.value)}</strong>
              <span class="metric-card__note">${escapeHtml(card.note || "")}</span>
            </article>
          `,
        )
        .join("")}
    </div>
  `;
}

function renderSuggestedActions(actions) {
  const list = Array.isArray(actions) ? actions : [];
  if (!list.length) {
    return "";
  }

  return `
    <section class="section">
      <div class="section__title">
        <div><h4 class="panel-title">Siguientes pasos</h4></div>
      </div>
      <div class="chip-row">
        ${list
          .map(
            (action) => `
              <button
                class="chip"
                type="button"
                data-chat-action="suggested"
                data-suggested-label="${escapeHtml(action)}"
              >
                ${escapeHtml(action)}
              </button>
            `,
          )
          .join("")}
      </div>
    </section>
  `;
}

function renderEmptyView(message) {
  return `
    <div class="empty-state">
      <strong>${escapeHtml(message)}</strong>
      <p>Intenta otra pregunta o usa una de las acciones rápidas del encabezado.</p>
    </div>
  `;
}

function renderMiniHighlight(text) {
  return `
    <article class="metric-card">
      <span class="metric-card__label">Destacado</span>
      <strong class="metric-card__value">${escapeHtml(text)}</strong>
    </article>
  `;
}

function renderFlagChips(flags) {
  const list = Array.isArray(flags) ? flags : [];
  if (!list.length) {
    return '<span class="data-table__subtle">Sin red flags</span>';
  }

  return `
    <div class="chip-row">
      ${list
        .slice(0, 3)
        .map((flag) => `<span class="badge badge--muted">${escapeHtml(typeof flag === "string" ? flag : flag.label || flag.code || "")}</span>`)
        .join("")}
    </div>
  `;
}

function buildMetaPills(response) {
  const meta = response.meta || {};
  const pills = [
    response.intent || "unknown_query",
    response.view_type || "project_overview",
    meta.depth ? `${meta.depth}` : null,
    meta.offline ? "modo local" : null,
    meta.confidence !== undefined ? `confianza ${Math.round(Number(meta.confidence) * 100)}%` : null,
  ].filter(Boolean);

  return pills.map((item) => `<span class="status-pill">${escapeHtml(String(item))}</span>`).join("");
}

function renderFilterPillList(filters) {
  const entries = Object.entries(filters || {}).filter(([, value]) => value !== undefined && value !== null && value !== "" && value !== "all");
  if (!entries.length) {
    return "";
  }
  return entries
    .map(([key, value]) => `<span class="status-pill">${escapeHtml(key)}: ${escapeHtml(String(value))}</span>`)
    .join("");
}

function statusClass(status) {
  const normalized = normalizeText(status || "");
  if (normalized.includes("CERR")) {
    return "badge--low";
  }
  if (normalized.includes("ABIER") || normalized.includes("PEND")) {
    return "badge--medium";
  }
  return "badge--muted";
}

function severityClass(severity) {
  const normalized = normalizeText(severity || "");
  if (normalized.startsWith("ALTA")) {
    return "badge--high";
  }
  if (normalized.startsWith("MEDIA")) {
    return "badge--medium";
  }
  if (normalized.startsWith("BAJA")) {
    return "badge--low";
  }
  return "badge--muted";
}

function riskLevelClass(value) {
  const normalized = normalizeRiskLabel(value);
  if (normalized === "Crítico") {
    return "badge--critical";
  }
  if (normalized === "Alto") {
    return "badge--high";
  }
  if (normalized === "Medio") {
    return "badge--medium";
  }
  return "badge--low";
}

function normalizeRiskLabel(value) {
  const text = String(value || "").toLowerCase();
  if (text.includes("crit")) {
    return "Crítico";
  }
  if (text.includes("alto")) {
    return "Alto";
  }
  if (text.includes("medio")) {
    return "Medio";
  }
  return "Bajo";
}

function formatRiskLabel(value) {
  if (typeof value === "number") {
    if (value >= 85) {
      return "Crítico";
    }
    if (value >= 56) {
      return "Alto";
    }
    if (value >= 31) {
      return "Medio";
    }
    return "Bajo";
  }
  return normalizeRiskLabel(value);
}

function labelForMode(mode) {
  if (mode === "supplier") {
    return "Comparación de proveedores";
  }
  if (mode === "entity") {
    return "Comparación de entidades";
  }
  if (mode === "region") {
    return "Resumen territorial";
  }
  return "Comparación";
}
