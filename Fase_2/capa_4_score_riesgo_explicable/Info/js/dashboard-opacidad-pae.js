import { loadDashboardDataset } from "./dataService.js";
import { createDefaultFilters, buildFilterOptions, applyFilters, buildPeriodLabel } from "./filters.js";
import {
  buildContractsTableMarkup,
  buildCsv,
  paginateContracts,
  sortContracts,
} from "./table.js";
import {
  calculateImpactMetrics,
  calculateKpis,
  calculateRedFlagSummary,
  calculateRiskDistribution,
  buildHistogram,
  buildComparisonDatasets,
} from "./analytics.js";
import {
  renderDoughnutChart,
  renderHeatmap,
  renderHistogram,
  renderHorizontalBarChart,
  renderLineChart,
  renderScatterChart,
} from "./charts.js";
import { createDetailModel, renderContractDetail } from "./contractDetail.js";
import {
  copyTextToClipboard,
  downloadTextFile,
  escapeHtml,
  formatDateTime,
  formatMoney,
  formatNumber,
  normalizeText,
} from "./utils.js";

const dom = {};

const state = {
  loading: true,
  error: "",
  contracts: [],
  config: null,
  options: null,
  filters: null,
  sort: { key: "risk_score", direction: "desc" },
  page: 1,
  selectedId: "",
  lastUpdated: new Date(),
  liveSummary: null,
  traceability: null,
  filteredContracts: [],
  sortedContracts: [],
  paginatedContracts: [],
  kpis: null,
  riskDistribution: [],
  histogram: [],
  redFlagSummary: null,
  comparison: null,
  impact: null,
  detailModel: null,
  chatMessages: [],
  chatActiveContractId: "",
};

if (typeof document !== "undefined") {
  document.addEventListener("DOMContentLoaded", bootstrap);
}

async function bootstrap() {
  cacheDom();
  bindEvents();
  await loadData();
  syncFiltersToDom();
  render();
}

function cacheDom() {
  const ids = [
    "statusSource",
    "statusUpdated",
    "statusContracts",
    "statusPeriod",
    "periodSelect",
    "refreshBtn",
    "exportCsvBtn",
    "printBtn",
    "searchForm",
    "kpiGrid",
    "riskDonut",
    "riskHistogram",
    "riskThresholdNotes",
    "redFlagBarChart",
    "redFlagHeatmap",
    "redFlagTableBody",
    "redFlagSummary",
    "deptRiskChart",
    "entityRiskChart",
    "entityAlertsChart",
    "modalityChart",
    "supplierChart",
    "timelineChart",
    "scatterChart",
    "additionChart",
    "topContractsChart",
    "mapDepartmentList",
    "searchQuery",
    "entityFilter",
    "supplierFilter",
    "objectFilter",
    "departmentFilter",
    "municipalityFilter",
    "modalityFilter",
    "riskLevelFilter",
    "redFlagFilter",
    "scoreMinFilter",
    "scoreMaxFilter",
    "valueMinFilter",
    "valueMaxFilter",
    "dateFromFilter",
    "dateToFilter",
    "resetFiltersBtn",
    "resultsCount",
    "contractsTableBody",
    "tablePagination",
    "tableShell",
    "detailPanel",
    "detailEmptyState",
    "processTimeline",
    "impactGrid",
    "chatStatus",
    "chatTranscript",
    "chatForm",
    "chatInput",
    "chatSendBtn",
    "chatQuickActions",
    "loadingState",
    "errorState",
  ];

  for (const id of ids) {
    dom[id] = document.getElementById(id);
  }
}

function bindEvents() {
  dom.refreshBtn?.addEventListener("click", async () => {
    await loadData(true);
    syncFiltersToDom();
    render();
  });

  dom.printBtn?.addEventListener("click", () => window.print());
  dom.exportCsvBtn?.addEventListener("click", exportFilteredCsv);
  dom.resetFiltersBtn?.addEventListener("click", resetFilters);
  dom.searchForm?.addEventListener("submit", handleSearchSubmit);

  dom.periodSelect?.addEventListener("change", () => {
    state.filters.period = dom.periodSelect.value;
    state.page = 1;
    render();
  });

  const filterInputs = [
    "searchQuery",
    "entityFilter",
    "supplierFilter",
    "objectFilter",
    "departmentFilter",
    "municipalityFilter",
    "modalityFilter",
    "riskLevelFilter",
    "redFlagFilter",
    "scoreMinFilter",
    "scoreMaxFilter",
    "valueMinFilter",
    "valueMaxFilter",
    "dateFromFilter",
    "dateToFilter",
  ];

  for (const id of filterInputs) {
    dom[id]?.addEventListener("input", updateFiltersFromDom);
    dom[id]?.addEventListener("change", updateFiltersFromDom);
  }

  dom.contractsTableBody?.addEventListener("click", handleTableClick);
  dom.tablePagination?.addEventListener("click", handlePaginationClick);
  dom.detailPanel?.addEventListener("click", handleDetailClick);
  dom.tableShell?.addEventListener("click", handleTableSortClick);
  dom.chatForm?.addEventListener("submit", handleChatSubmit);
  dom.chatQuickActions?.addEventListener("click", handleChatQuickAction);
  dom.chatInput?.addEventListener("keydown", handleChatKeydown);
}

async function loadData(forceReload = false) {
  state.loading = true;
  state.error = "";
  renderLoading();

  try {
    const previousFilters = state.filters;
    const previousSort = state.sort;
    const previousSelectedId = state.selectedId;
    const { contracts, config, summary, traceability, lastUpdated } = await loadDashboardDataset(forceReload);
    state.contracts = contracts;
    state.config = config;
    state.liveSummary = summary;
    state.traceability = traceability;
    state.options = buildFilterOptions(contracts, config);
    state.filters = previousFilters || createDefaultFilters(config);
    state.sort = previousSort || {
      key: config.default_sort?.key || "risk_score",
      direction: config.default_sort?.direction || "desc",
    };
    state.page = 1;
    state.selectedId = previousSelectedId || contracts[0]?.contract_id || "";
    state.lastUpdated = lastUpdated instanceof Date && !Number.isNaN(lastUpdated.getTime()) ? lastUpdated : new Date();
    state.chatMessages = [];
    state.chatActiveContractId = "";
  } catch (error) {
    state.error = error instanceof Error ? error.message : "No se pudo cargar el dashboard.";
  } finally {
    state.loading = false;
  }
}

function render() {
  if (state.loading) {
    renderLoading();
    return;
  }

  if (state.error) {
    renderError();
    return;
  }

  const filtered = applyFilters(state.contracts, state.filters);
  const sorted = sortContracts(filtered, state.sort);
  const pageSize = state.config.page_size;
  const pagination = paginateContracts(sorted, state.page, pageSize);
  const selectedContract = resolveSelectedContract(sorted, pagination.items);

  state.filteredContracts = filtered;
  state.sortedContracts = sorted;
  state.paginatedContracts = pagination.items;
  state.kpis = calculateKpis(sorted, state.config);
  state.riskDistribution = calculateRiskDistribution(sorted, state.config);
  state.histogram = buildHistogram(sorted, 10);
  state.redFlagSummary = calculateRedFlagSummary(sorted, state.config);
  state.comparison = buildComparisonDatasets(sorted, state.config);
  state.impact = calculateImpactMetrics(sorted, state.kpis);
  state.detailModel = createDetailModel(selectedContract, sorted, state.config);
  state.page = pagination.currentPage;

  updateHeroState(sorted, selectedContract);
  renderKpis();
  renderRiskSection();
  renderRedFlagsSection();
  renderComparisonsSection();
  renderMapSection();
  renderSearchSection(pagination, selectedContract);
  renderDetailSection(selectedContract);
  renderProcessTimeline();
  renderImpactSection();
  renderChatbotSection(selectedContract);
  renderTablesAndPagination(pagination, selectedContract);
  renderStatus();
}

function renderLoading() {
  dom.errorState?.classList.add("hidden");
  setText(dom.statusSource, "Cargando datos preprocesados...");
  setText(dom.statusUpdated, "Ultima actualizacion: ...");
  setText(dom.statusContracts, "Contratos analizados: ...");
  setText(dom.statusPeriod, "Periodo: ...");

  if (dom.loadingState) {
    dom.loadingState.classList.remove("hidden");
  }

  if (dom.kpiGrid) {
    dom.kpiGrid.innerHTML = `
      <div class="skeleton-grid">
        ${Array.from({ length: 10 }, () => `<div class="skeleton-card"></div>`).join("")}
      </div>
    `;
  }

  if (dom.detailPanel) {
    dom.detailPanel.innerHTML = `
      <div class="empty-state">
        <strong>Cargando detalle...</strong>
      </div>
    `;
  }
}

function renderError() {
  dom.loadingState?.classList.add("hidden");
  if (dom.errorState) {
    dom.errorState.classList.remove("hidden");
    dom.errorState.innerHTML = `
      <div class="empty-state">
        <strong>No fue posible cargar el dashboard.</strong>
        <p>${escapeHtml(state.error)}</p>
        <p>Verifica que abras la página desde un servidor local, por ejemplo <code>node Fase_2/capa_4_score_riesgo_explicable/Info/server.mjs</code> o un servidor estático en la carpeta <code>Fase_2/capa_4_score_riesgo_explicable/Info</code>.</p>
      </div>
    `;
  }
}

function renderStatus() {
  dom.loadingState?.classList.add("hidden");
  dom.errorState?.classList.add("hidden");
}

function updateHeroState(selectedContracts, selectedContract) {
  const traceabilityLabel = state.traceability?.status_label ? ` · ${state.traceability.status_label}` : "";
  setText(dom.statusSource, `${state.config.source_label}${traceabilityLabel}`);
  setText(dom.statusUpdated, `Ultima actualizacion: ${formatDateTime(state.lastUpdated)}`);
  setText(
    dom.statusContracts,
    `Contratos analizados: ${formatNumber(state.liveSummary?.row_count ?? state.contracts.length)}`,
  );
  setText(dom.statusPeriod, `Periodo: ${buildPeriodLabel(state.filters)}`);
}

function renderKpis() {
  const metrics = state.kpis;
  const trend = metrics.trend;
  const cards = [
    {
      label: "Total de contratos analizados",
      value: formatNumber(metrics.totalContracts),
      note: `${formatNumber(metrics.contractsWithFlags)} con al menos una red flag`,
      trend: trend.currentYear ? `Vigencia ${trend.currentYear}` : "Sin corte anual",
    },
    {
      label: "Valor total contratado",
      value: formatMoney(metrics.totalValue),
      note: trend.previousYear ? trendLabel(trend.valueDeltaPct, "vs", trend.previousYear) : "Sin comparacion anual",
      tone: trend.valueDeltaPct >= 0 ? "warn" : "good",
    },
    {
      label: "Score promedio de riesgo",
      value: `${formatNumber(metrics.averageRisk)}/100`,
      note: trend.previousYear ? `Delta ${formatNumber(trend.averageRiskDelta)} puntos` : "Sin comparacion anual",
      tone: trend.averageRiskDelta >= 0 ? "warn" : "good",
    },
    {
      label: "Contratos de riesgo alto",
      value: formatNumber(metrics.highRiskCount),
      note: trend.previousYear ? `Delta ${formatNumber(trend.highRiskDelta)} casos` : "Sin comparacion anual",
      tone: trend.highRiskDelta >= 0 ? "warn" : "good",
    },
    {
      label: "Contratos de riesgo medio",
      value: formatNumber(metrics.mediumRiskCount),
      note: `${Math.round((metrics.mediumRiskCount / Math.max(metrics.totalContracts, 1)) * 100)}% del total`,
      tone: "neutral",
    },
    {
      label: "Contratos de riesgo bajo",
      value: formatNumber(metrics.lowRiskCount),
      note: `${Math.round((metrics.lowRiskCount / Math.max(metrics.totalContracts, 1)) * 100)}% del total`,
      tone: "good",
    },
    {
      label: "Red flags detectadas",
      value: formatNumber(metrics.totalRedFlags),
      note: trend.previousYear ? `Delta ${formatNumber(trend.redFlagDelta)} vs vigencia anterior` : "Sin comparacion anual",
      tone: trend.redFlagDelta >= 0 ? "warn" : "good",
    },
    {
      label: "Top entidad con mas contratos riesgosos",
      value: metrics.topRiskEntity.label || "Sin dato",
      note: `${formatNumber(metrics.topRiskEntity.count)} contratos altos`,
      tone: "neutral",
    },
    {
      label: "Top proveedor con mas alertas",
      value: metrics.topAlertSupplier.label || "Sin dato",
      note: `${formatNumber(metrics.topAlertSupplier.count)} alertas`,
      tone: "neutral",
    },
    {
      label: "Modalidad mas frecuente",
      value: metrics.topModality.label || "Sin dato",
      note: `${formatNumber(metrics.topModality.count)} contratos`,
      tone: "neutral",
    },
  ];

  dom.kpiGrid.innerHTML = cards
    .map(
      (card) => `
        <article class="kpi-card kpi-card--${card.tone || "neutral"}">
          <span class="kpi-label">${escapeHtml(card.label)}</span>
          <strong class="kpi-value">${escapeHtml(card.value)}</strong>
          <span class="kpi-note">${escapeHtml(card.note || "")}</span>
          ${card.trend ? `<span class="kpi-trend">${escapeHtml(card.trend)}</span>` : ""}
        </article>
      `,
    )
    .join("");
}

function renderRiskSection() {
  renderDoughnutChart(dom.riskDonut, state.riskDistribution, {
    centerLabel: "contratos",
    centerNote: "bajo / medio / alto",
  });
  renderHistogram(dom.riskHistogram, state.histogram, {
    caption: "Histograma de scores de 0 a 100. Los umbrales se pueden modificar desde dashboard_config.json.",
  });

  if (dom.riskThresholdNotes) {
    const low = state.config.risk_thresholds.low;
    const medium = state.config.risk_thresholds.medium;
    const high = state.config.risk_thresholds.high;
    dom.riskThresholdNotes.innerHTML = `
      <ul class="threshold-list">
        <li><strong>Bajo</strong> ${low[0]} a ${low[1]}</li>
        <li><strong>Medio</strong> ${medium[0]} a ${medium[1]}</li>
        <li><strong>Alto</strong> ${high[0]} a ${high[1]}</li>
      </ul>
      <p class="chart-caption">Clasificacion editable desde la configuracion local del dashboard.</p>
    `;
  }
}

function renderRedFlagsSection() {
  const summaryRows = state.redFlagSummary.summaryRows;
  renderHorizontalBarChart(dom.redFlagBarChart, summaryRows, {
    caption: "Red flags mas frecuentes segun el universo filtrado.",
    formatValue: (value) => `${formatNumber(value)} contratos`,
  });

  renderHeatmap(
    dom.redFlagHeatmap,
    state.redFlagSummary.topDepartments.map((item) => item.department),
    state.redFlagSummary.topFlags,
    state.redFlagSummary.matrix,
    {
      caption: "Mapa de calor de red flags por departamento. Los tonos mas oscuros indican mayor concentracion de alertas.",
    },
  );

  if (dom.redFlagTableBody) {
    dom.redFlagTableBody.innerHTML = summaryRows
      .map(
        (row) => `
          <tr title="${escapeHtml(row.description || "")}">
            <td><strong>${escapeHtml(row.name)}</strong></td>
            <td>${escapeHtml(row.description || "")}</td>
            <td>${escapeHtml(formatNumber(row.count))}</td>
            <td><span class="severity-pill severity-pill--${normalizeText(row.severity)}">${escapeHtml(row.severity)}</span></td>
          </tr>
        `,
      )
      .join("");
  }

  if (dom.redFlagSummary) {
    const top = summaryRows[0];
    dom.redFlagSummary.innerHTML = top
      ? `
        <div class="summary-callout">
          <strong>Red flag dominante</strong>
          <p>${escapeHtml(top.name)} concentra ${escapeHtml(formatNumber(top.count))} contratos en el universo filtrado.</p>
        </div>
      `
      : `<div class="empty-state compact"><strong>No hay red flags en el universo filtrado.</strong></div>`;
  }
}

function renderComparisonsSection() {
  renderHorizontalBarChart(dom.deptRiskChart, state.comparison.departmentRisk, {
    caption: "Riesgo promedio por departamento.",
    formatValue: (value) => `${formatNumber(value)}/100`,
  });

  renderHorizontalBarChart(dom.entityRiskChart, state.comparison.entityRisk, {
    caption: "Riesgo promedio por entidad contratante.",
    formatValue: (value) => `${formatNumber(value)}/100`,
  });

  renderHorizontalBarChart(dom.entityAlertsChart, state.comparison.entityAlerts, {
    caption: "Top 10 entidades con mas alertas acumuladas.",
    formatValue: (value) => `${formatNumber(value)} alertas`,
  });

  renderDoughnutChart(
    dom.modalityChart,
    state.comparison.modalityCounts.map((item, index) => ({
      label: item.label,
      value: item.value,
      percent: (item.value / Math.max(state.kpis.totalContracts, 1)) * 100,
      color: state.config.chart_palette[index % state.config.chart_palette.length],
    })),
    {
      centerLabel: "modalidades",
      centerNote: "distribucion de contratos",
    },
  );

  renderHorizontalBarChart(dom.supplierChart, state.comparison.supplierAlerts, {
    caption: "Concentracion de alertas por proveedor.",
    formatValue: (value) => `${formatNumber(value)} alertas`,
  });

  renderLineChart(
    dom.timelineChart,
    [
      {
        label: "Contratos",
        color: state.config.chart_palette[0],
        values: state.comparison.timeline.map((item) => ({ label: item.displayLabel || item.label, value: item.contracts })),
      },
      {
        label: "Alertas",
        color: state.config.risk_colors.high,
        values: state.comparison.timeline.map((item) => ({ label: item.displayLabel || item.label, value: item.alerts })),
      },
    ],
    {
      caption: "Evolucion temporal de contratos y alertas detectadas.",
    },
  );

  renderScatterChart(dom.scatterChart, state.comparison.scatter, {
    caption: "Cada punto representa un contrato. El eje X usa escala logaritmica para el valor contratado.",
  });

  renderHorizontalBarChart(dom.additionChart, state.comparison.additionPressure, {
    caption: "Contratos con mayor presion de adiciones y modificaciones.",
    formatValue: (value) => `${formatNumber(value)}%`,
  });

  renderHorizontalBarChart(dom.topContractsChart, state.comparison.topContracts, {
    caption: "Top 10 contratos con mayor score de riesgo.",
    formatValue: (value) => `${formatNumber(value)}/100`,
  });
}

function renderMapSection() {
  if (!dom.mapDepartmentList) {
    return;
  }

  const topDepartments = [...state.redFlagSummary.topDepartments]
    .sort((left, right) => right.averageRisk - left.averageRisk)
    .slice(0, 5);

  dom.mapDepartmentList.innerHTML = topDepartments.length
    ? topDepartments
        .map(
          (item, index) => `
            <li class="map-list__item">
              <span class="map-list__rank">${formatNumber(index + 1)}</span>
              <div>
                <strong>${escapeHtml(item.department)}</strong>
                <p>${escapeHtml(formatNumber(item.averageRisk))}/100 de riesgo promedio · ${escapeHtml(formatNumber(item.totalFlags))} alertas</p>
              </div>
            </li>
          `,
        )
        .join("")
    : `<li class="map-list__item"><strong>Sin datos geograficos.</strong></li>`;
}

function renderSearchSection(pagination, selectedContract) {
  if (dom.resultsCount) {
    dom.resultsCount.textContent = `${formatNumber(state.filteredContracts.length)} contratos filtrados`;
  }

  if (dom.periodSelect && dom.periodSelect.options.length) {
    dom.periodSelect.value = state.filters.period;
  }

  if (dom.searchQuery) {
    dom.searchQuery.value = state.filters.query;
  }
  if (dom.objectFilter) {
    dom.objectFilter.value = state.filters.object;
  }
  if (dom.entityFilter) {
    dom.entityFilter.value = state.filters.entity;
  }
  if (dom.supplierFilter) {
    dom.supplierFilter.value = state.filters.supplier;
  }
  if (dom.departmentFilter) {
    dom.departmentFilter.value = state.filters.department;
  }
  if (dom.municipalityFilter) {
    dom.municipalityFilter.value = state.filters.municipality;
  }
  if (dom.modalityFilter) {
    dom.modalityFilter.value = state.filters.modality;
  }
  if (dom.riskLevelFilter) {
    dom.riskLevelFilter.value = state.filters.riskLevel;
  }
  if (dom.redFlagFilter) {
    dom.redFlagFilter.value = state.filters.redFlag;
  }
  if (dom.scoreMinFilter) {
    dom.scoreMinFilter.value = state.filters.scoreMin;
  }
  if (dom.scoreMaxFilter) {
    dom.scoreMaxFilter.value = state.filters.scoreMax;
  }
  if (dom.valueMinFilter) {
    dom.valueMinFilter.value = state.filters.valueMin;
  }
  if (dom.valueMaxFilter) {
    dom.valueMaxFilter.value = state.filters.valueMax;
  }
  if (dom.dateFromFilter) {
    dom.dateFromFilter.value = state.filters.dateFrom;
  }
  if (dom.dateToFilter) {
    dom.dateToFilter.value = state.filters.dateTo;
  }
}

function renderTablesAndPagination(pagination, selectedContract) {
  if (dom.contractsTableBody) {
    dom.contractsTableBody.innerHTML = buildContractsTableMarkup(pagination.items, state.sort, selectedContract?.contract_id || "");
  }

  if (dom.tablePagination) {
    dom.tablePagination.innerHTML = pagination.items.length
      ? buildPaginationMarkup(pagination.currentPage, pagination.totalPages, state.filteredContracts.length, state.config.page_size)
      : "";
  }
}

function renderDetailSection(selectedContract) {
  if (!dom.detailPanel) {
    return;
  }

  if (!selectedContract) {
    dom.detailPanel.innerHTML = `
      <div class="empty-state">
        <strong>No hay contrato seleccionado.</strong>
      </div>
    `;
    return;
  }

  renderContractDetail(dom.detailPanel, selectedContract, state.detailModel, state.config);
}

function renderProcessTimeline() {
  if (!dom.processTimeline) {
    return;
  }

  const steps = [
    {
      step: "1",
      title: "Apertura del HTML principal",
      body: "La persona usuaria entra por la columna vertebral del MVP y ve el resumen del proyecto con el nombre de Alejandro Montes.",
    },
    {
      step: "2",
      title: "Busqueda y filtros",
      body: "Se busca por ID, objeto, entidad, proveedor, municipio, departamento o palabra clave, con el filtro de departamento visible desde el inicio.",
    },
    {
      step: "3",
      title: "Tabla priorizada",
      body: "La tabla ordena primero por score de riesgo y luego por red flags, valor, competencia y concentración del proveedor.",
    },
    {
      step: "4",
      title: "Seleccion del contrato",
      body: "Al elegir un contrato, la vista lateral genera el reporte, el dashboard, los patrones y las conexiones relacionadas.",
    },
    {
      step: "5",
      title: "Reporte y dashboard",
      body: "Se muestran resumen ejecutivo, análisis técnico, interpretación ciudadana, recomendaciones y tarjetas de contexto para el contrato seleccionado.",
    },
    {
      step: "6",
      title: "Chatbot contextual",
      body: "El asistente solo responde sobre el contrato seleccionado, el informe visible y los datos disponibles; si falta información, lo dice claramente.",
    },
  ];

  dom.processTimeline.innerHTML = steps
    .map(
      (item) => `
        <article class="timeline-step">
          <span class="timeline-step__number">${escapeHtml(item.step)}</span>
          <strong>${escapeHtml(item.title)}</strong>
          <p>${escapeHtml(item.body)}</p>
        </article>
      `,
    )
    .join("");
}

function renderImpactSection() {
  if (!dom.impactGrid) {
    return;
  }

  const impact = state.impact;
  const cards = [
    {
      label: "Contratos priorizados automaticamente",
      value: formatNumber(impact.prioritizedContracts),
      note: "Score 60 o superior",
    },
    {
      label: "Horas estimadas de revision manual reducidas",
      value: `${formatNumber(impact.hoursSaved)} h`,
      note: "Estimacion basada en triaje automatico",
    },
    {
      label: "Porcentaje de contratos relevantes",
      value: `${formatNumber(impact.relevantRate)}%`,
      note: "Universo mock ya prefiltrado para PAE",
    },
    {
      label: "Contratos de alto riesgo encontrados",
      value: formatNumber(state.kpis.highRiskCount),
      note: "Revision documental prioritaria",
    },
    {
      label: "Valor economico bajo revision prioritaria",
      value: formatMoney(impact.highRiskValue),
      note: "Suma de contratos con score alto",
    },
    {
      label: "Red flag mas critica",
      value: impact.topCriticalFlag?.name || "Sin alerta",
      note: `${formatNumber(impact.topCriticalFlag?.count || 0)} casos`,
    },
  ];

  dom.impactGrid.innerHTML = cards
    .map(
      (card) => `
        <article class="impact-card">
          <span class="kpi-label">${escapeHtml(card.label)}</span>
          <strong class="kpi-value">${escapeHtml(card.value)}</strong>
          <span class="kpi-note">${escapeHtml(card.note)}</span>
        </article>
      `,
    )
    .join("");
}

function renderChatbotSection(selectedContract) {
  if (!dom.chatTranscript || !dom.chatInput || !dom.chatSendBtn) {
    return;
  }

  if (!selectedContract) {
    state.chatMessages = [];
    state.chatActiveContractId = "";
    dom.chatStatus && (dom.chatStatus.textContent = "Primero selecciona un contrato para activar el chatbot.");
    dom.chatTranscript.innerHTML = `
      <div class="empty-state">
        <strong>Primero selecciona un contrato para que pueda responder preguntas sobre su informe.</strong>
        <p>El chatbot solo usa el contrato seleccionado, el reporte visible y los datos disponibles.</p>
      </div>
    `;
    dom.chatInput.disabled = true;
    dom.chatSendBtn.disabled = true;
    dom.chatInput.placeholder = "Selecciona un contrato para activar el chatbot...";
    if (dom.chatQuickActions) {
      dom.chatQuickActions.innerHTML = "";
    }
    return;
  }

  if (state.chatActiveContractId !== selectedContract.contract_id) {
    state.chatActiveContractId = selectedContract.contract_id;
    state.chatMessages = [
      {
        role: "assistant",
        text: buildChatWelcomeMessage(selectedContract, state.detailModel),
      },
    ];
  }

  dom.chatStatus && (dom.chatStatus.textContent = `Chat activo para ${selectedContract.contract_id}`);
  dom.chatInput.disabled = false;
  dom.chatSendBtn.disabled = false;
  dom.chatInput.placeholder = "Pregunta por el informe, las red flags, las conexiones o las limitaciones...";

  if (dom.chatQuickActions) {
    dom.chatQuickActions.innerHTML = buildChatQuickActions(selectedContract)
      .map(
        (item) => `
          <button type="button" class="button secondary chat-quick-action" data-chat-query="${escapeHtml(item.query)}">
            ${escapeHtml(item.label)}
          </button>
        `,
      )
      .join("");
  }

  dom.chatTranscript.innerHTML = state.chatMessages
    .map((message) => renderChatMessage(message))
    .join("");
  dom.chatTranscript.scrollTop = dom.chatTranscript.scrollHeight;
}

function renderChatMessage(message) {
  const isUser = message.role === "user";
  return `
    <article class="chat-message ${isUser ? "chat-message--user" : "chat-message--assistant"}">
      <div class="chat-message__bubble">${escapeHtml(message.text || "")}</div>
    </article>
  `;
}

function buildChatWelcomeMessage(contract, detailModel) {
  const summary = detailModel?.chatbotContext?.summary || `Contrato ${contract.contract_id} cargado.`;
  const limitations = detailModel?.chatbotContext?.limitations || "No hay limitaciones adicionales registradas.";
  return `${summary} Puedes preguntarme por el score, las red flags, los patrones, las conexiones o las limitaciones del análisis. ${limitations}`;
}

function buildChatQuickActions(contract) {
  return [
    { label: "Explícame el informe", query: "Explícame este informe en palabras sencillas." },
    { label: "¿Por qué este score?", query: "¿Por qué este contrato tiene este score?" },
    { label: "Red flag clave", query: "¿Qué red flag es más importante?" },
    { label: "Conexiones", query: "¿Este proveedor aparece en otros contratos?" },
    { label: "Fuentes externas", query: "¿Qué fuentes externas debería consultar?" },
  ];
}

function buildChatReply(query, contract, detailModel) {
  if (!contract) {
    return "Primero selecciona un contrato para que pueda responder preguntas sobre su informe.";
  }

  const normalized = normalizeText(query);
  const redFlags = contract.red_flag_details || [];
  const topFlag = redFlags[0];
  const patternCount = detailModel?.riskPatterns?.length || 0;
  const connectionCount = detailModel?.connections?.length || 0;
  const supplierFrequency = detailModel?.dashboardCards?.find((card) => card.label === "Frecuencia del proveedor")?.value || "0";
  const limitations = detailModel?.chatbotContext?.limitations || contract.limitations || contract.risk_limitations || "No hay limitaciones adicionales registradas.";

  if (normalized.includes("score") || normalized.includes("riesgo")) {
    return `Este contrato tiene score ${formatNumber(contract.risk_score)}/100 y nivel ${contract.risk_level || "sin nivel"}. Frente a contratos comparables, el delta es de ${formatNumber(detailModel?.riskDelta || 0)} puntos. ${topFlag ? `La red flag principal es ${topFlag.name}.` : "No hay red flags principales registradas."}`;
  }

  if (normalized.includes("red flag") || normalized.includes("alerta") || normalized.includes("pluralidad")) {
    const names = redFlags.length ? redFlags.map((flag) => flag.name).join(", ") : "No se detectaron red flags";
    const evidence = topFlag?.evidence ? `Evidencia principal: ${topFlag.evidence}.` : "";
    return `${names}. ${evidence} Lo más importante es revisar el soporte documental antes de sacar conclusiones.`;
  }

  if (normalized.includes("proveedor") || normalized.includes("supplier")) {
    return `El proveedor es ${contract.supplier || "sin proveedor"}. Aparece de forma visible en ${supplierFrequency} contratos del universo filtrado. ${connectionCount ? `También hay ${connectionCount} conexiones visibles en la trazabilidad.` : "No hay conexiones adicionales visibles."}`;
  }

  if (normalized.includes("resumen") || normalized.includes("explica")) {
    return `${detailModel?.chatbotContext?.summary || "No hay resumen disponible."} El reporte visible incluye ${patternCount} patrones riesgosos y ${connectionCount} conexiones o fuentes relacionadas.`;
  }

  if (normalized.includes("fuente") || normalized.includes("conexion") || normalized.includes("extern")) {
    return `Las conexiones visibles incluyen contratos del mismo proveedor, de la misma entidad, del mismo departamento y objetos similares. Fuentes externas sugeridas: SECOP II, Datos Abiertos Colombia, Colombia Compra Eficiente, RUES, Contraloría y Procuraduría. ${limitations}`;
  }

  if (normalized.includes("auditoria") || normalized.includes("pregunta")) {
    return [
      "Preguntas de auditoría sugeridas:",
      "1. ¿El expediente contiene soportes completos y consistentes?",
      "2. ¿La modalidad y la competencia reportada justifican el valor contratado?",
      "3. ¿Las adiciones, modificaciones y la trazabilidad del proveedor son coherentes?",
    ].join(" ");
  }

  if (normalized.includes("dato") || normalized.includes("falt") || normalized.includes("limitac")) {
    return `No hay información suficiente en los datos cargados para responder con precisión. ${limitations}`;
  }

  return "No hay información suficiente en los datos cargados para responder con precisión. Puedes preguntarme por el score, las red flags, el proveedor, las conexiones o el resumen del informe.";
}

function pushChatMessage(role, text) {
  state.chatMessages.push({
    role,
    text: String(text || ""),
  });
}

function getSelectedContractFromState() {
  return (
    state.sortedContracts.find((record) => record.contract_id === state.selectedId) ||
    state.filteredContracts.find((record) => record.contract_id === state.selectedId) ||
    state.contracts.find((record) => record.contract_id === state.selectedId) ||
    null
  );
}

function focusSection(selector) {
  const element = document.getElementById(selector);
  if (!element || typeof element.scrollIntoView !== "function") {
    return;
  }

  element.scrollIntoView({ behavior: "smooth", block: "start" });
}

function handleSearchSubmit(event) {
  event.preventDefault();
  updateFiltersFromDom();
  window.setTimeout(() => focusSection("contractsTableBody"), 0);
}

function handleChatSubmit(event) {
  event.preventDefault();
  const contract = getSelectedContractFromState();
  if (!contract || dom.chatInput?.disabled) {
    return;
  }

  const query = String(dom.chatInput?.value || "").trim();
  if (!query) {
    return;
  }

  pushChatMessage("user", query);
  const reply = buildChatReply(query, contract, state.detailModel);
  pushChatMessage("assistant", reply);
  dom.chatInput.value = "";
  renderChatbotSection(contract);
}

function handleChatQuickAction(event) {
  const button = event.target.closest("[data-chat-query]");
  if (!button) {
    return;
  }

  const contract = getSelectedContractFromState();
  if (!contract) {
    return;
  }

  const query = String(button.dataset.chatQuery || "").trim();
  if (!query) {
    return;
  }

  pushChatMessage("user", query);
  const reply = buildChatReply(query, contract, state.detailModel);
  pushChatMessage("assistant", reply);
  if (dom.chatInput) {
    dom.chatInput.value = "";
  }
  renderChatbotSection(contract);
}

function handleChatKeydown(event) {
  if (event.key !== "Enter" || event.shiftKey) {
    return;
  }

  event.preventDefault();
  dom.chatForm?.requestSubmit();
}

function handleTableClick(event) {
  const actionButton = event.target.closest("[data-row-action]");
  const row = event.target.closest("[data-contract-id]");
  if (!row) {
    return;
  }

  state.selectedId = row.dataset.contractId;
  render();

  if (actionButton) {
    const action = actionButton.dataset.rowAction;
    if (action === "open-analysis" || action === "open-report" || action === "open-connections") {
      window.setTimeout(() => focusSection("detailPanel"), 0);
    }
  }
}

function handlePaginationClick(event) {
  const button = event.target.closest("[data-page-number], [data-page-action]");
  if (!button) {
    return;
  }

  if (button.dataset.pageNumber) {
    state.page = Number(button.dataset.pageNumber);
    render();
    return;
  }

  if (button.dataset.pageAction === "prev") {
    state.page = Math.max(1, state.page - 1);
    render();
    return;
  }

  if (button.dataset.pageAction === "next") {
    const totalPages = Math.max(1, Math.ceil(state.filteredContracts.length / state.config.page_size));
    state.page = Math.min(totalPages, state.page + 1);
    render();
  }
}

function handleTableSortClick(event) {
  const button = event.target.closest("[data-sort-key]");
  if (!button) {
    return;
  }

  const nextKey = button.dataset.sortKey;
  if (state.sort.key === nextKey) {
    state.sort.direction = state.sort.direction === "asc" ? "desc" : "asc";
  } else {
    state.sort.key = nextKey;
    state.sort.direction = nextKey === "contract_id" || nextKey === "entity" ? "asc" : "desc";
  }

  state.page = 1;
  render();
}

function handleDetailClick(event) {
  const actionButton = event.target.closest("[data-detail-action]");
  if (!actionButton || !state.detailModel) {
    return;
  }

  if (actionButton.dataset.detailAction === "copy-summary") {
    copyDetailSummary();
  }
}

async function copyDetailSummary() {
  try {
    const ok = await copyTextToClipboard(state.detailModel.summaryText);
    if (!ok) {
      throw new Error("No clipboard");
    }
  } catch {
    // No-op.
  }
}

function resolveSelectedContract(sorted, visiblePageItems) {
  if (!sorted.length) {
    return null;
  }

  const visibleMatch = visiblePageItems.find((record) => record.contract_id === state.selectedId);
  if (visibleMatch) {
    return visibleMatch;
  }

  const existingSelection = sorted.find((record) => record.contract_id === state.selectedId);
  if (existingSelection) {
    return existingSelection;
  }

  state.selectedId = sorted[0].contract_id;
  return sorted[0];
}

function syncFiltersToDom() {
  if (!state.config || !state.options) {
    return;
  }

  populateFilterOptions();
  dom.periodSelect.value = state.filters.period;
}

function populateFilterOptions() {
  if (!state.options) {
    return;
  }

  populateSelect(dom.periodSelect, [
    { value: "all", label: "Todo el periodo" },
    ...state.options.periods.map((period) => ({ value: period, label: `Vigencia ${period}` })),
  ]);

  populateSelect(dom.entityFilter, [
    { value: "all", label: "Todas las entidades" },
    ...state.options.entities.map((value) => ({ value, label: value })),
  ]);

  populateSelect(dom.supplierFilter, [
    { value: "all", label: "Todos los proveedores" },
    ...state.options.suppliers.map((value) => ({ value, label: value })),
  ]);

  populateSelect(dom.departmentFilter, [
    { value: "all", label: "Todos los departamentos" },
    ...state.options.departments.map((value) => ({ value, label: value })),
  ]);

  populateSelect(dom.municipalityFilter, [
    { value: "all", label: "Todos los municipios" },
    ...state.options.municipalities.map((value) => ({ value, label: value })),
  ]);

  populateSelect(dom.modalityFilter, [
    { value: "all", label: "Todas las modalidades" },
    ...state.options.modalities.map((value) => ({ value, label: value })),
  ]);

  populateSelect(dom.redFlagFilter, [
    { value: "all", label: "Todas las red flags" },
    ...state.options.redFlags.map((flag) => ({ value: flag.code, label: flag.name })),
  ]);
}

function populateSelect(select, options) {
  if (!select) {
    return;
  }

  const currentValue = select.value || options[0]?.value || "all";
  select.innerHTML = options
    .map((option) => `<option value="${escapeHtml(option.value)}">${escapeHtml(option.label)}</option>`)
    .join("");
  select.value = options.some((option) => option.value === currentValue) ? currentValue : options[0]?.value || "all";
}

function updateFiltersFromDom() {
  state.filters = {
    ...state.filters,
    period: dom.periodSelect?.value || "all",
    query: dom.searchQuery?.value || "",
    entity: dom.entityFilter?.value || "all",
    supplier: dom.supplierFilter?.value || "all",
    object: dom.objectFilter?.value || "",
    department: dom.departmentFilter?.value || "all",
    municipality: dom.municipalityFilter?.value || "all",
    modality: dom.modalityFilter?.value || "all",
    riskLevel: dom.riskLevelFilter?.value || "all",
    redFlag: dom.redFlagFilter?.value || "all",
    scoreMin: dom.scoreMinFilter?.value || "",
    scoreMax: dom.scoreMaxFilter?.value || "",
    valueMin: dom.valueMinFilter?.value || "",
    valueMax: dom.valueMaxFilter?.value || "",
    dateFrom: dom.dateFromFilter?.value || "",
    dateTo: dom.dateToFilter?.value || "",
  };

  state.page = 1;
  render();
}

function resetFilters() {
  state.filters = createDefaultFilters(state.config);
  state.page = 1;
  syncFiltersToDom();
  render();
}

function exportFilteredCsv() {
  if (!state.filteredContracts.length) {
    return;
  }

  const csv = buildCsv(state.sortedContracts);
  downloadTextFile("dashboard_opacidad_pae_filtrado.csv", csv, "text/csv;charset=utf-8");
}

function trendLabel(delta, prefix, year) {
  const sign = delta > 0 ? "+" : "";
  return `${sign}${formatNumber(delta)}% ${prefix} ${year}`;
}

function setText(element, value) {
  if (element) {
    element.textContent = value;
  }
}
