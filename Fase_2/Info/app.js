import {
  escapeHtml,
  formatDate,
  formatMoney,
  formatNumber,
  formatPercent,
  loadCatalogs,
  loadSourceRecords,
  normalizeContractRecord,
  normalizeText,
} from "./secop-api.js";
import {
  buildAuditQuestions,
  buildDifyPayload,
  buildGroupedSeries,
  buildIndicatorSummary,
  buildSignalNarrative,
  scoreContracts,
  sortRecords,
} from "./risk-engine.js";
import {
  buildAuditChecklist,
  buildPdfReportModel,
  buildMethodologyCopy,
  buildPrintableTitle,
  buildPromptPreview,
  buildSelectionContext,
} from "./report.js";
import {
  buildHtmlReport,
  buildMarkdownReport,
  buildReportFilename,
  downloadTextFile,
} from "./report-export.js";
import { buildConversationStarters, renderDifyWidget } from "./dify-widget.js";
import {
  buildDifyApiRequest,
  buildDifyApiStatusText,
  formatDifyApiResponse,
  sendDifyApiRequest,
} from "./dify-api.js";

const STORAGE_KEY = "gobia.auditor.custom.contracts.v1";
const DIFY_STORAGE_KEY = "gobia.auditor.dify.config.v1";
const DIFY_CONVERSATION_STORAGE_KEY = "gobia.auditor.dify.conversation.v1";
const DEFAULT_STATE = {
  dataMode: "mock",
  selectedSourceId: "secop_contracts",
  selectedIndicatorId: "risk",
  groupBy: "entity",
  statusFilter: "all",
  search: "",
  topN: 8,
};

const state = {
  catalogs: null,
  sources: [],
  indicators: [],
  riskRules: null,
  difyConfig: null,
  customRecords: [],
  scoredRecords: [],
  summary: null,
  visibleRecords: [],
  groupedSeries: [],
  selectedRecordId: "",
  difyConversationId: "",
  difyApiBusy: false,
  difyApiResult: null,
  difyApiError: "",
  currentReport: null,
  lastUpdated: "",
  busy: false,
  message: "Listo para analizar contratos SECOP II.",
  ...DEFAULT_STATE,
};

const dom = {};

document.addEventListener("DOMContentLoaded", bootstrap);

async function bootstrap() {
  cacheDom();
  bindEvents();
  await loadInitialData();
  await refreshAnalysis({ preserveSelection: false });
}

function cacheDom() {
  const ids = [
    "sourceCards",
    "indicatorSelect",
    "groupSelect",
    "statusSelect",
    "searchInput",
    "topNInput",
    "topNValue",
    "dataModeSelect",
    "manualSourceSelect",
    "manualForm",
    "manualEntity",
    "manualProvider",
    "manualModality",
    "manualAmount",
    "manualDate",
    "manualDepartment",
    "manualMunicipality",
    "manualParticipants",
    "manualOffers",
    "manualStatus",
    "manualDescription",
    "manualResult",
    "methodologyList",
    "kpiGrid",
    "chartTitle",
    "chartDescription",
    "chartSeries",
    "tableCount",
    "contractsTableBody",
    "contractDetail",
    "signalList",
    "auditQuestions",
    "difyConfigForm",
    "difyEnabledInput",
    "difyWebAppUrlInput",
    "difyTitleInput",
    "difyIframeHeightInput",
    "difyResetConfigBtn",
    "difyConfigStatus",
    "difyWidget",
    "difyStarters",
    "difyApiSendBtn",
    "difyApiCopyBtn",
    "difyApiResetBtn",
    "difyApiStatus",
    "difyApiResponse",
    "downloadReportHtmlBtn",
    "downloadReportMdBtn",
    "executiveSummary",
    "indicatorCards",
    "statusSource",
    "statusIndicator",
    "statusMode",
    "statusCount",
    "statusRisk",
    "statusUpdated",
    "printBtn",
    "copyBtn",
    "refreshBtn",
    "filterForm",
  ];

  for (const id of ids) {
    dom[id] = document.getElementById(id);
  }
}

function bindEvents() {
  dom.printBtn?.addEventListener("click", () => window.print());
  dom.copyBtn?.addEventListener("click", copySummaryToClipboard);
  dom.refreshBtn?.addEventListener("click", () => refreshAnalysis({ preserveSelection: true }));

  dom.dataModeSelect?.addEventListener("change", async (event) => {
    state.dataMode = event.target.value;
    clearDifyApiOutcome();
    await refreshAnalysis({ preserveSelection: true });
  });

  dom.indicatorSelect?.addEventListener("change", (event) => {
    state.selectedIndicatorId = event.target.value;
    clearDifyApiOutcome();
    const indicator = getActiveIndicator();
    if (indicator?.groupHint && !state.groupBy) {
      state.groupBy = indicator.groupHint;
      dom.groupSelect.value = indicator.groupHint;
    }
    render();
  });

  dom.groupSelect?.addEventListener("change", (event) => {
    state.groupBy = event.target.value;
    clearDifyApiOutcome();
    render();
  });

  dom.statusSelect?.addEventListener("change", (event) => {
    state.statusFilter = event.target.value;
    clearDifyApiOutcome();
    render();
  });

  dom.searchInput?.addEventListener("input", (event) => {
    state.search = event.target.value;
    clearDifyApiOutcome();
    render();
  });

  dom.topNInput?.addEventListener("input", (event) => {
    state.topN = Number(event.target.value);
    dom.topNValue.textContent = String(state.topN);
    clearDifyApiOutcome();
    render();
  });

  dom.sourceCards?.addEventListener("click", async (event) => {
    const card = event.target.closest("[data-source-id]");
    if (!card) {
      return;
    }

    state.selectedSourceId = card.dataset.sourceId;
    syncManualSource(state.selectedSourceId);
    clearDifyApiOutcome();
    await refreshAnalysis({ preserveSelection: false });
  });

  dom.indicatorCards?.addEventListener("click", (event) => {
    const card = event.target.closest("[data-indicator-id]");
    if (!card) {
      return;
    }

    state.selectedIndicatorId = card.dataset.indicatorId;
    dom.indicatorSelect.value = state.selectedIndicatorId;
    const indicator = getActiveIndicator();
    if (indicator?.groupHint) {
      state.groupBy = indicator.groupHint;
      dom.groupSelect.value = indicator.groupHint;
    }
    render();
  });

  dom.manualForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    addManualRecord();
    clearDifyApiOutcome();
    await refreshAnalysis({ preserveSelection: false, notice: "Registro manual agregado y analizado localmente." });
    dom.manualForm.reset();
    dom.manualParticipants.value = "1";
    dom.manualOffers.value = "1";
    dom.manualStatus.value = "Adjudicado";
    syncManualSource(state.selectedSourceId);
  });

  dom.contractsTableBody?.addEventListener("click", (event) => {
    const row = event.target.closest("[data-record-id]");
    if (!row) {
      return;
    }

    state.selectedRecordId = row.dataset.recordId;
    state.difyApiResult = null;
    state.difyApiError = "";
    render();
  });

  dom.filterForm?.addEventListener("submit", (event) => event.preventDefault());

  dom.difyConfigForm?.addEventListener("submit", (event) => {
    event.preventDefault();
    saveDifyConfigFromForm();
  });

  dom.difyResetConfigBtn?.addEventListener("click", resetDifyConfig);
  dom.difyApiSendBtn?.addEventListener("click", sendSelectedContractToDifyApi);
  dom.difyApiCopyBtn?.addEventListener("click", copyDifyApiPayload);
  dom.difyApiResetBtn?.addEventListener("click", resetDifyConversation);
  dom.downloadReportHtmlBtn?.addEventListener("click", () => downloadCurrentReport("html"));
  dom.downloadReportMdBtn?.addEventListener("click", () => downloadCurrentReport("md"));
}

async function loadInitialData() {
  const catalogs = await loadCatalogs();
  state.catalogs = catalogs;
  state.sources = catalogs.sources;
  state.indicators = catalogs.indicators;
  state.riskRules = catalogs.riskRules;
  state.difyConfig = mergeDifyConfig(catalogs.difyConfig, loadSavedDifyConfig());
  state.difyConversationId = loadSavedDifyConversationId();
  state.customRecords = loadCustomRecords();

  renderStaticSelects();
  renderSourceCards();
  renderIndicatorCards();
  renderMethodology();
  syncDifyConfigForm();

  state.selectedIndicatorId = state.indicators.find((indicator) => indicator.id === state.selectedIndicatorId)?.id ?? state.indicators[0]?.id ?? "risk";
  state.selectedSourceId = state.sources.find((source) => source.id === state.selectedSourceId)?.id ?? state.sources[1]?.id ?? "secop_contracts";
  state.groupBy = getActiveIndicator()?.groupHint ?? state.groupBy;

  dom.dataModeSelect.value = state.dataMode;
  dom.indicatorSelect.value = state.selectedIndicatorId;
  dom.groupSelect.value = state.groupBy;
  dom.statusSelect.value = state.statusFilter;
  dom.topNInput.value = String(state.topN);
  dom.topNValue.textContent = String(state.topN);
  syncManualSource(state.selectedSourceId);
}

function renderStaticSelects() {
  dom.indicatorSelect.innerHTML = state.indicators
    .map(
      (indicator) =>
        `<option value="${escapeHtml(indicator.id)}">${escapeHtml(indicator.label)}</option>`,
    )
    .join("");

  dom.manualSourceSelect.innerHTML = state.sources
    .filter((source) => source.id !== "all")
    .map(
      (source) =>
        `<option value="${escapeHtml(source.id)}">${escapeHtml(source.label)}</option>`,
    )
    .join("");
}

function renderSourceCards() {
  dom.sourceCards.innerHTML = state.sources
    .map((source) => {
      const active = source.id === state.selectedSourceId ? "active" : "";
      const tags = (source.focus || []).map((tag) => `<span class="tag">${escapeHtml(tag)}</span>`).join("");
      return `
        <button type="button" class="source-card ${active}" data-source-id="${escapeHtml(source.id)}">
          <strong>${escapeHtml(source.label)}</strong>
          <p>${escapeHtml(source.description || "")}</p>
          <div class="tag-row">
            <span class="tag">Dataset: ${escapeHtml(source.datasetId || "multi")}</span>
            ${tags}
          </div>
        </button>
      `;
    })
    .join("");
}

function renderIndicatorCards() {
  dom.indicatorCards.innerHTML = state.indicators
    .map((indicator) => {
      const active = indicator.id === state.selectedIndicatorId ? "active" : "";
      return `
        <button type="button" class="indicator-card ${active}" data-indicator-id="${escapeHtml(indicator.id)}">
          <strong>${escapeHtml(indicator.label)}</strong>
          <p>${escapeHtml(indicator.description || "")}</p>
          <div class="tag-row">
            <span class="tag">${escapeHtml(indicator.chartLabel || indicator.label)}</span>
            <span class="tag">${escapeHtml(indicator.question || "")}</span>
          </div>
        </button>
      `;
    })
    .join("");
}

function renderMethodology() {
  const items = buildMethodologyCopy();
  dom.methodologyList.innerHTML = items.map((item) => `<li>${escapeHtml(item)}</li>`).join("");
}

async function refreshAnalysis({ preserveSelection = true, notice = "" } = {}) {
  state.busy = true;
  setActionMessage(notice || "Recalculando indicadores y ranking.");

  const source = getActiveSource();
  const baseRecords = await loadSourceRecords(source, state.dataMode);
  const customRecords = getCustomRecordsForSource(source.id);
  const mergedRecords = [...baseRecords, ...customRecords];

  const normalizedRecords = mergedRecords.map((record, index) =>
    normalizeContractRecord(record, source, index),
  );
  const scored = scoreContracts(normalizedRecords, state.riskRules);
  state.scoredRecords = scored.records;
  state.summary = scored.summary;
  state.lastUpdated = new Date().toISOString();

  const filtered = getFilteredRecords();
  state.visibleRecords = filtered;
  const previousSelection = state.selectedRecordId;
  const nextSelection = resolveSelection(filtered, preserveSelection);
  state.selectedRecordId = nextSelection;
  if (nextSelection !== previousSelection) {
    state.difyApiResult = null;
    state.difyApiError = "";
  }
  state.groupedSeries = buildGroupedSeries(
    filtered,
    state.groupBy,
    state.selectedIndicatorId,
    state.topN,
  );

  render();
  state.busy = false;
}

function render() {
  const source = getActiveSource();
  const indicator = getActiveIndicator();
  const filtered = getFilteredRecords();
  const visible = filtered.slice(0, state.topN);
  const selected = resolveSelectedRecord(visible, filtered);
  const chartSeries = buildGroupedSeries(filtered, state.groupBy, state.selectedIndicatorId, state.topN);
  const summary = state.summary ?? {
    totalRecords: 0,
    totalAmount: 0,
    averageAmount: 0,
    averageRisk: 0,
    completeness: 0,
    highRiskCount: 0,
    mediumRiskCount: 0,
    lowRiskCount: 0,
    topSignal: "Sin señales dominantes",
  };

  state.visibleRecords = filtered;
  state.groupedSeries = chartSeries;

  renderStatus(source, indicator, filtered, selected, summary);
  renderKpis(summary, selected, filtered.length);
  renderChart(indicator, chartSeries);
  renderTable(visible, selected, summary);
  renderDetailPanel(selected, indicator, source, summary, chartSeries);
  renderExecutivePanel(selected, indicator, source, summary, chartSeries);
  renderDifyPanel(selected, indicator, source, summary);
  renderIndicatorGallery(indicator, chartSeries);
  renderSignalsPanel(selected);

  dom.tableCount.textContent = filtered.length
    ? `Mostrando ${visible.length} de ${filtered.length} registros`
    : "Sin registros filtrados";

  if (!selected) {
    renderEmptyState();
  }
}

function renderStatus(source, indicator, filtered, selected, summary) {
  dom.statusSource.textContent = `Fuente: ${source.label}`;
  dom.statusIndicator.textContent = `Indicador: ${indicator.label}`;
  dom.statusMode.textContent = `Modo: ${buildModeLabel()}`;
  dom.statusCount.textContent = `Contratos: ${filtered.length}/${summary.totalRecords}`;
  dom.statusRisk.textContent = selected
    ? `Riesgo: ${selected.score}/100 (${selected.level})`
    : "Riesgo: --";
  dom.statusUpdated.textContent = `Actualizado: ${formatTimestamp(state.lastUpdated)}`;
}

function renderKpis(summary, selected, visibleCount) {
  const kpis = [
    {
      label: "Registros analizados",
      value: formatNumber(summary.totalRecords),
      hint: `Universo cargado desde ${getActiveSource().label}.`,
    },
    {
      label: "Valor total",
      value: formatMoney(summary.totalAmount),
      hint: `Suma de los montos visibles.`,
    },
    {
      label: "Riesgo promedio",
      value: `${Math.round(summary.averageRisk)}/100`,
      hint: `Promedio del score deterministico.`,
    },
    {
      label: "Completitud",
      value: `${Math.round(summary.completeness)}%`,
      hint: `Campos criticos presentes en el conjunto.`,
    },
    {
      label: "Altos / visibles",
      value: `${summary.highRiskCount}/${visibleCount}`,
      hint: `Casos de revision prioritaria.`,
    },
  ];

  dom.kpiGrid.innerHTML = kpis
    .map(
      (kpi) => `
        <article class="kpi-card">
          <p class="kpi-label">${escapeHtml(kpi.label)}</p>
          <p class="kpi-value">${escapeHtml(kpi.value)}</p>
          <p class="kpi-hint">${escapeHtml(kpi.hint)}</p>
        </article>
      `,
    )
    .join("");
}

function renderChart(indicator, chartSeries) {
  dom.chartTitle.textContent = indicator.chartLabel || indicator.label;
  dom.chartDescription.textContent = indicator.study || indicator.description;

  if (!chartSeries.length) {
    dom.chartSeries.innerHTML = `
      <article class="summary-card">
        <p>No hay categorias suficientes para construir la grafica.</p>
      </article>
    `;
    return;
  }

  const maxValue = Math.max(...chartSeries.map((item) => item.value), 1);
  dom.chartSeries.innerHTML = chartSeries
    .map((item) => {
      const width = Math.max(8, Math.round((item.value / maxValue) * 100));
      const valueLabel =
        state.selectedIndicatorId === "value"
          ? formatMoney(item.value)
          : state.selectedIndicatorId === "quality"
            ? `${Math.round(item.value)}%`
            : state.selectedIndicatorId === "competition"
              ? `${Math.round(item.value)}%`
              : state.selectedIndicatorId === "timeline"
                ? formatMoney(item.value)
                : `${Math.round(item.value)}/100`;

      return `
        <div class="bar-row">
          <div class="bar-meta">
            <strong>${escapeHtml(item.label)}</strong>
            <span>${escapeHtml(valueLabel)} | ${escapeHtml(item.labelDetail)}</span>
          </div>
          <div class="bar-track">
            <div class="bar-fill" style="width: ${width}%"></div>
          </div>
        </div>
      `;
    })
    .join("");
}

function renderTable(records, selected, summary) {
  if (!records.length) {
    dom.contractsTableBody.innerHTML = `
      <tr>
        <td colspan="6">
          <div class="summary-card">No hay registros despues de aplicar los filtros.</div>
        </td>
      </tr>
    `;
    return;
  }

  dom.contractsTableBody.innerHTML = records
    .map((record) => {
      const active = selected?.id === record.id ? "active" : "";
      return `
        <tr class="${active}" data-record-id="${escapeHtml(record.id)}">
          <td>
            <span class="score-chip ${record.level}">${record.score}/100</span>
          </td>
          <td>
            <strong>${escapeHtml(record.id)}</strong>
            <div class="helper">${escapeHtml(record.status || "Sin estado")}</div>
          </td>
          <td>
            <strong>${escapeHtml(record.entity || "Sin entidad")}</strong>
            <div class="helper">${escapeHtml(record.provider || "Sin proveedor")}</div>
          </td>
          <td>${escapeHtml(formatMoney(record.amount))}</td>
          <td>
            <strong>${escapeHtml(String(record.signals.length))}</strong>
            <div class="helper">${escapeHtml(record.riskLabel)}</div>
          </td>
          <td>${escapeHtml(formatDate(record.date))}</td>
        </tr>
      `;
    })
    .join("");
}

function renderDetailPanel(selected, indicator, source, summary, chartSeries) {
  if (!selected) {
    dom.contractDetail.innerHTML = `
      <article class="summary-card">
        <p>No hay contrato seleccionado. Cambia los filtros o carga mas registros.</p>
      </article>
    `;
    dom.signalList.innerHTML = "";
    dom.auditQuestions.innerHTML = "";
    return;
  }

  const context = buildSelectionContext(selected);
  const summaryText = buildIndicatorSummary(state.selectedIndicatorId, summary, selected);
  const auditQuestions = buildAuditQuestions(
    selected.signals,
    [
      indicator.question,
      "Que documentos soportan este contrato?",
      "Que justifica la modalidad y el monto?",
    ],
  );

  dom.contractDetail.innerHTML = `
    <article class="record-card">
      <div class="record-title">${escapeHtml(selected.id)}</div>
      <div class="record-meta">
        <span>${escapeHtml(source.label)} | ${escapeHtml(selected.level.toUpperCase())}</span>
        <span>${escapeHtml(summaryText)}</span>
      </div>
      <div class="record-grid">
        ${context
          .map(
            (item) => `
              <div>
                <span>${escapeHtml(item.label)}</span>
                <strong>${escapeHtml(item.value)}</strong>
              </div>
            `,
          )
          .join("")}
      </div>
    </article>
  `;

  dom.signalList.innerHTML = selected.signals.length
    ? selected.signals
        .map(
          (signal) => `
            <article class="signal-card">
              <div class="signal-head">
                <strong>${escapeHtml(signal.label)}</strong>
                <span class="level-pill ${selected.level}">${escapeHtml(selected.level)}</span>
              </div>
              <p>${escapeHtml(signal.detail)}</p>
            </article>
          `,
        )
        .join("")
    : `
      <article class="summary-card">
        <p>No se detectaron señales relevantes para este contrato.</p>
      </article>
    `;

  dom.auditQuestions.innerHTML = auditQuestions.map((item) => `<li>${escapeHtml(item)}</li>`).join("");
}

function renderSignalsPanel(selected) {
  if (!selected) {
    dom.signalList.innerHTML = "";
    dom.auditQuestions.innerHTML = "";
    return;
  }
}

function renderExecutivePanel(selected, indicator, source, summary, chartSeries) {
  if (!selected) {
    state.currentReport = null;
    dom.executiveSummary.innerHTML = `
      <article class="summary-card">
        <p>Selecciona un contrato para generar el informe listo para PDF.</p>
      </article>
    `;
    return;
  }

  const topGroup = chartSeries[0] ?? null;
  const report = buildPdfReportModel({
    sourceLabel: source.label,
    indicatorLabel: indicator.label,
    stats: summary,
    selectedRecord: selected,
    topGroup,
    auditChecklist: buildAuditChecklist(selected.signals, selected),
    auditQuestions: buildAuditQuestions(selected.signals, [indicator.question]),
    conversationId: state.difyConversationId,
    difyAnswer: state.difyApiResult?.answer || "",
    difyStatus: buildDifyApiStatusText(state.difyConfig, state.difyConversationId),
    methodology: buildMethodologyCopy(),
    signalNarrative: buildSignalNarrative(selected.signals),
    lastUpdated: state.lastUpdated,
  });
  state.currentReport = report;

  dom.executiveSummary.innerHTML = `
    <article class="pdf-report">
      <header class="pdf-report-header">
        <div>
          <div class="panel-tag">Informe para PDF</div>
          <h3>${escapeHtml(report.title)}</h3>
          <p class="pdf-report-subtitle">${escapeHtml(report.subtitle)}</p>
        </div>
        <div class="pdf-report-meta">
          ${report.meta
            .map(
              (item) => `
                <div class="report-chip">
                  <span>${escapeHtml(item.label)}</span>
                  <strong>${escapeHtml(item.value)}</strong>
                </div>
              `,
            )
            .join("")}
        </div>
      </header>

      <div class="report-stats">
        ${report.stats
          .map(
            (item) => `
              <div class="report-stat">
                <span>${escapeHtml(item.label)}</span>
                <strong>${escapeHtml(item.value)}</strong>
              </div>
            `,
          )
          .join("")}
      </div>

      <div class="pdf-report-grid">
        <section class="pdf-report-section pdf-report-wide">
          <h4>Resumen ejecutivo</h4>
          <p>${escapeHtml(report.overview)}</p>
          <p>${escapeHtml(report.indicatorNote)}</p>
          <p>${escapeHtml(report.signalNarrative)}</p>
        </section>

        <section class="pdf-report-section">
          <h4>Ficha del contrato</h4>
          <div class="report-facts">
            ${report.selection
              .map(
                (item) => `
                  <div class="report-fact">
                    <span>${escapeHtml(item.label)}</span>
                    <strong>${escapeHtml(item.value)}</strong>
                  </div>
                `,
              )
              .join("")}
          </div>
        </section>

        <section class="pdf-report-section">
          <h4>Señales detectadas</h4>
          <ul class="report-list">
            ${
              report.signals.length
                ? report.signals
                    .map(
                      (signal) => `
                        <li><strong>${escapeHtml(signal.label)}:</strong> ${escapeHtml(signal.detail)}</li>
                      `,
                    )
                    .join("")
                : "<li>No se detectaron señales dominantes en el caso seleccionado.</li>"
            }
          </ul>
        </section>

        <section class="pdf-report-section">
          <h4>Preguntas para auditor humano</h4>
          <ul class="report-list">
            ${report.auditQuestions.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}
          </ul>
        </section>

        <section class="pdf-report-section">
          <h4>Metodología y contexto</h4>
          <ul class="report-list">
            ${report.methodology.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}
            ${report.auditChecklist.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}
          </ul>
          <div class="report-callout">
            <strong>Señal dominante</strong>
            <p>${escapeHtml(report.topSignal)}</p>
          </div>
        </section>

        <section class="pdf-report-section">
          <h4>Lectura de Dify</h4>
          <p>${escapeHtml(report.difyStatus)}</p>
          <p>${escapeHtml(report.difyAnswer || "La respuesta de Dify aparecerá aqui cuando envíes el contrato por API.")}</p>
          <div class="report-callout">
            <strong>Conversación</strong>
            <p>${escapeHtml(report.conversationId || "nueva")}</p>
          </div>
        </section>

        <section class="pdf-report-section pdf-report-wide report-callout">
          <strong>Recomendación final</strong>
          <p>${escapeHtml(report.recommendation)}</p>
          <p>${escapeHtml(`Actualizado: ${report.meta.find((item) => item.label === "Actualizado")?.value || "--"}`)}</p>
        </section>
      </div>
    </article>
  `;
}

function renderDifyPanel(selected, indicator, source, summary) {
  updateDifyConfigStatus();

  if (!selected) {
    dom.difyWidget.innerHTML = `
      <article class="summary-card">
        <p>Selecciona un contrato para activar el contexto del agente Dify.</p>
      </article>
    `;
    if (dom.difyStarters) {
      dom.difyStarters.innerHTML = "";
    }
    renderDifyApiPanel(null, indicator, source, summary);
    return;
  }

  const payload = buildDifyPayload(
    selected,
    {
      summary,
      signals: selected.signals,
      auditQuestions: buildAuditQuestions(selected.signals, [indicator.question]),
    },
    indicator.label,
    source.label,
  );

  renderDifyWidget(dom.difyWidget, state.difyConfig, selected, {
    signals: selected.signals,
    auditQuestions: payload.analysis.auditQuestions,
    summary,
  }, indicator.label);

  const starters = buildConversationStarters(selected, {
    signals: selected.signals,
    auditQuestions: payload.analysis.auditQuestions,
  });
  if (dom.difyStarters) {
    dom.difyStarters.innerHTML = starters.map((item) => `<li>${escapeHtml(item)}</li>`).join("");
  }

  renderDifyApiPanel(selected, indicator, source, summary);
}

function renderDifyApiPanel(selected, indicator, source, summary) {
  const apiEnabledText = buildDifyApiStatusText(state.difyConfig, state.difyConversationId);
  if (dom.difyApiStatus) {
    dom.difyApiStatus.textContent = selected
      ? apiEnabledText
      : "Selecciona un contrato para preparar el envio a Dify API.";
  }

  if (!dom.difyApiResponse) {
    return;
  }

  if (!selected) {
    dom.difyApiResponse.innerHTML = `
      <article class="summary-card">
        <p>Cuando selecciones un contrato, aqui apareceran el payload y la respuesta de Dify API.</p>
      </article>
    `;
    return;
  }

  const analysis = {
    signals: selected.signals,
    auditQuestions: buildAuditQuestions(selected.signals, [indicator.question]),
    summary,
  };
  const requestBundle = buildDifyApiRequest(selected, analysis, indicator.label, source.label, state.difyConfig, state.difyConversationId);
  const requestPreview = JSON.stringify(requestBundle.request, null, 2);

  if (state.difyApiBusy) {
    dom.difyApiResponse.innerHTML = `
      <article class="api-response-card">
        <div class="api-response-head">
          <strong>Enviando a Dify API</strong>
          <span class="detail-pill">Procesando</span>
        </div>
        <p class="api-response-body">Se esta enviando el contrato seleccionado al proxy local.</p>
      </article>
    `;
    return;
  }

  if (state.difyApiError) {
    dom.difyApiResponse.innerHTML = `
      <article class="api-response-card">
        <div class="api-response-head">
          <strong>Respuesta de Dify</strong>
          <span class="detail-pill">Error</span>
        </div>
        <p class="api-response-body">${escapeHtml(state.difyApiError)}</p>
        <details class="api-response-details">
          <summary>Payload enviado</summary>
          <pre>${escapeHtml(requestPreview)}</pre>
        </details>
      </article>
    `;
    return;
  }

  if (state.difyApiResult) {
    const result = state.difyApiResult;
    const answer = result.answer || "Dify no devolvio un texto de respuesta.";
    const metadataRows = [
      { label: "Conversacion", value: result.conversationId || state.difyConversationId || "nueva" },
      { label: "Task", value: result.taskId || "sin task" },
      { label: "Mensaje", value: result.messageId || "sin mensaje" },
    ];

    dom.difyApiResponse.innerHTML = `
      <article class="api-response-card">
        <div class="api-response-head">
          <strong>Respuesta de Dify</strong>
          <span class="detail-pill">Listo</span>
        </div>
        <p class="api-response-body">${escapeHtml(answer)}</p>
        <div class="api-response-meta">
          ${metadataRows
            .map(
              (item) => `
                <div>
                  <span>${escapeHtml(item.label)}</span>
                  <strong>${escapeHtml(item.value)}</strong>
                </div>
              `,
            )
            .join("")}
        </div>
        <details class="api-response-details">
          <summary>Payload enviado</summary>
          <pre>${escapeHtml(requestPreview)}</pre>
        </details>
      </article>
    `;
    return;
  }

  dom.difyApiResponse.innerHTML = `
    <article class="api-response-card">
      <div class="api-response-head">
        <strong>Payload preparado</strong>
        <span class="detail-pill">Listo para enviar</span>
      </div>
      <p class="api-response-body">
        El contrato seleccionado ya tiene contexto, score y señales listos para enviarse por el proxy local.
      </p>
      <details class="api-response-details">
        <summary>Payload JSON</summary>
        <pre>${escapeHtml(requestPreview)}</pre>
      </details>
    </article>
  `;
}

function renderIndicatorGallery(indicator, chartSeries) {
  dom.indicatorCards.innerHTML = state.indicators
    .map((entry) => {
      const active = entry.id === indicator.id ? "active" : "";
      return `
        <button type="button" class="indicator-card ${active}" data-indicator-id="${escapeHtml(entry.id)}">
          <strong>${escapeHtml(entry.label)}</strong>
          <p>${escapeHtml(entry.description)}</p>
          <div class="tag-row">
            <span class="tag">${escapeHtml(entry.chartLabel)}</span>
            <span class="tag">${escapeHtml(entry.question)}</span>
          </div>
        </button>
      `;
    })
    .join("");
}

function renderEmptyState() {
  state.currentReport = null;
  dom.contractDetail.innerHTML = `
    <article class="summary-card">
      <p>No hay resultados para el filtro actual.</p>
    </article>
  `;
  dom.signalList.innerHTML = "";
  dom.auditQuestions.innerHTML = "";
  dom.executiveSummary.innerHTML = `
    <article class="summary-card">
      <p>El resumen ejecutivo aparece cuando existe al menos un contrato visible.</p>
    </article>
  `;
  dom.difyWidget.innerHTML = `
    <article class="summary-card">
      <p>El contexto de Dify se activara cuando haya un contrato visible.</p>
    </article>
  `;
  if (dom.difyStarters) {
    dom.difyStarters.innerHTML = "";
  }
}

function getFilteredRecords() {
  const query = normalizeText(state.search);
  const selectedSource = getActiveSource();
  const records = [...state.scoredRecords];

  const filtered = records.filter((record) => {
    if (state.statusFilter !== "all" && normalizeText(record.status) !== normalizeText(state.statusFilter)) {
      return false;
    }

    if (query) {
      const searchable = normalizeText(
        [
          record.id,
          record.entity,
          record.provider,
          record.modality,
          record.department,
          record.municipality,
          record.description,
          record.status,
          record.sourceLabel,
          ...record.signals.map((signal) => signal.label),
        ].join(" "),
      );
      if (!searchable.includes(query)) {
        return false;
      }
    }

    if (selectedSource.id !== "all" && record.sourceId !== selectedSource.id) {
      return false;
    }

    return true;
  });

  return sortRecords(filtered, state.selectedIndicatorId);
}

function resolveSelection(records, preserveSelection) {
  if (preserveSelection && state.selectedRecordId) {
    const exists = records.some((record) => record.id === state.selectedRecordId);
    if (exists) {
      return state.selectedRecordId;
    }
  }

  return records[0]?.id ?? "";
}

function resolveSelectedRecord(visible, filtered) {
  const candidates = [...visible, ...filtered];
  const selected =
    candidates.find((record) => record.id === state.selectedRecordId) ?? candidates[0] ?? null;
  if (selected && selected.id !== state.selectedRecordId) {
    state.selectedRecordId = selected.id;
  }
  return selected;
}

function getActiveSource() {
  return state.sources.find((source) => source.id === state.selectedSourceId) ?? state.sources[0];
}

function getActiveIndicator() {
  return state.indicators.find((indicator) => indicator.id === state.selectedIndicatorId) ?? state.indicators[0];
}

function loadCustomRecords() {
  try {
    const payload = localStorage.getItem(STORAGE_KEY);
    const parsed = payload ? JSON.parse(payload) : [];
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function getCustomRecordsForSource(sourceId) {
  const records = loadCustomRecords();
  if (sourceId === "all") {
    return records;
  }

  return records.filter((record) => record.sourceId === sourceId);
}

function saveCustomRecords(records) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(records));
  state.customRecords = records;
}

function addManualRecord() {
  const manualSourceId = dom.manualSourceSelect.value || state.selectedSourceId || "secop_contracts";
  const source = state.sources.find((item) => item.id === manualSourceId) ?? getActiveSource();
  const existing = loadCustomRecords();

  const record = {
    sourceId: manualSourceId,
    sourceLabel: source?.label || manualSourceId,
    id: `MAN-${Date.now()}`,
    entity: dom.manualEntity.value.trim(),
    provider: dom.manualProvider.value.trim(),
    modality: dom.manualModality.value.trim(),
    department: dom.manualDepartment.value.trim(),
    municipality: dom.manualMunicipality.value.trim(),
    amount: Number(dom.manualAmount.value || 0),
    currency: "COP",
    date: dom.manualDate.value,
    description: dom.manualDescription.value.trim(),
    status: dom.manualStatus.value.trim() || "Adjudicado",
    participants: Number(dom.manualParticipants.value || 0),
    offers: Number(dom.manualOffers.value || 0),
    durationDays: 0,
    origin: "manual",
  };

  existing.push(record);
  saveCustomRecords(existing);
  setManualMessage(`Registro ${record.id} guardado en ${source?.label || manualSourceId}.`);
}

function setManualMessage(message) {
  if (dom.manualResult) {
    dom.manualResult.textContent = message;
  }
}

function syncManualSource(sourceId) {
  const nextSource = sourceId === "all" ? "secop_contracts" : sourceId;
  dom.manualSourceSelect.value = nextSource;
}

function setActionMessage(message) {
  if (dom.manualResult) {
    dom.manualResult.textContent = message;
  }
}

function buildModeLabel() {
  const source = getActiveSource();
  if (state.dataMode === "live" && source.id === "all") {
    return "live fallback mock";
  }
  return state.dataMode === "live" ? "live preview" : "mock local";
}

function formatTimestamp(value) {
  if (!value) {
    return "--";
  }

  return new Intl.DateTimeFormat("es-CO", {
    hour: "2-digit",
    minute: "2-digit",
    day: "2-digit",
    month: "short",
  }).format(new Date(value));
}

function loadSavedDifyConfig() {
  try {
    const payload = localStorage.getItem(DIFY_STORAGE_KEY);
    const parsed = payload ? JSON.parse(payload) : {};
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function loadSavedDifyConversationId() {
  try {
    return localStorage.getItem(DIFY_CONVERSATION_STORAGE_KEY) || "";
  } catch {
    return "";
  }
}

function saveDifyConversationId(value) {
  const conversationId = String(value || "").trim();
  if (conversationId) {
    localStorage.setItem(DIFY_CONVERSATION_STORAGE_KEY, conversationId);
  } else {
    localStorage.removeItem(DIFY_CONVERSATION_STORAGE_KEY);
  }
  state.difyConversationId = conversationId;
}

function mergeDifyConfig(base = {}, override = {}) {
  return normalizeDifyConfig({ ...base, ...override });
}

function normalizeDifyConfig(config = {}) {
  const iframeHeight = Number(config.iframeHeight);
  return {
    enabled: Boolean(config.enabled),
    title: String(config.title || "GobIA Auditor").trim() || "GobIA Auditor",
    webAppUrl: String(config.webAppUrl || "").trim(),
    iframeHeight: Number.isFinite(iframeHeight) ? Math.max(420, Math.min(1200, iframeHeight)) : 760,
    apiProxyPath: String(config.apiProxyPath || "/api/dify/chat").trim() || "/api/dify/chat",
    apiUserId: String(config.apiUserId || "gobia-auditor-local").trim() || "gobia-auditor-local",
    apiQuery:
      String(config.apiQuery || "Analiza el contrato seleccionado usando el contexto proporcionado por la aplicacion.")
        .trim() ||
      "Analiza el contrato seleccionado usando el contexto proporcionado por la aplicacion.",
    apiResponseMode: String(config.apiResponseMode || "blocking").trim() || "blocking",
    intro:
      String(config.intro || "Explicacion automatica del contrato seleccionado.").trim() ||
      "Explicacion automatica del contrato seleccionado.",
    cta:
      String(config.cta || "Pega el URL publico del chatflow para activarlo aqui.").trim() ||
      "Pega el URL publico del chatflow para activarlo aqui.",
  };
}

function syncDifyConfigForm() {
  if (!dom.difyEnabledInput) {
    return;
  }

  dom.difyEnabledInput.checked = Boolean(state.difyConfig?.enabled);
  dom.difyWebAppUrlInput.value = state.difyConfig?.webAppUrl || "";
  dom.difyTitleInput.value = state.difyConfig?.title || "GobIA Auditor";
  dom.difyIframeHeightInput.value = String(state.difyConfig?.iframeHeight || 760);
  updateDifyConfigStatus();
}

function updateDifyConfigStatus() {
  if (!dom.difyConfigStatus) {
    return;
  }

  if (state.difyConfig?.enabled && state.difyConfig?.webAppUrl) {
    dom.difyConfigStatus.textContent = `Dify conectado con iframe embebido en ${state.difyConfig.iframeHeight}px.`;
    return;
  }

  if (state.difyConfig?.enabled) {
    dom.difyConfigStatus.textContent = "Activa el chatflow, pero falta pegar el URL publico de Dify.";
    return;
  }

  dom.difyConfigStatus.textContent = "Activa el chatflow y pega el URL publico para cargarlo aqui.";
}

function saveDifyConfigFromForm() {
  const nextConfig = normalizeDifyConfig({
    ...state.difyConfig,
    enabled: dom.difyEnabledInput?.checked ?? false,
    webAppUrl: dom.difyWebAppUrlInput?.value || "",
    title: dom.difyTitleInput?.value || "GobIA Auditor",
    iframeHeight: dom.difyIframeHeightInput?.value || 760,
  });

  state.difyConfig = nextConfig;
  localStorage.setItem(DIFY_STORAGE_KEY, JSON.stringify(nextConfig));
  updateDifyConfigStatus();
  setActionMessage(
    nextConfig.enabled && nextConfig.webAppUrl
      ? "Dify conectado. El iframe ya queda embebido en el panel."
      : "Configuracion de Dify guardada, pero aun falta completar la conexion.",
  );
  render();
}

function resetDifyConfig() {
  if (!state.catalogs?.difyConfig) {
    return;
  }

  localStorage.removeItem(DIFY_STORAGE_KEY);
  state.difyConfig = mergeDifyConfig(state.catalogs.difyConfig, {});
  syncDifyConfigForm();
  setActionMessage("Configuracion de Dify restaurada a los valores base.");
  render();
}

function resetDifyConversation() {
  saveDifyConversationId("");
  state.difyApiResult = null;
  state.difyApiError = "";
  setActionMessage("Conversacion de Dify reiniciada.");
  render();
}

function clearDifyApiOutcome() {
  state.difyApiResult = null;
  state.difyApiError = "";
}

function downloadCurrentReport(format) {
  if (!state.currentReport) {
    setActionMessage("Selecciona un contrato antes de descargar el informe.");
    return;
  }

  if (format === "md") {
    const content = buildMarkdownReport(state.currentReport);
    downloadTextFile(buildReportFilename(state.currentReport, "md"), content, "text/markdown;charset=utf-8");
    setActionMessage("Informe Markdown descargado.");
    return;
  }

  const content = buildHtmlReport(state.currentReport);
  downloadTextFile(buildReportFilename(state.currentReport, "html"), content, "text/html;charset=utf-8");
  setActionMessage("Informe HTML descargado.");
}

async function copyDifyApiPayload() {
  const selected = resolveSelectedRecord(state.visibleRecords.slice(0, state.topN), state.visibleRecords);
  if (!selected) {
    setActionMessage("Selecciona un contrato antes de copiar el payload de Dify API.");
    return;
  }

  const indicator = getActiveIndicator();
  const source = getActiveSource();
  const analysis = {
    signals: selected.signals,
    auditQuestions: buildAuditQuestions(selected.signals, [indicator.question]),
    summary: state.summary,
  };
  const requestBundle = buildDifyApiRequest(
    selected,
    analysis,
    indicator.label,
    source.label,
    state.difyConfig,
    state.difyConversationId,
  );

  try {
    await navigator.clipboard.writeText(JSON.stringify(requestBundle.request, null, 2));
    setActionMessage("Payload de Dify API copiado al portapapeles.");
  } catch {
    setActionMessage("No se pudo copiar el payload automaticamente.");
  }
}

async function sendSelectedContractToDifyApi() {
  const selected = resolveSelectedRecord(state.visibleRecords.slice(0, state.topN), state.visibleRecords);
  if (!selected) {
    setActionMessage("Selecciona un contrato antes de enviar a Dify API.");
    return;
  }

  const indicator = getActiveIndicator();
  const source = getActiveSource();
  const analysis = {
    signals: selected.signals,
    auditQuestions: buildAuditQuestions(selected.signals, [indicator.question]),
    summary: state.summary,
  };
  const requestBundle = buildDifyApiRequest(
    selected,
    analysis,
    indicator.label,
    source.label,
    state.difyConfig,
    state.difyConversationId,
  );

  state.difyApiBusy = true;
  state.difyApiError = "";
  state.difyApiResult = null;
  setActionMessage("Enviando el contrato a Dify API...");
  render();

  try {
    const response = await sendDifyApiRequest(requestBundle.endpoint, requestBundle.request);
    const formatted = formatDifyApiResponse(response);
    state.difyApiResult = formatted;
    state.difyApiError = "";
    if (formatted.conversationId) {
      saveDifyConversationId(formatted.conversationId);
    }
    setActionMessage("Dify API devolvio una respuesta para el contrato seleccionado.");
  } catch (error) {
    state.difyApiResult = null;
    state.difyApiError = error instanceof Error ? error.message : "No se pudo enviar el contrato a Dify API.";
    setActionMessage("No se pudo completar la llamada a Dify API.");
  } finally {
    state.difyApiBusy = false;
    render();
  }
}

async function copySummaryToClipboard() {
  const selected = resolveSelectedRecord(state.visibleRecords.slice(0, state.topN), state.visibleRecords);
  if (!selected) {
    setManualMessage("No hay resumen para copiar porque todavia no hay un contrato seleccionado.");
    return;
  }

  const indicator = getActiveIndicator();
  const source = getActiveSource();
  const payload = buildPromptPreview(
    selected,
    {
      signals: selected.signals,
      auditQuestions: buildAuditQuestions(selected.signals, [indicator.question]),
    },
    indicator.label,
  );

  const summary = [
    buildPrintableTitle(source.label, indicator.label),
    "",
    `Contrato: ${selected.id}`,
    `Entidad: ${selected.entity}`,
    `Proveedor: ${selected.provider}`,
    `Monto: ${formatMoney(selected.amount)}`,
    `Score: ${selected.score}/100 (${selected.level})`,
    `Señales: ${selected.signals.map((signal) => signal.label).join(", ") || "Sin señales"}`,
    "",
    "Preguntas de auditoria:",
    ...payload.analysis.auditQuestions.map((item) => `- ${item}`),
  ].join("\n");

  try {
    await navigator.clipboard.writeText(summary);
    setManualMessage("Resumen copiado al portapapeles.");
  } catch {
    setManualMessage("No se pudo copiar automaticamente. Usa el informe visible para hacerlo manualmente.");
  }
}
