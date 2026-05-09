import {
  csvEscape,
  escapeHtml,
  formatDate,
  formatMoney,
  formatNumber,
  normalizeText,
  parseDate,
} from "./utils.js";

const RISK_ORDER = {
  Bajo: 1,
  Medio: 2,
  Alto: 3,
  Critico: 4,
};

export function sortContracts(contracts, sortState) {
  const key = sortState?.key || "risk_score";
  const direction = sortState?.direction === "asc" ? 1 : -1;

  return [...contracts].sort((left, right) => {
    if (key === "risk_score") {
      const priorityComparison = compareRiskPriority(left, right);
      return priorityComparison * direction;
    }

    const a = getSortValue(left, key);
    const b = getSortValue(right, key);

    let comparison = 0;
    if (typeof a === "string" && typeof b === "string") {
      comparison = normalizeText(a).localeCompare(normalizeText(b), "es");
    } else {
      comparison = toComparableNumber(a) - toComparableNumber(b);
    }

    return comparison * direction;
  });
}

export function paginateContracts(contracts, page, pageSize) {
  const totalPages = Math.max(1, Math.ceil(contracts.length / pageSize));
  const currentPage = Math.min(Math.max(1, page), totalPages);
  const startIndex = (currentPage - 1) * pageSize;
  const items = contracts.slice(startIndex, startIndex + pageSize);

  return {
    items,
    totalPages,
    currentPage,
    startIndex,
  };
}

export function buildContractsTableMarkup(records, sortState, selectedId) {
  if (!records.length) {
    return `
      <div class="empty-state">
        <strong>No hay contratos con estos filtros.</strong>
        <p>Prueba ampliar el rango de score, quitar una red flag o limpiar la búsqueda.</p>
      </div>
    `;
  }

  return `
    <div class="table-shell">
      <table class="contracts-table">
        <thead>
          <tr>
            ${buildSortHeader("ID contrato", "contract_id", sortState)}
            ${buildSortHeader("Departamento", "department", sortState)}
            ${buildSortHeader("Municipio", "municipality", sortState)}
            ${buildSortHeader("Entidad contratante", "entity", sortState)}
            ${buildSortHeader("Proveedor", "supplier", sortState)}
            ${buildSortHeader("Valor", "final_value", sortState)}
            ${buildSortHeader("Score de riesgo", "risk_score", sortState)}
            ${buildSortHeader("Red flags principales", "red_flag_count", sortState)}
            <th>Acción</th>
          </tr>
        </thead>
        <tbody>
          ${records
            .map((record) => {
              const levelClass = normalizeText(record.risk_level);
              const activeClass = record.contract_id === selectedId ? "is-active" : "";
              const flagPreview = buildFlagPreview(record);
              return `
                <tr class="${activeClass}" data-contract-id="${escapeHtml(record.contract_id)}">
                  <td>
                    <strong>${escapeHtml(record.contract_id)}</strong>
                  </td>
                  <td>
                    <strong>${escapeHtml(record.department || "Sin departamento")}</strong>
                  </td>
                  <td>
                    <strong>${escapeHtml(record.municipality || "Sin municipio")}</strong>
                  </td>
                  <td>
                    <strong>${escapeHtml(record.entity || "Sin entidad")}</strong>
                    <div class="table-subtle">${escapeHtml(record.status || "Sin estado")}</div>
                  </td>
                  <td>
                    <strong>${escapeHtml(record.supplier || "Sin proveedor")}</strong>
                    <div class="table-subtle">${escapeHtml(record.modality || "Sin modalidad")}</div>
                  </td>
                  <td>
                    <strong>${escapeHtml(formatMoney(record.final_value))}</strong>
                    <div class="table-subtle">Inicial ${escapeHtml(formatMoney(record.initial_value))}</div>
                  </td>
                  <td>
                    <span class="risk-badge ${levelClass}">${escapeHtml(formatNumber(record.risk_score))}</span>
                    <div class="table-subtle">${escapeHtml(record.risk_level || "Sin nivel")}</div>
                  </td>
                  <td class="table-object">
                    ${flagPreview}
                  </td>
                  <td>
                    <div class="row-actions">
                      <button type="button" class="link-button" data-row-action="open-analysis" data-contract-id="${escapeHtml(record.contract_id)}">
                        Ver análisis
                      </button>
                      <button type="button" class="link-button" data-row-action="open-report" data-contract-id="${escapeHtml(record.contract_id)}">
                        Generar reporte
                      </button>
                      <button type="button" class="link-button" data-row-action="open-connections" data-contract-id="${escapeHtml(record.contract_id)}">
                        Consultar conexiones
                      </button>
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

export function buildPaginationMarkup(page, totalPages, totalRecords, pageSize) {
  return `
    <div class="pagination">
      <div class="pagination-summary">
        Mostrando página ${formatNumber(page)} de ${formatNumber(totalPages)} · ${formatNumber(totalRecords)} contratos · ${formatNumber(pageSize)} por página
      </div>
      <div class="pagination-controls">
        <button type="button" class="pagination-button" data-page-action="prev" ${page <= 1 ? "disabled" : ""}>Anterior</button>
        ${buildPageButtons(page, totalPages)}
        <button type="button" class="pagination-button" data-page-action="next" ${page >= totalPages ? "disabled" : ""}>Siguiente</button>
      </div>
    </div>
  `;
}

export function buildCsv(records) {
  const headers = [
    "contract_id",
    "entity",
    "entity_nit",
    "supplier",
    "supplier_nit",
    "department",
    "municipality",
    "object",
    "initial_value",
    "final_value",
    "addition_value",
    "addition_percentage",
    "modality",
    "status",
    "start_date",
    "end_date",
    "risk_score",
    "risk_level",
    "red_flags",
    "recommended_action",
    "limitations",
    "additions_count",
    "modifications_count",
    "bidder_count",
    "offer_count",
  ];

  const rows = [headers.join(",")];
  for (const record of records) {
    rows.push(
      [
        record.contract_id,
        record.entity,
        record.entity_nit,
        record.supplier,
        record.supplier_nit,
        record.department,
        record.municipality,
        record.object,
        record.initial_value,
        record.final_value,
        record.addition_value,
        record.addition_percentage,
        record.modality,
        record.status,
        record.start_date,
        record.end_date,
        record.risk_score,
        record.risk_level,
        (record.red_flag_details || []).map((flag) => flag.name).join(" | "),
        record.recommended_action || "",
        record.limitations || record.risk_limitations || "",
        record.additions_count,
        record.modifications_count,
        record.bidder_count,
        record.offer_count,
      ]
        .map(csvEscape)
        .join(","),
    );
  }

  return rows.join("\n");
}

function buildSortHeader(label, key, sortState) {
  const active = sortState?.key === key;
  const direction = active ? sortState.direction : "";
  return `
    <th>
      <button type="button" class="table-sort" data-sort-key="${escapeHtml(key)}" aria-label="Ordenar por ${escapeHtml(label)}">
        <span>${escapeHtml(label)}</span>
        <span class="sort-state">${active ? direction.toUpperCase() : "ORD"}</span>
      </button>
    </th>
  `;
}

function buildPageButtons(page, totalPages) {
  const buttons = [];
  const pages = buildVisiblePages(page, totalPages);

  for (const item of pages) {
    if (item === "...") {
      buttons.push(`<span class="pagination-ellipsis">...</span>`);
      continue;
    }

    buttons.push(`
      <button type="button" class="pagination-button ${item === page ? "is-active" : ""}" data-page-number="${item}">
        ${formatNumber(item)}
      </button>
    `);
  }

  return buttons.join("");
}

function buildVisiblePages(page, totalPages) {
  const visible = new Set([1, totalPages, page - 1, page, page + 1]);
  const ordered = Array.from(visible)
    .filter((value) => value >= 1 && value <= totalPages)
    .sort((left, right) => left - right);

  const result = [];
  let previous = 0;

  for (const value of ordered) {
    if (previous && value - previous > 1) {
      result.push("...");
    }
    result.push(value);
    previous = value;
  }

  return result;
}

function getSortValue(record, key) {
  switch (key) {
    case "contract_id":
      return record.contract_id;
    case "entity":
      return record.entity;
    case "supplier":
      return record.supplier;
    case "department":
      return record.department;
    case "municipality":
      return record.municipality;
    case "object":
      return record.object;
    case "modality":
      return record.modality;
    case "status":
      return record.status;
    case "risk_score":
      return record.risk_score;
    case "final_value":
      return record.final_value;
    case "initial_value":
      return record.initial_value;
    case "addition_percentage":
      return record.addition_percentage;
    case "addition_value":
      return record.addition_value;
    case "red_flag_count":
      return record.red_flag_count;
    case "start_date":
      return parseDate(record.start_date)?.getTime() || 0;
    case "end_date":
      return parseDate(record.end_date)?.getTime() || 0;
    case "risk_level":
      return RISK_ORDER[record.risk_level] || 0;
    default:
      return record[key] ?? 0;
  }
}

function compareRiskPriority(left, right) {
  let comparison = toComparableNumber(left.risk_score) - toComparableNumber(right.risk_score);
  if (comparison !== 0) {
    return comparison;
  }

  comparison = toComparableNumber(left.red_flag_count) - toComparableNumber(right.red_flag_count);
  if (comparison !== 0) {
    return comparison;
  }

  comparison = toComparableNumber(left.additions_count) - toComparableNumber(right.additions_count);
  if (comparison !== 0) {
    return comparison;
  }

  comparison = toComparableNumber(left.final_value) - toComparableNumber(right.final_value);
  if (comparison !== 0) {
    return comparison;
  }

  comparison = toComparableNumber(right.offer_count) - toComparableNumber(left.offer_count);
  if (comparison !== 0) {
    return comparison;
  }

  comparison = toComparableNumber(right.bidder_count) - toComparableNumber(left.bidder_count);
  if (comparison !== 0) {
    return comparison;
  }

  return 0;
}

function buildFlagPreview(record) {
  const details = Array.isArray(record.red_flag_details) ? record.red_flag_details : [];
  if (!details.length) {
    return `<span class="table-subtle">Sin alertas visibles</span>`;
  }

  const names = details
    .slice(0, 3)
    .map((flag) => escapeHtml(flag.name || flag.code || "Red flag"))
    .join("<br />");

  const count = details.length > 3 ? `<div class="table-subtle">+${escapeHtml(formatNumber(details.length - 3))} mas</div>` : "";
  return `
    <div class="table-flag-stack">
      <strong>${names}</strong>
      ${count}
    </div>
  `;
}

function toComparableNumber(value) {
  if (typeof value === "number") {
    return value;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}
