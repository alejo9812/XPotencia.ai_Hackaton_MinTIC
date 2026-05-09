import { normalizeText, toNumber, uniqueSorted } from "./utils.js";

export function createDefaultFilters(config) {
  return {
    period: config?.period_default || "all",
    query: "",
    entity: "all",
    supplier: "all",
    object: "",
    department: "all",
    municipality: "all",
    modality: "all",
    riskLevel: "all",
    redFlag: "all",
    scoreMin: "",
    scoreMax: "",
    valueMin: "",
    valueMax: "",
    dateFrom: "",
    dateTo: "",
  };
}

export function buildFilterOptions(contracts, config) {
  const years = uniqueSorted(
    contracts
      .map((contract) => contract.year)
      .filter((year) => year !== null && year !== undefined)
      .map((year) => String(year)),
  ).sort((left, right) => Number(right) - Number(left));

  return {
    periods: years,
    entities: uniqueSorted(contracts.map((contract) => contract.entity)),
    suppliers: uniqueSorted(contracts.map((contract) => contract.supplier)),
    departments: uniqueSorted(contracts.map((contract) => contract.department)),
    municipalities: uniqueSorted(contracts.map((contract) => contract.municipality)),
    modalities: uniqueSorted(contracts.map((contract) => contract.modality)),
    redFlags: Array.isArray(config?.red_flags) ? config.red_flags : [],
  };
}

export function applyFilters(contracts, filters) {
  const query = normalizeText(filters.query);
  const objectQuery = normalizeText(filters.object);

  return contracts.filter((contract) => {
    if (filters.period !== "all" && String(contract.year) !== String(filters.period)) {
      return false;
    }

    if (filters.entity !== "all" && contract.entity !== filters.entity) {
      return false;
    }

    if (filters.supplier !== "all" && contract.supplier !== filters.supplier) {
      return false;
    }

    if (filters.department !== "all" && contract.department !== filters.department) {
      return false;
    }

    if (filters.municipality !== "all" && contract.municipality !== filters.municipality) {
      return false;
    }

    if (filters.modality !== "all" && contract.modality !== filters.modality) {
      return false;
    }

    if (filters.riskLevel !== "all" && normalizeText(contract.risk_level) !== normalizeText(filters.riskLevel)) {
      return false;
    }

    if (filters.redFlag !== "all" && !contract.red_flags.includes(filters.redFlag)) {
      return false;
    }

    if (filters.scoreMin !== "" && contract.risk_score < toNumber(filters.scoreMin)) {
      return false;
    }

    if (filters.scoreMax !== "" && contract.risk_score > toNumber(filters.scoreMax)) {
      return false;
    }

    if (filters.valueMin !== "" && contract.final_value < toNumber(filters.valueMin)) {
      return false;
    }

    if (filters.valueMax !== "" && contract.final_value > toNumber(filters.valueMax)) {
      return false;
    }

    if (filters.dateFrom && contract.start_date && contract.start_date < filters.dateFrom) {
      return false;
    }

    if (filters.dateTo && contract.start_date && contract.start_date > filters.dateTo) {
      return false;
    }

    if (query && !contract.search_blob.includes(query)) {
      return false;
    }

    if (objectQuery && !normalizeText(contract.object).includes(objectQuery)) {
      return false;
    }

    return true;
  });
}

export function buildPeriodLabel(filters) {
  if (filters.period !== "all") {
    return `Vigencia ${filters.period}`;
  }

  if (filters.dateFrom || filters.dateTo) {
    const start = filters.dateFrom || "inicio";
    const end = filters.dateTo || "fin";
    return `Rango ${start} a ${end}`;
  }

  return "Todo el periodo";
}
