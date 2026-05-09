import {
  buildSeriesMap,
  mean,
  median,
  monthLabel,
  normalizeText,
  sum,
  uniqueSorted,
} from "./utils.js";

const LEVEL_ORDER = {
  Bajo: 1,
  Medio: 2,
  Alto: 3,
  Critico: 4,
};

const SEVERITY_ORDER = {
  Baja: 1,
  Media: 2,
  Alta: 3,
};

export function calculateKpis(records, config) {
  const totalContracts = records.length;
  const totalValue = sum(records.map((record) => record.final_value));
  const averageRisk = totalContracts ? mean(records.map((record) => record.risk_score)) : 0;
  const highRiskCount = records.filter(
    (record) => record.risk_level === "Alto" || record.risk_level === "Critico" || record.risk_score >= config.risk_thresholds.high[0],
  ).length;
  const mediumRiskCount = records.filter((record) => record.risk_level === "Medio").length;
  const lowRiskCount = records.filter((record) => record.risk_level === "Bajo").length;
  const totalRedFlags = sum(records.map((record) => record.red_flag_count));
  const contractsWithFlags = records.filter((record) => record.red_flag_count > 0).length;

  const years = records.map((record) => record.year).filter((year) => Number.isFinite(year));
  const currentYear = years.length ? Math.max(...years) : 0;
  const previousYear = currentYear ? currentYear - 1 : 0;
  const currentYearRecords = records.filter((record) => record.year === currentYear);
  const previousYearRecords = records.filter((record) => record.year === previousYear);

  const currentYearStats = buildYearStats(currentYearRecords);
  const previousYearStats = buildYearStats(previousYearRecords);

  const topRiskEntity = pickTopEntity(records);
  const topAlertSupplier = pickTopSupplier(records);
  const topModality = pickTopModality(records);

  return {
    totalContracts,
    totalValue,
    averageRisk,
    highRiskCount,
    mediumRiskCount,
    lowRiskCount,
    totalRedFlags,
    contractsWithFlags,
    topRiskEntity,
    topAlertSupplier,
    topModality,
    trend: {
      currentYear,
      previousYear,
      currentYearStats,
      previousYearStats,
      averageRiskDelta: currentYearStats.averageRisk - previousYearStats.averageRisk,
      valueDeltaPct: percentChange(currentYearStats.totalValue, previousYearStats.totalValue),
      highRiskDelta: currentYearStats.highRiskCount - previousYearStats.highRiskCount,
      redFlagDelta: currentYearStats.totalRedFlags - previousYearStats.totalRedFlags,
    },
  };
}

export function calculateRiskDistribution(records, config) {
  const buckets = [
    {
      key: "Bajo",
      label: "Bajo",
      min: config.risk_thresholds.low[0],
      max: config.risk_thresholds.low[1],
      color: config.risk_colors.low,
      value: 0,
    },
    {
      key: "Medio",
      label: "Medio",
      min: config.risk_thresholds.medium[0],
      max: config.risk_thresholds.medium[1],
      color: config.risk_colors.medium,
      value: 0,
    },
    {
      key: "Alto",
      label: "Alto",
      min: config.risk_thresholds.high[0],
      max: config.risk_thresholds.high[1],
      color: config.risk_colors.high,
      value: 0,
    },
  ];

  for (const record of records) {
    const bucket = buckets.find((entry) => record.risk_score >= entry.min && record.risk_score <= entry.max) || buckets[0];
    bucket.value += 1;
  }

  const total = sum(buckets.map((bucket) => bucket.value)) || 1;
  return buckets.map((bucket) => ({
    ...bucket,
    percent: (bucket.value / total) * 100,
  }));
}

export function buildHistogram(records, binSize = 10) {
  const bins = [];
  for (let start = 0; start <= 90; start += binSize) {
    const isLast = start + binSize >= 100;
    bins.push({
      min: start,
      max: Math.min(start + binSize - 1, 100),
      label: `${start}-${isLast ? 100 : Math.min(start + binSize - 1, 100)}`,
      value: 0,
    });
  }

  for (const record of records) {
    const score = record.risk_score;
    const index = Math.min(Math.floor(score / binSize), bins.length - 1);
    bins[index].value += 1;
  }

  return bins;
}

export function calculateRedFlagSummary(records, config) {
  const counts = new Map();
  const departments = new Map();
  const flagCatalog = Array.isArray(config.red_flags) ? config.red_flags : [];

  for (const record of records) {
    const department = record.department || "Sin departamento";
    if (!departments.has(department)) {
      departments.set(department, new Map());
    }

    for (const flag of record.red_flag_details) {
      counts.set(flag.code, (counts.get(flag.code) || 0) + 1);
      const departmentMap = departments.get(department);
      departmentMap.set(flag.code, (departmentMap.get(flag.code) || 0) + 1);
    }
  }

  const frequency = flagCatalog
    .map((flag) => ({
      code: flag.code,
      name: flag.name,
      description: flag.description,
      severity: flag.severity,
      count: counts.get(flag.code) || 0,
      color: flag.color || severityColor(flag.severity, config),
    }))
    .filter((flag) => flag.count > 0)
    .sort((left, right) => right.count - left.count || severityScore(right.severity) - severityScore(left.severity));

  const topDepartments = uniqueSorted(
    records
      .map((record) => record.department)
      .filter(Boolean),
  )
    .map((department) => {
      const group = records.filter((record) => record.department === department);
      const totalFlags = sum(group.map((record) => record.red_flag_count));
      return {
        department,
        totalFlags,
        averageRisk: group.length ? mean(group.map((record) => record.risk_score)) : 0,
      };
    })
    .sort((left, right) => right.totalFlags - left.totalFlags || right.averageRisk - left.averageRisk)
    .slice(0, 6);

  const topFlags = frequency.slice(0, 6).map((flag) => flag.code);
  const matrix = topDepartments.map((departmentItem) => {
    const departmentMap = departments.get(departmentItem.department) || new Map();
    return topFlags.map((code) => departmentMap.get(code) || 0);
  });

  return {
    frequency,
    topDepartments,
    topFlags,
    matrix,
    summaryRows: frequency.slice(0, 10),
  };
}

export function buildComparisonDatasets(records, config) {
  const departmentRisk = buildAverageScoreSeries(records, (record) => record.department, config.chart_palette);
  const entityRisk = buildAverageScoreSeries(records, (record) => record.entity, config.chart_palette);
  const modalityCounts = buildCountSeries(records, (record) => record.modality, config.chart_palette);
  const supplierAlerts = buildSupplierSeries(records, config.chart_palette);
  const topContracts = [...records]
    .sort((left, right) => right.risk_score - left.risk_score || right.final_value - left.final_value)
    .slice(0, 10)
    .map((record) => ({
      label: record.contract_id,
      value: record.risk_score,
      subtitle: `${record.entity} · ${record.department}`,
      color: config.risk_colors.high,
    }));
  const additionPressure = [...records]
    .sort((left, right) => right.addition_percentage - left.addition_percentage || right.value_gap - left.value_gap)
    .slice(0, 8)
    .map((record) => ({
      label: record.contract_id,
      value: record.addition_percentage,
      subtitle: `${record.entity} · ${record.modifications_count} modificaciones`,
      color: config.risk_colors.medium,
    }));
  const entityAlerts = buildEntityAlertSeries(records, config.chart_palette);
  const timeline = buildTimelineSeries(records, config.chart_palette);
  const scatter = records.map((record) => ({
    label: record.contract_id,
    x: Math.max(record.final_value, 1),
    y: record.risk_score,
    subtitle: `${record.entity} · ${record.department}`,
    color:
      record.risk_level === "Alto" || record.risk_level === "Critico"
        ? config.risk_colors.high
        : record.risk_level === "Medio"
          ? config.risk_colors.medium
          : config.risk_colors.low,
  }));

  return {
    departmentRisk,
    entityRisk,
    modalityCounts,
    supplierAlerts,
    topContracts,
    additionPressure,
    entityAlerts,
    timeline,
    scatter,
  };
}

export function calculateImpactMetrics(records, kpis) {
  const prioritizedContracts = records.filter((record) => record.risk_score >= 60).length;
  const reviewHours = records.length * 1.6;
  const prioritizedHours = prioritizedContracts * 0.8;
  const hoursSaved = Math.max(0, reviewHours - prioritizedHours);
  const highRiskValue = sum(records.filter((record) => record.risk_score >= 70).map((record) => record.final_value));
  const recordsWithFlags = records.filter((record) => record.red_flag_count > 0).length;
  const topFlag = kpis.totalRedFlags
    ? records
        .flatMap((record) => record.red_flag_details)
        .reduce((accumulator, flag) => {
          if (!accumulator[flag.code]) {
            accumulator[flag.code] = {
              code: flag.code,
              name: flag.name,
              count: 0,
            };
          }
          accumulator[flag.code].count += 1;
          return accumulator;
        }, {})
    : {};

  const topCriticalFlag = Object.values(topFlag)
    .sort((left, right) => right.count - left.count)
    .slice(0, 1)[0] || { name: "Sin alertas criticas", count: 0 };

  return {
    prioritizedContracts,
    hoursSaved,
    highRiskValue,
    relevantRate: records.length ? 100 : 0,
    coverageRate: records.length ? Math.round((recordsWithFlags / records.length) * 100) : 0,
    topCriticalFlag,
    recordsWithFlags,
  };
}

function buildYearStats(records) {
  return {
    averageRisk: records.length ? mean(records.map((record) => record.risk_score)) : 0,
    totalValue: sum(records.map((record) => record.final_value)),
    highRiskCount: records.filter((record) => record.risk_score >= 70).length,
    totalRedFlags: sum(records.map((record) => record.red_flag_count)),
  };
}

function pickTopEntity(records) {
  const map = buildSeriesMap(
    records.filter((record) => record.risk_score >= 70),
    (record) => record.entity || "Sin entidad",
    () => 1,
  );

  return pickTopEntry(map);
}

function pickTopSupplier(records) {
  const map = buildSeriesMap(records, (record) => record.supplier || "Sin proveedor", (record) => record.red_flag_count);
  return pickTopEntry(map);
}

function pickTopModality(records) {
  const map = buildSeriesMap(records, (record) => record.modality || "Sin modalidad", () => 1);
  return pickTopEntry(map);
}

function pickTopEntry(map) {
  const entries = [...map.entries()].map(([label, count]) => ({ label, count }));
  entries.sort((left, right) => right.count - left.count || normalizeText(left.label).localeCompare(normalizeText(right.label), "es"));
  return entries[0] || { label: "Sin dato", count: 0 };
}

function buildAverageScoreSeries(records, keyFn, palette) {
  const groups = groupRecords(records, keyFn);
  return [...groups.entries()]
    .map(([label, items], index) => ({
      label,
      value: items.length ? mean(items.map((record) => record.risk_score)) : 0,
      subtitle: `${items.length} contratos`,
      count: items.length,
      color: palette[index % palette.length],
    }))
    .sort((left, right) => right.value - left.value || right.count - left.count)
    .slice(0, 8);
}

function buildCountSeries(records, keyFn, palette) {
  const groups = groupRecords(records, keyFn);
  return [...groups.entries()]
    .map(([label, items], index) => ({
      label,
      value: items.length,
      subtitle: `${Math.round(mean(items.map((record) => record.risk_score)))} de score promedio`,
      count: items.length,
      color: palette[index % palette.length],
    }))
    .sort((left, right) => right.value - left.value || normalizeText(left.label).localeCompare(normalizeText(right.label), "es"))
    .slice(0, 8);
}

function buildSupplierSeries(records, palette) {
  const groups = groupRecords(records, (record) => record.supplier || "Sin proveedor");
  return [...groups.entries()]
    .map(([label, items], index) => ({
      label,
      value: sum(items.map((record) => record.red_flag_count)),
      subtitle: `${items.length} contratos · score ${Math.round(mean(items.map((record) => record.risk_score)))}`,
      count: items.length,
      color: palette[index % palette.length],
    }))
    .sort((left, right) => right.value - left.value || right.count - left.count)
    .slice(0, 8);
}

function buildEntityAlertSeries(records, palette) {
  const groups = groupRecords(records, (record) => record.entity || "Sin entidad");
  return [...groups.entries()]
    .map(([label, items], index) => ({
      label,
      value: sum(items.map((record) => record.red_flag_count)),
      subtitle: `${items.length} contratos`,
      count: items.length,
      color: palette[index % palette.length],
    }))
    .sort((left, right) => right.value - left.value || right.count - left.count)
    .slice(0, 10);
}

function buildTimelineSeries(records, palette) {
  const groups = new Map();
  for (const record of records) {
    const key = record.month || "Sin fecha";
    if (!groups.has(key)) {
      groups.set(key, []);
    }
    groups.get(key).push(record);
  }

  return [...groups.entries()]
    .map(([month, items]) => ({
      label: month,
      displayLabel: month === "Sin fecha" ? "Sin fecha" : monthLabel(`${month}-01`),
      contracts: items.length,
      alerts: sum(items.map((record) => record.red_flag_count)),
      averageRisk: items.length ? mean(items.map((record) => record.risk_score)) : 0,
    }))
    .sort((left, right) => left.label.localeCompare(right.label))
    .map((item, index) => ({
      ...item,
      color: palette[index % palette.length],
    }));
}

function groupRecords(records, keyFn) {
  const groups = new Map();
  for (const record of records) {
    const key = keyFn(record) || "Sin dato";
    if (!groups.has(key)) {
      groups.set(key, []);
    }
    groups.get(key).push(record);
  }
  return groups;
}

function severityScore(severity) {
  return SEVERITY_ORDER[severity] || 0;
}

function severityColor(severity, config) {
  switch (severity) {
    case "Alta":
      return config.risk_colors.high;
    case "Media":
      return config.risk_colors.medium;
    default:
      return config.risk_colors.low;
  }
}

function percentChange(current, previous) {
  if (!previous) {
    return 0;
  }

  return ((current - previous) / previous) * 100;
}
