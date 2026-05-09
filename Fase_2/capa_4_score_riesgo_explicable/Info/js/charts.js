import {
  escapeHtml,
  formatMoney,
  formatNumber,
  mean,
  normalizeText,
  sum,
  toNumber,
} from "./utils.js";

export function renderDoughnutChart(container, segments, options = {}) {
  if (!container) {
    return;
  }

  if (!segments.length || sum(segments.map((segment) => segment.value)) <= 0) {
    container.innerHTML = `<div class="empty-state compact"><strong>Sin datos suficientes para la dona.</strong></div>`;
    return;
  }

  const total = sum(segments.map((segment) => segment.value)) || 1;
  const slices = [];
  let cursor = 0;

  for (const segment of segments) {
    const share = (segment.value / total) * 100;
    const nextCursor = cursor + share;
    slices.push(`${segment.color || "#1d4ed8"} ${cursor}% ${nextCursor}%`);
    cursor = nextCursor;
  }

  const legend = segments
    .map(
      (segment) => `
        <li class="legend-item">
          <span class="legend-dot" style="background:${escapeHtml(segment.color || "#1d4ed8")}"></span>
          <div class="legend-copy">
            <strong>${escapeHtml(segment.label)}</strong>
            <span>${escapeHtml(formatNumber(segment.value))} contratos · ${escapeHtml(formatNumber(segment.percent || 0))}%</span>
          </div>
        </li>
      `,
    )
    .join("");

  container.innerHTML = `
    <div class="chart-block">
      <div class="donut-chart" style="--donut-gradient: conic-gradient(${slices.join(", ")})">
        <div class="donut-chart__center">
          <strong>${escapeHtml(formatNumber(total))}</strong>
          <span>${escapeHtml(options.centerLabel || "contratos")}</span>
          <small>${escapeHtml(options.centerNote || "")}</small>
        </div>
      </div>
      <ul class="legend-list">${legend}</ul>
    </div>
  `;
}

export function renderHistogram(container, bins, options = {}) {
  if (!container) {
    return;
  }

  if (!bins.length || sum(bins.map((bin) => bin.value)) <= 0) {
    container.innerHTML = `<div class="empty-state compact"><strong>Sin datos suficientes para el histograma.</strong></div>`;
    return;
  }

  const max = Math.max(...bins.map((bin) => bin.value), 1);
  container.innerHTML = `
    <div class="chart-block">
      <div class="histogram">
        <div class="histogram__bars">
          ${bins
            .map((bin) => {
              const height = Math.max(10, Math.round((bin.value / max) * 100));
              return `
                <div class="histogram__bin" title="${escapeHtml(bin.label)}: ${escapeHtml(formatNumber(bin.value))} contratos">
                  <span class="histogram__count">${escapeHtml(formatNumber(bin.value))}</span>
                  <div class="histogram__bar" style="height:${height}%"></div>
                  <span class="histogram__label">${escapeHtml(bin.label)}</span>
                </div>
              `;
            })
            .join("")}
        </div>
        <p class="chart-caption">${escapeHtml(options.caption || "Distribucion del score de riesgo en intervalos de 10 puntos.")}</p>
      </div>
    </div>
  `;
}

export function renderHorizontalBarChart(container, items, options = {}) {
  if (!container) {
    return;
  }

  if (!items.length || Math.max(...items.map((item) => item.value), 0) <= 0) {
    container.innerHTML = `<div class="empty-state compact"><strong>Sin datos para graficar.</strong></div>`;
    return;
  }

  const max = Math.max(...items.map((item) => item.value), 1);
  container.innerHTML = `
    <div class="chart-block">
      <div class="bar-chart">
        ${items
          .map((item) => {
            const width = Math.max(4, Math.round((item.value / max) * 100));
            const valueLabel = options.formatValue ? options.formatValue(item.value, item) : formatNumber(item.value);
            return `
              <div class="bar-row" title="${escapeHtml(item.label)}">
                <div class="bar-row__meta">
                  <div class="bar-row__copy">
                    <strong>${escapeHtml(item.label)}</strong>
                    <span>${escapeHtml(item.subtitle || "")}</span>
                  </div>
                  <span class="bar-row__value">${escapeHtml(valueLabel)}</span>
                </div>
                <div class="bar-track">
                  <div class="bar-fill" style="width:${width}%; background:${escapeHtml(item.color || options.color || "#1d4ed8")}"></div>
                </div>
              </div>
            `;
          })
          .join("")}
      </div>
      ${options.caption ? `<p class="chart-caption">${escapeHtml(options.caption)}</p>` : ""}
    </div>
  `;
}

export function renderHeatmap(container, rows, columns, matrix, options = {}) {
  if (!container) {
    return;
  }

  if (!rows.length || !columns.length) {
    container.innerHTML = `<div class="empty-state compact"><strong>Sin datos para mapa de calor.</strong></div>`;
    return;
  }

  const max = Math.max(...matrix.flat(), 1);
  const columnHeader = columns
    .map((column) => `<div class="heatmap-head__cell">${escapeHtml(column)}</div>`)
    .join("");

  const body = rows
    .map((row, rowIndex) => {
      const cells = columns
        .map((column, columnIndex) => {
          const value = matrix[rowIndex]?.[columnIndex] || 0;
          const intensity = value / max;
          const alpha = 0.08 + intensity * 0.82;
          return `
            <div
              class="heatmap-cell"
              title="${escapeHtml(row)} · ${escapeHtml(column)}: ${escapeHtml(formatNumber(value))}"
              style="background: rgba(29, 78, 216, ${alpha.toFixed(2)}); color: ${intensity > 0.55 ? "#ffffff" : "#0f2742"}"
            >
              ${escapeHtml(formatNumber(value))}
            </div>
          `;
        })
        .join("");

      return `
        <div class="heatmap-row" style="grid-template-columns: minmax(160px, 240px) repeat(${columns.length}, minmax(0, 1fr));">
          <div class="heatmap-row__label">${escapeHtml(row)}</div>
          ${cells}
        </div>
      `;
    })
    .join("");

  container.innerHTML = `
    <div class="chart-block">
      <div class="heatmap-grid">
        <div class="heatmap-head" style="grid-column: 1 / -1; grid-template-columns: minmax(160px, 240px) repeat(${columns.length}, minmax(0, 1fr));">
          <div class="heatmap-head--empty"></div>
          ${columnHeader}
        </div>
        ${body}
      </div>
      ${options.caption ? `<p class="chart-caption">${escapeHtml(options.caption)}</p>` : ""}
    </div>
  `;
}

export function renderScatterChart(container, points, options = {}) {
  if (!container) {
    return;
  }

  if (!points.length) {
    container.innerHTML = `<div class="empty-state compact"><strong>Sin puntos para el scatter plot.</strong></div>`;
    return;
  }

  const width = 760;
  const height = 420;
  const padding = { top: 20, right: 24, bottom: 52, left: 68 };
  const usableWidth = width - padding.left - padding.right;
  const usableHeight = height - padding.top - padding.bottom;
  const xValues = points.map((point) => Math.log10(Math.max(point.x, 1)));
  const minX = Math.min(...xValues);
  const maxX = Math.max(...xValues);
  const minY = 0;
  const maxY = 100;

  const xScale = (value) => padding.left + ((value - minX) / (maxX - minX || 1)) * usableWidth;
  const yScale = (value) => padding.top + (1 - (value - minY) / (maxY - minY)) * usableHeight;

  const gridLines = [0, 25, 50, 75, 100]
    .map((value) => {
      const y = yScale(value);
      return `
        <line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" class="svg-grid" />
        <text x="${padding.left - 12}" y="${y + 4}" text-anchor="end" class="svg-axis-label">${value}</text>
      `;
    })
    .join("");

  const verticalTicks = points.length > 1
    ? buildVerticalTicks(points, xScale, padding, width, height)
    : "";

  const dots = points
    .map((point) => {
      const x = xScale(Math.log10(Math.max(point.x, 1)));
      const y = yScale(point.y);
      return `
        <g class="scatter-point">
          <circle cx="${x}" cy="${y}" r="6" fill="${escapeHtml(point.color || "#1d4ed8")}" opacity="0.9">
            <title>${escapeHtml(point.label)} · ${escapeHtml(point.subtitle || "")} · ${escapeHtml(formatMoney(point.x))} · score ${escapeHtml(formatNumber(point.y))}</title>
          </circle>
        </g>
      `;
    })
    .join("");

  container.innerHTML = `
    <div class="chart-block">
      <svg class="svg-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(options.ariaLabel || "Grafico de dispersion")}" preserveAspectRatio="none">
        <line x1="${padding.left}" y1="${padding.top}" x2="${padding.left}" y2="${height - padding.bottom}" class="svg-axis" />
        <line x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}" class="svg-axis" />
        ${gridLines}
        ${verticalTicks}
        ${dots}
        <text x="${padding.left}" y="${height - 12}" class="svg-axis-title">${escapeHtml(options.xLabel || "Valor contratado (escala log)")}</text>
        <text x="18" y="${padding.top + 12}" class="svg-axis-title" transform="rotate(-90 18 ${padding.top + 12})">${escapeHtml(options.yLabel || "Score de riesgo")}</text>
      </svg>
      <p class="chart-caption">${escapeHtml(options.caption || "Relacion entre valor contratado y score de riesgo.")}</p>
    </div>
  `;
}

export function renderLineChart(container, series, options = {}) {
  if (!container) {
    return;
  }

  if (!series.length || !series[0]?.values?.length) {
    container.innerHTML = `<div class="empty-state compact"><strong>Sin datos para la linea temporal.</strong></div>`;
    return;
  }

  const width = 760;
  const height = 380;
  const padding = { top: 22, right: 20, bottom: 52, left: 64 };
  const usableWidth = width - padding.left - padding.right;
  const usableHeight = height - padding.top - padding.bottom;
  const labels = series[0].values.map((item) => item.label);
  const maxValue = Math.max(...series.flatMap((item) => item.values.map((point) => point.value)), 1);

  const xScale = (index) => padding.left + (index / Math.max(labels.length - 1, 1)) * usableWidth;
  const yScale = (value) => padding.top + (1 - value / maxValue) * usableHeight;

  const gridLines = [0, maxValue * 0.25, maxValue * 0.5, maxValue * 0.75, maxValue]
    .map((value) => {
      const y = yScale(value);
      return `
        <line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" class="svg-grid" />
        <text x="${padding.left - 10}" y="${y + 4}" text-anchor="end" class="svg-axis-label">${escapeHtml(formatNumber(Math.round(value)))}</text>
      `;
    })
    .join("");

  const polylines = series
    .map((line) => {
      const points = line.values
        .map((point, index) => `${xScale(index)},${yScale(point.value)}`)
        .join(" ");

      const circles = line.values
        .map((point, index) => {
          const x = xScale(index);
          const y = yScale(point.value);
          return `
            <circle cx="${x}" cy="${y}" r="4.5" fill="${escapeHtml(line.color || "#1d4ed8")}">
              <title>${escapeHtml(line.label)} · ${escapeHtml(point.label)}: ${escapeHtml(formatNumber(point.value))}</title>
            </circle>
          `;
        })
        .join("");

      return `
        <polyline fill="none" stroke="${escapeHtml(line.color || "#1d4ed8")}" stroke-width="3" points="${points}" />
        ${circles}
      `;
    })
    .join("");

  const tickLabels = labels
    .map((label, index) => {
      const x = xScale(index);
      return `<text x="${x}" y="${height - 18}" text-anchor="middle" class="svg-axis-label">${escapeHtml(label)}</text>`;
    })
    .join("");

  container.innerHTML = `
    <div class="chart-block">
      <svg class="svg-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(options.ariaLabel || "Grafico de linea")}" preserveAspectRatio="none">
        <line x1="${padding.left}" y1="${padding.top}" x2="${padding.left}" y2="${height - padding.bottom}" class="svg-axis" />
        <line x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}" class="svg-axis" />
        ${gridLines}
        ${polylines}
        ${tickLabels}
      </svg>
      <div class="legend-inline">
        ${series
          .map(
            (line) => `
              <span class="legend-chip">
                <span class="legend-dot" style="background:${escapeHtml(line.color || "#1d4ed8")}"></span>
                ${escapeHtml(line.label)}
              </span>
            `,
          )
          .join("")}
      </div>
      <p class="chart-caption">${escapeHtml(options.caption || "Evolucion mensual de contratos y alertas.")}</p>
    </div>
  `;
}

function buildVerticalTicks(points, xScale, padding, width, height) {
  return points
    .map((point, index) => {
      if (index % Math.ceil(points.length / 6) !== 0 && index !== points.length - 1) {
        return "";
      }

      const x = xScale(Math.log10(Math.max(point.x, 1)));
      return `
        <line x1="${x}" y1="${height - padding.bottom}" x2="${x}" y2="${padding.top}" class="svg-grid" />
        <text x="${x}" y="${height - 18}" text-anchor="middle" class="svg-axis-label">${escapeHtml(point.label)}</text>
      `;
    })
    .join("");
}
