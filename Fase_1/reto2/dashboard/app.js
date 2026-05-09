const fmt = new Intl.NumberFormat("es-CO");

function percent(value, digits = 2) {
  return `${Number(value).toFixed(digits).replace(".", ",")}%`;
}

function formatMoney(value) {
  return fmt.format(Number(value || 0));
}

function renderPill(text, kind = "") {
  const span = document.createElement("span");
  span.className = kind ? `pill ${kind}` : "pill";
  span.textContent = text;
  return span;
}

function createBarRow(item, value, maxValue, unitLabel = "") {
  const row = document.createElement("div");
  row.className = "bar-row";

  const line = document.createElement("div");
  line.className = "bar-topline";
  line.innerHTML = `
    <strong>${item}</strong>
    <span class="bar-meta">${unitLabel ? `${unitLabel}: ` : ""}${fmt.format(value)}</span>
  `;

  const track = document.createElement("div");
  track.className = "bar-track";

  const fill = document.createElement("div");
  fill.className = "bar-fill";
  fill.style.width = `${Math.max(4, (Number(value) / Number(maxValue || 1)) * 100)}%`;

  track.appendChild(fill);
  row.appendChild(line);
  row.appendChild(track);
  return row;
}

function createCard(title, value, hint) {
  const card = document.createElement("div");
  card.className = "kpi-card";
  card.innerHTML = `
    <div class="kpi-label">${title}</div>
    <div class="kpi-value">${value}</div>
    <div class="kpi-hint">${hint}</div>
  `;
  return card;
}

async function loadData() {
  const response = await fetch("./api/summary.json", { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`No se pudo cargar /api/summary.json (${response.status})`);
  }
  return response.json();
}

function renderSidebar(data) {
  document.getElementById("datasetName").textContent = data.dataset.name;
  document.getElementById("datasetId").textContent = `ID: ${data.dataset.id}`;

  const sourceFacts = document.getElementById("sourceFacts");
  sourceFacts.innerHTML = "";
  const facts = [
    ["Registros", fmt.format(data.kpis.total_records)],
    ["Variables", fmt.format(data.kpis.total_variables)],
    ["Registros 2025", fmt.format(data.kpis.records_2025)],
    ["API", data.dataset.source_url],
  ];

  for (const [label, value] of facts) {
    const item = document.createElement("div");
    item.className = "fact-item";
    item.innerHTML = `<strong>${label}</strong><span>${value}</span>`;
    sourceFacts.appendChild(item);
  }

  const takeaways = [
    "La base sí permite medir Pymes, anticipos, obligaciones ambientales y tipo de contrato.",
    "La pregunta de género no tiene soporte directo en la fuente, así que no se debe inventar.",
    "Los valores financieros más altos requieren validación externa por su magnitud inusual.",
  ];
  const takeawaysList = document.getElementById("keyTakeaways");
  takeawaysList.innerHTML = takeaways.map((item) => `<li>${item}</li>`).join("");
}

function renderHero(data) {
  document.getElementById("paretoMetric").textContent = percent(data.kpis.pareto_share_pct);
  document.getElementById("paretoNote").textContent =
    `${fmt.format(data.kpis.unique_entities)} entidades únicas en el análisis de concentración`;

  const statusRow = document.getElementById("statusRow");
  statusRow.innerHTML = "";
  [
    `Registros: ${fmt.format(data.kpis.total_records)}`,
    `2025: ${fmt.format(data.kpis.records_2025)}`,
    `Pymes: ${percent(data.kpis.pyme_pct)}`,
    `Anticipos: ${percent(data.kpis.advance_pct, 2)}`,
    `Ambientales: ${fmt.format(data.kpis.environmental_yes)}`,
  ].forEach((label, idx) => {
    const kind = idx === 0 ? "ok" : idx === 2 ? "warn" : idx === 3 ? "warn" : idx === 4 ? "ok" : "";
    statusRow.appendChild(renderPill(label, kind));
  });
}

function renderKPIs(data) {
  const grid = document.getElementById("kpiGrid");
  grid.innerHTML = "";
  [
    ["Registros", fmt.format(data.kpis.total_records), "Universo total de contratos analizados"],
    ["Variables", fmt.format(data.kpis.total_variables), "Columnas reportadas en la base"],
    ["Pymes", `${fmt.format(data.kpis.pyme_yes)} (${percent(data.kpis.pyme_pct)})`, "Contratos adjudicados a Pymes"],
    ["Modalidad líder", data.preferred_modality.name, `Con ${fmt.format(data.preferred_modality.count)} contratos`],
    ["Anticipos", `${fmt.format(data.kpis.advance_yes)} (${percent(data.kpis.advance_pct)})`, "Contratos con pago adelantado"],
    ["Ambientales", fmt.format(data.kpis.environmental_yes), "Contratos con cláusulas ambientales"],
  ].forEach(([title, value, hint]) => grid.appendChild(createCard(title, value, hint)));
}

function renderBars(containerId, rows, valueKey, labelKey, unitLabel = "") {
  const container = document.getElementById(containerId);
  const maxValue = Math.max(...rows.map((row) => Number(row[valueKey] || 0)), 1);
  container.innerHTML = "";
  rows.forEach((row) => {
    container.appendChild(createBarRow(row[labelKey], row[valueKey], maxValue, unitLabel));
  });
}

function renderQuality(data) {
  const container = document.getElementById("qualityCards");
  const cards = [
    {
      title: "Brecha de género",
      body: "No se puede sustentar con esta base porque no existe un campo de sexo o género del representante legal.",
      kind: "warn",
    },
    {
      title: "Anomalías financieras",
      body: "Los tres valores más altos son atípicos por magnitud y deben revisarse contra la fuente original.",
      kind: "bad",
    },
    {
      title: "Calidad semántica",
      body: "Campos como es_pyme, habilita_pago_adelantado y obligaciones ambientales deberían normalizarse como booleanos.",
      kind: "ok",
    },
  ];
  container.innerHTML = cards
    .map(
      (card) => `
        <div class="quality-card">
          <strong>${card.title}</strong>
          <span>${card.body}</span>
          <span class="pill ${card.kind}">Hallazgo</span>
        </div>
      `,
    )
    .join("");
}

function renderQuestions(data) {
  const container = document.getElementById("questions");
  container.innerHTML = "";

  data.questions.forEach((question) => {
    const card = document.createElement("article");
    card.className = "question-card";

    const title = document.createElement("div");
    title.className = "question-title";
    title.textContent = `Q${question.n}. ${question.pregunta}`;
    card.appendChild(title);

    if (Array.isArray(question.respuesta)) {
      const firstItem = question.respuesta[0] || {};
      if (Object.prototype.hasOwnProperty.call(firstItem, "entidad")) {
        const list = document.createElement("div");
        list.className = "answer-list";
        question.respuesta.forEach((item) => {
          const answer = document.createElement("div");
          answer.className = "answer-card";
          answer.innerHTML = `
            <strong>${item.entidad}</strong>
            <span>Monto: ${formatMoney(item.monto)}</span>
            <span>Tipo: ${item.tipo_de_contrato}</span>
            <span>Modalidad: ${item.modalidad}</span>
            <span>Veredicto: ${item.veredicto}</span>
            <span>${item.sustento}</span>
          `;
          list.appendChild(answer);
        });
        card.appendChild(list);
      } else if (Object.prototype.hasOwnProperty.call(firstItem, "variable")) {
        const table = document.createElement("table");
        table.className = "answer-table";
        table.innerHTML = `
          <thead>
            <tr>
              <th>Variable</th>
              <th>Tipo esperado</th>
              <th>Motivo</th>
            </tr>
          </thead>
          <tbody>
            ${question.respuesta
              .map(
                (item) => `
                  <tr>
                    <td>${item.variable}</td>
                    <td>${item.tipo_esperado}</td>
                    <td>${item.motivo}</td>
                  </tr>
                `,
              )
              .join("")}
          </tbody>
        `;
        card.appendChild(table);
      } else {
        const list = document.createElement("div");
        list.className = "answer-list";
        question.respuesta.forEach((item) => {
          const answer = document.createElement("div");
          answer.className = "answer-card";
          answer.textContent = typeof item === "string" ? item : JSON.stringify(item);
          list.appendChild(answer);
        });
        card.appendChild(list);
      }
    } else {
      const body = document.createElement("p");
      body.className = "question-answer";
      body.textContent = question.respuesta;
      card.appendChild(body);
    }

    const status = document.createElement("div");
    status.className = `pill ${question.estado === "ok" ? "ok" : question.estado === "no_disponible" ? "warn" : "danger"}`;
    status.textContent =
      question.estado === "ok"
        ? "Estado: listo"
        : question.estado === "no_disponible"
          ? "Estado: no disponible en la base"
          : "Estado: completar manualmente";
    card.appendChild(status);

    container.appendChild(card);
  });
}

function renderAnomalies(data) {
  const tbody = document.getElementById("anomaliesTable");
  tbody.innerHTML = data.top_values
    .map(
      (item, index) => `
        <tr>
          <td>${index + 1}</td>
          <td>${item.entidad}</td>
          <td>${formatMoney(item.monto)}</td>
          <td>${item.tipo_de_contrato}</td>
          <td>${item.modalidad}</td>
          <td>${item.veredicto}</td>
        </tr>
      `,
    )
    .join("");
}

function bindActions() {
  document.getElementById("printBtn").addEventListener("click", () => window.print());
  document.getElementById("refreshBtn").addEventListener("click", () => window.location.reload());
}

async function bootstrap() {
  try {
    bindActions();
    const data = await loadData();

    renderHero(data);
    renderSidebar(data);
    renderKPIs(data);
    renderBars("departmentBars", data.departments, "count", "name", "Contratos");
    renderBars("entityBars", data.top_entities, "total", "name", "Valor");
    renderBars("contractBars", data.contract_types, "count", "name", "Registros");
    renderQuality(data);
    renderQuestions(data);
    renderAnomalies(data);
    document.getElementById("lastUpdated").textContent = "Fuente oficial SECOP II";
  } catch (error) {
    const main = document.querySelector(".content");
    main.innerHTML = `
      <section class="panel">
        <div class="panel-head">
          <div>
            <p class="section-kicker">Error</p>
            <h2>No se pudo cargar la API local</h2>
          </div>
        </div>
        <p class="lead">
          ${error.message}. Abre el dashboard desde <code>http://127.0.0.1:4176</code> para que la ruta <code>/api/summary.json</code> responda correctamente.
        </p>
      </section>
    `;
  }
}

bootstrap();
