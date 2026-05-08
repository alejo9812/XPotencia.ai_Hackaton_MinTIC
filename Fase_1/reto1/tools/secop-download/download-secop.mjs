#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import parquet from "parquetjs-lite";

const DEFAULT_BASE_URL = "https://www.datos.gov.co/resource";
const DEFAULT_DATASET = "jbjy-vk9h";
const DEFAULT_LIMIT = 1000;
const DEFAULT_MAX_ROWS = 10000;
const DEFAULT_ORDER = "ultima_actualizacion DESC";
const DEFAULT_FORMAT = "csv";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const dataset = args.dataset || DEFAULT_DATASET;
  const format = normalizeFormat(args.format || DEFAULT_FORMAT);
  const requestFormat = format === "parquet" ? "json" : format;
  const baseUrl = (args.baseUrl || process.env.SOCRATA_BASE_URL || DEFAULT_BASE_URL).replace(/\/+$/, "");
  const appToken = args.appToken || process.env.SOCRATA_APP_TOKEN || "";
  const limit = clampInt(args.limit, DEFAULT_LIMIT, 1, 50000);
  const maxRows = Math.max(1, clampInt(args.maxRows, DEFAULT_MAX_ROWS, 1, Number.MAX_SAFE_INTEGER));
  const delayMs = Math.max(0, clampInt(args.delayMs, process.env.SOCRATA_DELAY_MS || 0, 0, 10000));
  const where = (args.where || "").trim();
  const order = (args.order || DEFAULT_ORDER).trim();
  const outputPath = resolveOutputPath(args.output, dataset, format, maxRows);

  await fs.mkdir(path.dirname(outputPath), { recursive: true });

  const startedAt = new Date();
  const headers = appToken ? { "X-App-Token": appToken } : {};
  const requestHeaders = {
    Accept: requestFormat === "csv" ? "text/csv" : "application/json",
    ...headers,
  };

  let downloaded = 0;
  let offset = 0;
  let firstChunk = true;
  let wroteAnyData = false;
  let carryNewline = false;
  let parquetWriter = null;

  console.log(`Downloading ${dataset} as ${format.toUpperCase()}`);
  console.log(`Output: ${outputPath}`);
  console.log(`Limit: ${limit}, max rows: ${maxRows}`);
  if (where) {
    console.log(`Filter: ${where}`);
  }
  console.log(`Order: ${order}`);

  while (downloaded < maxRows) {
    const pageLimit = Math.min(limit, maxRows - downloaded);
    const url = buildUrl(baseUrl, dataset, requestFormat, {
      limit: pageLimit,
      offset,
      where,
      order,
    });

    const response = await fetchWithRetry(url, requestHeaders);
    if (!response.ok) {
      const errorText = await safeText(response);
      throw new Error(
        `Request failed for offset ${offset} with HTTP ${response.status}${errorText ? `: ${errorText}` : ""}`,
      );
    }

    const chunk = await response.text();
    if (!chunk.trim()) {
      break;
    }

    if (format === "csv") {
      const body = firstChunk ? chunk : stripCsvHeader(chunk);
      if (firstChunk) {
        await writeTextWithRetry(outputPath, body);
        firstChunk = false;
      } else if (body) {
        const payload = carryNewline ? `\n${body}` : body;
        await appendTextWithRetry(outputPath, payload);
      }

      carryNewline = !body.endsWith("\n") && !body.endsWith("\r");
      wroteAnyData = true;
      downloaded += pageLimit;

      if (!body.trim()) {
        break;
      }
    } else if (format === "json") {
      const pageRows = JSON.parse(chunk);
      if (!Array.isArray(pageRows) || pageRows.length === 0) {
        break;
      }

      const ndjson = pageRows.map((row) => JSON.stringify(row)).join("\n") + "\n";
      if (firstChunk) {
        await writeTextWithRetry(outputPath, ndjson);
        firstChunk = false;
      } else {
        await appendTextWithRetry(outputPath, ndjson);
      }

      wroteAnyData = true;
      downloaded += pageRows.length;
    } else {
      const pageRows = JSON.parse(chunk);
      if (!Array.isArray(pageRows) || pageRows.length === 0) {
        break;
      }

      const flatRows = pageRows.map(flattenRecord);
      if (!parquetWriter) {
        const schema = buildParquetSchema(flatRows);
        parquetWriter = await parquet.ParquetWriter.openFile(schema, outputPath);
      }

      for (const row of flatRows) {
        await parquetWriter.appendRow(row);
      }

      wroteAnyData = true;
      downloaded += pageRows.length;
    }

    if (delayMs > 0) {
      await sleep(delayMs);
    }

    offset += pageLimit;
    console.log(`Downloaded ${Math.min(downloaded, maxRows)} rows...`);

    if (downloaded >= maxRows) {
      break;
    }
  }

  if (parquetWriter) {
    await parquetWriter.close();
  }

  if (!wroteAnyData) {
    throw new Error("No rows were downloaded.");
  }

  const manifest = {
    dataset,
    format,
    baseUrl,
    where: where || null,
    order,
    limit,
    maxRows,
    outputPath,
    downloadedRows: downloaded,
    startedAt: startedAt.toISOString(),
    finishedAt: new Date().toISOString(),
  };

  const manifestPath = path.join(
    path.dirname(outputPath),
    `${path.basename(outputPath, path.extname(outputPath))}.manifest.json`,
  );
  await writeTextWithRetry(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);

  console.log(`Done. Rows downloaded: ${downloaded}`);
}

function buildUrl(baseUrl, dataset, format, options) {
  const url = new URL(`${baseUrl}/${dataset}.${format}`);
  url.searchParams.set("$limit", String(options.limit));
  url.searchParams.set("$offset", String(options.offset));

  if (options.where) {
    url.searchParams.set("$where", options.where);
  }

  if (options.order) {
    url.searchParams.set("$order", options.order);
  }

  return url;
}

function parseArgs(argv) {
  const result = {};

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (!arg.startsWith("--")) {
      continue;
    }

    const eqIndex = arg.indexOf("=");
    if (eqIndex !== -1) {
      const key = normalizeArgKey(arg.slice(2, eqIndex));
      const value = arg.slice(eqIndex + 1);
      result[key] = value;
      continue;
    }

    const key = normalizeArgKey(arg.slice(2));
    const next = argv[index + 1];
    if (!next || next.startsWith("--")) {
      result[key] = "true";
      continue;
    }

    result[key] = next;
    index += 1;
  }

  return result;
}

function normalizeArgKey(key) {
  return key.replace(/-([a-z])/g, (_, letter) => letter.toUpperCase());
}

function normalizeFormat(value) {
  const normalized = String(value || "").toLowerCase().trim();
  if (normalized === "json" || normalized === "ndjson") {
    return "json";
  }
  if (normalized === "parquet" || normalized === "pq") {
    return "parquet";
  }
  return "csv";
}

function resolveOutputPath(requested, dataset, format, maxRows) {
  if (requested) {
    return path.resolve(process.cwd(), requested);
  }

  const extension = format === "parquet" ? "parquet" : format === "json" ? "jsonl" : "csv";
  const fileName = `${dataset}_${format}_${maxRows}.${extension}`;
  return path.join(__dirname, "output", fileName);
}

function clampInt(value, fallback, min, max) {
  const parsed = Number.parseInt(String(value ?? fallback), 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }

  return Math.min(max, Math.max(min, parsed));
}

function stripCsvHeader(text) {
  const newlineMatch = text.match(/\r?\n/);
  if (!newlineMatch || newlineMatch.index == null) {
    return "";
  }

  return text.slice(newlineMatch.index + newlineMatch[0].length);
}

function flattenRecord(record) {
  const flat = {};

  for (const [key, value] of Object.entries(record || {})) {
    flat[key] = toParquetValue(value);
  }

  return flat;
}

function buildParquetSchema(rows) {
  const fieldNames = new Set();

  for (const row of rows) {
    for (const key of Object.keys(row)) {
      fieldNames.add(key);
    }
  }

  const fields = {};
  for (const key of fieldNames) {
    fields[key] = { type: "UTF8", optional: true };
  }

  return new parquet.ParquetSchema(fields);
}

function toParquetValue(value) {
  if (value == null) {
    return undefined;
  }

  if (Array.isArray(value)) {
    return value.map((item) => toParquetValue(item)).filter((item) => item != null).join(" | ") || undefined;
  }

  if (typeof value === "object") {
    if (typeof value.url === "string") {
      return value.url;
    }
    return JSON.stringify(value);
  }

  const text = String(value).trim();
  return text.length ? text : undefined;
}

function safeText(response) {
  return response.text().catch(() => "");
}

async function fetchWithRetry(url, headers, attempts = 6) {
  let lastResponse = null;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const response = await fetch(url, { headers });
    if (response.ok) {
      return response;
    }

    lastResponse = response;
    const shouldRetry = [429, 500, 502, 503, 504].includes(response.status);
    if (!shouldRetry || attempt === attempts) {
      return response;
    }

    const retryAfter = Number(response.headers.get("retry-after") || "0");
    const waitMs = Number.isFinite(retryAfter) && retryAfter > 0 ? retryAfter * 1000 : 750 * attempt;
    await sleep(waitMs);
  }

  return lastResponse;
}

async function writeTextWithRetry(filePath, content) {
  await retry(async () => fs.writeFile(filePath, content, "utf8"));
}

async function appendTextWithRetry(filePath, content) {
  await retry(async () => fs.appendFile(filePath, content, "utf8"));
}

async function retry(task, attempts = 5, delayMs = 120) {
  let lastError = null;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await task();
    } catch (error) {
      lastError = error;
      const code = error && typeof error === "object" ? error.code : "";
      const shouldRetry = code === "EBUSY" || code === "EPERM" || code === "ETXTBSY";

      if (!shouldRetry || attempt === attempts) {
        throw error;
      }

      await sleep(delayMs * attempt);
    }
  }

  throw lastError;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
