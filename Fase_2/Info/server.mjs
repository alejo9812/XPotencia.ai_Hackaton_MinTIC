import http from "node:http";
import { readFile } from "node:fs/promises";
import { extname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT_DIR = resolve(fileURLToPath(new URL(".", import.meta.url)));
const PORT = Number(process.env.PORT || 4175);
const DIFY_BASE_URL = (process.env.DIFY_BASE_URL || "https://api.dify.ai").replace(/\/+$/, "");
const DIFY_API_KEY = String(process.env.DIFY_API_KEY || "").trim();

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
  ".ico": "image/x-icon",
  ".txt": "text/plain; charset=utf-8",
};

const server = http.createServer(async (req, res) => {
  try {
    if (!req.url) {
      respondText(res, 400, "Solicitud invalida.");
      return;
    }

    const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);

    if (req.method === "OPTIONS") {
      respondEmpty(res, 204);
      return;
    }

    if (url.pathname === "/api/health") {
      respondJson(res, 200, {
        ok: true,
        difyConfigured: Boolean(DIFY_API_KEY),
        baseUrl: DIFY_BASE_URL,
        apiRoute: "/api/dify/chat",
      });
      return;
    }

    if (url.pathname === "/api/dify/chat") {
      await handleDifyProxy(req, res);
      return;
    }

    await serveStatic(url.pathname, res);
  } catch (error) {
    respondJson(res, 500, {
      ok: false,
      error: error instanceof Error ? error.message : "Error interno del servidor.",
    });
  }
});

server.listen(PORT, () => {
  console.log(`GobIA Auditor Fase 2 running at http://localhost:${PORT}`);
});

async function handleDifyProxy(req, res) {
  if (req.method !== "POST") {
    respondJson(res, 405, {
      ok: false,
      error: "Solo se permite POST en /api/dify/chat.",
    });
    return;
  }

  if (!DIFY_API_KEY) {
    respondJson(res, 503, {
      ok: false,
      error: "Falta configurar DIFY_API_KEY en el entorno del servidor.",
    });
    return;
  }

  const body = await readJsonBody(req);
  const upstreamResponse = await fetch(`${DIFY_BASE_URL}/v1/chat-messages`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${DIFY_API_KEY}`,
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify({
      query:
        body.query || "Analiza el contrato seleccionado y responde con la estructura de GobIA Auditor.",
      inputs: body.inputs || {},
      user: body.user || "gobia-auditor-local",
      response_mode: body.response_mode || "blocking",
      conversation_id: body.conversation_id || undefined,
      auto_generate_name: Boolean(body.auto_generate_name),
    }),
  });

  const contentType = upstreamResponse.headers.get("content-type") || "";
  const payload = contentType.includes("application/json")
    ? await upstreamResponse.json()
    : { raw: await upstreamResponse.text() };

  if (!upstreamResponse.ok) {
    respondJson(res, upstreamResponse.status, {
      ok: false,
      error: payload?.message || payload?.error || "Dify devolvio un error.",
      upstream: payload,
    });
    return;
  }

  respondJson(res, 200, {
    ok: true,
    ...payload,
  });
}

async function serveStatic(pathname, res) {
  const cleanPath = pathname === "/" ? "/index.html" : pathname;
  const filePath = resolve(join(ROOT_DIR, `.${cleanPath}`));

  if (!filePath.startsWith(ROOT_DIR)) {
    respondText(res, 403, "Acceso denegado.");
    return;
  }

  try {
    const data = await readFile(filePath);
    const contentType = MIME_TYPES[extname(filePath)] || "application/octet-stream";
    res.writeHead(200, {
      "Content-Type": contentType,
      "Cache-Control": "no-store",
    });
    res.end(data);
  } catch {
    respondText(res, 404, "Archivo no encontrado.");
  }
}

function respondJson(res, statusCode, payload) {
  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
  });
  res.end(JSON.stringify(payload));
}

function respondText(res, statusCode, text) {
  res.writeHead(statusCode, {
    "Content-Type": "text/plain; charset=utf-8",
    "Cache-Control": "no-store",
  });
  res.end(text);
}

function respondEmpty(res, statusCode) {
  res.writeHead(statusCode, {
    "Cache-Control": "no-store",
  });
  res.end();
}

async function readJsonBody(req) {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(chunk);
  }

  const raw = Buffer.concat(chunks).toString("utf8").trim();
  if (!raw) {
    return {};
  }

  try {
    return JSON.parse(raw);
  } catch {
    return {};
  }
}
