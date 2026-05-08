#!/usr/bin/env node

import { spawn } from "node:child_process";
import { resolve } from "node:path";

const root = resolve(process.cwd());
const downloader = resolve(root, "Fase_1", "reto1", "tools", "secop-download", "download-secop.mjs");
const output = resolve(root, "Fase_1", "reto1", "base2", "parquet", "dmgg-8hin_full.parquet");

const args = [
  downloader,
  "--dataset",
  "dmgg-8hin",
  "--format",
  "parquet",
  "--limit",
  "10000",
  "--max-rows",
  "17353029",
  "--order",
  "fecha_carga DESC",
  "--output",
  output,
];

const child = spawn(process.execPath, args, {
  stdio: "inherit",
  windowsHide: true,
});

child.on("exit", (code) => {
  process.exit(code ?? 1);
});
