#!/usr/bin/env node

import fs from "node:fs/promises";
import { createWriteStream } from "node:fs";
import path from "node:path";
import process from "node:process";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const root = path.resolve(__dirname, "../../../../");
const downloader = path.resolve(root, "Fase_1", "reto1", "tools", "secop-download", "download-secop.mjs");
const partitionsFile = path.resolve(root, "Fase_1", "reto1", "base2", "meta", "monthly-partitions.json");
const outputDir = path.resolve(root, "Fase_1", "reto1", "base2", "parquet");
const logDir = path.resolve(root, "Fase_1", "reto1", "base2", "meta", "logs");
const concurrency = Number.parseInt(process.env.BASE2_CONCURRENCY || "1", 10);

await fs.mkdir(outputDir, { recursive: true });
await fs.mkdir(logDir, { recursive: true });

const config = JSON.parse(await fs.readFile(partitionsFile, "utf8"));
const partitions = config.partitions || [];

console.log(`Launching ${partitions.length} monthly partitions with concurrency ${concurrency}`);

let cursor = 0;
let running = 0;
let completed = 0;
let failed = false;

await new Promise((resolve, reject) => {
  const launchNext = () => {
    if (failed) {
      return;
    }

    while (running < concurrency && cursor < partitions.length) {
      const partition = partitions[cursor++];
      running += 1;
      runPartition(partition)
        .then(() => {
          running -= 1;
          completed += 1;
          console.log(`Finished ${partition.month} (${completed}/${partitions.length})`);
          if (completed === partitions.length) {
            resolve();
          } else {
            launchNext();
          }
        })
        .catch((error) => {
          failed = true;
          reject(error);
        });
    }
  };

  launchNext();
});

async function runPartition(partition) {
  const fileName = `dmgg-8hin_${partition.month}.parquet`;
  const outputPath = path.join(outputDir, fileName);
  const outLog = path.join(logDir, `${partition.month}.out.log`);
  const errLog = path.join(logDir, `${partition.month}.err.log`);
  const manifestPath = path.join(outputDir, `dmgg-8hin_${partition.month}.manifest.json`);

  if (await partitionAlreadyExists(outputPath, manifestPath)) {
    console.log(`Skipping ${partition.month} (already exists)`);
    return;
  }

  const args = [
    downloader,
    "--dataset",
    "dmgg-8hin",
    "--format",
    "parquet",
    "--limit",
    "10000",
    "--max-rows",
    String(partition.rows),
    "--where",
    `fecha_carga >= '${partition.start}' AND fecha_carga < '${partition.end}'`,
    "--order",
    "fecha_carga DESC",
    "--delay-ms",
    "100",
    "--output",
    outputPath,
  ];

  await new Promise((resolve, reject) => {
    const child = spawn(process.execPath, args, {
      cwd: root,
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"],
    });

    const stdout = createWriteStream(outLog, { flags: "w" });
    const stderr = createWriteStream(errLog, { flags: "w" });

    child.stdout.pipe(stdout);
    child.stderr.pipe(stderr);

    child.on("error", (error) => {
      stdout.end();
      stderr.end();
      reject(error);
    });

    child.on("exit", (code) => {
      stdout.end();
      stderr.end();
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`Partition ${partition.month} exited with code ${code}`));
      }
    });
  });
}

async function partitionAlreadyExists(outputPath, manifestPath) {
  try {
    const [outputStat, manifestStat] = await Promise.all([fs.stat(outputPath), fs.stat(manifestPath)]);
    return outputStat.size > 0 && manifestStat.size > 0;
  } catch {
    return false;
  }
}
