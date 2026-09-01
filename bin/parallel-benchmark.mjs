#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import { BenchmarkController, DEFAULT_FIXTURE, DEFAULT_PUBLIC_DIR, PROJECT_ROOT } from "../src/controller.mjs";
import { createMockServer } from "../src/mock-endpoint.mjs";
import { validateRunRecord } from "../src/schema.mjs";

function option(args, name, fallback = undefined) {
  const index = args.indexOf(name);
  return index >= 0 ? args[index + 1] : fallback;
}

function numberOption(args, name, fallback) {
  const value = option(args, name, fallback);
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function printSummary(summary) {
  const rate = summary.aggregate_decode_tok_s == null ? "unavailable" : `${summary.aggregate_decode_tok_s.toFixed(1)} tok/s`;
  console.log(`streams: ${summary.requested_concurrency}`);
  console.log(`completed: ${summary.completed_count}  failed: ${summary.failed_count}`);
  console.log(`wall time: ${(summary.wall_time_ms / 1000).toFixed(2)}s`);
  console.log(`aggregate decode: ${rate}`);
  console.log(`TTFT (median): ${summary.ttft_ms == null ? "unavailable" : `${summary.ttft_ms.toFixed(0)}ms`}`);
}

async function waitForRun(run) {
  while (!run.done) await new Promise((resolve) => setTimeout(resolve, 25));
  return run;
}

async function main() {
  const [command = "serve", ...args] = process.argv.slice(2);
  if (command === "validate-fixture") {
    const fixture = JSON.parse(await readFile(DEFAULT_FIXTURE, "utf8"));
    const record = validateRunRecord(fixture);
    console.log(`fixture valid: ${record.events.length} events, ${record.summary.requested_concurrency} streams`);
    return;
  }

  if (command === "mock") {
    const mock = createMockServer({ port: numberOption(args, "--port", 4180) });
    await mock.listen();
    console.log("mock endpoint ready; press Ctrl-C to stop");
    await new Promise(() => {});
    return;
  }

  if (command === "demo") {
    const mock = createMockServer({ port: 4180 });
    await mock.listen();
    const controller = new BenchmarkController({
      endpoint: "http://127.0.0.1:4180/v1/chat/completions",
      model: "mock-model",
      publicDir: DEFAULT_PUBLIC_DIR,
    });
    const server = await controller.serve({ port: numberOption(args, "--port", 4173) });
    console.log("dashboard ready; open the local dashboard and press Ctrl-C to stop");
    const stop = () => {
      server.close();
      mock.close();
      process.exit(0);
    };
    process.once("SIGINT", stop);
    process.once("SIGTERM", stop);
    await new Promise(() => {});
    return;
  }

  if (command === "replay") {
    const controller = new BenchmarkController({ publicDir: DEFAULT_PUBLIC_DIR });
    const run = await waitForRun(await controller.startReplay({
      concurrency: numberOption(args, "--concurrency", 16),
      maxTokens: numberOption(args, "--max-tokens", 64),
    }));
    printSummary(run.summary);
    return;
  }

  if (command === "capture") {
    const promptFile = option(args, "--prompt-file");
    let prompts = null;
    if (promptFile) prompts = JSON.parse(await readFile(promptFile, "utf8"));
    const controller = new BenchmarkController({
      endpoint: process.env.BENCH_ENDPOINT || process.env.OPENAI_BASE_URL || "",
      apiKey: process.env.OPENAI_API_KEY || process.env.BENCH_API_KEY || "",
      model: process.env.BENCH_MODEL || "",
      captureDir: process.env.BENCH_CAPTURE_DIR || `${PROJECT_ROOT}/captures`,
    });
    const run = await controller.captureLive({
      concurrency: numberOption(args, "--concurrency", 16),
      maxTokens: numberOption(args, "--max-tokens", 64),
      timeoutMs: numberOption(args, "--timeout-ms", 30_000),
      prompt: option(args, "--prompt", ""),
      prompts,
    });
    console.log(`capture saved: ${run.run_id}`);
    printSummary(run.summary);
    return;
  }

  if (command === "serve") {
    const controller = new BenchmarkController({ publicDir: DEFAULT_PUBLIC_DIR });
    const server = await controller.serve({ port: numberOption(args, "--port", 4173) });
    console.log("dashboard ready; press Ctrl-C to stop");
    const stop = () => { server.close(); process.exit(0); };
    process.once("SIGINT", stop);
    process.once("SIGTERM", stop);
    await new Promise(() => {});
    return;
  }

  throw new Error(`unknown command: ${command}`);
}

main().catch((error) => {
  console.error(`parallel-benchmark: ${error?.message || "command failed"}`);
  process.exitCode = 1;
});
