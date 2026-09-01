import { createServer } from "node:http";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildDemoRecord } from "./demo-fixture.mjs";
import { computeSummary } from "./metrics.mjs";
import { indexTelemetryByStream, loadParallelHueTelemetryExport, reconcileTelemetry } from "./parallelhue-adapter.mjs";
import { extractDelta, parseSseBody } from "./sse.mjs";
import {
  createRequestId,
  createRunId,
  sanitizeErrorCode,
  sanitizeText,
  validateEvent,
  validateRunRecord,
  validateUsage,
  SchemaError,
} from "./schema.mjs";

const PROJECT_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const DEFAULT_FIXTURE = path.join(PROJECT_ROOT, "fixtures", "replay-c16.json");
const DEFAULT_PUBLIC_DIR = path.join(PROJECT_ROOT, "public");
const PARALLELHUE_EXPORT_LEDGER = ".parallelhue-export-ledger.json";

function jsonResponse(response, value, status = 200) {
  const payload = JSON.stringify(value);
  response.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
    "content-length": Buffer.byteLength(payload),
  });
  response.end(payload);
}

async function readJsonBody(request, maxBytes = 64 * 1024) {
  const chunks = [];
  let total = 0;
  for await (const chunk of request) {
    total += chunk.length;
    if (total > maxBytes) throw new SchemaError("request body is too large");
    chunks.push(chunk);
  }
  if (!chunks.length) return {};
  try {
    const value = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("body must be an object");
    return value;
  } catch {
    throw new SchemaError("request body must be JSON");
  }
}

function safeConcurrency(value, fallback = 16) {
  const concurrency = value == null ? fallback : Number(value);
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 64) {
    throw new SchemaError("concurrency must be an integer from 1 to 64");
  }
  return concurrency;
}

function safeMaxTokens(value, fallback = 64) {
  const maxTokens = value == null ? fallback : Number(value);
  if (!Number.isInteger(maxTokens) || maxTokens < 1 || maxTokens > 4096) {
    throw new SchemaError("max_tokens must be an integer from 1 to 4096");
  }
  return maxTokens;
}

function safeTimeout(value, fallback = 30_000) {
  const timeout = value == null ? fallback : Number(value);
  if (!Number.isInteger(timeout) || timeout < 100 || timeout > 300_000) {
    throw new SchemaError("timeout_ms must be an integer from 100 to 300000");
  }
  return timeout;
}

function safeParallelHueRunId(value) {
  if (typeof value !== "string" || !/^[0-9a-f]{32}$/.test(value)) {
    throw new SchemaError("exact telemetry requires a 32-character ParallelHue run_id");
  }
  return value;
}

function parallelHueRequestId(runId, streamIndex) {
  return `ph1_${safeParallelHueRunId(runId)}_${streamIndex}`;
}

function safeParallelHueFingerprint(value) {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value)) {
    throw new SchemaError("exact telemetry requires a SHA-256 export fingerprint");
  }
  return value;
}

function normalizeUsage(value) {
  if (!value || typeof value !== "object") return null;
  const usage = {
    prompt_tokens: Number(value.prompt_tokens),
    completion_tokens: Number(value.completion_tokens),
    total_tokens: Number(value.total_tokens),
  };
  if (!Object.values(usage).every((number) => Number.isInteger(number) && number >= 0)) return null;
  try {
    return validateUsage(usage);
  } catch {
    return null;
  }
}

function publicRun(run) {
  return {
    run_id: run.run_id,
    run_mode: run.run_mode,
    done: run.done,
    summary: run.summary,
    event_count: run.events.length,
  };
}

class RunState {
  constructor({ runId, runMode, recipe, parallelhueRunId = null }) {
    this.run_id = runId;
    this.run_mode = runMode;
    this.recipe = recipe;
    // This is controller-only handoff state. It is never emitted or persisted.
    this.parallelhueRunId = parallelhueRunId;
    this.events = [];
    this.summary = null;
    this.done = false;
    this.startedAt = Date.now();
    this.subscribers = new Set();
  }

  emit(input) {
    const event = validateEvent(input);
    this.events.push(event);
    const payload = `data: ${JSON.stringify(event)}\n\n`;
    for (const subscriber of [...this.subscribers]) {
      try {
        subscriber.write(payload);
      } catch {
        this.subscribers.delete(subscriber);
      }
    }
    return event;
  }

  elapsed() {
    return Math.max(0, Date.now() - this.startedAt);
  }

  closeSubscribers() {
    for (const subscriber of [...this.subscribers]) {
      try {
        subscriber.end();
      } catch {
        // Client disconnected; the response is already unusable.
      }
      this.subscribers.delete(subscriber);
    }
  }
}

export class BenchmarkController {
  constructor({
    fixturePath = DEFAULT_FIXTURE,
    publicDir = DEFAULT_PUBLIC_DIR,
    captureDir = path.join(PROJECT_ROOT, "captures"),
    endpoint = process.env.BENCH_ENDPOINT || process.env.OPENAI_BASE_URL || "",
    apiKey = process.env.OPENAI_API_KEY || process.env.BENCH_API_KEY || "",
    model = process.env.BENCH_MODEL || "",
    telemetryMode = process.env.BENCH_TELEMETRY_MODE || "SSE CHUNK MODE",
    telemetryFile = process.env.BENCH_PARALLELHUE_EVENTS_FILE || "",
    parallelhueRunId = process.env.BENCH_PARALLELHUE_RUN_ID || "",
    parallelhueExportSha256 = process.env.BENCH_PARALLELHUE_EXPORT_SHA256 || "",
  } = {}) {
    this.fixturePath = fixturePath;
    this.publicDir = publicDir;
    this.captureDir = captureDir;
    this.endpoint = endpoint;
    this.apiKey = apiKey;
    this.model = model;
    this.telemetryMode = telemetryMode === "EXACT SCHEDULER STEP" ? telemetryMode : "SSE CHUNK MODE";
    this.telemetryFile = telemetryFile;
    this.parallelhueRunId = parallelhueRunId;
    this.parallelhueExportSha256 = parallelhueExportSha256;
    this.consumedParallelHueExports = new Set();
    this.runs = new Map();
  }

  async loadFixture() {
    const value = JSON.parse(await readFile(this.fixturePath, "utf8"));
    return validateRunRecord(value);
  }

  async startReplay({ concurrency = 16, maxTokens = 64 } = {}) {
    const selectedConcurrency = safeConcurrency(concurrency);
    const source = selectedConcurrency === 16
      ? await this.loadFixture()
      : buildDemoRecord({ runId: `replay_20260901_c${selectedConcurrency}`, concurrency: selectedConcurrency, maxTokens: safeMaxTokens(maxTokens) });
    const run = new RunState({
      runId: source.run_id,
      runMode: "REPLAY",
      recipe: source.events.find((event) => event.event_type === "run.started")?.recipe,
    });
    this.runs.set(run.run_id, run);
    const replayStart = Date.now();
    for (const event of source.events) {
      const delay = Math.max(0, event.elapsed_ms - (Date.now() - replayStart));
      setTimeout(() => {
        if (run.done) return;
        run.emit({ ...event, elapsed_ms: Math.max(0, Date.now() - replayStart) });
        if (event.event_type === "run.completed") {
          run.summary = event.summary;
          run.done = true;
          run.closeSubscribers();
        }
      }, delay);
    }
    return run;
  }

  async captureLive({ concurrency = 16, maxTokens = 64, timeoutMs = 30_000, prompt = "", prompts = null, runId = null } = {}) {
    const selectedConcurrency = safeConcurrency(concurrency);
    const selectedMaxTokens = safeMaxTokens(maxTokens);
    const selectedTimeout = safeTimeout(timeoutMs);
    if (!this.endpoint || !/^https?:\/\//i.test(this.endpoint)) {
      throw new SchemaError("live endpoint is not configured");
    }
    const selectedRunId = runId || createRunId("run");
    let telemetryByStream = new Map();
    let selectedParallelHueRunId = null;
    if (this.telemetryMode === "EXACT SCHEDULER STEP") {
      if (!this.telemetryFile) throw new SchemaError("exact telemetry requires a local ParallelHue sidecar");
      // The owning experiment supplies this fresh run ID in the same handoff
      // as the sidecar. Never infer it from a file that could be stale.
      selectedParallelHueRunId = safeParallelHueRunId(this.parallelhueRunId);
      const expectedFingerprint = safeParallelHueFingerprint(this.parallelhueExportSha256);
      const telemetryExport = await loadParallelHueTelemetryExport(this.telemetryFile);
      if (telemetryExport.runId !== selectedParallelHueRunId) {
        throw new SchemaError("exact telemetry run_id does not match the owner-supplied run");
      }
      if (telemetryExport.fingerprint !== expectedFingerprint) {
        throw new SchemaError("exact telemetry export fingerprint does not match the owner handoff");
      }
      telemetryByStream = indexTelemetryByStream(telemetryExport.byRequest);
      for (let streamIndex = 0; streamIndex < selectedConcurrency; streamIndex += 1) {
        const events = telemetryByStream.get(streamIndex);
        if (!events?.length) throw new SchemaError("exact telemetry is missing a requested stream");
        const expectedRequestId = parallelHueRequestId(selectedParallelHueRunId, streamIndex);
        for (const event of events) {
          if (event.parallelhue_run_id !== selectedParallelHueRunId) {
            throw new SchemaError("exact telemetry run_id does not match the owner-supplied run");
          }
          if (event.parallelhue_request_id !== expectedRequestId) {
            throw new SchemaError("exact telemetry request_id does not match the requested stream");
          }
          if (event.choice_index !== 0) {
            throw new SchemaError("exact telemetry only supports choice_index 0");
          }
        }
      }
      await this.claimParallelHueExport({
        parallelhueRunId: selectedParallelHueRunId,
        fingerprint: telemetryExport.fingerprint,
        controllerRunId: selectedRunId,
      });
    }
    const run = new RunState({
      runId: selectedRunId,
      runMode: "LIVE",
      parallelhueRunId: selectedParallelHueRunId,
      recipe: {
        concurrency: selectedConcurrency,
        max_tokens: selectedMaxTokens,
        model_label: "private model",
        telemetry_mode: this.telemetryMode,
      },
    });
    this.runs.set(run.run_id, run);
    run.emit({
      schema_version: 1,
      event_type: "run.started",
      run_id: run.run_id,
      elapsed_ms: 0,
      run_mode: "LIVE",
      recipe: run.recipe,
    });
    const promptList = Array.isArray(prompts) && prompts.length ? prompts.map((value) => String(value)) : null;
    await Promise.all(Array.from({ length: selectedConcurrency }, (_, streamIndex) => this.captureStream({
      run,
      streamIndex,
      prompt: promptList ? promptList[streamIndex % promptList.length] : String(prompt || ""),
      maxTokens: selectedMaxTokens,
      timeoutMs: selectedTimeout,
      telemetryEvents: telemetryByStream.get(streamIndex) || [],
    })));
    const completedAt = run.elapsed();
    const completedEvent = {
      schema_version: 1,
      event_type: "run.completed",
      run_id: run.run_id,
      elapsed_ms: completedAt,
      summary: {},
    };
    run.summary = computeSummary(
      [...run.events, completedEvent],
      selectedConcurrency,
      `${selectedConcurrency} users generating in parallel · ${this.telemetryMode.toLowerCase()} · private model`,
    );
    run.emit({ ...completedEvent, summary: run.summary });
    run.done = true;
    run.closeSubscribers();
    await this.persistRun(run);
    return run;
  }

  async claimParallelHueExport({ parallelhueRunId, fingerprint, controllerRunId }) {
    const safeRun = safeParallelHueRunId(parallelhueRunId);
    const safeFingerprint = safeParallelHueFingerprint(fingerprint);
    const claimKey = `${safeRun}:${safeFingerprint}`;
    if (this.consumedParallelHueExports.has(claimKey)) {
      throw new SchemaError("exact telemetry export was already consumed");
    }
    await mkdir(this.captureDir, { recursive: true, mode: 0o700 });
    const ledgerPath = path.join(this.captureDir, PARALLELHUE_EXPORT_LEDGER);
    let ledger = { schema_version: 1, entries: [] };
    try {
      ledger = JSON.parse(await readFile(ledgerPath, "utf8"));
    } catch (error) {
      if (error?.code !== "ENOENT") throw new SchemaError("exact telemetry export ledger is unreadable");
    }
    if (!ledger || ledger.schema_version !== 1 || !Array.isArray(ledger.entries)) {
      throw new SchemaError("exact telemetry export ledger is invalid");
    }
    if (!ledger.entries.every((entry) => (
      entry &&
      typeof entry.parallelhue_run_id === "string" && /^[0-9a-f]{32}$/.test(entry.parallelhue_run_id) &&
      typeof entry.export_sha256 === "string" && /^[0-9a-f]{64}$/.test(entry.export_sha256) &&
      typeof entry.controller_run_id === "string"
    ))) {
      throw new SchemaError("exact telemetry export ledger is invalid");
    }
    if (ledger.entries.some((entry) => entry?.parallelhue_run_id === safeRun || entry?.export_sha256 === safeFingerprint)) {
      throw new SchemaError("exact telemetry export was already consumed");
    }
    this.consumedParallelHueExports.add(claimKey);
    try {
      ledger.entries = [
        ...ledger.entries.slice(-255),
        {
          parallelhue_run_id: safeRun,
          export_sha256: safeFingerprint,
          controller_run_id: String(controllerRunId || "unknown").slice(0, 80),
        },
      ];
      await writeFile(ledgerPath, `${JSON.stringify(ledger, null, 2)}\n`, { mode: 0o600 });
    } catch (error) {
      this.consumedParallelHueExports.delete(claimKey);
      throw new SchemaError("exact telemetry export ledger could not be written");
    }
  }

  async captureStream({ run, streamIndex, prompt, maxTokens, timeoutMs, telemetryEvents = [] }) {
    const requestId = createRequestId(run.run_id, streamIndex);
    const exact = run.recipe.telemetry_mode === "EXACT SCHEDULER STEP";
    run.emit({
      schema_version: 1,
      event_type: "stream.started",
      run_id: run.run_id,
      request_id: requestId,
      stream_index: streamIndex,
      elapsed_ms: run.elapsed(),
      mode: exact ? "EXACT SCHEDULER STEP" : "SSE CHUNK MODE",
    });
    let sequence = 0;
    let sawDelta = false;
    let usage = null;
    let finishReason = "stop";
    let telemetryCursor = 0;
    try {
      const endpoint = this.endpoint;
      const isChat = /\/chat\/completions(?:\/)?$/i.test(endpoint);
      const parallelHueRequest = exact ? parallelHueRequestId(run.parallelhueRunId, streamIndex) : null;
      const body = {
        model: this.model,
        max_tokens: maxTokens,
        stream: true,
        stream_options: { include_usage: true },
        ...(exact ? { return_token_ids: true, request_id: parallelHueRequest } : {}),
        ...(isChat ? { messages: [{ role: "user", content: prompt }] } : { prompt }),
      };
      const headers = {
        accept: "text/event-stream",
        "content-type": "application/json",
      };
      if (this.apiKey) headers.authorization = `Bearer ${this.apiKey}`;
      const response = await fetch(endpoint, {
        method: "POST",
        headers,
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(timeoutMs),
      });
      if (!response.ok) {
        run.emit({
          schema_version: 1,
          event_type: "stream.failed",
          run_id: run.run_id,
          request_id: requestId,
          stream_index: streamIndex,
          elapsed_ms: run.elapsed(),
          mode: exact ? "EXACT SCHEDULER STEP" : "SSE CHUNK MODE",
          sequence,
          failure_kind: "http",
          error_code: `http_${response.status}`,
        });
        return;
      }
      for await (const payload of parseSseBody(response.body)) {
        const delta = extractDelta(payload);
        if (delta.finish_reason) finishReason = delta.finish_reason;
        const nextUsage = normalizeUsage(delta.usage);
        if (nextUsage) usage = nextUsage;
        if (!delta.text && !delta.token_ids.length) continue;
        sawDelta = true;
        if (exact) {
          const matched = reconcileTelemetry(telemetryEvents.slice(telemetryCursor), delta.text, delta.token_ids);
          if (!matched) throw new SchemaError("SSE chunk did not reconcile with ParallelHue telemetry");
          for (const telemetryEvent of matched) {
            run.emit({
              schema_version: 1,
              event_type: "stream.delta",
              run_id: run.run_id,
              request_id: requestId,
              stream_index: streamIndex,
              elapsed_ms: run.elapsed(),
              mode: "EXACT SCHEDULER STEP",
              sequence,
              text: telemetryEvent.text,
              token_ids: telemetryEvent.token_ids,
              step_id: telemetryEvent.step_id,
            });
            sequence += 1;
          }
          telemetryCursor += matched.length;
        } else {
          run.emit({
            schema_version: 1,
            event_type: "stream.delta",
            run_id: run.run_id,
            request_id: requestId,
            stream_index: streamIndex,
            elapsed_ms: run.elapsed(),
            mode: "SSE CHUNK MODE",
            sequence,
            text: delta.text,
            token_ids: delta.token_ids,
          });
          sequence += 1;
        }
      }
      if (exact && (!telemetryEvents.length || telemetryCursor !== telemetryEvents.length || !telemetryEvents[telemetryEvents.length - 1].finished)) {
        throw new SchemaError("exact telemetry is absent or incomplete");
      }
      run.emit({
        schema_version: 1,
        event_type: "stream.completed",
        run_id: run.run_id,
        request_id: requestId,
        stream_index: streamIndex,
        elapsed_ms: run.elapsed(),
        mode: exact ? "EXACT SCHEDULER STEP" : "SSE CHUNK MODE",
        sequence,
        status: usage ? "success" : "usage_unavailable",
        finish_reason: finishReason,
        usage,
      });
    } catch (error) {
      const timedOut = error?.name === "TimeoutError" || error?.name === "AbortError";
      run.emit({
        schema_version: 1,
        event_type: "stream.failed",
        run_id: run.run_id,
        request_id: requestId,
        stream_index: streamIndex,
        elapsed_ms: run.elapsed(),
        mode: exact ? "EXACT SCHEDULER STEP" : "SSE CHUNK MODE",
        sequence,
        failure_kind: timedOut ? (sawDelta ? "wedge" : "timeout") : error instanceof SchemaError && exact ? "telemetry" : error instanceof SchemaError ? "parse" : "unknown",
        error_code: timedOut ? "request_timeout" : sanitizeErrorCode(error?.name || "capture_error"),
      });
    }
  }

  async persistRun(run) {
    await mkdir(this.captureDir, { recursive: true, mode: 0o700 });
    const record = validateRunRecord({
      schema_version: 1,
      kind: "benchmark.run",
      run_id: run.run_id,
      run_mode: run.run_mode,
      events: run.events,
      summary: run.summary,
    });
    await writeFile(path.join(this.captureDir, `${run.run_id}.json`), `${JSON.stringify(record, null, 2)}\n`, { mode: 0o600 });
    await writeFile(path.join(this.captureDir, `${run.run_id}.ndjson`), `${record.events.map((event) => JSON.stringify(event)).join("\n")}\n`, { mode: 0o600 });
    return record;
  }

  subscribe(runId, response) {
    const run = this.runs.get(runId);
    if (!run) return false;
    response.writeHead(200, {
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-store",
      connection: "keep-alive",
      "x-content-type-options": "nosniff",
    });
    for (const event of run.events) response.write(`data: ${JSON.stringify(event)}\n\n`);
    if (run.done) {
      response.end();
    } else {
      run.subscribers.add(response);
      response.on("close", () => run.subscribers.delete(response));
    }
    return true;
  }

  async serve({ port = 4173, host = "127.0.0.1" } = {}) {
    const server = createServer(async (request, response) => {
      try {
        const url = new URL(request.url || "/", `http://${request.headers.host || "localhost"}`);
        if (request.method === "GET" && url.pathname === "/api/health") {
          jsonResponse(response, { ok: true, modes: ["LIVE", "REPLAY"] });
          return;
        }
        if (request.method === "POST" && url.pathname === "/api/runs") {
          const body = await readJsonBody(request);
          const mode = String(body.mode || "REPLAY").toUpperCase();
          if (mode === "REPLAY") {
            const run = await this.startReplay({ concurrency: body.concurrency, maxTokens: body.max_tokens });
            jsonResponse(response, publicRun(run), 202);
            return;
          }
          if (mode !== "LIVE") throw new SchemaError("mode must be LIVE or REPLAY");
          if (!this.endpoint || !/^https?:\/\//i.test(this.endpoint)) {
            throw new SchemaError("live endpoint is not configured");
          }
          const runId = createRunId("run");
          // Start the capture with the already-created ID only inside the
          // controller; the HTTP response contains no endpoint information.
          const promise = this.captureLive({
            concurrency: body.concurrency,
            maxTokens: body.max_tokens,
            timeoutMs: body.timeout_ms,
            prompt: body.prompt,
            prompts: body.prompts,
            runId,
          });
          promise.catch(() => {});
          // The real run ID is generated in captureLive; expose a short-lived
          // pending handle only when a caller explicitly asks for LIVE.
          jsonResponse(response, { run_id: runId, run_mode: "LIVE", pending: true }, 202);
          return;
        }
        const eventMatch = /^\/api\/runs\/([^/]+)\/events$/.exec(url.pathname);
        if (request.method === "GET" && eventMatch) {
          if (!this.subscribe(eventMatch[1], response)) jsonResponse(response, { error: "run not found" }, 404);
          return;
        }
        const runMatch = /^\/api\/runs\/([^/]+)$/.exec(url.pathname);
        if (request.method === "GET" && runMatch) {
          const run = this.runs.get(runMatch[1]);
          if (!run) jsonResponse(response, { error: "run not found" }, 404);
          else jsonResponse(response, publicRun(run));
          return;
        }
        if (request.method === "GET") {
          const requestedPath = url.pathname === "/" ? "/index.html" : url.pathname;
          if (requestedPath.includes("..") || requestedPath !== "/index.html") {
            jsonResponse(response, { error: "not found" }, 404);
            return;
          }
          const html = await readFile(path.join(this.publicDir, "index.html"));
          response.writeHead(200, { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" });
          response.end(html);
          return;
        }
        jsonResponse(response, { error: "method not allowed" }, 405);
      } catch (error) {
        const message = error instanceof SchemaError ? error.message : "request failed";
        jsonResponse(response, { error: sanitizeText(message, 120) }, error instanceof SchemaError ? 400 : 500);
      }
    });
    await new Promise((resolve) => server.listen(port, host, resolve));
    return server;
  }
}

export { DEFAULT_FIXTURE, DEFAULT_PUBLIC_DIR, PROJECT_ROOT };
