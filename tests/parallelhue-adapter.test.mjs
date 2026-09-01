import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { BenchmarkController } from "../src/controller.mjs";
import { createMockServer } from "../src/mock-endpoint.mjs";
import { indexTelemetryByStream, loadParallelHueTelemetry, reconcileTelemetry } from "../src/parallelhue-adapter.mjs";

test("ParallelHue sidecar normalizes and reconciles exact scheduler steps", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "parallelhue-adapter-"));
  const runId = "a".repeat(32);
  const requestId = `ph1_${runId}_0`;
  const sidecar = [
    ["parallel ", [0], false],
    ["requests ", [1], false],
    ["stay ", [2], true],
  ].map(([text, token_ids], sequence) => JSON.stringify({
    schema_version: 1,
    run_id: runId,
    request_id: requestId,
    sequence,
    step_id: 40 + sequence,
    choice_index: 0,
    token_ids,
    text,
    finished: sequence === 2,
  })).join("\n") + "\n";
  const file = path.join(root, "events.ndjson");
  await writeFile(file, sidecar);
  try {
    const byRequest = await loadParallelHueTelemetry(file);
    const byStream = indexTelemetryByStream(byRequest);
    assert.equal(byStream.get(0).length, 3);
    assert.equal(byStream.get(0)[0].parallelhue_run_id, runId);
    assert.equal(byStream.get(0)[0].parallelhue_request_id, requestId);
    assert.equal(byStream.get(0)[2].step_id, 42);
    const matched = reconcileTelemetry(byStream.get(0), "parallel requests ", [0, 1]);
    assert.deepEqual(matched.map((event) => event.step_id), [40, 41]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("exact live mode requires reconciled ParallelHue telemetry", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "parallelhue-live-"));
  const runId = "b".repeat(32);
  const requestId = `ph1_${runId}_0`;
  const sidecar = ["parallel ", "requests ", "stay "].map((text, sequence) => JSON.stringify({
    schema_version: 1,
    run_id: runId,
    request_id: requestId,
    sequence,
    step_id: 70 + sequence,
    choice_index: 0,
    token_ids: [sequence],
    text,
    finished: sequence === 2,
  })).join("\n") + "\n";
  const telemetryFile = path.join(root, "events.ndjson");
  await writeFile(telemetryFile, sidecar);
  const requests = [];
  const mock = createMockServer({ port: 0, delayMs: 1, onRequest: (body) => requests.push(body) });
  await mock.listen();
  try {
    const controller = new BenchmarkController({
      endpoint: `http://127.0.0.1:${mock.server.address().port}/v1/chat/completions`,
      model: "mock-model",
      telemetryMode: "EXACT SCHEDULER STEP",
      telemetryFile,
      parallelhueRunId: runId,
      captureDir: path.join(root, "captures"),
    });
    const run = await controller.captureLive({ concurrency: 1, maxTokens: 1, runId: "exact_test" });
    assert.equal(run.recipe.telemetry_mode, "EXACT SCHEDULER STEP");
    assert.equal(run.summary.completed_count, 1);
    assert.equal(run.summary.failed_count, 0);
    assert.equal(requests.length, 1);
    assert.equal(requests[0].request_id, requestId);
    assert.equal(requests[0].return_token_ids, true);
    assert.equal(run.events.filter((event) => event.event_type === "stream.delta").every((event) => event.mode === "EXACT SCHEDULER STEP"), true);
    assert.equal(run.events.find((event) => event.event_type === "stream.completed").mode, "EXACT SCHEDULER STEP");
  } finally {
    await mock.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("exact live mode rejects a stale or differently bound sidecar", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "parallelhue-stale-"));
  const sidecarRunId = "b".repeat(32);
  const requestId = `ph1_${sidecarRunId}_0`;
  const telemetryFile = path.join(root, "events.ndjson");
  await writeFile(telemetryFile, `${JSON.stringify({
    schema_version: 1,
    run_id: sidecarRunId,
    request_id: requestId,
    sequence: 0,
    step_id: 1,
    choice_index: 0,
    token_ids: [0],
    text: "parallel ",
    finished: true,
  })}\n`);
  try {
    const controller = new BenchmarkController({
      endpoint: "http://127.0.0.1:1/v1/chat/completions",
      model: "mock-model",
      telemetryMode: "EXACT SCHEDULER STEP",
      telemetryFile,
      parallelhueRunId: "c".repeat(32),
    });
    await assert.rejects(
      controller.captureLive({ concurrency: 1, maxTokens: 1, runId: "exact_stale" }),
      /does not match the owner-supplied run/,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
