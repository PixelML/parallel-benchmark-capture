import test from "node:test";
import assert from "node:assert/strict";
import { BenchmarkController } from "../src/controller.mjs";
import { createMockServer } from "../src/mock-endpoint.mjs";

async function wait(run) {
  while (!run.done) await new Promise((resolve) => setTimeout(resolve, 10));
  return run;
}

test("replay is deterministic and keeps the same schema", async () => {
  const controller = new BenchmarkController();
  const run = await wait(await controller.startReplay({ concurrency: 2 }));
  assert.equal(run.run_mode, "REPLAY");
  assert.equal(run.summary.requested_concurrency, 2);
  assert.equal(run.summary.completed_count, 2);
  assert.equal(run.events.some((event) => event.mode === "EXACT SCHEDULER STEP"), false);
});

test("live mock capture is concurrent and usage-authoritative", async () => {
  const mock = createMockServer({ port: 0, delayMs: 1 });
  await mock.listen();
  const port = mock.server.address().port;
  const controller = new BenchmarkController({
    endpoint: `http://127.0.0.1:${port}/v1/chat/completions`,
    model: "mock-model",
    captureDir: "/tmp/parallel-benchmark-capture-test",
  });
  const run = await controller.captureLive({ concurrency: 2, maxTokens: 16, prompt: "test" });
  assert.equal(run.run_mode, "LIVE");
  assert.equal(run.summary.requested_concurrency, 2);
  assert.equal(run.summary.completed_count, 2);
  assert.equal(run.events.every((event) => !Object.hasOwn(event, "endpoint")), true);
  await mock.close();
});

test("HTTP controller serves dashboard and sanitized replay SSE", async () => {
  const controller = new BenchmarkController();
  const server = await controller.serve({ port: 0 });
  const port = server.address().port;
  const page = await fetch(`http://127.0.0.1:${port}/`);
  assert.equal(page.status, 200);
  assert.match(await page.text(), /16 users generating in parallel/);
  const started = await fetch(`http://127.0.0.1:${port}/api/runs`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ mode: "REPLAY", concurrency: 1 }),
  });
  const handle = await started.json();
  assert.equal(started.status, 202);
  const stream = await fetch(`http://127.0.0.1:${port}/api/runs/${handle.run_id}/events`);
  const body = await stream.text();
  assert.match(body, /stream\.delta/);
  assert.doesNotMatch(body, /BENCH_ENDPOINT|authorization|api_key/i);
  await new Promise((resolve) => server.close(resolve));
});

test("HTTP LIVE handle is backed by the controller-owned mock endpoint", async () => {
  const mock = createMockServer({ port: 0, delayMs: 1 });
  await mock.listen();
  const controller = new BenchmarkController({
    endpoint: `http://127.0.0.1:${mock.server.address().port}/v1/chat/completions`,
    model: "mock-model",
    captureDir: "/tmp/parallel-benchmark-capture-http-test",
  });
  const server = await controller.serve({ port: 0 });
  const port = server.address().port;
  const started = await fetch(`http://127.0.0.1:${port}/api/runs`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ mode: "LIVE", concurrency: 1, prompt: "controller test" }),
  });
  const handle = await started.json();
  const stream = await fetch(`http://127.0.0.1:${port}/api/runs/${handle.run_id}/events`);
  const body = await stream.text();
  assert.match(body, /run\.completed/);
  assert.match(body, /"requested_concurrency":1/);
  assert.doesNotMatch(body, /127\.0\.0\.1|authorization|api_key/i);
  await new Promise((resolve) => server.close(resolve));
  await mock.close();
});
