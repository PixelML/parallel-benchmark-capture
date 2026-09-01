import test from "node:test";
import assert from "node:assert/strict";
import { computeSummary } from "../src/metrics.mjs";
import { createRequestId } from "../src/schema.mjs";

test("metrics use terminal usage and run wall time", () => {
  const runId = "metric_test";
  const requestId = createRequestId(runId, 0);
  const events = [
    { schema_version: 1, event_type: "run.started", run_id: runId, elapsed_ms: 0, run_mode: "REPLAY", recipe: { concurrency: 1, max_tokens: 8, model_label: "fixture", telemetry_mode: "SSE CHUNK MODE" } },
    { schema_version: 1, event_type: "stream.started", run_id: runId, request_id: requestId, stream_index: 0, elapsed_ms: 20, mode: "SSE CHUNK MODE" },
    { schema_version: 1, event_type: "stream.delta", run_id: runId, request_id: requestId, stream_index: 0, elapsed_ms: 80, mode: "SSE CHUNK MODE", sequence: 0, text: "x", token_ids: [99, 100] },
    { schema_version: 1, event_type: "stream.completed", run_id: runId, request_id: requestId, stream_index: 0, elapsed_ms: 220, mode: "SSE CHUNK MODE", sequence: 1, status: "success", usage: { prompt_tokens: 4, completion_tokens: 7, total_tokens: 11 }, finish_reason: "stop" },
    { schema_version: 1, event_type: "run.completed", run_id: runId, elapsed_ms: 300, summary: {} },
  ];
  const summary = computeSummary(events, 1, "fixture recipe");
  assert.equal(summary.total_completion_tokens, 7);
  assert.equal(summary.wall_time_ms, 300);
  assert.equal(summary.aggregate_decode_tok_s, 23.333);
  assert.equal(summary.throughput_distribution[0].completion_tokens, 7);
  assert.equal(summary.ttft_ms, 60);
});
