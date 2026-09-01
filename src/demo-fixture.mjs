import { createRequestId, validateRunRecord } from "./schema.mjs";
import { computeSummary } from "./metrics.mjs";

const WORDS = [
  ["route", "the", "prompt", "through", "one", "safe", "controller"],
  ["fan", "out", "the", "requests", "and", "keep", "the", "receipt"],
  ["show", "each", "stream", "as", "it", "arrives", "in", "order"],
  ["wait", "for", "final", "usage", "before", "counting", "tokens"],
  ["replay", "the", "same", "record", "for", "a", "stable", "render"],
  ["finish", "with", "four", "numbers", "and", "the", "recipe"],
];

export function buildDemoRecord({ runId = "demo_20260901_c16", concurrency = 16, maxTokens = 64 } = {}) {
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 64) throw new Error("concurrency must be 1..64");
  const events = [{
    schema_version: 1,
    event_type: "run.started",
    run_id: runId,
    elapsed_ms: 0,
    run_mode: "REPLAY",
    recipe: {
      concurrency,
      max_tokens: maxTokens,
      model_label: "mock-model",
      telemetry_mode: "SSE CHUNK MODE",
    },
  }];
  for (let streamIndex = 0; streamIndex < concurrency; streamIndex += 1) {
    const requestId = createRequestId(runId, streamIndex);
    const startedAt = 90 + streamIndex * 9;
    events.push({
      schema_version: 1,
      event_type: "stream.started",
      run_id: runId,
      request_id: requestId,
      stream_index: streamIndex,
      elapsed_ms: startedAt,
      mode: "SSE CHUNK MODE",
    });
    let sequence = 0;
    let tokenCount = 0;
    for (let chunkIndex = 0; chunkIndex < WORDS.length; chunkIndex += 1) {
      const phrase = `${WORDS[chunkIndex][streamIndex % WORDS[chunkIndex].length]} `;
      const tokenIds = Array.from({ length: phrase.trim().split(/\s+/).length }, (_, index) => 1000 + streamIndex * 100 + chunkIndex * 10 + index);
      tokenCount += tokenIds.length;
      events.push({
        schema_version: 1,
        event_type: "stream.delta",
        run_id: runId,
        request_id: requestId,
        stream_index: streamIndex,
        elapsed_ms: startedAt + 90 + chunkIndex * 145 + (streamIndex % 3) * 12,
        mode: "SSE CHUNK MODE",
        sequence,
        text: phrase,
        token_ids: tokenIds,
      });
      sequence += 1;
    }
    const completedAt = startedAt + 90 + (WORDS.length - 1) * 145 + (streamIndex % 3) * 12 + 170;
    events.push({
      schema_version: 1,
      event_type: "stream.completed",
      run_id: runId,
      request_id: requestId,
      stream_index: streamIndex,
      elapsed_ms: completedAt,
      mode: "SSE CHUNK MODE",
      sequence,
      status: "success",
      finish_reason: "stop",
      usage: {
        prompt_tokens: 12 + (streamIndex % 3),
        completion_tokens: tokenCount,
        total_tokens: 12 + (streamIndex % 3) + tokenCount,
      },
    });
  }
  const wall = Math.max(...events.map((event) => event.elapsed_ms)) + 130;
  events.push({ schema_version: 1, event_type: "run.completed", run_id: runId, elapsed_ms: wall, summary: {} });
  const summary = computeSummary(events, concurrency, `${concurrency} users generating in parallel · SSE chunk mode · mock-model`);
  events[events.length - 1].summary = summary;
  return validateRunRecord({ schema_version: 1, kind: "benchmark.run", run_id: runId, run_mode: "REPLAY", events, summary });
}
