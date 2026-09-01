import { validateEvent, validateSummary } from "./schema.mjs";

function median(values) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}

function round(value, digits = 3) {
  return Number(value.toFixed(digits));
}

/**
 * Compute metrics from sanitized events. Token totals come only from terminal
 * usage objects; delta token IDs are progress evidence, never the authority.
 */
export function computeSummary(events, requestedConcurrency = 0, winningRecipe = "not selected") {
  const validated = events.map(validateEvent);
  const streams = new Map();
  let runStarted = validated.find((event) => event.event_type === "run.started");
  let runCompleted = validated.find((event) => event.event_type === "run.completed");
  const ensureStream = (event) => {
    const key = event.request_id;
    if (!streams.has(key)) {
      streams.set(key, {
        stream_index: event.stream_index,
        started: null,
        first_delta: null,
        completed: null,
        failed: null,
        chunk_count: 0,
      });
    }
    return streams.get(key);
  };

  for (const event of validated) {
    if (event.event_type === "stream.started") ensureStream(event).started = event;
    if (event.event_type === "stream.delta") {
      const stream = ensureStream(event);
      stream.chunk_count += 1;
      if (!stream.first_delta) stream.first_delta = event;
    }
    if (event.event_type === "stream.completed") ensureStream(event).completed = event;
    if (event.event_type === "stream.failed") ensureStream(event).failed = event;
  }

  if (!runStarted) runStarted = { elapsed_ms: 0 };
  const maxElapsed = validated.reduce((max, event) => Math.max(max, event.elapsed_ms), 0);
  const wallTimeMs = runCompleted?.elapsed_ms ?? maxElapsed;
  const distribution = [];
  const failureLabels = [];
  let totalCompletionTokens = 0;
  let completedCount = 0;
  let failedCount = 0;
  const ttfts = [];
  for (const stream of streams.values()) {
    const terminal = stream.completed;
    const failure = stream.failed;
    const startMs = stream.started?.elapsed_ms ?? runStarted.elapsed_ms ?? 0;
    const endMs = terminal?.elapsed_ms ?? failure?.elapsed_ms ?? startMs;
    const requestWallMs = Math.max(0, endMs - startMs);
    const usage = terminal?.usage;
    const success = Boolean(terminal && terminal.status === "success" && usage);
    const ttft = stream.first_delta ? Math.max(0, stream.first_delta.elapsed_ms - startMs) : null;
    if (success) {
      completedCount += 1;
      totalCompletionTokens += usage.completion_tokens;
      if (ttft != null) ttfts.push(ttft);
      distribution.push({
        stream_index: stream.stream_index,
        completion_tokens: usage.completion_tokens,
        wall_time_ms: requestWallMs,
        tok_s: requestWallMs > 0 ? round(usage.completion_tokens / (requestWallMs / 1000)) : null,
        status: "success",
        ttft_ms: ttft,
        failure_kind: null,
      });
    } else {
      failedCount += 1;
      const kind = failure?.failure_kind || (terminal?.status === "usage_unavailable" ? "usage_unavailable" : "unknown");
      if (!failureLabels.includes(kind)) failureLabels.push(kind);
      distribution.push({
        stream_index: stream.stream_index,
        completion_tokens: 0,
        wall_time_ms: requestWallMs,
        tok_s: null,
        status: "failed",
        ttft_ms: ttft,
        failure_kind: kind,
      });
    }
  }

  const requested = requestedConcurrency || streams.size;
  const aggregate = wallTimeMs > 0 ? totalCompletionTokens / (wallTimeMs / 1000) : null;
  return validateSummary({
    requested_concurrency: requested,
    active_count: Math.max(0, requested - completedCount - failedCount),
    completed_count: completedCount,
    failed_count: failedCount,
    wall_time_ms: wallTimeMs,
    total_completion_tokens: totalCompletionTokens,
    aggregate_decode_tok_s: aggregate == null ? null : round(aggregate),
    ttft_ms: median(ttfts),
    completion_count: completedCount,
    throughput_distribution: distribution.sort((a, b) => a.stream_index - b.stream_index),
    failure_labels: failureLabels,
    winning_recipe: winningRecipe,
  });
}

export function progressFromEvents(events) {
  const validated = events.map(validateEvent);
  const streamCount = validated.find((event) => event.event_type === "run.started")?.recipe?.concurrency ||
    new Set(validated.filter((event) => event.request_id).map((event) => event.request_id)).size;
  const states = new Map();
  let totalChunks = 0;
  let totalTokens = 0;
  for (const event of validated) {
    if (!event.request_id) continue;
    const state = states.get(event.request_id) || { status: "active", chunkCount: 0, tokenProgress: 0 };
    if (event.event_type === "stream.delta") {
      state.chunkCount += 1;
      state.tokenProgress += event.token_ids?.length || 0;
      totalChunks += 1;
      totalTokens += event.token_ids?.length || 0;
    }
    if (event.event_type === "stream.completed") {
      state.status = event.status === "success" ? "done" : "failed";
      state.usage = event.usage;
    }
    if (event.event_type === "stream.failed") state.status = "failed";
    states.set(event.request_id, state);
  }
  return {
    active: [...states.values()].filter((state) => state.status === "active").length,
    done: [...states.values()].filter((state) => state.status === "done").length,
    failed: [...states.values()].filter((state) => state.status === "failed").length,
    streamCount,
    totalChunks,
    tokenProgress: totalTokens,
    states,
  };
}
