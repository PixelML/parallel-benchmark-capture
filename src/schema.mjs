import { randomUUID } from "node:crypto";

export const SCHEMA_VERSION = 1;
export const EVENT_TYPES = Object.freeze([
  "run.started",
  "stream.started",
  "stream.delta",
  "stream.completed",
  "stream.failed",
  "run.completed",
]);
export const MODES = Object.freeze(["SSE CHUNK MODE", "EXACT SCHEDULER STEP"]);
export const RUN_MODES = Object.freeze(["LIVE", "REPLAY"]);
export const FAILURE_KINDS = Object.freeze([
  "http",
  "timeout",
  "wedge",
  "parse",
  "usage_unavailable",
  "telemetry",
  "unknown",
]);

const ANSI_RE = /(?:\u001b\][^\u0007]*(?:\u0007|\u001b\\)|\u001b\[[0-?]*[ -/]*[@-~]|\u001b[@-_])/gs;
const BIDI = new Set([
  0x061c,
  0x200e,
  0x200f,
  0x202a,
  0x202b,
  0x202c,
  0x202d,
  0x202e,
  0x2066,
  0x2067,
  0x2068,
  0x2069,
]);

export class SchemaError extends Error {
  constructor(message) {
    super(message);
    this.name = "SchemaError";
  }
}

export function sanitizeText(value, maxLength = 12000) {
  if (typeof value !== "string") throw new SchemaError("text must be a string");
  const stripped = value.replace(ANSI_RE, "");
  const safe = Array.from(stripped)
    .filter((char) => {
      const code = char.codePointAt(0);
      return (
        char === "\n" ||
        char === "\r" ||
        char === "\t" ||
        (code >= 0x20 && code !== 0x7f && code < 0x7f) ||
        code >= 0xa0
      ) && !BIDI.has(code);
    })
    .join("");
  return safe.length <= maxLength ? safe : `${safe.slice(0, maxLength)}…`;
}

function assert(condition, message) {
  if (!condition) throw new SchemaError(message);
}

function nonNegativeInt(value, field) {
  assert(Number.isInteger(value) && value >= 0, `${field} must be a non-negative integer`);
  return value;
}

function boundedInt(value, field, minimum, maximum) {
  assert(Number.isInteger(value) && value >= minimum && value <= maximum, `${field} must be an integer from ${minimum} to ${maximum}`);
  return value;
}

function safeRunId(value) {
  assert(typeof value === "string" && /^[a-z0-9][a-z0-9_-]{0,63}$/.test(value), "invalid run_id");
  return value;
}

function safeRequestId(value) {
  assert(typeof value === "string" && /^req_[a-z0-9][a-z0-9_-]{0,63}_\d{2,4}$/.test(value), "invalid request_id");
  return value;
}

function validateTokenIds(value) {
  assert(Array.isArray(value), "token_ids must be an array");
  assert(value.length <= 4096, "token_ids exceeds the event limit");
  return value.map((token) => nonNegativeInt(token, "token_ids"));
}

export function createRunId(prefix = "run") {
  const compact = randomUUID().replaceAll("-", "");
  return `${prefix}_${compact.slice(0, 24)}`;
}

export function createRequestId(runId, streamIndex) {
  safeRunId(runId);
  nonNegativeInt(streamIndex, "stream_index");
  return `req_${runId}_${String(streamIndex).padStart(2, "0")}`;
}

export function sanitizeErrorCode(value) {
  if (typeof value !== "string") return "unknown";
  const normalized = value.toLowerCase().replace(/[^a-z_]/g, "").slice(0, 48);
  return normalized || "unknown";
}

export function validateUsage(value) {
  if (value == null) return null;
  assert(value && typeof value === "object" && !Array.isArray(value), "usage must be an object");
  const usage = {
    prompt_tokens: nonNegativeInt(value.prompt_tokens, "usage.prompt_tokens"),
    completion_tokens: nonNegativeInt(value.completion_tokens, "usage.completion_tokens"),
    total_tokens: nonNegativeInt(value.total_tokens, "usage.total_tokens"),
  };
  assert(usage.total_tokens >= usage.completion_tokens, "usage.total_tokens is too small");
  return usage;
}

/**
 * Validate and sanitize one event. Unknown fields are dropped so browser
 * projections and persisted receipts cannot accidentally carry request data.
 */
export function validateEvent(input) {
  assert(input && typeof input === "object" && !Array.isArray(input), "event must be an object");
  assert(input.schema_version === SCHEMA_VERSION, "unsupported schema_version");
  assert(EVENT_TYPES.includes(input.event_type), "unknown event_type");
  const event = {
    schema_version: SCHEMA_VERSION,
    event_type: input.event_type,
    run_id: safeRunId(input.run_id),
    elapsed_ms: nonNegativeInt(input.elapsed_ms, "elapsed_ms"),
  };

  if (input.event_type === "run.started") {
    assert(RUN_MODES.includes(input.run_mode), "run.started requires LIVE or REPLAY run_mode");
    assert(input.recipe && typeof input.recipe === "object" && !Array.isArray(input.recipe), "run.started requires recipe");
    const recipe = input.recipe;
    boundedInt(recipe.concurrency, "recipe.concurrency", 1, 64);
    boundedInt(recipe.max_tokens, "recipe.max_tokens", 1, 4096);
    assert(typeof recipe.model_label === "string", "recipe.model_label must be a string");
    assert(MODES.includes(recipe.telemetry_mode), "recipe.telemetry_mode must be a supported mode");
    event.run_mode = input.run_mode;
    event.recipe = {
      concurrency: recipe.concurrency,
      max_tokens: recipe.max_tokens,
      model_label: sanitizeText(recipe.model_label, 80),
      telemetry_mode: recipe.telemetry_mode,
    };
    return event;
  }

  if (input.event_type === "run.completed") {
    assert(input.summary && typeof input.summary === "object" && !Array.isArray(input.summary), "run.completed requires summary");
    event.summary = validateSummary(input.summary);
    return event;
  }

  assert(typeof input.request_id === "string", `${input.event_type} requires request_id`);
  boundedInt(input.stream_index, "stream_index", 0, 63);
  event.request_id = safeRequestId(input.request_id);
  event.stream_index = input.stream_index;
  assert(event.request_id.endsWith(`_${String(event.stream_index).padStart(2, "0")}`), "request_id stream index mismatch");

  if (input.event_type === "stream.started") {
    assert(MODES.includes(input.mode), "stream.started requires mode");
    event.mode = input.mode;
    return event;
  }

  assert(Number.isInteger(input.sequence), `${input.event_type} requires sequence`);
  event.sequence = nonNegativeInt(input.sequence, "sequence");
  if (input.event_type === "stream.delta") {
    assert(MODES.includes(input.mode), "stream.delta requires mode");
    assert(typeof input.text === "string", "stream.delta requires text");
    assert(Array.isArray(input.token_ids), "stream.delta requires token_ids");
    event.mode = input.mode;
    event.text = sanitizeText(input.text);
    event.token_ids = validateTokenIds(input.token_ids);
    if (input.step_id != null) event.step_id = nonNegativeInt(input.step_id, "step_id");
    return event;
  }

  if (input.event_type === "stream.completed") {
    assert(input.status === "success" || input.status === "usage_unavailable", "stream.completed requires status");
    assert(typeof input.finish_reason === "string", "stream.completed requires finish_reason");
    assert(MODES.includes(input.mode), "stream.completed requires mode");
    if (input.usage !== null && input.usage !== undefined) validateUsage(input.usage);
    assert(Object.hasOwn(input, "usage"), "stream.completed requires usage");
    event.status = input.status;
    event.finish_reason = sanitizeText(input.finish_reason, 40);
    event.usage = validateUsage(input.usage);
    event.mode = input.mode;
    return event;
  }

  assert(FAILURE_KINDS.includes(input.failure_kind), "stream.failed requires failure_kind");
  assert(typeof input.error_code === "string", "stream.failed requires error_code");
  assert(MODES.includes(input.mode), "stream.failed requires mode");
  event.failure_kind = input.failure_kind;
  event.error_code = sanitizeErrorCode(input.error_code);
  event.mode = input.mode;
  return event;
}

export function validateSummary(input) {
  const summary = {
    requested_concurrency: boundedInt(input.requested_concurrency, "requested_concurrency", 1, 64),
    active_count: nonNegativeInt(input.active_count || 0, "active_count"),
    completed_count: nonNegativeInt(input.completed_count || 0, "completed_count"),
    failed_count: nonNegativeInt(input.failed_count || 0, "failed_count"),
    wall_time_ms: nonNegativeInt(input.wall_time_ms || 0, "wall_time_ms"),
    total_completion_tokens: nonNegativeInt(input.total_completion_tokens || 0, "total_completion_tokens"),
    aggregate_decode_tok_s: typeof input.aggregate_decode_tok_s === "number" && Number.isFinite(input.aggregate_decode_tok_s)
      ? Math.max(0, Number(input.aggregate_decode_tok_s.toFixed(3)))
      : null,
    ttft_ms: typeof input.ttft_ms === "number" && Number.isFinite(input.ttft_ms)
      ? Math.max(0, Number(input.ttft_ms.toFixed(3)))
      : null,
    completion_count: nonNegativeInt(input.completion_count || 0, "completion_count"),
    throughput_distribution: Array.isArray(input.throughput_distribution)
      ? input.throughput_distribution.slice(0, 64).map((item) => ({
        stream_index: nonNegativeInt(item.stream_index, "throughput_distribution.stream_index"),
        completion_tokens: nonNegativeInt(item.completion_tokens, "throughput_distribution.completion_tokens"),
        wall_time_ms: nonNegativeInt(item.wall_time_ms, "throughput_distribution.wall_time_ms"),
        tok_s: typeof item.tok_s === "number" && Number.isFinite(item.tok_s)
          ? Math.max(0, Number(item.tok_s.toFixed(3)))
          : null,
        status: item.status === "success" ? "success" : "failed",
        ttft_ms: typeof item.ttft_ms === "number" && Number.isFinite(item.ttft_ms)
          ? Math.max(0, Number(item.ttft_ms.toFixed(3)))
          : null,
        failure_kind: item.failure_kind && FAILURE_KINDS.includes(item.failure_kind) ? item.failure_kind : null,
      }))
      : [],
    failure_labels: Array.isArray(input.failure_labels)
      ? input.failure_labels.filter((item) => FAILURE_KINDS.includes(item)).slice(0, 16)
      : [],
    winning_recipe: sanitizeText(String(input.winning_recipe || "not selected"), 120),
  };
  return summary;
}

export function validateRunRecord(record) {
  assert(record && typeof record === "object" && !Array.isArray(record), "run record must be an object");
  assert(record.schema_version === SCHEMA_VERSION, "unsupported run schema_version");
  assert(record.kind === "benchmark.run", "run record kind must be benchmark.run");
  const runId = safeRunId(record.run_id);
  assert(RUN_MODES.includes(record.run_mode), "run record requires LIVE or REPLAY run_mode");
  assert(Array.isArray(record.events), "run record events must be an array");
  const events = record.events.map(validateEvent);
  for (const event of events) assert(event.run_id === runId, "event run_id mismatch");
  const summary = validateSummary(record.summary || {});
  return {
    schema_version: SCHEMA_VERSION,
    kind: "benchmark.run",
    run_id: runId,
    run_mode: record.run_mode,
    events,
    summary,
  };
}

export function eventLine(event) {
  return `${JSON.stringify(validateEvent(event))}\n`;
}
