import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { validateEvent, SchemaError } from "./schema.mjs";

export const PARALLELHUE_PROTOCOL_REVISION = "be9b02680f0a2326cc7068dc592dd0ad2fe7de71";

/**
 * Read a sanitized ParallelHue StepEvent NDJSON sidecar.
 *
 * This adapter intentionally proves only the published wire shape and
 * contiguous per-request ordering. The controller still reconciles text and
 * token IDs against each SSE chunk before emitting EXACT SCHEDULER STEP.
 */
export async function loadParallelHueTelemetry(filePath) {
  return (await loadParallelHueTelemetryExport(filePath)).byRequest;
}

/**
 * Read one owner-bound export and return its single-use fingerprint. The
 * controller claims this fingerprint before dispatch so the same export
 * cannot be replayed for a second measured run.
 */
export async function loadParallelHueTelemetryExport(filePath) {
  if (!filePath) return { byRequest: new Map(), runId: null, fingerprint: null };
  const text = await readFile(filePath, "utf8");
  const byRequest = new Map();
  const expected = new Map();
  let sidecarRunId = null;
  for (const [lineNumber, line] of text.split(/\r?\n/).entries()) {
    if (!line.trim()) continue;
    let raw;
    try {
      raw = JSON.parse(line);
    } catch {
      throw new SchemaError(`invalid telemetry JSON at line ${lineNumber + 1}`);
    }
    if (typeof raw.run_id !== "string" || !/^[0-9a-f]{32}$/.test(raw.run_id)) {
      throw new SchemaError(`invalid ParallelHue run_id at line ${lineNumber + 1}`);
    }
    if (sidecarRunId && sidecarRunId !== raw.run_id) {
      throw new SchemaError(`telemetry run_id changed at line ${lineNumber + 1}`);
    }
    sidecarRunId = raw.run_id;
    const event = normalizeStepEvent(raw);
    const next = expected.get(event.request_id) || 0;
    if (event.sequence !== next) throw new SchemaError(`telemetry sequence gap at line ${lineNumber + 1}`);
    expected.set(event.request_id, next + 1);
    const events = byRequest.get(event.request_id) || [];
    events.push(event);
    byRequest.set(event.request_id, events);
  }
  return {
    byRequest,
    runId: sidecarRunId,
    fingerprint: createHash("sha256").update(text, "utf8").digest("hex"),
  };
}

export function indexTelemetryByStream(byRequest) {
  const byStream = new Map();
  for (const events of byRequest.values()) {
    for (const event of events) {
      const list = byStream.get(event.stream_index) || [];
      list.push(event);
      byStream.set(event.stream_index, list);
    }
  }
  for (const list of byStream.values()) list.sort((a, b) => a.sequence - b.sequence);
  return byStream;
}

function normalizeStepEvent(raw) {
  if (!raw || typeof raw !== "object") throw new SchemaError("telemetry event must be an object");
  if (raw.schema_version !== 1) throw new SchemaError("unsupported ParallelHue telemetry schema");
  const requestId = typeof raw.request_id === "string" ? raw.request_id : "";
  const match = /^ph1_([0-9a-f]{32})_(\d+)$/.exec(requestId);
  if (!match) throw new SchemaError("invalid ParallelHue request_id");
  if (raw.run_id !== match[1]) throw new SchemaError("ParallelHue request_id run mismatch");
  const streamIndex = Number(match[2]);
  if (!Number.isInteger(streamIndex) || streamIndex < 0 || streamIndex > 63) {
    throw new SchemaError("ParallelHue stream index must be 0..63");
  }
  if (!Number.isInteger(raw.choice_index) || raw.choice_index < 0) {
    throw new SchemaError("invalid ParallelHue choice_index");
  }
  if (typeof raw.finished !== "boolean") throw new SchemaError("invalid ParallelHue finished flag");
  const runId = `telemetry_${match[1].slice(0, 24)}`;
  const normalized = validateEvent({
    schema_version: 1,
    event_type: "stream.delta",
    run_id: runId,
    request_id: `req_${runId}_${String(streamIndex).padStart(2, "0")}`,
    stream_index: streamIndex,
    elapsed_ms: Number.isInteger(raw.sequence) ? raw.sequence : 0,
    mode: "EXACT SCHEDULER STEP",
    sequence: raw.sequence,
    text: raw.text,
    token_ids: raw.token_ids,
    step_id: raw.step_id,
  });
  return {
    ...normalized,
    parallelhue_run_id: match[1],
    parallelhue_request_id: requestId,
    finished: raw.finished === true,
    choice_index: raw.choice_index,
  };
}

export function telemetryRequestKey(streamIndex) {
  if (!Number.isInteger(streamIndex) || streamIndex < 0) throw new SchemaError("invalid telemetry stream index");
  return String(streamIndex);
}

/** Reconcile a chunk against a contiguous prefix of telemetry events. */
export function reconcileTelemetry(events, text, tokenIds) {
  if (!Array.isArray(events) || !events.length) return null;
  const ids = Array.isArray(tokenIds) ? tokenIds : [];
  const matched = [];
  const textParts = [];
  const idParts = [];
  for (const event of events) {
    matched.push(event);
    textParts.push(event.text);
    idParts.push(...event.token_ids);
    if (textParts.join("") === text && idParts.length === ids.length && idParts.every((value, index) => value === ids[index])) {
      return matched;
    }
    if (textParts.join("").length > text.length || idParts.length > ids.length) return null;
  }
  return null;
}
