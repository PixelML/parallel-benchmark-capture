import test from "node:test";
import assert from "node:assert/strict";
import { createRequestId, sanitizeText, validateEvent, validateRunRecord, SchemaError } from "../src/schema.mjs";

test("sanitizes terminal control and bidi characters", () => {
  assert.equal(sanitizeText("ok\u001b]2;bad\u0007\u202Etext"), "oktext");
});

test("validates one event envelope and drops unknown fields", () => {
  const event = validateEvent({
    schema_version: 1,
    event_type: "stream.delta",
    run_id: "demo_test",
    request_id: createRequestId("demo_test", 0),
    stream_index: 0,
    elapsed_ms: 12,
    mode: "SSE CHUNK MODE",
    sequence: 0,
    text: "hello",
    token_ids: [1, 2],
    endpoint: "must never survive",
  });
  assert.equal(event.endpoint, undefined);
  assert.deepEqual(event.token_ids, [1, 2]);
});

test("rejects malformed event and run records", () => {
  assert.throws(() => validateEvent({ schema_version: 1, event_type: "stream.delta" }), SchemaError);
  assert.throws(() => validateRunRecord({ schema_version: 1, kind: "wrong" }), SchemaError);
});
