import test from "node:test";
import assert from "node:assert/strict";
import { extractDelta, parseSseLines } from "../src/sse.mjs";

test("parses OpenAI SSE data and done marker", () => {
  const payloads = [...parseSseLines([
    ": keepalive\n",
    'data: {"choices":[{"delta":{"content":"hi","token_ids":[7]}}]}\n',
    "\n",
    "data: [DONE]\n",
    "\n",
  ])];
  assert.equal(payloads.length, 1);
  assert.deepEqual(extractDelta(payloads[0]), { text: "hi", token_ids: [7], finish_reason: null, usage: null });
});
