import { SchemaError } from "./schema.mjs";

export async function* parseSseBody(body) {
  if (!body) throw new SchemaError("SSE response body is missing");
  const decoder = new TextDecoder();
  let buffer = "";
  let dataLines = [];
  for await (const chunk of body) {
    buffer += typeof chunk === "string" ? chunk : decoder.decode(chunk, { stream: true });
    const lines = buffer.split(/\r?\n/);
    buffer = lines.pop() || "";
    for (const line of lines) {
      yield* parseSseLine(line, dataLines);
    }
  }
  if (buffer) yield* parseSseLine(buffer, dataLines);
  yield* flushData(dataLines);
}

export function* parseSseLines(lines) {
  const dataLines = [];
  for (const line of lines) yield* parseSseLine(String(line).replace(/[\r\n]+$/, ""), dataLines);
  yield* flushData(dataLines);
}

function* parseSseLine(line, dataLines) {
  if (line === "") {
    yield* flushData(dataLines);
    return;
  }
  if (line.startsWith(":")) return;
  if (line.startsWith("data:")) dataLines.push(line.slice(5).replace(/^ /, ""));
}

function* flushData(dataLines) {
  if (!dataLines.length) return;
  const payload = dataLines.join("\n");
  dataLines.length = 0;
  if (payload === "[DONE]") return;
  let value;
  try {
    value = JSON.parse(payload);
  } catch {
    throw new SchemaError("invalid SSE JSON payload");
  }
  if (value && typeof value === "object" && !Array.isArray(value)) yield value;
}

export function extractDelta(payload) {
  const choice = Array.isArray(payload?.choices) && payload.choices[0] && typeof payload.choices[0] === "object"
    ? payload.choices[0]
    : {};
  const delta = choice.delta && typeof choice.delta === "object" ? choice.delta : {};
  const text = [delta.reasoning, delta.reasoning_content, delta.content, choice.text]
    .filter((value) => typeof value === "string")
    .join("");
  const tokenCandidates = [delta.token_ids, choice.token_ids, payload.output_token_ids, payload.token_ids];
  const tokenIds = tokenCandidates.find((candidate) => Array.isArray(candidate) && candidate.every((token) => Number.isInteger(token) && token >= 0)) || [];
  const finishReason = typeof choice.finish_reason === "string" ? choice.finish_reason : null;
  return {
    text,
    token_ids: tokenIds,
    finish_reason: finishReason,
    usage: payload.usage && typeof payload.usage === "object" ? payload.usage : null,
  };
}
