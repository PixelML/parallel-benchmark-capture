import { createServer } from "node:http";

const TOKENS = [
  "parallel", "requests", "stay", "inside", "the", "controller", "while", "events", "remain", "sanitized", "and", "replayable",
];

async function readJson(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    return {};
  }
}

function streamIndex(requestBody) {
  const candidate = String(requestBody.request_id || "").split("_").pop();
  const parsed = Number(candidate);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : 0;
}

export function createMockServer({ port = 4180, host = "127.0.0.1", delayMs = 45 } = {}) {
  const server = createServer(async (request, response) => {
    if (request.method === "GET" && request.url === "/health") {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ ok: true }));
      return;
    }
    if (request.method !== "POST" || !/\/v1\/(?:chat\/)?completions$/.test(request.url || "")) {
      response.writeHead(404);
      response.end();
      return;
    }
    const body = await readJson(request);
    const index = streamIndex(body);
    const maxTokens = Number.isInteger(body.max_tokens) ? Math.max(1, Math.min(body.max_tokens, 64)) : 32;
    const output = Array.from({ length: Math.min(6, Math.max(3, Math.ceil(maxTokens / 10))) }, (_, chunkIndex) => {
      const word = TOKENS[(index + chunkIndex) % TOKENS.length];
      return `${word}${chunkIndex === 5 ? "." : ""} `;
    });
    const tokenCount = output.reduce((total, chunk) => total + chunk.trim().split(/\s+/).length, 0);
    response.writeHead(200, {
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-cache",
      connection: "keep-alive",
    });
    response.write(`data: ${JSON.stringify({ choices: [{ delta: { role: "assistant" } }] })}\n\n`);
    for (let chunkIndex = 0; chunkIndex < output.length; chunkIndex += 1) {
      if (response.destroyed) return;
      const text = output[chunkIndex];
      response.write(`data: ${JSON.stringify({ choices: [{ delta: { content: text, token_ids: [index * 100 + chunkIndex] } }] })}\n\n`);
      await new Promise((resolve) => setTimeout(resolve, delayMs + ((index + chunkIndex) % 3) * 9));
    }
    if (!response.destroyed) {
      response.write(`data: ${JSON.stringify({ choices: [{ delta: {}, finish_reason: "stop" }], usage: { prompt_tokens: 12, completion_tokens: tokenCount, total_tokens: 12 + tokenCount } })}\n\n`);
      response.write("data: [DONE]\n\n");
      response.end();
    }
  });
  return {
    server,
    async listen() {
      await new Promise((resolve) => server.listen(port, host, resolve));
      return server;
    },
    close() {
      return new Promise((resolve) => server.close(() => resolve()));
    },
  };
}
