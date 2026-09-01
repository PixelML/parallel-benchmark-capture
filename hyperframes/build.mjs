import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.dirname(fileURLToPath(import.meta.url));
const project = path.resolve(root, "..");
const template = await readFile(path.join(root, "index.template"), "utf8");
const receipt = JSON.parse(await readFile(path.join(project, "fixtures", "replay-c16.json"), "utf8"));
const video = JSON.parse(await readFile(path.join(project, "docs", "benchmark-video.json"), "utf8"));
const streams = receipt.events
  .filter((event) => event.event_type === "stream.completed")
  .map((event) => ({
    stream_index: event.stream_index,
    completion_tokens: event.usage?.completion_tokens ?? 0,
    preview: receipt.events
      .filter((candidate) => candidate.event_type === "stream.delta" && candidate.stream_index === event.stream_index)
      .slice(0, 2)
      .map((candidate) => candidate.text)
      .join("")
      .trim(),
  }));
const payload = {
  title: video.title,
  subtitle: video.subtitle,
  eyebrow: video.eyebrow,
  status: video.status,
  summary: video.summary,
  verdict: video.verdict,
  limitation: video.limitation,
  footer: video.footer,
  fixture: { streams },
};
const serialized = JSON.stringify(payload).replaceAll("<", "\\u003c").replaceAll("&", "\\u0026");
if (template.split("__VIDEO_PAYLOAD__").length !== 2) throw new Error("video payload marker missing or duplicated");
await writeFile(path.join(root, "index.html"), template.replace("__VIDEO_PAYLOAD__", serialized));
