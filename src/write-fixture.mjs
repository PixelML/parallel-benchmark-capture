import { mkdir, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { buildDemoRecord } from "./demo-fixture.mjs";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const record = buildDemoRecord();
await mkdir(path.join(root, "fixtures"), { recursive: true });
await writeFile(path.join(root, "fixtures", "replay-c16.json"), `${JSON.stringify(record, null, 2)}\n`);
await writeFile(path.join(root, "fixtures", "replay-c16.ndjson"), `${record.events.map((event) => JSON.stringify(event)).join("\n")}\n`);
