import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import Ajv2020 from "ajv/dist/2020.js";
import { validateEvent, validateRunRecord, SchemaError } from "../src/schema.mjs";

const ROOT = path.resolve(import.meta.dirname, "..");

async function readJson(relativePath) {
  return JSON.parse(await readFile(path.join(ROOT, relativePath), "utf8"));
}

test("published event and run schemas validate portable fixtures", async () => {
  const eventSchema = await readJson("schemas/event.schema.json");
  const runSchema = await readJson("schemas/run.schema.json");
  const summarySchema = await readJson("schemas/summary.schema.json");
  const validEvent = await readJson("schemas/fixtures/valid-event.json");
  const invalidEvent = await readJson("schemas/fixtures/invalid-event.json");
  const validCompletedEvent = await readJson("schemas/fixtures/valid-run-completed-event.json");
  const invalidCompletedEvent = await readJson("schemas/fixtures/invalid-run-completed-event.json");
  const validRun = await readJson("schemas/fixtures/valid-run.json");
  const invalidRun = await readJson("schemas/fixtures/invalid-run.json");

  const ajv = new Ajv2020({ allErrors: true, strict: true, strictRequired: false });
  ajv.addSchema(summarySchema);
  ajv.addSchema(eventSchema);
  const validateEventSchema = ajv.compile(eventSchema);
  const validateSummarySchema = ajv.compile(summarySchema);
  const validateRunSchema = ajv.compile(runSchema);

  assert.equal(validateEventSchema(validEvent), true, JSON.stringify(validateEventSchema.errors));
  assert.equal(validateEventSchema(invalidEvent), false);
  assert.equal(validateEventSchema(validCompletedEvent), true, JSON.stringify(validateEventSchema.errors));
  assert.equal(validateEventSchema(invalidCompletedEvent), false);
  assert.equal(validateSummarySchema(validCompletedEvent.summary), true, JSON.stringify(validateSummarySchema.errors));
  assert.equal(validateSummarySchema(invalidCompletedEvent.summary), false);
  assert.equal(validateRunSchema(validRun), true, JSON.stringify(validateRunSchema.errors));
  assert.equal(validateRunSchema(invalidRun), false);
  assert.doesNotThrow(() => validateRunRecord(validRun));
  assert.throws(() => validateRunRecord(invalidRun), SchemaError);
  assert.doesNotThrow(() => validateEvent(validCompletedEvent));
  assert.throws(() => validateEvent(invalidCompletedEvent), SchemaError);
});
