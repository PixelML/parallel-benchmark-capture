# Parallel benchmark capture

## Boundary

The local controller is the only component allowed to know an operator endpoint
or API key. It performs concurrent OpenAI-compatible streaming requests, parses
SSE, validates optional version-gated telemetry, computes metrics from terminal
usage objects, and writes sanitized run events. The browser receives only a
public-safe projection over an SSE connection.

HyperFrames consumes only a frozen replay record. A render has no network path
and no access to live endpoint configuration.

## Event model

One versioned event envelope covers run lifecycle, stream lifecycle, output
deltas, and terminal usage:

```json
{
  "schema_version": 1,
  "event_type": "stream.delta",
  "run_id": "demo_20260901_c16",
  "request_id": "req_demo_20260901_c16_00",
  "stream_index": 0,
  "elapsed_ms": 420,
  "mode": "SSE CHUNK MODE",
  "sequence": 3,
  "text": "hello",
  "token_ids": [101, 102]
}
```

Lifecycle events omit fields that do not apply. `stream.completed` carries the
authoritative final `usage` object. A failed or wedged stream carries
`failure_kind` and never receives a fabricated numeric score.

`mode` is exactly `SSE CHUNK MODE` unless a version-gated ParallelHue adapter
proves the event stream and reconciliation contract; the controller never
infers scheduler steps from transport chunks.

The exact adapter is pinned to ParallelHue revision
[`be9b02680f0a2326cc7068dc592dd0ad2fe7de71`](https://github.com/hikarioyama/ParallelHue/tree/be9b02680f0a2326cc7068dc592dd0ad2fe7de71).
An owning experiment supplies a fresh run ID, a sanitized same-run sidecar
export, and its SHA-256 fingerprint. The controller creates a fresh local run
binding and claims that export once in a private single-use ledger; a second
controller run cannot consume the same run ID or export fingerprint. For stream
`i`, the controller sends
`ph1_<32-hex-run-id>_<i>` in the OpenAI-compatible request body's
`request_id`; the sidecar must carry the same run ID and request ID, with
`choice_index: 0`. The controller rejects stale/differently bound exports
before dispatch and then reconciles every SSE chunk against the ordered text
and token IDs. The run IDs, fingerprint, and ledger are controller-only and
never enter a public event.

## Live flow

1. `POST /api/runs` validates a preset or arbitrary concurrency and creates a
   run record without returning endpoint configuration.
2. The controller launches bounded concurrent `fetch` requests. Each request
   gets a run-scoped ID and a sanitized stream of events.
3. The controller parses `stream_options.include_usage` terminal frames. It
   records final usage and request timings; it does not sum chunks as tokens.
4. `GET /api/runs/<id>/events` exposes the sanitized event stream to the
   dashboard. No headers, URLs, prompts, or environment values are emitted.
5. The run summary computes successful completion tokens divided by run wall
   time for aggregate decode throughput, plus a per-request distribution.

## Replay flow

Replay loads `fixtures/replay-c16.json`, validates every event against the same
schema, and schedules the same dashboard events with their recorded relative
timings. The HyperFrames project reads this frozen JSON and renders the live
grid first, then the four key numbers and winning recipe.

## Safety rules

- Endpoint and key are read from process environment or an ignored local file.
- Endpoint, key, private prompts, hostnames, addresses, and raw logs never
  enter browser bundles, persisted public fixtures, rendered media metadata,
  GitHub metadata, or release artifacts.
- The public fixture is synthetic and explicitly labeled `SSE CHUNK MODE`.
- Completion tokens come only from a final usage object. Missing usage is an
  explicit `usage_unavailable` condition.
- Exact telemetry is opt-in only; a fresh owner handoff binds the sidecar to a
  single ParallelHue run, the controller consumes it once, and stale or
  mismatched IDs fail closed.
