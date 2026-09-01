# Verification record

This checkout is a local, public-safe draft. It contains no live endpoint
receipt and makes no scheduler-step claim in the checked-in fixture or video.

## Automated checks

- `npm test`: 13 tests passed.
- `npm run validate:fixture`: 130 events, 16 streams.
- HyperFrames `0.8.22` upgrade check: no update available.
- HyperFrames check: lint, runtime, layout, motion, and contrast passed; 152
  contrast checks passed with zero errors.
- Snapshots: four timed frames plus an end frame and contact sheet were
  inspected.

## Render

The replay-only composition renders to 1080 x 1080 H.264, 30 fps, 8 seconds.
Its sequence is the 16-stream grid, authoritative usage receipt, then four
summary facts and the winning recipe. The rendered poster is the final frame.

```sh
cd hyperframes
npm run build
npx --yes hyperframes@0.8.22 check
npx --yes hyperframes@0.8.22 snapshot . --at 0.4,2.2,4.8,7.7 --output ./snapshots
npx --yes hyperframes@0.8.22 render . --quality high --workers 2 --output ../docs/parallel-benchmark-capture.mp4
ffmpeg -y -sseof -0.1 -i ../docs/parallel-benchmark-capture.mp4 -frames:v 1 ../docs/parallel-benchmark-capture-poster.png
```

## Boundary

The browser and HyperFrames project receive sanitized replay events only.
Controller endpoint configuration and authorization headers are process-local;
they are not emitted in events, receipts, browser bundles, logs, or media
metadata. The exact telemetry adapter is opt-in and fails closed unless a
fresh owner-bound run ID and single, ordered, version-1 ParallelHue sidecar
reconcile every stream. The adapter contract is pinned to ParallelHue revision
`be9b02680f0a2326cc7068dc592dd0ad2fe7de71`.
