# Changelog

## Unreleased

- Keep the recovery supervisor alive through temporary PostgreSQL/configuration
  store failures; reap dead runners and retry eligible incremental jobs with
  capped exponential backoff and jitter.
- Stop work on lease loss, wait for old workers to exit, and guard ES writes
  and Redis batch acknowledgements with ownership checks.
- Atomically acknowledge or requeue Redis batches by batch ID so stale runners
  cannot discard a replacement runner's inflight work.
- Expose recovery status in the UI, diagnostics, and Prometheus metrics; add
  dependency-independent `/health/live` and update the demo health check.
- Add fault-injection and runtime-credential regression tests, including Lua
  script coverage, and document Kubernetes probe rollout and recovery limits.

## 0.1.0

- Prepare an independent MIT-licensed PGSync Studio source distribution.
- Preserve the native engine, Web UI, cross-database assembly, durable event
  capture, recovery, per-job coordination, replica routing, and partition maintenance.
- Provide neutral UI examples, environment-based setup, and a local book-catalog demo.
- Add standalone packaging and CI without internal deployment or publication credentials.
