# Changelog

## Unreleased

- Retry Elasticsearch 401/403 access failures instead of permanently blocking
  resumable tasks. Use persisted, jittered 60-to-300-second backoff without a
  maximum retry count, including item-level bulk indexing and deletion errors.
- Migrate recognizable legacy ES access blocks without bypassing manual stops,
  baseline checks, incompatible resume strategies, or exclusive job leases.
- Show ES access retry plans in the UI and add regression coverage for a
  disconnect-to-401-to-403-to-recovery sequence, retained queues, and normal
  incremental catch-up without rebuilding tasks or indices.
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
