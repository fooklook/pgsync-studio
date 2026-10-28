# Task auto-recovery

Studio supervises its task runners and retries eligible incremental tasks after
temporary PostgreSQL, configuration-store, or Redis failures. No extra retry
switch is required. This covers recovery of an already initialized task; it is
not an automatic full rebuild of an incomplete or damaged synchronization.

## Recovery behavior

- A task must still be desired `running`, have completed its baseline, and have
  a checkpoint compatible with its synchronization mode. Manually stopped tasks
  stay stopped. A task marked as requiring a full sync is not auto-resumed.
- A failed recovery cycle is logged and retried; a temporary configuration-store
  outage must not permanently kill the supervisor. Dead runner threads are
  removed even when their exception handler could not persist a failed status.
- Failed tasks use persisted exponential backoff starting at 10 seconds,
  with up to 10% jitter and a 300-second cap. The default supervisor poll interval
  is 10 seconds (`PGSYNC_STUDIO_RECOVERY_POLL_INTERVAL_SECONDS`). A healthy resumed
  runner resets its consecutive-failure backoff after 60 seconds.
- A runner that loses its Redis lease stops new work. Its old workers must exit
  before a replacement is started, and ownership is checked around ES writes
  and batch acknowledgement. Lease inspection failures do not establish that
  a lease is free.
- Redis batch acknowledgement and requeue use atomic Lua scripts and batch IDs.
  A stale batch cannot acknowledge or requeue a newer owner's inflight batch.
  Uncertain writes retain their root keys for replay; repeated writes can occur
  under the existing document-ID based synchronization semantics.
- Recognized authentication, PostgreSQL schema/permission, and search
  configuration/permission errors block automatic retries. Fix the cause and
  start the task manually; inspect `runner_recovery_blocked_reason` first.

Restore dependency connectivity and credentials before expecting a task to
resume. A long outage may leave up to 300 seconds of retry delay, plus supervisor
polling, worker shutdown, lease handling, and backlog processing. Recovery of the
runner and completion of catch-up are separate observations.

## Persistent state and limits

Keep the configuration store, runtime checkpoints, Redis queue contents, and
required outbox history persistent. Configure Redis persistence and eviction
policy for this workload. Reconnecting to Redis with its existing queues is
covered; flushing Redis or losing its data is not equivalent to a connection
outage. Queue loss can require a controlled rebuild or compensation.

This is eventually consistent synchronization. It does not provide a distributed
transaction or exactly-once delivery across PostgreSQL, Redis, and Elasticsearch.
It also cannot recreate events that were never captured because source triggers
were disabled. Incomplete baselines and tasks requiring full synchronization
need operator review.

Native setup and LISTEN reconnects resolve the current shared PostgreSQL
credentials at runtime. Masked connection records do not need stored passwords;
the service must still receive valid credentials through its environment.

## Health probes and deployment

`GET /health/live` returns HTTP 200 while the local recovery supervisor thread is
alive and not stopping, or 503 otherwise. It reads local state only, so a database
or Redis outage does not itself fail this probe. This is a thread-liveness
signal, not a guarantee of timely task progress or a check for a stuck thread.
The response also contains the last cycle time, last cycle error, and error count.

Replace dependency-sensitive container liveness checks with this endpoint when
deploying the new image. Update the image and probes in the same Deployment
template change; changing the image in a separate rollout can briefly run the
wrong probe against an older image without this endpoint.

Merge this fragment into your existing Kubernetes container configuration:

```yaml
spec:
  template:
    spec:
      terminationGracePeriodSeconds: 60
      containers:
        - name: pgsync-studio
          image: your-registry.example.com/pgsync-studio:your-version
          startupProbe:
            httpGet:
              path: /health/live
              port: 8180
            periodSeconds: 5
            timeoutSeconds: 5
            failureThreshold: 60
          livenessProbe:
            httpGet:
              path: /health/live
              port: 8180
            periodSeconds: 10
            timeoutSeconds: 5
            failureThreshold: 6
```

Keep your environment settings, secrets, volumes, pull policy, and readiness
policy in the existing manifest. The health endpoint does not verify source
availability, search readiness, or that the tasks are caught up. Do not use a
temporary dependency outage as the reason to repeatedly restart the container.
The Compose demo already checks `/health/live`; Docker health status alone does
not configure automatic restarts of an unhealthy container.

## Verify recovery

Use `/api/jobs/<id>/debug` to compare persisted status with actual runner and
listener threads, retry state, and Redis hot/backlog/inflight queues. Check
durable outbox backlog indicators too; an empty root queue is insufficient.
Prometheus `/metrics` exposes recovery supervisor, runner liveness, retry, and
last incremental-sync metrics for external alerting.

For an end-to-end fault drill, use disposable databases, Redis, and a synthetic
job. Interrupt connectivity for only that Studio instance, keep Redis data and
outbox history intact, restore access, and verify runner/listener recovery and
the final documents against the source. Include an outage longer than the
configured lease TTL and a manual-stop case. Never stop shared dependencies to
perform this check.

The unit suite includes temporary configuration-store failures, dead runners,
lease loss, recovery-loop failures, manual stops, blocked authentication errors,
stale batch acknowledgements, and runtime credential resolution. See
[Contributing](../CONTRIBUTING.md) for the commands.
