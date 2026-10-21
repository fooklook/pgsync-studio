# Deployment and configuration

## Configuration sources

Use environment variables or `.env` for a standalone installation. Nacos is an
optional bootstrap adapter. With the local JSON store, configure Elasticsearch
and Redis through the UI; with a PostgreSQL configuration store, environment
synchronization can initialize these service settings.

Minimal standalone console, on a POSIX shell:

```sh
export PG_USER=studio ELASTICSEARCH=true
export PGSYNC_STUDIO_PG_USERNAME=studio
export PGSYNC_STUDIO_PG_PASSWORD='set-a-local-development-password'
pgsync-ui
```

This starts the UI with JSON configuration under `runtime/studio/`; source
connections are configured in the UI. Set Elasticsearch and Redis before starting
a job. `PG_USER` is also required by the retained native settings module.

The UI's PostgreSQL username/password fields are intentionally read-only. Source
connections share `PGSYNC_STUDIO_PG_USERNAME` / `PGSYNC_STUDIO_PG_PASSWORD`; they
can also be supplied through the supported alternative environment pairs in
`pgsync/web/db.py`. This release does not add per-connection credentials.

## PostgreSQL configuration store

Set these variables separately so passwords containing `@`, `/`, or spaces do
not need to be inserted into a hand-built URL:

```dotenv
PGSYNC_STUDIO_CONFIG_DATABASE_URL=postgresql+psycopg2://studio@localhost:5432/studio
PGSYNC_STUDIO_CONFIG_DATABASE_USERNAME=studio
PGSYNC_STUDIO_CONFIG_DATABASE_PASSWORD=replace-with-a-secret
PGSYNC_STUDIO_SYNC_SERVICES_FROM_ENV=true
PGSYNC_STUDIO_SYNC_CONNECTIONS_FROM_ENV=false
ELASTICSEARCH_URL=http://localhost:9200
REDIS_HOST=localhost
REDIS_PORT=6379
```

The configuration store creates `system_pgsync_*` tables in its configured
database. It is independent of the source databases. Keep its credentials and
runtime volume persistent. Environment synchronization is implemented by the
PostgreSQL configuration store; it is not an implicit configuration import for
the JSON store.

## Optional Nacos

Set `PGSYNC_STUDIO_PROFILE=production`, `NACOS_ENABLED=true`,
`NACOS_SERVER_ADDR`, `NACOS_NAMESPACE`, `NACOS_USERNAME`, and `NACOS_PASSWORD`.
The explicit data ID setting is `PGSYNC_STUDIO_NACOS_DATA_ID`; the default is
`pgsync-studio-<profile>.yaml`. The development profile skips Nacos even if its
enabled flag is set. Keep secrets in environment variables and reference them
from the Nacos document where appropriate.

Example document:

```yaml
pgsync:
  studio:
    host: 0.0.0.0
    port: 8180
    config_database_url: postgresql+psycopg2://studio@postgres:5432/studio
    sync_services: true
    sync_connections: false
    read_replica:
      enabled: false
      host: replica.example.com
      wait_timeout_ms: 2000
      poll_interval_ms: 100
      fallback_to_primary: true
      cooldown_ms: 30000
  services:
    search:
      url: http://elasticsearch:9200
      username: ${ELASTICSEARCH_USERNAME}
      password: ${ELASTICSEARCH_PASSWORD}
      verify_certs: true
    redis:
      host: redis
      port: 6379
      password: ${REDIS_PASSWORD}
```

Database credentials remain supplied by the service's environment. The optional
Nacos adapter supports the existing Studio configuration shape without requiring
any organization-specific directory layout or deployment service.

## Optional replica reads

```dotenv
PGSYNC_STUDIO_READ_REPLICA_ENABLED=true
PGSYNC_STUDIO_READ_REPLICA_HOST=replica.example.com
PGSYNC_STUDIO_READ_REPLICA_PORT=5432
PGSYNC_STUDIO_READ_REPLICA_WAIT_TIMEOUT_MS=2000
PGSYNC_STUDIO_READ_REPLICA_POLL_INTERVAL_MS=100
PGSYNC_STUDIO_READ_REPLICA_FALLBACK_TO_PRIMARY=true
PGSYNC_STUDIO_READ_REPLICA_COOLDOWN_MS=30000
```

Source connections must still address a primary. Replica reads use the source
connection's credentials, so the account must be valid on both servers. Each
assembly work unit captures a required WAL position and verifies that the
replica has replayed it before querying. An unavailable or slow replica can
fall back to primary according to policy. Trigger and outbox writes always use
the primary. Leave routing disabled when no replica exists.

## Outbox maintenance

Row cleanup and partition maintenance are separate mechanisms. Both default to
disabled. The code supports daily partitions and creation of future partitions,
but does not automatically convert a populated ordinary outbox table into a
partitioned one. Plan any such migration separately with task backups and recovery.

Retention is measured in UTC. A historical partition must satisfy both its age
policy and the safe watermarks of every relevant consumer. Paused or lagging
consumers can intentionally block removal. Orphan-event handling requires the
corresponding safe capture reconciliation policy; it does not allow arbitrary
unconsumed events to be deleted.

Read `durable_outbox_cleanup_config`, `durable_outbox_partition_maintenance_config`, and
the `durable_outbox_*` status exposed by `/api/bootstrap` before enabling a policy.
See the configuration resolver methods in `pgsync/web/sync_manager.py` for the
complete supported names; do not enable deletion based only on a retention period.

## Runtime observations

Use `/api/jobs/<id>/debug` to check actual runner/listener threads and Redis
queues alongside the persisted task status. A zero root queue does not prove
the durable outbox has caught up. Check `durable_event_backlog_present` and
its per-group values. `durable_event_lag=1` can be an existence flag rather than
an exact event count; inspect `durable_event_lag_exact` before interpreting it.

The `/metrics` endpoint exposes query, bulk-indexing, runner, and queue metrics.
Protect diagnostics with the same access controls as the administrative API.
