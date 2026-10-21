# PGSync Studio

[中文说明](README.zh-CN.md)

A web console and document assembly engine for synchronizing PostgreSQL data
into Elasticsearch. Configure connections, join related tables across databases,
build a full baseline, and keep the resulting documents up to date.

This is an independent extension of [PGSync](https://github.com/toluaina/pgsync),
distributed under MIT. It is not the upstream PGSync release. The original
copyright notice and credits are preserved in [LICENSE](LICENSE),
[AUTHORS.rst](AUTHORS.rst), and [UPSTREAM.md](UPSTREAM.md).

## Features

- A web UI for database connections, jobs, relation trees, queries, aliases, and runtime diagnostics.
- Single-database and cross-database document assembly, soft deletes, and fixed relation filters.
- Baseline synchronization, PostgreSQL triggers/notifications, durable outbox events, and recovery scans.
- Redis coordination, per-job leases, persistent root queues, and PostgreSQL-backed configuration.
- Optional replica reads with a batch-level WAL LSN fence, bounded waits, and primary fallback.
- Optional daily outbox partitions, advance partition creation, retention checks, and consumer watermarks.
- Prometheus metrics at `/metrics`.

The current Web UI is in Simplified Chinese; the public setup documentation is bilingual.

Studio connections support PostgreSQL. The upstream command-line code is retained;
its wider backend support is not a compatibility claim for the Studio interface.

## Quick start

Install Docker with Compose v2, then run these commands from the repository root:

```sh
cp .env.example .env
# Edit DEMO_POSTGRES_PASSWORD in .env before the first start.
docker compose up --build -d --wait
docker compose exec studio python examples/studio/bootstrap_demo.py
```

In PowerShell, use `Copy-Item .env.example .env` for the first command.

Open <http://127.0.0.1:8180>. The demo creates two PostgreSQL databases (`catalog`
and `analytics`), a small book dataset, and the `book_catalog_v1` synchronization
job. It joins books with authors and metrics across the two databases. The
bootstrap script preserves an existing demo job rather than replacing it.

Verify the documents from the **ES query** screen, selecting `book_catalog_v1`:

```json
{"query": {"match_all": {}}, "size": 10}
```

Change a metric and confirm the nested `book_metrics` value changes in ES:

```sh
docker compose exec postgres psql -U studio -d analytics -c "UPDATE book_metrics SET views = views + 1, updated_at = now() WHERE book_id = 1;"
```

Stop the demo with `docker compose down`. Named volumes preserve the configuration,
database, Redis state, and search index. The SQL initializer runs only when the
PostgreSQL data volume is first created; later password edits do not change an
existing PostgreSQL role's password.

The Compose stack is a local demonstration. Only the Studio port is published,
on loopback; PostgreSQL, Redis, and Elasticsearch remain on the Compose network.

## Development

Use Python 3.10 or newer in a dedicated virtual environment. This distribution
includes the `pgsync` import package, so do not also install upstream `pgsync`
into the same environment.

```sh
python -m venv .venv
# Activate .venv for your shell.
python -m pip install -r requirements/dev.txt
python -m pip install --no-deps -e .
```

Studio and replica-routing unit tests do not require running databases:

```sh
python -m pytest -q tests/test_studio.py tests/test_studio_metrics.py tests/test_read_replica.py tests/test_nacos_read_replica.py
python tools/check_public_export.py
```

Set `PG_USER=studio` and `ELASTICSEARCH=true` in the test environment before
running these commands. See [CONTRIBUTING.md](CONTRIBUTING.md) for PowerShell
examples and the distinction between unit and integration tests.

For a standalone empty console, configure PostgreSQL credentials in the environment
or `.env`, then run `pgsync-ui`. Configure Elasticsearch and Redis through the UI
when using the local JSON configuration store.

## Deployment and operational limits

Read [deployment and configuration](docs/deployment.md) before connecting your
own databases. Nacos is optional; the default demo uses environment variables.
Administrative access must be protected by an authenticated reverse proxy or
private network: this version does not include user authentication or role-based
access control. See [SECURITY.md](SECURITY.md).

Sync is eventually consistent, not an atomic transaction across PostgreSQL and
Elasticsearch. Trigger/outbox writes require a primary connection. Cursor fields,
source indexes, event throughput, and relation fan-out affect latency. Daily
partition retention alone does not guarantee a partition is safe to drop: active
consumer watermarks are also checked. Maintenance and replica routing are opt-in.

## Layout

- `pgsync/`: upstream engine and the Studio implementation under `web/`.
- `examples/studio/`: synthetic, local-only cross-database demo.
- `examples/`: preserved upstream examples.
- `tests/`: unit tests and upstream service-dependent integration tests.
- `docs/`: deployment and maintenance documentation.
- `.github/workflows/ci.yml`: unit tests, package builds, and public-export checks.

The repository contains no environment backups, production task definitions,
internal deployment pipelines, or private repository history.
