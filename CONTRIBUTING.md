# Contributing

Use a separate virtual environment and install `requirements/dev.txt`, followed
by `python -m pip install --no-deps -e .`. Do not install the upstream PyPI
distribution in that same environment; it uses the same import namespace.

## Tests

On POSIX shells:

```sh
export PG_USER=studio ELASTICSEARCH=true
python -m pytest -q tests/test_studio.py tests/test_studio_metrics.py tests/test_read_replica.py tests/test_nacos_read_replica.py
```

In PowerShell:

```powershell
$env:PG_USER = 'studio'
$env:ELASTICSEARCH = 'true'
python -m pytest -q tests/test_studio.py tests/test_studio_metrics.py tests/test_read_replica.py tests/test_nacos_read_replica.py
```

These unit tests use temporary stores, mocks, and synthetic data. The remaining
upstream tests exercise real PostgreSQL/Redis/Elasticsearch services. Run those
only against disposable instances configured with a `testdb` database and the
`PG_*`, `REDIS_*`, and `ELASTICSEARCH_*` settings. Some integration fixtures create
or delete tables, databases, replication slots, and indices.

The public CI runs the unit selection and a local Compose end-to-end demo. The
integration tests for other upstream backends are retained but are not claimed
as validated by the Studio CI.

## Packaging

```sh
python -m pip install build
python -m build
```

The wheel must include `pgsync/web/static/` so the installed `pgsync-ui` command
can serve the UI. The source distribution includes the demo, docs, tests, and
license. No publishing token is required to build either artifact.

## Public changes

Run `python tools/check_public_export.py` before committing. Keep all `.env`
files, runtime state, exports of real task definitions, credentials, private
keys, and deployment-specific addresses outside the repository. Example
credentials and data must be clearly synthetic. This lightweight check is a
guardrail and does not replace reviewing a change before publication.

Submit changes through pull requests. Describe the resulting behavior, the
affected synchronization mode, and the validation performed. Retain the upstream
MIT notice in distributed copies.
