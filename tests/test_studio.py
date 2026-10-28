"""Tests for the PGSync Studio extension."""

import io
import json
import os
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa

import pgsync.web.config_store as config_store_module
import pgsync.web.db as db_module
import pgsync.web.sync_manager as sync_manager_module
from pgsync.web.config_store import ConfigStore, default_config
from pgsync.web.credentials import resolve_env_placeholders
from pgsync.web.db import (
    build_postgres_url,
    build_seek_pagination_where,
    config_database_url_from_env,
    DatabaseManager,
    format_order_by,
    MASKED_SHARED_CREDENTIAL,
    physical_database_identity,
)
from pgsync.web.external_store import create_store, DatabaseConfigStore
from pgsync.web.nacos_bootstrap import apply_nacos_config
from pgsync.web.redis_coordination import (
    DEFAULT_LEASE_KEY_PREFIX,
    DEFAULT_LEASE_RENEW_INTERVAL_SECONDS,
    DEFAULT_LEASE_TTL_SECONDS,
    NoopJobLease,
    RedisJobLease,
    resolve_job_lease_key_prefix,
    resolve_job_lease_renew_interval_seconds,
    resolve_job_lease_ttl_seconds,
)
from pgsync.web.sync_manager import (
    AssemblyRunner,
    CrossDatabaseHybridRunner,
    DEFAULT_AUTO_DISCOVER_CONNECTIONS_TIME,
    NativeProcessRunner,
    RuntimeTuningConfig,
    SearchAdapter,
    StudioError,
    SyncManager,
)


class FakeRootQueue:
    backend = "fake"

    def __init__(self) -> None:
        self.hot: list[tuple] = []
        self.backlog: list[tuple] = []

    def _enqueue(self, target: list[tuple], root_keys) -> int:
        added = 0
        for root_key in root_keys:
            normalized = tuple(root_key)
            if normalized in self.hot or normalized in self.backlog:
                continue
            target.append(normalized)
            added += 1
        return added

    def clear(self) -> None:
        self.hot.clear()
        self.backlog.clear()

    def recover_inflight(self, *, to_hot: bool = True) -> int:
        return 0

    def enqueue_hot(self, root_keys) -> int:
        return self._enqueue(self.hot, root_keys)

    def enqueue_backlog(self, root_keys) -> int:
        return self._enqueue(self.backlog, root_keys)

    def claim_batch(self, *, hot_limit: int, backlog_limit: int) -> dict:
        claimed_hot = self.hot[:hot_limit]
        claimed_backlog = self.backlog[:backlog_limit]
        self.hot = self.hot[hot_limit:]
        self.backlog = self.backlog[backlog_limit:]
        return {
            "batch_id": (
                "fake-batch" if claimed_hot or claimed_backlog else None
            ),
            "root_keys": [*claimed_hot, *claimed_backlog],
            "hot_count": len(claimed_hot),
            "backlog_count": len(claimed_backlog),
        }

    def ack_batch(self, batch_id=None) -> None:
        return None

    def requeue_inflight(self, batch_id=None, *, to_hot: bool = True) -> int:
        return 0

    def queued_size(self) -> int:
        return len(self.hot) + len(self.backlog)

    def snapshot(self, sample_size: int = 10) -> dict:
        sample = [
            list(item) for item in [*self.hot, *self.backlog][:sample_size]
        ]
        return {
            "backend": self.backend,
            "queued_size": self.queued_size(),
            "hot_size": len(self.hot),
            "backlog_size": len(self.backlog),
            "inflight_size": 0,
            "inflight_batch_id": None,
            "sample": sample,
        }


class FakeInflightRootQueue(FakeRootQueue):
    def __init__(self) -> None:
        super().__init__()
        self.inflight: list[tuple] = []
        self.acked: list[str | None] = []
        self.requeued: list[str | None] = []

    def claim_batch(self, *, hot_limit: int, backlog_limit: int) -> dict:
        batch = super().claim_batch(
            hot_limit=hot_limit,
            backlog_limit=backlog_limit,
        )
        self.inflight = list(batch["root_keys"])
        return batch

    def ack_batch(self, batch_id=None) -> None:
        self.acked.append(batch_id)
        self.inflight.clear()

    def requeue_inflight(self, batch_id=None, *, to_hot: bool = True) -> int:
        self.requeued.append(batch_id)
        target = self.hot if to_hot else self.backlog
        moved = len(self.inflight)
        target[0:0] = self.inflight
        self.inflight.clear()
        return moved

    def queued_size(self) -> int:
        return super().queued_size() + len(self.inflight)

    def snapshot(self, sample_size: int = 10) -> dict:
        snapshot = super().snapshot(sample_size=sample_size)
        snapshot["queued_size"] = self.queued_size()
        snapshot["inflight_size"] = len(self.inflight)
        snapshot["inflight_batch_id"] = "fake-batch" if self.inflight else None
        return snapshot


def test_resolve_env_placeholders_is_recursive_and_non_mutating(monkeypatch):
    monkeypatch.setenv("PGSYNC_SECRET_VALUE", "real-secret")
    payload = {
        "password": "${PGSYNC_SECRET_VALUE}",
        "url": "postgresql://user:${PGSYNC_SECRET_VALUE}@db:5432/app",
        "nested": ["${MISSING_VALUE:fallback}", 7],
    }

    resolved = resolve_env_placeholders(payload)

    assert resolved["password"] == "real-secret"
    assert resolved["url"] == "postgresql://user:real-secret@db:5432/app"
    assert resolved["nested"] == ["fallback", 7]
    assert payload["password"] == "${PGSYNC_SECRET_VALUE}"


def test_build_postgres_url_resolves_password_placeholder(monkeypatch):
    monkeypatch.setenv("PGSYNC_DB_PASSWORD", "p@ ss/")

    url = build_postgres_url(
        {
            "host": "127.0.0.1",
            "port": 5432,
            "database": "demo",
            "username": "app",
            "password": "${PGSYNC_DB_PASSWORD}",
        }
    )

    assert url == "postgresql+psycopg2://app:p%40+ss%2F@127.0.0.1:5432/demo"


def test_config_store_persists_connection(tmp_path: Path):
    store = ConfigStore(str(tmp_path / "config.json"))
    connection = store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )

    assert connection["id"] == "conn_1"
    assert store.get_connection("conn_1")["database"] == "postgres"


def test_postgres_url_uses_shared_config_credentials(monkeypatch):
    for username_key, password_key in db_module.SHARED_PG_CREDENTIAL_ENV_PAIRS:
        monkeypatch.delenv(username_key, raising=False)
        monkeypatch.delenv(password_key, raising=False)
    monkeypatch.setenv(
        "PGSYNC_STUDIO_CONFIG_DATABASE_URL",
        "postgresql://shared_user:shared%40pass@config-db:5432/studio",
    )

    url = build_postgres_url(
        {
            "id": "conn_1",
            "host": "business-db",
            "port": 5432,
            "database": "business",
            "username": "stored_user",
            "password": "stored_password",
        }
    )

    assert url == (
        "postgresql+psycopg2://"
        "shared_user:shared%40pass@business-db:5432/business"
    )


def test_config_database_url_injects_and_encodes_secret_credentials(
    monkeypatch,
):
    monkeypatch.setenv(
        "PGSYNC_STUDIO_CONFIG_DATABASE_URL",
        "postgresql+psycopg2://config-db:5432/studio",
    )
    monkeypatch.setenv("PGSYNC_STUDIO_CONFIG_DATABASE_USERNAME", "studio_user")
    monkeypatch.setenv("PGSYNC_STUDIO_CONFIG_DATABASE_PASSWORD", "p@ss/#word")

    assert config_database_url_from_env() == (
        "postgresql+psycopg2://studio_user:p%40ss%2F%23word@config-db:5432/studio"
    )


def test_config_database_url_keeps_test_env_compatibility(monkeypatch):
    monkeypatch.delenv("PGSYNC_STUDIO_CONFIG_DATABASE_URL", raising=False)
    monkeypatch.delenv("PGSYNC_STUDIO_CONFIG_DATABASE_USERNAME", raising=False)
    monkeypatch.delenv("PGSYNC_STUDIO_CONFIG_DATABASE_PASSWORD", raising=False)
    monkeypatch.setenv(
        "PGSYNC_STUDIO_CONFIG_DATABASE_URL_SECRET",
        "postgresql+psycopg2://test_user:test_pass@test-db:5432/studio",
    )

    assert config_database_url_from_env() == (
        "postgresql+psycopg2://test_user:test_pass@test-db:5432/studio"
    )


def test_sync_manager_stores_masked_connection_credentials(
    tmp_path: Path, monkeypatch
):
    for username_key, password_key in db_module.SHARED_PG_CREDENTIAL_ENV_PAIRS:
        monkeypatch.delenv(username_key, raising=False)
        monkeypatch.delenv(password_key, raising=False)
    monkeypatch.setenv(
        "PGSYNC_STUDIO_CONFIG_DATABASE_URL",
        "postgresql://shared_user:shared%40pass@config-db:5432/studio",
    )

    class FakeDatabaseManager:
        tested_connection = None

        def test_connection(self, connection):
            self.tested_connection = dict(connection)
            return {"ok": True}

    store = ConfigStore(str(tmp_path / "config.json"))
    db = FakeDatabaseManager()
    manager = SyncManager(store=store, db=db, repo_root=str(tmp_path))

    saved = manager.save_connection(
        {
            "name": "business",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "business",
        }
    )

    assert saved["username"] == MASKED_SHARED_CREDENTIAL
    assert saved["password"] == ""
    assert store.list_connections()[0]["username"] == MASKED_SHARED_CREDENTIAL
    assert store.list_connections()[0]["password"] == ""
    assert db.tested_connection["username"] == MASKED_SHARED_CREDENTIAL


def test_bootstrap_payload_masks_existing_connection_credentials(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "business",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "business",
            "username": "stored_user",
            "password": "stored_password",
            "url": "postgresql://stored_user:stored_password@127.0.0.1/business",
        }
    )
    manager = SyncManager(
        store=store, db=DatabaseManager(), repo_root=str(tmp_path)
    )

    connection = manager.bootstrap_payload()["connections"][0]

    assert connection["username"] == MASKED_SHARED_CREDENTIAL
    assert connection["password"] == ""
    assert connection["url"] == ""


def test_sync_manager_add_index_alias(tmp_path: Path, monkeypatch):
    calls = {}

    class FakeSearchAdapter:
        def __init__(self, services):
            calls["services"] = services

        def add_index_alias(self, index_name, alias_name):
            calls["index"] = index_name
            calls["alias"] = alias_name
            return {
                "index": index_name,
                "alias": alias_name,
                "aliases": [alias_name],
            }

    monkeypatch.setattr(
        "pgsync.web.sync_manager.SearchAdapter",
        FakeSearchAdapter,
    )
    store = ConfigStore(str(tmp_path / "config.json"))
    store.save_services(
        {
            "search": {"url": "http://es:9200"},
            "redis": {},
        }
    )
    manager = SyncManager(
        store=store, db=DatabaseManager(), repo_root=str(tmp_path)
    )

    result = manager.add_index_alias(
        {
            "index": "book_collections_v3",
            "alias": "book_collections",
        }
    )

    assert result["aliases"] == ["book_collections"]
    assert calls["index"] == "book_collections_v3"
    assert calls["alias"] == "book_collections"


def durable_cleanup_job(
    job_id: str,
    name: str,
    table_name: str,
    *,
    connection_id: str = "conn_1",
    schema: str = "public",
) -> dict:
    table_id = f"table_{job_id}"
    return {
        "id": job_id,
        "name": name,
        "index_name": name,
        "root_table_id": table_id,
        "tables": [
            {
                "id": table_id,
                "connection_id": connection_id,
                "schema": schema,
                "table": table_name,
                "primary_key": ["id"],
                "columns": ["id"],
                "children": [],
            }
        ],
        "relations": [],
        "enable_durable_change_events": True,
    }


def test_durable_outbox_cleanup_uses_min_cursor_watermark(tmp_path: Path):
    class FakeDatabaseManager:
        def __init__(self):
            self.calls = []

        def purge_change_events_cycle(self, connection, schema, **kwargs):
            self.calls.append(
                {
                    "connection": connection,
                    "schema": schema,
                    **kwargs,
                }
            )
            return {
                "locked": True,
                "deleted_rows": 7,
                "batches": 1,
                "elapsed_ms": 10,
                "rate_limited_ms": 0,
                "stop_reason": "exhausted",
            }

    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "business",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "business",
            "username": "postgres",
            "password": "",
            "url": "",
        }
    )
    store.upsert_job(durable_cleanup_job("job_a", "job_a", "table_a"))
    store.upsert_job(durable_cleanup_job("job_b", "job_b", "table_a"))
    store.update_job_runtime(
        "job_a",
        {"native_change_event_cursors": {"conn_1:public": 10_120}},
    )
    store.update_job_runtime(
        "job_b",
        {"native_change_event_cursors": {"conn_1:public": 10_080}},
    )
    db = FakeDatabaseManager()
    manager = SyncManager(store=store, db=db, repo_root=str(tmp_path))

    summary = manager.run_durable_outbox_cleanup_cycle(
        {
            "enabled": True,
            "retention_hours": 168,
            "batch_size": 5000,
            "max_batches_per_run": 3,
        }
    )

    assert summary["deleted_rows"] == 7
    assert len(db.calls) == 1
    assert db.calls[0]["safe_event_id"] == 5080
    assert db.calls[0]["table_name_filter"] == "table_a"
    assert db.calls[0]["schema"] == "public"
    assert db.calls[0]["batch_size"] == 5000
    assert summary["groups"][0]["safe_event_id"] == 5080
    assert summary["groups"][0]["batches"] == 1


def test_durable_outbox_cleanup_skips_group_with_missing_cursor(
    tmp_path: Path,
):
    class FakeDatabaseManager:
        def __init__(self):
            self.calls = []

        def purge_change_events_cycle(self, connection, schema, **kwargs):
            self.calls.append({"schema": schema, **kwargs})
            return {
                "locked": True,
                "deleted_rows": 1,
                "batches": 1,
                "elapsed_ms": 1,
                "rate_limited_ms": 0,
                "stop_reason": "exhausted",
            }

    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "business",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "business",
            "username": "postgres",
            "password": "",
            "url": "",
        }
    )
    store.upsert_job(durable_cleanup_job("job_a", "job_a", "table_a"))
    store.upsert_job(durable_cleanup_job("job_b", "job_b", "table_b"))
    store.update_job_runtime(
        "job_a",
        {"native_change_event_cursors": {"conn_1:public": 10_120}},
    )
    db = FakeDatabaseManager()
    manager = SyncManager(store=store, db=db, repo_root=str(tmp_path))

    summary = manager.run_durable_outbox_cleanup_cycle(
        {
            "enabled": True,
            "retention_hours": 168,
            "batch_size": 5000,
            "max_batches_per_run": 3,
        }
    )

    assert summary["deleted_rows"] == 1
    assert len(db.calls) == 1
    assert db.calls[0]["table_name_filter"] == "table_a"
    blocked_group = next(
        group
        for group in summary["groups"]
        if group["table_name"] == "table_b"
    )
    assert blocked_group["skipped"] is True
    assert blocked_group["skip_reason"] == "missing_cursor"
    assert blocked_group["blocked_jobs"] == ["job_b"]


def test_durable_outbox_cleanup_merges_duplicate_physical_connections(
    tmp_path: Path,
):
    class FakeDatabaseManager:
        def __init__(self):
            self.calls = []

        def purge_change_events_cycle(self, connection, schema, **kwargs):
            self.calls.append({"connection": connection, **kwargs})
            return {
                "locked": True,
                "deleted_rows": 0,
                "batches": 1,
                "elapsed_ms": 1,
                "rate_limited_ms": 0,
                "stop_reason": "exhausted",
            }

    store = ConfigStore(str(tmp_path / "config.json"))
    for connection_id in ("conn_a", "conn_b"):
        store.upsert_connection(
            {
                "id": connection_id,
                "name": connection_id,
                "type": "postgresql",
                "host": "DB.EXAMPLE.COM.",
                "port": 5432,
                "database": "business",
                "username": "postgres",
                "password": "",
                "url": "",
            }
        )
    store.upsert_job(
        durable_cleanup_job(
            "job_a", "job_a", "table_a", connection_id="conn_a"
        )
    )
    store.upsert_job(
        durable_cleanup_job(
            "job_b", "job_b", "table_a", connection_id="conn_b"
        )
    )
    store.update_job_runtime(
        "job_a",
        {"native_change_event_cursors": {"conn_a:public": 20_000}},
    )
    store.update_job_runtime(
        "job_b",
        {"native_change_event_cursors": {"conn_b:public": 15_000}},
    )
    db = FakeDatabaseManager()
    manager = SyncManager(store=store, db=db, repo_root=str(tmp_path))

    summary = manager.run_durable_outbox_cleanup_cycle(
        {"enabled": True, "retention_hours": 168}
    )

    assert len(summary["groups"]) == 1
    assert len(db.calls) == 1
    assert summary["groups"][0]["connection_ids"] == ["conn_a", "conn_b"]
    assert summary["groups"][0]["safe_event_id"] == 10_000
    assert physical_database_identity(db.calls[0]["connection"]) == (
        "db.example.com|5432|business"
    )


def test_durable_outbox_cleanup_blocks_shared_table_if_any_cursor_missing(
    tmp_path: Path,
):
    class FakeDatabaseManager:
        def __init__(self):
            self.calls = []

        def purge_change_events_cycle(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            return {"locked": True, "deleted_rows": 1}

    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "business",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "business",
            "username": "postgres",
            "password": "",
            "url": "",
        }
    )
    store.upsert_job(durable_cleanup_job("job_a", "job_a", "table_a"))
    store.upsert_job(durable_cleanup_job("job_b", "job_b", "table_a"))
    store.update_job_runtime(
        "job_a",
        {"native_change_event_cursors": {"conn_1:public": 20_000}},
    )
    db = FakeDatabaseManager()
    manager = SyncManager(store=store, db=db, repo_root=str(tmp_path))

    summary = manager.run_durable_outbox_cleanup_cycle(
        {"enabled": True, "retention_hours": 168}
    )

    assert db.calls == []
    assert len(summary["groups"]) == 1
    assert summary["groups"][0]["skip_reason"] == "missing_cursor"
    assert summary["groups"][0]["blocked_jobs"] == ["job_b"]


def test_durable_outbox_cleanup_config_defaults_to_disabled_and_fails_closed(
    tmp_path: Path,
    monkeypatch,
):
    for name in (
        "PGSYNC_STUDIO_DURABLE_OUTBOX_CLEANUP_ENABLED",
        "PGSYNC_STUDIO_DURABLE_OUTBOX_CLEANUP_INTERVAL_SECONDS",
    ):
        monkeypatch.delenv(name, raising=False)
    manager = SyncManager(
        store=ConfigStore(str(tmp_path / "config.json")),
        db=DatabaseManager(),
        repo_root=str(tmp_path),
    )
    monkeypatch.setattr(
        manager.runtime_tuning,
        "snapshot",
        lambda: {
            "payload": {},
            "last_error": "",
        },
    )
    assert manager.durable_outbox_cleanup_config()["enabled"] is False

    monkeypatch.setattr(
        manager.runtime_tuning,
        "snapshot",
        lambda: {
            "payload": {
                "pgsync": {"durable_outbox_cleanup": {"enabled": True}}
            },
            "last_error": "nacos unavailable",
        },
    )
    config = manager.durable_outbox_cleanup_config()
    assert config["enabled"] is False
    assert config["enabled_source"] == "nacos_error_fail_closed"


def test_durable_outbox_cleanup_environment_false_is_kill_switch(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setenv("PGSYNC_STUDIO_DURABLE_OUTBOX_CLEANUP_ENABLED", "false")
    manager = SyncManager(
        store=ConfigStore(str(tmp_path / "config.json")),
        db=DatabaseManager(),
        repo_root=str(tmp_path),
    )
    monkeypatch.setattr(
        manager.runtime_tuning,
        "snapshot",
        lambda: {
            "payload": {
                "pgsync": {"durable_outbox_cleanup": {"enabled": True}}
            },
            "last_error": "",
        },
    )

    config = manager.durable_outbox_cleanup_config()

    assert config["enabled"] is False
    assert config["enabled_source"] == "environment_kill_switch"


def test_capture_reconciliation_defaults_disabled_and_fails_closed(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.delenv(
        "PGSYNC_STUDIO_DURABLE_OUTBOX_CAPTURE_RECONCILIATION_ENABLED",
        raising=False,
    )
    manager = SyncManager(
        store=ConfigStore(str(tmp_path / "config.json")),
        db=DatabaseManager(),
        repo_root=str(tmp_path),
    )
    monkeypatch.setattr(
        manager.runtime_tuning,
        "snapshot",
        lambda: {"payload": {}, "last_error": ""},
    )

    config = manager.durable_outbox_capture_reconciliation_config()

    assert config["enabled"] is False
    assert config["apply_changes"] is False

    monkeypatch.setattr(
        manager.runtime_tuning,
        "snapshot",
        lambda: {
            "payload": {
                "pgsync": {
                    "durable_outbox_capture_reconciliation": {
                        "enabled": True,
                        "apply_changes": True,
                    }
                }
            },
            "last_error": "nacos unavailable",
        },
    )
    config = manager.durable_outbox_capture_reconciliation_config()
    assert config["enabled"] is False
    assert config["enabled_source"] == "nacos_error_fail_closed"


def test_capture_reconciliation_merges_jobs_and_keeps_stopped_references(
    tmp_path: Path,
):
    class FakeDatabaseManager:
        def __init__(self):
            self.calls = []

        def list_change_capture_schemas(self, connection):
            return ["public"]

        def reconcile_change_capture(self, connection, schema, **kwargs):
            self.calls.append(
                {"connection": connection, "schema": schema, **kwargs}
            )
            return {
                "ok": True,
                "safe_for_orphan_cleanup": True,
                "pending_changes": False,
            }

    store = ConfigStore(str(tmp_path / "config.json"))
    for connection_id in ("conn_a", "conn_b"):
        store.upsert_connection(
            {
                "id": connection_id,
                "name": connection_id,
                "type": "postgresql",
                "host": "DB.EXAMPLE.COM.",
                "port": 5432,
                "database": "business",
                "username": "postgres",
                "password": "",
                "url": "",
            }
        )
    store.upsert_job(
        durable_cleanup_job(
            "job_a", "index_a", "shared_table", connection_id="conn_a"
        )
    )
    store.upsert_job(
        durable_cleanup_job(
            "job_b", "index_b", "shared_table", connection_id="conn_b"
        )
    )
    store.update_job_runtime(
        "job_b", {"status": "stopped", "desired_state": "stopped"}
    )
    db = FakeDatabaseManager()
    manager = SyncManager(store=store, db=db, repo_root=str(tmp_path))

    summary = manager.run_durable_outbox_capture_reconciliation_cycle(
        {
            "enabled": True,
            "apply_changes": True,
            "lock_timeout_ms": 1000,
        }
    )

    assert summary["ok"] is True
    assert len(db.calls) == 1
    assert db.calls[0]["referenced_table_names"] == ["shared_table"]
    assert db.calls[0]["desired_indices_by_table"] == {
        "shared_table": ["index_a", "index_b"]
    }
    assert summary["groups"][0]["connection_ids"] == ["conn_a", "conn_b"]


def test_capture_reconciliation_discovers_schema_after_last_job_deleted(
    tmp_path: Path,
):
    class FakeDatabaseManager:
        def __init__(self):
            self.calls = []

        def list_change_capture_schemas(self, connection):
            return ["public"]

        def reconcile_change_capture(self, connection, schema, **kwargs):
            self.calls.append({"schema": schema, **kwargs})
            return {
                "ok": True,
                "safe_for_orphan_cleanup": True,
                "pending_changes": False,
            }

    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "business",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "business",
            "username": "postgres",
            "password": "",
            "url": "",
        }
    )
    db = FakeDatabaseManager()
    manager = SyncManager(store=store, db=db, repo_root=str(tmp_path))

    summary = manager.run_durable_outbox_capture_reconciliation_cycle(
        {"enabled": True, "apply_changes": True, "lock_timeout_ms": 1000}
    )

    assert summary["ok"] is True
    assert len(db.calls) == 1
    assert db.calls[0]["schema"] == "public"
    assert db.calls[0]["referenced_table_names"] == []
    assert db.calls[0]["desired_indices_by_table"] == {}


def test_durable_outbox_partition_config_is_disabled_and_fail_closed(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.delenv(
        "PGSYNC_STUDIO_DURABLE_OUTBOX_PARTITION_MAINTENANCE_ENABLED",
        raising=False,
    )
    manager = SyncManager(
        store=ConfigStore(str(tmp_path / "config.json")),
        db=DatabaseManager(),
        repo_root=str(tmp_path),
    )
    monkeypatch.setattr(
        manager.runtime_tuning,
        "snapshot",
        lambda: {"payload": {}, "last_error": ""},
    )
    config = manager.durable_outbox_partition_maintenance_config()
    assert config["enabled"] is False
    assert config["retention_days"] == 7
    assert config["future_days"] == 7
    assert config["allow_orphan_tables"] is False
    assert config["run_times_utc"] == ["00:15", "12:15"]

    monkeypatch.setattr(
        manager.runtime_tuning,
        "snapshot",
        lambda: {
            "payload": {
                "pgsync": {
                    "durable_outbox_partition_maintenance": {
                        "enabled": True,
                        "run_times_utc": ["01:30", "13:45"],
                    }
                }
            },
            "last_error": "nacos unavailable",
        },
    )
    config = manager.durable_outbox_partition_maintenance_config()
    assert config["enabled"] is False
    assert config["enabled_source"] == "nacos_error_fail_closed"


def test_partition_orphan_cleanup_requires_safe_capture_reconciliation(
    tmp_path: Path,
    monkeypatch,
):
    class FakeDatabaseManager:
        def __init__(self):
            self.maintenance_calls = []

        def list_change_capture_schemas(self, connection):
            return ["public"]

        def reconcile_change_capture(self, connection, schema, **kwargs):
            return {
                "ok": True,
                "referenced_table_names": ["table_a"],
                "safe_for_orphan_cleanup": True,
                "pending_changes": False,
            }

        def maintain_change_event_partitions(
            self, connection, schema, **kwargs
        ):
            self.maintenance_calls.append(kwargs)
            return {
                "mode": "partitioned",
                "locked": True,
                "ensured_partitions": [],
                "dropped_partitions": [],
                "blocked_partitions": [],
                "orphan_tables": [],
                "stop_reason": "completed",
            }

    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "business",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "business",
            "username": "postgres",
            "password": "",
            "url": "",
        }
    )
    store.upsert_job(durable_cleanup_job("job_a", "job_a", "table_a"))
    store.update_job_runtime(
        "job_a",
        {"native_change_event_cursors": {"conn_1:public": 10_120}},
    )
    db = FakeDatabaseManager()
    manager = SyncManager(store=store, db=db, repo_root=str(tmp_path))
    monkeypatch.setattr(
        manager.runtime_tuning,
        "snapshot",
        lambda: {
            "payload": {
                "pgsync": {
                    "durable_outbox_capture_reconciliation": {
                        "enabled": True,
                        "apply_changes": True,
                    }
                }
            },
            "last_error": "",
        },
    )

    summary = manager.run_durable_outbox_partition_maintenance_cycle(
        {
            "enabled": True,
            "retention_days": 7,
            "future_days": 7,
            "allow_orphan_tables": True,
        }
    )

    assert summary["ok"] is True
    assert db.maintenance_calls[0]["allow_orphan_tables"] is True
    assert db.maintenance_calls[0]["referenced_table_names"] == ["table_a"]


def test_durable_outbox_partition_uses_per_table_safe_watermarks(
    tmp_path: Path,
):
    class FakeDatabaseManager:
        def __init__(self):
            self.calls = []

        def maintain_change_event_partitions(
            self, connection, schema, **kwargs
        ):
            self.calls.append({"schema": schema, **kwargs})
            return {
                "mode": "partitioned",
                "locked": True,
                "ensured_partitions": ["_pgsync_change_event_p20260815"],
                "dropped_partitions": [],
                "blocked_partitions": [],
                "stop_reason": "completed",
            }

    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "business",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "business",
            "username": "postgres",
            "password": "",
            "url": "",
        }
    )
    store.upsert_job(durable_cleanup_job("job_a", "job_a", "table_a"))
    store.upsert_job(durable_cleanup_job("job_b", "job_b", "table_b"))
    store.update_job_runtime(
        "job_a",
        {"native_change_event_cursors": {"conn_1:public": 10_120}},
    )
    store.update_job_runtime(
        "job_b",
        {"native_change_event_cursors": {"conn_1:public": 20_080}},
    )
    db = FakeDatabaseManager()
    manager = SyncManager(store=store, db=db, repo_root=str(tmp_path))

    summary = manager.run_durable_outbox_partition_maintenance_cycle(
        {
            "enabled": True,
            "retention_days": 7,
            "future_days": 7,
            "lock_timeout_ms": 1000,
        },
        now=datetime(2026, 8, 15, 1, tzinfo=timezone.utc),
    )

    assert summary["ok"] is True
    assert len(db.calls) == 1
    assert db.calls[0]["safe_event_ids_by_table"] == {
        "table_a": 5120,
        "table_b": 15080,
    }
    assert db.calls[0]["blocked_table_names"] == []


def test_durable_outbox_partition_blocks_only_table_with_missing_cursor(
    tmp_path: Path,
):
    class FakeDatabaseManager:
        def __init__(self):
            self.calls = []

        def maintain_change_event_partitions(
            self, connection, schema, **kwargs
        ):
            self.calls.append(kwargs)
            return {
                "mode": "partitioned",
                "locked": True,
                "ensured_partitions": [],
                "dropped_partitions": [],
                "blocked_partitions": [],
                "stop_reason": "completed",
            }

    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "business",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "business",
            "username": "postgres",
            "password": "",
            "url": "",
        }
    )
    store.upsert_job(durable_cleanup_job("job_a", "job_a", "table_a"))
    store.upsert_job(durable_cleanup_job("job_b", "job_b", "table_b"))
    store.update_job_runtime(
        "job_a",
        {"native_change_event_cursors": {"conn_1:public": 10_120}},
    )
    db = FakeDatabaseManager()
    manager = SyncManager(store=store, db=db, repo_root=str(tmp_path))

    manager.run_durable_outbox_partition_maintenance_cycle(
        {"enabled": True, "retention_days": 7, "future_days": 7}
    )

    assert db.calls[0]["safe_event_ids_by_table"] == {"table_a": 5120}
    assert db.calls[0]["blocked_table_names"] == ["table_b"]


def test_next_partition_maintenance_run_is_twice_daily_utc():
    next_run = SyncManager._next_partition_maintenance_run(
        ["00:15", "12:15"],
        datetime(2026, 8, 15, 1, 0, tzinfo=timezone.utc),
    )
    assert next_run.isoformat() == "2026-08-15T12:15:00+00:00"

    next_run = SyncManager._next_partition_maintenance_run(
        ["00:15", "12:15"],
        datetime(2026, 8, 15, 13, 0, tzinfo=timezone.utc),
    )
    assert next_run.isoformat() == "2026-08-16T00:15:00+00:00"


def test_ensure_change_event_outbox_creates_daily_partitions(monkeypatch):
    class FakeResult:
        def __init__(self, value=None):
            self.value = value

        def scalar_one_or_none(self):
            return self.value

    class FakeConnection:
        def __init__(self):
            self.statements = []
            self.relation_reads = 0

        def execute(self, statement, params=None):
            sql = str(statement)
            self.statements.append(sql)
            if "SELECT c.relkind" in sql:
                self.relation_reads += 1
                return FakeResult(None if self.relation_reads == 1 else "p")
            return FakeResult()

        def commit(self):
            return None

    db = DatabaseManager()
    conn = FakeConnection()
    monkeypatch.setattr(
        db,
        "_run_with_retry",
        lambda _connection, operation, **_kwargs: operation(conn),
    )

    result = db.ensure_change_event_outbox(
        {"id": "conn_1"},
        "public",
        future_days=7,
        now=datetime(2026, 8, 15, 8, tzinfo=timezone.utc),
    )

    ddl = "\n".join(conn.statements)
    assert result["mode"] == "partitioned"
    assert len(result["ensured_partitions"]) == 8
    assert "PARTITION BY RANGE (created_at)" in ddl
    assert "PRIMARY KEY (created_at, id)" in ddl
    assert "_pgsync_change_event_p20260815" in ddl
    assert "_pgsync_change_event_p20260822" in ddl


def test_ensure_change_event_outbox_keeps_existing_regular_table(monkeypatch):
    class FakeResult:
        def scalar_one_or_none(self):
            return "r"

    class FakeConnection:
        def __init__(self):
            self.statements = []

        def execute(self, statement, params=None):
            self.statements.append(str(statement))
            return FakeResult()

        def commit(self):
            return None

    db = DatabaseManager()
    conn = FakeConnection()
    monkeypatch.setattr(
        db,
        "_run_with_retry",
        lambda _connection, operation, **_kwargs: operation(conn),
    )

    result = db.ensure_change_event_outbox(
        {"id": "conn_1"}, "public", future_days=7
    )

    ddl = "\n".join(conn.statements)
    assert result["mode"] == "regular"
    assert "PARTITION BY RANGE" not in ddl
    assert "_pgsync_change_event_table_id_idx" in ddl


def test_reconcile_change_capture_removes_only_unreferenced_managed_artifacts(
    monkeypatch,
):
    class FakeResult:
        def __init__(self, *, scalar=None, rows=None):
            self.scalar = scalar
            self.rows = rows or []

        def scalar_one_or_none(self):
            return self.scalar

        def mappings(self):
            return self.rows

    class FakeConnection:
        def __init__(self):
            self.statements = []

        def execute(self, statement, params=None):
            sql = str(statement)
            self.statements.append((type(statement).__name__, sql, params))
            if "SELECT c.relkind FROM pg_class" in sql:
                return FakeResult(scalar="m")
            if "SELECT * FROM" in sql:
                return FakeResult(
                    rows=[
                        {
                            "table_name": "active_table",
                            "primary_keys": ["id"],
                            "foreign_keys": [],
                            "indices": ["old_index"],
                            "columns": ["id"],
                            "payload_columns": [],
                        },
                        {
                            "table_name": "orphan_table",
                            "primary_keys": ["id"],
                            "foreign_keys": [],
                            "indices": ["deleted_index"],
                            "columns": ["id"],
                            "payload_columns": [],
                        },
                    ]
                )
            if "FROM pg_trigger t" in sql:
                return FakeResult(
                    rows=[
                        {
                            "trigger_name": "public_active_table_notify",
                            "table_name": "active_table",
                            "function_schema": "public",
                            "function_name": "_pgsync_table_notify",
                        },
                        {
                            "trigger_name": "public_orphan_table_notify",
                            "table_name": "orphan_table",
                            "function_schema": "public",
                            "function_name": "table_notify",
                        },
                        {
                            "trigger_name": "public_orphan_table_truncate",
                            "table_name": "orphan_table",
                            "function_schema": "public",
                            "function_name": "table_notify",
                        },
                    ]
                )
            return FakeResult()

    class FakeEngine:
        def __init__(self, connection):
            self.connection = connection

        @contextmanager
        def begin(self):
            yield self.connection

    connection = FakeConnection()
    db = DatabaseManager()
    monkeypatch.setattr(
        db,
        "get_engine",
        lambda *_args, **_kwargs: FakeEngine(connection),
    )

    result = db.reconcile_change_capture(
        {"id": "conn_1"},
        "public",
        desired_indices_by_table={"active_table": ["current_index"]},
        referenced_table_names=["active_table"],
        apply_changes=True,
        lock_timeout_ms=1000,
    )

    assert result["stale_view_tables"] == ["orphan_table"]
    assert result["orphan_capture_tables"] == ["orphan_table"]
    assert result["changed_view_indices"] == [
        {
            "table_name": "active_table",
            "current": ["old_index"],
            "desired": ["current_index"],
        }
    ]
    assert [
        trigger["trigger_name"] for trigger in result["dropped_triggers"]
    ] == [
        "public_orphan_table_notify",
        "public_orphan_table_truncate",
    ]
    assert result["applied"] is True
    assert result["safe_for_orphan_cleanup"] is True
    drop_sql = "\n".join(sql for _, sql, _ in connection.statements)
    assert 'DROP TRIGGER IF EXISTS "public_orphan_table_notify"' in drop_sql
    assert (
        'DROP TRIGGER IF EXISTS "public_active_table_notify"' not in drop_sql
    )


def test_reconcile_change_capture_fails_closed_on_unmanaged_trigger(
    monkeypatch,
):
    class FakeResult:
        def __init__(self, *, scalar=None, rows=None):
            self.scalar = scalar
            self.rows = rows or []

        def scalar_one_or_none(self):
            return self.scalar

        def mappings(self):
            return self.rows

    class FakeConnection:
        def __init__(self):
            self.statements = []

        def execute(self, statement, params=None):
            sql = str(statement)
            self.statements.append(sql)
            if "SELECT c.relkind FROM pg_class" in sql:
                return FakeResult(scalar="m")
            if "SELECT * FROM" in sql:
                return FakeResult(
                    rows=[
                        {
                            "table_name": "orphan_table",
                            "primary_keys": ["id"],
                            "foreign_keys": [],
                            "indices": ["deleted_index"],
                            "columns": ["id"],
                            "payload_columns": [],
                        }
                    ]
                )
            if "FROM pg_trigger t" in sql:
                return FakeResult(
                    rows=[
                        {
                            "trigger_name": "public_orphan_table_notify",
                            "table_name": "orphan_table",
                            "function_schema": "public",
                            "function_name": "business_notify",
                        }
                    ]
                )
            return FakeResult()

    class FakeEngine:
        @contextmanager
        def begin(self):
            yield connection

    connection = FakeConnection()
    db = DatabaseManager()
    monkeypatch.setattr(
        db, "get_engine", lambda *_args, **_kwargs: FakeEngine()
    )
    monkeypatch.setattr(
        db_module,
        "replace_view_rows",
        lambda *_args, **_kwargs: pytest.fail(
            "unmanaged conflicts must block view replacement"
        ),
    )

    result = db.reconcile_change_capture(
        {"id": "conn_1"},
        "public",
        desired_indices_by_table={},
        referenced_table_names=[],
        apply_changes=True,
        lock_timeout_ms=1000,
    )

    assert result["apply_blocked_reason"] == "unmanaged_trigger_conflict"
    assert result["applied"] is False
    assert result["safe_for_orphan_cleanup"] is False
    assert not any("DROP TRIGGER" in sql for sql in connection.statements)


def test_reconcile_change_capture_aborts_when_job_ownership_changes(
    monkeypatch,
):
    class FakeResult:
        pass

    class FakeConnection:
        def __init__(self):
            self.statements = []

        def execute(self, statement, params=None):
            sql = str(statement)
            self.statements.append(sql)
            if "SELECT c.relkind" in sql:
                pytest.fail(
                    "ownership must be rechecked before metadata reads"
                )
            return FakeResult()

    class FakeEngine:
        @contextmanager
        def begin(self):
            yield connection

    connection = FakeConnection()
    db = DatabaseManager()
    monkeypatch.setattr(
        db, "get_engine", lambda *_args, **_kwargs: FakeEngine()
    )

    result = db.reconcile_change_capture(
        {"id": "conn_1"},
        "public",
        desired_indices_by_table={},
        referenced_table_names=[],
        apply_changes=True,
        lock_timeout_ms=1000,
        ownership_guard=lambda: False,
    )

    assert result["ok"] is False
    assert result["ownership_changed"] is True
    assert result["applied"] is False
    assert result["safe_for_orphan_cleanup"] is False


def test_partition_maintenance_ignores_verified_orphan_table(monkeypatch):
    class FakeResult:
        def __init__(self, *, scalar=None, rows=None):
            self.scalar = scalar
            self.rows = rows or []

        def scalar_one(self):
            return self.scalar

        def mappings(self):
            return self.rows

    class FakeConnection:
        def __init__(self):
            self.statements = []
            self.closed = False

        def execution_options(self, **kwargs):
            return self

        def execute(self, statement, params=None):
            sql = str(statement)
            self.statements.append(sql)
            if "PG_TRY_ADVISORY_LOCK" in sql:
                return FakeResult(scalar=True)
            if "PG_ADVISORY_UNLOCK" in sql:
                return FakeResult(scalar=True)
            if "FROM pg_class child" in sql:
                return FakeResult(
                    rows=[
                        {
                            "partition_name": (
                                "_pgsync_change_event_p20260801"
                            ),
                            "attached": False,
                            "detach_pending": False,
                        }
                    ]
                )
            if "GROUP BY table_name" in sql:
                return FakeResult(
                    rows=[
                        {"table_name": "active_table", "max_event_id": 100},
                        {"table_name": "orphan_table", "max_event_id": 200},
                    ]
                )
            return FakeResult()

        def close(self):
            self.closed = True

    class FakeEngine:
        def __init__(self, connection):
            self.connection = connection

        def connect(self):
            return self.connection

    connection = FakeConnection()
    db = DatabaseManager()
    monkeypatch.setattr(
        db,
        "ensure_change_event_outbox",
        lambda *_args, **_kwargs: {
            "mode": "partitioned",
            "ensured_partitions": [],
        },
    )
    monkeypatch.setattr(
        db,
        "get_engine",
        lambda *_args, **_kwargs: FakeEngine(connection),
    )

    result = db.maintain_change_event_partitions(
        {"id": "conn_1"},
        "public",
        retention_days=7,
        future_days=7,
        safe_event_ids_by_table={"active_table": 150},
        blocked_table_names=[],
        lock_timeout_ms=1000,
        lock_key="partition:test",
        allow_orphan_tables=True,
        referenced_table_names=["active_table"],
        now=datetime(2026, 8, 15, 1, tzinfo=timezone.utc),
    )

    assert result["dropped_partitions"] == ["_pgsync_change_event_p20260801"]
    assert result["blocked_partitions"] == []
    assert result["orphan_tables"] == [
        {
            "partition": "_pgsync_change_event_p20260801",
            "table_name": "orphan_table",
            "max_event_id": 200,
        }
    ]
    assert connection.closed is True


def test_row_cleanup_skips_partitioned_outbox(tmp_path: Path):
    class FakeDatabaseManager:
        def __init__(self):
            self.purge_called = False

        def change_event_outbox_kind(self, connection, schema):
            return "p"

        def purge_change_events_cycle(self, *args, **kwargs):
            self.purge_called = True
            raise AssertionError("row cleanup must not run on partitions")

    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "business",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "business",
            "username": "postgres",
            "password": "",
            "url": "",
        }
    )
    store.upsert_job(durable_cleanup_job("job_a", "job_a", "table_a"))
    store.update_job_runtime(
        "job_a",
        {"native_change_event_cursors": {"conn_1:public": 10_120}},
    )
    db = FakeDatabaseManager()
    manager = SyncManager(store=store, db=db, repo_root=str(tmp_path))

    summary = manager.run_durable_outbox_cleanup_cycle(
        {"enabled": True, "retention_hours": 168}
    )

    assert db.purge_called is False
    assert summary["groups"][0]["skipped"] is True
    assert summary["groups"][0]["skip_reason"] == "partitioned_table"


def test_purge_change_events_cycle_enforces_row_budget(monkeypatch):
    class FakeResult:
        def __init__(self, value):
            self.value = value

        def scalar_one(self):
            return self.value

    class FakeConnection:
        def __init__(self):
            self.delete_limits = []
            self.delete_results = iter((1000, 500))
            self.statements = []
            self.commits = 0
            self.rollbacks = 0
            self.closed = False

        def execute(self, statement, params=None):
            sql = str(statement)
            self.statements.append(sql)
            if "PG_TRY_ADVISORY_LOCK" in sql:
                return FakeResult(True)
            if "PG_ADVISORY_UNLOCK" in sql:
                return FakeResult(True)
            if "set_config" in sql:
                return FakeResult(None)
            self.delete_limits.append(params["limit"])
            return FakeResult(next(self.delete_results))

        def commit(self):
            self.commits += 1

        def rollback(self):
            self.rollbacks += 1

        def close(self):
            self.closed = True

        def invalidate(self):
            raise AssertionError("connection should not be invalidated")

    class FakeEngine:
        def __init__(self, connection):
            self.connection = connection

        def connect(self):
            return self.connection

    connection = {
        "id": "conn_1",
        "host": "localhost",
        "port": 5432,
        "database": "business",
        "username": "postgres",
        "password": "",
    }
    fake_connection = FakeConnection()
    manager = DatabaseManager()
    monkeypatch.setattr(
        manager,
        "get_engine",
        lambda *_args, **_kwargs: FakeEngine(fake_connection),
    )

    result = manager.purge_change_events_cycle(
        connection,
        "public",
        table_name_filter="book_metrics",
        safe_event_id=50_000,
        cutoff=datetime.utcnow(),
        batch_size=1000,
        max_batches=10,
        max_rows=1500,
        max_run_seconds=30,
        max_rows_per_second=100_000,
        batch_pause_ms=0,
        statement_timeout_ms=5000,
        lock_timeout_ms=1000,
        lock_key="cleanup:test",
    )

    assert result["deleted_rows"] == 1500
    assert result["batches"] == 2
    assert result["stop_reason"] == "row_budget"
    assert fake_connection.delete_limits == [1000, 500]
    assert fake_connection.closed is True
    lock_sql = "\n".join(fake_connection.statements)
    assert "CAST(:lock_key AS text)" in lock_sql
    assert "pgsync-cleanup-v2:" in lock_sql
    assert "HASHTEXTENDED" not in lock_sql


def test_purge_change_events_cycle_releases_lock_after_delete_error(
    monkeypatch,
):
    class FakeResult:
        def __init__(self, value):
            self.value = value

        def scalar_one(self):
            return self.value

    class FakeConnection:
        def __init__(self):
            self.unlock_called = False
            self.closed = False
            self.invalidated = False

        def execute(self, statement, params=None):
            sql = str(statement)
            if "PG_TRY_ADVISORY_LOCK" in sql:
                return FakeResult(True)
            if "PG_ADVISORY_UNLOCK" in sql:
                self.unlock_called = True
                return FakeResult(True)
            if "set_config" in sql:
                return FakeResult(None)
            raise RuntimeError("delete failed")

        def commit(self):
            return None

        def rollback(self):
            return None

        def close(self):
            self.closed = True

        def invalidate(self):
            self.invalidated = True

    class FakeEngine:
        def __init__(self, connection):
            self.connection = connection

        def connect(self):
            return self.connection

    fake_connection = FakeConnection()
    manager = DatabaseManager()
    monkeypatch.setattr(
        manager,
        "get_engine",
        lambda *_args, **_kwargs: FakeEngine(fake_connection),
    )

    with pytest.raises(RuntimeError, match="delete failed"):
        manager.purge_change_events_cycle(
            {
                "id": "conn_1",
                "host": "localhost",
                "port": 5432,
                "database": "business",
                "username": "postgres",
                "password": "",
            },
            "public",
            table_name_filter="book_metrics",
            safe_event_id=50_000,
            cutoff=datetime.utcnow(),
            batch_size=1000,
            max_batches=10,
            max_rows=9000,
            max_run_seconds=30,
            max_rows_per_second=1000,
            batch_pause_ms=0,
            statement_timeout_ms=5000,
            lock_timeout_ms=1000,
            lock_key="cleanup:test-error",
        )

    assert fake_connection.unlock_called is True
    assert fake_connection.invalidated is False
    assert fake_connection.closed is True


def test_config_store_normalizes_empty_payload(tmp_path: Path):
    path = tmp_path / "config.json"
    path.write_text("{}", encoding="utf-8")

    store = ConfigStore(str(path))
    snapshot = store.snapshot()

    assert snapshot["jobs"] == []
    assert snapshot["connections"] == []
    assert snapshot["job_runtime"] == {}


def test_config_store_falls_back_when_replace_permission_denied(
    tmp_path: Path,
    monkeypatch,
):
    replace_calls = {"count": 0}

    def broken_replace(src, dst):
        replace_calls["count"] += 1
        raise PermissionError(13, "Permission denied", dst)

    monkeypatch.setattr(config_store_module.os, "replace", broken_replace)
    monkeypatch.setattr(config_store_module.time, "sleep", lambda _: None)

    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )

    payload = json.loads(
        (tmp_path / "config.json").read_text(encoding="utf-8")
    )
    assert replace_calls["count"] >= 2
    assert payload["connections"][0]["id"] == "conn_1"


def test_config_store_stores_runtime_logs_in_separate_files(tmp_path: Path):
    store = ConfigStore(str(tmp_path / "config.json"))

    store.update_job_runtime("job_1", {"status": "running"})
    store.append_job_log("job_1", "[t1] sync started", limit=80)
    store.append_job_log("job_1", "[t2] sync finished", limit=80)

    config_payload = json.loads(
        (tmp_path / "config.json").read_text(encoding="utf-8")
    )
    runtime_payload = config_payload["job_runtime"]["job_1"]

    assert "logs" not in runtime_payload
    assert store.get_job_runtime("job_1")["logs"] == [
        "[t1] sync started",
        "[t2] sync finished",
    ]
    log_files = list(
        (tmp_path / ConfigStore.JOB_LOG_DIRNAME).glob("job_1.json")
    )
    assert len(log_files) == 1


def test_config_store_migrates_inline_runtime_logs(tmp_path: Path):
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:00+00:00",
                "services": default_config()["services"],
                "connections": [],
                "jobs": [],
                "job_runtime": {
                    "job_1": {
                        "status": "failed",
                        "logs": ["line 1", "line 2"],
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    store = ConfigStore(str(path))
    runtime = store.get_job_runtime("job_1")
    config_payload = json.loads(path.read_text(encoding="utf-8"))

    assert runtime["logs"] == ["line 1", "line 2"]
    assert "logs" not in config_payload["job_runtime"]["job_1"]


def test_sync_manager_append_log_ignores_runtime_persist_error(
    tmp_path: Path,
    monkeypatch,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))

    def raise_permission_error(job_id, line, limit=None):
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr(store, "append_job_log", raise_permission_error)

    manager.append_log("job_1", "hello")


def test_format_order_by_supports_desc():
    assert (
        format_order_by(["id", "-update_time"])
        == '"id" ASC, "update_time" DESC'
    )


def test_database_manager_fetch_page_uses_seek_after(monkeypatch):
    db = DatabaseManager()
    captured: dict = {}

    class DummyResult:
        @staticmethod
        def mappings():
            return []

    class DummyConnection:
        @staticmethod
        def execute(statement, params):
            captured["sql"] = statement.text
            captured["params"] = params
            return DummyResult()

    @contextmanager
    def fake_connect(connection):
        yield DummyConnection()

    monkeypatch.setattr(db, "connect", fake_connect)

    rows = db.fetch_page(
        {
            "id": "conn_1",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
        },
        "public",
        "book",
        order_by=["id", "tenant_id"],
        limit=200,
        seek_after=[10, 20],
    )

    assert rows == []
    assert captured["sql"] == (
        'SELECT * FROM "public"."book" '
        'WHERE ROW("id", "tenant_id") > ROW(:seek_0, :seek_1) '
        'ORDER BY "id" ASC, "tenant_id" ASC LIMIT :limit'
    )
    assert captured["params"] == {
        "limit": 200,
        "seek_0": 10,
        "seek_1": 20,
    }


def test_build_seek_pagination_where_falls_back_for_mixed_order():
    clause, params = build_seek_pagination_where(
        ["id", "-updated_at"],
        [10, 20],
    )

    assert clause == (
        '("id" > :seek_0 OR ("id" = :seek_0 AND "updated_at" < :seek_1))'
    )
    assert params == {
        "seek_0": 10,
        "seek_1": 20,
    }


def test_database_manager_fetch_changed_rows_uses_row_comparison(monkeypatch):
    db = DatabaseManager()
    captured: dict = {}

    class DummyResult:
        @staticmethod
        def mappings():
            return []

    class DummyConnection:
        @staticmethod
        def execute(statement, params):
            captured["sql"] = statement.text
            captured["params"] = params
            return DummyResult()

    @contextmanager
    def fake_connect(connection):
        yield DummyConnection()

    monkeypatch.setattr(db, "connect", fake_connect)

    rows, next_cursor = db.fetch_changed_rows(
        {
            "id": "conn_1",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
        },
        "public",
        "book",
        cursor_column="updated_at",
        last_cursor=100,
        last_primary_key=[10, 20],
        primary_key=["id", "tenant_id"],
        limit=200,
    )

    assert rows == []
    assert next_cursor is None
    assert captured["sql"] == (
        'SELECT * FROM "public"."book"'
        ' WHERE ROW("updated_at", "id", "tenant_id") > ROW(:last_cursor, :last_pk_0, :last_pk_1)'
        ' ORDER BY "updated_at", "id", "tenant_id" LIMIT :limit'
    )
    assert captured["params"] == {
        "limit": 200,
        "last_cursor": 100,
        "last_pk_0": 10,
        "last_pk_1": 20,
    }


def test_database_manager_fetch_rows_retries_transient_operational_error(
    monkeypatch,
):
    db = DatabaseManager()
    attempts = {"count": 0}
    disposed = []

    class DummyResult:
        @staticmethod
        def mappings():
            return [{"id": 1, "name": "demo"}]

    class DummyConnection:
        @staticmethod
        def execute(statement, params):
            return DummyResult()

    @contextmanager
    def fake_connect(connection):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise sa.exc.OperationalError(
                "SELECT 1",
                {},
                Exception("server closed the connection unexpectedly"),
            )
        yield DummyConnection()

    monkeypatch.setattr(db, "connect", fake_connect)
    monkeypatch.setattr(
        db,
        "dispose_engine",
        lambda connection: disposed.append(connection["id"]),
    )

    rows = db.fetch_rows_by_fields(
        {
            "id": "conn_1",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
        },
        "public",
        "book",
        filters={"id": 1},
    )

    assert rows == [{"id": 1, "name": "demo"}]
    assert attempts["count"] == 2
    assert disposed == ["conn_1"]


def test_sync_manager_builds_native_schema(tmp_path: Path):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "soft_delete_column": "deleted",
            },
            {
                "id": "t2",
                "alias": "author",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "author",
                "primary_key": ["id"],
                "soft_delete_column": "deleted",
            },
        ],
        "relations": [
            {
                "id": "r1",
                "parent_table_id": "t1",
                "child_table_id": "t2",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
                "fixed_conditions": [
                    {"field": "relate_type", "operator": "eq", "value": 1},
                ],
            }
        ],
    }

    schema = manager.build_native_schema(job)

    assert manager.determine_job_mode(job) == "native"
    assert schema[0]["database"] == "postgres"
    assert schema[0]["nodes"]["soft_delete_column"] == "deleted"
    assert (
        schema[0]["nodes"]["children"][0]["relationship"]["variant"]
        == "nested"
    )
    assert schema[0]["nodes"]["children"][0]["soft_delete_column"] == "deleted"
    assert schema[0]["nodes"]["children"][0]["relationship"][
        "foreign_key"
    ] == {
        "parent": ["id"],
        "child": ["book_id"],
    }
    assert schema[0]["nodes"]["children"][0]["relationship"][
        "fixed_conditions"
    ] == [{"field": "relate_type", "operator": "eq", "value": 1}]


def test_sync_manager_builds_native_schema_for_many_to_one(tmp_path: Path):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
            },
            {
                "id": "t2",
                "alias": "publisher",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "publisher",
                "primary_key": ["id"],
                "soft_delete_column": "deleted",
            },
        ],
        "relations": [
            {
                "id": "r1",
                "parent_table_id": "t1",
                "child_table_id": "t2",
                "type": "many_to_one",
                "field_pairs": [
                    {"parent_field": "publisher_id", "child_field": "id"},
                ],
            }
        ],
    }

    schema = manager.build_native_schema(job)

    assert manager.determine_job_mode(job) == "native"
    assert (
        schema[0]["nodes"]["children"][0]["relationship"]["variant"]
        == "object"
    )
    assert (
        schema[0]["nodes"]["children"][0]["relationship"]["type"]
        == "one_to_one"
    )
    assert schema[0]["nodes"]["children"][0]["soft_delete_column"] == "deleted"
    assert schema[0]["nodes"]["children"][0]["relationship"][
        "foreign_key"
    ] == {
        "parent": ["publisher_id"],
        "child": ["id"],
    }


def test_bootstrap_payload_normalizes_legacy_many_to_many_relation(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_job(
        {
            "id": "job_1",
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "tables": [
                {
                    "id": "t1",
                    "alias": "book",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "book",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                },
                {
                    "id": "t2",
                    "alias": "publisher",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "publisher",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                },
            ],
            "relations": [
                {
                    "id": "r1",
                    "parent_table_id": "t1",
                    "child_table_id": "t2",
                    "type": "many_to_many",
                    "field_pairs": [
                        {"parent_field": "publisher_id", "child_field": "id"},
                    ],
                }
            ],
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))

    payload = manager.bootstrap_payload()

    assert payload["jobs"][0]["relations"][0]["type"] == "many_to_one"
    assert manager.determine_job_mode(payload["jobs"][0]) == "native"


def test_bootstrap_payload_preserves_parent_fixed_condition_source(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_job(
        {
            "id": "job_1",
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "tables": [
                {
                    "id": "t1",
                    "alias": "book",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "book",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                },
                {
                    "id": "t2",
                    "alias": "author",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "author",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                },
            ],
            "relations": [
                {
                    "id": "r1",
                    "parent_table_id": "t1",
                    "child_table_id": "t2",
                    "type": "one_to_many",
                    "field_pairs": [
                        {"parent_field": "id", "child_field": "book_id"},
                    ],
                    "fixed_conditions": [
                        {
                            "field": "buyer_id",
                            "operator": "eq",
                            "value": 7,
                            "source": "parent",
                        }
                    ],
                }
            ],
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))

    payload = manager.bootstrap_payload()

    assert payload["jobs"][0]["relations"][0]["fixed_conditions"] == [
        {
            "field": "buyer_id",
            "operator": "eq",
            "value": 7,
            "source": "parent",
        }
    ]


def test_bootstrap_payload_preserves_assembly_fallback_flag_for_cross_database_jobs(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_connection(
        {
            "id": "conn_2",
            "name": "demo_b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_job(
        {
            "id": "job_1",
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "tables": [
                {
                    "id": "t1",
                    "alias": "book",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "book",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                },
                {
                    "id": "t2",
                    "alias": "author",
                    "connection_id": "conn_2",
                    "schema": "public",
                    "table": "author",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                },
            ],
            "relations": [
                {
                    "id": "r1",
                    "parent_table_id": "t1",
                    "child_table_id": "t2",
                    "type": "one_to_many",
                    "field_pairs": [
                        {"parent_field": "id", "child_field": "book_id"},
                    ],
                }
            ],
            "enable_assembly_fallback": False,
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))

    payload = manager.bootstrap_payload()

    assert payload["jobs"][0]["enable_assembly_fallback"] is False


def test_search_adapter_converts_large_integers_for_ui():
    class DummyIndices:
        @staticmethod
        def exists(index: str) -> bool:
            return index == "demo_index"

    class DummyClient:
        indices = DummyIndices()

        @staticmethod
        def search(index: str, body: dict) -> dict:
            assert index == "demo_index"
            assert body["track_total_hits"] is True
            return {
                "took": 3,
                "hits": {
                    "total": {"value": 1},
                    "hits": [
                        {
                            "_id": "doc_1",
                            "_score": 1.0,
                            "_source": {
                                "id": 2036441118266179584,
                                "product_code": "SKU-DEMO-001",
                                "nested": [
                                    {
                                        "tag_id": 2012426559042162688,
                                    }
                                ],
                            },
                        }
                    ],
                },
            }

    adapter = SearchAdapter.__new__(SearchAdapter)
    adapter.client = DummyClient()

    result = adapter.search_documents_by_body(
        "demo_index",
        {"query": {"match_all": {}}},
    )

    assert result["hits"][0]["source"]["id"] == "2036441118266179584"
    assert (
        result["hits"][0]["source"]["nested"][0]["tag_id"]
        == "2012426559042162688"
    )


def test_sync_manager_search_index_documents_uses_selected_job(
    tmp_path: Path, monkeypatch
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_job(
        {
            "id": "job_1",
            "name": "demo",
            "index_name": "demo_index",
            "tables": [],
            "relations": [],
            "root_table_id": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))

    class DummySearchAdapter:
        def __init__(self, services: dict) -> None:
            self.services = services

        def search_documents_by_body(
            self, index_name: str, body: dict
        ) -> dict:
            return {
                "exists": True,
                "total": 1,
                "page": 1,
                "size": 10,
                "hits": [{"id": "doc_1", "score": 1.0, "source": body}],
                "index_name": index_name,
            }

    monkeypatch.setattr(
        "pgsync.web.sync_manager.SearchAdapter", DummySearchAdapter
    )

    result = manager.search_index_documents(
        {
            "job_id": "job_1",
            "body": {"query": {"match_all": {}}, "size": 10},
        }
    )

    assert result["job"]["index_name"] == "demo_index"
    assert result["index_name"] == "demo_index"
    assert result["hits"][0]["source"]["query"] == {"match_all": {}}


def test_bootstrap_payload_includes_index_document_count(
    tmp_path: Path, monkeypatch
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_job(
        {
            "id": "job_1",
            "name": "demo",
            "index_name": "demo_index",
            "tables": [],
            "relations": [],
            "root_table_id": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))

    class DummySearchAdapter:
        def __init__(self, services: dict) -> None:
            self.services = services

        def index_exists(self, index_name: str) -> bool:
            assert index_name == "demo_index"
            return True

        def count_documents(self, index_name: str) -> int:
            assert index_name == "demo_index"
            return 321

    monkeypatch.setattr(
        "pgsync.web.sync_manager.SearchAdapter", DummySearchAdapter
    )

    payload = manager.bootstrap_payload()

    assert payload["job_runtime"]["job_1"]["index_exists"] is True
    assert payload["job_runtime"]["job_1"]["job_locked"] is True
    assert payload["job_runtime"]["job_1"]["index_document_count"] == 321


def test_sync_manager_delete_job_preserves_index_by_default(
    tmp_path: Path, monkeypatch
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_job(
        {
            "id": "job_1",
            "name": "demo",
            "index_name": "demo_index",
            "tables": [],
            "relations": [],
            "root_table_id": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))

    class DummySearchAdapter:
        def __init__(self, services: dict) -> None:
            raise AssertionError("delete_index=False 时不应触发 ES 删除")

    monkeypatch.setattr(
        "pgsync.web.sync_manager.SearchAdapter", DummySearchAdapter
    )

    result = manager.delete_job("job_1")

    assert result["delete_index"] is False
    assert result["index_deleted"] is False
    assert store.get_job("job_1") is None


def test_sync_manager_delete_job_can_delete_index(tmp_path: Path, monkeypatch):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_job(
        {
            "id": "job_1",
            "name": "demo",
            "index_name": "demo_index",
            "tables": [],
            "relations": [],
            "root_table_id": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    deleted = []

    class DummySearchAdapter:
        def __init__(self, services: dict) -> None:
            self.services = services

        def delete_index(self, index_name: str) -> bool:
            deleted.append(index_name)
            return True

    monkeypatch.setattr(
        "pgsync.web.sync_manager.SearchAdapter", DummySearchAdapter
    )

    result = manager.delete_job("job_1", delete_index=True)

    assert deleted == ["demo_index"]
    assert result["delete_index"] is True
    assert result["index_deleted"] is True
    assert store.get_job("job_1") is None


def test_bootstrap_payload_clears_stale_running_runtime(tmp_path: Path):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_job(
        {
            "id": "job_1",
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "tables": [
                {
                    "id": "t1",
                    "alias": "book",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "book",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                }
            ],
            "relations": [],
        }
    )
    store.update_job_runtime(
        "job_1",
        {
            "status": "running",
            "pid": 12345,
        },
    )

    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))

    payload = manager.bootstrap_payload()

    assert payload["job_runtime"]["job_1"]["status"] == "stopped"
    assert payload["job_runtime"]["job_1"]["pid"] is None
    assert payload["running_jobs"] == []


def test_bootstrap_payload_marks_resumable_job_as_waiting_incremental_resume(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_job(
        {
            "id": "job_1",
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "tables": [
                {
                    "id": "t1",
                    "alias": "book",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "book",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                }
            ],
            "relations": [],
        }
    )
    store.update_job_runtime(
        "job_1",
        {
            "status": "running",
            "phase": "assembly_incremental",
            "desired_state": "running",
            "baseline_completed": True,
            "resume_strategy": "native_incremental",
            "owner": "",
        },
    )

    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))

    payload = manager.bootstrap_payload()

    assert payload["job_runtime"]["job_1"]["status"] == "stopped"
    assert (
        payload["job_runtime"]["job_1"]["phase"]
        == "awaiting_incremental_resume"
    )
    assert payload["job_runtime"]["job_1"]["baseline_completed"] is True
    assert (
        payload["job_runtime"]["job_1"]["resume_strategy"]
        == "native_incremental"
    )


def test_sync_manager_reuses_persisted_instance_id_across_restart(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_job(
        {
            "id": "job_1",
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "tables": [
                {
                    "id": "t1",
                    "alias": "book",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "book",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                }
            ],
            "relations": [],
        }
    )

    first_manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    store.update_job_runtime(
        "job_1",
        {
            "status": "running",
            "phase": "assembly_incremental",
            "desired_state": "running",
            "baseline_completed": True,
            "resume_strategy": "native_incremental",
            "owner": first_manager.instance_id,
        },
    )

    restarted_manager = SyncManager(
        store=store,
        db=None,
        repo_root=str(tmp_path),
    )
    payload = restarted_manager.bootstrap_payload()

    assert restarted_manager.instance_id == first_manager.instance_id
    assert (
        payload["job_runtime"]["job_1"]["phase"]
        == "awaiting_incremental_resume"
    )
    assert payload["job_runtime"]["job_1"]["owner"] is None


def test_sync_manager_reconciles_resumable_job_with_stale_remote_owner(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_job(
        {
            "id": "job_1",
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "tables": [
                {
                    "id": "t1",
                    "alias": "book",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "book",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                }
            ],
            "relations": [],
        }
    )
    store.update_job_runtime(
        "job_1",
        {
            "status": "running",
            "phase": "assembly_incremental",
            "desired_state": "running",
            "baseline_completed": True,
            "resume_strategy": "native_incremental",
            "owner": "other-studio",
        },
    )

    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    payload = manager.bootstrap_payload()

    assert payload["job_runtime"]["job_1"]["status"] == "stopped"
    assert (
        payload["job_runtime"]["job_1"]["phase"]
        == "awaiting_incremental_resume"
    )
    assert payload["job_runtime"]["job_1"]["owner"] is None


def build_failed_recovery_store(
    tmp_path: Path,
    runtime_patch: dict | None = None,
) -> ConfigStore:
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_job(
        {
            "id": "job_1",
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "tables": [
                {
                    "id": "t1",
                    "alias": "book",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "book",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                }
            ],
            "relations": [],
        }
    )
    runtime = {
        "status": "failed",
        "phase": "failed",
        "desired_state": "running",
        "baseline_completed": True,
        "resume_strategy": "native_incremental",
        "last_error": "Elasticsearch unavailable",
    }
    runtime.update(runtime_patch or {})
    store.update_job_runtime("job_1", runtime)
    return store


def test_failed_resumable_job_uses_persisted_exponential_backoff(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        sync_manager_module.random, "uniform", lambda *args: 1.0
    )
    clock = {"now": 1000.0}
    monkeypatch.setattr(
        sync_manager_module,
        "utc_timestamp",
        lambda: clock["now"],
    )
    store = build_failed_recovery_store(tmp_path)
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    started_job_ids: list[str] = []
    monkeypatch.setattr(
        manager, "describe_job_holder", lambda *args, **kwargs: {}
    )
    monkeypatch.setattr(
        manager,
        "start_job",
        lambda job_id: started_job_ids.append(job_id) or {},
    )

    runtime = store.get_job_runtime("job_1")
    assert runtime["status"] == "failed"
    assert runtime["runner_recovery_attempt_count"] == 1
    assert runtime["runner_recovery_retry_delay_seconds"] == 10
    assert runtime["runner_recovery_next_attempt_ts"] == 1010.0
    assert manager.run_recovery_cycle() == []

    clock["now"] = 1010.0
    assert manager.run_recovery_cycle() == ["job_1"]
    assert started_job_ids == ["job_1"]

    clock["now"] = 1011.0
    store.update_job_runtime(
        "job_1",
        {
            "status": "failed",
            "phase": "failed",
            "last_error": "Elasticsearch still unavailable",
        },
    )
    manager.on_runner_finished("job_1")

    runtime = store.get_job_runtime("job_1")
    assert runtime["runner_recovery_attempt_count"] == 2
    assert runtime["runner_recovery_retry_delay_seconds"] == 20
    assert runtime["runner_recovery_next_attempt_ts"] == 1031.0
    assert runtime["runner_recovery_last_error"] == (
        "Elasticsearch still unavailable"
    )

    clock["now"] = 1030.0
    assert manager.run_recovery_cycle() == []
    clock["now"] = 1031.0
    assert manager.run_recovery_cycle() == ["job_1"]
    assert started_job_ids == ["job_1", "job_1"]


@pytest.mark.parametrize(
    "runtime_patch",
    [
        {"runner_recovery_blocked_reason": "authentication_error"},
        {"desired_state": "stopped"},
        {"baseline_completed": False},
        {"resume_strategy": "assembly_incremental"},
        {"assembly_recovery_requires_full_sync": True},
    ],
    ids=[
        "authentication-error",
        "desired-stopped",
        "baseline-incomplete",
        "strategy-mismatch",
        "full-sync-required",
    ],
)
def test_failed_job_auto_recovery_rejects_unsafe_runtime(
    tmp_path: Path,
    monkeypatch,
    runtime_patch: dict,
):
    monkeypatch.setattr(sync_manager_module, "utc_timestamp", lambda: 1000.0)
    store = build_failed_recovery_store(tmp_path, runtime_patch)
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    started_job_ids: list[str] = []
    monkeypatch.setattr(
        manager, "describe_job_holder", lambda *args, **kwargs: {}
    )
    monkeypatch.setattr(
        manager,
        "start_job",
        lambda job_id: started_job_ids.append(job_id) or {},
    )

    assert manager.run_recovery_cycle() == []
    assert started_job_ids == []
    runtime = store.get_job_runtime("job_1")
    assert "runner_recovery_next_attempt_ts" not in runtime


def test_recovery_cycle_does_not_restart_job_stopped_after_reconciliation(
    tmp_path: Path,
    monkeypatch,
):
    store = build_failed_recovery_store(tmp_path)
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    started_job_ids: list[str] = []

    def stop_job_during_reconciliation() -> list[str]:
        store.update_job_runtime(
            "job_1",
            {
                "status": "stopped",
                "phase": "stopped",
                "desired_state": "stopped",
            },
        )
        return ["job_1"]

    monkeypatch.setattr(
        manager,
        "reconcile_runtime_states",
        stop_job_during_reconciliation,
    )
    monkeypatch.setattr(
        manager,
        "start_job",
        lambda job_id: started_job_ids.append(job_id) or {},
    )

    assert manager.run_recovery_cycle() == []
    assert started_job_ids == []


def test_assembly_runner_failure_preserves_lease_lost_terminal_phase(
    tmp_path: Path,
    monkeypatch,
):
    store = build_failed_recovery_store(tmp_path)
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    store.update_job_runtime(
        "job_1",
        {
            "status": "running",
            "phase": "lease_lost",
            "last_error": "Redis job lease lost",
            "runner_recovery_attempt_count": 0,
            "runner_recovery_next_attempt_ts": None,
        },
    )
    runner = AssemblyRunner(
        manager,
        store.get_job("job_1"),
        incremental_only=True,
    )
    runner.lease_lost = True

    class FailingSearchAdapter:
        def __init__(self, services: dict) -> None:
            store.update_job_runtime(
                "job_1",
                {
                    "status": "failed",
                    "phase": "lease_lost",
                    "last_error": "Redis job lease lost",
                },
            )
            raise RuntimeError("Elasticsearch unavailable")

    monkeypatch.setattr(
        sync_manager_module,
        "SearchAdapter",
        FailingSearchAdapter,
    )

    runner.run()

    runtime = store.get_job_runtime("job_1")
    assert runtime["status"] == "failed"
    assert runtime["phase"] == "lease_lost"
    assert runtime["last_error"] == "Redis job lease lost"
    assert runtime["runner_recovery_next_attempt_ts"] is None


def test_failed_recovery_start_error_advances_backoff(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        sync_manager_module.random, "uniform", lambda *args: 1.0
    )
    clock = {"now": 1000.0}
    monkeypatch.setattr(
        sync_manager_module,
        "utc_timestamp",
        lambda: clock["now"],
    )
    store = build_failed_recovery_store(tmp_path)
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    monkeypatch.setattr(
        manager, "describe_job_holder", lambda *args, **kwargs: {}
    )
    monkeypatch.setattr(
        manager,
        "start_job",
        lambda job_id: (_ for _ in ()).throw(
            RuntimeError("Elasticsearch unavailable")
        ),
    )

    clock["now"] = 1010.0
    assert manager.run_recovery_cycle() == []

    runtime = store.get_job_runtime("job_1")
    assert runtime["status"] == "failed"
    assert runtime["runner_recovery_attempt_count"] == 2
    assert runtime["runner_recovery_retry_delay_seconds"] == 20
    assert runtime["runner_recovery_next_attempt_ts"] == 1030.0


def test_stable_auto_recovered_runner_resets_consecutive_failure_backoff(
    tmp_path: Path,
    monkeypatch,
):
    clock = {"now": 1000.0}
    monkeypatch.setattr(
        sync_manager_module,
        "utc_timestamp",
        lambda: clock["now"],
    )
    store = build_failed_recovery_store(tmp_path)
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    store.update_job_runtime(
        "job_1",
        {
            "status": "running",
            "runner_recovery_attempt_count": 3,
            "runner_recovery_next_attempt_ts": None,
            "runner_recovery_started_at": "2026-01-01T00:00:00+00:00",
            "runner_recovery_started_ts": 1000.0,
        },
    )
    manager.runners["job_1"] = SimpleNamespace(
        thread=SimpleNamespace(is_alive=lambda: True)
    )

    clock["now"] = 1059.0
    manager.reconcile_runtime_states()
    assert store.get_job_runtime("job_1")["runner_recovery_attempt_count"] == 3

    clock["now"] = 1060.0
    manager.reconcile_runtime_states()
    runtime = store.get_job_runtime("job_1")
    assert runtime["runner_recovery_attempt_count"] == 0
    assert runtime["runner_recovery_succeeded_at"]


def test_run_recovery_cycle_auto_starts_awaiting_incremental_resume_job(
    tmp_path: Path,
    monkeypatch,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_job(
        {
            "id": "job_1",
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "tables": [
                {
                    "id": "t1",
                    "alias": "book",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "book",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                }
            ],
            "relations": [],
        }
    )
    store.update_job_runtime(
        "job_1",
        {
            "status": "stopped",
            "phase": "awaiting_incremental_resume",
            "desired_state": "running",
            "baseline_completed": True,
            "resume_strategy": "native_incremental",
        },
    )

    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    started_job_ids: list[str] = []

    monkeypatch.setattr(
        manager, "describe_job_holder", lambda *args, **kwargs: {}
    )
    monkeypatch.setattr(
        manager,
        "start_job",
        lambda job_id: started_job_ids.append(job_id) or {},
    )

    assert manager.run_recovery_cycle() == ["job_1"]
    assert started_job_ids == ["job_1"]


def test_sync_manager_reconciles_resumable_stopped_job_back_to_waiting_incremental_resume(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_job(
        {
            "id": "job_1",
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "tables": [
                {
                    "id": "t1",
                    "alias": "book",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "book",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                }
            ],
            "relations": [],
        }
    )
    store.update_job_runtime(
        "job_1",
        {
            "status": "stopped",
            "phase": "stopped",
            "desired_state": "running",
            "baseline_completed": True,
            "resume_strategy": "native_incremental",
            "owner": "old-owner",
            "heartbeat_at": "2026-01-01T00:00:00+00:00",
        },
    )

    SyncManager(store=store, db=None, repo_root=str(tmp_path))
    runtime = store.get_job_runtime("job_1")

    assert runtime["status"] == "stopped"
    assert runtime["phase"] == "awaiting_incremental_resume"
    assert runtime["owner"] is None
    assert runtime["heartbeat_at"] is None


def test_sync_manager_builds_native_sql_preview(tmp_path: Path):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    preview = manager.preview_job_sql(
        {
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "tables": [
                {
                    "id": "t1",
                    "alias": "book",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "book",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                },
                {
                    "id": "t2",
                    "alias": "author",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "author",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                },
            ],
            "relations": [
                {
                    "id": "r1",
                    "parent_table_id": "t1",
                    "child_table_id": "t2",
                    "type": "one_to_many",
                    "field_pairs": [
                        {"parent_field": "id", "child_field": "book_id"},
                    ],
                    "fixed_conditions": [
                        {"field": "relate_type", "operator": "eq", "value": 1},
                    ],
                }
            ],
        }
    )

    assert preview["mode"] == "native"
    assert len(preview["steps"]) == 4
    assert preview["steps"][0]["kind"] == "native_setup_checkpoint"
    assert preview["steps"][1]["kind"] == "assembly_full_sync_query"
    assert preview["steps"][2]["kind"] == "assembly_full_sync_query"
    assert '"author"."book_id" = :book_id' in preview["steps"][2]["sql"]
    assert '"author"."relate_type" = 1' in preview["steps"][2]["sql"]
    assert preview["steps"][3]["kind"] == "native_incremental_note"


def test_sync_manager_builds_native_sql_preview_with_parent_fixed_condition_source(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    preview = manager.preview_job_sql(
        {
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "tables": [
                {
                    "id": "t1",
                    "alias": "book",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "book",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                },
                {
                    "id": "t2",
                    "alias": "author",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "author",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                },
            ],
            "relations": [
                {
                    "id": "r1",
                    "parent_table_id": "t1",
                    "child_table_id": "t2",
                    "type": "one_to_many",
                    "field_pairs": [
                        {"parent_field": "id", "child_field": "book_id"},
                    ],
                    "fixed_conditions": [
                        {"field": "relate_type", "operator": "eq", "value": 1},
                        {
                            "field": "buyer_id",
                            "operator": "eq",
                            "value": 7,
                            "source": "parent",
                        },
                    ],
                }
            ],
        }
    )

    assert preview["mode"] == "native"
    assert len(preview["steps"]) == 4
    assert preview["steps"][0]["kind"] == "native_setup_checkpoint"
    assert preview["steps"][2]["kind"] == "assembly_full_sync_query"
    assert '"author"."relate_type" = 1' in preview["steps"][2]["sql"]
    assert ":book_buyer_id = 7" in preview["steps"][2]["sql"]
    assert preview["steps"][3]["kind"] == "native_incremental_note"


def test_sync_manager_builds_cross_database_sql_preview(tmp_path: Path):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_connection(
        {
            "id": "conn_2",
            "name": "db-b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    preview = manager.preview_job_sql(
        {
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "tables": [
                {
                    "id": "t1",
                    "alias": "book",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "book",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                },
                {
                    "id": "t2",
                    "alias": "author",
                    "connection_id": "conn_2",
                    "schema": "public",
                    "table": "author",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                },
            ],
            "relations": [
                {
                    "id": "r1",
                    "parent_table_id": "t1",
                    "child_table_id": "t2",
                    "type": "one_to_many",
                    "field_pairs": [
                        {"parent_field": "id", "child_field": "book_id"},
                    ],
                }
            ],
        }
    )

    assert preview["mode"] == "native"
    assert len(preview["steps"]) == 4
    assert preview["steps"][0]["connection_name"] == "db-a"
    assert preview["steps"][0]["kind"] == "native_setup_checkpoint"
    assert "变更捕获能力" in preview["steps"][0]["sql"]
    assert preview["steps"][1]["connection_name"] == "db-a"
    assert preview["steps"][2]["connection_name"] == "db-b"
    assert ":book_id" in preview["steps"][2]["sql"]
    assert preview["steps"][3]["kind"] == "cross_database_incremental_note"
    assert (
        "Assembly 按 root key 回源重建整篇文档" in preview["steps"][3]["sql"]
    )


def test_sync_manager_builds_cross_database_sql_preview_with_parent_fixed_condition_source(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_connection(
        {
            "id": "conn_2",
            "name": "db-b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    preview = manager.preview_job_sql(
        {
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "tables": [
                {
                    "id": "t1",
                    "alias": "book",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "book",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                },
                {
                    "id": "t2",
                    "alias": "author",
                    "connection_id": "conn_2",
                    "schema": "public",
                    "table": "author",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                },
            ],
            "relations": [
                {
                    "id": "r1",
                    "parent_table_id": "t1",
                    "child_table_id": "t2",
                    "type": "one_to_many",
                    "field_pairs": [
                        {"parent_field": "id", "child_field": "book_id"},
                    ],
                    "fixed_conditions": [
                        {"field": "relate_type", "operator": "eq", "value": 1},
                        {
                            "field": "buyer_id",
                            "operator": "eq",
                            "value": 7,
                            "source": "parent",
                        },
                    ],
                }
            ],
        }
    )

    assert preview["mode"] == "native"
    assert len(preview["steps"]) == 4
    assert '"author"."relate_type" = 1' in preview["steps"][2]["sql"]
    assert ":book_buyer_id = 7" in preview["steps"][2]["sql"]


def test_sync_manager_builds_cross_database_sql_preview_with_assembly_fallback_note(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_connection(
        {
            "id": "conn_2",
            "name": "db-b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    preview = manager.preview_job_sql(
        {
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "enable_assembly_fallback": True,
            "tables": [
                {
                    "id": "t1",
                    "alias": "book",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "book",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                },
                {
                    "id": "t2",
                    "alias": "author",
                    "connection_id": "conn_2",
                    "schema": "public",
                    "table": "author",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                },
            ],
            "relations": [
                {
                    "id": "r1",
                    "parent_table_id": "t1",
                    "child_table_id": "t2",
                    "type": "one_to_many",
                    "field_pairs": [
                        {"parent_field": "id", "child_field": "book_id"},
                    ],
                }
            ],
        }
    )

    assert preview["steps"][3]["kind"] == "cross_database_incremental_note"
    assert "Recovery Scan" in preview["steps"][3]["sql"]


def test_prepare_job_keeps_assembly_fallback_optional_for_cross_database_jobs(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_connection(
        {
            "id": "conn_2",
            "name": "db-b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
            {
                "id": "t2",
                "alias": "author",
                "connection_id": "conn_2",
                "schema": "public",
                "table": "author",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
        ],
        "relations": [
            {
                "id": "r1",
                "parent_table_id": "t1",
                "child_table_id": "t2",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
            }
        ],
        "enable_assembly_fallback": False,
    }

    prepared = manager.prepare_job(job)

    assert manager.determine_job_mode(prepared) == "native"
    assert prepared["enable_assembly_fallback"] is False
    assert prepared["poll_interval_seconds"] == 60


def test_sync_manager_uses_native_pipeline_for_single_database_one_to_many(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
            {
                "id": "t2",
                "alias": "author",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "author",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
        ],
        "relations": [
            {
                "id": "r1",
                "parent_table_id": "t1",
                "child_table_id": "t2",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
            }
        ],
        "enable_assembly_fallback": False,
    }

    prepared = manager.prepare_job(job)

    assert manager.determine_job_mode(prepared) == "native"


def test_prepare_job_keeps_assembly_fallback_for_single_database_native_job(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "enable_assembly_fallback": True,
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            }
        ],
        "relations": [],
    }

    prepared = manager.prepare_job(job)

    assert prepared["enable_assembly_fallback"] is True
    assert manager.determine_job_mode(prepared) == "native"


def test_prepare_job_defaults_durable_change_events_off(tmp_path: Path):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            }
        ],
        "relations": [],
    }

    prepared = manager.prepare_job(job)

    assert prepared["enable_durable_change_events"] is False


def test_prepare_job_accepts_durable_change_event_options(tmp_path: Path):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "enable_durable_change_events": "true",
        "durable_change_event_batch_size": "800",
        "durable_change_event_replay_window": "9000",
        "durable_change_event_processed_window": "12000",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            }
        ],
        "relations": [],
    }

    prepared = manager.prepare_job(job)

    assert prepared["enable_durable_change_events"] is True
    assert prepared["durable_change_event_batch_size"] == 800
    assert prepared["durable_change_event_replay_window"] == 9000
    assert prepared["durable_change_event_processed_window"] == 12000


def test_sync_manager_builds_single_database_native_capture_sql_preview(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))

    preview = manager.preview_job_sql(
        {
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "tables": [
                {
                    "id": "t1",
                    "alias": "book",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "book",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                },
                {
                    "id": "t2",
                    "alias": "author",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "author",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                },
            ],
            "relations": [
                {
                    "id": "r1",
                    "parent_table_id": "t1",
                    "child_table_id": "t2",
                    "type": "one_to_many",
                    "field_pairs": [
                        {"parent_field": "id", "child_field": "book_id"},
                    ],
                }
            ],
        }
    )

    assert preview["mode"] == "native"
    assert preview["steps"][0]["kind"] == "native_setup_checkpoint"
    assert "监听器会覆盖当前库内参与同步的所有表" in preview["steps"][0]["sql"]
    assert preview["steps"][-1]["kind"] == "native_incremental_note"
    assert (
        "Assembly 按 root key 回源重建整篇文档" in preview["steps"][-1]["sql"]
    )


def test_start_job_uses_hybrid_runner_for_single_database_native_job(
    tmp_path: Path,
    monkeypatch,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    services = default_config()["services"]
    services["search"]["url"] = "http://localhost:9200"
    services["redis"]["host"] = "127.0.0.1"
    store.save_services(services)
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    store.upsert_job(
        {
            "id": "job_1",
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "tables": [
                {
                    "id": "t1",
                    "alias": "book",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "book",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                }
            ],
            "relations": [],
        }
    )

    started: list[str] = []

    monkeypatch.setattr(
        SyncManager,
        "acquire_job_lease",
        lambda self, job_id, services: NoopJobLease(),
    )
    monkeypatch.setattr(
        "pgsync.web.sync_manager.RedisRootQueue.from_services",
        lambda services, job_id: FakeRootQueue(),
    )
    monkeypatch.setattr(
        SyncManager,
        "start_job_lease_monitor",
        lambda self, job_id, runner, lease: None,
    )
    monkeypatch.setattr(
        CrossDatabaseHybridRunner,
        "start",
        lambda self: started.append(self.__class__.__name__),
    )

    runtime = manager.start_job("job_1")

    assert started == ["CrossDatabaseHybridRunnerV2"]
    assert isinstance(manager.runners["job_1"], CrossDatabaseHybridRunner)
    assert runtime["resume_strategy"] == "native_incremental"


def test_cross_database_runner_single_database_disables_recovery_scan(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            }
        ],
        "relations": [],
    }

    runner = CrossDatabaseHybridRunner(manager, job)

    assert runner.is_cross_database is False
    assert runner.assembly_fallback_enabled is False
    assert runner.recovery_scan_enabled is True
    assert runner.periodic_recovery_scan_enabled is False


def test_cross_database_runner_cross_database_recovery_scan_follows_fallback_flag(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_connection(
        {
            "id": "conn_2",
            "name": "db-b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "enable_assembly_fallback": True,
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
            {
                "id": "t2",
                "alias": "author",
                "connection_id": "conn_2",
                "schema": "public",
                "table": "author",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
        ],
        "relations": [
            {
                "id": "r1",
                "parent_table_id": "t1",
                "child_table_id": "t2",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
            }
        ],
    }

    runner = CrossDatabaseHybridRunner(manager, job)

    assert runner.is_cross_database is True
    assert runner.assembly_fallback_enabled is True
    assert runner.recovery_scan_enabled is True


def test_sync_manager_builds_native_baseline_job_for_cross_database_tree(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_connection(
        {
            "id": "conn_2",
            "name": "db-b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
            },
            {
                "id": "t2",
                "alias": "author",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "author",
                "primary_key": ["id"],
            },
            {
                "id": "t3",
                "alias": "tag",
                "connection_id": "conn_2",
                "schema": "public",
                "table": "tag",
                "primary_key": ["id"],
            },
        ],
        "relations": [
            {
                "id": "r1",
                "parent_table_id": "t1",
                "child_table_id": "t2",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
            },
            {
                "id": "r2",
                "parent_table_id": "t2",
                "child_table_id": "t3",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "author_id"},
                ],
            },
        ],
    }

    baseline_job = manager.build_native_baseline_job(job)

    assert [table["id"] for table in baseline_job["tables"]] == ["t1", "t2"]
    assert [relation["id"] for relation in baseline_job["relations"]] == ["r1"]


def test_native_runner_uses_bootstrap_then_daemon_for_initial_start(
    tmp_path: Path,
    monkeypatch,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    connection = store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            }
        ],
        "relations": [],
    }
    runner = NativeProcessRunner(manager, job, connection)
    events = []

    class FakeStopEvent:
        @staticmethod
        def wait(interval: int) -> bool:
            return True

        @staticmethod
        def set() -> None:
            return None

    runner.stop_event = FakeStopEvent()
    monkeypatch.setattr(
        runner,
        "_run_bootstrap",
        lambda: (
            events.append("bootstrap"),
            manager.update_runtime(
                "job_1",
                {
                    "baseline_completed": True,
                    "resume_strategy": "native_incremental",
                },
            ),
        )[-1],
    )
    monkeypatch.setattr(
        runner,
        "_start_daemon",
        lambda: events.append("start_daemon"),
    )
    monkeypatch.setattr(runner, "_terminate_process", lambda process: None)

    runner.run()

    runtime = store.get_job_runtime("job_1")

    assert events == ["bootstrap", "start_daemon"]
    assert runtime["resume_strategy"] == "native_incremental"


def test_native_runner_resume_incremental_only_skips_bootstrap(
    tmp_path: Path,
    monkeypatch,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    connection = store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            }
        ],
        "relations": [],
    }
    runner = NativeProcessRunner(
        manager,
        job,
        connection,
        resume_incremental_only=True,
    )
    events = []

    class FakeStopEvent:
        @staticmethod
        def wait(interval: int) -> bool:
            return True

        @staticmethod
        def set() -> None:
            return None

    runner.stop_event = FakeStopEvent()
    monkeypatch.setattr(
        runner,
        "_ensure_resume_checkpoint",
        lambda: events.append("ensure_resume_checkpoint"),
    )
    monkeypatch.setattr(
        runner,
        "_run_bootstrap",
        lambda: (_ for _ in ()).throw(AssertionError("should not bootstrap")),
    )
    monkeypatch.setattr(
        runner,
        "_start_daemon",
        lambda: events.append("start_daemon"),
    )
    monkeypatch.setattr(runner, "_terminate_process", lambda process: None)

    runner.run()

    runtime = store.get_job_runtime("job_1")

    assert events == ["ensure_resume_checkpoint", "start_daemon"]
    assert runtime["resume_strategy"] == "native_incremental"


def test_cross_database_runner_builds_native_listener_specs_for_child_tables(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_connection(
        {
            "id": "conn_2",
            "name": "db-b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )

    class FakeDb:
        @staticmethod
        def get_table_columns(
            connection: dict, schema: str, table: str
        ) -> list[dict]:
            if table == "book":
                return [
                    {"name": "id"},
                    {"name": "updated_at"},
                    {"name": "title"},
                ]
            if table == "author":
                return [
                    {"name": "id"},
                    {"name": "book_id"},
                    {"name": "updated_at"},
                ]
            raise AssertionError(f"unexpected table: {table}")

    manager = SyncManager(store=store, db=FakeDb(), repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
            {
                "id": "t2",
                "alias": "author",
                "connection_id": "conn_2",
                "schema": "public",
                "table": "author",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
        ],
        "relations": [
            {
                "id": "r1",
                "parent_table_id": "t1",
                "child_table_id": "t2",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
            }
        ],
    }
    runner = CrossDatabaseHybridRunner(manager, job)
    specs = runner._build_native_listener_specs()

    assert specs["conn_1"]["schemas"]["public"]["tables"] == {"book"}
    assert specs["conn_2"]["schemas"]["public"]["tables"] == {"author"}
    assert specs["conn_2"]["schemas"]["public"][
        "user_defined_fkey_tables"
    ] == {"author": {"book_id"}}
    assert specs["conn_2"]["watched_locations"] == {("public", "author")}


@pytest.mark.parametrize(
    "credential_env",
    [
        {
            "PGSYNC_STUDIO_PG_USERNAME": "runtime_user",
            "PGSYNC_STUDIO_PG_PASSWORD": "runtime:$ecret",
        },
        {
            "PGSYNC_STUDIO_CONFIG_DATABASE_USERNAME": "runtime_user",
            "PGSYNC_STUDIO_CONFIG_DATABASE_PASSWORD": "runtime:$ecret",
        },
        {
            "PGSYNC_STUDIO_CONFIG_DATABASE_URL": (
                "postgresql+psycopg2://runtime_user:runtime%3A%24ecret"
                "@localhost:5432/studio"
            ),
        },
    ],
)
def test_cross_database_capture_uses_runtime_credentials(
    tmp_path: Path,
    monkeypatch,
    credential_env,
):
    for username_env, password_env in db_module.SHARED_PG_CREDENTIAL_ENV_PAIRS:
        monkeypatch.delenv(username_env, raising=False)
        monkeypatch.delenv(password_env, raising=False)
    for url_env in db_module.CONFIG_DATABASE_URL_ENV_NAMES:
        monkeypatch.delenv(url_env, raising=False)
    for key, value in credential_env.items():
        monkeypatch.setenv(key, value)

    store = ConfigStore(str(tmp_path / "config.json"))
    connection = store.upsert_connection(
        {
            "id": "conn_1",
            "host": "localhost",
            "port": 5432,
            "database": "catalog",
            "username": MASKED_SHARED_CREDENTIAL,
            "password": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    runner = CrossDatabaseHybridRunner.__new__(CrossDatabaseHybridRunner)
    runner.manager = manager
    runner.job = {"id": "job_1", "index_name": "book_catalog_v1"}
    runner.setup_phase = "native_setup"
    runner.native_listener_specs = {
        "conn_1": {"connection": connection, "schemas": {}}
    }
    helper_factory = MagicMock()
    monkeypatch.setattr(sync_manager_module, "PGSyncBase", helper_factory)

    runner._prepare_native_incremental_start()

    helper_factory.assert_called_once_with(
        "catalog",
        host="localhost",
        port=5432,
        user="runtime_user",
        password="runtime:$ecret",
    )
    assert connection["username"] == MASKED_SHARED_CREDENTIAL
    assert connection["password"] == ""
    assert store.get_connection("conn_1") == connection


def test_native_listener_refreshes_runtime_credentials_on_reconnect(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setenv("PGSYNC_STUDIO_PG_USERNAME", "initial_user")
    monkeypatch.setenv("PGSYNC_STUDIO_PG_PASSWORD", "initial:$ecret")
    store = ConfigStore(str(tmp_path / "config.json"))
    connection = store.upsert_connection(
        {
            "id": "conn_1",
            "host": "localhost",
            "port": 5432,
            "database": "catalog",
            "username": MASKED_SHARED_CREDENTIAL,
            "password": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    runner = CrossDatabaseHybridRunner.__new__(CrossDatabaseHybridRunner)
    runner.manager = manager
    runner.job = {"id": "job_1"}
    runner.stop_requested = False
    runner.native_listener_states = {}
    runner.store = store
    monkeypatch.setattr(runner, "_request_recovery_scan", lambda reason: None)
    listener_stop_event = threading.Event()
    monkeypatch.setattr(listener_stop_event, "wait", lambda timeout: False)
    raw_connection = MagicMock()
    raw_connection.cursor.return_value.execute.side_effect = (
        lambda query: listener_stop_event.set()
    )
    connect_calls = []

    def connect(**kwargs):
        connect_calls.append(kwargs)
        if len(connect_calls) == 1:
            monkeypatch.setenv("PGSYNC_STUDIO_PG_USERNAME", "rotated_user")
            monkeypatch.setenv("PGSYNC_STUDIO_PG_PASSWORD", "rotated:$ecret")
            raise sync_manager_module.psycopg2.OperationalError(
                "simulated disconnect"
            )
        return raw_connection

    monkeypatch.setattr(sync_manager_module.psycopg2, "connect", connect)

    runner._native_listener_loop(
        "conn_1", connection, {("public", "books")}, listener_stop_event
    )

    assert [(call["user"], call["password"]) for call in connect_calls] == [
        ("initial_user", "initial:$ecret"),
        ("rotated_user", "rotated:$ecret"),
    ]
    assert all(call["dbname"] == "catalog" for call in connect_calls)
    raw_connection.cursor.return_value.execute.assert_called_once_with(
        'LISTEN "catalog"'
    )
    raw_connection.close.assert_called_once()
    assert connection["username"] == MASKED_SHARED_CREDENTIAL
    assert connection["password"] == ""
    assert store.get_connection("conn_1") == connection


def test_cross_database_runner_restarts_dead_native_listeners(
    tmp_path: Path,
    monkeypatch,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    runner = CrossDatabaseHybridRunner.__new__(CrossDatabaseHybridRunner)
    runner.manager = manager
    runner.store = store
    runner.job = {"id": "job_1"}
    runner.incremental_phase = "native_incremental"
    runner.stop_requested = False
    runner.stop_event = threading.Event()
    runner.native_listener_states = {
        "conn_1": {
            "state": "listening",
            "reason": "listening",
            "reconnect_count": 0,
        }
    }
    runner.native_recovery_event = threading.Event()
    runner.native_listener_specs = {
        "conn_1": {
            "connection": {"database": "postgres_a"},
            "watched_locations": {("public", "book")},
        }
    }
    runner.native_listener_stop_events = {}
    runner.native_listener_threads = {}
    runner.native_listener_threads["conn_1"] = threading.Thread(
        target=lambda: None
    )

    restarted: list[str] = []
    monkeypatch.setattr(
        runner,
        "_start_native_daemon",
        lambda **kwargs: restarted.append("started"),
    )
    monkeypatch.setattr(
        runner,
        "_request_recovery_scan",
        lambda reason: restarted.append(reason),
    )

    runner._restart_dead_native_listeners_if_needed()

    assert restarted == [
        "listener thread restarted after becoming inactive",
        "started",
    ]
    assert "conn_1" not in runner.native_listener_threads


def test_cross_database_runner_uses_setup_before_full_sync(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setattr(
        "pgsync.web.sync_manager.RedisRootQueue.from_services",
        lambda services, job_id: FakeRootQueue(),
    )
    store = ConfigStore(str(tmp_path / "config.json"))
    store.save_services(
        {
            "search": {
                "enabled": True,
                "url": "http://localhost:9200",
                "username": "",
                "password": "",
                "verify_certs": False,
            },
            "redis": {
                "enabled": True,
                "host": "127.0.0.1",
                "port": 6379,
                "password": "",
                "db": 0,
            },
        }
    )
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_connection(
        {
            "id": "conn_2",
            "name": "db-b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "poll_interval_seconds": 1,
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
            {
                "id": "t2",
                "alias": "author",
                "connection_id": "conn_2",
                "schema": "public",
                "table": "author",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
        ],
        "relations": [
            {
                "id": "r1",
                "parent_table_id": "t1",
                "child_table_id": "t2",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
            }
        ],
    }
    runner = CrossDatabaseHybridRunner(manager, job)
    events = []

    class DummySearchAdapter:
        def __init__(self, services: dict) -> None:
            self.services = services

        def ensure_index(
            self,
            index_name: str,
            mapping: dict | None = None,
        ) -> None:
            events.append(f"ensure_index:{index_name}")

    monkeypatch.setattr(
        "pgsync.web.sync_manager.SearchAdapter", DummySearchAdapter
    )
    monkeypatch.setattr(
        runner,
        "_prepare_native_incremental_start",
        lambda: events.append("prepare_native_incremental_start"),
    )
    monkeypatch.setattr(
        runner,
        "_build_full_sync_cursor_seed",
        lambda: {},
    )
    monkeypatch.setattr(
        runner.assembly_runner,
        "full_sync",
        lambda search, **kwargs: (
            events.append("assembly_full_sync"),
            setattr(runner, "stop_requested", True),
        )[-1],
    )
    monkeypatch.setattr(
        runner,
        "_start_native_daemon",
        lambda **kwargs: events.append(
            "start_native_daemon:%s:%s"
            % (
                kwargs.get("phase", "cross_database_incremental"),
                kwargs.get("baseline_completed", True),
            )
        ),
    )

    runner.run()

    runtime = store.get_job_runtime("job_1")

    assert events == [
        "ensure_index:demo_index",
        "prepare_native_incremental_start",
        "start_native_daemon:cross_database_full_sync:False",
        "assembly_full_sync",
    ]
    assert runtime["status"] == "starting"


def test_cross_database_runner_resume_incremental_only_skips_setup_and_full_sync(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        "pgsync.web.sync_manager.RedisRootQueue.from_services",
        lambda services, job_id: FakeRootQueue(),
    )
    store = ConfigStore(str(tmp_path / "config.json"))
    store.save_services(
        {
            "search": {
                "enabled": True,
                "url": "http://localhost:9200",
                "username": "",
                "password": "",
                "verify_certs": False,
            },
            "redis": {
                "enabled": True,
                "host": "127.0.0.1",
                "port": 6379,
                "password": "",
                "db": 0,
            },
        }
    )
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_connection(
        {
            "id": "conn_2",
            "name": "db-b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "poll_interval_seconds": 1,
        "enable_assembly_fallback": True,
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
            {
                "id": "t2",
                "alias": "author",
                "connection_id": "conn_2",
                "schema": "public",
                "table": "author",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
        ],
        "relations": [
            {
                "id": "r1",
                "parent_table_id": "t1",
                "child_table_id": "t2",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
            }
        ],
    }
    runner = CrossDatabaseHybridRunner(
        manager,
        job,
        resume_incremental_only=True,
    )
    events = []

    class DummySearchAdapter:
        def __init__(self, services: dict) -> None:
            self.services = services

        def ensure_index(
            self,
            index_name: str,
            mapping: dict | None = None,
        ) -> None:
            events.append(f"ensure_index:{index_name}")

    class FakeStopEvent:
        @staticmethod
        def wait(interval: int) -> bool:
            return True

        @staticmethod
        def is_set() -> bool:
            return False

        @staticmethod
        def set() -> None:
            return None

    runner.stop_event = FakeStopEvent()
    monkeypatch.setattr(
        "pgsync.web.sync_manager.SearchAdapter", DummySearchAdapter
    )
    monkeypatch.setattr(
        runner,
        "_ensure_resume_checkpoint",
        lambda: events.append("ensure_resume_checkpoint"),
    )
    monkeypatch.setattr(
        runner,
        "_prepare_native_incremental_start",
        lambda: (_ for _ in ()).throw(AssertionError("should not setup")),
    )
    monkeypatch.setattr(
        runner.assembly_runner,
        "full_sync",
        lambda search: (_ for _ in ()).throw(
            AssertionError("should not full sync")
        ),
    )
    monkeypatch.setattr(
        runner.assembly_runner,
        "incremental_sync",
        lambda search: events.append("assembly_incremental_sync"),
    )
    monkeypatch.setattr(
        runner,
        "_start_native_daemon",
        lambda **kwargs: events.append("start_native_daemon"),
    )
    monkeypatch.setattr(runner, "_terminate_process", lambda process: None)

    runner.run()

    runtime = store.get_job_runtime("job_1")

    assert events == [
        "ensure_index:demo_index",
        "ensure_resume_checkpoint",
        "start_native_daemon",
    ]
    assert runtime["resume_strategy"] == "cross_database_incremental"
    assert runtime["health_status"] == "degraded"


def test_cross_database_runner_resume_incremental_only_without_fallback_runs_immediate_recovery_cycle(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        "pgsync.web.sync_manager.RedisRootQueue.from_services",
        lambda services, job_id: FakeRootQueue(),
    )
    store = ConfigStore(str(tmp_path / "config.json"))
    store.save_services(
        {
            "search": {
                "enabled": True,
                "url": "http://localhost:9200",
                "username": "",
                "password": "",
                "verify_certs": False,
            },
            "redis": {
                "enabled": True,
                "host": "127.0.0.1",
                "port": 6379,
                "password": "",
                "db": 0,
            },
        }
    )
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_connection(
        {
            "id": "conn_2",
            "name": "db-b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "poll_interval_seconds": 1,
        "enable_assembly_fallback": False,
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
            {
                "id": "t2",
                "alias": "author",
                "connection_id": "conn_2",
                "schema": "public",
                "table": "author",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
        ],
        "relations": [
            {
                "id": "r1",
                "parent_table_id": "t1",
                "child_table_id": "t2",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
            }
        ],
    }
    runner = CrossDatabaseHybridRunner(
        manager,
        job,
        resume_incremental_only=True,
    )
    events = []

    class DummySearchAdapter:
        def __init__(self, services: dict) -> None:
            self.services = services

        def ensure_index(
            self,
            index_name: str,
            mapping: dict | None = None,
        ) -> None:
            events.append(f"ensure_index:{index_name}")

    class FakeStopEvent:
        @staticmethod
        def wait(interval: int) -> bool:
            return True

        @staticmethod
        def is_set() -> bool:
            return False

        @staticmethod
        def set() -> None:
            return None

    runner.stop_event = FakeStopEvent()
    monkeypatch.setattr(
        "pgsync.web.sync_manager.SearchAdapter", DummySearchAdapter
    )
    monkeypatch.setattr(
        runner,
        "_ensure_resume_checkpoint",
        lambda: events.append("ensure_resume_checkpoint"),
    )
    monkeypatch.setattr(
        runner,
        "_prepare_native_incremental_start",
        lambda: (_ for _ in ()).throw(AssertionError("should not setup")),
    )
    monkeypatch.setattr(
        runner.assembly_runner,
        "full_sync",
        lambda search: (_ for _ in ()).throw(
            AssertionError("should not full sync")
        ),
    )
    run_count = {"value": 0}

    def fake_run_assembly_incremental_cycle(search) -> bool:
        run_count["value"] += 1
        events.append("assembly_incremental_sync")
        runner.stop_requested = True
        return True

    monkeypatch.setattr(
        runner,
        "_run_assembly_incremental_cycle",
        fake_run_assembly_incremental_cycle,
    )
    monkeypatch.setattr(
        runner,
        "_start_native_daemon",
        lambda **kwargs: events.append("start_native_daemon"),
    )
    monkeypatch.setattr(runner, "_terminate_process", lambda process: None)

    runner.run()

    assert events == [
        "ensure_index:demo_index",
        "ensure_resume_checkpoint",
        "start_native_daemon",
        "assembly_incremental_sync",
    ]
    runtime = store.get_job_runtime("job_1")
    assert run_count["value"] == 1
    assert runtime["resume_strategy"] == "cross_database_incremental"
    assert runtime["status"] == "stopped"


def test_cross_database_runner_stop_requested_marks_auto_resume_phase(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        "pgsync.web.sync_manager.RedisRootQueue.from_services",
        lambda services, job_id: FakeRootQueue(),
    )
    store = ConfigStore(str(tmp_path / "config.json"))
    store.save_services(
        {
            "search": {
                "enabled": True,
                "url": "http://localhost:9200",
                "username": "",
                "password": "",
                "verify_certs": False,
            },
            "redis": {
                "enabled": True,
                "host": "127.0.0.1",
                "port": 6379,
                "password": "",
                "db": 0,
            },
        }
    )
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_connection(
        {
            "id": "conn_2",
            "name": "db-b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "poll_interval_seconds": 1,
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
            {
                "id": "t2",
                "alias": "author",
                "connection_id": "conn_2",
                "schema": "public",
                "table": "author",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
        ],
        "relations": [
            {
                "id": "r1",
                "parent_table_id": "t1",
                "child_table_id": "t2",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
            }
        ],
    }
    store.upsert_job(job)
    store.update_job_runtime(
        "job_1",
        {
            "desired_state": "running",
            "baseline_completed": True,
            "resume_strategy": "cross_database_incremental",
            "owner": manager.instance_id,
        },
    )
    runner = CrossDatabaseHybridRunner(
        manager,
        job,
        resume_incremental_only=True,
    )

    class DummySearchAdapter:
        def __init__(self, services: dict) -> None:
            self.services = services

        @staticmethod
        def ensure_index(
            index_name: str,
            mapping: dict | None = None,
        ) -> None:
            return None

    class FakeStopEvent:
        def __init__(self) -> None:
            self.calls = 0

        def wait(self, interval: int) -> bool:
            self.calls += 1
            runner.stop_requested = True
            return True

        @staticmethod
        def set() -> None:
            return None

        @staticmethod
        def is_set() -> bool:
            return False

    runner.stop_event = FakeStopEvent()
    monkeypatch.setattr(
        "pgsync.web.sync_manager.SearchAdapter", DummySearchAdapter
    )
    monkeypatch.setattr(runner, "_ensure_resume_checkpoint", lambda: None)
    monkeypatch.setattr(runner, "_start_native_daemon", lambda **kwargs: None)
    monkeypatch.setattr(
        runner.assembly_runner,
        "incremental_sync",
        lambda search: None,
    )
    monkeypatch.setattr(runner, "_terminate_process", lambda process: None)

    runner.run()

    runtime = store.get_job_runtime("job_1")
    assert runtime["status"] == "stopped"
    assert runtime["phase"] == "awaiting_incremental_resume"
    assert runtime["desired_state"] == "running"


def test_assembly_runner_incremental_only_skips_full_sync(
    tmp_path: Path,
    monkeypatch,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )

    class DummySearchAdapter:
        def __init__(self, services: dict) -> None:
            self.services = services

        def ensure_index(
            self,
            index_name: str,
            mapping: dict | None = None,
        ) -> None:
            events.append(f"ensure_index:{index_name}")

    class FakeStopEvent:
        @staticmethod
        def wait(interval: int) -> bool:
            return True

        @staticmethod
        def set() -> None:
            return None

    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            }
        ],
        "relations": [],
    }
    runner = AssemblyRunner(
        manager,
        job,
        incremental_only=True,
    )
    events = []

    monkeypatch.setattr(
        "pgsync.web.sync_manager.SearchAdapter", DummySearchAdapter
    )
    monkeypatch.setattr(
        runner,
        "full_sync",
        lambda search: (_ for _ in ()).throw(
            AssertionError("should not full sync")
        ),
    )
    monkeypatch.setattr(
        runner,
        "incremental_sync",
        lambda search: events.append("incremental_sync"),
    )
    runner.stop_event = FakeStopEvent()

    runner.run()

    runtime = store.get_job_runtime("job_1")

    assert events == ["ensure_index:demo_index", "incremental_sync"]
    assert runtime["baseline_completed"] is True
    assert runtime["resume_strategy"] == "assembly_incremental"


def test_assembly_incremental_recovers_from_confirmed_root_queue(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )

    class FakeDb:
        def __init__(self) -> None:
            self.changed_rows = [
                {
                    "id": 2,
                    "updated_at": 101,
                }
            ]
            self.fetch_calls: list[tuple] = []

        def fetch_changed_rows(
            self,
            connection: dict,
            schema: str,
            table: str,
            cursor_column: str,
            last_cursor,
            last_primary_key,
            primary_key: list[str],
            limit: int = 100,
        ):
            self.fetch_calls.append(
                (last_cursor, list(last_primary_key or []))
            )
            if not self.changed_rows:
                return [], None
            row = self.changed_rows[-1]
            return list(self.changed_rows), {
                "cursor": row[cursor_column],
                "pk": [row[column] for column in primary_key],
            }

        @staticmethod
        def fetch_rows_by_filter_groups(
            connection: dict,
            schema: str,
            table: str,
            filter_groups: list[dict[str, object]],
            fixed_filters=None,
            columns=None,
            order_by=None,
            limit=None,
        ):
            return [
                {"id": group["id"], "updated_at": 101}
                for group in filter_groups
            ]

        @staticmethod
        def fetch_rows_by_fields(
            connection: dict,
            schema: str,
            table: str,
            filters: dict,
            columns=None,
            order_by=None,
            limit=None,
        ):
            row_id = filters.get("id")
            if row_id is None:
                return []
            return [{"id": row_id, "updated_at": 101}]

    class FlakySearch:
        def __init__(self) -> None:
            self.bulk_calls = 0
            self.bulk_payloads: list[list[dict]] = []
            self.deletions: list[list[str]] = []

        def bulk(self, actions: list[dict]) -> None:
            self.bulk_calls += 1
            self.bulk_payloads.append(actions)
            if self.bulk_calls == 1:
                raise RuntimeError("boom")

        def delete_many(
            self, index_name: str, document_ids: list[str]
        ) -> None:
            self.deletions.append(document_ids)

    db = FakeDb()
    manager = SyncManager(store=store, db=db, repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            }
        ],
        "relations": [],
    }
    runner = AssemblyRunner(manager, job)
    store.update_job_runtime(
        "job_1",
        {
            "cursors": {
                "t1": {
                    "applied": {"cursor": 100, "pk": [1]},
                    "pending": None,
                }
            }
        },
    )
    search = FlakySearch()

    with pytest.raises(RuntimeError, match="boom"):
        runner.incremental_sync(search)

    runtime = store.get_job_runtime("job_1")
    assert db.fetch_calls == [(100, [1])]
    assert runtime["assembly_incremental_running"] is False
    assert runtime["assembly_root_queue"] == [[2]]
    assert runtime["assembly_root_queue_size"] == 1
    assert runtime["cursors"]["t1"]["applied"] == {
        "cursor": 101,
        "pk": [2],
    }
    assert runtime["cursors"]["t1"]["pending"] is None

    db.changed_rows = []
    runner.incremental_sync(search)

    runtime = store.get_job_runtime("job_1")
    assert runtime["assembly_root_queue"] == []
    assert runtime["assembly_root_queue_size"] == 0
    assert search.bulk_calls == 2
    assert search.bulk_payloads[-1][0]["_id"] == "2"


def test_assembly_incremental_replays_from_applied_checkpoint_when_pending_exists(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )

    class FakeDb:
        def __init__(self) -> None:
            self.fetch_calls: list[tuple] = []

        def fetch_changed_rows(
            self,
            connection: dict,
            schema: str,
            table: str,
            cursor_column: str,
            last_cursor,
            last_primary_key,
            primary_key: list[str],
            limit: int = 100,
        ):
            self.fetch_calls.append(
                (last_cursor, list(last_primary_key or []))
            )
            rows = [{"id": 2, "updated_at": 101}]
            return rows, {
                "cursor": 101,
                "pk": [2],
            }

        @staticmethod
        def fetch_rows_by_filter_groups(
            connection: dict,
            schema: str,
            table: str,
            filter_groups: list[dict[str, object]],
            fixed_filters=None,
            columns=None,
            order_by=None,
            limit=None,
        ):
            return [
                {"id": group["id"], "updated_at": 101}
                for group in filter_groups
            ]

        @staticmethod
        def fetch_rows_by_fields(
            connection: dict,
            schema: str,
            table: str,
            filters: dict,
            columns=None,
            order_by=None,
            limit=None,
        ):
            return [{"id": filters["id"], "updated_at": 101}]

    class Search:
        def __init__(self) -> None:
            self.bulk_payloads: list[list[dict]] = []

        def bulk(self, actions: list[dict]) -> None:
            self.bulk_payloads.append(actions)

        @staticmethod
        def delete_many(index_name: str, document_ids: list[str]) -> None:
            return None

    db = FakeDb()
    manager = SyncManager(store=store, db=db, repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            }
        ],
        "relations": [],
    }
    runner = AssemblyRunner(manager, job)
    store.update_job_runtime(
        "job_1",
        {
            "cursors": {
                "t1": {
                    "applied": {"cursor": 100, "pk": [1]},
                    "pending": {"cursor": 101, "pk": [2]},
                }
            }
        },
    )

    runner.incremental_sync(Search())

    runtime = store.get_job_runtime("job_1")
    assert db.fetch_calls == [(100, [1])]
    assert runtime["cursors"]["t1"]["applied"] == {
        "cursor": 101,
        "pk": [2],
    }
    assert runtime["cursors"]["t1"]["pending"] is None


def test_assembly_incremental_deduplicates_root_queue_by_root_id(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )

    class FakeDb:
        @staticmethod
        def fetch_changed_rows(
            connection: dict,
            schema: str,
            table: str,
            cursor_column: str,
            last_cursor,
            last_primary_key,
            primary_key: list[str],
            limit: int = 100,
        ):
            rows = [
                {"id": 1, "updated_at": 101},
                {"id": 2, "updated_at": 101},
            ]
            return rows, {
                "cursor": 101,
                "pk": [2],
            }

        @staticmethod
        def fetch_rows_by_filter_groups(
            connection: dict,
            schema: str,
            table: str,
            filter_groups: list[dict[str, object]],
            fixed_filters=None,
            columns=None,
            order_by=None,
            limit=None,
        ):
            return [
                {"id": group["id"], "updated_at": 101}
                for group in filter_groups
            ]

        @staticmethod
        def fetch_rows_by_fields(
            connection: dict,
            schema: str,
            table: str,
            filters: dict,
            columns=None,
            order_by=None,
            limit=None,
        ):
            return [{"id": filters["id"], "updated_at": 101}]

    class Search:
        def __init__(self) -> None:
            self.bulk_payloads: list[list[dict]] = []

        def bulk(self, actions: list[dict]) -> None:
            self.bulk_payloads.append(actions)

        @staticmethod
        def delete_many(index_name: str, document_ids: list[str]) -> None:
            return None

    manager = SyncManager(store=store, db=FakeDb(), repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "incremental_batch_size": 1,
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            }
        ],
        "relations": [],
    }
    runner = AssemblyRunner(manager, job)
    store.update_job_runtime(
        "job_1",
        {
            "cursors": {
                "t1": {
                    "applied": {"cursor": 100, "pk": [0]},
                    "pending": None,
                }
            },
            "assembly_root_queue": [[1]],
        },
    )
    search = Search()

    runner.incremental_sync(search)

    runtime = store.get_job_runtime("job_1")
    assert search.bulk_payloads[0][0]["_id"] == "1"
    assert runtime["assembly_root_queue"] == [[2]]
    assert runtime["assembly_root_queue_size"] == 1


def test_assembly_incremental_prefetches_one_hop_children_in_batch(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_connection(
        {
            "id": "conn_2",
            "name": "db-b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )

    class FakeDb:
        def __init__(self) -> None:
            self.batch_fetch_calls: list[dict] = []

        @staticmethod
        def fetch_changed_rows(
            connection: dict,
            schema: str,
            table: str,
            cursor_column: str,
            last_cursor,
            last_primary_key,
            primary_key: list[str],
            limit: int = 100,
        ):
            if table == "book":
                rows = [
                    {"id": 1, "updated_at": 101},
                    {"id": 2, "updated_at": 101},
                ]
                return rows, {
                    "cursor": 101,
                    "pk": [2],
                }
            return [], None

        def fetch_rows_by_filter_groups(
            self,
            connection: dict,
            schema: str,
            table: str,
            filter_groups: list[dict[str, object]],
            fixed_filters=None,
            columns=None,
            order_by=None,
            limit=None,
        ):
            self.batch_fetch_calls.append(
                {
                    "table": table,
                    "filter_groups": filter_groups,
                    "fixed_filters": fixed_filters,
                }
            )
            if table == "book":
                return [
                    {"id": 1, "title": "book-1", "updated_at": 101},
                    {"id": 2, "title": "book-2", "updated_at": 101},
                ]
            if table == "author":
                return [
                    {"id": 101, "book_id": 1, "name": "author-a"},
                    {"id": 102, "book_id": 1, "name": "author-b"},
                    {"id": 201, "book_id": 2, "name": "author-c"},
                ]
            raise AssertionError(f"unexpected table: {table}")

        @staticmethod
        def fetch_rows_by_fields(
            connection: dict,
            schema: str,
            table: str,
            filters: dict,
            columns=None,
            order_by=None,
            limit=None,
        ):
            raise AssertionError(
                "one-hop incremental rebuild should not use row-by-row fetches"
            )

    class Search:
        def __init__(self) -> None:
            self.bulk_payloads: list[list[dict]] = []

        def bulk(self, actions: list[dict]) -> None:
            self.bulk_payloads.append(actions)

        @staticmethod
        def delete_many(index_name: str, document_ids: list[str]) -> None:
            return None

    manager = SyncManager(store=store, db=FakeDb(), repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
            {
                "id": "t2",
                "alias": "author",
                "connection_id": "conn_2",
                "schema": "public",
                "table": "author",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
        ],
        "relations": [
            {
                "id": "r1",
                "parent_table_id": "t1",
                "child_table_id": "t2",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
                "fixed_conditions": [],
            }
        ],
    }
    runner = AssemblyRunner(manager, job)
    store.update_job_runtime(
        "job_1",
        {
            "cursors": {
                "t1": {
                    "applied": {"cursor": 100, "pk": [0]},
                    "pending": None,
                }
            }
        },
    )
    search = Search()

    runner.incremental_sync(search)

    runtime = store.get_job_runtime("job_1")
    assert len(manager.db.batch_fetch_calls) == 2
    assert manager.db.batch_fetch_calls[0]["table"] == "book"
    assert manager.db.batch_fetch_calls[0]["filter_groups"] == [
        {"id": 1},
        {"id": 2},
    ]
    assert manager.db.batch_fetch_calls[1]["table"] == "author"
    assert manager.db.batch_fetch_calls[1]["filter_groups"] == [
        {"book_id": 1},
        {"book_id": 2},
    ]
    assert search.bulk_payloads[0][0]["_source"]["author"] == [
        {"id": 101, "book_id": 1, "name": "author-a"},
        {"id": 102, "book_id": 1, "name": "author-b"},
    ]
    assert search.bulk_payloads[0][1]["_source"]["author"] == [
        {"id": 201, "book_id": 2, "name": "author-c"},
    ]
    assert runtime["assembly_root_queue"] == []
    assert runtime["assembly_root_queue_size"] == 0


def test_assembly_full_sync_prefetches_one_hop_children_in_batch(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_connection(
        {
            "id": "conn_2",
            "name": "db-b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )

    class FakeDb:
        def __init__(self) -> None:
            self.batch_fetch_calls: list[dict] = []
            self.row_fetch_calls: list[dict] = []
            self.page_calls = 0
            self.page_seek_calls: list[list[object] | None] = []

        def fetch_page(
            self,
            connection: dict,
            schema: str,
            table: str,
            order_by: list[str],
            limit: int,
            offset: int = 0,
            seek_after=None,
        ):
            self.page_calls += 1
            self.page_seek_calls.append(
                list(seek_after) if seek_after is not None else None
            )
            if self.page_calls == 1:
                return [
                    {"id": 1, "title": "book-1"},
                    {"id": 2, "title": "book-2"},
                ]
            return []

        def fetch_rows_by_filter_groups(
            self,
            connection: dict,
            schema: str,
            table: str,
            filter_groups: list[dict[str, object]],
            fixed_filters=None,
            columns=None,
            order_by=None,
            limit=None,
        ):
            self.batch_fetch_calls.append(
                {
                    "table": table,
                    "filter_groups": filter_groups,
                    "fixed_filters": fixed_filters,
                }
            )
            assert table == "author"
            return [
                {"id": 101, "book_id": 1, "name": "author-a"},
                {"id": 102, "book_id": 1, "name": "author-b"},
                {"id": 201, "book_id": 2, "name": "author-c"},
            ]

        def fetch_rows_by_fields(
            self,
            connection: dict,
            schema: str,
            table: str,
            filters: dict,
            columns=None,
            order_by=None,
            limit=None,
        ):
            self.row_fetch_calls.append({"table": table, "filters": filters})
            if filters == {}:
                if table == "book":
                    return [{"id": 2, "updated_at": 20}]
                return [{"id": 201, "updated_at": 30}]
            raise AssertionError(
                "one-hop batch full sync should not use row-by-row child fetches"
            )

    class Search:
        def __init__(self) -> None:
            self.bulk_payloads: list[list[dict]] = []

        def bulk(self, actions: list[dict]) -> None:
            self.bulk_payloads.append(actions)

    manager = SyncManager(store=store, db=FakeDb(), repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
            {
                "id": "t2",
                "alias": "author",
                "connection_id": "conn_2",
                "schema": "public",
                "table": "author",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
        ],
        "relations": [
            {
                "id": "r1",
                "parent_table_id": "t1",
                "child_table_id": "t2",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
                "fixed_conditions": [],
            }
        ],
    }
    runner = AssemblyRunner(manager, job)
    search = Search()

    runner.full_sync(search)

    runtime = store.get_job_runtime("job_1")
    assert len(manager.db.batch_fetch_calls) == 1
    assert manager.db.page_seek_calls == [None, [2]]
    assert manager.db.batch_fetch_calls[0]["filter_groups"] == [
        {"book_id": 1},
        {"book_id": 2},
    ]
    assert len(manager.db.row_fetch_calls) == 2
    assert search.bulk_payloads[0][0]["_source"]["author"] == [
        {"id": 101, "book_id": 1, "name": "author-a"},
        {"id": 102, "book_id": 1, "name": "author-b"},
    ]
    assert search.bulk_payloads[0][1]["_source"]["author"] == [
        {"id": 201, "book_id": 2, "name": "author-c"},
    ]
    assert runtime["document_count"] == 2
    assert runtime["phase"] == "assembly_incremental"


def test_assembly_full_sync_prefetches_one_hop_children_with_parent_fixed_condition_source(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_connection(
        {
            "id": "conn_2",
            "name": "db-b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )

    class FakeDb:
        def __init__(self) -> None:
            self.batch_fetch_calls: list[dict] = []
            self.row_fetch_calls: list[dict] = []
            self.page_calls = 0

        def fetch_page(
            self,
            connection: dict,
            schema: str,
            table: str,
            order_by: list[str],
            limit: int,
            offset: int = 0,
            seek_after=None,
        ):
            self.page_calls += 1
            if self.page_calls == 1:
                return [
                    {"id": 1, "title": "book-1", "buyer_id": 7},
                    {"id": 2, "title": "book-2", "buyer_id": 9},
                ]
            return []

        def fetch_rows_by_filter_groups(
            self,
            connection: dict,
            schema: str,
            table: str,
            filter_groups: list[dict[str, object]],
            fixed_filters=None,
            columns=None,
            order_by=None,
            limit=None,
        ):
            self.batch_fetch_calls.append(
                {
                    "table": table,
                    "filter_groups": filter_groups,
                    "fixed_filters": fixed_filters,
                }
            )
            rows: list[dict[str, object]] = []
            for group in filter_groups:
                if group["book_id"] == 1:
                    rows.append({"id": 101, "book_id": 1, "name": "author-a"})
                if group["book_id"] == 2:
                    rows.append({"id": 201, "book_id": 2, "name": "author-b"})
            return rows

        def fetch_rows_by_fields(
            self,
            connection: dict,
            schema: str,
            table: str,
            filters: dict,
            columns=None,
            order_by=None,
            limit=None,
        ):
            self.row_fetch_calls.append({"table": table, "filters": filters})
            if filters == {}:
                if table == "book":
                    return [{"id": 2, "updated_at": 20}]
                return [{"id": 201, "updated_at": 30}]
            raise AssertionError(
                "one-hop batch full sync should not use row-by-row child fetches"
            )

    class Search:
        def __init__(self) -> None:
            self.bulk_payloads: list[list[dict]] = []

        def bulk(self, actions: list[dict]) -> None:
            self.bulk_payloads.append(actions)

    manager = SyncManager(store=store, db=FakeDb(), repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
            {
                "id": "t2",
                "alias": "author",
                "connection_id": "conn_2",
                "schema": "public",
                "table": "author",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
        ],
        "relations": [
            {
                "id": "r1",
                "parent_table_id": "t1",
                "child_table_id": "t2",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
                "fixed_conditions": [
                    {
                        "field": "buyer_id",
                        "operator": "eq",
                        "value": 7,
                        "source": "parent",
                    }
                ],
            }
        ],
    }
    runner = AssemblyRunner(manager, job)
    search = Search()

    runner.full_sync(search)

    runtime = store.get_job_runtime("job_1")
    assert len(manager.db.batch_fetch_calls) == 1
    assert manager.db.batch_fetch_calls[0]["filter_groups"] == [
        {"book_id": 1},
    ]
    assert manager.db.batch_fetch_calls[0]["fixed_filters"] == {}
    assert search.bulk_payloads[0][0]["_source"]["author"] == [
        {"id": 101, "book_id": 1, "name": "author-a"},
    ]
    assert search.bulk_payloads[0][1]["_source"]["author"] == []
    assert runtime["document_count"] == 2
    assert runtime["phase"] == "assembly_incremental"


def test_assembly_resolve_root_keys_applies_parent_fixed_condition_source(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    for connection in (
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        },
        {
            "id": "conn_2",
            "name": "db-b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        },
    ):
        store.upsert_connection(connection)

    class FakeDb:
        @staticmethod
        def fetch_rows_by_fields(
            connection: dict,
            schema: str,
            table: str,
            filters: dict,
            columns=None,
            order_by=None,
            limit=None,
        ):
            assert table == "book"
            row_id = filters["id"]
            buyer_ids = {
                42: 7,
                84: 9,
            }
            return [{"id": row_id, "buyer_id": buyer_ids[row_id]}]

    manager = SyncManager(store=store, db=FakeDb(), repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
            {
                "id": "t2",
                "alias": "remark",
                "connection_id": "conn_2",
                "schema": "public",
                "table": "remark",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
        ],
        "relations": [
            {
                "id": "r1",
                "parent_table_id": "t1",
                "child_table_id": "t2",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
                "fixed_conditions": [
                    {
                        "field": "buyer_id",
                        "operator": "eq",
                        "value": 7,
                        "source": "parent",
                    }
                ],
            }
        ],
    }
    runner = AssemblyRunner(manager, job)

    assert runner.resolve_root_keys("t2", {"id": 10, "book_id": 42}) == {(42,)}
    assert runner.resolve_root_keys("t2", {"id": 11, "book_id": 84}) == set()


def test_assembly_full_sync_prefetches_multi_hop_children_in_batch(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_connection(
        {
            "id": "conn_2",
            "name": "db-b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )

    class FakeDb:
        def __init__(self) -> None:
            self.page_calls = 0
            self.page_seek_calls: list[list[object] | None] = []
            self.batch_fetch_calls: list[dict] = []
            self.row_fetch_calls: list[dict] = []

        def fetch_page(
            self,
            connection: dict,
            schema: str,
            table: str,
            order_by: list[str],
            limit: int,
            offset: int = 0,
            seek_after=None,
        ):
            self.page_calls += 1
            self.page_seek_calls.append(
                list(seek_after) if seek_after is not None else None
            )
            if self.page_calls == 1:
                return [{"id": 1, "title": "book-1"}]
            return []

        def fetch_rows_by_filter_groups(
            self,
            connection: dict,
            schema: str,
            table: str,
            filter_groups: list[dict[str, object]],
            fixed_filters=None,
            columns=None,
            order_by=None,
            limit=None,
        ):
            self.batch_fetch_calls.append(
                {
                    "table": table,
                    "filter_groups": filter_groups,
                    "fixed_filters": fixed_filters,
                }
            )
            if table == "author":
                return [{"id": 10, "book_id": 1, "name": "author-a"}]
            if table == "tag":
                return [{"id": 100, "author_id": 10, "name": "tag-a"}]
            raise AssertionError(f"unexpected batch fetch table: {table}")

        def fetch_rows_by_fields(
            self,
            connection: dict,
            schema: str,
            table: str,
            filters: dict,
            columns=None,
            order_by=None,
            limit=None,
        ):
            self.row_fetch_calls.append({"table": table, "filters": filters})
            if filters == {}:
                if table == "book":
                    return [{"id": 1, "updated_at": 10}]
                if table == "author":
                    return [{"id": 10, "updated_at": 20}]
                if table == "tag":
                    return [{"id": 100, "updated_at": 30}]
                return []
            raise AssertionError(
                "multi-hop batch full sync should not use row-by-row child fetches"
            )

    class Search:
        def __init__(self) -> None:
            self.bulk_payloads: list[list[dict]] = []

        def bulk(self, actions: list[dict]) -> None:
            self.bulk_payloads.append(actions)

    manager = SyncManager(store=store, db=FakeDb(), repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
            {
                "id": "t2",
                "alias": "author",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "author",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
            {
                "id": "t3",
                "alias": "tag",
                "connection_id": "conn_2",
                "schema": "public",
                "table": "tag",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
        ],
        "relations": [
            {
                "id": "r1",
                "parent_table_id": "t1",
                "child_table_id": "t2",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
                "fixed_conditions": [],
            },
            {
                "id": "r2",
                "parent_table_id": "t2",
                "child_table_id": "t3",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "author_id"},
                ],
                "fixed_conditions": [],
            },
        ],
    }
    runner = AssemblyRunner(manager, job)
    search = Search()

    runner.full_sync(search)

    runtime = store.get_job_runtime("job_1")
    assert runner.supports_batch_one_hop_full_sync() is False
    assert manager.db.page_seek_calls == [None, [1]]
    assert [call["table"] for call in manager.db.batch_fetch_calls] == [
        "author",
        "tag",
    ]
    assert search.bulk_payloads[0][0]["_source"]["author"] == [
        {
            "id": 10,
            "book_id": 1,
            "name": "author-a",
            "tag": [{"id": 100, "author_id": 10, "name": "tag-a"}],
        }
    ]
    assert runtime["document_count"] == 1
    assert runtime["phase"] == "assembly_incremental"


def test_cross_database_runner_skips_assembly_when_previous_batch_is_running(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setattr(
        "pgsync.web.sync_manager.RedisRootQueue.from_services",
        lambda services, job_id: FakeRootQueue(),
    )
    store = ConfigStore(str(tmp_path / "config.json"))
    store.save_services(
        {
            "search": {
                "enabled": True,
                "url": "http://localhost:9200",
                "username": "",
                "password": "",
                "verify_certs": False,
            },
            "redis": {
                "enabled": True,
                "host": "127.0.0.1",
                "port": 6379,
                "password": "",
                "db": 0,
            },
        }
    )
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_connection(
        {
            "id": "conn_2",
            "name": "db-b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "enable_assembly_fallback": True,
        "poll_interval_seconds": 1,
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
            {
                "id": "t2",
                "alias": "author",
                "connection_id": "conn_2",
                "schema": "public",
                "table": "author",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
        ],
        "relations": [
            {
                "id": "r1",
                "parent_table_id": "t1",
                "child_table_id": "t2",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
            }
        ],
    }
    runner = CrossDatabaseHybridRunner(manager, job)
    runner.root_queue.enqueue_hot([(1,)])
    runner.incremental_run_lock.acquire()
    events = []

    class DummySearchAdapter:
        def __init__(self, services: dict) -> None:
            self.services = services

        def ensure_index(
            self,
            index_name: str,
            mapping: dict | None = None,
        ) -> None:
            events.append(f"ensure_index:{index_name}")

    class FakeStopEvent:
        def __init__(self) -> None:
            self.calls = 0

        def wait(self, interval: int) -> bool:
            self.calls += 1
            return self.calls > 1

        @staticmethod
        def set() -> None:
            return None

        @staticmethod
        def is_set() -> bool:
            return False

    runner.stop_event = FakeStopEvent()

    monkeypatch.setattr(
        "pgsync.web.sync_manager.SearchAdapter", DummySearchAdapter
    )
    monkeypatch.setattr(
        runner,
        "_prepare_native_incremental_start",
        lambda: events.append("prepare_native_incremental_start"),
    )
    monkeypatch.setattr(
        runner,
        "_build_full_sync_cursor_seed",
        lambda: {},
    )

    def fake_full_sync(search, **kwargs):
        events.append("assembly_full_sync")
        runner.native_activity_event.set()

    monkeypatch.setattr(
        runner.assembly_runner,
        "full_sync",
        fake_full_sync,
    )
    monkeypatch.setattr(
        runner.assembly_runner,
        "incremental_sync",
        lambda search: events.append("assembly_incremental_sync"),
    )
    monkeypatch.setattr(
        runner,
        "_start_native_daemon",
        lambda **kwargs: events.append("start_native_daemon"),
    )
    monkeypatch.setattr(
        runner, "_start_recovery_scan_if_needed", lambda: False
    )
    monkeypatch.setattr(
        runner, "_maybe_request_idle_recovery_scan", lambda: None
    )

    try:
        runner.run()
    finally:
        runner.incremental_run_lock.release()

    runtime = store.get_job_runtime("job_1")
    assert events[:4] == [
        "ensure_index:demo_index",
        "prepare_native_incremental_start",
        "start_native_daemon",
        "assembly_full_sync",
    ]
    assert "assembly_incremental_sync" not in events
    assert runtime["status"] == "running"
    assert "Incremental rebuild cycle is still running" in "\n".join(
        runtime.get("logs", [])
    )


def test_cross_database_runner_drains_pending_root_queue_without_waiting(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setattr(
        "pgsync.web.sync_manager.RedisRootQueue.from_services",
        lambda services, job_id: FakeRootQueue(),
    )
    store = ConfigStore(str(tmp_path / "config.json"))
    store.save_services(
        {
            "search": {
                "enabled": True,
                "url": "http://localhost:9200",
                "username": "",
                "password": "",
                "verify_certs": False,
            },
            "redis": {
                "enabled": True,
                "host": "127.0.0.1",
                "port": 6379,
                "password": "",
                "db": 0,
            },
        }
    )
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_connection(
        {
            "id": "conn_2",
            "name": "db-b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "enable_assembly_fallback": True,
        "poll_interval_seconds": 60,
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
            {
                "id": "t2",
                "alias": "author",
                "connection_id": "conn_2",
                "schema": "public",
                "table": "author",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
        ],
        "relations": [
            {
                "id": "r1",
                "parent_table_id": "t1",
                "child_table_id": "t2",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
            }
        ],
    }
    runner = CrossDatabaseHybridRunner(
        manager,
        job,
        resume_incremental_only=True,
    )
    events = []

    class DummySearchAdapter:
        def __init__(self, services: dict) -> None:
            self.services = services

        def ensure_index(
            self,
            index_name: str,
            mapping: dict | None = None,
        ) -> None:
            events.append(f"ensure_index:{index_name}")

    class FakeStopEvent:
        def __init__(self) -> None:
            self.calls = 0

        def wait(self, interval: int) -> bool:
            self.calls += 1
            return True

        @staticmethod
        def is_set() -> bool:
            return False

        @staticmethod
        def set() -> None:
            return None

    runner.stop_event = FakeStopEvent()
    store.update_job_runtime("job_1", {"assembly_root_queue_size": 250})

    monkeypatch.setattr(
        "pgsync.web.sync_manager.SearchAdapter", DummySearchAdapter
    )
    monkeypatch.setattr(
        runner,
        "_ensure_resume_checkpoint",
        lambda: events.append("ensure_resume_checkpoint"),
    )
    monkeypatch.setattr(
        runner,
        "_start_native_daemon",
        lambda **kwargs: events.append("start_native_daemon"),
    )
    queue_sizes = [150, 50, 0]

    def fake_run_cycle(search) -> None:
        next_size = queue_sizes.pop(0)
        events.append(f"drain_to:{next_size}")
        store.update_job_runtime(
            "job_1",
            {"assembly_root_queue_size": next_size},
        )
        if next_size == 0:
            runner.stop_requested = True
        return True

    monkeypatch.setattr(
        runner, "_run_assembly_incremental_cycle", fake_run_cycle
    )
    monkeypatch.setattr(runner, "_terminate_process", lambda process: None)

    runner.run()

    runtime = store.get_job_runtime("job_1")
    assert events == [
        "ensure_index:demo_index",
        "ensure_resume_checkpoint",
        "start_native_daemon",
        "drain_to:150",
        "drain_to:50",
        "drain_to:0",
    ]
    assert queue_sizes == []
    assert runtime["status"] == "stopped"


def test_runtime_tuning_uses_job_override_before_global(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    manager.runtime_tuning = RuntimeTuningConfig()
    manager.runtime_tuning._payload = {
        "pgsync": {
            "assembly": {
                "rebuild_batch_size": 300,
                "parallel_enabled": True,
                "max_workers": 3,
                "worker_threshold_2": 5000,
                "worker_threshold_3": 10000,
            },
            "jobs": {
                "demo": {
                    "assembly_rebuild_batch_size": 500,
                    "max_workers": 2,
                }
            },
        }
    }
    manager.runtime_tuning._last_refresh_ts = 10**12
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "incremental_batch_size": 100,
    }

    batch_size, source = manager.runtime_rebuild_batch_size(job)
    workers, config = manager.runtime_assembly_worker_count(
        job,
        queue_size=12000,
        batch_size=batch_size,
    )

    assert batch_size == 500
    assert source == "nacos_job"
    assert workers == 2
    assert config["parallel_enabled"] is True


def test_runtime_tuning_defaults_keep_single_worker(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    manager.runtime_tuning._last_refresh_ts = 10**12
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "incremental_batch_size": 100,
    }

    batch_size, source = manager.runtime_rebuild_batch_size(job)
    workers, config = manager.runtime_assembly_worker_count(
        job,
        queue_size=20000,
        batch_size=batch_size,
    )

    assert batch_size == 100
    assert source == "job"
    assert workers == 1
    assert config["parallel_enabled"] is False


def test_cross_database_incremental_dedupes_and_parallelizes_batch(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setattr(
        "pgsync.web.sync_manager.RedisRootQueue.from_services",
        lambda services, job_id: FakeInflightRootQueue(),
    )
    store = ConfigStore(str(tmp_path / "config.json"))
    store.save_services(
        {
            "search": {
                "enabled": True,
                "url": "http://localhost:9200",
                "username": "",
                "password": "",
                "verify_certs": False,
            },
            "redis": {
                "enabled": True,
                "host": "127.0.0.1",
                "port": 6379,
                "password": "",
                "db": 0,
            },
        }
    )
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    manager.runtime_tuning._payload = {
        "pgsync": {
            "assembly": {
                "parallel_enabled": True,
                "rebuild_batch_size": 6,
                "max_workers": 3,
                "worker_threshold_2": 2,
                "worker_threshold_3": 4,
            }
        }
    }
    manager.runtime_tuning._last_refresh_ts = 10**12
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            }
        ],
        "relations": [],
    }
    runner = CrossDatabaseHybridRunner(
        manager,
        job,
        resume_incremental_only=True,
    )
    runner.root_queue.enqueue_hot([(1,), (2,), (2,), (3,), (4,), (5,)])
    built_chunks: list[list[tuple]] = []

    def fake_build(root_keys):
        built_chunks.append(list(root_keys))
        return [
            {
                "_op_type": "index",
                "_index": "demo_index",
                "_id": str(root_key[0]),
                "_source": {"id": root_key[0]},
            }
            for root_key in root_keys
        ], []

    class Search:
        def __init__(self) -> None:
            self.actions: list[dict] = []

        def bulk(self, actions: list[dict]) -> None:
            self.actions.extend(actions)

        @staticmethod
        def delete_many(index_name: str, document_ids: list[str]) -> None:
            return None

    search = Search()
    monkeypatch.setattr(
        runner.assembly_runner,
        "build_rebuild_actions_for_root_keys",
        fake_build,
    )

    assert runner._run_assembly_incremental_cycle(search) is True

    runtime = store.get_job_runtime("job_1")
    assert sorted(action["_id"] for action in search.actions) == [
        "1",
        "2",
        "3",
        "4",
        "5",
    ]
    assert len(built_chunks) == 3
    assert runtime["assembly_last_batch_raw_root_key_count"] == 5
    assert runtime["assembly_last_batch_deduped_root_key_count"] == 5
    assert runtime["assembly_last_batch_worker_count"] == 3
    assert runtime["assembly_last_batch_size_source"] == "nacos_global"


def test_cross_database_incremental_requeues_whole_batch_on_worker_failure(
    tmp_path: Path, monkeypatch
):
    queue = FakeInflightRootQueue()
    monkeypatch.setattr(
        "pgsync.web.sync_manager.RedisRootQueue.from_services",
        lambda services, job_id: queue,
    )
    store = ConfigStore(str(tmp_path / "config.json"))
    store.save_services(
        {
            "search": {"enabled": True, "url": "http://localhost:9200"},
            "redis": {"enabled": True, "host": "127.0.0.1", "port": 6379},
        }
    )
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    manager.runtime_tuning._payload = {
        "pgsync": {
            "assembly": {
                "parallel_enabled": True,
                "rebuild_batch_size": 3,
                "max_workers": 3,
                "worker_threshold_2": 1,
                "worker_threshold_3": 2,
            }
        }
    }
    manager.runtime_tuning._last_refresh_ts = 10**12
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            }
        ],
        "relations": [],
    }
    runner = CrossDatabaseHybridRunner(
        manager,
        job,
        resume_incremental_only=True,
    )
    runner.root_queue.enqueue_hot([(1,), (2,), (3,)])

    def fake_build(root_keys):
        if (2,) in root_keys:
            raise RuntimeError("boom")
        return [], []

    monkeypatch.setattr(
        runner.assembly_runner,
        "build_rebuild_actions_for_root_keys",
        fake_build,
    )

    with pytest.raises(RuntimeError, match="boom"):
        runner._run_assembly_incremental_cycle(object())

    assert queue.acked == []
    assert queue.requeued == ["fake-batch"]
    assert sorted(queue.hot) == [(1,), (2,), (3,)]


def test_single_database_runner_periodic_recovery_scan_runs_when_enabled(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        "pgsync.web.sync_manager.RedisRootQueue.from_services",
        lambda services, job_id: FakeRootQueue(),
    )
    store = ConfigStore(str(tmp_path / "config.json"))
    store.save_services(
        {
            "search": {
                "enabled": True,
                "url": "http://localhost:9200",
                "username": "",
                "password": "",
                "verify_certs": False,
            },
            "redis": {
                "enabled": True,
                "host": "127.0.0.1",
                "port": 6379,
                "password": "",
                "db": 0,
            },
        }
    )
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "enable_assembly_fallback": True,
        "poll_interval_seconds": 1,
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            }
        ],
        "relations": [],
    }
    runner = CrossDatabaseHybridRunner(
        manager,
        job,
        resume_incremental_only=True,
    )
    events = []

    class DummySearchAdapter:
        def __init__(self, services: dict) -> None:
            self.services = services

        def ensure_index(
            self,
            index_name: str,
            mapping: dict | None = None,
        ) -> None:
            events.append(f"ensure_index:{index_name}")

    class FakeStopEvent:
        def __init__(self) -> None:
            self.calls = 0

        def wait(self, interval: int) -> bool:
            self.calls += 1
            return self.calls > 1

        @staticmethod
        def is_set() -> bool:
            return False

        @staticmethod
        def set() -> None:
            return None

    runner.stop_event = FakeStopEvent()
    monkeypatch.setattr(
        "pgsync.web.sync_manager.SearchAdapter", DummySearchAdapter
    )
    monkeypatch.setattr(runner, "_ensure_resume_checkpoint", lambda: None)
    monkeypatch.setattr(
        runner,
        "_start_native_daemon",
        lambda **kwargs: events.append("start_native_daemon"),
    )
    monkeypatch.setattr(
        runner,
        "_start_recovery_scan_if_needed",
        lambda: (events.append("recovery_scan"), False)[-1],
    )
    monkeypatch.setattr(
        runner,
        "_run_assembly_incremental_cycle",
        lambda search: (events.append("assembly_cycle"), False)[-1],
    )

    runner.run()

    assert "recovery_scan" in events


def test_cross_database_runner_native_child_notification_enqueues_root_queue(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_connection(
        {
            "id": "conn_2",
            "name": "db-b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )

    class FakeDb:
        @staticmethod
        def fetch_rows_by_fields(
            connection: dict,
            schema: str,
            table: str,
            filters: dict,
            columns=None,
            order_by=None,
            limit=None,
        ):
            assert table == "book"
            return [{"id": filters["id"], "updated_at": 101}]

    manager = SyncManager(store=store, db=FakeDb(), repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
            {
                "id": "t2",
                "alias": "author",
                "connection_id": "conn_2",
                "schema": "public",
                "table": "author",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
        ],
        "relations": [
            {
                "id": "r1",
                "parent_table_id": "t1",
                "child_table_id": "t2",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
            }
        ],
    }
    runner = CrossDatabaseHybridRunner(manager, job)
    runner.root_queue = FakeRootQueue()

    runner._handle_native_notification(
        "conn_2",
        {
            "schema": "public",
            "table": "author",
            "tg_op": "UPDATE",
            "new": {"id": 10, "book_id": 42},
            "old": {"id": 10, "book_id": 41},
        },
    )

    runtime = store.get_job_runtime("job_1")
    assert sorted(runtime["assembly_root_queue"]) == [[41], [42]]
    assert runtime["assembly_root_queue_size"] == 2
    assert runtime["last_native_event_source"] == "public.author"


def test_cross_database_runner_native_root_insert_enqueues_root_key(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "book_collections_v3",
        "index_name": "book_collections_v3",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book_collections",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book_collections",
                "primary_key": ["id"],
                "cursor_column": "update_time",
            },
        ],
        "relations": [],
    }
    runner = CrossDatabaseHybridRunner(manager, job)
    runner.root_queue = FakeRootQueue()

    runner._handle_native_notification(
        "conn_1",
        {
            "schema": "public",
            "table": "book_collections",
            "tg_op": "INSERT",
            "new": {"id": 2062774561086492672},
            "old": None,
        },
    )

    runtime = store.get_job_runtime("job_1")
    assert runtime["assembly_root_queue"] == [[2062774561086492672]]
    assert runtime["assembly_root_queue_size"] == 1
    assert (
        runtime["last_native_event_source"] == "public.book_collections"
    )
    assert runtime["last_native_event_enqueued_count"] == 1
    assert runtime["last_native_event_root_key_count"] == 1
    assert runtime["last_native_event_root_keys"] == [[2062774561086492672]]
    assert isinstance(runtime["last_native_event_ts"], float)


def test_cross_database_runner_request_recovery_marks_runtime_degraded(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    store.update_job_runtime("job_1", {})
    runner = CrossDatabaseHybridRunner.__new__(CrossDatabaseHybridRunner)
    runner.manager = manager
    runner.store = store
    runner.job = {"id": "job_1"}
    runner.native_recovery_event = threading.Event()
    runner.native_listener_states = {}

    runner._request_recovery_scan("listener disconnected")

    runtime = store.get_job_runtime("job_1")
    assert runner.native_recovery_event.is_set() is True
    assert runtime["health_status"] == "degraded"
    assert runtime["health_reason"] == "listener disconnected"
    assert runtime["last_recovery_reason"] == "listener disconnected"


def test_cross_database_runner_idle_recovery_scan_uses_timestamp_field(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    runner = CrossDatabaseHybridRunner.__new__(CrossDatabaseHybridRunner)
    runner.manager = manager
    runner.store = store
    runner.job = {"id": "job_1"}
    runner.stop_requested = False
    runner.native_recovery_event = threading.Event()

    store.update_job_runtime(
        "job_1",
        {
            "assembly_recovery_scan_running": False,
            "last_native_event_at": "2026-06-01T10:00:00Z",
            "last_native_event_ts": 1.0,
        },
    )

    runner._native_idle_recovery_threshold_seconds = lambda: 10
    runner._iter_physical_recovery_groups = lambda: []

    runner._maybe_request_idle_recovery_scan()

    assert runner.native_recovery_event.is_set() is False


def test_cross_database_runner_notification_fans_out_all_aliases_for_same_table(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    for connection in (
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        },
        {
            "id": "conn_2",
            "name": "db-b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        },
    ):
        store.upsert_connection(connection)

    class FakeDb:
        @staticmethod
        def fetch_rows_by_fields(
            connection: dict,
            schema: str,
            table: str,
            filters: dict,
            columns=None,
            order_by=None,
            limit=None,
        ):
            assert table == "book"
            return [{"id": filters["id"], "updated_at": 101}]

    manager = SyncManager(store=store, db=FakeDb(), repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
            {
                "id": "t2",
                "alias": "remark_model",
                "connection_id": "conn_2",
                "schema": "public",
                "table": "remark",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
            {
                "id": "t3",
                "alias": "remark_business",
                "connection_id": "conn_2",
                "schema": "public",
                "table": "remark",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
        ],
        "relations": [
            {
                "id": "r1",
                "parent_table_id": "t1",
                "child_table_id": "t2",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
                "fixed_conditions": [
                    {"field": "type", "operator": "eq", "value": 1},
                ],
            },
            {
                "id": "r2",
                "parent_table_id": "t1",
                "child_table_id": "t3",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
                "fixed_conditions": [
                    {"field": "type", "operator": "eq", "value": 3},
                ],
            },
        ],
    }
    runner = CrossDatabaseHybridRunner(manager, job)
    runner.root_queue = FakeRootQueue()

    runner._handle_native_notification(
        "conn_2",
        {
            "schema": "public",
            "table": "remark",
            "tg_op": "UPDATE",
            "new": {"id": 10, "book_id": 42, "type": 1},
            "old": {"id": 10, "book_id": 41, "type": 1},
        },
    )
    runner._handle_native_notification(
        "conn_2",
        {
            "schema": "public",
            "table": "remark",
            "tg_op": "UPDATE",
            "new": {"id": 11, "book_id": 84, "type": 3},
            "old": {"id": 11, "book_id": 83, "type": 3},
        },
    )
    runner._handle_native_notification(
        "conn_2",
        {
            "schema": "public",
            "table": "remark",
            "tg_op": "UPDATE",
            "new": {"id": 12, "book_id": 999, "type": 2},
            "old": {"id": 12, "book_id": 998, "type": 2},
        },
    )

    runtime = store.get_job_runtime("job_1")
    assert sorted(runtime["assembly_root_queue"]) == [
        [41],
        [42],
        [83],
        [84],
    ]
    assert runtime["assembly_root_queue_size"] == 4


def test_cross_database_recovery_scan_groups_same_physical_table_and_fans_out(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    for connection in (
        {
            "id": "conn_1",
            "name": "db-a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        },
        {
            "id": "conn_2",
            "name": "db-b",
            "type": "postgresql",
            "host": "127.0.0.2",
            "port": 5432,
            "database": "postgres_b",
            "username": "postgres",
            "password": "secret",
            "url": "",
        },
    ):
        store.upsert_connection(connection)

    class FakeDb:
        def __init__(self) -> None:
            self.fetch_changed_rows_calls = []

        def fetch_changed_rows(
            self,
            connection: dict,
            schema: str,
            table: str,
            cursor_column: str,
            last_cursor,
            last_primary_key,
            primary_key,
            limit: int = 100,
        ):
            self.fetch_changed_rows_calls.append(
                (
                    schema,
                    table,
                    cursor_column,
                    last_cursor,
                    tuple(last_primary_key or []),
                    tuple(primary_key),
                    limit,
                )
            )
            if table == "remark":
                return (
                    [
                        {
                            "id": 10,
                            "book_id": 42,
                            "type": 1,
                            "updated_at": 101,
                        },
                        {
                            "id": 11,
                            "book_id": 84,
                            "type": 3,
                            "updated_at": 102,
                        },
                        {
                            "id": 12,
                            "book_id": 999,
                            "type": 2,
                            "updated_at": 103,
                        },
                    ],
                    {"cursor": 103, "pk": [12]},
                )
            return ([], None)

        @staticmethod
        def fetch_rows_by_fields(
            connection: dict,
            schema: str,
            table: str,
            filters: dict,
            columns=None,
            order_by=None,
            limit=None,
        ):
            assert table == "book"
            return [{"id": filters["id"], "updated_at": 201}]

    db = FakeDb()
    manager = SyncManager(store=store, db=db, repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
            {
                "id": "t2",
                "alias": "remark_model",
                "connection_id": "conn_2",
                "schema": "public",
                "table": "remark",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
            {
                "id": "t3",
                "alias": "remark_business",
                "connection_id": "conn_2",
                "schema": "public",
                "table": "remark",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            },
        ],
        "relations": [
            {
                "id": "r1",
                "parent_table_id": "t1",
                "child_table_id": "t2",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
                "fixed_conditions": [
                    {"field": "type", "operator": "eq", "value": 1},
                ],
            },
            {
                "id": "r2",
                "parent_table_id": "t1",
                "child_table_id": "t3",
                "type": "one_to_many",
                "field_pairs": [
                    {"parent_field": "id", "child_field": "book_id"},
                ],
                "fixed_conditions": [
                    {"field": "type", "operator": "eq", "value": 3},
                ],
            },
        ],
    }
    runner = CrossDatabaseHybridRunner(manager, job)
    runner.root_queue = FakeRootQueue()
    store.update_job_runtime(
        "job_1",
        {
            "cursors": {
                "t2": {"applied": {"cursor": 100, "pk": [9]}, "pending": None},
                "t3": {"applied": {"cursor": 100, "pk": [9]}, "pending": None},
            }
        },
    )

    result = runner._scan_recovery_changes()

    runtime = store.get_job_runtime("job_1")
    remark_calls = [
        call for call in db.fetch_changed_rows_calls if call[1] == "remark"
    ]
    assert len(remark_calls) == 1
    assert remark_calls[0][:3] == (
        "public",
        "remark",
        "updated_at",
    )
    assert result["changed_row_count"] == 3
    assert result["resolved_root_count"] == 2
    assert result["enqueued_count"] == 2
    assert sorted(runtime["assembly_root_queue"]) == [[42], [84]]
    assert runtime["cursors"]["t2"]["applied"]["cursor"] == 103
    assert runtime["cursors"]["t3"]["applied"]["cursor"] == 103


def test_durable_change_events_drain_enqueues_roots_and_advances_cursor(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )

    class FakeDb:
        def __init__(self) -> None:
            self.fetch_args = []

        def fetch_change_events(
            self,
            connection: dict,
            schema: str,
            *,
            table_names: list[str],
            after_id: int,
            limit: int = 100,
            exclude_ids=None,
        ):
            self.fetch_args.append(
                (
                    schema,
                    tuple(table_names),
                    after_id,
                    limit,
                    tuple(exclude_ids or []),
                )
            )
            return [
                {
                    "id": 101,
                    "schema_name": "public",
                    "table_name": "book",
                    "payload": {
                        "schema": "public",
                        "table": "book",
                        "tg_op": "INSERT",
                        "new": {"id": 42},
                    },
                },
                {
                    "id": 102,
                    "schema_name": "public",
                    "table_name": "book",
                    "payload": {
                        "schema": "public",
                        "table": "book",
                        "tg_op": "UPDATE",
                        "new": {"id": 42},
                        "old": {"id": 42},
                    },
                },
            ]

    db = FakeDb()
    manager = SyncManager(store=store, db=db, repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "enable_durable_change_events": True,
        "durable_change_event_batch_size": 500,
        "durable_change_event_replay_window": 50,
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            }
        ],
        "relations": [],
    }
    store.upsert_job(job)
    store.update_job_runtime(
        "job_1",
        {
            "native_change_event_cursors": {"conn_1:public": 100},
            "native_change_event_processed_ids": {"conn_1:public": [99, 100]},
        },
    )
    runner = CrossDatabaseHybridRunner(manager, job)
    runner.root_queue = FakeRootQueue()

    drained = runner._drain_durable_change_events()

    runtime = store.get_job_runtime("job_1")
    assert drained is True
    assert db.fetch_args == [("public", ("book",), 50, 501, (99, 100))]
    assert runtime["assembly_root_queue"] == [[42]]
    assert runtime["native_change_event_cursors"]["conn_1:public"] == 102
    assert runtime["native_change_event_processed_ids"]["conn_1:public"] == [
        99,
        100,
        101,
        102,
    ]
    assert runtime["durable_last_drain_event_count"] == 2
    assert runtime["durable_last_drain_root_key_count"] == 1
    assert runtime["durable_last_drain_enqueued_count"] == 1
    assert runtime["durable_event_lag"] == 0
    assert runtime["durable_event_lag_exact"] is True
    assert runtime["durable_event_backlog_present"] is False


def test_durable_change_events_drain_uses_lookahead_without_skipping_event(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )

    class FakeDb:
        def __init__(self) -> None:
            self.events = [
                {
                    "id": event_id,
                    "schema_name": "public",
                    "table_name": "book",
                    "payload": {
                        "schema": "public",
                        "table": "book",
                        "tg_op": "UPDATE",
                        "new": {"id": event_id},
                        "old": {"id": event_id},
                    },
                }
                for event_id in (101, 102, 103)
            ]

        def fetch_change_events(
            self,
            connection: dict,
            schema: str,
            *,
            table_names: list[str],
            after_id: int,
            limit: int = 100,
            exclude_ids=None,
        ):
            excluded = set(exclude_ids or [])
            return [
                event
                for event in self.events
                if event["id"] > after_id and event["id"] not in excluded
            ][:limit]

    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "enable_durable_change_events": True,
        "durable_change_event_batch_size": 2,
        "durable_change_event_replay_window": 50,
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            }
        ],
        "relations": [],
    }
    store.upsert_job(job)
    store.update_job_runtime(
        "job_1",
        {
            "native_change_event_cursors": {"conn_1:public": 100},
            "native_change_event_processed_ids": {"conn_1:public": [100]},
        },
    )
    manager = SyncManager(store=store, db=FakeDb(), repo_root=str(tmp_path))
    runner = CrossDatabaseHybridRunner(manager, job)
    runner.root_queue = FakeRootQueue()

    assert runner._drain_durable_change_events() is True

    runtime = store.get_job_runtime("job_1")
    assert runtime["native_change_event_cursors"]["conn_1:public"] == 102
    assert runtime["native_change_event_processed_ids"]["conn_1:public"] == [
        100,
        101,
        102,
    ]
    assert runtime["durable_event_lag"] == 1
    assert runtime["durable_event_lag_exact"] is False
    assert runtime["durable_event_backlog_present"] is True

    assert runner._drain_durable_change_events() is True

    runtime = store.get_job_runtime("job_1")
    assert runtime["native_change_event_cursors"]["conn_1:public"] == 103
    assert runtime["native_change_event_processed_ids"]["conn_1:public"] == [
        100,
        101,
        102,
        103,
    ]
    assert runtime["durable_event_lag"] == 0
    assert runtime["durable_event_lag_exact"] is True
    assert runtime["durable_event_backlog_present"] is False


def test_durable_change_events_drain_does_not_advance_cursor_on_fanout_error(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )

    class FakeDb:
        @staticmethod
        def fetch_change_events(
            connection: dict,
            schema: str,
            *,
            table_names: list[str],
            after_id: int,
            limit: int = 100,
            exclude_ids=None,
        ):
            return [
                {
                    "id": 101,
                    "schema_name": "public",
                    "table_name": "book",
                    "payload": {
                        "schema": "public",
                        "table": "book",
                        "tg_op": "INSERT",
                        "new": {"id": 42},
                    },
                }
            ]

    manager = SyncManager(store=store, db=FakeDb(), repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "enable_durable_change_events": True,
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
            }
        ],
        "relations": [],
    }
    store.upsert_job(job)
    store.update_job_runtime(
        "job_1",
        {
            "native_change_event_cursors": {"conn_1:public": 100},
            "native_change_event_processed_ids": {"conn_1:public": [100]},
        },
    )
    runner = CrossDatabaseHybridRunner(manager, job)
    runner.root_queue = FakeRootQueue()

    def broken_resolve(*args, **kwargs):
        return set(), ["boom"]

    runner._resolve_payload_root_keys = broken_resolve

    with pytest.raises(StudioError):
        runner._drain_durable_change_events()

    runtime = store.get_job_runtime("job_1")
    assert runner.root_queue.hot == []
    assert runtime.get("assembly_root_queue", []) == []
    assert runtime["native_change_event_cursors"]["conn_1:public"] == 100
    assert runtime["native_change_event_processed_ids"]["conn_1:public"] == [
        100
    ]


def test_database_config_store_migrates_file_snapshot(tmp_path: Path):
    seed_path = tmp_path / "config.json"
    file_store = ConfigStore(str(seed_path))
    file_store.save_services(
        {
            "search": {
                "url": "http://localhost:9200",
                "username": "elastic",
                "password": "secret",
                "verify_certs": False,
            },
            "redis": {
                "host": "127.0.0.1",
                "port": 6379,
                "password": "pwd",
                "db": 0,
            },
        }
    )
    file_store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    file_store.upsert_job(
        {
            "id": "job_1",
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "tables": [],
            "relations": [],
        }
    )
    file_store.update_job_runtime(
        "job_1",
        {
            "status": "running",
            "phase": "assembly_incremental",
            "desired_state": "running",
        },
    )
    file_store.append_job_log("job_1", "hello", limit=80)

    store = DatabaseConfigStore(
        f"sqlite:///{tmp_path / 'studio.db'}",
        base_dir=str(tmp_path),
        seed_config_path=str(seed_path),
    )

    snapshot = store.snapshot()

    assert snapshot["services"]["search"]["url"] == "http://localhost:9200"
    assert snapshot["connections"][0]["id"] == "conn_1"
    assert snapshot["jobs"][0]["id"] == "job_1"
    assert snapshot["job_runtime"]["job_1"]["phase"] == "assembly_incremental"
    assert snapshot["job_runtime"]["job_1"]["logs"] == ["hello"]


def test_create_store_uses_database_backend_when_env_present(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setenv(
        "PGSYNC_STUDIO_CONFIG_DATABASE_URL",
        f"sqlite:///{tmp_path / 'studio.db'}",
    )
    monkeypatch.setenv("PGSYNC_STUDIO_HOME", str(tmp_path))

    store = create_store()

    assert isinstance(store, DatabaseConfigStore)


def test_database_store_persists_service_credential_placeholders(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setenv("PGSYNC_STUDIO_SYNC_SERVICES_FROM_ENV", "true")
    monkeypatch.setenv("ELASTICSEARCH_URL", "http://search:9200")
    monkeypatch.setenv("ELASTICSEARCH_USERNAME", "search_user")
    monkeypatch.setenv("ELASTICSEARCH_PASSWORD", "search_secret")
    monkeypatch.setenv("REDIS_HOST", "redis")
    monkeypatch.setenv("REDIS_PASSWORD", "redis_secret")

    store = DatabaseConfigStore(
        f"sqlite:///{tmp_path / 'studio.db'}",
        base_dir=str(tmp_path),
    )
    services = store.snapshot()["services"]

    assert services["search"]["username"] == "${ELASTICSEARCH_USERNAME}"
    assert services["search"]["password"] == "${ELASTICSEARCH_PASSWORD}"
    assert services["redis"]["password"] == "${REDIS_PASSWORD}"
    assert "search_secret" not in json.dumps(services)
    assert "redis_secret" not in json.dumps(services)

    resolved = resolve_env_placeholders(services)
    assert resolved["search"]["username"] == "search_user"
    assert resolved["search"]["password"] == "search_secret"
    assert resolved["redis"]["password"] == "redis_secret"


def test_database_store_append_job_log_falls_back_when_replace_is_denied(
    tmp_path: Path,
    monkeypatch,
):
    store = DatabaseConfigStore(
        f"sqlite:///{tmp_path / 'studio.db'}",
        base_dir=str(tmp_path),
    )

    def raise_permission_error(src: str, dst: str) -> None:
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr(
        "pgsync.web.external_store.os.replace", raise_permission_error
    )

    logs = store.append_job_log("job_1", "hello", limit=80)

    assert logs == ["hello"]
    assert store.get_job_runtime("job_1").get("logs") == ["hello"]


def test_database_store_runtime_locks_do_not_block_other_jobs(tmp_path: Path):
    store = DatabaseConfigStore(
        f"sqlite:///{tmp_path / 'studio.db'}",
        base_dir=str(tmp_path),
    )
    slow_lock = store._job_lock("job_slow")
    slow_attempted = threading.Event()
    fast_finished = threading.Event()
    errors: list[BaseException] = []
    original_job_lock = store._job_lock

    def observed_job_lock(job_id: str):
        lock = original_job_lock(job_id)
        if job_id == "job_slow":
            slow_attempted.set()
        return lock

    store._job_lock = observed_job_lock  # type: ignore[method-assign]

    def update_slow_job() -> None:
        try:
            store.update_job_runtime("job_slow", {"status": "running"})
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    def update_fast_job() -> None:
        try:
            store.update_job_runtime("job_fast", {"status": "running"})
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)
        finally:
            fast_finished.set()

    slow_lock.acquire()
    slow_thread = threading.Thread(target=update_slow_job)
    fast_thread = threading.Thread(target=update_fast_job)
    try:
        slow_thread.start()
        assert slow_attempted.wait(timeout=1.0)
        fast_thread.start()
        assert fast_finished.wait(timeout=1.0)
    finally:
        slow_lock.release()
        slow_thread.join(timeout=2.0)
        fast_thread.join(timeout=2.0)

    assert not slow_thread.is_alive()
    assert not fast_thread.is_alive()
    assert errors == []
    assert store.get_job_runtime("job_slow")["status"] == "running"
    assert store.get_job_runtime("job_fast")["status"] == "running"


def test_database_store_runtime_updates_for_same_job_are_serialized(
    tmp_path: Path,
    monkeypatch,
):
    store = DatabaseConfigStore(
        f"sqlite:///{tmp_path / 'studio.db'}",
        base_dir=str(tmp_path),
    )
    first_read = threading.Event()
    allow_first_update = threading.Event()
    second_read = threading.Event()
    errors: list[BaseException] = []
    call_count = 0
    call_count_lock = threading.Lock()
    original_get_payload_row = store._get_payload_row

    def blocking_get_payload_row(conn, table, key_column, key_value):
        nonlocal call_count
        if table is store.runtime_table and key_value == "job_1":
            with call_count_lock:
                call_count += 1
                current_call = call_count
            if current_call == 1:
                first_read.set()
                assert allow_first_update.wait(timeout=2.0)
            elif current_call == 2:
                second_read.set()
        return original_get_payload_row(conn, table, key_column, key_value)

    monkeypatch.setattr(store, "_get_payload_row", blocking_get_payload_row)

    def update_runtime(patch: dict) -> None:
        try:
            store.update_job_runtime("job_1", patch)
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    first_thread = threading.Thread(
        target=update_runtime,
        args=({"status": "running"},),
    )
    second_thread = threading.Thread(
        target=update_runtime,
        args=({"phase": "cross_database_incremental"},),
    )
    first_thread.start()
    assert first_read.wait(timeout=1.0)
    second_thread.start()
    try:
        assert not second_read.wait(timeout=0.2)
    finally:
        allow_first_update.set()
        first_thread.join(timeout=2.0)
        second_thread.join(timeout=2.0)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert second_read.is_set()
    assert errors == []
    runtime = store.get_job_runtime("job_1")
    assert runtime["status"] == "running"
    assert runtime["phase"] == "cross_database_incremental"


def test_build_native_env_enables_redis_checkpoint(tmp_path: Path):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.save_services(
        {
            "search": {
                "url": "http://localhost:9200",
                "username": "elastic",
                "password": "secret",
                "verify_certs": False,
            },
            "redis": {
                "host": "127.0.0.1",
                "port": 6379,
                "password": "pwd",
                "db": 1,
            },
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))

    env = manager.build_native_env(
        {
            "id": "conn_1",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
        }
    )

    assert env["REDIS_CHECKPOINT"] == "true"
    assert env["REDIS_DB"] == "1"


def test_build_native_env_resolves_runtime_credentials(
    tmp_path: Path,
    monkeypatch,
):
    for username_env, password_env in db_module.SHARED_PG_CREDENTIAL_ENV_PAIRS:
        monkeypatch.delenv(username_env, raising=False)
        monkeypatch.delenv(password_env, raising=False)

    monkeypatch.setenv("PGSYNC_STUDIO_PG_USERNAME", "runtime_pg_user")
    monkeypatch.setenv("PGSYNC_STUDIO_PG_PASSWORD", "pg:$ecret")
    monkeypatch.setenv("ELASTICSEARCH_USERNAME", "runtime_search_user")
    monkeypatch.setenv("ELASTICSEARCH_PASSWORD", "search:$ecret")
    monkeypatch.setenv("REDIS_USER", "runtime_redis_user")
    monkeypatch.setenv("REDIS_PASSWORD", "redis:$ecret")

    store = ConfigStore(str(tmp_path / "config.json"))
    services = {
        "search": {
            "url": "http://localhost:9200",
            "username": "${ELASTICSEARCH_USERNAME}",
            "password": "${ELASTICSEARCH_PASSWORD}",
            "verify_certs": False,
        },
        "redis": {
            "host": "127.0.0.1",
            "port": 6379,
            "username": "${REDIS_USER}",
            "password": "${REDIS_PASSWORD}",
            "db": 1,
        },
    }
    store.save_services(services)
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    connection = {
        "id": "conn_1",
        "host": "127.0.0.1",
        "port": 5432,
        "database": "postgres",
        "username": MASKED_SHARED_CREDENTIAL,
        "password": "",
    }

    env = manager.build_native_env(connection)

    assert env["PG_USER"] == "runtime_pg_user"
    assert env["PG_PASSWORD"] == "pg:$ecret"
    assert env["ELASTICSEARCH_USERNAME"] == "runtime_search_user"
    assert env["ELASTICSEARCH_PASSWORD"] == "search:$ecret"
    assert env["ELASTICSEARCH_BASIC_AUTH"] == (
        "runtime_search_user:search:$ecret"
    )
    assert env["ELASTICSEARCH_HTTP_AUTH"] == (
        "runtime_search_user,search:$ecret"
    )
    assert env["REDIS_USER"] == "runtime_redis_user"
    assert env["REDIS_PASSWORD"] == "redis:$ecret"
    assert store.snapshot()["services"]["redis"]["password"] == (
        "${REDIS_PASSWORD}"
    )
    assert connection["username"] == MASKED_SHARED_CREDENTIAL
    assert connection["password"] == ""


def test_job_lease_defaults_use_new_values(monkeypatch):
    monkeypatch.delenv("PGSYNC_STUDIO_JOB_LEASE_TTL_SECONDS", raising=False)
    monkeypatch.delenv(
        "PGSYNC_STUDIO_JOB_LEASE_RENEW_INTERVAL_SECONDS",
        raising=False,
    )
    monkeypatch.delenv("PGSYNC_STUDIO_JOB_LEASE_KEY_PREFIX", raising=False)

    lease = NoopJobLease()

    assert DEFAULT_LEASE_TTL_SECONDS == 300
    assert DEFAULT_LEASE_RENEW_INTERVAL_SECONDS == 60
    assert lease.ttl_seconds == 300
    assert lease.renew_interval_seconds == 60
    assert resolve_job_lease_key_prefix() == DEFAULT_LEASE_KEY_PREFIX


def test_job_lease_env_overrides_take_effect(monkeypatch):
    monkeypatch.setenv("PGSYNC_STUDIO_JOB_LEASE_TTL_SECONDS", "480")
    monkeypatch.setenv(
        "PGSYNC_STUDIO_JOB_LEASE_RENEW_INTERVAL_SECONDS",
        "120",
    )
    monkeypatch.setenv(
        "PGSYNC_STUDIO_JOB_LEASE_KEY_PREFIX",
        "custom:studio:lease:",
    )

    lease = RedisJobLease(
        client=object(),
        job_id="job_1",
        owner_instance_id="studio-a",
    )

    assert resolve_job_lease_ttl_seconds() == 480
    assert resolve_job_lease_renew_interval_seconds(480) == 120
    assert resolve_job_lease_key_prefix() == "custom:studio:lease"
    assert lease.ttl_seconds == 480
    assert lease.renew_interval_seconds == 120
    assert lease.key == "custom:studio:lease:job_1"


def test_job_lease_renew_interval_is_clamped_below_ttl(monkeypatch):
    monkeypatch.setenv("PGSYNC_STUDIO_JOB_LEASE_TTL_SECONDS", "60")
    monkeypatch.setenv(
        "PGSYNC_STUDIO_JOB_LEASE_RENEW_INTERVAL_SECONDS",
        "300",
    )

    assert resolve_job_lease_ttl_seconds() == 60
    assert resolve_job_lease_renew_interval_seconds(60) == 59


def test_apply_nacos_config_sets_job_lease_envs(monkeypatch):
    monkeypatch.delenv("PGSYNC_STUDIO_JOB_LEASE_TTL_SECONDS", raising=False)
    monkeypatch.delenv(
        "PGSYNC_STUDIO_JOB_LEASE_RENEW_INTERVAL_SECONDS",
        raising=False,
    )
    monkeypatch.delenv("PGSYNC_STUDIO_JOB_LEASE_KEY_PREFIX", raising=False)

    apply_nacos_config(
        {
            "pgsync": {
                "studio": {
                    "job_lease_ttl_seconds": 420,
                    "job_lease_renew_interval_seconds": 90,
                    "job_lease_key_prefix": "nacos:job:lease",
                }
            }
        }
    )

    assert resolve_job_lease_ttl_seconds() == 420
    assert resolve_job_lease_renew_interval_seconds(420) == 90
    assert resolve_job_lease_key_prefix() == "nacos:job:lease"


def test_apply_nacos_config_sets_auto_discover_connections_time(monkeypatch):
    monkeypatch.delenv(
        "PGSYNC_STUDIO_AUTO_DISCOVER_CONNECTIONS_TIME",
        raising=False,
    )

    apply_nacos_config(
        {
            "pgsync": {
                "studio": {
                    "auto_discover_connections_time": "21:15",
                }
            }
        }
    )

    assert (
        os.environ["PGSYNC_STUDIO_AUTO_DISCOVER_CONNECTIONS_TIME"] == "21:15"
    )


def test_apply_nacos_config_sets_auto_discover_connections_excludes(
    monkeypatch,
):
    monkeypatch.delenv(
        "PGSYNC_STUDIO_AUTO_DISCOVER_CONNECTIONS_EXCLUDE_DATABASES",
        raising=False,
    )

    apply_nacos_config(
        {
            "pgsync": {
                "studio": {
                    "auto_discover_connections_exclude_databases": (
                        "postgres,template1,meta_db"
                    ),
                }
            }
        }
    )

    assert (
        os.environ["PGSYNC_STUDIO_AUTO_DISCOVER_CONNECTIONS_EXCLUDE_DATABASES"]
        == "postgres,template1,meta_db"
    )


def test_sync_manager_auto_discovery_time_defaults_and_overrides(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.delenv(
        "PGSYNC_STUDIO_AUTO_DISCOVER_CONNECTIONS_TIME",
        raising=False,
    )
    store = ConfigStore(str(tmp_path / "config.json"))
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))

    assert (
        manager.connection_auto_discovery_time_label
        == DEFAULT_AUTO_DISCOVER_CONNECTIONS_TIME
    )
    assert (
        manager._seconds_until_next_connection_auto_discovery_run(
            datetime(2026, 5, 21, 19, 0, 0)
        )
        == 3600
    )

    monkeypatch.setenv(
        "PGSYNC_STUDIO_AUTO_DISCOVER_CONNECTIONS_TIME",
        "21:30",
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    assert manager.connection_auto_discovery_time_label == "21:30"


def test_database_config_store_sync_instance_connections_adds_new_databases(
    tmp_path: Path,
    monkeypatch,
):
    store = DatabaseConfigStore(
        f"sqlite:///{tmp_path / 'studio.db'}",
        base_dir=str(tmp_path),
    )
    monkeypatch.setattr(
        store,
        "supports_connection_auto_discovery",
        lambda: True,
    )
    monkeypatch.setattr(
        store,
        "_config_instance_connection_template",
        lambda: {
            "host": "127.0.0.1",
            "port": 5432,
            "username": "postgres",
            "password": "secret",
            "database": "studio_meta",
        },
    )
    monkeypatch.setattr(
        store,
        "_list_instance_databases",
        lambda conn: ["biz_a", "biz_b", "studio_meta"],
    )
    store.upsert_connection(
        {
            "id": "manual_biz_a",
            "name": "biz_a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "biz_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )

    summary = store.sync_instance_connections()
    connections = store.list_connections()

    assert summary["ok"] is True
    assert summary["config_database"] == "studio_meta"
    assert summary["discovered_databases"] == ["biz_a", "biz_b"]
    assert summary["added_databases"] == ["biz_b"]
    assert len(connections) == 2
    assert {item["database"] for item in connections} == {"biz_a", "biz_b"}


def test_database_config_store_sync_instance_connections_excludes_configured_databases(
    tmp_path: Path,
    monkeypatch,
):
    store = DatabaseConfigStore(
        f"sqlite:///{tmp_path / 'studio.db'}",
        base_dir=str(tmp_path),
    )
    monkeypatch.setattr(
        store,
        "supports_connection_auto_discovery",
        lambda: True,
    )
    monkeypatch.setattr(
        store,
        "_config_instance_connection_template",
        lambda: {
            "host": "127.0.0.1",
            "port": 5432,
            "username": "postgres",
            "password": "secret",
            "database": "studio_meta",
        },
    )
    monkeypatch.setattr(
        store,
        "_list_instance_databases",
        lambda conn: [
            "biz_a",
            "postgres",
            "template1",
            "meta_db",
            "studio_meta",
        ],
    )
    monkeypatch.setenv(
        "PGSYNC_STUDIO_AUTO_DISCOVER_CONNECTIONS_EXCLUDE_DATABASES",
        "postgres, template1, meta_db",
    )

    summary = store.sync_instance_connections()
    connections = store.list_connections()

    assert summary["ok"] is True
    assert summary["excluded_databases"] == [
        "meta_db",
        "postgres",
        "template0",
        "template1",
    ]
    assert summary["discovered_databases"] == ["biz_a"]
    assert summary["added_databases"] == ["biz_a"]
    assert len(connections) == 1
    assert connections[0]["database"] == "biz_a"


def test_database_config_store_sync_instance_connections_uses_default_excludes(
    tmp_path: Path,
    monkeypatch,
):
    store = DatabaseConfigStore(
        f"sqlite:///{tmp_path / 'studio.db'}",
        base_dir=str(tmp_path),
    )
    monkeypatch.setattr(
        store,
        "supports_connection_auto_discovery",
        lambda: True,
    )
    monkeypatch.setattr(
        store,
        "_config_instance_connection_template",
        lambda: {
            "host": "127.0.0.1",
            "port": 5432,
            "username": "postgres",
            "password": "secret",
            "database": "studio_meta",
        },
    )
    monkeypatch.setattr(
        store,
        "_list_instance_databases",
        lambda conn: [
            "biz_a",
            "postgres",
            "template0",
            "template1",
            "studio_meta",
        ],
    )
    monkeypatch.delenv(
        "PGSYNC_STUDIO_AUTO_DISCOVER_CONNECTIONS_EXCLUDE_DATABASES",
        raising=False,
    )

    summary = store.sync_instance_connections()

    assert summary["ok"] is True
    assert summary["excluded_databases"] == [
        "postgres",
        "template0",
        "template1",
    ]
    assert summary["discovered_databases"] == ["biz_a"]
    assert summary["added_databases"] == ["biz_a"]


def test_database_config_store_sync_instance_connections_respects_startup_snapshot(
    tmp_path: Path,
    monkeypatch,
):
    store = DatabaseConfigStore(
        f"sqlite:///{tmp_path / 'studio.db'}",
        base_dir=str(tmp_path),
    )
    monkeypatch.setattr(
        store,
        "supports_connection_auto_discovery",
        lambda: True,
    )
    monkeypatch.setattr(
        store,
        "_config_instance_connection_template",
        lambda: {
            "host": "127.0.0.1",
            "port": 5432,
            "username": "postgres",
            "password": "secret",
            "database": "studio_meta",
        },
    )
    monkeypatch.setattr(
        store,
        "_list_instance_databases",
        lambda conn: ["biz_a", "biz_b", "studio_meta"],
    )
    store.upsert_connection(
        {
            "id": "manual_biz_a",
            "name": "biz_a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "biz_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )

    startup_snapshot = store.list_instance_connection_databases()
    assert startup_snapshot == {"biz_a"}

    assert store.delete_connection("manual_biz_a") is True

    summary = store.sync_instance_connections(
        extra_excluded_databases=startup_snapshot
    )

    assert summary["ok"] is True
    assert "biz_a" in summary["excluded_databases"]
    assert summary["discovered_databases"] == ["biz_b"]
    assert summary["added_databases"] == ["biz_b"]
    assert {item["database"] for item in store.list_connections()} == {"biz_b"}


def test_connection_auto_discovery_scheduler_runs_immediately_on_startup(
    tmp_path: Path,
    monkeypatch,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    triggered = threading.Event()

    monkeypatch.setattr(
        manager,
        "connection_auto_discovery_supported",
        lambda: True,
    )
    monkeypatch.setattr(
        manager,
        "_seconds_until_next_connection_auto_discovery_run",
        lambda now=None: 86400.0,
    )

    def fake_cycle() -> dict:
        triggered.set()
        manager._connection_auto_discovery_stop_event.set()
        return {"ok": True, "supported": True}

    monkeypatch.setattr(
        manager,
        "run_connection_auto_discovery_cycle",
        fake_cycle,
    )

    manager.start_connection_auto_discovery_scheduler()

    assert triggered.wait(1.0) is True
    manager.stop_connection_auto_discovery_scheduler()


def test_connection_auto_discovery_scheduler_snapshots_existing_databases(
    tmp_path: Path,
    monkeypatch,
):
    store = DatabaseConfigStore(
        f"sqlite:///{tmp_path / 'studio.db'}",
        base_dir=str(tmp_path),
    )
    monkeypatch.setattr(
        store,
        "supports_connection_auto_discovery",
        lambda: True,
    )
    monkeypatch.setattr(
        store,
        "_config_instance_connection_template",
        lambda: {
            "host": "127.0.0.1",
            "port": 5432,
            "username": "postgres",
            "password": "secret",
            "database": "studio_meta",
        },
    )
    store.upsert_connection(
        {
            "id": "manual_biz_a",
            "name": "biz_a",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "biz_a",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    triggered = threading.Event()
    captured: dict[str, set[str]] = {}

    monkeypatch.setattr(
        manager,
        "_seconds_until_next_connection_auto_discovery_run",
        lambda now=None: 86400.0,
    )

    def fake_sync_instance_connections(
        *,
        extra_excluded_databases=None,
    ) -> dict:
        captured["databases"] = set(extra_excluded_databases or [])
        triggered.set()
        manager._connection_auto_discovery_stop_event.set()
        return {"ok": True, "supported": True, "added_databases": []}

    monkeypatch.setattr(
        store,
        "sync_instance_connections",
        fake_sync_instance_connections,
    )

    manager.start_connection_auto_discovery_scheduler()

    assert triggered.wait(1.0) is True
    assert captured["databases"] == {"biz_a"}
    manager.stop_connection_auto_discovery_scheduler()


def test_stop_job_rejects_remote_owner(tmp_path: Path):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_job(
        {
            "id": "job_1",
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "tables": [],
            "relations": [],
        }
    )
    store.update_job_runtime(
        "job_1",
        {
            "status": "running",
            "phase": "assembly_incremental",
            "owner": "other-studio",
            "desired_state": "running",
        },
    )
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(
        SyncManager,
        "describe_job_holder",
        lambda self, job_id, services=None: {
            "owner_instance_id": "other-studio"
        },
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))

    try:
        with pytest.raises(Exception):
            manager.stop_job("job_1")
    finally:
        monkeypatch.undo()


def test_stop_job_allows_stale_remote_owner_without_active_lease(
    tmp_path: Path,
    monkeypatch,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_job(
        {
            "id": "job_1",
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "tables": [],
            "relations": [],
        }
    )
    store.update_job_runtime(
        "job_1",
        {
            "status": "running",
            "phase": "assembly_incremental",
            "owner": "other-studio",
            "desired_state": "running",
        },
    )
    monkeypatch.setattr(
        SyncManager,
        "reconcile_runtime_states",
        lambda self: [],
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    monkeypatch.setattr(
        manager, "describe_job_holder", lambda *args, **kwargs: {}
    )

    runtime = manager.stop_job("job_1")

    assert runtime["status"] == "running" or runtime["status"] == "stopped"
    refreshed = store.get_job_runtime("job_1")
    assert refreshed["desired_state"] == "stopped"
    assert refreshed["owner"] is None


def test_prepare_for_shutdown_marks_resumable_job_for_auto_resume(
    tmp_path: Path,
):
    class DummyThread:
        def is_alive(self) -> bool:
            return False

        def join(self, timeout: float | None = None) -> None:
            return None

    class DummyRunner:
        def __init__(self) -> None:
            self.thread = DummyThread()
            self.stop_called = False

        def stop(self) -> None:
            self.stop_called = True

    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_job(
        {
            "id": "job_1",
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "tables": [
                {
                    "id": "t1",
                    "alias": "book",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "book",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                }
            ],
            "relations": [],
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    store.update_job_runtime(
        "job_1",
        {
            "status": "running",
            "phase": "assembly_incremental",
            "desired_state": "running",
            "baseline_completed": True,
            "resume_strategy": "native_incremental",
            "owner": manager.instance_id,
        },
    )
    runner = DummyRunner()
    manager.runners["job_1"] = runner

    manager.prepare_for_shutdown(join_timeout_seconds=0.01)

    runtime = store.get_job_runtime("job_1")
    assert runner.stop_called is True
    assert runtime["status"] == "stopped"
    assert runtime["phase"] == "awaiting_incremental_resume"
    assert runtime["desired_state"] == "running"
    assert runtime["owner"] is None
    assert "job_1" not in manager.runners


def test_save_job_resets_baseline_when_index_name_changes(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
                "soft_delete_column": "",
            }
        ],
        "relations": [],
    }
    store.upsert_job(job)
    store.update_job_runtime(
        "job_1",
        {
            "status": "stopped",
            "phase": "awaiting_incremental_resume",
            "baseline_completed": True,
            "resume_strategy": "native_incremental",
            "document_count": 321,
            "cursors": {"t1": {"applied": {"cursor": "2026-05-27T00:00:00"}}},
            "last_full_sync_at": "2026-05-27T00:00:00+00:00",
            "last_incremental_sync_at": "2026-05-27T01:00:00+00:00",
            "native_setup_at": "2026-05-27T01:05:00+00:00",
            "assembly_recovery_requires_full_sync": True,
        },
    )

    updated = dict(job)
    updated["index_name"] = "demo_index_v2"

    result = manager.save_job(updated, start=False)
    runtime = result["runtime"]

    assert runtime["baseline_completed"] is False
    assert runtime["document_count"] == 0
    assert runtime["cursors"] == {}
    assert runtime["last_full_sync_at"] is None
    assert runtime["last_incremental_sync_at"] is None
    assert runtime["native_setup_at"] is None
    assert runtime["assembly_recovery_requires_full_sync"] is False
    assert runtime["resume_strategy"] == "native_incremental"
    assert runtime["durable_event_lag_exact"] is False
    assert runtime["durable_event_lag_observed_at"] is None


def test_save_job_keeps_baseline_when_only_name_changes(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
                "soft_delete_column": "",
            }
        ],
        "relations": [],
    }
    store.upsert_job(job)
    store.update_job_runtime(
        "job_1",
        {
            "status": "stopped",
            "phase": "awaiting_incremental_resume",
            "baseline_completed": True,
            "resume_strategy": "native_incremental",
            "document_count": 321,
        },
    )

    updated = dict(job)
    updated["name"] = "demo_v2"

    result = manager.save_job(updated, start=False)
    runtime = result["runtime"]

    assert runtime["baseline_completed"] is True
    assert runtime["document_count"] == 321
    assert runtime["resume_strategy"] == "native_incremental"


def test_save_job_rejects_edit_when_existing_index_exists(
    tmp_path: Path,
    monkeypatch,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    services = default_config()["services"]
    services["search"]["url"] = "http://search.test:9200"
    store.save_services(services)
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
                "soft_delete_column": "",
            }
        ],
        "relations": [],
    }
    store.upsert_job(job)
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))

    class DummySearchAdapter:
        def __init__(self, services: dict) -> None:
            self.services = services

        def index_exists(self, index_name: str) -> bool:
            return index_name == "demo_index"

    monkeypatch.setattr(
        "pgsync.web.sync_manager.SearchAdapter", DummySearchAdapter
    )

    updated = dict(job)
    updated["name"] = "demo_v2"

    with pytest.raises(StudioError, match="任务已锁定"):
        manager.save_job(updated, start=False)


def test_save_job_rejects_duplicate_job_name_and_index_name(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    store.upsert_job(
        {
            "id": "job_1",
            "name": "demo",
            "index_name": "demo_index",
            "root_table_id": "t1",
            "tables": [
                {
                    "id": "t1",
                    "alias": "book",
                    "connection_id": "conn_1",
                    "schema": "public",
                    "table": "book",
                    "primary_key": ["id"],
                    "cursor_column": "updated_at",
                    "soft_delete_column": "",
                }
            ],
            "relations": [],
        }
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))

    with pytest.raises(StudioError, match="任务名称 demo 已存在"):
        manager.save_job(
            {
                "name": "demo",
                "index_name": "demo_index_v2",
                "tables": [
                    {
                        "id": "t2",
                        "alias": "book_copy",
                        "connection_id": "conn_1",
                        "schema": "public",
                        "table": "book",
                        "primary_key": ["id"],
                        "cursor_column": "updated_at",
                        "soft_delete_column": "",
                    }
                ],
                "relations": [],
            },
            start=False,
        )

    with pytest.raises(
        StudioError, match="索引名称 demo_index 已被任务 demo 使用"
    ):
        manager.save_job(
            {
                "name": "demo_v2",
                "index_name": "demo_index",
                "tables": [
                    {
                        "id": "t2",
                        "alias": "book_copy",
                        "connection_id": "conn_1",
                        "schema": "public",
                        "table": "book",
                        "primary_key": ["id"],
                        "cursor_column": "updated_at",
                        "soft_delete_column": "",
                    }
                ],
                "relations": [],
            },
            start=False,
        )


def test_copy_job_creates_new_saved_job_without_runtime_inheritance(
    tmp_path: Path,
):
    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    source_job = {
        "id": "job_1",
        "name": "demo",
        "index_name": "demo_index",
        "root_table_id": "t1",
        "tables": [
            {
                "id": "t1",
                "alias": "book",
                "connection_id": "conn_1",
                "schema": "public",
                "table": "book",
                "primary_key": ["id"],
                "cursor_column": "updated_at",
                "soft_delete_column": "",
            }
        ],
        "relations": [],
    }
    store.upsert_job(source_job)
    store.update_job_runtime(
        "job_1",
        {
            "status": "stopped",
            "phase": "awaiting_incremental_resume",
            "baseline_completed": True,
            "resume_strategy": "native_incremental",
            "document_count": 321,
            "cursors": {"t1": {"applied": {"cursor": "2026-05-27T00:00:00"}}},
        },
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))

    result = manager.copy_job(
        "job_1",
        {
            "name": "demo_copy",
            "index_name": "demo_index_copy",
        },
    )

    copied_job = result["job"]
    copied_runtime = result["runtime"]

    assert result["source_job_id"] == "job_1"
    assert copied_job["id"] != "job_1"
    assert copied_job["name"] == "demo_copy"
    assert copied_job["index_name"] == "demo_index_copy"
    assert copied_runtime["status"] == "saved"
    assert copied_runtime["desired_state"] == "stopped"
    assert copied_runtime["baseline_completed"] is False
    assert copied_runtime["document_count"] == 0
    assert copied_runtime["cursors"] == {}
    assert copied_runtime["resume_strategy"] == "native_incremental"


def test_prepare_for_shutdown_stops_all_runners_before_waiting(
    tmp_path: Path,
    monkeypatch,
):
    class BlockingThread:
        def __init__(self, assert_before_join) -> None:
            self.assert_before_join = assert_before_join
            self.join_called = False

        def is_alive(self) -> bool:
            return not self.join_called

        def join(self, timeout: float | None = None) -> None:
            self.assert_before_join()
            self.join_called = True

    class FinishedThread:
        @staticmethod
        def is_alive() -> bool:
            return False

        @staticmethod
        def join(timeout: float | None = None) -> None:
            return None

    class DummyRunner:
        def __init__(self, thread) -> None:
            self.thread = thread
            self.stop_called = False

        def stop(self) -> None:
            self.stop_called = True

    store = ConfigStore(str(tmp_path / "config.json"))
    store.upsert_connection(
        {
            "id": "conn_1",
            "name": "demo",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "secret",
            "url": "",
        }
    )
    for job_id in ("job_1", "job_2"):
        store.upsert_job(
            {
                "id": job_id,
                "name": job_id,
                "index_name": f"{job_id}_index",
                "root_table_id": "t1",
                "tables": [
                    {
                        "id": "t1",
                        "alias": "book",
                        "connection_id": "conn_1",
                        "schema": "public",
                        "table": "book",
                        "primary_key": ["id"],
                        "cursor_column": "updated_at",
                    }
                ],
                "relations": [],
            }
        )
        store.update_job_runtime(
            job_id,
            {
                "status": "running",
                "phase": "assembly_incremental",
                "desired_state": "running",
                "baseline_completed": True,
                "resume_strategy": "assembly_incremental",
            },
        )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    monkeypatch.setattr(
        manager,
        "runtime_should_resume_incremental",
        lambda job_id, runtime: True,
    )
    runner_2 = DummyRunner(FinishedThread())

    def assert_second_runner_already_stopped() -> None:
        assert runner_2.stop_called is True

    runner_1 = DummyRunner(
        BlockingThread(assert_second_runner_already_stopped)
    )
    manager.runners["job_1"] = runner_1
    manager.runners["job_2"] = runner_2

    manager.prepare_for_shutdown(join_timeout_seconds=0.01)

    assert runner_1.stop_called is True
    assert runner_2.stop_called is True
