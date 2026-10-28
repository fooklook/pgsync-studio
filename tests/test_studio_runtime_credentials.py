"""Masked connections must work for capture setup and LISTEN reconnects."""

import threading
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

import pgsync.web.db as db_module
import pgsync.web.sync_manager as module
from pgsync.web.sync_manager import CrossDatabaseHybridRunner, SyncManager
from tests.test_studio import build_failed_recovery_store


@pytest.fixture
def masked_runner(tmp_path, monkeypatch):
    for user_key, password_key in db_module.SHARED_PG_CREDENTIAL_ENV_PAIRS:
        monkeypatch.delenv(user_key, raising=False)
        monkeypatch.delenv(password_key, raising=False)
    for key in db_module.CONFIG_DATABASE_URL_ENV_NAMES:
        monkeypatch.delenv(key, raising=False)

    store = build_failed_recovery_store(tmp_path)
    db = SimpleNamespace(test_connection=lambda connection: {"ok": True})
    manager = SyncManager(store=store, db=db, repo_root=str(tmp_path))
    manager.save_connection(
        {
            "id": "conn_1",
            "name": "new connection",
            "host": "primary-host",
            "port": 5432,
            "database": "postgres",
        }
    )
    runner = CrossDatabaseHybridRunner(manager, store.get_job("job_1"))
    return runner, store


def configure_shared_credentials(monkeypatch, source):
    if source == "config_url":
        monkeypatch.setenv(
            "PGSYNC_STUDIO_CONFIG_DATABASE_URL",
            "postgresql://runtime_user:runtime%40pass@config-host/studio",
        )
    else:
        user_key, password_key = {
            "studio": (
                "PGSYNC_STUDIO_PG_USERNAME",
                "PGSYNC_STUDIO_PG_PASSWORD",
            ),
            "pgsql": ("PGSQL_USERNAME", "PGSQL_PASSWORD"),
        }[source]
        monkeypatch.setenv(user_key, "runtime_user")
        monkeypatch.setenv(password_key, "runtime@pass")


@pytest.mark.parametrize("source", ["studio", "pgsql", "config_url"])
def test_native_setup_uses_shared_credentials_for_new_masked_connection(
    masked_runner, monkeypatch, source
):
    runner, store = masked_runner
    configure_shared_credentials(monkeypatch, source)
    calls = []

    class NativeBase:
        def __init__(self, database, **kwargs):
            calls.append((database, kwargs))
            self.engine = SimpleNamespace(dispose=lambda: None)

        @contextmanager
        def advisory_lock(self, *args, **kwargs):
            yield

        def ensure_function(self, *args):
            pass

        def create_view(self, *args):
            pass

        def create_triggers(self, *args, **kwargs):
            pass

    monkeypatch.setattr(module, "PGSyncBase", NativeBase)
    runner._prepare_native_incremental_start()

    assert calls == [
        (
            "postgres",
            {
                "host": "primary-host",
                "port": 5432,
                "user": "runtime_user",
                "password": "runtime@pass",
            },
        )
    ]
    stored = store.get_connection("conn_1")
    assert stored["username"] == db_module.MASKED_SHARED_CREDENTIAL
    assert stored["password"] == ""


@pytest.mark.parametrize("rotate_password", [False, True])
def test_listener_injects_credentials_on_each_connection_attempt(
    masked_runner, monkeypatch, rotate_password
):
    runner, store = masked_runner
    configure_shared_credentials(monkeypatch, "studio")
    stop = threading.Event()
    attempts = []
    executed = []
    closed = []
    cursor = SimpleNamespace(execute=executed.append, close=lambda: None)
    connection = SimpleNamespace(
        set_session=lambda **kwargs: None,
        cursor=lambda: cursor,
        close=lambda: closed.append(True),
    )

    def connect(**kwargs):
        attempts.append(kwargs)
        if rotate_password and len(attempts) == 1:
            monkeypatch.setenv("PGSYNC_STUDIO_PG_PASSWORD", "rotated@pass")
            raise OSError("temporary connection failure")
        return connection

    def select_ready(*args):
        stop.set()
        return [], [], []

    monkeypatch.setattr(module.psycopg2, "connect", connect)
    monkeypatch.setattr(module.select, "select", select_ready)
    runner._native_listener_loop(
        "conn_1", store.get_connection("conn_1"), {("public", "book")}, stop
    )

    assert len(attempts) == (2 if rotate_password else 1)
    assert all(item["user"] == "runtime_user" for item in attempts)
    assert attempts[0]["password"] == "runtime@pass"
    assert attempts[-1]["password"] == (
        "rotated@pass" if rotate_password else "runtime@pass"
    )
    assert all(item["host"] == "primary-host" for item in attempts)
    assert executed == ['LISTEN "postgres"']
    assert closed == [True]
    assert store.get_connection("conn_1")["password"] == ""


def test_native_credentials_keep_legacy_and_placeholder_compatibility(
    masked_runner, monkeypatch
):
    runner, store = masked_runner
    monkeypatch.setenv("TEST_NATIVE_HOST", "legacy-primary")
    monkeypatch.setenv("TEST_NATIVE_PASSWORD", "legacy@pass")
    connection = {
        "host": "${TEST_NATIVE_HOST}",
        "port": 5432,
        "username": "legacy_user",
        "password": "${TEST_NATIVE_PASSWORD}",
    }
    assert runner._connection_kwargs(connection) == {
        "host": "legacy-primary",
        "port": 5432,
        "user": "legacy_user",
        "password": "legacy@pass",
    }
    assert connection["password"] == "${TEST_NATIVE_PASSWORD}"
