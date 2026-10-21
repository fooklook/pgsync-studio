"""Tests for read-replica settings loaded from Nacos."""

import os

import pytest

from pgsync.web.nacos_bootstrap import apply_nacos_config

READ_REPLICA_ENV_NAMES = (
    "PGSYNC_STUDIO_READ_REPLICA_ENABLED",
    "PGSYNC_STUDIO_READ_REPLICA_HOST",
    "PGSYNC_STUDIO_READ_REPLICA_PORT",
    "PGSYNC_STUDIO_READ_REPLICA_CONNECTION_IDS",
    "PGSYNC_STUDIO_READ_REPLICA_WAIT_TIMEOUT_MS",
    "PGSYNC_STUDIO_READ_REPLICA_POLL_INTERVAL_MS",
    "PGSYNC_STUDIO_READ_REPLICA_COOLDOWN_MS",
    "PGSYNC_STUDIO_READ_REPLICA_FALLBACK_TO_PRIMARY",
)


@pytest.fixture(autouse=True)
def isolate_read_replica_env(monkeypatch):
    for name in READ_REPLICA_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)
    yield
    for name in READ_REPLICA_ENV_NAMES:
        os.environ.pop(name, None)


def _clear_read_replica_env(monkeypatch) -> None:
    for name in READ_REPLICA_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)


def test_apply_nacos_config_maps_read_replica_settings(monkeypatch):
    _clear_read_replica_env(monkeypatch)

    apply_nacos_config(
        {
            "pgsync": {
                "studio": {
                    "read_replica": {
                        "enabled": True,
                        "host": "replica.internal",
                        "port": 6543,
                        "connection_ids": "conn_orders,conn_members",
                        "wait_timeout_ms": 2500,
                        "poll_interval_ms": 125,
                        "cooldown_ms": 45000,
                        "fallback_to_primary": False,
                    }
                }
            }
        }
    )

    assert os.environ["PGSYNC_STUDIO_READ_REPLICA_ENABLED"] == "true"
    assert os.environ["PGSYNC_STUDIO_READ_REPLICA_HOST"] == "replica.internal"
    assert os.environ["PGSYNC_STUDIO_READ_REPLICA_PORT"] == "6543"
    assert (
        os.environ["PGSYNC_STUDIO_READ_REPLICA_CONNECTION_IDS"]
        == "conn_orders,conn_members"
    )
    assert os.environ["PGSYNC_STUDIO_READ_REPLICA_WAIT_TIMEOUT_MS"] == "2500"
    assert os.environ["PGSYNC_STUDIO_READ_REPLICA_POLL_INTERVAL_MS"] == "125"
    assert os.environ["PGSYNC_STUDIO_READ_REPLICA_COOLDOWN_MS"] == "45000"
    assert (
        os.environ["PGSYNC_STUDIO_READ_REPLICA_FALLBACK_TO_PRIMARY"] == "false"
    )


def test_apply_nacos_config_without_read_replica_does_not_enable_it(
    monkeypatch,
):
    _clear_read_replica_env(monkeypatch)

    apply_nacos_config({"pgsync": {"studio": {"host": "0.0.0.0"}}})

    assert all(name not in os.environ for name in READ_REPLICA_ENV_NAMES)


def test_apply_nacos_config_preserves_read_replica_env_without_overwrite(
    monkeypatch,
):
    _clear_read_replica_env(monkeypatch)
    monkeypatch.setenv("PGSYNC_STUDIO_READ_REPLICA_ENABLED", "false")
    monkeypatch.setenv("PGSYNC_STUDIO_READ_REPLICA_HOST", "env-replica")

    apply_nacos_config(
        {
            "pgsync": {
                "studio": {
                    "read_replica": {
                        "enabled": True,
                        "host": "nacos-replica",
                    }
                }
            }
        },
        overwrite=False,
    )

    assert os.environ["PGSYNC_STUDIO_READ_REPLICA_ENABLED"] == "false"
    assert os.environ["PGSYNC_STUDIO_READ_REPLICA_HOST"] == "env-replica"
