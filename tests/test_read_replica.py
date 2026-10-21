"""Unit tests for batch-scoped PostgreSQL read-replica routing."""

from contextlib import contextmanager
import threading

import pytest
import sqlalchemy as sa

from pgsync.web.db import (
    AssemblyReadPlan,
    ConnectionReadRoute,
    DatabaseManager,
    ReplicaReadConfig,
    ReplicaReadQueryError,
    ReplicaReadUnavailable,
    connection_identity,
)
from pgsync.web.sync_manager import CrossDatabaseHybridRunnerV2


def source_connection(connection_id: str = "conn_1") -> dict:
    return {
        "id": connection_id,
        "host": "primary-db",
        "port": 5432,
        "database": "business",
        "username": "app",
        "password": "secret",
        "url": "",
    }


def replica_config(**overrides) -> ReplicaReadConfig:
    values = {
        "enabled": True,
        "host": "replica-db",
        "port": None,
        "wait_timeout_ms": 2000,
        "poll_interval_ms": 100,
        "fallback_to_primary": True,
    }
    values.update(overrides)
    return ReplicaReadConfig(**values)


def test_disabled_plan_stays_on_primary_without_lsn_query(monkeypatch):
    manager = DatabaseManager()
    connection = source_connection()

    def unexpected_lsn_query(_connection):
        raise AssertionError("disabled routing must not query WAL state")

    monkeypatch.setattr(
        manager,
        "_current_wal_insert_lsn",
        unexpected_lsn_query,
    )
    monkeypatch.setattr(
        "pgsync.web.db.build_postgres_url",
        lambda _connection: (_ for _ in ()).throw(
            AssertionError("disabled routing must not parse connection URLs")
        ),
    )

    plan = manager.build_assembly_read_plan(
        [connection],
        config=ReplicaReadConfig(enabled=False),
    )

    route = plan.route_for(connection)
    assert route is not None
    assert route.role == "primary"
    assert route.reason == "disabled"
    assert route.target_connection == connection


def test_plan_waits_until_replica_replays_required_lsn(monkeypatch):
    manager = DatabaseManager()
    connection = source_connection()
    replay_states = iter(
        [
            (True, "0/10", False),
            (True, "0/20", True),
        ]
    )
    sleep_calls = []
    monkeypatch.setattr(
        manager,
        "_current_wal_insert_lsn",
        lambda _connection: "0/20",
    )
    monkeypatch.setattr(
        manager,
        "_replica_replay_state",
        lambda _connection, _required_lsn: next(replay_states),
    )
    monkeypatch.setattr(
        "pgsync.web.db.time.sleep",
        lambda seconds: sleep_calls.append(seconds),
    )

    plan = manager.build_assembly_read_plan(
        [connection],
        config=replica_config(),
    )

    route = plan.route_for(connection)
    assert route is not None
    assert route.role == "replica"
    assert route.required_lsn == "0/20"
    assert route.replay_lsn == "0/20"
    assert route.reason == "caught_up"
    assert "@replica-db:5432/business" in route.target_connection["url"]
    assert sleep_calls


def test_plan_uses_replica_immediately_when_already_caught_up(monkeypatch):
    manager = DatabaseManager()
    connection = source_connection()
    replay_checks = []
    monkeypatch.setattr(
        manager,
        "_current_wal_insert_lsn",
        lambda _connection: "0/20",
    )

    def replay_state(_connection, required_lsn):
        replay_checks.append(required_lsn)
        return True, "0/30", True

    monkeypatch.setattr(manager, "_replica_replay_state", replay_state)

    plan = manager.build_assembly_read_plan(
        [connection],
        config=replica_config(),
    )

    route = plan.route_for(connection)
    assert route is not None
    assert route.role == "replica"
    assert route.waited_ms >= 0
    assert replay_checks == ["0/20"]


@pytest.mark.parametrize(
    ("state", "reason"),
    [
        ((True, "0/10", False), "replay_lsn_timeout"),
        ((False, "0/20", True), "replica_not_in_recovery"),
    ],
)
def test_unready_or_wrong_role_replica_falls_back_before_reads(
    monkeypatch,
    state,
    reason,
):
    manager = DatabaseManager()
    connection = source_connection()
    monkeypatch.setattr(
        manager,
        "_current_wal_insert_lsn",
        lambda _connection: "0/20",
    )
    monkeypatch.setattr(
        manager,
        "_replica_replay_state",
        lambda _connection, _required_lsn: state,
    )

    plan = manager.build_assembly_read_plan(
        [connection],
        config=replica_config(wait_timeout_ms=0),
    )

    route = plan.route_for(connection)
    assert route is not None
    assert route.role == "primary"
    assert route.reason == f"work_unit_fallback:{reason}"


def test_fallback_can_be_disabled_without_mixing_sources(monkeypatch):
    manager = DatabaseManager()
    connection = source_connection()
    monkeypatch.setattr(
        manager,
        "_current_wal_insert_lsn",
        lambda _connection: "0/20",
    )
    monkeypatch.setattr(
        manager,
        "_replica_replay_state",
        lambda _connection, _required_lsn: (True, "0/10", False),
    )

    with pytest.raises(ReplicaReadUnavailable, match="replay_lsn_timeout"):
        manager.build_assembly_read_plan(
            [connection],
            config=replica_config(
                wait_timeout_ms=0,
                fallback_to_primary=False,
            ),
        )


def test_caught_up_plan_preserves_strict_no_fallback_setting(monkeypatch):
    manager = DatabaseManager()
    connection = source_connection()
    monkeypatch.setattr(
        manager,
        "_current_wal_insert_lsn",
        lambda _connection: "0/20",
    )
    monkeypatch.setattr(
        manager,
        "_replica_replay_state",
        lambda _connection, _required_lsn: (True, "0/20", True),
    )

    plan = manager.build_assembly_read_plan(
        [connection],
        config=replica_config(fallback_to_primary=False),
    )

    assert plan.uses_replica is True
    assert plan.fallback_to_primary is False


def test_global_replica_endpoint_rejects_multiple_primary_clusters(
    monkeypatch,
):
    manager = DatabaseManager()
    first = source_connection("conn_1")
    second = {
        **source_connection("conn_2"),
        "host": "another-primary-cluster",
    }

    def unexpected_lsn_query(_connection):
        raise AssertionError("unsafe multi-cluster LSNs must not be compared")

    monkeypatch.setattr(
        manager,
        "_current_wal_insert_lsn",
        unexpected_lsn_query,
    )

    plan = manager.build_assembly_read_plan(
        [first, second],
        config=replica_config(),
    )

    assert plan.summary()["replica_count"] == 0
    assert plan.summary()["primary_count"] == 2
    assert {route.reason for route in plan.routes.values()} == {
        "multiple_primary_endpoints_unsupported"
    }


def test_one_unready_database_falls_back_the_whole_work_unit(monkeypatch):
    manager = DatabaseManager()
    first = source_connection("conn_1")
    second = {
        **source_connection("conn_2"),
        "database": "business_2",
    }
    monkeypatch.setattr(
        manager,
        "_current_wal_insert_lsn",
        lambda _connection: "0/20",
    )

    def replay_state(connection, _required_lsn):
        if connection["id"] == "conn_1":
            return True, "0/20", True
        return True, "0/10", False

    monkeypatch.setattr(manager, "_replica_replay_state", replay_state)

    plan = manager.build_assembly_read_plan(
        [first, second],
        config=replica_config(wait_timeout_ms=0),
    )

    assert plan.summary()["replica_count"] == 0
    assert plan.summary()["primary_count"] == 2
    assert all(
        route.reason.startswith("work_unit_fallback:replay_lsn_timeout")
        for route in plan.routes.values()
    )


def test_read_replica_config_loads_allowlist_and_cooldown(monkeypatch):
    monkeypatch.setenv(
        "PGSYNC_STUDIO_READ_REPLICA_CONNECTION_IDS",
        " conn_1,conn_2,conn_1 ",
    )
    monkeypatch.setenv("PGSYNC_STUDIO_READ_REPLICA_COOLDOWN_MS", "45000")

    config = ReplicaReadConfig.from_env()

    assert config.connection_ids == frozenset({"conn_1", "conn_2"})
    assert config.cooldown_ms == 45000


def test_allowlist_miss_keeps_whole_mixed_work_unit_on_primary(monkeypatch):
    manager = DatabaseManager()
    first = source_connection("conn_1")
    second = {
        **source_connection("conn_2"),
        "database": "business_2",
    }

    def unexpected_lsn_query(_connection):
        raise AssertionError("allowlist miss must be decided before LSN probes")

    monkeypatch.setattr(
        manager,
        "_current_wal_insert_lsn",
        unexpected_lsn_query,
    )

    plan = manager.build_assembly_read_plan(
        [first, second],
        config=replica_config(
            connection_ids=frozenset({"conn_1"}),
            fallback_to_primary=False,
        ),
    )

    assert plan.summary()["replica_count"] == 0
    assert plan.summary()["primary_count"] == 2
    assert {
        route.reason for route in plan.routes.values()
    } == {"connection_not_allowlisted"}


def test_circuit_cools_down_without_reprobing_then_closes_on_success(
    monkeypatch,
):
    manager = DatabaseManager()
    connection = source_connection()
    now = [100.0]
    replay_checks = [0]
    monkeypatch.setattr("pgsync.web.db.time.monotonic", lambda: now[0])
    monkeypatch.setattr(
        manager,
        "_current_wal_insert_lsn",
        lambda _connection: "0/20",
    )

    def lagging_replay(_connection, _required_lsn):
        replay_checks[0] += 1
        return True, "0/10", False

    monkeypatch.setattr(manager, "_replica_replay_state", lagging_replay)
    config = replica_config(wait_timeout_ms=0, cooldown_ms=30000)

    first_plan = manager.build_assembly_read_plan(
        [connection],
        config=config,
    )
    assert first_plan.route_for(connection).reason == (
        "work_unit_fallback:replay_lsn_timeout"
    )
    assert manager.replica_circuit_snapshot()["open"] is True

    second_plan = manager.build_assembly_read_plan(
        [connection],
        config=config,
    )
    assert second_plan.route_for(connection).reason == (
        "circuit_open:replay_lsn_timeout"
    )
    assert replay_checks[0] == 1

    now[0] += 31.0
    monkeypatch.setattr(
        manager,
        "_replica_replay_state",
        lambda _connection, _required_lsn: (True, "0/20", True),
    )
    recovered_plan = manager.build_assembly_read_plan(
        [connection],
        config=config,
    )
    assert recovered_plan.uses_replica is True
    assert manager.replica_circuit_snapshot()["open"] is False


def test_open_circuit_remains_fail_closed_when_fallback_is_disabled():
    manager = DatabaseManager()
    connection = source_connection()
    manager._open_replica_circuit(30000, reason="replica_operational_error")

    with pytest.raises(ReplicaReadUnavailable, match="circuit is open"):
        manager.build_assembly_read_plan(
            [connection],
            config=replica_config(fallback_to_primary=False),
        )


def test_replica_connection_adds_bounded_connect_timeout():
    manager = DatabaseManager()

    replica = manager._replica_connection(
        source_connection(),
        replica_config(wait_timeout_ms=2000),
    )

    assert "connect_timeout=2" in replica["url"]


def test_query_uses_target_and_role_frozen_in_plan(monkeypatch):
    manager = DatabaseManager()
    primary = source_connection()
    replica = {**primary, "host": "replica-db"}
    route = ConnectionReadRoute(
        connection_key=connection_identity(primary),
        role="replica",
        target_connection=replica,
        required_lsn="0/20",
        replay_lsn="0/20",
    )
    plan = AssemblyReadPlan(routes={route.connection_key: route}, enabled=True)
    calls = []
    metric_roles = []

    @contextmanager
    def fake_connect(connection, *, role="primary"):
        calls.append((connection["host"], role))
        yield object()

    monkeypatch.setattr(manager, "connect", fake_connect)
    monkeypatch.setattr(
        manager,
        "_assert_replica_session_ready",
        lambda _connection, _required_lsn: None,
    )
    monkeypatch.setattr(
        "pgsync.web.db.record_pg_query",
        lambda *_args, **kwargs: metric_roles.append(kwargs["role"]),
    )

    result = manager._run_with_retry(
        primary,
        lambda _connection: "ok",
        label="test read",
        read_plan=plan,
    )

    assert result == "ok"
    assert calls == [("replica-db", "replica")]
    assert metric_roles == ["replica"]
    assert manager._engine_key(primary, role="primary") != manager._engine_key(
        primary,
        role="replica",
    )


def test_selected_replica_operational_error_is_marked_for_whole_unit_retry(
    monkeypatch,
):
    manager = DatabaseManager()
    primary = source_connection()
    replica = {**primary, "host": "replica-db"}
    route = ConnectionReadRoute(
        connection_key=connection_identity(primary),
        role="replica",
        target_connection=replica,
    )
    plan = AssemblyReadPlan(routes={route.connection_key: route}, enabled=True)

    @contextmanager
    def broken_connect(_connection, *, role="primary"):
        assert role == "replica"
        raise sa.exc.OperationalError(
            "SELECT 1",
            {},
            RuntimeError("canceling statement due to conflict with recovery"),
        )
        yield

    monkeypatch.setattr(manager, "connect", broken_connect)

    with pytest.raises(ReplicaReadQueryError):
        manager._run_with_retry(
            primary,
            lambda _connection: "unreachable",
            label="test read",
            read_plan=plan,
        )
    assert manager.replica_circuit_snapshot()["open"] is True


def test_selected_replica_interface_error_is_marked_for_whole_unit_retry(
    monkeypatch,
):
    manager = DatabaseManager()
    primary = source_connection()
    replica = {**primary, "host": "replica-db"}
    route = ConnectionReadRoute(
        connection_key=connection_identity(primary),
        role="replica",
        target_connection=replica,
    )
    plan = AssemblyReadPlan(routes={route.connection_key: route}, enabled=True)

    @contextmanager
    def broken_connect(_connection, *, role="primary"):
        assert role == "replica"
        raise sa.exc.InterfaceError(
            "SELECT 1",
            {},
            RuntimeError("connection closed"),
        )
        yield

    monkeypatch.setattr(manager, "connect", broken_connect)

    with pytest.raises(ReplicaReadQueryError):
        manager._run_with_retry(
            primary,
            lambda _connection: "unreachable",
            label="test read",
            read_plan=plan,
        )
    assert manager.replica_circuit_snapshot()["open"] is True


@pytest.mark.parametrize(
    ("in_recovery", "caught_up", "error_text"),
    [
        (False, True, "not a standby"),
        (True, False, "behind the work-unit LSN"),
    ],
)
def test_each_replica_data_session_rechecks_role_and_lsn(
    in_recovery,
    caught_up,
    error_text,
):
    manager = DatabaseManager()

    class Result:
        @staticmethod
        def mappings():
            return Result()

        @staticmethod
        def one():
            return {
                "in_recovery": in_recovery,
                "caught_up": caught_up,
            }

    class Connection:
        @staticmethod
        def execute(_statement, _params):
            return Result()

    with pytest.raises(ReplicaReadQueryError, match=error_text):
        manager._assert_replica_session_ready(Connection(), "0/20")


def test_recovery_root_resolution_does_not_swallow_replica_query_error():
    runner = object.__new__(CrossDatabaseHybridRunnerV2)

    class FakeAssemblyRunner:
        @staticmethod
        def resolve_root_keys(_table_id, _row, *, read_plan=None):
            raise ReplicaReadQueryError("replica canceled query")

    runner.assembly_runner = FakeAssemblyRunner()

    with pytest.raises(ReplicaReadQueryError, match="replica canceled query"):
        runner._resolve_rows_root_keys(
            ["table_1"],
            [{"id": 1}],
            read_plan=AssemblyReadPlan(enabled=True),
        )


def test_parallel_workers_receive_the_exact_same_read_plan():
    runner = object.__new__(CrossDatabaseHybridRunnerV2)
    runner.job = {"id": "job_1", "name": "job"}
    seen_plan_ids = []
    seen_lock = threading.Lock()
    plan = AssemblyReadPlan(enabled=True)

    class FakeAssemblyRunner:
        def build_rebuild_actions_for_root_keys(
            self,
            root_keys,
            *,
            read_plan=None,
        ):
            with seen_lock:
                seen_plan_ids.append(id(read_plan))
            return ([{"_id": str(item[0])} for item in root_keys], [])

    runner.assembly_runner = FakeAssemblyRunner()

    actions, deletions, elapsed = runner._build_rebuild_actions_parallel(
        [(1,), (2,), (3,), (4,)],
        worker_count=2,
        read_plan=plan,
    )

    assert [action["_id"] for action in actions] == ["1", "3", "2", "4"]
    assert deletions == []
    assert len(elapsed) == 2
    assert seen_plan_ids == [id(plan), id(plan)]
