"""ES access failures retry existing jobs without resetting durable work."""

from types import SimpleNamespace

import fakeredis
import pytest
from elasticsearch import helpers
from elasticsearch.exceptions import (
    AuthenticationException,
    AuthorizationException,
    RequestError,
)

import pgsync.web.sync_manager as module
from pgsync.web.root_queue import RedisRootQueue
from pgsync.web.sync_manager import (
    CrossDatabaseHybridRunner,
    SearchAdapter,
    SearchBulkError,
    SyncManager,
)
from tests.test_studio_recovery import Lease, Runner, state  # noqa: F401


def bulk_error(*statuses):
    return helpers.BulkIndexError(
        "synthetic bulk failure",
        [{"index": {"status": code}} for code in statuses],
    )


@pytest.mark.parametrize(
    "error,wait,blocked",
    [
        (
            AuthenticationException(401, "denied"),
            "search_authentication_error",
            "",
        ),
        (AuthorizationException(403, "denied"), "search_permission_error", ""),
        (bulk_error(401), "search_authentication_error", ""),
        (bulk_error(403), "search_permission_error", ""),
        (bulk_error(401, 503), "search_authentication_error", ""),
        (
            RequestError(400, "invalid mapping"),
            "",
            "search_configuration_or_permission_error",
        ),
        (bulk_error(400, 403), "", "search_configuration_or_permission_error"),
    ],
)
def test_search_access_and_configuration_errors_are_distinguished(
    error, wait, blocked
):
    assert SyncManager.recovery_wait_reason(error) == wait
    assert SyncManager.recovery_blocked_reason(error) == blocked


@pytest.mark.parametrize("status", [401, 403])
def test_bulk_deletion_access_error_keeps_status_for_slow_retry(
    monkeypatch, status
):
    search = object.__new__(SearchAdapter)
    search.client = object()
    monkeypatch.setattr(
        module.helpers,
        "bulk",
        lambda *a, **kw: (0, [{"delete": {"status": status}}]),
    )
    with pytest.raises(SearchBulkError) as caught:
        search.delete_many("demo_index", ["101"])
    assert not SyncManager.recovery_blocked_reason(caught.value)
    assert SyncManager.recovery_wait_reason(caught.value) in {
        "search_authentication_error",
        "search_permission_error",
    }


def test_bulk_deletion_missing_document_remains_idempotent(monkeypatch):
    search = object.__new__(SearchAdapter)
    search.client = object()
    monkeypatch.setattr(
        module.helpers,
        "bulk",
        lambda *a, **kw: (0, [{"delete": {"status": 404}}]),
    )
    search.delete_many("demo_index", ["101"])


@pytest.mark.parametrize(
    "error",
    [
        AuthenticationException(401, "denied"),
        AuthorizationException(403, "denied"),
    ],
)
def test_persistent_es_access_failure_keeps_retrying_with_capped_backoff(
    state, monkeypatch, error
):
    manager, store, clock = state
    store.update_job_runtime(
        "job_1",
        {
            "runner_recovery_attempt_count": 0,
            "runner_recovery_next_attempt_ts": None,
        },
    )
    manager.record_runner_failure("job_1", Runner(), error)
    manager.on_runner_finished("job_1")
    attempts = []

    def fail(job_id):
        attempts.append(job_id)
        raise error

    monkeypatch.setattr(manager, "start_job", fail)
    for expected in (60, 120, 240, 300, 300, 300, 300):
        runtime = store.get_job_runtime("job_1")
        assert runtime["runner_recovery_retry_delay_seconds"] == expected
        assert not runtime["runner_recovery_blocked_reason"]
        assert runtime["native_change_event_cursors"] == {"conn_1:public": 123}
        due = runtime["runner_recovery_next_attempt_ts"]
        before = len(attempts)
        clock["now"] = due - 0.1
        assert manager.run_recovery_cycle() == []
        assert len(attempts) == before
        clock["now"] = due
        assert manager.run_recovery_cycle() == []
        assert len(attempts) == before + 1
        assert store.get_job("job_1")["index_name"] == "demo_index"


@pytest.mark.parametrize("status", [401, 403])
def test_access_recovery_uses_current_services_and_original_checkpoint(
    state, monkeypatch, status
):
    manager, store, clock = state
    store.update_job_runtime(
        "job_1",
        {
            "runner_recovery_attempt_count": 0,
            "runner_recovery_next_attempt_ts": None,
        },
    )
    error = AuthenticationException(status, "temporary access failure")
    manager.record_runner_failure("job_1", Runner(), error)
    manager.on_runner_finished("job_1")
    services = store.snapshot()["services"]
    services["redis"]["host"] = "fake-redis"
    services["search"].update(
        url="http://recovered-search", password="new-test-secret"
    )
    store.save_services(services)
    observed = []

    class ResumedRunner(Runner):
        def __init__(self, manager, job, *, resume_incremental_only):
            super().__init__()
            assert resume_incremental_only
            observed.append(manager.store.snapshot()["services"]["search"])
            observed.append(
                manager.store.get_job_runtime(job["id"])[
                    "native_change_event_cursors"
                ]
            )

        def start(self):
            self.alive = True
            manager.update_runtime("job_1", {"status": "running"})

    monkeypatch.setattr(module, "CrossDatabaseHybridRunner", ResumedRunner)
    monkeypatch.setattr(manager, "acquire_job_lease", lambda *args: Lease())
    monkeypatch.setattr(manager, "job_has_active_lease", lambda *args: False)
    monkeypatch.setattr(manager, "start_job_lease_monitor", lambda *args: None)
    clock["now"] = store.get_job_runtime("job_1")[
        "runner_recovery_next_attempt_ts"
    ]
    assert manager.run_recovery_cycle() == ["job_1"]
    runtime = store.get_job_runtime("job_1")
    assert observed[0]["url"] == "http://recovered-search"
    assert observed[0]["password"] == "new-test-secret"
    assert observed[1] == {"conn_1:public": 123}
    assert runtime["status"] == "running"
    assert runtime["baseline_completed"]
    assert runtime["runner_recovery_wait_reason"] == ""
    assert runtime["runner_recovery_blocked_reason"] == ""
    assert manager.run_recovery_cycle() == []
    assert len(observed) == 2


@pytest.mark.parametrize(
    "error,reason",
    [
        ("AuthenticationException(401, '')", "search_authentication_error"),
        ("AuthorizationException(403, 'denied')", "search_permission_error"),
        ("TransportError(401, 'denied')", "search_authentication_error"),
        ("TransportError(403, 'denied')", "search_permission_error"),
    ],
)
def test_old_release_access_block_migrates_and_timer_survives_restart(
    state, monkeypatch, error, reason
):
    manager, store, clock = state
    store.update_job_runtime(
        "job_1",
        {
            "last_error": error,
            "runner_recovery_blocked_reason": "search_configuration_or_permission_error",
            "runner_recovery_next_attempt_ts": None,
            "runner_recovery_attempt_count": 0,
        },
    )
    assert manager.run_recovery_cycle() == []
    runtime = store.get_job_runtime("job_1")
    assert runtime["runner_recovery_blocked_reason"] == ""
    assert runtime["runner_recovery_wait_reason"] == reason
    assert runtime["runner_recovery_next_attempt_ts"] == 1060
    restarted = SyncManager(store=store, db=None, repo_root=manager.repo_root)
    assert (
        store.get_job_runtime("job_1")["runner_recovery_next_attempt_ts"]
        == 1060
    )
    assert store.get_job_runtime("job_1")["runner_recovery_attempt_count"] == 1
    attempts = []
    monkeypatch.setattr(
        restarted, "start_job", lambda job_id: attempts.append(job_id)
    )
    clock["now"] = 1059
    assert restarted.run_recovery_cycle() == []
    clock["now"] = 1060
    assert restarted.run_recovery_cycle() == ["job_1"]
    assert attempts == ["job_1"]
    assert store.get_job_runtime("job_1")["native_change_event_cursors"] == {
        "conn_1:public": 123
    }


@pytest.mark.parametrize(
    "patch",
    [
        {"last_error": "RequestError(400, 'mapping contains 401 and 403')"},
        {"last_error": "unknown search configuration error"},
        {
            "last_error": "AuthenticationException(401, '')",
            "desired_state": "stopped",
        },
        {
            "last_error": "AuthenticationException(401, '')",
            "baseline_completed": False,
        },
        {
            "last_error": "AuthenticationException(401, '')",
            "assembly_recovery_requires_full_sync": True,
        },
        {
            "last_error": "AuthenticationException(401, '')",
            "resume_strategy": "wrong_strategy",
        },
    ],
)
def test_legacy_migration_does_not_bypass_manual_stop_or_safety_gates(
    state, patch
):
    manager, store, clock = state
    store.update_job_runtime(
        "job_1",
        {
            "runner_recovery_blocked_reason": "search_configuration_or_permission_error",
            "runner_recovery_next_attempt_ts": None,
            **patch,
        },
    )
    assert manager.run_recovery_cycle() == []
    runtime = store.get_job_runtime("job_1")
    assert (
        runtime["runner_recovery_blocked_reason"]
        == "search_configuration_or_permission_error"
    )
    assert runtime["runner_recovery_next_attempt_ts"] is None


def test_manual_stop_wins_over_pending_search_access_retry(state, monkeypatch):
    manager, store, clock = state
    manager._record_recovery_failure(
        "job_1", AuthorizationException(403, "denied")
    )
    manager.stop_job("job_1")
    attempts = []
    monkeypatch.setattr(
        manager, "start_job", lambda job_id: attempts.append(job_id)
    )
    clock["now"] = 99999
    assert manager.run_recovery_cycle() == []
    assert attempts == []
    assert store.get_job_runtime("job_1")["desired_state"] == "stopped"


def test_mapping_error_after_access_failure_stops_automatic_retry(state):
    manager, store, clock = state
    manager._record_recovery_failure(
        "job_1", AuthenticationException(401, "denied")
    )
    assert (
        store.get_job_runtime("job_1")["runner_recovery_wait_reason"]
        == "search_authentication_error"
    )
    manager._record_recovery_failure(
        "job_1", RequestError(400, "invalid mapping")
    )
    clock["now"] = 99999
    assert manager.run_recovery_cycle() == []
    runtime = store.get_job_runtime("job_1")
    assert runtime["runner_recovery_wait_reason"] == ""
    assert (
        runtime["runner_recovery_blocked_reason"]
        == "search_configuration_or_permission_error"
    )
    assert runtime["runner_recovery_next_attempt_ts"] is None


def test_search_access_retry_jitter_is_bounded(state, monkeypatch):
    manager, store, clock = state
    monkeypatch.setattr(module.random, "uniform", lambda *args: 1.1)
    store.update_job_runtime(
        "job_1",
        {
            "runner_recovery_attempt_count": 0,
            "runner_recovery_next_attempt_ts": None,
        },
    )
    manager._record_recovery_failure(
        "job_1", AuthenticationException(401, "denied")
    )
    assert (
        store.get_job_runtime("job_1")["runner_recovery_retry_delay_seconds"]
        == 66
    )
    store.update_job_runtime(
        "job_1",
        {
            "runner_recovery_attempt_count": 50,
            "runner_recovery_next_attempt_ts": None,
        },
    )
    manager.schedule_failed_runtime_recovery("job_1")
    assert (
        store.get_job_runtime("job_1")["runner_recovery_retry_delay_seconds"]
        == 300
    )


@pytest.mark.parametrize("status", [401, 403])
def test_es_write_denial_retains_batch_until_normal_incremental_retry(
    state, monkeypatch, status
):
    manager, store, clock = state
    queue = RedisRootQueue(
        fakeredis.FakeRedis(decode_responses=True), job_id="job_1"
    )
    queue.enqueue_hot([(101,), (102,)])
    monkeypatch.setattr(
        module.RedisRootQueue, "from_services", lambda *args, **kwargs: queue
    )
    runner = CrossDatabaseHybridRunner(
        manager,
        manager.normalize_job_definition(store.get_job("job_1")),
        resume_incremental_only=True,
    )
    manager.runners["job_1"] = runner
    manager.job_leases["job_1"] = Lease()
    monkeypatch.setattr(
        runner.assembly_runner, "build_read_plan", lambda: None
    )
    monkeypatch.setattr(
        runner,
        "_build_rebuild_actions_parallel",
        lambda root_keys, *args, **kwargs: (
            [{"_id": str(key[0])} for key in root_keys],
            [],
            [0],
        ),
    )

    def denied(actions):
        raise AuthorizationException(status, "write denied")

    search = SimpleNamespace(bulk=denied)
    with pytest.raises(AuthorizationException):
        runner._run_assembly_incremental_cycle(search)
    assert queue.queued_size() == 2
    assert queue.snapshot()["inflight_size"] == 0
    assert store.get_job_runtime("job_1")["native_change_event_cursors"] == {
        "conn_1:public": 123
    }
    assert not store.get_job_runtime("job_1").get(
        "assembly_last_batch_confirmed_at"
    )
    # The normal incremental path, not a separate compensation program, drains
    # both the retained work and a new change arriving during the outage.
    queue.enqueue_hot([(103,)])
    written = []
    search.bulk = lambda actions: written.extend(actions)
    assert runner._run_assembly_incremental_cycle(search)
    assert {action["_id"] for action in written} == {"101", "102", "103"}
    assert queue.queued_size() == 0
    assert queue.snapshot()["inflight_size"] == 0
    assert store.get_job_runtime("job_1")["assembly_last_batch_confirmed_at"]
    assert (
        store.get_job_runtime("job_1")["assembly_last_batch_updated_count"]
        == 3
    )


def test_search_retry_waits_for_old_workers_and_peer_lease(state, monkeypatch):
    manager, store, clock = state
    manager._record_recovery_failure(
        "job_1", AuthenticationException(401, "denied")
    )
    old, child = Runner(), Runner(alive=True)
    old.native_listener_threads["conn_1"] = child.thread
    manager.runners["job_1"] = old
    lease = Lease()
    manager.job_leases["job_1"] = lease
    attempts = []
    monkeypatch.setattr(
        manager, "start_job", lambda job_id: attempts.append(job_id)
    )
    clock["now"] = 9999
    assert manager.run_recovery_cycle() == []
    assert lease.released == 0
    child.alive = False
    monkeypatch.setattr(manager, "job_has_active_lease", lambda *args: True)
    assert manager.run_recovery_cycle() == []
    assert lease.released == 1
    assert not attempts
    monkeypatch.setattr(manager, "job_has_active_lease", lambda *args: False)
    assert manager.run_recovery_cycle() == ["job_1"]
    assert attempts == ["job_1"]


def test_disconnect_then_401_then_403_then_recovery_drains_existing_queue(
    state, monkeypatch
):
    manager, store, clock = state
    services = store.snapshot()["services"]
    services["redis"]["host"] = "fake-redis"
    services["search"]["url"] = "http://fake-search"
    store.save_services(services)
    queue = RedisRootQueue(
        fakeredis.FakeRedis(decode_responses=True), job_id="job_1"
    )
    queue.enqueue_hot([(101,), (102,)])
    # A previous owner left an unconfirmed batch when its ES connection broke.
    queue.claim_batch(hot_limit=2, backlog_limit=0)
    failures = [
        OSError("connection refused"),
        AuthenticationException(401, "authentication temporarily unavailable"),
        AuthorizationException(403, "writes temporarily denied"),
    ]
    written = []
    starts = []

    def bulk(actions):
        if failures:
            raise failures.pop(0)
        written.extend(actions)

    class TrialRunner(CrossDatabaseHybridRunner):
        def start(self):
            self.alive = True
            self.thread = SimpleNamespace(is_alive=lambda: self.alive)
            starts.append(self.resume_incremental_only)
            self.root_queue.recover_inflight()
            self.assembly_runner.build_read_plan = lambda: None
            self._build_rebuild_actions_parallel = lambda keys, *a, **kw: (
                [{"_id": str(key[0])} for key in keys],
                [],
                [0],
            )
            try:
                self._run_assembly_incremental_cycle(
                    SimpleNamespace(bulk=bulk)
                )
            except Exception as exc:
                self.manager.record_runner_failure(self.job_id, self, exc)
                self.alive = False
                self.manager.on_runner_finished(self.job_id, self)
            else:
                self.manager.update_runtime(
                    self.job_id,
                    {"status": "running", "last_error": ""},
                )

    monkeypatch.setattr(
        module.RedisRootQueue, "from_services", lambda *a, **kw: queue
    )
    monkeypatch.setattr(module, "CrossDatabaseHybridRunner", TrialRunner)
    monkeypatch.setattr(manager, "acquire_job_lease", lambda *a: Lease())
    monkeypatch.setattr(manager, "job_has_active_lease", lambda *a: False)
    monkeypatch.setattr(manager, "start_job_lease_monitor", lambda *a: None)
    for expected_error, wait_reason in (
        ("connection refused", ""),
        ("401", "search_authentication_error"),
        ("403", "search_permission_error"),
    ):
        clock["now"] = store.get_job_runtime("job_1")[
            "runner_recovery_next_attempt_ts"
        ]
        assert manager.run_recovery_cycle() == ["job_1"]
        runtime = store.get_job_runtime("job_1")
        assert runtime["status"] == "failed"
        assert expected_error in runtime["last_error"]
        assert runtime["runner_recovery_wait_reason"] == wait_reason
        assert not runtime["runner_recovery_blocked_reason"]
        assert queue.queued_size() == 2
        assert written == []
    queue.enqueue_hot([(103,)])
    clock["now"] = store.get_job_runtime("job_1")[
        "runner_recovery_next_attempt_ts"
    ]
    assert manager.run_recovery_cycle() == ["job_1"]
    runtime = store.get_job_runtime("job_1")
    assert runtime["status"] == "running"
    assert runtime["baseline_completed"]
    assert runtime["native_change_event_cursors"] == {"conn_1:public": 123}
    assert runtime["assembly_last_batch_confirmed_at"]
    assert {action["_id"] for action in written} == {"101", "102", "103"}
    assert queue.snapshot()["inflight_size"] == queue.queued_size() == 0
    assert starts == [True, True, True, True]
    assert store.get_job("job_1")["index_name"] == "demo_index"
