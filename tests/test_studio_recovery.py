"""Fault-injection coverage for task recovery and ownership safety."""

import asyncio
import json
import threading
from types import SimpleNamespace

import fakeredis
import pytest
from redis.exceptions import AuthenticationError, ConnectionError

import pgsync.web.sync_manager as module
from pgsync.web.app import liveness
from pgsync.web.metrics import render_prometheus
from pgsync.web.redis_coordination import RedisJobLease
from pgsync.web.root_queue import RedisRootQueue
from pgsync.web.sync_manager import (
    CrossDatabaseHybridRunner,
    StudioError,
    SyncManager,
)
from tests.test_studio import (
    build_failed_recovery_store,
    FakeInflightRootQueue,
)


class Runner:
    def __init__(self, *, alive=False):
        self.alive = alive
        self.thread = SimpleNamespace(is_alive=lambda: self.alive)
        self.stop_event = threading.Event()
        self.stop_requested = False
        self.lease_lost = False
        self.native_listener_stop_events = {}
        self.native_listener_threads = {}


class Lease:
    renew_interval_seconds = 0

    def __init__(self, *, owned=True):
        self.owned = owned
        self.released = 0
        self.renewed = 0

    def renew(self):
        self.renewed += 1
        return self.owned

    def is_owned(self):
        return self.owned

    def release(self):
        self.released += 1


class Ticks:
    def __init__(self, count):
        self.count = count

    def wait(self, timeout):
        self.count -= 1
        return self.count < 0

    def is_set(self):
        return False


@pytest.fixture
def state(tmp_path, monkeypatch):
    clock = {"now": 1000.0}
    monkeypatch.setattr(module, "utc_timestamp", lambda: clock["now"])
    monkeypatch.setattr(module.random, "uniform", lambda *args: 1.0)
    store = build_failed_recovery_store(tmp_path)
    store.update_job_runtime(
        "job_1", {"native_change_event_cursors": {"conn_1:public": 123}}
    )
    manager = SyncManager(store=store, db=None, repo_root=str(tmp_path))
    return manager, store, clock


def unavailable(*args, **kwargs):
    raise ConnectionError("dependency unavailable")


def test_database_failure_during_hybrid_cleanup_preserves_checkpoint_and_queue(
    state, monkeypatch
):
    manager, store, clock = state
    queue = FakeInflightRootQueue()
    queue.enqueue_hot([(101,), (102,)])
    monkeypatch.setattr(
        module.RedisRootQueue, "from_services", lambda *args, **kwargs: queue
    )
    runner = CrossDatabaseHybridRunner(
        manager,
        manager.normalize_job_definition(store.get_job("job_1")),
        resume_incremental_only=True,
    )
    listener_stop = threading.Event()
    runner.native_listener_stop_events["conn_1"] = listener_stop
    runner.native_listener_states["conn_1"] = {"state": "listening"}
    manager.runners["job_1"] = runner
    lease = Lease()
    manager.job_leases["job_1"] = lease
    monitor_stop = threading.Event()
    manager.job_lease_threads["job_1"] = {
        "stop_event": monitor_stop,
        "thread": None,
    }
    store.update_job_runtime(
        "job_1", {"status": "running", "phase": "cross_database_incremental"}
    )

    with monkeypatch.context() as fault:
        fault.setattr(store, "get_job_runtime", unavailable)
        fault.setattr(store, "update_job_runtime", unavailable)
        runner.run()

    assert listener_stop.is_set() and monitor_stop.is_set()
    assert "job_1" not in manager.runners
    assert "job_1" not in manager.job_leases
    assert lease.released == 1
    assert queue.hot == [(101,), (102,)]
    assert store.get_job_runtime("job_1")["native_change_event_cursors"] == {
        "conn_1:public": 123
    }
    started = []
    monkeypatch.setattr(
        manager, "start_job", lambda job_id: started.append(job_id)
    )
    assert manager.run_recovery_cycle() == ["job_1"]
    assert started == ["job_1"]


def test_dead_registered_runner_is_reaped_then_retried(state, monkeypatch):
    manager, store, clock = state
    runner, lease = Runner(), Lease()
    manager.runners["job_1"] = runner
    manager.job_leases["job_1"] = lease
    store.update_job_runtime(
        "job_1", {"status": "running", "runner_recovery_next_attempt_ts": None}
    )
    started = []
    monkeypatch.setattr(
        manager, "start_job", lambda job_id: started.append(job_id)
    )

    assert manager.run_recovery_cycle() == []
    assert lease.released == 1
    assert runner.stop_event.is_set()
    assert "job_1" not in manager.runners
    clock["now"] = store.get_job_runtime("job_1")[
        "runner_recovery_next_attempt_ts"
    ]
    assert manager.run_recovery_cycle() == ["job_1"]
    assert started == ["job_1"]


def test_recovery_waits_for_old_listener_to_exit(state, monkeypatch):
    manager, store, clock = state
    runner, lease = Runner(), Lease()
    child = Runner(alive=True)
    runner.native_listener_threads["conn_1"] = child.thread
    runner.native_listener_stop_events["conn_1"] = child.stop_event
    manager.runners["job_1"] = runner
    manager.job_leases["job_1"] = lease
    started = []
    monkeypatch.setattr(
        manager, "start_job", lambda job_id: started.append(job_id)
    )
    clock["now"] = 1010
    assert manager.run_recovery_cycle() == []
    assert manager.runners["job_1"] is runner
    assert child.stop_event.is_set()
    assert lease.released == 0

    child.alive = False
    assert manager.run_recovery_cycle() == ["job_1"]
    assert lease.released == 1


def test_late_finalizer_cannot_release_replacement_runner(state):
    manager, store, clock = state
    old, new, lease = Runner(), Runner(alive=True), Lease()
    manager.runners["job_1"] = new
    manager.job_leases["job_1"] = lease
    before = store.get_job_runtime("job_1")
    manager.on_runner_finished("job_1", old)
    assert manager.runners["job_1"] is new
    assert manager.job_leases["job_1"] is lease
    assert lease.released == 0
    assert store.get_job_runtime("job_1") == before


def test_redis_loss_stops_workers_even_when_config_database_is_down(
    state, monkeypatch
):
    manager, store, clock = state
    runner, lease = Runner(alive=True), Lease()
    runner.assembly_runner = Runner(alive=True)
    manager.runners["job_1"] = runner
    monkeypatch.setattr(lease, "renew", unavailable)
    monkeypatch.setattr(store, "get_job_runtime", unavailable)
    monkeypatch.setattr(store, "update_job_runtime", unavailable)

    manager._lease_monitor_loop("job_1", runner, lease, Ticks(1))
    assert runner.lease_lost and runner.stop_event.is_set()
    assert runner.assembly_runner.lease_lost
    assert runner.assembly_runner.stop_event.is_set()


def test_database_heartbeat_error_does_not_kill_lease_monitor(
    state, monkeypatch
):
    manager, store, clock = state
    runner, lease = Runner(alive=True), Lease()
    manager.runners["job_1"] = runner
    monkeypatch.setattr(store, "update_job_runtime", unavailable)
    manager._lease_monitor_loop("job_1", runner, lease, Ticks(2))
    assert lease.renewed == 2
    assert not runner.lease_lost


def test_dead_runner_does_not_keep_renewing_lease(state):
    manager, store, clock = state
    runner, lease = Runner(), Lease()
    manager.runners["job_1"] = runner
    manager.job_leases["job_1"] = lease
    manager._lease_monitor_loop("job_1", runner, lease, Ticks(1))
    assert lease.renewed == 0
    assert lease.released == 1


def test_lease_lost_resumes_only_after_exclusive_ownership_is_available(
    state, monkeypatch
):
    manager, store, clock = state
    client = fakeredis.FakeRedis(decode_responses=True)
    services = store.snapshot()["services"]
    services["redis"]["host"] = "fake-redis"
    services["search"]["url"] = "http://fake-search"
    store.save_services(services)
    store.update_job_runtime("job_1", {"phase": "lease_lost"})
    peer = RedisJobLease(client, job_id="job_1", owner_instance_id="peer")
    assert peer.acquire()

    monkeypatch.setattr(
        module.RedisJobLease,
        "from_services",
        lambda services, job_id, owner_instance_id: RedisJobLease(
            client, job_id=job_id, owner_instance_id=owner_instance_id
        ),
    )
    started = []

    class ResumedRunner(Runner):
        def __init__(self, manager, job, *, resume_incremental_only):
            super().__init__()
            assert resume_incremental_only

        def start(self):
            self.alive = True
            started.append(True)

    monkeypatch.setattr(module, "CrossDatabaseHybridRunner", ResumedRunner)
    monkeypatch.setattr(manager, "start_job_lease_monitor", lambda *args: None)
    clock["now"] = 1010
    assert manager.run_recovery_cycle() == []
    assert not started
    assert client.get(peer.key) == peer.value
    assert peer.release()
    assert manager.run_recovery_cycle() == ["job_1"]
    assert started == [True]
    assert manager.job_leases["job_1"].is_owned()
    assert store.get_job_runtime("job_1")["native_change_event_cursors"] == {
        "conn_1:public": 123
    }


def test_unknown_redis_ownership_is_not_treated_as_a_free_lease(
    state, monkeypatch
):
    manager, store, clock = state
    services = store.snapshot()["services"]
    services["redis"]["host"] = "fake-redis"
    store.save_services(services)
    monkeypatch.setattr(module.RedisJobLease, "from_services", unavailable)
    with pytest.raises(ConnectionError):
        manager.job_has_active_lease("job_1")


def test_failed_start_releases_lease_even_before_runner_was_constructed(
    state, monkeypatch
):
    manager, store, clock = state
    services = store.snapshot()["services"]
    services["search"]["url"] = "http://fake-search"
    store.save_services(services)
    lease = Lease()
    monkeypatch.setattr(manager, "acquire_job_lease", lambda *args: lease)
    monkeypatch.setattr(store, "update_job_runtime", unavailable)
    with pytest.raises(ConnectionError):
        manager.start_job("job_1")
    assert lease.released == 1
    assert not manager.runners and not manager.job_leases


def test_one_recovery_failure_does_not_block_other_jobs(state, monkeypatch):
    manager, store, clock = state
    second = store.get_job("job_1")
    second.update(id="job_2", name="second", index_name="second")
    store.upsert_job(second)
    store.update_job_runtime("job_2", store.get_job_runtime("job_1"))
    started = []
    monkeypatch.setattr(
        manager, "start_job", lambda job_id: started.append(job_id)
    )

    def inspect(job_id, services=None):
        if job_id == "job_1":
            raise ConnectionError("first job lease lookup failed")
        return False

    monkeypatch.setattr(manager, "job_has_active_lease", inspect)
    clock["now"] = 1010
    assert manager.run_recovery_cycle() == ["job_2"]
    assert started == ["job_2"]
    assert (
        store.get_job_runtime("job_1")["runner_recovery_next_attempt_ts"]
        == 1030
    )


def test_recovery_loop_survives_repeated_database_outages(state, monkeypatch):
    manager, store, clock = state
    attempts = []
    manager.recovery_poll_interval_seconds = 0.001

    def cycle():
        attempts.append(True)
        if len(attempts) < 3:
            raise ConnectionError("configuration database unavailable")
        manager._recovery_stop_event.set()

    monkeypatch.setattr(manager, "run_recovery_cycle", cycle)
    manager._recovery_loop()
    assert len(attempts) == 3
    assert manager.recovery_health()["cycle_errors"] == 2
    assert manager.recovery_health()["last_error"] == ""


def test_manual_stop_wins_over_a_pending_lease_recovery(state, monkeypatch):
    manager, store, clock = state
    store.update_job_runtime("job_1", {"phase": "lease_lost"})
    manager.stop_job("job_1")
    started = []
    monkeypatch.setattr(
        manager, "start_job", lambda job_id: started.append(job_id)
    )
    clock["now"] = 9999
    assert manager.run_recovery_cycle() == []
    assert not started
    assert store.get_job_runtime("job_1")["desired_state"] == "stopped"


def test_manual_stop_can_signal_local_runner_while_redis_is_down(
    state, monkeypatch
):
    manager, store, clock = state
    runner = Runner(alive=True)
    runner.stop = runner.stop_event.set
    manager.runners["job_1"] = runner
    monkeypatch.setattr(manager, "describe_job_holder", unavailable)
    manager.stop_job("job_1")
    assert runner.stop_event.is_set()
    assert store.get_job_runtime("job_1")["desired_state"] == "stopped"


def test_authentication_failure_blocks_automatic_restart(state, monkeypatch):
    manager, store, clock = state
    manager.record_runner_failure(
        "job_1", Runner(), AuthenticationError("bad credentials")
    )
    clock["now"] = 9999
    assert manager.run_recovery_cycle() == []
    assert (
        store.get_job_runtime("job_1")["runner_recovery_blocked_reason"]
        == "authentication_error"
    )


def test_expired_lease_prevents_an_old_runner_from_writing_to_es(state):
    manager, store, clock = state
    runner = Runner(alive=True)
    manager.runners["job_1"] = runner
    manager.job_leases["job_1"] = Lease(owned=False)
    runner.manager = manager
    runner.job_id = "job_1"
    writes = []
    search = SimpleNamespace(bulk=lambda actions: writes.extend(actions))
    with pytest.raises(StudioError, match="lease lost"):
        CrossDatabaseHybridRunner._apply_rebuild_actions(
            runner, search, [{"_id": "1"}], []
        )
    assert writes == []
    assert runner.lease_lost and runner.stop_event.is_set()


def test_old_batch_cannot_acknowledge_or_requeue_a_recovered_batch():
    queue = RedisRootQueue(
        fakeredis.FakeRedis(decode_responses=True), job_id="job"
    )
    queue.enqueue_hot([(1,), (2,)])
    old = queue.claim_batch(hot_limit=2, backlog_limit=0)
    assert queue.recover_inflight() == 2
    new = queue.claim_batch(hot_limit=2, backlog_limit=0)
    assert old["batch_id"] != new["batch_id"]
    assert queue.ack_batch(old["batch_id"]) is False
    assert queue.requeue_inflight(old["batch_id"]) == 0
    assert queue.snapshot()["inflight_size"] == 2
    assert queue.snapshot()["inflight_batch_id"] == new["batch_id"]
    assert queue.ack_batch(new["batch_id"]) is True
    assert queue.snapshot()["inflight_size"] == 0


def test_requeue_after_uncertain_write_retains_all_root_keys():
    queue = RedisRootQueue(
        fakeredis.FakeRedis(decode_responses=True), job_id="job"
    )
    queue.enqueue_hot([(1,), (2,)])
    batch = queue.claim_batch(hot_limit=2, backlog_limit=0)
    assert queue.requeue_inflight(batch["batch_id"]) == 2
    assert queue.snapshot()["inflight_size"] == 0
    assert queue.claim_batch(hot_limit=2, backlog_limit=0)["root_keys"] == [
        (1,),
        (2,),
    ]


def test_retry_jitter_is_bounded_and_backoff_remains_capped(
    state, monkeypatch
):
    manager, store, clock = state
    monkeypatch.setattr(module.random, "uniform", lambda *args: 1.1)
    store.update_job_runtime(
        "job_1",
        {
            "runner_recovery_next_attempt_ts": None,
            "runner_recovery_attempt_count": 0,
        },
    )
    assert (
        manager.schedule_failed_runtime_recovery("job_1")[
            "runner_recovery_retry_delay_seconds"
        ]
        == 11
    )
    store.update_job_runtime(
        "job_1",
        {
            "runner_recovery_next_attempt_ts": None,
            "runner_recovery_attempt_count": 20,
        },
    )
    assert (
        manager.schedule_failed_runtime_recovery("job_1")[
            "runner_recovery_retry_delay_seconds"
        ]
        == 300
    )


@pytest.mark.parametrize("alive,expected", [(True, 200), (False, 503)])
def test_liveness_reports_supervisor_health_without_reading_database(
    state, monkeypatch, alive, expected
):
    manager, store, clock = state
    manager._recovery_thread = SimpleNamespace(is_alive=lambda: alive)
    monkeypatch.setattr(store, "snapshot", unavailable)
    response = asyncio.run(liveness(SimpleNamespace(app={"manager": manager})))
    assert response.status == expected
    assert json.loads(response.body)["ok"] is alive


def test_metrics_expose_dead_runner_even_when_stored_status_is_running(state):
    manager, store, clock = state
    store.update_job_runtime(
        "job_1",
        {
            "status": "running",
            "mode": "native",
            "last_incremental_sync_ts": 100,
        },
    )
    manager.runners["job_1"] = Runner()
    output = render_prometheus(manager)
    assert (
        'pgsync_job_runner_alive{index="demo_index",job="demo",job_id="job_1",mode="native"} 0'
        in output
    )
    assert (
        'pgsync_job_last_incremental_sync_timestamp_seconds{index="demo_index",job="demo",job_id="job_1",mode="native"} 100'
        in output
    )


def test_redis_inspection_failure_does_not_overwrite_peer_state(
    state, monkeypatch
):
    manager, store, clock = state
    store.update_job_runtime(
        "job_1",
        {
            "status": "running",
            "phase": "cross_database_incremental",
            "owner": "another-instance",
            "last_error": "",
        },
    )
    monkeypatch.setattr(manager, "job_has_active_lease", unavailable)
    assert manager.run_recovery_cycle() == []
    runtime = store.get_job_runtime("job_1")
    assert runtime["status"] == "running"
    assert runtime["owner"] == "another-instance"
    assert runtime["last_error"] == ""


def test_lease_loss_after_es_write_keeps_inflight_for_the_next_owner(
    state, monkeypatch
):
    manager, store, clock = state
    queue = RedisRootQueue(
        fakeredis.FakeRedis(decode_responses=True), job_id="job_1"
    )
    monkeypatch.setattr(
        module.RedisRootQueue, "from_services", lambda *args, **kwargs: queue
    )
    runner = CrossDatabaseHybridRunner(
        manager,
        manager.normalize_job_definition(store.get_job("job_1")),
        resume_incremental_only=True,
    )
    lease = Lease()
    manager.runners["job_1"] = runner
    manager.job_leases["job_1"] = lease
    queue.enqueue_hot([(101,), (102,)])
    monkeypatch.setattr(
        runner.assembly_runner, "build_read_plan", lambda: None
    )
    monkeypatch.setattr(
        runner,
        "_build_rebuild_actions_parallel",
        lambda *args, **kwargs: ([{"_id": "101"}], [], [0]),
    )
    written = []

    def write_then_lose_lease(actions):
        written.extend(actions)
        lease.owned = False

    with pytest.raises(StudioError, match="lease lost"):
        runner._run_assembly_incremental_cycle(
            SimpleNamespace(bulk=write_then_lose_lease)
        )

    assert written == [{"_id": "101"}]
    assert queue.snapshot()["inflight_size"] == 2
    assert queue.recover_inflight() == 2
    assert set(
        queue.claim_batch(hot_limit=2, backlog_limit=0)["root_keys"]
    ) == {(101,), (102,)}
