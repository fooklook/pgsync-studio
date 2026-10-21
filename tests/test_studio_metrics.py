"""Tests for PGSync Studio Prometheus metrics."""

import pytest

from pgsync.web.metrics import (
    metrics_context,
    record_es_bulk,
    record_pg_query,
    record_rebuild_batch,
    render_prometheus,
)


def test_metrics_render_internal_observations() -> None:
    job = {
        "id": "job-1",
        "name": "demo_job",
        "index_name": "demo_index",
    }

    with metrics_context(job):
        record_pg_query(
            {"database": "demo_db"},
            "public.demo fetch_changed_rows",
            0.012,
            status="success",
            rows=3,
        )
        record_pg_query(
            {"database": "demo_db"},
            "public.demo fetch_page",
            0.008,
            status="success",
            rows=2,
            role="replica",
        )
        record_es_bulk(
            "demo_index",
            "index",
            3,
            0.034,
            status="success",
        )

    record_rebuild_batch(
        job,
        raw_root_keys=5,
        deduped_root_keys=3,
        worker_count=2,
        updated_count=3,
        deleted_count=1,
        elapsed_seconds=0.05,
        status="success",
    )

    text = render_prometheus()

    assert "# TYPE pgsync_pg_query_seconds histogram" in text
    assert (
        'pgsync_pg_query_total{database="demo_db",job="demo_job",'
        'job_id="job-1",operation="public.demo fetch_changed_rows",'
        'role="primary",status="success"} 1'
    ) in text
    assert (
        'pgsync_pg_query_total{database="demo_db",job="demo_job",'
        'job_id="job-1",operation="public.demo fetch_page",'
        'role="replica",status="success"} 1'
    ) in text
    assert (
        'pgsync_es_bulk_actions_total{index="demo_index",job="demo_job",'
        'job_id="job-1",op="index",status="success"} 3'
    ) in text
    assert (
        'pgsync_rebuild_docs_total{job="demo_job",job_id="job-1",'
        'op="delete",status="success"} 1'
    ) in text


def test_metrics_render_runtime_snapshot() -> None:
    class Store:
        def snapshot(self) -> dict:
            return {
                "jobs": [
                    {
                        "id": "job-2",
                        "name": "runtime_job",
                        "index_name": "runtime_index",
                    }
                ],
                "job_runtime": {
                    "job-2": {
                        "status": "running",
                        "mode": "native",
                        "assembly_root_queue_size": 42,
                        "assembly_hot_queue_size": 30,
                        "assembly_backlog_queue_size": 12,
                        "assembly_inflight_batch_size": 5,
                        "listener_total_count": 2,
                        "listener_healthy_count": 2,
                        "durable_event_lag": 7,
                        "durable_event_lag_by_group": {"conn:public": 7},
                        "durable_event_lag_exact": False,
                        "durable_event_lag_exact_by_group": {
                            "conn:public": False
                        },
                        "durable_event_backlog_present": True,
                        "durable_event_backlog_present_by_group": {
                            "conn:public": True
                        },
                        "assembly_last_batch_elapsed_ms": 1234,
                        "assembly_last_batch_worker_count": 3,
                        "assembly_last_batch_raw_root_key_count": 10,
                        "assembly_last_batch_deduped_root_key_count": 8,
                        "assembly_last_batch_updated_count": 6,
                        "assembly_last_batch_deleted_count": 2,
                        "durable_change_events_enabled": True,
                    }
                },
            }

    class Manager:
        store = Store()

    text = render_prometheus(Manager())

    assert (
        'pgsync_job_running{index="runtime_index",job="runtime_job",'
        'job_id="job-2",mode="native"} 1'
    ) in text
    assert (
        'pgsync_root_queue_size{index="runtime_index",job="runtime_job",'
        'job_id="job-2",mode="native",type="total"} 42'
    ) in text
    assert (
        'pgsync_durable_event_lag{group="conn:public",index="runtime_index",'
        'job="runtime_job",job_id="job-2",mode="native"} 7'
    ) in text
    assert (
        'pgsync_durable_event_lag_exact{group="conn:public",'
        'index="runtime_index",job="runtime_job",job_id="job-2",'
        'mode="native"} 0'
    ) in text
    assert (
        'pgsync_durable_event_backlog_present{group="conn:public",'
        'index="runtime_index",job="runtime_job",job_id="job-2",'
        'mode="native"} 1'
    ) in text
    assert (
        'pgsync_last_batch_elapsed_seconds{index="runtime_index",'
        'job="runtime_job",job_id="job-2",mode="native"} 1.234'
    ) in text


def test_metrics_route_registered(monkeypatch, tmp_path) -> None:
    pytest.importorskip("aiohttp")
    from pgsync.web.app import create_app

    monkeypatch.setenv("PGSYNC_STUDIO_HOME", str(tmp_path))
    app = create_app()

    paths = {route.resource.canonical for route in app.router.routes()}

    assert "/metrics" in paths
    assert "/api/search/alias" in paths
