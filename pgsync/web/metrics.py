"""In-process Prometheus metrics for PGSync Studio."""

from __future__ import annotations

import contextlib
import contextvars
import math
import threading
import time
import typing as t
from collections import defaultdict

LabelMap = dict[str, t.Any]
LabelKey = tuple[tuple[str, str], ...]

_DURATION_BUCKETS: tuple[float, ...] = (
    0.001,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
)
_MAX_LABEL_VALUE_LENGTH = 160
_UNKNOWN_JOB = {"job": "unknown", "job_id": "unknown"}

_current_job_labels: contextvars.ContextVar[dict[str, str]] = (
    contextvars.ContextVar(
        "pgsync_studio_current_job_labels",
        default=dict(_UNKNOWN_JOB),
    )
)


def _normalize_label_value(value: t.Any, *, default: str = "unknown") -> str:
    if value is None:
        text = default
    else:
        text = str(value)
    text = text.strip()
    if not text:
        text = default
    if len(text) > _MAX_LABEL_VALUE_LENGTH:
        text = text[: _MAX_LABEL_VALUE_LENGTH - 1] + "~"
    return text


def job_metric_labels(job: t.Optional[dict]) -> dict[str, str]:
    if not isinstance(job, dict):
        return dict(_UNKNOWN_JOB)
    job_id = _normalize_label_value(job.get("id"))
    job_name = _normalize_label_value(
        job.get("name") or job.get("index_name") or job_id
    )
    return {"job": job_name, "job_id": job_id}


def set_metrics_context(job: t.Optional[dict]) -> contextvars.Token:
    return _current_job_labels.set(job_metric_labels(job))


def reset_metrics_context(token: contextvars.Token) -> None:
    with contextlib.suppress(Exception):
        _current_job_labels.reset(token)


@contextlib.contextmanager
def metrics_context(job: t.Optional[dict]):
    token = set_metrics_context(job)
    try:
        yield
    finally:
        reset_metrics_context(token)


def _current_labels(extra: t.Optional[LabelMap] = None) -> dict[str, str]:
    labels = dict(_current_job_labels.get())
    for key, value in (extra or {}).items():
        labels[str(key)] = _normalize_label_value(value)
    return labels


def _label_key(labels: LabelMap) -> LabelKey:
    return tuple(
        sorted(
            (str(key), _normalize_label_value(value))
            for key, value in labels.items()
        )
    )


def _escape_label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _format_float(value: t.Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        number = 0.0
    if math.isnan(number) or math.isinf(number):
        number = 0.0
    return f"{number:.12g}"


def _sample_line(name: str, labels: LabelMap, value: t.Any) -> str:
    normalized = _label_key(labels)
    if normalized:
        label_text = ",".join(
            f'{key}="{_escape_label_value(value)}"'
            for key, value in normalized
        )
        return f"{name}{{{label_text}}} {_format_float(value)}"
    return f"{name} {_format_float(value)}"


class _MetricStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._counters: defaultdict[tuple[str, LabelKey], float] = defaultdict(
            float
        )
        self._histogram_buckets: dict[tuple[str, LabelKey], list[int]] = {}
        self._histogram_sum: defaultdict[tuple[str, LabelKey], float] = (
            defaultdict(float)
        )
        self._histogram_count: defaultdict[tuple[str, LabelKey], int] = (
            defaultdict(int)
        )
        self._histogram_bucket_values: dict[str, tuple[float, ...]] = {}
        self._help: dict[str, str] = {}
        self._types: dict[str, str] = {}

    def _register(self, name: str, metric_type: str, help_text: str) -> None:
        self._types.setdefault(name, metric_type)
        self._help.setdefault(name, help_text)

    def inc(
        self,
        name: str,
        labels: LabelMap,
        amount: float = 1.0,
        *,
        help_text: str,
    ) -> None:
        if amount <= 0:
            return
        with self._lock:
            self._register(name, "counter", help_text)
            self._counters[(name, _label_key(labels))] += float(amount)

    def observe(
        self,
        name: str,
        labels: LabelMap,
        value: float,
        *,
        help_text: str,
        buckets: tuple[float, ...] = _DURATION_BUCKETS,
    ) -> None:
        observed = max(0.0, float(value or 0.0))
        with self._lock:
            self._register(name, "histogram", help_text)
            self._histogram_bucket_values.setdefault(name, buckets)
            key = (name, _label_key(labels))
            counts = self._histogram_buckets.setdefault(
                key,
                [0 for _ in buckets],
            )
            for index, bucket in enumerate(buckets):
                if observed <= bucket:
                    counts[index] += 1
            self._histogram_sum[key] += observed
            self._histogram_count[key] += 1

    def render(self) -> list[str]:
        with self._lock:
            counters = dict(self._counters)
            histogram_buckets = {
                key: list(value)
                for key, value in self._histogram_buckets.items()
            }
            histogram_sum = dict(self._histogram_sum)
            histogram_count = dict(self._histogram_count)
            histogram_bucket_values = dict(self._histogram_bucket_values)
            help_text = dict(self._help)
            types = dict(self._types)

        lines: list[str] = []
        metric_names = sorted(
            {
                *(name for name, _ in counters),
                *(name for name, _ in histogram_buckets),
            }
        )
        for name in metric_names:
            lines.append(f"# HELP {name} {help_text.get(name, name)}")
            lines.append(f"# TYPE {name} {types.get(name, 'gauge')}")
            if types.get(name) == "histogram":
                buckets = histogram_bucket_values.get(name, _DURATION_BUCKETS)
                for (metric_name, labels), counts in sorted(
                    histogram_buckets.items()
                ):
                    if metric_name != name:
                        continue
                    label_dict = dict(labels)
                    for bucket, count in zip(buckets, counts):
                        lines.append(
                            _sample_line(
                                f"{name}_bucket",
                                {**label_dict, "le": _format_float(bucket)},
                                count,
                            )
                        )
                    lines.append(
                        _sample_line(
                            f"{name}_bucket",
                            {**label_dict, "le": "+Inf"},
                            histogram_count.get((name, labels), 0),
                        )
                    )
                    lines.append(
                        _sample_line(
                            f"{name}_sum",
                            label_dict,
                            histogram_sum.get((name, labels), 0.0),
                        )
                    )
                    lines.append(
                        _sample_line(
                            f"{name}_count",
                            label_dict,
                            histogram_count.get((name, labels), 0),
                        )
                    )
            else:
                for (metric_name, labels), value in sorted(counters.items()):
                    if metric_name == name:
                        lines.append(_sample_line(name, dict(labels), value))
        return lines


_METRICS = _MetricStore()


def _safe_record(func: t.Callable[[], None]) -> None:
    with contextlib.suppress(Exception):
        func()


def record_pg_query(
    connection: dict,
    operation: str,
    elapsed_seconds: float,
    *,
    status: str,
    rows: int = 0,
    role: str = "primary",
) -> None:
    labels = _current_labels(
        {
            "database": (
                connection.get("database")
                if isinstance(connection, dict)
                else None
            ),
            "operation": operation,
            "role": role,
            "status": status,
        }
    )

    def record() -> None:
        _METRICS.inc(
            "pgsync_pg_query_total",
            labels,
            help_text="Total PostgreSQL queries executed by PGSync Studio.",
        )
        _METRICS.inc(
            "pgsync_pg_rows_returned_total",
            labels,
            max(0, int(rows or 0)),
            help_text="Total rows returned from PostgreSQL queries executed by PGSync Studio.",
        )
        _METRICS.observe(
            "pgsync_pg_query_seconds",
            labels,
            elapsed_seconds,
            help_text="PostgreSQL query duration observed by PGSync Studio.",
        )

    _safe_record(record)


def record_es_bulk(
    index_name: str,
    op: str,
    action_count: int,
    elapsed_seconds: float,
    *,
    status: str,
) -> None:
    labels = _current_labels(
        {
            "index": index_name,
            "op": op,
            "status": status,
        }
    )

    def record() -> None:
        _METRICS.inc(
            "pgsync_es_bulk_total",
            labels,
            help_text="Total Elasticsearch bulk requests executed by PGSync Studio.",
        )
        _METRICS.inc(
            "pgsync_es_bulk_actions_total",
            labels,
            max(0, int(action_count or 0)),
            help_text="Total Elasticsearch bulk actions executed by PGSync Studio.",
        )
        _METRICS.observe(
            "pgsync_es_bulk_seconds",
            labels,
            elapsed_seconds,
            help_text="Elasticsearch bulk request duration observed by PGSync Studio.",
        )

    _safe_record(record)


def record_rebuild_batch(
    job: dict,
    *,
    raw_root_keys: int,
    deduped_root_keys: int,
    worker_count: int,
    updated_count: int,
    deleted_count: int,
    elapsed_seconds: float,
    status: str,
) -> None:
    base_labels = job_metric_labels(job)
    status_labels = {**base_labels, "status": status}

    def record() -> None:
        _METRICS.inc(
            "pgsync_rebuild_batch_total",
            status_labels,
            help_text="Total assembly rebuild batches executed by PGSync Studio.",
        )
        _METRICS.observe(
            "pgsync_rebuild_batch_seconds",
            status_labels,
            elapsed_seconds,
            help_text="Assembly rebuild batch duration observed by PGSync Studio.",
        )
        _METRICS.inc(
            "pgsync_rebuild_root_keys_total",
            {**status_labels, "type": "raw"},
            max(0, int(raw_root_keys or 0)),
            help_text="Total assembly rebuild root keys observed by PGSync Studio.",
        )
        _METRICS.inc(
            "pgsync_rebuild_root_keys_total",
            {**status_labels, "type": "deduped"},
            max(0, int(deduped_root_keys or 0)),
            help_text="Total assembly rebuild root keys observed by PGSync Studio.",
        )
        _METRICS.inc(
            "pgsync_rebuild_docs_total",
            {**status_labels, "op": "index"},
            max(0, int(updated_count or 0)),
            help_text="Total documents rebuilt by PGSync Studio assembly batches.",
        )
        _METRICS.inc(
            "pgsync_rebuild_docs_total",
            {**status_labels, "op": "delete"},
            max(0, int(deleted_count or 0)),
            help_text="Total documents rebuilt by PGSync Studio assembly batches.",
        )
        _METRICS.inc(
            "pgsync_rebuild_workers_total",
            status_labels,
            max(0, int(worker_count or 0)),
            help_text="Total worker slots used by assembly rebuild batches.",
        )

    _safe_record(record)


def _numeric(value: t.Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _runtime_metric_labels(job: dict, runtime: dict) -> dict[str, str]:
    labels = job_metric_labels(job)
    labels["index"] = _normalize_label_value(job.get("index_name"), default="")
    labels["mode"] = _normalize_label_value(runtime.get("mode"), default="")
    return labels


def _append_dynamic_help(
    lines: list[str], name: str, metric_type: str, help_text: str
) -> None:
    lines.append(f"# HELP {name} {help_text}")
    lines.append(f"# TYPE {name} {metric_type}")


def _render_runtime_metrics(manager: t.Any) -> list[str]:
    lines: list[str] = []
    if hasattr(manager, "recovery_health"):
        recovery = manager.recovery_health()
        for name, value, help_text in (
            (
                "pgsync_runner_recovery_loop_alive",
                int(recovery["running"]),
                "Whether the local task recovery supervisor is alive.",
            ),
            (
                "pgsync_runner_recovery_cycle_errors",
                recovery["cycle_errors"],
                "Recovery cycles that failed in this process.",
            ),
            (
                "pgsync_runner_recovery_last_cycle_timestamp_seconds",
                recovery["last_cycle_timestamp_seconds"],
                "Last time the local recovery supervisor began a cycle.",
            ),
        ):
            _append_dynamic_help(lines, name, "gauge", help_text)
            lines.append(_sample_line(name, {}, value))
    try:
        snapshot = manager.store.snapshot()
    except Exception:
        snapshot = {}
    jobs = snapshot.get("jobs") if isinstance(snapshot, dict) else []
    runtimes = (
        snapshot.get("job_runtime") if isinstance(snapshot, dict) else {}
    )
    if not isinstance(jobs, list):
        jobs = []
    if not isinstance(runtimes, dict):
        runtimes = {}

    metric_defs = [
        (
            "pgsync_job_info",
            "gauge",
            "Static job identity exported by PGSync Studio.",
        ),
        (
            "pgsync_job_running",
            "gauge",
            "Whether a PGSync Studio job is currently running or starting.",
        ),
        (
            "pgsync_job_error_state",
            "gauge",
            "Whether a PGSync Studio job currently reports an error state.",
        ),
        (
            "pgsync_job_runner_alive",
            "gauge",
            "Whether this instance has a live runner thread for the job.",
        ),
        (
            "pgsync_job_recovery_attempts",
            "gauge",
            "Consecutive task recovery attempts.",
        ),
        (
            "pgsync_job_recovery_next_attempt_timestamp_seconds",
            "gauge",
            "Scheduled retry time, or zero when none is pending.",
        ),
        (
            "pgsync_job_recovery_blocked",
            "gauge",
            "Whether automatic recovery requires a configuration or permission fix.",
        ),
        (
            "pgsync_job_last_incremental_sync_timestamp_seconds",
            "gauge",
            "Last confirmed incremental batch timestamp.",
        ),
        (
            "pgsync_root_queue_size",
            "gauge",
            "Current assembly root queue size from PGSync Studio runtime state.",
        ),
        (
            "pgsync_listener_count",
            "gauge",
            "Current native listener count from PGSync Studio runtime state.",
        ),
        (
            "pgsync_durable_event_lag",
            "gauge",
            "Observed durable change-event lag lower bound from "
            "PGSync Studio runtime state.",
        ),
        (
            "pgsync_durable_event_lag_exact",
            "gauge",
            "Whether the durable change-event lag observation is exact.",
        ),
        (
            "pgsync_durable_event_backlog_present",
            "gauge",
            "Whether additional durable change events remain after "
            "the current batch.",
        ),
        (
            "pgsync_last_batch_elapsed_seconds",
            "gauge",
            "Last assembly rebuild batch elapsed seconds.",
        ),
        (
            "pgsync_last_batch_workers",
            "gauge",
            "Last assembly rebuild batch worker count.",
        ),
        (
            "pgsync_last_batch_root_keys",
            "gauge",
            "Last assembly rebuild batch root key count.",
        ),
        (
            "pgsync_last_batch_docs",
            "gauge",
            "Last assembly rebuild batch document count.",
        ),
        (
            "pgsync_recovery_scan_running",
            "gauge",
            "Whether recovery scan is currently running for a job.",
        ),
        (
            "pgsync_durable_change_events_enabled",
            "gauge",
            "Whether durable change events are enabled for a job.",
        ),
        (
            "pgsync_metrics_scrape_timestamp_seconds",
            "gauge",
            "Unix timestamp when PGSync Studio rendered the metrics response.",
        ),
    ]
    for name, metric_type, help_text in metric_defs:
        _append_dynamic_help(lines, name, metric_type, help_text)

    for job in jobs:
        if not isinstance(job, dict):
            continue
        job_id = str(job.get("id") or "")
        runtime = runtimes.get(job_id) if job_id else {}
        if not isinstance(runtime, dict):
            runtime = {}
        labels = _runtime_metric_labels(job, runtime)
        status = str(runtime.get("status") or "")
        running = 1 if status in {"starting", "running", "stopping"} else 0
        error_state = (
            1 if status == "failed" or runtime.get("last_error") else 0
        )
        lines.append(_sample_line("pgsync_job_info", labels, 1))
        lines.append(_sample_line("pgsync_job_running", labels, running))
        lines.append(
            _sample_line("pgsync_job_error_state", labels, error_state)
        )
        if hasattr(manager, "runner_is_alive"):
            lines.append(
                _sample_line(
                    "pgsync_job_runner_alive",
                    labels,
                    int(manager.runner_is_alive(manager.runners.get(job_id))),
                )
            )
        for name, value in (
            (
                "pgsync_job_recovery_attempts",
                runtime.get("runner_recovery_attempt_count"),
            ),
            (
                "pgsync_job_recovery_next_attempt_timestamp_seconds",
                runtime.get("runner_recovery_next_attempt_ts"),
            ),
            (
                "pgsync_job_recovery_blocked",
                bool(runtime.get("runner_recovery_blocked_reason")),
            ),
            (
                "pgsync_job_last_incremental_sync_timestamp_seconds",
                runtime.get("last_incremental_sync_ts"),
            ),
        ):
            lines.append(_sample_line(name, labels, _numeric(value)))
        lines.append(
            _sample_line(
                "pgsync_durable_change_events_enabled",
                labels,
                1 if runtime.get("durable_change_events_enabled") else 0,
            )
        )
        lines.append(
            _sample_line(
                "pgsync_recovery_scan_running",
                labels,
                1 if runtime.get("assembly_recovery_scan_running") else 0,
            )
        )

        queue_values = {
            "total": runtime.get("assembly_root_queue_size"),
            "hot": runtime.get("assembly_hot_queue_size"),
            "backlog": runtime.get("assembly_backlog_queue_size"),
            "inflight": runtime.get("assembly_inflight_batch_size"),
        }
        for queue_type, value in queue_values.items():
            lines.append(
                _sample_line(
                    "pgsync_root_queue_size",
                    {**labels, "type": queue_type},
                    _numeric(value),
                )
            )

        listener_values = {
            "total": runtime.get("listener_total_count"),
            "healthy": runtime.get("listener_healthy_count"),
            "recovering": runtime.get("listener_recovering_count"),
            "degraded": runtime.get("listener_degraded_count"),
        }
        for state, value in listener_values.items():
            lines.append(
                _sample_line(
                    "pgsync_listener_count",
                    {**labels, "state": state},
                    _numeric(value),
                )
            )

        lag_total = _numeric(runtime.get("durable_event_lag"))
        lines.append(
            _sample_line(
                "pgsync_durable_event_lag",
                {**labels, "group": "__total__"},
                lag_total,
            )
        )
        lag_by_group = runtime.get("durable_event_lag_by_group")
        if isinstance(lag_by_group, dict):
            for group, value in sorted(lag_by_group.items()):
                lines.append(
                    _sample_line(
                        "pgsync_durable_event_lag",
                        {**labels, "group": group},
                        _numeric(value),
                    )
                )

        lag_exact = runtime.get("durable_event_lag_exact")
        lines.append(
            _sample_line(
                "pgsync_durable_event_lag_exact",
                {**labels, "group": "__total__"},
                1 if lag_exact is True else 0,
            )
        )
        lag_exact_by_group = runtime.get("durable_event_lag_exact_by_group")
        if isinstance(lag_exact_by_group, dict):
            for group, value in sorted(lag_exact_by_group.items()):
                lines.append(
                    _sample_line(
                        "pgsync_durable_event_lag_exact",
                        {**labels, "group": group},
                        1 if value is True else 0,
                    )
                )

        backlog_present = runtime.get("durable_event_backlog_present")
        lines.append(
            _sample_line(
                "pgsync_durable_event_backlog_present",
                {**labels, "group": "__total__"},
                1 if backlog_present is True else 0,
            )
        )
        backlog_by_group = runtime.get(
            "durable_event_backlog_present_by_group"
        )
        if isinstance(backlog_by_group, dict):
            for group, value in sorted(backlog_by_group.items()):
                lines.append(
                    _sample_line(
                        "pgsync_durable_event_backlog_present",
                        {**labels, "group": group},
                        1 if value is True else 0,
                    )
                )

        elapsed_ms = _numeric(runtime.get("assembly_last_batch_elapsed_ms"))
        lines.append(
            _sample_line(
                "pgsync_last_batch_elapsed_seconds",
                labels,
                elapsed_ms / 1000.0,
            )
        )
        lines.append(
            _sample_line(
                "pgsync_last_batch_workers",
                labels,
                _numeric(runtime.get("assembly_last_batch_worker_count")),
            )
        )
        lines.append(
            _sample_line(
                "pgsync_last_batch_root_keys",
                {**labels, "type": "raw"},
                _numeric(
                    runtime.get("assembly_last_batch_raw_root_key_count")
                ),
            )
        )
        lines.append(
            _sample_line(
                "pgsync_last_batch_root_keys",
                {**labels, "type": "deduped"},
                _numeric(
                    runtime.get("assembly_last_batch_deduped_root_key_count")
                ),
            )
        )
        lines.append(
            _sample_line(
                "pgsync_last_batch_docs",
                {**labels, "op": "index"},
                _numeric(runtime.get("assembly_last_batch_updated_count")),
            )
        )
        lines.append(
            _sample_line(
                "pgsync_last_batch_docs",
                {**labels, "op": "delete"},
                _numeric(runtime.get("assembly_last_batch_deleted_count")),
            )
        )

    lines.append(
        _sample_line(
            "pgsync_metrics_scrape_timestamp_seconds",
            {},
            time.time(),
        )
    )
    return lines


def render_prometheus(manager: t.Any = None) -> str:
    lines: list[str] = []
    lines.extend(_METRICS.render())
    if manager is not None:
        lines.extend(_render_runtime_metrics(manager))
    return "\n".join(lines) + "\n"
