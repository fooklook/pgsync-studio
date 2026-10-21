"""PGSync Studio orchestration and validation."""

from __future__ import annotations

import json
import logging
import math
import os
import select
import socket
import subprocess
import sys
import threading
import time
import traceback
import typing as t
import uuid
from concurrent.futures import as_completed, ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timedelta, timezone

import psycopg2
from elasticsearch import Elasticsearch, helpers

from pgsync.base import Base as PGSyncBase

from .config_store import ConfigStore, utcnow
from .credentials import resolve_env_placeholders
from .db import (
    apply_shared_postgres_credentials,
    AssemblyReadPlan,
    DatabaseManager,
    mask_connection_credentials,
    MASKED_SHARED_CREDENTIAL,
    physical_database_identity,
    qualified_table,
    quote_ident,
    ReplicaReadQueryError,
    split_order_by_item,
)
from .external_store import DatabaseConfigStore
from .metrics import (
    metrics_context,
    record_es_bulk,
    record_rebuild_batch,
    reset_metrics_context,
    set_metrics_context,
)
from .nacos_bootstrap import fetch_nacos_payload_from_env
from .redis_coordination import NoopJobLease, RedisJobLease
from .root_queue import NoopRootQueue, RedisRootQueue

logger = logging.getLogger(__name__)

SUPPORTED_RELATION_TYPES = {"one_to_one", "one_to_many", "many_to_one"}
SUPPORTED_NATIVE_RELATIONS = {"one_to_one", "one_to_many", "many_to_one"}
LOG_TAIL_LIMIT = 80
JS_SAFE_INTEGER = (1 << 53) - 1
NATIVE_SETUP_CHECKPOINT_MARKER = "__PGSYNC_CHECKPOINT__="
DEFAULT_POLL_INTERVAL_SECONDS = 60
DEFAULT_RECOVERY_POLL_INTERVAL_SECONDS = 10
DEFAULT_FAILED_RECOVERY_INITIAL_DELAY_SECONDS = 10
DEFAULT_FAILED_RECOVERY_MAX_DELAY_SECONDS = 300
DEFAULT_FAILED_RECOVERY_STABLE_SECONDS = 60
DEFAULT_AUTO_DISCOVER_CONNECTIONS_TIME = "20:00"
DEFAULT_CROSS_DB_RECOVERY_SCAN_INTERVAL_SECONDS = 60
DEFAULT_CROSS_DB_REBUILD_BATCH_SIZE = 20
DEFAULT_CROSS_DB_RECOVERY_SCAN_BATCH_SIZE = 100
DEFAULT_CROSS_DB_HOT_BATCH_RATIO = 0.8
DEFAULT_CROSS_DB_MIN_HOT_BATCH_SIZE = 10
DEFAULT_CROSS_DB_MIN_BACKLOG_BATCH_SIZE = 10
DEFAULT_RUNTIME_NACOS_REFRESH_INTERVAL_SECONDS = 600
DEFAULT_ASSEMBLY_MAX_REBUILD_BATCH_SIZE = 1000
DEFAULT_ASSEMBLY_MAX_WORKER_THRESHOLD = 10_000_000
DEFAULT_ASSEMBLY_PARALLEL_ENABLED = False
DEFAULT_ASSEMBLY_WORKER_THRESHOLD_2 = 5000
DEFAULT_ASSEMBLY_WORKER_THRESHOLD_3 = 10000
DEFAULT_ASSEMBLY_MAX_WORKERS = 3
DEFAULT_LISTENER_KEEPALIVES_IDLE_SECONDS = 60
DEFAULT_LISTENER_KEEPALIVES_INTERVAL_SECONDS = 10
DEFAULT_LISTENER_KEEPALIVES_COUNT = 3
DEFAULT_LISTENER_CONNECT_TIMEOUT_SECONDS = 10
DEFAULT_LISTENER_NOTIFY_RUNTIME_UPDATE_INTERVAL_SECONDS = 5
DEFAULT_NATIVE_IDLE_RECOVERY_THRESHOLD_SECONDS = 300
DEFAULT_DURABLE_CHANGE_EVENT_BATCH_SIZE = 500
DEFAULT_DURABLE_CHANGE_EVENT_REPLAY_WINDOW = 5000
DEFAULT_DURABLE_CHANGE_EVENT_PROCESSED_WINDOW = 5000
DEFAULT_DURABLE_OUTBOX_CLEANUP_ENABLED = False
DEFAULT_DURABLE_OUTBOX_CLEANUP_RETENTION_HOURS = 168
DEFAULT_DURABLE_OUTBOX_CLEANUP_INTERVAL_SECONDS = 15
DEFAULT_DURABLE_OUTBOX_CLEANUP_BATCH_SIZE = 1000
DEFAULT_DURABLE_OUTBOX_CLEANUP_MAX_BATCHES_PER_RUN = 9
DEFAULT_DURABLE_OUTBOX_CLEANUP_MAX_ROWS_PER_RUN = 9000
DEFAULT_DURABLE_OUTBOX_CLEANUP_MAX_RUN_SECONDS = 45
DEFAULT_DURABLE_OUTBOX_CLEANUP_MAX_ROWS_PER_SECOND = 200
DEFAULT_DURABLE_OUTBOX_CLEANUP_BATCH_PAUSE_MS = 250
DEFAULT_DURABLE_OUTBOX_CLEANUP_STATEMENT_TIMEOUT_MS = 5000
DEFAULT_DURABLE_OUTBOX_CLEANUP_LOCK_TIMEOUT_MS = 1000
DEFAULT_DURABLE_OUTBOX_CLEANUP_DISABLED_POLL_SECONDS = 60
DEFAULT_DURABLE_OUTBOX_CAPTURE_RECONCILIATION_ENABLED = False
DEFAULT_DURABLE_OUTBOX_CAPTURE_RECONCILIATION_APPLY_CHANGES = False
DEFAULT_DURABLE_OUTBOX_CAPTURE_RECONCILIATION_LOCK_TIMEOUT_MS = 1000
DEFAULT_DURABLE_OUTBOX_PARTITION_MAINTENANCE_ENABLED = False
DEFAULT_DURABLE_OUTBOX_PARTITION_RETENTION_DAYS = 7
DEFAULT_DURABLE_OUTBOX_PARTITION_FUTURE_DAYS = 7
DEFAULT_DURABLE_OUTBOX_PARTITION_ALLOW_ORPHAN_TABLES = False
DEFAULT_DURABLE_OUTBOX_PARTITION_RUN_TIMES_UTC = ("00:15", "12:15")
DEFAULT_DURABLE_OUTBOX_PARTITION_LOCK_TIMEOUT_MS = 1000
DEFAULT_DURABLE_OUTBOX_PARTITION_DISABLED_POLL_SECONDS = 60
RESUME_STRATEGY_NATIVE_INCREMENTAL = "native_incremental"
RESUME_STRATEGY_ASSEMBLY_INCREMENTAL = "assembly_incremental"
RESUME_STRATEGY_CROSS_DATABASE_INCREMENTAL = "cross_database_incremental"
PHASE_AWAITING_INCREMENTAL_RESUME = "awaiting_incremental_resume"
HEALTH_STATUS_RUNNING = "running"
HEALTH_STATUS_DEGRADED = "degraded"
HEALTH_STATUS_RECOVERING = "recovering"
AUTO_DISCOVER_CONNECTIONS_TIME_ENV = (
    "PGSYNC_STUDIO_AUTO_DISCOVER_CONNECTIONS_TIME"
)


class StudioError(RuntimeError):
    """Domain exception for the studio layer."""


def get_in(payload: t.Any, *path: str) -> t.Any:
    """Safely read a nested mapping value."""

    current: t.Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


class RuntimeTuningConfig:
    """Runtime Nacos-backed tuning config with safe stale-cache fallback."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._payload: dict[str, t.Any] = {}
        self._last_refresh_ts: float = 0.0
        self._last_success_at: t.Optional[str] = None
        self._last_error: str = ""

    def _refresh_interval_seconds_locked(self) -> int:
        configured: t.Any = get_in(
            self._payload,
            "pgsync",
            "nacos",
            "runtime_refresh_interval_seconds",
        )
        if configured is None:
            configured = get_in(
                self._payload,
                "nacos",
                "runtime_refresh_interval_seconds",
            )
        if configured is None:
            configured = os.environ.get(
                "PGSYNC_STUDIO_NACOS_RUNTIME_REFRESH_INTERVAL_SECONDS"
            )
        try:
            return max(
                1,
                int(
                    configured
                    or DEFAULT_RUNTIME_NACOS_REFRESH_INTERVAL_SECONDS
                ),
            )
        except (TypeError, ValueError):
            return DEFAULT_RUNTIME_NACOS_REFRESH_INTERVAL_SECONDS

    def refresh_if_due(self, *, force: bool = False) -> None:
        now: float = time.time()
        with self._lock:
            interval: int = self._refresh_interval_seconds_locked()
            if (
                not force
                and self._last_refresh_ts
                and (now - self._last_refresh_ts < interval)
            ):
                return
        try:
            payload: dict = fetch_nacos_payload_from_env()
        except Exception as exc:
            with self._lock:
                self._last_refresh_ts = now
                self._last_error = str(exc)
            logger.warning(
                "Could not refresh runtime Nacos tuning config: %s", exc
            )
            return
        with self._lock:
            self._payload = payload if isinstance(payload, dict) else {}
            self._last_refresh_ts = now
            self._last_success_at = utcnow()
            self._last_error = ""

    def snapshot(self) -> dict[str, t.Any]:
        self.refresh_if_due()
        with self._lock:
            return {
                "payload": deepcopy(self._payload),
                "last_refresh_ts": self._last_refresh_ts,
                "last_success_at": self._last_success_at,
                "last_error": self._last_error,
                "refresh_interval_seconds": (
                    self._refresh_interval_seconds_locked()
                ),
            }


def _as_bool(value: t.Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized: str = str(value).strip().lower()
    if not normalized:
        return default
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _clamped_int(
    value: t.Any,
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    try:
        parsed: int = int(value if value not in (None, "") else default)
    except (TypeError, ValueError):
        parsed = default
    return min(maximum, max(minimum, parsed))


def make_id(prefix: str) -> str:
    """Return a short stable identifier."""
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def utc_timestamp() -> float:
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc).timestamp()


def normalize_checkpoint_scalar(value: t.Any) -> t.Any:
    """Normalize persisted cursor values back into comparable Python values."""
    if isinstance(value, str):
        candidate: str = value.strip()
        if candidate:
            if candidate.lstrip("-").isdigit():
                try:
                    return int(candidate)
                except ValueError:
                    pass
            try:
                return datetime.fromisoformat(candidate)
            except ValueError:
                return candidate
        return candidate
    return value


def normalize_checkpoint_payload(
    payload: t.Any,
    primary_key: list[str],
) -> dict[str, t.Any]:
    """Normalize checkpoint payloads into a comparable structure."""
    pk_size: int = len(primary_key)
    if isinstance(payload, dict):
        pk_values: list[t.Any] = [
            normalize_checkpoint_scalar(item)
            for item in list(payload.get("pk") or [])
        ]
        return {
            "cursor": normalize_checkpoint_scalar(payload.get("cursor")),
            "pk": pk_values[:pk_size]
            + [None] * max(0, pk_size - len(pk_values)),
        }
    return {
        "cursor": normalize_checkpoint_scalar(payload),
        "pk": [None] * pk_size,
    }


def compare_checkpoint_scalar(left: t.Any, right: t.Any) -> int:
    """Compare scalar cursor values, treating None as negative infinity."""
    if left == right:
        return 0
    if left is None:
        return -1
    if right is None:
        return 1
    if left < right:
        return -1
    return 1


def compare_checkpoints(
    left: t.Any,
    right: t.Any,
    primary_key: list[str],
) -> int:
    """Compare two checkpoints lexicographically by cursor then primary key."""
    normalized_left: dict[str, t.Any] = normalize_checkpoint_payload(
        left,
        primary_key,
    )
    normalized_right: dict[str, t.Any] = normalize_checkpoint_payload(
        right,
        primary_key,
    )
    cursor_cmp: int = compare_checkpoint_scalar(
        normalized_left.get("cursor"),
        normalized_right.get("cursor"),
    )
    if cursor_cmp:
        return cursor_cmp
    for left_pk, right_pk in zip(
        normalized_left.get("pk") or [],
        normalized_right.get("pk") or [],
    ):
        pk_cmp: int = compare_checkpoint_scalar(left_pk, right_pk)
        if pk_cmp:
            return pk_cmp
    return 0


def checkpoint_is_after(
    candidate: t.Any,
    checkpoint: t.Any,
    primary_key: list[str],
) -> bool:
    """Return True when candidate is strictly newer than checkpoint."""
    return compare_checkpoints(candidate, checkpoint, primary_key) > 0


def max_checkpoint(
    left: t.Any,
    right: t.Any,
    primary_key: list[str],
) -> dict[str, t.Any]:
    """Return the newer checkpoint of the two."""
    if compare_checkpoints(left, right, primary_key) >= 0:
        return normalize_checkpoint_payload(left, primary_key)
    return normalize_checkpoint_payload(right, primary_key)


def document_id_from_values(values: t.Iterable[t.Any]) -> str:
    """Build an Elasticsearch document id from root primary key values."""
    return "::".join("" if value is None else str(value) for value in values)


def normalize_relation_type(value: t.Optional[str]) -> str:
    """Normalize legacy or empty relation types."""
    relation_type: str = (value or "").strip()
    if relation_type == "many_to_many":
        return "many_to_one"
    return relation_type


def native_relation_type(value: t.Optional[str]) -> str:
    """Map studio relation types to PGSync native relation types."""
    relation_type: str = normalize_relation_type(value)
    if relation_type == "many_to_one":
        return "one_to_one"
    return relation_type


def relation_is_collection(value: t.Optional[str]) -> bool:
    """Return whether the relation should render as a list in the document."""
    return normalize_relation_type(value) == "one_to_many"


def default_relation_variant(value: t.Optional[str]) -> str:
    """Return the default PGSync relationship variant for a studio relation."""
    if relation_is_collection(value):
        return "nested"
    return "object"


def normalize_soft_delete_column(value: t.Any) -> str:
    """Normalize optional soft-delete column names."""
    return str(value or "").strip()


def soft_delete_row_is_deleted(
    row: dict[str, t.Any],
    column: t.Optional[str],
) -> bool:
    """Return whether a row should be treated as soft-deleted."""
    column_name: str = normalize_soft_delete_column(column)
    if not column_name:
        return False
    value: t.Any = row.get(column_name)
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value != 0
    if isinstance(value, str):
        candidate: str = value.strip().lower()
        if candidate in {"", "0", "false", "f", "n", "no", "off", "null"}:
            return False
        if candidate in {"1", "true", "t", "y", "yes", "on"}:
            return True
        if candidate.lstrip("-").isdigit():
            try:
                return int(candidate) != 0
            except ValueError:
                pass
    return bool(value)


def build_relation_container_mapping(
    relation_type: t.Optional[str],
    child_properties: dict[str, t.Any],
) -> dict[str, t.Any]:
    """Return the Elasticsearch container mapping for a relation."""
    if relation_is_collection(relation_type):
        return {
            "type": "nested",
            "properties": child_properties,
        }
    return {
        "properties": child_properties,
    }


class SearchAdapter:
    """Thin Elasticsearch wrapper for the studio runners."""

    def __init__(self, services: dict) -> None:
        services = resolve_env_placeholders(services)
        search: dict = services.get("search", {})
        if not search.get("url"):
            raise StudioError("启动同步任务前必须先配置 Elasticsearch 地址。")
        kwargs: dict[str, t.Any] = {
            "hosts": [search["url"]],
            "verify_certs": bool(search.get("verify_certs", False)),
        }
        username: str = search.get("username") or ""
        password: str = search.get("password") or ""
        if username and password:
            kwargs["http_auth"] = (username, password)
        self.client: Elasticsearch = Elasticsearch(**kwargs)

    def ensure_index(
        self,
        index_name: str,
        mapping: t.Optional[dict[str, t.Any]] = None,
    ) -> None:
        if not self.index_exists(index_name):
            body: dict[str, t.Any] = {}
            if mapping:
                body["mappings"] = mapping
            self.client.indices.create(index=index_name, body=body)

    def index_exists(self, index_name: str) -> bool:
        if not index_name:
            return False
        return bool(self.client.indices.exists(index=index_name))

    def count_documents(self, index_name: str) -> int:
        if not self.index_exists(index_name):
            return 0
        response: dict = self.client.count(index=index_name)
        return int(response.get("count") or 0)

    def delete_index(self, index_name: str) -> bool:
        if not self.index_exists(index_name):
            return False
        self.client.indices.delete(index=index_name, ignore=[400, 404])
        return True

    @staticmethod
    def _plain_response(payload: t.Any) -> dict:
        if isinstance(payload, dict):
            return dict(payload)
        body = getattr(payload, "body", None)
        if isinstance(body, dict):
            return body
        try:
            return dict(payload)
        except Exception:
            return {"raw": str(payload)}

    def add_index_alias(self, index_name: str, alias_name: str) -> dict:
        index_name = index_name.strip()
        alias_name = alias_name.strip()
        if not index_name:
            raise StudioError("索引名称不能为空。")
        if not alias_name:
            raise StudioError("索引别名不能为空。")
        if index_name == alias_name:
            raise StudioError("索引名称和别名不能相同。")
        if not self.index_exists(index_name):
            raise StudioError(f"Elasticsearch 索引 {index_name} 不存在。")
        response = self.client.indices.update_aliases(
            body={
                "actions": [
                    {
                        "add": {
                            "index": index_name,
                            "alias": alias_name,
                        }
                    }
                ]
            }
        )
        response_payload: dict = self._plain_response(response)
        aliases: dict = self._plain_response(
            self.client.indices.get_alias(index=index_name)
        )
        return {
            "index": index_name,
            "alias": alias_name,
            "response": response_payload,
            "aliases": sorted(
                (aliases.get(index_name, {}).get("aliases") or {}).keys()
            ),
        }

    def _bulk_metric_summary(
        self,
        actions: list[dict],
        *,
        default_index: str = "",
        default_op: str = "index",
    ) -> dict[tuple[str, str], int]:
        summary: dict[tuple[str, str], int] = {}
        for action in actions:
            index_name = str(
                action.get("_index") or default_index or "unknown"
            )
            op_type = str(action.get("_op_type") or default_op or "index")
            key = (index_name, op_type)
            summary[key] = summary.get(key, 0) + 1
        return summary

    def _record_bulk_metrics(
        self,
        summary: dict[tuple[str, str], int],
        elapsed_seconds: float,
        *,
        status: str,
    ) -> None:
        for (index_name, op_type), action_count in summary.items():
            record_es_bulk(
                index_name,
                op_type,
                action_count,
                elapsed_seconds,
                status=status,
            )

    def bulk(self, actions: list[dict]) -> None:
        if not actions:
            return
        summary = self._bulk_metric_summary(actions)
        started_at = time.perf_counter()
        status = "success"
        try:
            helpers.bulk(self.client, actions, refresh=False)
        except Exception:
            status = "error"
            raise
        finally:
            self._record_bulk_metrics(
                summary,
                time.perf_counter() - started_at,
                status=status,
            )

    def delete_many(self, index_name: str, document_ids: list[str]) -> None:
        actions: list[dict] = [
            {
                "_op_type": "delete",
                "_index": index_name,
                "_id": document_id,
            }
            for document_id in document_ids
        ]
        if not actions:
            return
        summary = self._bulk_metric_summary(
            actions,
            default_index=index_name,
            default_op="delete",
        )
        started_at = time.perf_counter()
        metric_status = "success"
        try:
            _, errors = helpers.bulk(
                self.client,
                actions,
                refresh=False,
                raise_on_error=False,
                raise_on_exception=False,
            )
            unexpected_errors: list[dict] = []
            for error in errors:
                if not isinstance(error, dict):
                    unexpected_errors.append({"raw": error})
                    continue
                op_type, payload = next(iter(error.items()))
                status: int = int(payload.get("status") or 0)
                result: str = str(payload.get("result") or "").lower()
                error_type: str = str(
                    (payload.get("error") or {}).get("type") or ""
                ).lower()
                if op_type == "delete" and (
                    status == 404
                    or result == "not_found"
                    or error_type == "document_missing_exception"
                ):
                    continue
                unexpected_errors.append(error)
            if unexpected_errors:
                metric_status = "error"
                raise StudioError(
                    f"Elasticsearch delete_many failed: {unexpected_errors[:3]}"
                )
        except Exception:
            metric_status = "error"
            raise
        finally:
            self._record_bulk_metrics(
                summary,
                time.perf_counter() - started_at,
                status=metric_status,
            )

    def search_documents(
        self,
        index_name: str,
        keyword: str = "",
        page: int = 1,
        size: int = 10,
    ) -> dict:
        if not self.client.indices.exists(index=index_name):
            return {
                "exists": False,
                "total": 0,
                "page": page,
                "size": size,
                "hits": [],
            }

        body: dict[str, t.Any] = {
            "from": max(page - 1, 0) * size,
            "size": size,
            "track_total_hits": True,
        }
        keyword = keyword.strip()
        if keyword:
            body["query"] = {
                "simple_query_string": {
                    "query": keyword,
                    "fields": ["*"],
                    "default_operator": "and",
                }
            }
        else:
            body["query"] = {"match_all": {}}

        response: dict = self.client.search(index=index_name, body=body)
        return self._build_search_result(response, page=page, size=size)

    def search_documents_by_body(self, index_name: str, body: dict) -> dict:
        body = deepcopy(body or {})
        if not self.client.indices.exists(index=index_name):
            size: int = int(body.get("size") or 10)
            return {
                "exists": False,
                "total": 0,
                "page": 1,
                "size": size,
                "hits": [],
            }

        if not body:
            body = {
                "size": 10,
                "query": {"match_all": {}},
            }
        body.setdefault("track_total_hits", True)
        response: dict = self.client.search(index=index_name, body=body)
        return self._build_search_result(
            response,
            page=1,
            size=int(body.get("size") or 10),
        )

    def _build_search_result(
        self,
        response: dict,
        *,
        page: int,
        size: int,
    ) -> dict:
        total: t.Any = response.get("hits", {}).get("total", 0)
        if isinstance(total, dict):
            total = total.get("value", 0)

        return {
            "exists": True,
            "total": int(total or 0),
            "page": page,
            "size": size,
            "took": int(response.get("took") or 0),
            "hits": [
                {
                    "id": hit.get("_id"),
                    "score": hit.get("_score"),
                    "source": self._normalize_json_value(
                        hit.get("_source", {})
                    ),
                }
                for hit in response.get("hits", {}).get("hits", [])
            ],
        }

    def _normalize_json_value(self, value: t.Any) -> t.Any:
        if isinstance(value, dict):
            return {
                key: self._normalize_json_value(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [self._normalize_json_value(item) for item in value]
        if isinstance(value, int) and abs(value) > JS_SAFE_INTEGER:
            return str(value)
        return value


class NativeProcessRunner:
    """Spawn a native PGSync process for same-database jobs."""

    def __init__(
        self,
        manager: "SyncManager",
        job: dict,
        connection: dict,
        *,
        resume_incremental_only: bool = False,
    ) -> None:
        self.manager = manager
        self.job = deepcopy(job)
        self.connection = deepcopy(connection)
        self.store = manager.store
        self.schema_path: str = manager.write_native_schema(job)
        self.thread: t.Optional[threading.Thread] = None
        self.bootstrap_process: t.Optional[subprocess.Popen] = None
        self.daemon_process: t.Optional[subprocess.Popen] = None
        self.stop_event: threading.Event = threading.Event()
        self.stop_requested: bool = False
        self.lease_lost: bool = False
        self.resume_incremental_only: bool = bool(resume_incremental_only)
        self.resume_strategy: str = RESUME_STRATEGY_NATIVE_INCREMENTAL

    @property
    def job_id(self) -> str:
        return self.job["id"]

    def start(self) -> None:
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def _consume_output(self) -> None:
        if not self.process or not self.process.stdout:
            return
        for line in self.process.stdout:
            text: str = line.rstrip()
            if text:
                self.manager.append_log(self.job_id, text)

    def _wait_for_exit(self) -> None:
        if not self.process:
            return
        exit_code: int = self.process.wait()
        if self.lease_lost:
            status: str = "failed"
            phase: str = "lease_lost"
        elif self.stop_requested:
            status = "stopped"
            phase = "stopped"
        else:
            status = "failed"
            phase = "failed"
        patch: dict = {
            "status": status,
            "phase": phase,
            "exit_code": exit_code,
            "pid": None,
        }
        if self.lease_lost:
            patch["last_error"] = (
                "Redis job lease lost; the current Studio instance stopped the task."
            )
        elif exit_code != 0 and not self.stop_requested:
            patch["last_error"] = f"原生 PGSync 进程退出，退出码：{exit_code}"
            self.manager.append_log(
                self.job_id,
                f"原生 PGSync 进程异常退出，退出码：{exit_code}",
            )
        self.manager.update_runtime(self.job_id, patch)
        self.manager.on_runner_finished(self.job_id)

    def stop(self) -> None:
        self.stop_requested = True
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
        self.manager.update_runtime(
            self.job_id,
            {
                "status": "stopped",
                "pid": None,
                "phase": "stopping",
            },
        )

    def handle_lease_lost(self) -> None:
        self.lease_lost = True
        self.stop_requested = True
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()

    def run(self) -> None:
        try:
            self.manager.update_runtime(
                self.job_id,
                {
                    "status": "starting",
                    "mode": "native",
                    "phase": (
                        "native_incremental"
                        if self.resume_incremental_only
                        else "native_bootstrap"
                    ),
                    "schema_path": self.schema_path,
                    "last_error": "",
                    "exit_code": None,
                    "resume_strategy": self.resume_strategy,
                },
            )
            if self.resume_incremental_only:
                self.manager.append_log(
                    self.job_id,
                    "检测到存量基线已完成，跳过 Native bootstrap，直接恢复增量。",
                )
                self._ensure_resume_checkpoint()
            else:
                self._run_bootstrap()
            if self.stop_requested:
                return
            self._start_daemon()
            while not self.stop_event.wait(DEFAULT_POLL_INTERVAL_SECONDS):
                if (
                    self.daemon_process
                    and self.daemon_process.poll() is not None
                ):
                    exit_code: int = int(self.daemon_process.poll() or 0)
                    raise StudioError(
                        f"原生 PGSync 增量进程异常退出，退出码：{exit_code}"
                    )
        except Exception as exc:
            logger.exception("原生 PGSync 同步失败")
            self.manager.append_log(
                self.job_id,
                f"原生 PGSync 同步失败：{exc}",
            )
            runtime: dict = self.store.get_job_runtime(self.job_id)
            lease_lost: bool = bool(
                self.lease_lost or runtime.get("phase") == "lease_lost"
            )
            self.manager.update_runtime(
                self.job_id,
                {
                    "status": "failed",
                    "phase": "lease_lost" if lease_lost else "failed",
                    "last_error": (
                        runtime.get("last_error") if lease_lost else str(exc)
                    ),
                    "pid": None,
                },
            )
        else:
            if self.lease_lost:
                patch: dict = {
                    "status": "failed",
                    "phase": "lease_lost",
                    "pid": None,
                }
            else:
                patch = self.manager.build_resumable_stop_runtime_patch(
                    self.job_id,
                    self.store.get_job_runtime(self.job_id),
                )
                patch["pid"] = None
            self.manager.update_runtime(self.job_id, patch)
        finally:
            self._terminate_process(self.bootstrap_process)
            self._terminate_process(self.daemon_process)
            self.manager.on_runner_finished(self.job_id)

    def _native_command(self, *, daemon: bool, bootstrap: bool) -> list[str]:
        command: list[str] = [
            sys.executable,
            "-u",
            "-m",
            "pgsync.sync",
            "-c",
            self.schema_path,
        ]
        if daemon:
            command.append("-d")
        if bootstrap:
            command.append("-b")
        return command

    def _spawn_native_process(
        self,
        *,
        daemon: bool,
        bootstrap: bool,
    ) -> subprocess.Popen:
        return subprocess.Popen(
            self._native_command(daemon=daemon, bootstrap=bootstrap),
            cwd=self.manager.repo_root,
            env=self.manager.build_native_env(self.connection),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

    def _run_native_checkpoint_probe(self) -> t.Optional[str]:
        script: str = "\n".join(
            [
                "import sys",
                "from pgsync.sync import Sync",
                "from pgsync.utils import config_loader",
                "",
                "config_path = sys.argv[1]",
                "for doc in config_loader(config=config_path, schema_url=None, s3_schema_url=None):",
                "    sync = Sync(doc, validate=False)",
                "    checkpoint = sync.checkpoint",
                "    if checkpoint is not None:",
                f"        print('{NATIVE_SETUP_CHECKPOINT_MARKER}' + str(checkpoint), flush=True)",
            ]
        )
        process = subprocess.Popen(
            [sys.executable, "-c", script, self.schema_path],
            cwd=self.manager.repo_root,
            env=self.manager.build_native_env(self.connection),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        checkpoint: t.Optional[str] = None
        try:
            if process.stdout:
                for line in process.stdout:
                    text: str = line.rstrip()
                    if not text:
                        continue
                    if text.startswith(NATIVE_SETUP_CHECKPOINT_MARKER):
                        checkpoint = text[
                            len(NATIVE_SETUP_CHECKPOINT_MARKER) :
                        ].strip()
                    else:
                        self.manager.append_log(
                            self.job_id,
                            f"[native-checkpoint] {text}",
                        )
            exit_code: int = process.wait()
        finally:
            self._terminate_process(process)
        if exit_code != 0:
            raise StudioError(
                f"原生 PGSync checkpoint 校验失败，退出码：{exit_code}"
            )
        return checkpoint

    def _ensure_resume_checkpoint(self) -> None:
        checkpoint: t.Optional[str] = self._run_native_checkpoint_probe()
        if checkpoint:
            return
        raise StudioError(
            "未找到原生 PGSync checkpoint，无法只恢复增量。请重新执行全量同步。"
        )

    def _run_bootstrap(self) -> None:
        self.manager.append_log(
            self.job_id,
            "开始执行原生 PGSync bootstrap。",
        )
        process = self._spawn_native_process(daemon=False, bootstrap=True)
        self.bootstrap_process = process
        self.manager.update_runtime(
            self.job_id,
            {
                "status": "running",
                "pid": process.pid,
                "phase": "native_bootstrap",
            },
        )
        try:
            if process.stdout:
                for line in process.stdout:
                    text: str = line.rstrip()
                    if text:
                        self.manager.append_log(
                            self.job_id,
                            f"[native-bootstrap] {text}",
                        )
                    if self.stop_requested:
                        break
            exit_code: int = process.wait()
        finally:
            self.bootstrap_process = None
        if self.stop_requested:
            return
        if exit_code != 0:
            raise StudioError(
                f"原生 PGSync bootstrap 失败，退出码：{exit_code}"
            )
        now: str = utcnow()
        self.manager.update_runtime(
            self.job_id,
            {
                "pid": None,
                "native_bootstrap_at": now,
                "last_full_sync_at": now,
                "last_full_sync_ts": utc_timestamp(),
                "baseline_completed": True,
                "resume_strategy": self.resume_strategy,
            },
        )
        self.manager.append_log(
            self.job_id,
            "原生 PGSync bootstrap 完成。",
        )

    def _start_daemon(self) -> None:
        self.manager.append_log(
            self.job_id,
            "开始启动原生 PGSync 增量进程。",
        )
        process = self._spawn_native_process(daemon=True, bootstrap=False)
        self.daemon_process = process
        self.manager.update_runtime(
            self.job_id,
            {
                "status": "running",
                "pid": process.pid,
                "native_started_at": utcnow(),
                "phase": "native_incremental",
                "baseline_completed": True,
                "resume_strategy": self.resume_strategy,
            },
        )
        threading.Thread(
            target=self._consume_daemon_output,
            daemon=True,
        ).start()

    def _consume_daemon_output(self) -> None:
        if not self.daemon_process or not self.daemon_process.stdout:
            return
        for line in self.daemon_process.stdout:
            text: str = line.rstrip()
            if text:
                self.manager.append_log(
                    self.job_id,
                    f"[native-daemon] {text}",
                )

    def stop(self) -> None:
        self.stop_requested = True
        self.stop_event.set()
        self._terminate_process(self.bootstrap_process)
        self._terminate_process(self.daemon_process)
        self.manager.update_runtime(
            self.job_id,
            {
                "status": "stopped",
                "pid": None,
                "phase": "stopping",
            },
        )

    def handle_lease_lost(self) -> None:
        self.lease_lost = True
        self.stop_requested = True
        self.stop_event.set()
        self._terminate_process(self.bootstrap_process)
        self._terminate_process(self.daemon_process)

    def _terminate_process(
        self,
        process: t.Optional[subprocess.Popen],
    ) -> None:
        if not process or process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()


class AssemblyRunner:
    """Application-side multi-database assembly runner."""

    def __init__(
        self,
        manager: "SyncManager",
        job: dict,
        *,
        incremental_only: bool = False,
        resume_strategy: str = RESUME_STRATEGY_ASSEMBLY_INCREMENTAL,
        full_sync_phase: str = "assembly_full_sync",
        incremental_phase: str = "assembly_incremental",
    ) -> None:
        self.manager = manager
        self.store = manager.store
        self.db = manager.db
        self.job = deepcopy(job)
        self.stop_event: threading.Event = threading.Event()
        self.thread: t.Optional[threading.Thread] = None
        self.incremental_lock: threading.Lock = threading.Lock()
        self.lease_lost: bool = False
        self.incremental_only: bool = bool(incremental_only)
        self.resume_strategy: str = resume_strategy
        self.full_sync_phase: str = full_sync_phase
        self.incremental_phase: str = incremental_phase
        self.connections: dict[str, dict] = {
            connection["id"]: connection
            for connection in self.manager.store.list_connections()
        }
        self.tables_by_id: dict[str, dict] = {
            table["id"]: deepcopy(table) for table in self.job["tables"]
        }
        self.relations_by_parent: dict[str, list[dict]] = {}
        self.relation_by_child: dict[str, dict] = {}
        for relation in self.job["relations"]:
            self.relations_by_parent.setdefault(
                relation["parent_table_id"],
                [],
            ).append(deepcopy(relation))
            self.relation_by_child[relation["child_table_id"]] = deepcopy(
                relation
            )

    @property
    def job_id(self) -> str:
        return self.job["id"]

    @property
    def root_table(self) -> dict:
        return self.tables_by_id[self.job["root_table_id"]]

    def build_read_plan(
        self,
        connections: t.Optional[t.Iterable[dict]] = None,
    ) -> t.Optional[AssemblyReadPlan]:
        """Build one fixed source route per connection for an assembly work unit."""
        builder = getattr(self.db, "build_assembly_read_plan", None)
        if not callable(builder):
            return None
        if connections is None:
            connection_ids: list[str] = list(
                dict.fromkeys(
                    str(table["connection_id"]) for table in self.job["tables"]
                )
            )
            connections = [
                self.connections[connection_id]
                for connection_id in connection_ids
            ]
        return builder(connections)

    def read_plan_kwargs(
        self,
        read_plan: t.Optional[AssemblyReadPlan],
    ) -> dict[str, t.Any]:
        """Keep older test/adaptor implementations source compatible."""
        return (
            {"read_plan": read_plan}
            if read_plan is not None and read_plan.enabled
            else {}
        )

    def build_primary_read_plan(
        self,
        *,
        reason: str,
    ) -> t.Optional[AssemblyReadPlan]:
        builder = getattr(self.db, "build_primary_assembly_read_plan", None)
        if not callable(builder):
            return None
        connection_ids: list[str] = list(
            dict.fromkeys(
                str(table["connection_id"]) for table in self.job["tables"]
            )
        )
        return builder(
            [self.connections[item] for item in connection_ids],
            reason=reason,
        )

    def table_soft_delete_column(self, table: dict) -> str:
        return normalize_soft_delete_column(table.get("soft_delete_column"))

    def row_is_soft_deleted(self, table: dict, row: dict) -> bool:
        return soft_delete_row_is_deleted(
            row,
            self.table_soft_delete_column(table),
        )

    def filter_active_rows(
        self,
        table: dict,
        rows: t.Iterable[dict[str, t.Any]],
    ) -> list[dict[str, t.Any]]:
        return [
            dict(row)
            for row in rows
            if not self.row_is_soft_deleted(table, dict(row))
        ]

    def split_root_rows_for_sync(
        self,
        rows: t.Iterable[dict[str, t.Any]],
    ) -> tuple[list[dict[str, t.Any]], list[str]]:
        root: dict = self.root_table
        active_rows: list[dict[str, t.Any]] = []
        deletions: list[str] = []
        for row in rows:
            normalized_row: dict[str, t.Any] = dict(row)
            if self.row_is_soft_deleted(root, normalized_row):
                deletions.append(self.document_id(root, normalized_row))
                continue
            active_rows.append(normalized_row)
        return active_rows, deletions

    def start(self) -> None:
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.manager.update_runtime(
            self.job_id,
            {
                "status": "stopping",
                "phase": "stopping",
            },
        )

    def log(self, message: str) -> None:
        self.manager.append_log(self.job_id, message)

    def _default_cursor_state(self) -> dict:
        return {
            "applied": {
                "cursor": None,
                "pk": [],
            },
            "pending": None,
        }

    def _normalize_checkpoint(
        self, payload: t.Any, primary_key: list[str]
    ) -> dict:
        return normalize_checkpoint_payload(payload, primary_key)

    def cursor_state_for_table(self, runtime: dict, table: dict) -> dict:
        raw_state: t.Any = deepcopy(runtime.get("cursors", {})).get(
            table["id"]
        )
        if isinstance(raw_state, dict) and (
            "applied" in raw_state or "pending" in raw_state
        ):
            return {
                "applied": self._normalize_checkpoint(
                    raw_state.get("applied"),
                    table["primary_key"],
                ),
                "pending": (
                    self._normalize_checkpoint(
                        raw_state.get("pending"),
                        table["primary_key"],
                    )
                    if raw_state.get("pending")
                    else None
                ),
            }
        if isinstance(raw_state, dict) and (
            "cursor" in raw_state or "pk" in raw_state
        ):
            return {
                "applied": self._normalize_checkpoint(
                    raw_state,
                    table["primary_key"],
                ),
                "pending": None,
            }
        return {
            "applied": self._normalize_checkpoint(
                raw_state,
                table["primary_key"],
            ),
            "pending": None,
        }

    def cursor_checkpoint_from_row(self, table: dict, row: dict) -> dict:
        cursor_column: str = table["cursor_column"]
        return {
            "cursor": row.get(cursor_column),
            "pk": [row.get(column) for column in table["primary_key"]],
        }

    def load_root_queue(self, runtime: dict) -> list[tuple]:
        queue: list[tuple] = []
        for entry in runtime.get("assembly_root_queue", []):
            if isinstance(entry, list):
                queue.append(tuple(entry))
            elif isinstance(entry, tuple):
                queue.append(entry)
        return queue

    def dump_root_queue(self, queue: list[tuple]) -> list[list[t.Any]]:
        return [list(root_key) for root_key in queue]

    def merge_root_queue(
        self, existing: list[tuple], incoming: t.Iterable[tuple]
    ) -> list[tuple]:
        merged: list[tuple] = []
        seen: set[tuple] = set()
        for root_key in [*existing, *incoming]:
            normalized: tuple = tuple(root_key)
            if normalized in seen:
                continue
            seen.add(normalized)
            merged.append(normalized)
        return merged

    def mark_incremental_batch_running(self) -> None:
        self.manager.update_runtime(
            self.job_id,
            {
                "assembly_incremental_running": True,
                "assembly_current_batch_started_at": utcnow(),
            },
        )

    def clear_incremental_batch_running(self) -> None:
        self.manager.update_runtime(
            self.job_id,
            {
                "assembly_incremental_running": False,
                "assembly_current_batch_started_at": None,
            },
        )

    def run(self) -> None:
        metric_token = set_metrics_context(self.job)
        self.manager.update_runtime(
            self.job_id,
            {
                "status": "running",
                "mode": "assembly",
                "phase": (
                    self.incremental_phase
                    if self.incremental_only
                    else self.full_sync_phase
                ),
                "last_error": "",
                "resume_strategy": self.resume_strategy,
            },
        )
        try:
            search = SearchAdapter(self.store.snapshot()["services"])
            search.ensure_index(
                self.job["index_name"],
                mapping=self.manager.build_search_mapping(self.job),
            )
            if self.incremental_only:
                self.log(
                    "检测到存量基线已完成，跳过 Assembly full_sync，直接恢复增量。"
                )
                self.manager.update_runtime(
                    self.job_id,
                    {
                        "baseline_completed": True,
                        "resume_strategy": self.resume_strategy,
                    },
                )
                self.incremental_sync(search)
            else:
                self.full_sync(search)
            interval: int = int(
                self.job.get("poll_interval_seconds")
                or DEFAULT_POLL_INTERVAL_SECONDS
            )
            resync_interval: int = int(
                self.job.get("full_resync_interval_seconds") or 1800
            )
            while not self.stop_event.wait(interval):
                if not self.incremental_only:
                    runtime: dict = self.store.get_job_runtime(self.job_id)
                    last_full_sync: float = float(
                        runtime.get("last_full_sync_ts") or 0
                    )
                    if utc_timestamp() - last_full_sync >= resync_interval:
                        self.full_sync(search)
                        continue
                self.incremental_sync(search)
        except Exception as exc:
            logger.exception("应用层组装同步失败")
            self.log(f"应用层组装同步失败：{exc}")
            runtime: dict = self.store.get_job_runtime(self.job_id)
            lease_lost: bool = bool(
                self.lease_lost or runtime.get("phase") == "lease_lost"
            )
            self.manager.update_runtime(
                self.job_id,
                {
                    "status": "failed",
                    "phase": "lease_lost" if lease_lost else "failed",
                    "last_error": (
                        runtime.get("last_error") if lease_lost else str(exc)
                    ),
                },
            )
        else:
            if self.lease_lost:
                patch = {
                    "status": "failed",
                    "phase": "lease_lost",
                }
            else:
                patch = self.manager.build_resumable_stop_runtime_patch(
                    self.job_id,
                    self.store.get_job_runtime(self.job_id),
                )
            self.manager.update_runtime(self.job_id, patch)
        finally:
            self.manager.on_runner_finished(self.job_id)
            reset_metrics_context(metric_token)

    def supports_batch_one_hop_full_sync(self) -> bool:
        root_id: str = self.root_table["id"]
        relations: list[dict] = self.relations_by_parent.get(root_id, [])
        if not relations:
            return False
        for relation in self.job["relations"]:
            if relation["parent_table_id"] != root_id:
                return False
            if self.relations_by_parent.get(relation["child_table_id"]):
                return False
        return True

    def relation_parent_key_from_row(
        self,
        relation: dict,
        row: dict,
    ) -> tuple[t.Any, ...]:
        return tuple(
            row.get(pair["parent_field"]) for pair in relation["field_pairs"]
        )

    def relation_child_key_from_row(
        self,
        relation: dict,
        row: dict,
    ) -> tuple[t.Any, ...]:
        return tuple(
            row.get(pair["child_field"]) for pair in relation["field_pairs"]
        )

    def batch_prefetch_one_hop_children(
        self,
        root_rows: list[dict],
        read_plan: t.Optional[AssemblyReadPlan] = None,
    ) -> dict[str, dict[tuple[t.Any, ...], list[dict]]]:
        return self.batch_prefetch_tree(
            self.root_table["id"],
            root_rows,
            read_plan=read_plan,
        )

    def batch_prefetch_tree(
        self,
        table_id: str,
        parent_rows: list[dict],
        *,
        read_plan: t.Optional[AssemblyReadPlan] = None,
    ) -> dict[str, dict[tuple[t.Any, ...], list[dict]]]:
        prefetched: dict[str, dict[tuple[t.Any, ...], list[dict]]] = {}
        if not parent_rows:
            return prefetched
        for relation in self.relations_by_parent.get(table_id, []):
            eligible_parent_rows: list[dict] = [
                row
                for row in parent_rows
                if self.manager.relation_row_matches_fixed_conditions(
                    relation,
                    row,
                    source="parent",
                )
            ]
            if not eligible_parent_rows:
                prefetched[relation["id"]] = {}
                continue
            child_table: dict = self.tables_by_id[relation["child_table_id"]]
            child_connection: dict = self.connections[
                child_table["connection_id"]
            ]
            filter_groups: list[dict[str, t.Any]] = [
                {
                    pair["child_field"]: row.get(pair["parent_field"])
                    for pair in relation["field_pairs"]
                }
                for row in eligible_parent_rows
            ]
            child_rows: list[dict] = self.db.fetch_rows_by_filter_groups(
                child_connection,
                child_table["schema"],
                child_table["table"],
                filter_groups=filter_groups,
                fixed_filters=self.manager.relation_fixed_eq_filters(
                    relation,
                    source="child",
                ),
                order_by=child_table["primary_key"],
                **self.read_plan_kwargs(read_plan),
            )
            child_rows = self.filter_active_rows(child_table, child_rows)
            grouped: dict[tuple[t.Any, ...], list[dict]] = {}
            for child_row in child_rows:
                normalized_child_row: dict = dict(child_row)
                key: tuple[t.Any, ...] = self.relation_child_key_from_row(
                    relation,
                    normalized_child_row,
                )
                grouped.setdefault(key, []).append(normalized_child_row)
            prefetched[relation["id"]] = grouped
            if child_rows:
                prefetched.update(
                    self.batch_prefetch_tree(
                        child_table["id"],
                        [dict(child_row) for child_row in child_rows],
                        read_plan=read_plan,
                    )
                )
        return prefetched

    def assemble_one_hop_document(
        self,
        row: dict,
        prefetched_children: dict[str, dict[tuple[t.Any, ...], list[dict]]],
    ) -> dict:
        return self.assemble_prefetched_document(
            self.root_table["id"],
            row,
            prefetched_children,
        )

    def assemble_prefetched_document(
        self,
        table_id: str,
        row: dict,
        prefetched_children: dict[str, dict[tuple[t.Any, ...], list[dict]]],
    ) -> dict:
        document: dict = dict(row)
        for relation in self.relations_by_parent.get(table_id, []):
            relation_type: str = normalize_relation_type(relation["type"])
            child_table: dict = self.tables_by_id[relation["child_table_id"]]
            key: tuple[t.Any, ...] = self.relation_parent_key_from_row(
                relation,
                row,
            )
            nested: list[dict] = [
                self.assemble_prefetched_document(
                    child_table["id"],
                    item,
                    prefetched_children,
                )
                for item in prefetched_children.get(relation["id"], {}).get(
                    key,
                    [],
                )
            ]
            if not relation_is_collection(relation_type):
                document[child_table["alias"]] = nested[0] if nested else None
            else:
                document[child_table["alias"]] = nested
        return document

    def supports_batch_one_hop_rebuild(self) -> bool:
        return self.supports_batch_one_hop_full_sync()

    def root_key_from_row(self, row: dict) -> tuple[t.Any, ...]:
        return tuple(
            row.get(column) for column in self.root_table["primary_key"]
        )

    def build_index_actions_for_root_rows(
        self,
        root_rows: list[dict],
        *,
        read_plan: t.Optional[AssemblyReadPlan] = None,
    ) -> list[dict]:
        if not root_rows:
            return []

        root: dict = self.root_table
        if self.job["relations"]:
            prefetched_children = self.batch_prefetch_tree(
                root["id"],
                root_rows,
                read_plan=read_plan,
            )
            documents: list[dict] = [
                self.assemble_prefetched_document(
                    root["id"],
                    row,
                    prefetched_children,
                )
                for row in root_rows
            ]
        else:
            documents = [dict(row) for row in root_rows]

        return [
            {
                "_op_type": "index",
                "_index": self.job["index_name"],
                "_id": self.document_id(root, row),
                "_source": document,
            }
            for row, document in zip(root_rows, documents)
        ]

    def fetch_root_rows_by_keys(
        self,
        root_keys: list[tuple[t.Any, ...]],
        *,
        read_plan: t.Optional[AssemblyReadPlan] = None,
    ) -> dict[tuple[t.Any, ...], dict]:
        if not root_keys:
            return {}

        root: dict = self.root_table
        root_connection: dict = self.connections[root["connection_id"]]
        rows: list[dict] = self.db.fetch_rows_by_filter_groups(
            root_connection,
            root["schema"],
            root["table"],
            filter_groups=[
                dict(zip(root["primary_key"], tuple(root_key)))
                for root_key in root_keys
            ],
            order_by=root["primary_key"],
            **self.read_plan_kwargs(read_plan),
        )
        return {self.root_key_from_row(row): dict(row) for row in rows}

    def build_rebuild_actions_for_root_keys(
        self,
        root_keys: list[tuple[t.Any, ...]],
        *,
        read_plan: t.Optional[AssemblyReadPlan] = None,
    ) -> tuple[list[dict], list[str]]:
        root_rows_by_key: dict[tuple[t.Any, ...], dict] = (
            self.fetch_root_rows_by_keys(
                root_keys,
                read_plan=read_plan,
            )
        )
        root_rows: list[dict] = []
        deletions: list[str] = []
        for root_key in root_keys:
            normalized: tuple[t.Any, ...] = tuple(root_key)
            row: t.Optional[dict] = root_rows_by_key.get(normalized)
            if row is None:
                deletions.append(document_id_from_values(normalized))
                continue
            if self.row_is_soft_deleted(self.root_table, row):
                deletions.append(document_id_from_values(normalized))
                continue
            root_rows.append(row)

        actions: list[dict] = self.build_index_actions_for_root_rows(
            root_rows,
            read_plan=read_plan,
        )
        return actions, deletions

    def apply_root_rebuild_batch(
        self,
        search: SearchAdapter,
        root_keys: list[tuple[t.Any, ...]],
        *,
        read_plan: t.Optional[AssemblyReadPlan] = None,
    ) -> tuple[int, int]:
        actions, deletions = self.build_rebuild_actions_for_root_keys(
            root_keys,
            read_plan=read_plan,
        )
        if actions:
            search.bulk(actions)
        if deletions:
            search.delete_many(self.job["index_name"], deletions)
        if len(root_keys) <= 5:
            logger.warning(
                "[trace-rebuild][job:%s] batch_root_keys=%s deletions=%s actions=%s",
                self.job["id"],
                [list(root_key) for root_key in root_keys],
                deletions,
                len(actions),
            )
        return len(actions), len(deletions)

    def full_sync_pages(
        self,
        search: SearchAdapter,
        *,
        read_plan: t.Optional[AssemblyReadPlan] = None,
        retry_on_primary: bool = True,
    ) -> int:
        root: dict = self.root_table
        root_connection: dict = self.connections[root["connection_id"]]
        page_size: int = int(self.job.get("page_size") or 200)
        order_by: list[str] = root["primary_key"] or [root["table"]]
        offset: int = 0
        seek_after: t.Optional[list[t.Any]] = None
        use_seek_pagination: bool = bool(root["primary_key"]) and bool(
            order_by
        )
        total_docs: int = 0
        resolved_read_plan: t.Optional[AssemblyReadPlan] = (
            read_plan or self.build_read_plan()
        )
        if resolved_read_plan is not None:
            self.log(
                "Assembly full sync read plan: "
                + json.dumps(
                    resolved_read_plan.summary(),
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
        try:
            while True:
                if self.stop_event.is_set():
                    self.log("Assembly full sync stopped before completion.")
                    break
                rows: list[dict] = self.db.fetch_page(
                    root_connection,
                    root["schema"],
                    root["table"],
                    order_by=order_by,
                    limit=page_size,
                    offset=offset,
                    seek_after=(seek_after if use_seek_pagination else None),
                    **self.read_plan_kwargs(resolved_read_plan),
                )
                if not rows:
                    break
                root_rows, deletions = self.split_root_rows_for_sync(rows)
                actions: list[dict] = self.build_index_actions_for_root_rows(
                    root_rows,
                    read_plan=resolved_read_plan,
                )
                if actions:
                    search.bulk(actions)
                if deletions:
                    search.delete_many(self.job["index_name"], deletions)
                total_docs += len(actions)
                if use_seek_pagination:
                    seek_after = [
                        rows[-1].get(split_order_by_item(item)[0])
                        for item in order_by
                    ]
                else:
                    offset += len(rows)
                self.log(
                    f"Assembly full sync has written {total_docs} documents."
                )
        except ReplicaReadQueryError:
            if (
                not retry_on_primary
                or resolved_read_plan is None
                or not resolved_read_plan.uses_replica
                or not resolved_read_plan.fallback_to_primary
            ):
                raise
            self.log(
                "Assembly replica query failed; restarting the whole full sync on primary."
            )
            primary_plan: t.Optional[AssemblyReadPlan] = (
                self.build_primary_read_plan(
                    reason="replica_query_error_restart"
                )
            )
            return self.full_sync_pages(
                search,
                read_plan=primary_plan,
                retry_on_primary=False,
            )
        return total_docs

    def full_sync_recursive(self, search: SearchAdapter) -> int:
        root: dict = self.root_table
        root_connection: dict = self.connections[root["connection_id"]]
        page_size: int = int(self.job.get("page_size") or 200)
        order_by: list[str] = root["primary_key"] or [root["table"]]
        offset: int = 0
        total_docs: int = 0
        read_plan: t.Optional[AssemblyReadPlan] = self.build_read_plan()
        while True:
            if self.stop_event.is_set():
                self.log("检测到停止信号，提前结束 Assembly 全量同步。")
                break
            rows: list[dict] = self.db.fetch_page(
                root_connection,
                root["schema"],
                root["table"],
                order_by=order_by,
                limit=page_size,
                offset=offset,
                **self.read_plan_kwargs(read_plan),
            )
            if not rows:
                break
            root_rows, deletions = self.split_root_rows_for_sync(rows)
            actions: list[dict] = []
            for row in root_rows:
                document = self.assemble_document(
                    root["id"],
                    row,
                    read_plan=read_plan,
                )
                document_id = self.document_id(root, row)
                actions.append(
                    {
                        "_op_type": "index",
                        "_index": self.job["index_name"],
                        "_id": document_id,
                        "_source": document,
                    }
                )
            if actions:
                search.bulk(actions)
            if deletions:
                search.delete_many(self.job["index_name"], deletions)
            total_docs += len(actions)
            offset += len(rows)
            self.log(f"当前已写入 {total_docs} 条文档。")
        return total_docs

    def full_sync_batch_one_hop(self, search: SearchAdapter) -> int:
        root: dict = self.root_table
        root_connection: dict = self.connections[root["connection_id"]]
        page_size: int = int(self.job.get("page_size") or 200)
        order_by: list[str] = root["primary_key"] or [root["table"]]
        offset: int = 0
        total_docs: int = 0
        read_plan: t.Optional[AssemblyReadPlan] = self.build_read_plan()
        while True:
            if self.stop_event.is_set():
                self.log("检测到停止信号，提前结束 Assembly 全量同步。")
                break
            rows: list[dict] = self.db.fetch_page(
                root_connection,
                root["schema"],
                root["table"],
                order_by=order_by,
                limit=page_size,
                offset=offset,
                **self.read_plan_kwargs(read_plan),
            )
            if not rows:
                break
            root_rows, deletions = self.split_root_rows_for_sync(rows)
            prefetched_children = self.batch_prefetch_one_hop_children(
                root_rows,
                read_plan=read_plan,
            )
            actions: list[dict] = []
            for row in root_rows:
                actions.append(
                    {
                        "_op_type": "index",
                        "_index": self.job["index_name"],
                        "_id": self.document_id(root, row),
                        "_source": self.assemble_one_hop_document(
                            row,
                            prefetched_children,
                        ),
                    }
                )
            if actions:
                search.bulk(actions)
            if deletions:
                search.delete_many(self.job["index_name"], deletions)
            total_docs += len(actions)
            offset += len(rows)
            self.log(f"当前已写入 {total_docs} 条文档。")
        return total_docs

    def full_sync(
        self,
        search: SearchAdapter,
        *,
        cursor_seed: t.Optional[dict[str, dict[str, t.Any]]] = None,
    ) -> None:
        total_docs: int = 0
        self.log("开始执行全量同步。")
        self.manager.update_runtime(
            self.job_id,
            {
                "phase": self.full_sync_phase,
            },
        )
        if self.supports_batch_one_hop_rebuild():
            self.log(
                "Assembly 全量同步使用 root seek 分页 + 一跳子表批量预取模式。"
            )
            total_docs = self.full_sync_pages(search)
        else:
            if self.job["relations"]:
                self.log(
                    "Assembly 全量同步使用 root seek 分页 + 多跳关系批量预取组装模式。"
                )
            total_docs = self.full_sync_pages(search)
        if self.stop_event.is_set():
            return
        resolved_cursor_seed: dict[str, dict[str, t.Any]] = (
            self.build_cursor_seed()
            if cursor_seed is None
            else deepcopy(cursor_seed)
        )
        self.manager.update_runtime(
            self.job_id,
            {
                "status": "running",
                "phase": self.incremental_phase,
                "baseline_completed": True,
                "resume_strategy": self.resume_strategy,
                "cursors": resolved_cursor_seed,
                "assembly_incremental_running": False,
                "assembly_current_batch_started_at": None,
                "assembly_pending_batch_count": 0,
                "assembly_pending_batch_at": None,
                "assembly_root_queue": [],
                "assembly_root_queue_size": 0,
                "document_count": total_docs,
                "last_full_sync_at": utcnow(),
                "last_full_sync_ts": utc_timestamp(),
            },
        )
        self.log(f"全量同步完成，共写入 {total_docs} 条文档。")

    def seed_cursors(self) -> None:
        self.manager.update_runtime(
            self.job_id,
            {"cursors": self.build_cursor_seed()},
        )

    def build_cursor_seed(
        self,
        *,
        read_plan: t.Optional[AssemblyReadPlan] = None,
        retry_on_primary: bool = True,
    ) -> dict[str, dict[str, t.Any]]:
        cursors: dict = deepcopy(
            self.store.get_job_runtime(self.job_id).get("cursors", {})
        )
        resolved_read_plan: t.Optional[AssemblyReadPlan] = (
            read_plan or self.build_read_plan()
        )
        try:
            for table in self.job["tables"]:
                cursor_column: t.Optional[str] = table.get("cursor_column")
                if not cursor_column:
                    continue
                latest: list[dict] = self.db.fetch_rows_by_fields(
                    self.connections[table["connection_id"]],
                    table["schema"],
                    table["table"],
                    filters={},
                    order_by=[f"-{cursor_column}"]
                    + [f"-{column}" for column in table["primary_key"]],
                    limit=1,
                    **self.read_plan_kwargs(resolved_read_plan),
                )
                if latest:
                    cursors[table["id"]] = {
                        "applied": self.cursor_checkpoint_from_row(
                            table,
                            latest[-1],
                        ),
                        "pending": None,
                    }
                else:
                    cursors[table["id"]] = self._default_cursor_state()
        except ReplicaReadQueryError:
            if (
                not retry_on_primary
                or resolved_read_plan is None
                or not resolved_read_plan.uses_replica
                or not resolved_read_plan.fallback_to_primary
            ):
                raise
            self.log(
                "Assembly cursor seed replica query failed; restarting the whole seed read on primary."
            )
            return self.build_cursor_seed(
                read_plan=self.build_primary_read_plan(
                    reason="cursor_seed_replica_query_error_retry"
                ),
                retry_on_primary=False,
            )
        return cursors

    def incremental_sync(
        self,
        search: SearchAdapter,
        *,
        read_plan: t.Optional[AssemblyReadPlan] = None,
        retry_replica_query: bool = True,
    ) -> None:
        if not self.incremental_lock.acquire(blocking=False):
            self.log("Assembly 增量批次仍在执行，跳过本轮调度。")
            return

        self.manager.update_runtime(
            self.job_id,
            {
                "phase": self.incremental_phase,
            },
        )
        self.mark_incremental_batch_running()
        retry_primary_plan: t.Optional[AssemblyReadPlan] = None
        try:
            read_plan = read_plan or self.build_read_plan()
            runtime: dict = self.store.get_job_runtime(self.job_id)
            cursors: dict = deepcopy(runtime.get("cursors", {}))
            root_queue: list[tuple] = self.load_root_queue(runtime)
            changed_roots: list[tuple] = []
            seen_roots: set[tuple] = set(root_queue)
            pending_cursor_states: dict[str, dict] = {}
            fallback_full_sync: bool = False
            batch_limit: int = int(
                self.job.get("incremental_batch_size") or 100
            )

            for table in self.job["tables"]:
                cursor_column: t.Optional[str] = table.get("cursor_column")
                if not cursor_column:
                    fallback_full_sync = True
                    continue
                connection: dict = self.connections[table["connection_id"]]
                cursor_state: dict = self.cursor_state_for_table(
                    runtime,
                    table,
                )
                rows, next_checkpoint = self.db.fetch_changed_rows(
                    connection,
                    table["schema"],
                    table["table"],
                    cursor_column=cursor_column,
                    last_cursor=cursor_state["applied"].get("cursor"),
                    last_primary_key=cursor_state["applied"].get("pk") or [],
                    primary_key=table["primary_key"],
                    limit=batch_limit,
                    **self.read_plan_kwargs(read_plan),
                )
                if not rows:
                    continue
                self.log(
                    f"检测到 {len(rows)} 条变更数据，来源于 "
                    f"{table['schema']}.{table['table']}。"
                )
                resolved_roots: set[tuple] = set()
                for row in rows:
                    resolved_roots.update(
                        self.resolve_root_keys(
                            table["id"],
                            row,
                            read_plan=read_plan,
                        )
                    )
                if not resolved_roots:
                    self.log(
                        f"{table['schema']}.{table['table']} 检测到变更，但未能回溯到 root_id，请确认 delete / 解绑 / 多跳关系是否满足开发约束。"
                    )
                for root_key in resolved_roots:
                    normalized: tuple = tuple(root_key)
                    if normalized in seen_roots:
                        continue
                    seen_roots.add(normalized)
                    changed_roots.append(normalized)
                pending_cursor_states[table["id"]] = {
                    "applied": deepcopy(cursor_state["applied"]),
                    "pending": (
                        deepcopy(next_checkpoint) if next_checkpoint else None
                    ),
                }

            if pending_cursor_states:
                prepared_cursors: dict = deepcopy(cursors)
                for table_id, state in pending_cursor_states.items():
                    prepared_cursors[table_id] = state
                self.manager.update_runtime(
                    self.job_id,
                    {
                        "cursors": prepared_cursors,
                        "assembly_pending_batch_count": len(
                            pending_cursor_states
                        ),
                        "assembly_pending_batch_at": utcnow(),
                    },
                )

                merged_queue: list[tuple] = self.merge_root_queue(
                    root_queue,
                    changed_roots,
                )
                confirmed_cursors: dict = deepcopy(prepared_cursors)
                for table in self.job["tables"]:
                    state: t.Optional[dict] = pending_cursor_states.get(
                        table["id"]
                    )
                    if not state:
                        continue
                    existing_state: dict = self.cursor_state_for_table(
                        {"cursors": confirmed_cursors},
                        table,
                    )
                    confirmed_cursors[table["id"]] = {
                        "applied": max_checkpoint(
                            existing_state["applied"],
                            state.get("pending") or state["applied"],
                            table["primary_key"],
                        ),
                        "pending": None,
                    }
                self.manager.update_runtime(
                    self.job_id,
                    {
                        "cursors": confirmed_cursors,
                        "assembly_root_queue": self.dump_root_queue(
                            merged_queue
                        ),
                        "assembly_root_queue_size": len(merged_queue),
                        "assembly_pending_batch_count": 0,
                        "assembly_pending_batch_at": None,
                        "assembly_last_batch_confirmed_at": utcnow(),
                    },
                )
                cursors = confirmed_cursors
                root_queue = merged_queue

            if not root_queue and fallback_full_sync:
                self.log(
                    "存在未配置游标字段的数据表，且当前无可回放的 root 队列，回退执行全量同步。"
                )
                self.full_sync(search)
                return

            if not root_queue:
                if pending_cursor_states:
                    self.manager.update_runtime(
                        self.job_id,
                        {
                            "last_incremental_sync_at": utcnow(),
                            "last_incremental_sync_ts": utc_timestamp(),
                        },
                    )
                return

            roots_to_process: list[tuple] = root_queue[:batch_limit]
            started_at: float = time.perf_counter()
            updated_count: int = 0
            deleted_count: int = 0
            try:
                try:
                    updated_count, deleted_count = (
                        self.apply_root_rebuild_batch(
                            search,
                            roots_to_process,
                            read_plan=read_plan,
                        )
                    )
                except ReplicaReadQueryError:
                    if (
                        read_plan is None
                        or not read_plan.uses_replica
                        or not read_plan.fallback_to_primary
                    ):
                        raise
                    self.log(
                        "Assembly replica query failed; rebuilding the whole batch on primary."
                    )
                    read_plan = self.build_primary_read_plan(
                        reason="replica_query_error_retry"
                    )
                    updated_count, deleted_count = (
                        self.apply_root_rebuild_batch(
                            search,
                            roots_to_process,
                            read_plan=read_plan,
                        )
                    )

                remaining_queue: list[tuple] = root_queue[
                    len(roots_to_process) :
                ]
                self.manager.update_runtime(
                    self.job_id,
                    {
                        "cursors": cursors,
                        "assembly_root_queue": self.dump_root_queue(
                            remaining_queue
                        ),
                        "assembly_root_queue_size": len(remaining_queue),
                        "last_incremental_sync_at": utcnow(),
                        "last_incremental_sync_ts": utc_timestamp(),
                    },
                )
            except Exception:
                record_rebuild_batch(
                    self.job,
                    raw_root_keys=len(roots_to_process),
                    deduped_root_keys=len(roots_to_process),
                    worker_count=1,
                    updated_count=updated_count,
                    deleted_count=deleted_count,
                    elapsed_seconds=time.perf_counter() - started_at,
                    status="error",
                )
                raise
            record_rebuild_batch(
                self.job,
                raw_root_keys=len(roots_to_process),
                deduped_root_keys=len(roots_to_process),
                worker_count=1,
                updated_count=updated_count,
                deleted_count=deleted_count,
                elapsed_seconds=time.perf_counter() - started_at,
                status="success",
            )
            if updated_count or deleted_count:
                self.log(
                    f"增量同步完成，更新 {updated_count} 条文档，删除 {deleted_count} 条文档，剩余待处理 root 队列 {len(remaining_queue)} 条。"
                )
        except ReplicaReadQueryError:
            if (
                not retry_replica_query
                or read_plan is None
                or not read_plan.uses_replica
                or not read_plan.fallback_to_primary
            ):
                raise
            self.log(
                "Assembly replica query failed during change discovery; restarting the whole cycle on primary."
            )
            retry_primary_plan = self.build_primary_read_plan(
                reason="replica_discovery_query_error_retry"
            )
            if retry_primary_plan is None:
                raise
        finally:
            self.clear_incremental_batch_running()
            self.incremental_lock.release()
        if retry_primary_plan is not None:
            self.incremental_sync(
                search,
                read_plan=retry_primary_plan,
                retry_replica_query=False,
            )

    def handle_lease_lost(self) -> None:
        self.lease_lost = True
        self.stop_event.set()

    def document_id(self, table: dict, row: dict) -> str:
        return document_id_from_values(
            row.get(column) for column in table["primary_key"]
        )

    def assemble_document(
        self,
        table_id: str,
        row: dict,
        *,
        read_plan: t.Optional[AssemblyReadPlan] = None,
    ) -> dict:
        document: dict = dict(row)
        for relation in self.relations_by_parent.get(table_id, []):
            relation_type: str = normalize_relation_type(relation["type"])
            child_table: dict = self.tables_by_id[relation["child_table_id"]]
            nested: list[dict] = []
            if self.manager.relation_row_matches_fixed_conditions(
                relation,
                row,
                source="parent",
            ):
                child_connection: dict = self.connections[
                    child_table["connection_id"]
                ]
                filters: dict = {
                    pair["child_field"]: row.get(pair["parent_field"])
                    for pair in relation["field_pairs"]
                }
                filters.update(
                    self.manager.relation_fixed_eq_filters(
                        relation,
                        source="child",
                    )
                )
                child_rows: list[dict] = self.db.fetch_rows_by_fields(
                    child_connection,
                    child_table["schema"],
                    child_table["table"],
                    filters=filters,
                    order_by=child_table["primary_key"],
                    **self.read_plan_kwargs(read_plan),
                )
                child_rows = self.filter_active_rows(child_table, child_rows)
                nested = [
                    self.assemble_document(
                        child_table["id"],
                        child_row,
                        read_plan=read_plan,
                    )
                    for child_row in child_rows
                ]
            if not relation_is_collection(relation_type):
                document[child_table["alias"]] = nested[0] if nested else None
            else:
                document[child_table["alias"]] = nested
        return document

    def resolve_root_keys(
        self,
        table_id: str,
        row: dict,
        *,
        read_plan: t.Optional[AssemblyReadPlan] = None,
    ) -> set[tuple]:
        root: dict = self.root_table
        if table_id == root["id"]:
            return {tuple(row.get(column) for column in root["primary_key"])}

        relation: dict = self.relation_by_child[table_id]
        child_fixed_filters: dict[str, t.Any] = (
            self.manager.relation_fixed_eq_filters(
                relation,
                source="child",
            )
        )
        for field, value in child_fixed_filters.items():
            if row.get(field) != value:
                return set()
        parent_table: dict = self.tables_by_id[relation["parent_table_id"]]
        parent_connection: dict = self.connections[
            parent_table["connection_id"]
        ]
        filters: dict = {
            pair["parent_field"]: row.get(pair["child_field"])
            for pair in relation["field_pairs"]
        }
        parent_rows: list[dict] = self.db.fetch_rows_by_fields(
            parent_connection,
            parent_table["schema"],
            parent_table["table"],
            filters=filters,
            order_by=parent_table["primary_key"],
            **self.read_plan_kwargs(read_plan),
        )
        results: set[tuple] = set()
        for parent_row in parent_rows:
            if not self.manager.relation_row_matches_fixed_conditions(
                relation,
                parent_row,
                source="parent",
            ):
                continue
            results.update(
                self.resolve_root_keys(
                    parent_table["id"],
                    parent_row,
                    read_plan=read_plan,
                )
            )
        return results


class CrossDatabaseHybridRunner:
    """Cross-database runner with assembly full sync and dual incremental sync."""

    def __init__(
        self,
        manager: "SyncManager",
        job: dict,
        *,
        resume_incremental_only: bool = False,
    ) -> None:
        self.manager = manager
        self.store = manager.store
        self.job = deepcopy(job)
        self.root_table: dict = next(
            table
            for table in self.job["tables"]
            if table["id"] == self.job["root_table_id"]
        )
        self.root_connection: dict = self.manager.require_connection(
            self.root_table["connection_id"]
        )
        self.native_job: dict = self.manager.build_native_baseline_job(
            self.job
        )
        self.schema_path: str = self.manager.write_native_schema(
            self.native_job,
            filename="schema_native_baseline.json",
        )
        self.assembly_runner: AssemblyRunner = AssemblyRunner(
            manager,
            self.job,
            resume_strategy=RESUME_STRATEGY_CROSS_DATABASE_INCREMENTAL,
            full_sync_phase="cross_database_full_sync",
            incremental_phase="cross_database_incremental",
        )
        self.stop_event: threading.Event = threading.Event()
        self.thread: t.Optional[threading.Thread] = None
        self.bootstrap_process: t.Optional[subprocess.Popen] = None
        self.setup_process: t.Optional[subprocess.Popen] = None
        self.daemon_process: t.Optional[subprocess.Popen] = None
        self.stop_requested: bool = False
        self.lease_lost: bool = False
        self.resume_incremental_only: bool = bool(resume_incremental_only)
        self.resume_strategy: str = RESUME_STRATEGY_CROSS_DATABASE_INCREMENTAL

    @property
    def job_id(self) -> str:
        return self.job["id"]

    @property
    def assembly_fallback_enabled(self) -> bool:
        return bool(self.job.get("enable_assembly_fallback"))

    @property
    def recovery_scan_enabled(self) -> bool:
        return self.is_cross_database and self.assembly_fallback_enabled

    def start(self) -> None:
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_requested = True
        self.stop_event.set()
        self.assembly_runner.stop_event.set()
        self.manager.update_runtime(
            self.job_id,
            {
                "status": "stopping",
                "phase": "stopping",
            },
        )
        self._terminate_process(self.bootstrap_process)
        self._terminate_process(self.setup_process)
        self._terminate_process(self.daemon_process)

    def _native_command(self, *, daemon: bool, bootstrap: bool) -> list[str]:
        command: list[str] = [
            sys.executable,
            "-m",
            "pgsync.sync",
            "-c",
            self.schema_path,
        ]
        if daemon:
            command.append("-d")
        if bootstrap:
            command.append("-b")
        return command

    def _spawn_native_process(
        self,
        *,
        daemon: bool,
        bootstrap: bool,
    ) -> subprocess.Popen:
        env: dict[str, str] = self.manager.build_native_env(
            self.root_connection
        )
        return subprocess.Popen(
            self._native_command(daemon=daemon, bootstrap=bootstrap),
            cwd=self.manager.repo_root,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

    def _run_native_bootstrap(self) -> None:
        self.manager.append_log(
            self.job_id,
            "开始执行原生 PGSync bootstrap。",
        )
        process = self._spawn_native_process(daemon=False, bootstrap=True)
        self.bootstrap_process = process
        self.manager.update_runtime(
            self.job_id,
            {
                "native_bootstrap_pid": process.pid,
                "phase": "cross_database_native_bootstrap",
            },
        )
        try:
            if process.stdout:
                for line in process.stdout:
                    text: str = line.rstrip()
                    if text:
                        self.manager.append_log(
                            self.job_id,
                            f"[native-bootstrap] {text}",
                        )
                    if self.stop_requested:
                        break
            exit_code: int = process.wait()
        finally:
            self.bootstrap_process = None
            self.manager.update_runtime(
                self.job_id,
                {
                    "native_bootstrap_pid": None,
                },
            )
        if self.stop_requested:
            return
        if exit_code != 0:
            raise StudioError(
                f"原生 PGSync bootstrap 失败，退出码：{exit_code}"
            )
        self.manager.update_runtime(
            self.job_id,
            {
                "native_bootstrap_at": utcnow(),
            },
        )
        self.manager.append_log(
            self.job_id,
            "原生 PGSync bootstrap 完成。",
        )

    def _native_setup_command(self) -> list[str]:
        script: str = "\n".join(
            [
                "import sys",
                "from pgsync.sync import Sync",
                "from pgsync.utils import config_loader",
                "",
                "config_path = sys.argv[1]",
                "for doc in config_loader(config=config_path, schema_url=None, s3_schema_url=None):",
                "    sync = Sync(doc, validate=False)",
                "    sync.setup()",
                "    checkpoint = sync.txid_current",
                "    sync.checkpoint = checkpoint",
                f"    print('{NATIVE_SETUP_CHECKPOINT_MARKER}' + str(checkpoint), flush=True)",
            ]
        )
        return [sys.executable, "-c", script, self.schema_path]

    def _spawn_native_setup_process(self) -> subprocess.Popen:
        env: dict[str, str] = self.manager.build_native_env(
            self.root_connection
        )
        return subprocess.Popen(
            self._native_setup_command(),
            cwd=self.manager.repo_root,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

    def _prepare_native_incremental_start(self) -> None:
        self.manager.append_log(
            self.job_id,
            "开始准备原生 PGSync 增量起点，仅执行 setup 和 checkpoint 初始化。",
        )
        process = self._spawn_native_setup_process()
        self.setup_process = process
        self.manager.update_runtime(
            self.job_id,
            {
                "native_setup_pid": process.pid,
                "phase": "cross_database_setup",
            },
        )
        checkpoint: t.Optional[int] = None
        try:
            if process.stdout:
                for line in process.stdout:
                    text: str = line.rstrip()
                    if text:
                        if text.startswith(NATIVE_SETUP_CHECKPOINT_MARKER):
                            raw_checkpoint: str = text[
                                len(NATIVE_SETUP_CHECKPOINT_MARKER) :
                            ]
                            checkpoint = int(raw_checkpoint.strip())
                        else:
                            self.manager.append_log(
                                self.job_id,
                                f"[native-setup] {text}",
                            )
                    if self.stop_requested:
                        break
            exit_code: int = process.wait()
        finally:
            self.setup_process = None
            self.manager.update_runtime(
                self.job_id,
                {
                    "native_setup_pid": None,
                },
            )
        if self.stop_requested:
            return
        if exit_code != 0:
            raise StudioError(f"原生 PGSync setup 失败，退出码：{exit_code}")
        if checkpoint is None:
            raise StudioError(
                "原生 PGSync setup 完成，但未返回增量起点 checkpoint。"
            )
        self.manager.update_runtime(
            self.job_id,
            {
                "native_setup_at": utcnow(),
                "native_incremental_start_checkpoint": checkpoint,
            },
        )
        self.manager.append_log(
            self.job_id,
            f"原生 PGSync setup 完成，增量起点 checkpoint={checkpoint}。",
        )

    def _consume_daemon_output(self) -> None:
        if not self.daemon_process or not self.daemon_process.stdout:
            return
        for line in self.daemon_process.stdout:
            text: str = line.rstrip()
            if text:
                self.manager.append_log(
                    self.job_id,
                    f"[native-daemon] {text}",
                )

    def _run_native_checkpoint_probe(self) -> t.Optional[str]:
        script: str = "\n".join(
            [
                "import sys",
                "from pgsync.sync import Sync",
                "from pgsync.utils import config_loader",
                "",
                "config_path = sys.argv[1]",
                "for doc in config_loader(config=config_path, schema_url=None, s3_schema_url=None):",
                "    sync = Sync(doc, validate=False)",
                "    checkpoint = sync.checkpoint",
                "    if checkpoint is not None:",
                f"        print('{NATIVE_SETUP_CHECKPOINT_MARKER}' + str(checkpoint), flush=True)",
            ]
        )
        process = subprocess.Popen(
            [sys.executable, "-c", script, self.schema_path],
            cwd=self.manager.repo_root,
            env=self.manager.build_native_env(self.root_connection),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        checkpoint: t.Optional[str] = None
        try:
            if process.stdout:
                for line in process.stdout:
                    text: str = line.rstrip()
                    if not text:
                        continue
                    if text.startswith(NATIVE_SETUP_CHECKPOINT_MARKER):
                        checkpoint = text[
                            len(NATIVE_SETUP_CHECKPOINT_MARKER) :
                        ].strip()
                    else:
                        self.manager.append_log(
                            self.job_id,
                            f"[native-checkpoint] {text}",
                        )
            exit_code: int = process.wait()
        finally:
            self._terminate_process(process)
        if exit_code != 0:
            raise StudioError(
                f"原生 PGSync checkpoint 校验失败，退出码：{exit_code}"
            )
        return checkpoint

    def _ensure_resume_checkpoint(self) -> None:
        checkpoint: t.Optional[str] = self._run_native_checkpoint_probe()
        if checkpoint:
            return
        raise StudioError(
            "未找到原生 PGSync checkpoint，无法只恢复增量。请重新执行全量同步。"
        )

    def _start_native_daemon(self) -> None:
        self.manager.append_log(
            self.job_id,
            "开始启动原生 PGSync 增量进程。",
        )
        process = self._spawn_native_process(daemon=True, bootstrap=False)
        self.daemon_process = process
        self.manager.update_runtime(
            self.job_id,
            {
                "status": "running",
                "native_pid": process.pid,
                "native_started_at": utcnow(),
                "phase": "cross_database_incremental",
                "baseline_completed": True,
                "resume_strategy": self.resume_strategy,
            },
        )
        threading.Thread(
            target=self._consume_daemon_output,
            daemon=True,
        ).start()

    def _run_assembly_incremental_cycle(
        self,
        search: SearchAdapter,
    ) -> None:
        runtime: dict = self.store.get_job_runtime(self.job_id)
        if runtime.get("assembly_incremental_running"):
            self.manager.append_log(
                self.job_id,
                "Assembly incremental batch is still running; skipping this round.",
            )
            return
        self.assembly_runner.incremental_sync(search)

    def _pending_assembly_root_queue_size(self) -> int:
        runtime: dict = self.store.get_job_runtime(self.job_id)
        return max(0, int(runtime.get("assembly_root_queue_size") or 0))

    def run(self) -> None:
        search: t.Optional[SearchAdapter] = None
        try:
            self.manager.update_runtime(
                self.job_id,
                {
                    "status": "starting",
                    "mode": "native",
                    "phase": (
                        "cross_database_incremental"
                        if self.resume_incremental_only
                        else "cross_database_setup"
                    ),
                    "cross_database": True,
                    "assembly_fallback_enabled": self.assembly_fallback_enabled,
                    "schema_path": self.schema_path,
                    "native_pid": None,
                    "native_bootstrap_pid": None,
                    "native_setup_pid": None,
                    "last_error": "",
                    "exit_code": None,
                    "resume_strategy": self.resume_strategy,
                },
            )
            search = SearchAdapter(self.store.snapshot()["services"])
            search.ensure_index(
                self.job["index_name"],
                mapping=self.manager.build_search_mapping(self.job),
            )
            if self.resume_incremental_only:
                self.manager.append_log(
                    self.job_id,
                    "检测到存量基线已完成，跳过跨库 setup 和 Assembly full_sync，直接恢复增量。",
                )
                self.manager.update_runtime(
                    self.job_id,
                    {
                        "baseline_completed": True,
                        "resume_strategy": self.resume_strategy,
                    },
                )
                self._ensure_resume_checkpoint()
            else:
                self.manager.append_log(
                    self.job_id,
                    "Cross-database incremental will use the shared assembly rebuild pipeline.",
                )
                self._prepare_native_incremental_start()
                if self.stop_requested:
                    return
                self.manager.append_log(
                    self.job_id,
                    "开始执行跨库存量补齐。",
                )
                self.manager.update_runtime(
                    self.job_id,
                    {
                        "phase": "cross_database_full_sync",
                    },
                )
                self.assembly_runner.full_sync(search)
                self.manager.append_log(
                    self.job_id,
                    "跨库存量补齐完成。",
                )
            if self.stop_requested:
                return

            self._start_native_daemon()
            interval: int = int(
                self.job.get("poll_interval_seconds")
                or DEFAULT_POLL_INTERVAL_SECONDS
            )
            next_cycle_delay: int = (
                0 if self.assembly_fallback_enabled else interval
            )
            while True:
                if next_cycle_delay > 0 and self.stop_event.wait(
                    next_cycle_delay
                ):
                    break
                if (
                    self.daemon_process
                    and self.daemon_process.poll() is not None
                ):
                    exit_code: int = int(self.daemon_process.poll() or 0)
                    raise StudioError(
                        f"原生 PGSync 增量进程异常退出，退出码：{exit_code}"
                    )
                if self.stop_requested:
                    break
                if not self.assembly_fallback_enabled:
                    next_cycle_delay = interval
                    continue
                runtime: dict = self.store.get_job_runtime(self.job_id)
                if runtime.get("assembly_incremental_running"):
                    self.manager.append_log(
                        self.job_id,
                        "Assembly 增量批次仍在执行，继续让 Native 主链路优先处理，本轮不启动新的 Assembly 批次。",
                    )
                    next_cycle_delay = interval
                    continue
                self._run_assembly_incremental_cycle(search)
                pending_queue_size: int = (
                    self._pending_assembly_root_queue_size()
                )
                next_cycle_delay = 0 if pending_queue_size > 0 else interval
        except Exception as exc:
            logger.exception("跨库 native/assembly 混合同步失败")
            self.manager.append_log(
                self.job_id,
                f"跨库 native/assembly 混合同步失败：{exc}",
            )
            runtime: dict = self.store.get_job_runtime(self.job_id)
            lease_lost: bool = bool(
                self.lease_lost or runtime.get("phase") == "lease_lost"
            )
            self.manager.update_runtime(
                self.job_id,
                {
                    "status": "failed",
                    "phase": "lease_lost" if lease_lost else "failed",
                    "last_error": (
                        runtime.get("last_error") if lease_lost else str(exc)
                    ),
                },
            )
        else:
            if self.lease_lost:
                patch = {
                    "status": "failed",
                    "phase": "lease_lost",
                }
            elif self.stop_requested:
                patch = self.manager.build_resumable_stop_runtime_patch(
                    self.job_id,
                    self.store.get_job_runtime(self.job_id),
                )
            else:
                patch = {
                    "status": "running",
                    "phase": "cross_database_incremental",
                }
            self.manager.update_runtime(self.job_id, patch)
        finally:
            self._terminate_process(self.setup_process)
            self._terminate_process(self.daemon_process)
            self.manager.on_runner_finished(self.job_id)

    def _terminate_process(
        self,
        process: t.Optional[subprocess.Popen],
    ) -> None:
        if not process or process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()

    def handle_lease_lost(self) -> None:
        self.lease_lost = True
        self.stop_requested = True
        self.stop_event.set()
        self.assembly_runner.handle_lease_lost()
        self._terminate_process(self.bootstrap_process)
        self._terminate_process(self.setup_process)
        self._terminate_process(self.daemon_process)


class CrossDatabaseHybridRunnerV2:
    """Runner using native LISTEN capture plus assembly rebuild."""

    def __init__(
        self,
        manager: "SyncManager",
        job: dict,
        *,
        resume_incremental_only: bool = False,
    ) -> None:
        self.manager = manager
        self.store = manager.store
        self.job = deepcopy(job)
        self.root_table: dict = next(
            table
            for table in self.job["tables"]
            if table["id"] == self.job["root_table_id"]
        )
        self.root_connection: dict = self.manager.require_connection(
            self.root_table["connection_id"]
        )
        self.is_cross_database: bool = (
            self.manager.job_has_cross_database_tables(self.job)
        )
        self.full_sync_phase: str = (
            "cross_database_full_sync"
            if self.is_cross_database
            else "assembly_full_sync"
        )
        self.incremental_phase: str = (
            "cross_database_incremental"
            if self.is_cross_database
            else "native_incremental"
        )
        self.setup_phase: str = (
            "cross_database_setup"
            if self.is_cross_database
            else "native_setup"
        )
        self.native_job: dict = self.manager.build_native_baseline_job(
            self.job
        )
        self.schema_path: str = self.manager.write_native_schema(
            self.native_job,
            filename="schema_native_baseline.json",
        )
        self.assembly_runner: AssemblyRunner = AssemblyRunner(
            manager,
            self.job,
            resume_strategy=self.manager.expected_resume_strategy(
                self.job,
                "native",
            ),
            full_sync_phase=self.full_sync_phase,
            incremental_phase=self.incremental_phase,
        )
        self.stop_event: threading.Event = threading.Event()
        self.thread: t.Optional[threading.Thread] = None
        self.stop_requested: bool = False
        self.lease_lost: bool = False
        self.resume_incremental_only: bool = bool(resume_incremental_only)
        self.resume_strategy: str = self.manager.expected_resume_strategy(
            self.job,
            "native",
        )
        self.incremental_run_lock: threading.Lock = threading.Lock()
        self.queue_lock: threading.Lock = threading.Lock()
        self.recovery_scan_thread: t.Optional[threading.Thread] = None
        self.native_activity_event: threading.Event = threading.Event()
        self.native_recovery_event: threading.Event = threading.Event()
        self.native_listener_states: dict[str, dict[str, t.Any]] = {}
        self.native_listener_specs: t.Optional[dict[str, dict[str, t.Any]]] = (
            None
        )
        self.native_listener_threads: dict[str, threading.Thread] = {}
        self.native_listener_stop_events: dict[str, threading.Event] = {}
        self.native_listener_notify_runtime_update_ts: dict[str, float] = {}
        self.native_event_history_by_source: dict[str, dict[str, t.Any]] = {}
        self.last_native_event_ts: float = 0.0
        services: dict = self.store.snapshot().get("services", {})
        self.root_queue: t.Union[RedisRootQueue, NoopRootQueue] = (
            RedisRootQueue.from_services(
                services,
                job_id=self.job_id,
            )
        )
        self.fixed_payload_fields_by_table_id: dict[str, tuple[str, ...]] = {}
        self.table_ids_by_location: dict[tuple[str, str, str], list[str]] = {}
        self.tables_by_location: dict[
            tuple[str, str, str], list[dict[str, t.Any]]
        ] = {}
        for table in self.job["tables"]:
            location_key: tuple[str, str, str] = self._location_key(
                table["connection_id"],
                table["schema"],
                table["table"],
            )
            self.table_ids_by_location.setdefault(location_key, []).append(
                table["id"]
            )
            self.tables_by_location.setdefault(location_key, []).append(
                deepcopy(table)
            )
        for relation in self.job["relations"]:
            fixed_fields: list[str] = [
                condition["field"]
                for condition in self.manager.relation_fixed_conditions(
                    relation,
                    source="child",
                )
                if condition.get("field")
                and (
                    (condition.get("operator") or "eq").strip().lower() == "eq"
                )
            ]
            if fixed_fields:
                merged_fields: set[str] = set(
                    self.fixed_payload_fields_by_table_id.get(
                        relation["child_table_id"],
                        (),
                    )
                )
                merged_fields.update(fixed_fields)
                self.fixed_payload_fields_by_table_id[
                    relation["child_table_id"]
                ] = tuple(sorted(merged_fields))

    @property
    def job_id(self) -> str:
        return self.job["id"]

    @property
    def assembly_fallback_enabled(self) -> bool:
        return bool(self.job.get("enable_assembly_fallback"))

    @property
    def durable_change_events_enabled(self) -> bool:
        return bool(self.job.get("enable_durable_change_events"))

    @property
    def recovery_scan_enabled(self) -> bool:
        return True

    @property
    def periodic_recovery_scan_enabled(self) -> bool:
        return self.assembly_fallback_enabled

    def _durable_change_event_batch_size(self) -> int:
        configured: t.Any = self.job.get("durable_change_event_batch_size")
        try:
            return max(
                1,
                int(configured or DEFAULT_DURABLE_CHANGE_EVENT_BATCH_SIZE),
            )
        except (TypeError, ValueError):
            return DEFAULT_DURABLE_CHANGE_EVENT_BATCH_SIZE

    def _durable_change_event_replay_window(self) -> int:
        configured: t.Any = self.job.get("durable_change_event_replay_window")
        try:
            return max(
                0,
                int(configured or DEFAULT_DURABLE_CHANGE_EVENT_REPLAY_WINDOW),
            )
        except (TypeError, ValueError):
            return DEFAULT_DURABLE_CHANGE_EVENT_REPLAY_WINDOW

    def _durable_change_event_processed_window(self) -> int:
        configured: t.Any = self.job.get(
            "durable_change_event_processed_window"
        )
        try:
            configured_window: int = int(
                configured or DEFAULT_DURABLE_CHANGE_EVENT_PROCESSED_WINDOW
            )
        except (TypeError, ValueError):
            configured_window = DEFAULT_DURABLE_CHANGE_EVENT_PROCESSED_WINDOW
        return max(
            1,
            max(
                self._durable_change_event_replay_window(),
                configured_window,
            ),
        )

    def _event_cursor_key(self, connection_id: str, schema: str) -> str:
        return f"{connection_id}:{schema.lower()}"

    def _durable_event_cursor(self, runtime: dict, key: str) -> int:
        cursors: t.Any = runtime.get("native_change_event_cursors") or {}
        value: t.Any = cursors.get(key) if isinstance(cursors, dict) else None
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError):
            return 0

    def _durable_processed_event_ids(
        self, runtime: dict, key: str
    ) -> set[int]:
        processed: t.Any = (
            runtime.get("native_change_event_processed_ids") or {}
        )
        values: t.Any = (
            processed.get(key) if isinstance(processed, dict) else None
        )
        if not isinstance(values, list):
            return set()
        normalized: set[int] = set()
        for value in values:
            try:
                normalized.add(max(0, int(value or 0)))
            except (TypeError, ValueError):
                continue
        return {value for value in normalized if value > 0}

    def _trim_durable_processed_event_ids(
        self,
        event_ids: t.Iterable[int],
        *,
        last_event_id: int,
    ) -> list[int]:
        lower_bound: int = max(
            0,
            int(last_event_id or 0)
            - self._durable_change_event_processed_window(),
        )
        return sorted(
            {
                int(event_id)
                for event_id in event_ids
                if int(event_id) > lower_bound
            }
        )

    def _iter_durable_event_groups(self) -> list[dict[str, t.Any]]:
        grouped: dict[tuple[str, str], dict[str, t.Any]] = {}
        for (
            connection_id,
            schema,
            table,
        ), tables in self.tables_by_location.items():
            group_key: tuple[str, str] = (connection_id, schema)
            group: dict[str, t.Any] = grouped.setdefault(
                group_key,
                {
                    "connection_id": connection_id,
                    "connection": self.manager.require_connection(
                        connection_id
                    ),
                    "schema": schema,
                    "table_names": set(),
                },
            )
            group["table_names"].add(table)
        return [
            {
                **group,
                "table_names": sorted(group["table_names"]),
                "cursor_key": self._event_cursor_key(
                    group["connection_id"],
                    group["schema"],
                ),
            }
            for group in grouped.values()
        ]

    def _build_durable_event_cursor_seed(self) -> dict[str, int]:
        if not self.durable_change_events_enabled:
            return {}
        seed: dict[str, int] = {}
        for group in self._iter_durable_event_groups():
            seed[group["cursor_key"]] = self.manager.db.max_change_event_id(
                group["connection"],
                group["schema"],
                table_names=group["table_names"],
            )
        return seed

    def _build_durable_processed_seed(
        self,
        cursors: dict[str, int],
    ) -> dict[str, list[int]]:
        if not self.durable_change_events_enabled:
            return {}
        processed: dict[str, list[int]] = {}
        replay_window: int = self._durable_change_event_replay_window()
        if replay_window <= 0:
            return processed
        for group in self._iter_durable_event_groups():
            cursor_key: str = group["cursor_key"]
            last_event_id: int = int(cursors.get(cursor_key) or 0)
            if last_event_id <= 0:
                continue
            after_id: int = max(0, last_event_id - replay_window)
            event_ids: list[int] = self.manager.db.fetch_change_event_ids(
                group["connection"],
                group["schema"],
                table_names=group["table_names"],
                after_id=after_id,
                limit=replay_window,
            )
            processed[cursor_key] = self._trim_durable_processed_event_ids(
                event_ids,
                last_event_id=last_event_id,
            )
        return processed

    def _decode_change_event_payload(self, event: dict) -> dict:
        payload: t.Any = event.get("payload")
        if isinstance(payload, dict):
            return dict(payload)
        if isinstance(payload, str):
            decoded: t.Any = json.loads(payload)
            return decoded if isinstance(decoded, dict) else {}
        return {}

    def _drain_durable_change_events(self) -> bool:
        if not self.durable_change_events_enabled:
            return False
        runtime: dict = self.store.get_job_runtime(self.job_id)
        cursor_patch: dict[str, int] = deepcopy(
            runtime.get("native_change_event_cursors") or {}
        )
        processed_patch: dict[str, list[int]] = deepcopy(
            runtime.get("native_change_event_processed_ids") or {}
        )
        batch_size: int = self._durable_change_event_batch_size()
        replay_window: int = self._durable_change_event_replay_window()
        total_events: int = 0
        total_roots: int = 0
        total_enqueued: int = 0
        max_event_id: int = int(runtime.get("durable_last_event_id") or 0)
        lag_by_group: dict[str, int] = {}
        lag_exact_by_group: dict[str, bool] = {}
        backlog_present_by_group: dict[str, bool] = {}

        for group in self._iter_durable_event_groups():
            cursor_key: str = group["cursor_key"]
            last_event_id: int = self._durable_event_cursor(
                {"native_change_event_cursors": cursor_patch},
                cursor_key,
            )
            after_id: int = max(0, last_event_id - replay_window)
            processed_ids: set[int] = self._durable_processed_event_ids(
                {"native_change_event_processed_ids": processed_patch},
                cursor_key,
            )
            fetched_events: list[dict] = self.manager.db.fetch_change_events(
                group["connection"],
                group["schema"],
                table_names=group["table_names"],
                after_id=after_id,
                limit=batch_size + 1,
                exclude_ids=sorted(processed_ids),
            )
            has_more: bool = len(fetched_events) > batch_size
            events: list[dict] = fetched_events[:batch_size]
            backlog_present_by_group[cursor_key] = has_more
            lag_by_group[cursor_key] = 1 if has_more else 0
            lag_exact_by_group[cursor_key] = not has_more
            if not events:
                continue

            root_keys: set[tuple[t.Any, ...]] = set()
            group_max_event_id: int = last_event_id
            handled_event_ids: set[int] = set()
            for event in events:
                event_id: int = int(event.get("id") or 0)
                if event_id in processed_ids:
                    continue
                payload: dict = self._decode_change_event_payload(event)
                if not payload:
                    raise StudioError(
                        f"Durable change event {event_id} has invalid payload."
                    )
                table_ids: list[str] = self.table_ids_by_location.get(
                    self._location_key(
                        group["connection_id"],
                        str(
                            payload.get("schema")
                            or event.get("schema_name")
                            or ""
                        ),
                        str(
                            payload.get("table")
                            or event.get("table_name")
                            or ""
                        ),
                    ),
                    [],
                )
                handled_event_ids.add(event_id)
                if not table_ids:
                    group_max_event_id = max(group_max_event_id, event_id)
                    continue
                event_roots, errors = self._resolve_payload_root_keys(
                    group["connection_id"],
                    table_ids,
                    payload,
                )
                if errors:
                    raise StudioError(
                        "Durable change event %s fan-out failed: %s"
                        % (event_id, "; ".join(errors[:5]))
                    )
                root_keys.update(event_roots)
                group_max_event_id = max(group_max_event_id, event_id)

            enqueued_count: int = self._enqueue_root_keys(
                root_keys,
                source=f"durable:{group['connection_id']}:{group['schema']}",
            )
            cursor_patch[cursor_key] = max(
                self._durable_event_cursor(
                    {"native_change_event_cursors": cursor_patch},
                    cursor_key,
                ),
                group_max_event_id,
            )
            processed_patch[cursor_key] = (
                self._trim_durable_processed_event_ids(
                    [*processed_ids, *handled_event_ids],
                    last_event_id=cursor_patch[cursor_key],
                )
            )
            total_events += len(handled_event_ids)
            total_roots += len(root_keys)
            total_enqueued += enqueued_count
            max_event_id = max(max_event_id, group_max_event_id)

        if not total_events and not lag_by_group:
            return False
        self.manager.update_runtime(
            self.job_id,
            {
                "native_change_event_cursors": cursor_patch,
                "native_change_event_processed_ids": processed_patch,
                "durable_change_events_enabled": True,
                "durable_last_drain_at": utcnow(),
                "durable_last_drain_event_count": total_events,
                "durable_last_drain_root_key_count": total_roots,
                "durable_last_drain_enqueued_count": total_enqueued,
                "durable_last_event_id": max_event_id,
                "durable_event_lag_by_group": lag_by_group,
                "durable_event_lag": sum(lag_by_group.values()),
                "durable_event_lag_exact_by_group": lag_exact_by_group,
                "durable_event_lag_exact": all(lag_exact_by_group.values()),
                "durable_event_backlog_present_by_group": (
                    backlog_present_by_group
                ),
                "durable_event_backlog_present": any(
                    backlog_present_by_group.values()
                ),
                "durable_event_lag_observed_at": utcnow(),
            },
        )
        if total_events:
            self.manager.append_log(
                self.job_id,
                "Durable change outbox drained: "
                f"events={total_events}, roots={total_roots}, "
                f"enqueued={total_enqueued}, "
                f"backlog={'yes' if any(backlog_present_by_group.values()) else 'no'}.",
            )
        return total_events > 0 or total_enqueued > 0

    def _location_key(
        self,
        connection_id: str,
        schema: str,
        table: str,
    ) -> tuple[str, str, str]:
        return (connection_id, schema.lower(), table.lower())

    def _connection_kwargs(self, connection: dict) -> dict[str, t.Any]:
        kwargs: dict[str, t.Any] = {
            "host": connection["host"],
            "port": connection["port"],
            "user": connection["username"],
            "password": connection.get("password") or "",
        }
        if connection.get("url"):
            kwargs["url"] = connection["url"]
        return kwargs

    def _listener_keepalive_options(self) -> dict[str, int]:
        return {
            "keepalives": 1,
            "keepalives_idle": int(
                os.environ.get(
                    "PGSYNC_STUDIO_LISTENER_KEEPALIVES_IDLE_SECONDS",
                    DEFAULT_LISTENER_KEEPALIVES_IDLE_SECONDS,
                )
            ),
            "keepalives_interval": int(
                os.environ.get(
                    "PGSYNC_STUDIO_LISTENER_KEEPALIVES_INTERVAL_SECONDS",
                    DEFAULT_LISTENER_KEEPALIVES_INTERVAL_SECONDS,
                )
            ),
            "keepalives_count": int(
                os.environ.get(
                    "PGSYNC_STUDIO_LISTENER_KEEPALIVES_COUNT",
                    DEFAULT_LISTENER_KEEPALIVES_COUNT,
                )
            ),
            "connect_timeout": int(
                os.environ.get(
                    "PGSYNC_STUDIO_LISTENER_CONNECT_TIMEOUT_SECONDS",
                    DEFAULT_LISTENER_CONNECT_TIMEOUT_SECONDS,
                )
            ),
        }

    def _listener_notify_runtime_update_interval_seconds(self) -> int:
        try:
            return max(
                1,
                int(
                    os.environ.get(
                        "PGSYNC_STUDIO_LISTENER_NOTIFY_RUNTIME_UPDATE_INTERVAL_SECONDS",
                        DEFAULT_LISTENER_NOTIFY_RUNTIME_UPDATE_INTERVAL_SECONDS,
                    )
                ),
            )
        except (TypeError, ValueError):
            return DEFAULT_LISTENER_NOTIFY_RUNTIME_UPDATE_INTERVAL_SECONDS

    def _native_idle_recovery_threshold_seconds(self) -> int:
        configured: t.Any = self.job.get(
            "native_idle_recovery_threshold_seconds"
        )
        if configured in (None, ""):
            configured = os.environ.get(
                "PGSYNC_STUDIO_NATIVE_IDLE_RECOVERY_THRESHOLD_SECONDS"
            )
        try:
            return max(
                30,
                int(
                    configured
                    or DEFAULT_NATIVE_IDLE_RECOVERY_THRESHOLD_SECONDS
                ),
            )
        except (TypeError, ValueError):
            return DEFAULT_NATIVE_IDLE_RECOVERY_THRESHOLD_SECONDS

    def _listener_runtime_snapshot(self) -> dict[str, dict[str, t.Any]]:
        return {
            connection_id: deepcopy(state)
            for connection_id, state in self.native_listener_states.items()
        }

    def _set_health_status(
        self,
        status: str,
        *,
        reason: str = "",
        extra: t.Optional[dict[str, t.Any]] = None,
    ) -> None:
        patch: dict[str, t.Any] = {
            "health_status": status,
            "health_reason": reason,
            "listener_states": self._listener_runtime_snapshot(),
        }
        if extra:
            patch.update(extra)
        self.manager.update_runtime(self.job_id, patch)

    def _update_listener_state(
        self,
        connection_id: str,
        *,
        state: t.Optional[str] = None,
        reason: t.Optional[str] = None,
        extra: t.Optional[dict[str, t.Any]] = None,
    ) -> None:
        current: dict[str, t.Any] = deepcopy(
            self.native_listener_states.get(connection_id, {})
        )
        if state is not None:
            current["state"] = state
        if reason is not None:
            current["reason"] = reason
        if extra:
            current.update(extra)
        self.native_listener_states[connection_id] = current
        healthy_count: int = sum(
            1
            for item in self.native_listener_states.values()
            if item.get("state") == "listening"
        )
        recovering_count: int = sum(
            1
            for item in self.native_listener_states.values()
            if item.get("state") == "recovering"
        )
        degraded_count: int = sum(
            1
            for item in self.native_listener_states.values()
            if item.get("state") == "degraded"
        )
        total_count: int = len(self.native_listener_states)
        patch: dict[str, t.Any] = {
            "listener_states": self._listener_runtime_snapshot(),
            "listener_total_count": total_count,
            "listener_healthy_count": healthy_count,
            "listener_recovering_count": recovering_count,
            "listener_degraded_count": degraded_count,
        }
        current_health: str = str(
            self.store.get_job_runtime(self.job_id).get("health_status")
            or HEALTH_STATUS_RUNNING
        )
        if degraded_count > 0:
            patch["health_status"] = HEALTH_STATUS_DEGRADED
            patch["health_reason"] = reason or current.get("reason") or ""
        elif recovering_count > 0:
            patch["health_status"] = HEALTH_STATUS_RECOVERING
            patch["health_reason"] = reason or current.get("reason") or ""
        elif total_count > 0 and healthy_count == total_count:
            if current_health in {
                HEALTH_STATUS_DEGRADED,
                HEALTH_STATUS_RECOVERING,
            }:
                patch["health_status"] = HEALTH_STATUS_RUNNING
                patch["health_reason"] = ""
        self.manager.update_runtime(self.job_id, patch)

    def _record_listener_notify(self, connection_id: str) -> None:
        now_at: str = utcnow()
        now_ts: float = utc_timestamp()
        current: dict[str, t.Any] = deepcopy(
            self.native_listener_states.get(connection_id, {})
        )
        current["state"] = current.get("state") or "listening"
        current["reason"] = current.get("reason") or "listening"
        current["last_notify_at"] = now_at
        current["last_notify_ts"] = now_ts
        self.native_listener_states[connection_id] = current

        previous_ts: float = float(
            self.native_listener_notify_runtime_update_ts.get(connection_id)
            or 0.0
        )
        if (
            now_ts - previous_ts
            < self._listener_notify_runtime_update_interval_seconds()
        ):
            return
        self.native_listener_notify_runtime_update_ts[connection_id] = now_ts
        self._update_listener_state(
            connection_id,
            state=str(current.get("state") or "listening"),
            reason=str(current.get("reason") or "listening"),
            extra={
                "last_notify_at": now_at,
                "last_notify_ts": now_ts,
            },
        )

    def _request_recovery_scan(self, reason: str) -> None:
        self.native_recovery_event.set()
        self._set_health_status(
            HEALTH_STATUS_DEGRADED,
            reason=reason,
            extra={
                "last_recovery_requested_at": utcnow(),
                "last_recovery_reason": reason,
            },
        )

    def _maybe_request_idle_recovery_scan(self) -> None:
        runtime: dict = self.store.get_job_runtime(self.job_id)
        if runtime.get("assembly_recovery_scan_running"):
            return
        last_native_event_ts_value: t.Any = runtime.get("last_native_event_ts")
        if last_native_event_ts_value in (None, ""):
            return
        try:
            last_native_event_ts: float = float(last_native_event_ts_value)
        except (TypeError, ValueError):
            return
        if (
            utc_timestamp() - last_native_event_ts
        ) < self._native_idle_recovery_threshold_seconds():
            return
        for group in self._iter_physical_recovery_groups():
            cursor_column: t.Optional[str] = group.get("cursor_column")
            if not cursor_column:
                continue
            connection: dict = self.manager.require_connection(
                group["connection_id"]
            )
            rows, _ = self.manager.db.fetch_changed_rows(
                connection,
                group["schema"],
                group["table"],
                cursor_column=cursor_column,
                last_cursor=None,
                last_primary_key=[],
                primary_key=group["primary_key"],
                limit=1,
            )
            if not rows:
                continue
            latest_row: dict = rows[-1]
            latest_checkpoint: dict[str, t.Any] = {
                "cursor": latest_row.get(cursor_column),
                "pk": [
                    latest_row.get(column) for column in group["primary_key"]
                ],
            }
            for table in group["tables"]:
                cursor_state: dict = (
                    self.assembly_runner.cursor_state_for_table(
                        runtime,
                        table,
                    )
                )
                if checkpoint_is_after(
                    latest_checkpoint,
                    cursor_state["applied"],
                    table["primary_key"],
                ):
                    self._request_recovery_scan(
                        "source cursor advanced while no native events arrived"
                    )
                    return

    def _candidate_rows_from_payload(self, payload: dict) -> list[dict]:
        tg_op: str = str(payload.get("tg_op") or "").upper()
        if tg_op == "TRUNCATE":
            self.native_recovery_event.set()
            self.manager.append_log(
                self.job_id,
                f"TRUNCATE detected on {payload.get('schema')}.{payload.get('table')}; run a full rebuild if needed.",
            )
            return []
        candidate_rows: list[dict] = []
        new_row: t.Any = payload.get("new")
        old_row: t.Any = payload.get("old")
        if isinstance(new_row, dict) and new_row:
            candidate_rows.append(dict(new_row))
        if isinstance(old_row, dict) and old_row:
            candidate_rows.append(dict(old_row))
        return candidate_rows

    def _required_fixed_fields_for_table_ids(
        self,
        table_ids: t.Iterable[str],
    ) -> set[str]:
        required_fields: set[str] = set()
        for table_id in table_ids:
            required_fields.update(
                self.fixed_payload_fields_by_table_id.get(table_id, ())
            )
        return required_fields

    def _hydrate_row_fixed_fields(
        self,
        connection_id: str,
        schema: str,
        table: str,
        table_ids: t.Iterable[str],
        row: dict,
    ) -> tuple[dict, set[str]]:
        hydrated_row: dict = dict(row)
        required_fields: set[str] = self._required_fixed_fields_for_table_ids(
            table_ids
        )
        missing_fields: set[str] = {
            field for field in required_fields if field not in hydrated_row
        }
        if not missing_fields:
            return hydrated_row, set()

        location_key: tuple[str, str, str] = self._location_key(
            connection_id,
            schema,
            table,
        )
        physical_tables: list[dict[str, t.Any]] = self.tables_by_location.get(
            location_key,
            [],
        )
        if not physical_tables:
            return hydrated_row, missing_fields

        physical_table: dict[str, t.Any] = physical_tables[0]
        primary_key: list[str] = list(physical_table.get("primary_key") or [])
        if not primary_key:
            return hydrated_row, missing_fields

        filters: dict[str, t.Any] = {}
        for column in primary_key:
            if column not in hydrated_row:
                return hydrated_row, missing_fields
            value = hydrated_row.get(column)
            if value is None:
                return hydrated_row, missing_fields
            filters[column] = value

        columns: list[str] = list(
            dict.fromkeys([*primary_key, *sorted(missing_fields)])
        )
        rows: list[dict] = self.manager.db.fetch_rows_by_fields(
            self.manager.require_connection(connection_id),
            schema,
            table,
            filters=filters,
            columns=columns,
            order_by=primary_key,
            limit=1,
        )
        if not rows:
            return hydrated_row, missing_fields

        fetched_row: dict = rows[0]
        recovered_fields: list[str] = []
        for field in list(missing_fields):
            if field in fetched_row:
                hydrated_row[field] = fetched_row[field]
                recovered_fields.append(field)
        if recovered_fields:
            self.manager.append_log(
                self.job_id,
                "[native-listener:%s] hydrated missing fixed fields for %s.%s pk=%s: %s"
                % (
                    connection_id,
                    schema,
                    table,
                    filters,
                    ", ".join(sorted(recovered_fields)),
                ),
            )
        remaining_missing: set[str] = {
            field for field in missing_fields if field not in hydrated_row
        }
        return hydrated_row, remaining_missing

    def _hydrate_candidate_rows(
        self,
        connection_id: str,
        schema: str,
        table: str,
        table_ids: t.Iterable[str],
        rows: list[dict],
    ) -> tuple[list[dict], list[str]]:
        hydrated_rows: list[dict] = []
        errors: list[str] = []
        table_ids_list: list[str] = list(table_ids)
        for row in rows:
            hydrated_row, missing_fields = self._hydrate_row_fixed_fields(
                connection_id,
                schema,
                table,
                table_ids_list,
                row,
            )
            hydrated_rows.append(hydrated_row)
            if missing_fields:
                errors.append(
                    "missing fixed fields after hydration for %s.%s: %s"
                    % (
                        schema,
                        table,
                        ", ".join(sorted(missing_fields)),
                    )
                )
        return hydrated_rows, errors

    def _resolve_rows_root_keys(
        self,
        table_ids: t.Iterable[str],
        rows: t.Iterable[dict],
        *,
        read_plan: t.Optional[AssemblyReadPlan] = None,
    ) -> tuple[set[tuple], list[str]]:
        root_keys: set[tuple] = set()
        errors: list[str] = []
        candidate_rows: list[dict] = [
            dict(row) for row in rows if isinstance(row, dict)
        ]
        for table_id in table_ids:
            for row in candidate_rows:
                try:
                    root_keys.update(
                        self.assembly_runner.resolve_root_keys(
                            table_id,
                            row,
                            read_plan=read_plan,
                        )
                    )
                except ReplicaReadQueryError:
                    raise
                except Exception as exc:
                    errors.append(f"{table_id}: {exc}")
        return root_keys, errors

    def _iter_physical_recovery_groups(self) -> list[dict[str, t.Any]]:
        groups: list[dict[str, t.Any]] = []
        for location_key, tables in self.tables_by_location.items():
            if not tables:
                continue
            by_cursor: dict[str, list[dict[str, t.Any]]] = {}
            for table in tables:
                by_cursor.setdefault(
                    table.get("cursor_column") or "", []
                ).append(deepcopy(table))
            for cursor_column, grouped_tables in by_cursor.items():
                first_table: dict[str, t.Any] = grouped_tables[0]
                groups.append(
                    {
                        "location_key": location_key,
                        "connection_id": first_table["connection_id"],
                        "schema": first_table["schema"],
                        "table": first_table["table"],
                        "cursor_column": cursor_column,
                        "primary_key": list(first_table["primary_key"]),
                        "tables": grouped_tables,
                    }
                )
        return groups

    def _merged_confirmed_cursors(
        self,
        runtime: dict,
        pending_cursor_states: dict[str, dict[str, t.Any]],
    ) -> dict[str, dict[str, t.Any]]:
        confirmed_cursors: dict[str, dict[str, t.Any]] = deepcopy(
            runtime.get("cursors", {})
        )
        for table in self.job["tables"]:
            pending_state: t.Optional[dict[str, t.Any]] = (
                pending_cursor_states.get(table["id"])
            )
            if not pending_state:
                continue
            existing_state: dict[str, t.Any] = (
                self.assembly_runner.cursor_state_for_table(
                    {"cursors": confirmed_cursors},
                    table,
                )
            )
            next_checkpoint: dict[str, t.Any] = deepcopy(
                pending_state.get("pending") or pending_state["applied"]
            )
            confirmed_cursors[table["id"]] = {
                "applied": max_checkpoint(
                    existing_state["applied"],
                    next_checkpoint,
                    table["primary_key"],
                ),
                "pending": None,
            }
        return confirmed_cursors

    def _build_native_listener_specs(self) -> dict[str, dict[str, t.Any]]:
        specs: dict[str, dict[str, t.Any]] = {}
        tables_by_id: dict[str, dict] = {
            table["id"]: deepcopy(table) for table in self.job["tables"]
        }
        for table in self.job["tables"]:
            connection_id: str = table["connection_id"]
            spec: dict[str, t.Any] = specs.setdefault(
                connection_id,
                {
                    "connection": self.manager.require_connection(
                        connection_id
                    ),
                    "schemas": {},
                    "watched_locations": set(),
                },
            )
            schema_spec: dict[str, t.Any] = spec["schemas"].setdefault(
                table["schema"],
                {
                    "tables": set(),
                    "node_columns": {},
                    "payload_columns": {},
                    "user_defined_fkey_tables": {},
                },
            )
            column_names: list[str] = []
            if hasattr(self.manager.db, "get_table_columns"):
                try:
                    column_names = [
                        column["name"]
                        for column in self.manager.db.get_table_columns(
                            spec["connection"],
                            table["schema"],
                            table["table"],
                        )
                    ]
                except Exception:
                    column_names = []
            if not column_names:
                column_names = list(table["primary_key"])
            schema_spec["tables"].add(table["table"])
            schema_spec["node_columns"][table["table"]] = set(column_names)
            spec["watched_locations"].add(
                self._location_key(
                    connection_id,
                    table["schema"],
                    table["table"],
                )[1:]
            )

        for relation in self.job["relations"]:
            child_table: dict = tables_by_id[relation["child_table_id"]]
            child_spec: dict[str, t.Any] = specs[child_table["connection_id"]][
                "schemas"
            ][child_table["schema"]]
            child_fields: set[str] = {
                pair["child_field"] for pair in relation["field_pairs"]
            }
            fixed_fields: set[str] = {
                condition["field"]
                for condition in self.manager.relation_fixed_conditions(
                    relation,
                    source="child",
                )
                if condition.get("field")
                and (
                    (condition.get("operator") or "eq").strip().lower() == "eq"
                )
            }
            child_spec["user_defined_fkey_tables"].setdefault(
                child_table["table"],
                set(),
            ).update(child_fields)
            child_spec["node_columns"].setdefault(
                child_table["table"],
                set(),
            ).update(child_fields)
            if fixed_fields:
                child_spec["payload_columns"].setdefault(
                    child_table["table"],
                    set(),
                ).update(fixed_fields)

        return specs

    def start(self) -> None:
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_requested = True
        self.stop_event.set()
        self.assembly_runner.stop_event.set()
        self._stop_native_listeners()
        self.manager.update_runtime(
            self.job_id,
            {
                "status": "stopping",
                "phase": "stopping",
            },
        )

    def _prepare_native_incremental_start(self) -> None:
        self.manager.update_runtime(
            self.job_id,
            {
                "phase": self.setup_phase,
            },
        )
        self.manager.append_log(
            self.job_id,
            "Preparing native change-capture triggers.",
        )
        if self.native_listener_specs is None:
            self.native_listener_specs = self._build_native_listener_specs()
        for spec in self.native_listener_specs.values():
            connection: dict = spec["connection"]
            helper = PGSyncBase(
                connection["database"],
                **self._connection_kwargs(connection),
            )
            try:
                with helper.advisory_lock(
                    connection["database"],
                    max_retries=None,
                    retry_interval=0.1,
                ):
                    for schema, schema_spec in spec["schemas"].items():
                        if self.durable_change_events_enabled:
                            self.manager.db.ensure_change_event_outbox(
                                connection,
                                schema,
                            )
                        helper.ensure_function(schema)
                        helper.create_view(
                            self.job["index_name"],
                            schema,
                            set(schema_spec["tables"]),
                            {
                                table: sorted(columns)
                                for table, columns in schema_spec[
                                    "user_defined_fkey_tables"
                                ].items()
                            },
                            {
                                table: sorted(columns)
                                for table, columns in schema_spec[
                                    "node_columns"
                                ].items()
                            },
                            {
                                table: sorted(columns)
                                for table, columns in schema_spec[
                                    "payload_columns"
                                ].items()
                            },
                        )
                        helper.create_triggers(
                            schema,
                            tables=sorted(schema_spec["tables"]),
                            if_not_exists=True,
                        )
            finally:
                try:
                    helper.engine.dispose()
                except Exception:
                    pass
        self.manager.update_runtime(
            self.job_id,
            {
                "native_setup_at": utcnow(),
                "native_incremental_start_checkpoint": None,
                "durable_change_events_enabled": self.durable_change_events_enabled,
            },
        )

    def _ensure_resume_checkpoint(self) -> None:
        self._prepare_native_incremental_start()
        runtime: dict = self.store.get_job_runtime(self.job_id)
        if not runtime.get("baseline_completed"):
            raise StudioError(
                "Incremental resume requires a completed baseline."
            )
        if not runtime.get("cursors"):
            raise StudioError(
                "Incremental resume requires stored cursor state."
            )

    def _native_listener_loop(
        self,
        connection_id: str,
        connection: dict,
        watched_locations: set[tuple[str, str]],
        listener_stop_event: threading.Event,
    ) -> None:
        set_metrics_context(self.job)
        channel: str = connection["database"]
        while not self.stop_requested and not listener_stop_event.is_set():
            raw_connection = None
            cursor = None
            try:
                self._update_listener_state(
                    connection_id,
                    state="connecting",
                    reason="connecting",
                )
                connect_kwargs: dict[str, t.Any] = {
                    "host": connection["host"],
                    "port": connection["port"],
                    "dbname": connection["database"],
                    "user": connection["username"],
                    "password": connection.get("password") or "",
                    "application_name": (
                        f"pgsync_listener:{self.job_id}:{connection_id}"
                    ),
                    **self._listener_keepalive_options(),
                }
                raw_connection = psycopg2.connect(
                    **connect_kwargs,
                )
                raw_connection.set_session(autocommit=True)
                cursor = raw_connection.cursor()
                cursor.execute(f'LISTEN "{channel}"')
                now = utcnow()
                previous_state: str = str(
                    self.native_listener_states.get(connection_id, {}).get(
                        "state"
                    )
                    or ""
                )
                self._update_listener_state(
                    connection_id,
                    state="listening",
                    reason="listening",
                    extra={
                        "last_connected_at": now,
                        "last_reconnect_at": now,
                        "reconnect_count": int(
                            self.native_listener_states.get(
                                connection_id, {}
                            ).get("reconnect_count")
                            or 0
                        )
                        + 1,
                        "keepalive_enabled": True,
                    },
                )
                self.manager.append_log(
                    self.job_id,
                    f"[native-listener:{connection_id}] listening on {channel}",
                )
                if previous_state in {"degraded", "recovering"}:
                    self._update_listener_state(
                        connection_id,
                        state="recovering",
                        reason="reconnected; scheduling recovery scan",
                    )
                    self._request_recovery_scan(
                        f"listener {connection_id} reconnected"
                    )
                while (
                    not self.stop_requested
                    and not listener_stop_event.is_set()
                ):
                    ready = select.select([raw_connection], [], [], 1.0)
                    if not ready[0]:
                        continue
                    raw_connection.poll()
                    while raw_connection.notifies:
                        notification = raw_connection.notifies.pop(0)
                        try:
                            payload = json.loads(notification.payload)
                        except json.JSONDecodeError:
                            self.manager.append_log(
                                self.job_id,
                                f"[native-listener:{connection_id}] invalid payload: {notification.payload}",
                            )
                            self._request_recovery_scan(
                                f"listener {connection_id} received invalid payload"
                            )
                            continue
                        self._record_listener_notify(connection_id)
                        location = (
                            str(payload.get("schema") or "").lower(),
                            str(payload.get("table") or "").lower(),
                        )
                        payload_table: str = str(payload.get("table") or "")
                        if payload_table in {
                            "activity_base_info",
                            "activity_award_record_info",
                        }:
                            logger.warning(
                                "[trace-listener:%s][job:%s] received payload location=%s watched=%s payload=%s",
                                connection_id,
                                self.job_id,
                                location,
                                sorted(watched_locations),
                                payload,
                            )
                        if location not in watched_locations:
                            if payload_table in {
                                "activity_base_info",
                                "activity_award_record_info",
                            }:
                                logger.warning(
                                    "[trace-listener:%s][job:%s] filtered by watched_locations location=%s",
                                    connection_id,
                                    self.job_id,
                                    location,
                                )
                            continue
                        if self.durable_change_events_enabled:
                            self.native_activity_event.set()
                            continue
                        self._handle_native_notification(
                            connection_id,
                            payload,
                        )
            except Exception as exc:
                if self.stop_requested or listener_stop_event.is_set():
                    break
                now = utcnow()
                self._update_listener_state(
                    connection_id,
                    state="degraded",
                    reason=str(exc),
                    extra={
                        "last_disconnect_at": now,
                        "last_error_at": now,
                        "last_error_message": str(exc),
                    },
                )
                self.manager.append_log(
                    self.job_id,
                    f"[native-listener:{connection_id}] {exc}",
                )
                self._request_recovery_scan(
                    f"listener {connection_id} error: {exc}"
                )
                if listener_stop_event.wait(1.0):
                    break
            finally:
                try:
                    if cursor is not None:
                        cursor.close()
                except Exception:
                    pass
                try:
                    if raw_connection is not None:
                        raw_connection.close()
                except Exception:
                    pass

    def _start_native_daemon(
        self,
        *,
        phase: str = "native_incremental",
        baseline_completed: bool = True,
    ) -> None:
        if self.native_listener_specs is None:
            self.native_listener_specs = self._build_native_listener_specs()
        self.manager.append_log(
            self.job_id,
            "Starting native change listeners.",
        )
        for connection_id, spec in self.native_listener_specs.items():
            if (
                connection_id in self.native_listener_threads
                and self.native_listener_threads[connection_id].is_alive()
            ):
                continue
            self.native_listener_states.setdefault(
                connection_id,
                {
                    "state": "connecting",
                    "reason": "starting",
                    "database": spec["connection"].get("database"),
                    "host": spec["connection"].get("host"),
                    "port": spec["connection"].get("port"),
                    "watched_locations": [
                        list(location)
                        for location in sorted(spec["watched_locations"])
                    ],
                    "reconnect_count": 0,
                    "keepalive_enabled": True,
                },
            )
            listener_stop_event = threading.Event()
            thread = threading.Thread(
                target=self._native_listener_loop,
                args=(
                    connection_id,
                    spec["connection"],
                    set(spec["watched_locations"]),
                    listener_stop_event,
                ),
                daemon=True,
            )
            self.native_listener_stop_events[connection_id] = (
                listener_stop_event
            )
            self.native_listener_threads[connection_id] = thread
            thread.start()
        self.manager.update_runtime(
            self.job_id,
            {
                "status": "running",
                "native_pid": None,
                "native_started_at": utcnow(),
                "phase": phase,
                "baseline_completed": baseline_completed,
                "resume_strategy": self.resume_strategy,
                "native_listener_count": len(self.native_listener_threads),
                "listener_states": self._listener_runtime_snapshot(),
            },
        )

    def _restart_dead_native_listeners_if_needed(self) -> None:
        if self.stop_requested or self.stop_event.is_set():
            return
        if not self.native_listener_specs:
            return
        restart_connection_ids: list[str] = []
        for connection_id in self.native_listener_specs.keys():
            thread = self.native_listener_threads.get(connection_id)
            if thread is None or not thread.is_alive():
                restart_connection_ids.append(connection_id)
        if not restart_connection_ids:
            return
        for connection_id in restart_connection_ids:
            stop_event = self.native_listener_stop_events.pop(
                connection_id, None
            )
            if stop_event is not None:
                stop_event.set()
            self.native_listener_threads.pop(connection_id, None)
        self.manager.append_log(
            self.job_id,
            "Restarting inactive native listeners: "
            + ", ".join(sorted(restart_connection_ids)),
        )
        for connection_id in restart_connection_ids:
            self._update_listener_state(
                connection_id,
                state="recovering",
                reason="listener thread restarted",
            )
        self._request_recovery_scan(
            "listener thread restarted after becoming inactive"
        )
        self._start_native_daemon(
            phase=self.incremental_phase,
            baseline_completed=True,
        )

    def _stop_native_listeners(self) -> None:
        for stop_event in self.native_listener_stop_events.values():
            stop_event.set()
        for connection_id in list(self.native_listener_states.keys()):
            self._update_listener_state(
                connection_id,
                state="stopped",
                reason="stopped",
            )

    def _cross_db_recovery_scan_interval_seconds(self) -> int:
        configured: t.Any = self.job.get(
            "cross_db_recovery_scan_interval_seconds"
        )
        if configured in (None, ""):
            configured = os.environ.get(
                "PGSYNC_CROSS_DB_RECOVERY_SCAN_INTERVAL_SECONDS"
            )
        try:
            return max(
                5,
                int(
                    configured
                    or DEFAULT_CROSS_DB_RECOVERY_SCAN_INTERVAL_SECONDS
                ),
            )
        except (TypeError, ValueError):
            return DEFAULT_CROSS_DB_RECOVERY_SCAN_INTERVAL_SECONDS

    def _cross_db_rebuild_batch_size(self) -> int:
        batch_size, _ = self.manager.runtime_rebuild_batch_size(self.job)
        return batch_size

    def _cross_db_rebuild_batch_size_with_source(self) -> tuple[int, str]:
        return self.manager.runtime_rebuild_batch_size(self.job)

    def _cross_db_recovery_batch_size(self) -> int:
        configured: t.Any = self.job.get("cross_db_recovery_scan_batch_size")
        if configured in (None, ""):
            configured = (
                self.job.get("incremental_batch_size")
                or DEFAULT_CROSS_DB_RECOVERY_SCAN_BATCH_SIZE
            )
        try:
            return max(
                1,
                int(configured or DEFAULT_CROSS_DB_RECOVERY_SCAN_BATCH_SIZE),
            )
        except (TypeError, ValueError):
            return DEFAULT_CROSS_DB_RECOVERY_SCAN_BATCH_SIZE

    def _cross_db_hot_batch_ratio(self) -> float:
        configured: t.Any = self.job.get("assembly_hot_batch_ratio")
        try:
            ratio: float = float(
                configured
                if configured not in (None, "")
                else DEFAULT_CROSS_DB_HOT_BATCH_RATIO
            )
        except (TypeError, ValueError):
            ratio = DEFAULT_CROSS_DB_HOT_BATCH_RATIO
        return min(1.0, max(0.0, ratio))

    def _cross_db_min_hot_batch_size(self) -> int:
        configured: t.Any = self.job.get("assembly_hot_min_batch_size")
        try:
            return max(
                1,
                int(configured or DEFAULT_CROSS_DB_MIN_HOT_BATCH_SIZE),
            )
        except (TypeError, ValueError):
            return DEFAULT_CROSS_DB_MIN_HOT_BATCH_SIZE

    def _cross_db_min_backlog_batch_size(self) -> int:
        configured: t.Any = self.job.get("assembly_backlog_min_batch_size")
        try:
            return max(
                1,
                int(configured or DEFAULT_CROSS_DB_MIN_BACKLOG_BATCH_SIZE),
            )
        except (TypeError, ValueError):
            return DEFAULT_CROSS_DB_MIN_BACKLOG_BATCH_SIZE

    def _queue_runtime_patch(self) -> dict[str, t.Any]:
        snapshot: dict = self.root_queue.snapshot(sample_size=12)
        return {
            "assembly_queue_backend": snapshot.get("backend"),
            "assembly_root_queue": snapshot.get("sample", []),
            "assembly_root_queue_size": int(snapshot.get("queued_size") or 0),
            "assembly_hot_queue_size": int(snapshot.get("hot_size") or 0),
            "assembly_backlog_queue_size": int(
                snapshot.get("backlog_size") or 0
            ),
            "assembly_inflight_batch_id": snapshot.get("inflight_batch_id"),
            "assembly_inflight_batch_size": int(
                snapshot.get("inflight_size") or 0
            ),
        }

    def _refresh_queue_runtime(
        self,
        *,
        extra: t.Optional[dict[str, t.Any]] = None,
    ) -> dict:
        patch: dict[str, t.Any] = self._queue_runtime_patch()
        if extra:
            patch.update(extra)
        return self.manager.update_runtime(self.job_id, patch)

    def _mark_incremental_batch_running(
        self,
        *,
        batch_id: t.Optional[str] = None,
        batch_size: int = 0,
    ) -> None:
        self.manager.update_runtime(
            self.job_id,
            {
                "assembly_incremental_running": True,
                "assembly_current_batch_started_at": utcnow(),
                "assembly_inflight_batch_id": batch_id,
                "assembly_inflight_batch_size": batch_size,
                "phase": "cross_database_incremental",
            },
        )

    def _clear_incremental_batch_running(self) -> None:
        self.manager.update_runtime(
            self.job_id,
            {
                "assembly_incremental_running": False,
                "assembly_current_batch_started_at": None,
            },
        )

    def _recovery_scan_due(self, runtime: dict) -> bool:
        if self.native_recovery_event.is_set():
            return True
        if not self.periodic_recovery_scan_enabled:
            return False
        last_scan_ts: float = float(
            runtime.get("assembly_last_recovery_scan_ts") or 0
        )
        if last_scan_ts <= 0:
            return True
        return (
            utc_timestamp() - last_scan_ts
            >= self._cross_db_recovery_scan_interval_seconds()
        )

    def _recovery_scan_running(self) -> bool:
        return bool(
            self.recovery_scan_thread and self.recovery_scan_thread.is_alive()
        )

    def _recovery_scan_worker(self) -> None:
        metric_token = set_metrics_context(self.job)
        started_at = utcnow()
        self.manager.update_runtime(
            self.job_id,
            {
                "assembly_recovery_scan_running": True,
                "last_recovery_started_at": started_at,
                "health_status": HEALTH_STATUS_RECOVERING,
            },
        )
        try:
            result: dict[str, t.Any] = self._scan_recovery_changes()
            if result.get("fallback_full_sync"):
                self.manager.append_log(
                    self.job_id,
                    "Recovery scan detected tables without cursor support; full-sync fallback is still required for full correctness.",
                )
                self.manager.update_runtime(
                    self.job_id,
                    {"assembly_recovery_requires_full_sync": True},
                )
            self._set_health_status(
                HEALTH_STATUS_RUNNING,
                reason="",
                extra={"last_recovery_finished_at": utcnow()},
            )
        except Exception as exc:
            self.manager.append_log(
                self.job_id,
                f"[recovery-scan] failed: {exc}",
            )
            self._request_recovery_scan(f"recovery scan failed: {exc}")
        finally:
            self.manager.update_runtime(
                self.job_id,
                {"assembly_recovery_scan_running": False},
            )
            reset_metrics_context(metric_token)

    def _start_recovery_scan_if_needed(self) -> bool:
        runtime: dict = self.store.get_job_runtime(self.job_id)
        if not self._recovery_scan_due(runtime):
            return False
        if self._recovery_scan_running():
            return False
        self.native_recovery_event.clear()
        thread = threading.Thread(
            target=self._recovery_scan_worker,
            daemon=True,
        )
        self.recovery_scan_thread = thread
        thread.start()
        return True

    def _scan_recovery_changes(self) -> dict[str, t.Any]:
        read_plan: t.Optional[AssemblyReadPlan] = (
            self.assembly_runner.build_read_plan()
        )
        try:
            return self._scan_recovery_changes_with_plan(read_plan)
        except ReplicaReadQueryError:
            if (
                read_plan is None
                or not read_plan.uses_replica
                or not read_plan.fallback_to_primary
            ):
                raise
            self.manager.append_log(
                self.job_id,
                "Recovery scan replica query failed; restarting the whole scan on primary.",
            )
            primary_plan: t.Optional[AssemblyReadPlan] = (
                self.assembly_runner.build_primary_read_plan(
                    reason="recovery_replica_query_error_retry"
                )
            )
            return self._scan_recovery_changes_with_plan(primary_plan)

    def _scan_recovery_changes_with_plan(
        self,
        read_plan: t.Optional[AssemblyReadPlan],
    ) -> dict[str, t.Any]:
        runtime: dict = self.store.get_job_runtime(self.job_id)
        read_plan_summary: dict[str, t.Any] = (
            read_plan.summary() if read_plan is not None else {}
        )
        pending_cursor_states: dict[str, dict] = {}
        resolved_root_keys: set[tuple[t.Any, ...]] = set()
        fallback_full_sync: bool = False
        changed_row_count: int = 0
        batch_limit: int = self._cross_db_recovery_batch_size()

        for group in self._iter_physical_recovery_groups():
            if self.stop_requested:
                break
            cursor_column: t.Optional[str] = group.get("cursor_column")
            if not cursor_column:
                fallback_full_sync = True
                continue
            connection: dict = self.manager.require_connection(
                group["connection_id"]
            )
            group_tables: list[dict[str, t.Any]] = list(group["tables"])
            first_table: dict[str, t.Any] = group_tables[0]
            group_checkpoint: t.Optional[dict[str, t.Any]] = None
            table_states: dict[str, dict[str, t.Any]] = {}
            for table in group_tables:
                cursor_state: dict = (
                    self.assembly_runner.cursor_state_for_table(
                        runtime,
                        table,
                    )
                )
                table_states[table["id"]] = cursor_state
                if (
                    group_checkpoint is None
                    or compare_checkpoints(
                        cursor_state["applied"],
                        group_checkpoint,
                        table["primary_key"],
                    )
                    < 0
                ):
                    group_checkpoint = deepcopy(cursor_state["applied"])
            if group_checkpoint is None:
                group_checkpoint = {"cursor": None, "pk": []}
            rows, next_checkpoint = self.manager.db.fetch_changed_rows(
                connection,
                group["schema"],
                group["table"],
                cursor_column=cursor_column,
                last_cursor=group_checkpoint.get("cursor"),
                last_primary_key=group_checkpoint.get("pk") or [],
                primary_key=group["primary_key"],
                limit=batch_limit,
                **self.assembly_runner.read_plan_kwargs(read_plan),
            )
            if not rows:
                continue
            changed_row_count += len(rows)
            self.manager.append_log(
                self.job_id,
                f"[recovery-scan] {group['schema']}.{group['table']} fetched {len(rows)} changed rows for {len(group_tables)} alias(es).",
            )
            group_root_count_before: int = len(resolved_root_keys)
            for table in group_tables:
                cursor_state = table_states[table["id"]]
                table_rows: list[dict] = [
                    row
                    for row in rows
                    if checkpoint_is_after(
                        self.assembly_runner.cursor_checkpoint_from_row(
                            table,
                            row,
                        ),
                        cursor_state["applied"],
                        table["primary_key"],
                    )
                ]
                if not table_rows:
                    continue
                table_roots, errors = self._resolve_rows_root_keys(
                    [table["id"]],
                    table_rows,
                    read_plan=read_plan,
                )
                if errors:
                    self.manager.append_log(
                        self.job_id,
                        "[recovery-scan] root fan-out errors: "
                        + "; ".join(errors[:5]),
                    )
                    self.native_recovery_event.set()
                    continue
                for root_key in table_roots:
                    normalized: tuple[t.Any, ...] = tuple(root_key)
                    if normalized and any(
                        value is not None for value in normalized
                    ):
                        resolved_root_keys.add(normalized)
                pending_cursor_states[table["id"]] = {
                    "applied": deepcopy(cursor_state["applied"]),
                    "pending": deepcopy(
                        self.assembly_runner.cursor_checkpoint_from_row(
                            table,
                            table_rows[-1],
                        )
                    ),
                }
            if len(resolved_root_keys) == group_root_count_before:
                self.manager.append_log(
                    self.job_id,
                    f"[recovery-scan] {group['schema']}.{group['table']} resolved no root keys after alias fan-out.",
                )

        patch: dict[str, t.Any] = {
            "assembly_last_recovery_scan_at": utcnow(),
            "assembly_last_recovery_scan_ts": utc_timestamp(),
            "assembly_last_recovery_scan_row_count": changed_row_count,
            "assembly_last_recovery_scan_root_count": len(resolved_root_keys),
            "assembly_last_recovery_read_plan": read_plan_summary,
        }

        enqueued_count: int = 0
        if pending_cursor_states:
            if resolved_root_keys:
                enqueued_count = self.root_queue.enqueue_backlog(
                    list(resolved_root_keys)
                )
            confirmed_cursors: dict[str, dict[str, t.Any]] = (
                self._merged_confirmed_cursors(
                    runtime,
                    pending_cursor_states,
                )
            )
            patch.update(
                {
                    "cursors": confirmed_cursors,
                    "last_incremental_sync_at": utcnow(),
                    "last_incremental_sync_ts": utc_timestamp(),
                    "assembly_last_recovery_enqueue_count": enqueued_count,
                }
            )
            if changed_row_count:
                self.manager.append_log(
                    self.job_id,
                    f"[recovery-scan] queued {enqueued_count} root keys from {changed_row_count} changed rows.",
                )

        with self.queue_lock:
            self._refresh_queue_runtime(extra=patch)

        return {
            "fallback_full_sync": fallback_full_sync,
            "changed_row_count": changed_row_count,
            "resolved_root_count": len(resolved_root_keys),
            "enqueued_count": enqueued_count,
        }

    def _claim_root_batch(self) -> dict[str, t.Any]:
        batch_size, batch_size_source = (
            self._cross_db_rebuild_batch_size_with_source()
        )
        ratio: float = self._cross_db_hot_batch_ratio()
        snapshot: dict = self.root_queue.snapshot(sample_size=0)
        hot_size: int = int(snapshot.get("hot_size") or 0)
        backlog_size: int = int(snapshot.get("backlog_size") or 0)
        if hot_size <= 0:
            hot_limit = 0
            backlog_limit = batch_size
        elif backlog_size <= 0:
            hot_limit = batch_size
            backlog_limit = 0
        else:
            min_hot: int = min(hot_size, self._cross_db_min_hot_batch_size())
            min_backlog: int = min(
                backlog_size,
                self._cross_db_min_backlog_batch_size(),
            )
            if min_hot + min_backlog > batch_size:
                min_hot = 1
                min_backlog = 1 if batch_size > 1 else 0
            hot_limit = min_hot
            backlog_limit = min_backlog
            remaining: int = max(0, batch_size - hot_limit - backlog_limit)
            target_hot: int = max(1, int(round(batch_size * ratio)))
            hot_room: int = max(0, hot_size - hot_limit)
            desired_hot_extra: int = max(0, target_hot - hot_limit)
            hot_extra: int = min(remaining, hot_room, desired_hot_extra)
            hot_limit += hot_extra
            remaining -= hot_extra
            backlog_room: int = max(0, backlog_size - backlog_limit)
            backlog_extra: int = min(remaining, backlog_room)
            backlog_limit += backlog_extra
            remaining -= backlog_extra
            if remaining > 0:
                hot_limit += min(remaining, max(0, hot_size - hot_limit))
        batch: dict[str, t.Any] = self.root_queue.claim_batch(
            hot_limit=hot_limit,
            backlog_limit=backlog_limit,
        )
        batch["configured_batch_size"] = batch_size
        batch["batch_size_source"] = batch_size_source
        batch["queued_size_before_claim"] = hot_size + backlog_size
        return batch

    def _dedupe_root_keys(
        self,
        root_keys: t.Iterable[tuple[t.Any, ...]],
    ) -> list[tuple[t.Any, ...]]:
        deduped: list[tuple[t.Any, ...]] = []
        seen: set[tuple[t.Any, ...]] = set()
        for root_key in root_keys:
            normalized: tuple[t.Any, ...] = tuple(root_key)
            if normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)
        return deduped

    def _split_root_key_chunks(
        self,
        root_keys: list[tuple[t.Any, ...]],
        worker_count: int,
    ) -> list[list[tuple[t.Any, ...]]]:
        if not root_keys:
            return []
        worker_count = max(1, min(worker_count, len(root_keys)))
        chunks: list[list[tuple[t.Any, ...]]] = [
            [] for _ in range(worker_count)
        ]
        for index, root_key in enumerate(root_keys):
            chunks[index % worker_count].append(root_key)
        return [chunk for chunk in chunks if chunk]

    def _build_rebuild_actions_parallel(
        self,
        root_keys: list[tuple[t.Any, ...]],
        worker_count: int,
        *,
        read_plan: t.Optional[AssemblyReadPlan] = None,
    ) -> tuple[list[dict], list[str], list[int]]:
        chunks: list[list[tuple[t.Any, ...]]] = self._split_root_key_chunks(
            root_keys,
            worker_count,
        )

        def build_chunk(
            chunk: list[tuple[t.Any, ...]],
        ) -> tuple[list[dict], list[str]]:
            with metrics_context(self.job):
                read_kwargs: dict[str, t.Any] = (
                    {"read_plan": read_plan}
                    if read_plan is not None and read_plan.enabled
                    else {}
                )
                return (
                    self.assembly_runner.build_rebuild_actions_for_root_keys(
                        chunk,
                        **read_kwargs,
                    )
                )

        if len(chunks) <= 1:
            actions, deletions = build_chunk(chunks[0] if chunks else [])
            return actions, deletions, [0]

        actions_by_index: dict[int, list[dict]] = {}
        deletions_by_index: dict[int, list[str]] = {}
        chunk_elapsed_ms: list[int] = [0 for _ in chunks]
        with ThreadPoolExecutor(
            max_workers=len(chunks),
            thread_name_prefix=f"pgsync-{self.job_id}-rebuild",
        ) as executor:
            futures = {
                executor.submit(
                    build_chunk,
                    chunk,
                ): index
                for index, chunk in enumerate(chunks)
            }
            started_at_by_future: dict[t.Any, float] = {
                future: time.perf_counter() for future in futures
            }
            for future in as_completed(futures):
                index: int = futures[future]
                actions, deletions = future.result()
                chunk_elapsed_ms[index] = int(
                    (time.perf_counter() - started_at_by_future[future]) * 1000
                )
                actions_by_index[index] = actions
                deletions_by_index[index] = deletions

        merged_actions: list[dict] = []
        merged_deletions: list[str] = []
        for index in range(len(chunks)):
            merged_actions.extend(actions_by_index.get(index, []))
            merged_deletions.extend(deletions_by_index.get(index, []))
        return merged_actions, merged_deletions, chunk_elapsed_ms

    def _apply_rebuild_actions(
        self,
        search: SearchAdapter,
        actions: list[dict],
        deletions: list[str],
    ) -> tuple[int, int]:
        if actions:
            search.bulk(actions)
        if deletions:
            search.delete_many(self.job["index_name"], deletions)
        return len(actions), len(deletions)

    def _enqueue_root_keys(
        self,
        root_keys: t.Iterable[tuple[t.Any, ...]],
        *,
        source: str,
    ) -> int:
        normalized_roots: list[tuple[t.Any, ...]] = [
            tuple(root_key)
            for root_key in root_keys
            if root_key and any(value is not None for value in root_key)
        ]
        if not normalized_roots:
            return 0
        event_at: str = utcnow()
        event_ts: float = utc_timestamp()
        self.last_native_event_ts = event_ts
        event_summary: dict[str, t.Any] = {
            "at": event_at,
            "ts": event_ts,
            "enqueued_count": 0,
            "root_key_count": len(normalized_roots),
            "root_keys": [
                list(root_key) for root_key in normalized_roots[:12]
            ],
        }
        with self.queue_lock:
            enqueued_count: int = self.root_queue.enqueue_hot(normalized_roots)
            event_summary["enqueued_count"] = enqueued_count
            self.native_event_history_by_source[source] = deepcopy(
                event_summary
            )
            self._refresh_queue_runtime(
                extra={
                    "last_native_event_at": event_at,
                    "last_native_event_ts": event_ts,
                    "last_native_event_source": source,
                    "last_native_event_enqueued_count": enqueued_count,
                    "last_native_event_root_key_count": len(normalized_roots),
                    "last_native_event_root_keys": [
                        list(root_key) for root_key in normalized_roots[:12]
                    ],
                    "last_native_events_by_source": deepcopy(
                        self.native_event_history_by_source
                    ),
                }
            )
        if source.endswith(".activity_base_info") or source.endswith(
            ".activity_award_record_info"
        ):
            logger.warning(
                "[trace:%s][job:%s] enqueue root keys count=%s roots=%s",
                source,
                self.job_id,
                enqueued_count,
                [list(root_key) for root_key in normalized_roots],
            )
            self.manager.append_log(
                self.job_id,
                "[trace:%s] enqueue root keys count=%s roots=%s"
                % (
                    source,
                    enqueued_count,
                    [list(root_key) for root_key in normalized_roots],
                ),
            )
        self.native_activity_event.set()
        return enqueued_count

    def _resolve_payload_root_keys(
        self,
        connection_id: str,
        table_ids: t.Iterable[str],
        payload: dict,
    ) -> tuple[set[tuple], list[str]]:
        candidate_rows: list[dict] = self._candidate_rows_from_payload(payload)
        if not candidate_rows:
            return set(), []
        hydrated_rows, errors = self._hydrate_candidate_rows(
            connection_id,
            str(payload.get("schema") or ""),
            str(payload.get("table") or ""),
            table_ids,
            candidate_rows,
        )
        root_keys, root_errors = self._resolve_rows_root_keys(
            table_ids,
            hydrated_rows,
        )
        payload_table: str = str(payload.get("table") or "")
        if payload_table in {
            "activity_base_info",
            "activity_award_record_info",
        }:
            logger.warning(
                "[trace:%s][job:%s] resolve payload root keys table_ids=%s candidate_rows=%s hydrated_rows=%s root_keys=%s errors=%s",
                payload_table,
                self.job_id,
                list(table_ids),
                candidate_rows[:3],
                hydrated_rows[:3],
                [list(root_key) for root_key in sorted(root_keys)],
                (errors + root_errors)[:5],
            )
            self.manager.append_log(
                self.job_id,
                "[trace:%s] resolve payload root keys table_ids=%s candidate_rows=%s hydrated_rows=%s root_keys=%s errors=%s"
                % (
                    payload_table,
                    list(table_ids),
                    candidate_rows[:3],
                    hydrated_rows[:3],
                    [list(root_key) for root_key in sorted(root_keys)],
                    (errors + root_errors)[:5],
                ),
            )
        return root_keys, errors + root_errors

    def _handle_native_notification(
        self,
        connection_id: str,
        payload: dict,
    ) -> None:
        table_ids: list[str] = self.table_ids_by_location.get(
            self._location_key(
                connection_id,
                str(payload.get("schema") or ""),
                str(payload.get("table") or ""),
            ),
            [],
        )
        if not table_ids:
            return
        payload_table: str = str(payload.get("table") or "")
        if payload_table in {
            "activity_base_info",
            "activity_award_record_info",
        }:
            logger.warning(
                "[trace:%s][job:%s] native notification payload=%s table_ids=%s",
                payload_table,
                self.job_id,
                payload,
                table_ids,
            )
            self.manager.append_log(
                self.job_id,
                "[trace:%s] native notification payload=%s table_ids=%s"
                % (
                    payload_table,
                    payload,
                    table_ids,
                ),
            )
        root_keys, errors = self._resolve_payload_root_keys(
            connection_id,
            table_ids,
            payload,
        )
        if errors:
            self.manager.append_log(
                self.job_id,
                "[native-listener:%s] root fan-out errors for %s.%s: %s"
                % (
                    connection_id,
                    payload.get("schema"),
                    payload.get("table"),
                    "; ".join(errors[:5]),
                ),
            )
            self.native_recovery_event.set()
        if not root_keys:
            if payload_table in {
                "activity_base_info",
                "activity_award_record_info",
            }:
                logger.warning(
                    "[trace:%s][job:%s] resolved no root keys for payload id=%s",
                    payload_table,
                    self.job_id,
                    get_in(payload, "new", "id")
                    or get_in(payload, "old", "id"),
                )
                self.manager.append_log(
                    self.job_id,
                    "[trace:%s] resolved no root keys for payload id=%s"
                    % (
                        payload_table,
                        get_in(payload, "new", "id")
                        or get_in(payload, "old", "id"),
                    ),
                )
            return
        self._enqueue_root_keys(
            root_keys,
            source=f"{payload.get('schema')}.{payload.get('table')}",
        )

    def _run_assembly_incremental_cycle(
        self,
        search: SearchAdapter,
    ) -> bool:
        if not self.incremental_run_lock.acquire(blocking=False):
            self.manager.append_log(
                self.job_id,
                "Incremental rebuild cycle is still running; skipping this round.",
            )
            return False
        batch: t.Optional[dict[str, t.Any]] = None
        raw_count: int = 0
        deduped_count: int = 0
        worker_count: int = 0
        updated_count: int = 0
        deleted_count: int = 0
        started_at: t.Optional[float] = None
        self._mark_incremental_batch_running()
        try:
            batch = self._claim_root_batch()
            if not batch.get("root_keys"):
                return False

            raw_root_keys: list[tuple[t.Any, ...]] = [
                tuple(root_key) for root_key in (batch.get("root_keys") or [])
            ]
            raw_count = len(raw_root_keys)
            deduped_root_keys: list[tuple[t.Any, ...]] = (
                self._dedupe_root_keys(raw_root_keys)
            )
            deduped_count = len(deduped_root_keys)
            worker_count, worker_config = (
                self.manager.runtime_assembly_worker_count(
                    self.job,
                    queue_size=int(batch.get("queued_size_before_claim") or 0),
                    batch_size=deduped_count,
                )
            )
            self._mark_incremental_batch_running(
                batch_id=batch.get("batch_id"),
                batch_size=raw_count,
            )
            started_at = time.perf_counter()
            read_plan: t.Optional[AssemblyReadPlan] = (
                self.assembly_runner.build_read_plan()
            )
            read_plan_summary: dict[str, t.Any] = (
                read_plan.summary() if read_plan is not None else {}
            )
            try:
                actions, deletions, chunk_elapsed_ms = (
                    self._build_rebuild_actions_parallel(
                        deduped_root_keys,
                        worker_count,
                        read_plan=read_plan,
                    )
                )
            except ReplicaReadQueryError:
                if (
                    read_plan is None
                    or not read_plan.uses_replica
                    or not read_plan.fallback_to_primary
                ):
                    raise
                self.manager.append_log(
                    self.job_id,
                    "Assembly replica query failed; rebuilding the whole batch on primary.",
                )
                read_plan = self.assembly_runner.build_primary_read_plan(
                    reason="replica_query_error_retry"
                )
                read_plan_summary = (
                    read_plan.summary() if read_plan is not None else {}
                )
                actions, deletions, chunk_elapsed_ms = (
                    self._build_rebuild_actions_parallel(
                        deduped_root_keys,
                        worker_count,
                        read_plan=read_plan,
                    )
                )
            updated_count, deleted_count = self._apply_rebuild_actions(
                search,
                actions,
                deletions,
            )
            elapsed_seconds: float = time.perf_counter() - started_at
            elapsed_ms: int = int(elapsed_seconds * 1000)
            self.root_queue.ack_batch(batch.get("batch_id"))
            with self.queue_lock:
                self._refresh_queue_runtime(
                    extra={
                        "last_incremental_sync_at": utcnow(),
                        "last_incremental_sync_ts": utc_timestamp(),
                        "assembly_last_batch_confirmed_at": utcnow(),
                        "assembly_last_batch_updated_count": updated_count,
                        "assembly_last_batch_deleted_count": deleted_count,
                        "assembly_last_batch_root_key_count": len(
                            raw_root_keys
                        ),
                        "assembly_last_batch_raw_root_key_count": len(
                            raw_root_keys
                        ),
                        "assembly_last_batch_deduped_root_key_count": len(
                            deduped_root_keys
                        ),
                        "assembly_last_batch_worker_count": worker_count,
                        "assembly_last_batch_elapsed_ms": elapsed_ms,
                        "assembly_last_batch_chunk_elapsed_ms": chunk_elapsed_ms,
                        "assembly_last_batch_configured_size": batch.get(
                            "configured_batch_size"
                        ),
                        "assembly_last_batch_size_source": batch.get(
                            "batch_size_source"
                        ),
                        "assembly_last_batch_root_keys": [
                            list(root_key) for root_key in raw_root_keys[:12]
                        ],
                        "assembly_last_read_plan": read_plan_summary,
                    }
                )
            record_rebuild_batch(
                self.job,
                raw_root_keys=raw_count,
                deduped_root_keys=deduped_count,
                worker_count=worker_count,
                updated_count=updated_count,
                deleted_count=deleted_count,
                elapsed_seconds=elapsed_seconds,
                status="success",
            )
            self.manager.append_log(
                self.job_id,
                "Assembly rebuild completed: "
                f"raw={len(raw_root_keys)}, deduped={len(deduped_root_keys)}, "
                f"workers={worker_count}, updated={updated_count}, "
                f"deleted={deleted_count}, elapsed_ms={elapsed_ms}, "
                f"chunk_elapsed_ms={chunk_elapsed_ms}, "
                f"batch_size={batch.get('configured_batch_size')}, "
                f"batch_size_source={batch.get('batch_size_source')}, "
                f"parallel_enabled={worker_config.get('parallel_enabled')}, "
                f"read_replica={read_plan_summary.get('replica_count', 0)}, "
                f"read_primary={read_plan_summary.get('primary_count', 0)}, "
                f"read_wait_ms={read_plan_summary.get('max_waited_ms', 0)}, "
                f"hot={batch.get('hot_count')}, backlog={batch.get('backlog_count')}, "
                f"remaining={self._pending_assembly_root_queue_size()}.",
            )
            return True
        except Exception:
            elapsed_seconds = (
                time.perf_counter() - started_at
                if started_at is not None
                else 0.0
            )
            record_rebuild_batch(
                self.job,
                raw_root_keys=raw_count,
                deduped_root_keys=deduped_count,
                worker_count=worker_count,
                updated_count=updated_count,
                deleted_count=deleted_count,
                elapsed_seconds=elapsed_seconds,
                status="error",
            )
            if batch and batch.get("batch_id"):
                with self.queue_lock:
                    self.root_queue.requeue_inflight(
                        batch.get("batch_id"),
                        to_hot=True,
                    )
                    self._refresh_queue_runtime()
            raise
        finally:
            self._clear_incremental_batch_running()
            self.incremental_run_lock.release()

    def _build_full_sync_cursor_seed(self) -> dict[str, dict[str, t.Any]]:
        return self.assembly_runner.build_cursor_seed()

    def _pending_assembly_root_queue_size(self) -> int:
        return max(0, int(self.root_queue.queued_size() or 0))

    def _wait_for_incremental_signal(self, interval_seconds: int) -> bool:
        deadline: float = time.time() + max(1, int(interval_seconds))
        while time.time() < deadline:
            if self.stop_requested or self.stop_event.is_set():
                return False
            if (
                self.native_recovery_event.is_set()
                or self.native_activity_event.is_set()
                or self._pending_assembly_root_queue_size() > 0
            ):
                return True
            remaining: float = max(0.0, deadline - time.time())
            self.stop_event.wait(min(1.0, remaining))
        return bool(
            self.native_recovery_event.is_set()
            or self.native_activity_event.is_set()
            or self._pending_assembly_root_queue_size() > 0
        )

    def run(self) -> None:
        metric_token = set_metrics_context(self.job)
        search: t.Optional[SearchAdapter] = None
        try:
            self.manager.update_runtime(
                self.job_id,
                {
                    "status": "starting",
                    "mode": "native",
                    "phase": (
                        self.incremental_phase
                        if self.resume_incremental_only
                        else self.setup_phase
                    ),
                    "cross_database": self.is_cross_database,
                    "assembly_fallback_enabled": self.assembly_fallback_enabled,
                    "schema_path": self.schema_path,
                    "native_pid": None,
                    "native_bootstrap_pid": None,
                    "native_setup_pid": None,
                    "last_error": "",
                    "exit_code": None,
                    "resume_strategy": self.resume_strategy,
                    "assembly_queue_backend": self.root_queue.backend,
                    "durable_change_events_enabled": self.durable_change_events_enabled,
                    "health_status": HEALTH_STATUS_RUNNING,
                    "health_reason": "",
                },
            )
            search = SearchAdapter(self.store.snapshot()["services"])
            search.ensure_index(
                self.job["index_name"],
                mapping=self.manager.build_search_mapping(self.job),
            )
            if self.resume_incremental_only:
                self.manager.append_log(
                    self.job_id,
                    "Baseline already exists; resume native capture plus assembly rebuild incremental only.",
                )
                self.manager.update_runtime(
                    self.job_id,
                    {
                        "baseline_completed": True,
                        "resume_strategy": self.resume_strategy,
                    },
                )
                self._ensure_resume_checkpoint()
                with self.queue_lock:
                    recovered_count: int = self.root_queue.recover_inflight(
                        to_hot=True
                    )
                    self._refresh_queue_runtime(
                        extra={
                            "assembly_recovered_inflight_count": recovered_count,
                        }
                    )
            else:
                self.manager.append_log(
                    self.job_id,
                    "Incremental sync uses native change capture plus assembly rebuild.",
                )
                self._prepare_native_incremental_start()
                if self.stop_requested:
                    return
                durable_cursor_seed: dict[str, int] = (
                    self._build_durable_event_cursor_seed()
                    if self.durable_change_events_enabled
                    else {}
                )
                durable_processed_seed: dict[str, list[int]] = (
                    self._build_durable_processed_seed(durable_cursor_seed)
                    if durable_cursor_seed
                    else {}
                )
                if self.durable_change_events_enabled:
                    self.manager.update_runtime(
                        self.job_id,
                        {
                            "native_change_event_cursors": durable_cursor_seed,
                            "native_change_event_processed_ids": durable_processed_seed,
                            "durable_change_events_enabled": True,
                            "durable_event_lag_by_group": {},
                            "durable_event_lag": 0,
                            "durable_event_lag_exact_by_group": {},
                            "durable_event_lag_exact": False,
                            "durable_event_backlog_present_by_group": {},
                            "durable_event_backlog_present": False,
                            "durable_event_lag_observed_at": None,
                        },
                    )
                with self.queue_lock:
                    self.root_queue.clear()
                    self._refresh_queue_runtime(
                        extra={
                            "assembly_recovered_inflight_count": 0,
                        }
                    )
                baseline_cursors: dict[str, dict[str, t.Any]] = (
                    self._build_full_sync_cursor_seed()
                )
                self._start_native_daemon(
                    phase=self.full_sync_phase,
                    baseline_completed=False,
                )
                self.manager.append_log(
                    self.job_id,
                    "Starting assembly full sync with listeners already capturing live events.",
                )
                self.manager.update_runtime(
                    self.job_id,
                    {
                        "phase": self.full_sync_phase,
                    },
                )
                self.assembly_runner.full_sync(
                    search, cursor_seed=baseline_cursors
                )
                with self.queue_lock:
                    self._refresh_queue_runtime()
                self.manager.append_log(
                    self.job_id,
                    "Assembly full sync completed.",
                )
            if self.stop_requested:
                return

            if not self.native_listener_threads:
                self._start_native_daemon(
                    phase=self.incremental_phase,
                    baseline_completed=True,
                )
            else:
                self._restart_dead_native_listeners_if_needed()
            with self.queue_lock:
                self._refresh_queue_runtime()
            self._request_recovery_scan("native listeners started")
            interval: int = int(
                self.job.get("poll_interval_seconds")
                or DEFAULT_POLL_INTERVAL_SECONDS
            )
            while True:
                self._restart_dead_native_listeners_if_needed()
                if self.recovery_scan_enabled:
                    self._start_recovery_scan_if_needed()
                durable_drained: bool = self._drain_durable_change_events()
                self._maybe_request_idle_recovery_scan()
                should_run_immediately: bool = bool(
                    self.native_activity_event.is_set()
                    or self.native_recovery_event.is_set()
                    or self._pending_assembly_root_queue_size() > 0
                    or durable_drained
                )
                if not should_run_immediately:
                    should_run_immediately = self._wait_for_incremental_signal(
                        interval
                    )
                    if should_run_immediately:
                        durable_drained = self._drain_durable_change_events()
                if self.stop_requested or self.stop_event.is_set():
                    break
                if self.stop_requested:
                    break
                if not should_run_immediately and not durable_drained:
                    continue
                ran_cycle: bool = self._run_assembly_incremental_cycle(search)
                if not ran_cycle:
                    if self.stop_event.wait(1.0):
                        break
                    continue
                self.native_activity_event.clear()
                if self._pending_assembly_root_queue_size() > 0:
                    continue
        except Exception as exc:
            logger.exception("Native capture / assembly rebuild sync failed")
            self.manager.append_log(
                self.job_id,
                f"Native capture / assembly rebuild sync failed: {exc}",
            )
            runtime: dict = self.store.get_job_runtime(self.job_id)
            lease_lost: bool = bool(
                self.lease_lost or runtime.get("phase") == "lease_lost"
            )
            self.manager.update_runtime(
                self.job_id,
                {
                    "status": "failed",
                    "phase": "lease_lost" if lease_lost else "failed",
                    "last_error": (
                        runtime.get("last_error") if lease_lost else str(exc)
                    ),
                },
            )
        else:
            if self.lease_lost:
                patch = {
                    "status": "failed",
                    "phase": "lease_lost",
                }
            elif self.stop_requested:
                patch = self.manager.build_resumable_stop_runtime_patch(
                    self.job_id,
                    self.store.get_job_runtime(self.job_id),
                )
            else:
                patch = {
                    "status": "running",
                    "phase": self.incremental_phase,
                }
            self.manager.update_runtime(self.job_id, patch)
        finally:
            self._stop_native_listeners()
            self.manager.on_runner_finished(self.job_id)
            reset_metrics_context(metric_token)

    def _terminate_process(
        self,
        process: t.Optional[subprocess.Popen],
    ) -> None:
        if not process or process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()

    def handle_lease_lost(self) -> None:
        self.lease_lost = True
        self.stop_requested = True
        self.stop_event.set()
        self._stop_native_listeners()
        self.assembly_runner.handle_lease_lost()


CrossDatabaseHybridRunner = CrossDatabaseHybridRunnerV2


class SyncManager:
    """High level orchestration for the studio web layer."""

    def __init__(
        self,
        store: ConfigStore,
        db: DatabaseManager,
        repo_root: str,
    ) -> None:
        self.store = store
        self.db = db or DatabaseManager()
        self.repo_root = os.path.abspath(repo_root)
        self.runtime_dir = os.path.join(store.base_dir, "jobs")
        os.makedirs(self.runtime_dir, exist_ok=True)
        self.instance_id = self._load_instance_id()
        self.runners: dict[
            str,
            t.Union[
                NativeProcessRunner,
                AssemblyRunner,
                CrossDatabaseHybridRunner,
            ],
        ] = {}
        self.job_leases: dict[str, t.Union[RedisJobLease, NoopJobLease]] = {}
        self.job_lease_threads: dict[str, dict[str, t.Any]] = {}
        self._lock: threading.RLock = threading.RLock()
        configured_recovery_interval: str = (
            os.environ.get("PGSYNC_STUDIO_RECOVERY_POLL_INTERVAL_SECONDS")
            or ""
        ).strip()
        try:
            self.recovery_poll_interval_seconds: float = max(
                1.0,
                float(
                    configured_recovery_interval
                    or DEFAULT_RECOVERY_POLL_INTERVAL_SECONDS
                ),
            )
        except ValueError:
            self.recovery_poll_interval_seconds = float(
                DEFAULT_RECOVERY_POLL_INTERVAL_SECONDS
            )
        self.failed_recovery_initial_delay_seconds: float = float(
            DEFAULT_FAILED_RECOVERY_INITIAL_DELAY_SECONDS
        )
        self.failed_recovery_max_delay_seconds: float = float(
            DEFAULT_FAILED_RECOVERY_MAX_DELAY_SECONDS
        )
        self.failed_recovery_stable_seconds: float = float(
            DEFAULT_FAILED_RECOVERY_STABLE_SECONDS
        )
        self._recovery_stop_event: threading.Event = threading.Event()
        self._recovery_thread: t.Optional[threading.Thread] = None
        (
            self.connection_auto_discovery_hour,
            self.connection_auto_discovery_minute,
            self.connection_auto_discovery_time_label,
        ) = self._load_connection_auto_discovery_time()
        self._connection_auto_discovery_stop_event: threading.Event = (
            threading.Event()
        )
        self._connection_auto_discovery_thread: t.Optional[
            threading.Thread
        ] = None
        self._connection_auto_discovery_runtime_excluded_databases: set[
            str
        ] = set()
        self._durable_outbox_cleanup_stop_event: threading.Event = (
            threading.Event()
        )
        self._durable_outbox_cleanup_thread: t.Optional[threading.Thread] = (
            None
        )
        self._durable_outbox_cleanup_status_lock = threading.RLock()
        self._durable_outbox_cleanup_last_summary: t.Optional[
            dict[str, t.Any]
        ] = None
        self._durable_outbox_partition_stop_event = threading.Event()
        self._durable_outbox_partition_thread: t.Optional[threading.Thread] = (
            None
        )
        self._durable_outbox_partition_status_lock = threading.RLock()
        self._durable_outbox_partition_last_summary: t.Optional[
            dict[str, t.Any]
        ] = None
        self._durable_outbox_partition_next_run_at: t.Optional[str] = None
        self._durable_outbox_capture_reconciliation_last_summary: t.Optional[
            dict[str, t.Any]
        ] = None
        self.runtime_tuning = RuntimeTuningConfig()
        self.reconcile_runtime_states()

    @property
    def instance_id_path(self) -> str:
        return os.path.join(self.store.base_dir, ".studio_instance_id")

    def _load_instance_id(self) -> str:
        configured: str = (
            os.environ.get("PGSYNC_STUDIO_INSTANCE_ID") or ""
        ).strip()
        if configured:
            return configured

        try:
            if os.path.exists(self.instance_id_path):
                with open(
                    self.instance_id_path,
                    "r",
                    encoding="utf-8",
                ) as fp:
                    persisted: str = fp.read().strip()
                if persisted:
                    return persisted
        except OSError:
            logger.warning(
                "Could not read Studio instance id from %s",
                self.instance_id_path,
            )

        instance_id: str = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        try:
            with open(
                self.instance_id_path,
                "w",
                encoding="utf-8",
            ) as fp:
                fp.write(instance_id)
        except OSError:
            logger.warning(
                "Could not persist Studio instance id to %s",
                self.instance_id_path,
            )
        return instance_id

    def _load_connection_auto_discovery_time(
        self,
    ) -> tuple[int, int, str]:
        configured: str = (
            os.environ.get(AUTO_DISCOVER_CONNECTIONS_TIME_ENV) or ""
        ).strip()
        candidate: str = configured or DEFAULT_AUTO_DISCOVER_CONNECTIONS_TIME
        try:
            hour_text, minute_text = candidate.split(":", 1)
            hour = int(hour_text)
            minute = int(minute_text)
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError
        except (TypeError, ValueError):
            hour_text, minute_text = (
                DEFAULT_AUTO_DISCOVER_CONNECTIONS_TIME.split(
                    ":",
                    1,
                )
            )
            hour = int(hour_text)
            minute = int(minute_text)
        return hour, minute, f"{hour:02d}:{minute:02d}"

    def _runtime_tuning_root(self) -> dict[str, t.Any]:
        snapshot: dict[str, t.Any] = self.runtime_tuning.snapshot()
        payload: dict[str, t.Any] = snapshot.get("payload") or {}
        pgsync: t.Any = payload.get("pgsync")
        return pgsync if isinstance(pgsync, dict) else payload

    def _runtime_tuning_job_config(self, job: dict) -> dict[str, t.Any]:
        root: dict[str, t.Any] = self._runtime_tuning_root()
        jobs: t.Any = root.get("jobs")
        if not isinstance(jobs, dict):
            return {}
        candidates: list[str] = [
            str(job.get("name") or "").strip(),
            str(job.get("index_name") or "").strip(),
            str(job.get("id") or "").strip(),
        ]
        for key in candidates:
            if key and isinstance(jobs.get(key), dict):
                return jobs[key]
        return {}

    def _runtime_tuning_assembly_config(self) -> dict[str, t.Any]:
        root: dict[str, t.Any] = self._runtime_tuning_root()
        assembly: t.Any = root.get("assembly")
        return assembly if isinstance(assembly, dict) else {}

    def runtime_rebuild_batch_size(self, job: dict) -> tuple[int, str]:
        job_cfg: dict[str, t.Any] = self._runtime_tuning_job_config(job)
        assembly_cfg: dict[str, t.Any] = self._runtime_tuning_assembly_config()
        candidates: list[tuple[t.Any, str]] = [
            (job_cfg.get("assembly_rebuild_batch_size"), "nacos_job"),
            (job_cfg.get("rebuild_batch_size"), "nacos_job"),
            (assembly_cfg.get("assembly_rebuild_batch_size"), "nacos_global"),
            (assembly_cfg.get("rebuild_batch_size"), "nacos_global"),
            (job.get("assembly_rebuild_batch_size"), "job"),
            (job.get("incremental_batch_size"), "job"),
        ]
        for value, source in candidates:
            if value not in (None, ""):
                return (
                    _clamped_int(
                        value,
                        default=DEFAULT_CROSS_DB_REBUILD_BATCH_SIZE,
                        minimum=1,
                        maximum=DEFAULT_ASSEMBLY_MAX_REBUILD_BATCH_SIZE,
                    ),
                    source,
                )
        return DEFAULT_CROSS_DB_REBUILD_BATCH_SIZE, "default"

    def runtime_parallel_enabled(self, job: dict) -> bool:
        job_cfg: dict[str, t.Any] = self._runtime_tuning_job_config(job)
        assembly_cfg: dict[str, t.Any] = self._runtime_tuning_assembly_config()
        if job_cfg.get("parallel_enabled") not in (None, ""):
            return _as_bool(job_cfg.get("parallel_enabled"), default=False)
        if assembly_cfg.get("parallel_enabled") not in (None, ""):
            return _as_bool(
                assembly_cfg.get("parallel_enabled"),
                default=DEFAULT_ASSEMBLY_PARALLEL_ENABLED,
            )
        return DEFAULT_ASSEMBLY_PARALLEL_ENABLED

    def runtime_assembly_worker_count(
        self,
        job: dict,
        *,
        queue_size: int,
        batch_size: int,
    ) -> tuple[int, dict[str, int | bool]]:
        job_cfg: dict[str, t.Any] = self._runtime_tuning_job_config(job)
        assembly_cfg: dict[str, t.Any] = self._runtime_tuning_assembly_config()

        def pick(name: str, default: int) -> int:
            job_value: t.Any = job_cfg.get(name)
            if job_value not in (None, ""):
                return _clamped_int(
                    job_value,
                    default=default,
                    minimum=1,
                    maximum=DEFAULT_ASSEMBLY_MAX_WORKER_THRESHOLD,
                )
            global_value: t.Any = assembly_cfg.get(name)
            if global_value not in (None, ""):
                return _clamped_int(
                    global_value,
                    default=default,
                    minimum=1,
                    maximum=DEFAULT_ASSEMBLY_MAX_WORKER_THRESHOLD,
                )
            return default

        threshold_2: int = pick(
            "worker_threshold_2",
            DEFAULT_ASSEMBLY_WORKER_THRESHOLD_2,
        )
        threshold_3: int = pick(
            "worker_threshold_3",
            DEFAULT_ASSEMBLY_WORKER_THRESHOLD_3,
        )
        configured_max_workers: int = min(
            DEFAULT_ASSEMBLY_MAX_WORKERS,
            _clamped_int(
                (
                    job_cfg.get("max_workers")
                    if job_cfg.get("max_workers") not in (None, "")
                    else assembly_cfg.get("max_workers")
                ),
                default=DEFAULT_ASSEMBLY_MAX_WORKERS,
                minimum=1,
                maximum=DEFAULT_ASSEMBLY_MAX_WORKERS,
            ),
        )
        enabled: bool = self.runtime_parallel_enabled(job)
        workers: int = 1
        if enabled and configured_max_workers > 1 and batch_size > 1:
            if queue_size > threshold_3 and configured_max_workers >= 3:
                workers = 3
            elif queue_size > threshold_2 and configured_max_workers >= 2:
                workers = 2
            workers = min(workers, configured_max_workers, batch_size)
        return workers, {
            "parallel_enabled": enabled,
            "worker_threshold_2": threshold_2,
            "worker_threshold_3": threshold_3,
            "max_workers": configured_max_workers,
        }

    def durable_outbox_cleanup_config(self) -> dict[str, t.Any]:
        tuning_snapshot: dict[str, t.Any] = self.runtime_tuning.snapshot()
        payload: dict[str, t.Any] = tuning_snapshot.get("payload") or {}
        pgsync_payload: t.Any = payload.get("pgsync")
        root: dict[str, t.Any] = (
            pgsync_payload if isinstance(pgsync_payload, dict) else payload
        )
        cleanup_cfg: t.Any = root.get("durable_outbox_cleanup")
        if not isinstance(cleanup_cfg, dict):
            cleanup_cfg = root.get("durable_change_event_cleanup")
        if not isinstance(cleanup_cfg, dict):
            cleanup_cfg = {}

        def pick(name: str, env_name: str, default: t.Any) -> t.Any:
            value: t.Any = cleanup_cfg.get(name)
            if value not in (None, ""):
                return value
            env_value: str = (os.environ.get(env_name) or "").strip()
            if env_value:
                return env_value
            return default

        enabled_env: str = (
            os.environ.get("PGSYNC_STUDIO_DURABLE_OUTBOX_CLEANUP_ENABLED")
            or ""
        ).strip()
        enabled_nacos: t.Any = cleanup_cfg.get("enabled")
        enabled_source: str = "default"
        enabled_value: t.Any = DEFAULT_DURABLE_OUTBOX_CLEANUP_ENABLED
        if enabled_nacos not in (None, ""):
            enabled_source = "nacos"
            enabled_value = enabled_nacos
        elif enabled_env:
            enabled_source = "environment"
            enabled_value = enabled_env
        enabled: bool = _as_bool(enabled_value, default=False)
        config_error: str = str(tuning_snapshot.get("last_error") or "")
        # A false environment value is an emergency kill switch and always
        # wins.  A failed Nacos refresh must never turn destructive cleanup on.
        if enabled_env and not _as_bool(enabled_env, default=False):
            enabled = False
            enabled_source = "environment_kill_switch"
        elif config_error:
            enabled = False
            enabled_source = "nacos_error_fail_closed"

        interval_seconds_value: t.Any = cleanup_cfg.get("interval_seconds")
        if interval_seconds_value in (None, ""):
            env_interval_seconds: str = (
                os.environ.get(
                    "PGSYNC_STUDIO_DURABLE_OUTBOX_CLEANUP_INTERVAL_SECONDS"
                )
                or ""
            ).strip()
            interval_seconds_value = env_interval_seconds or None
        if interval_seconds_value in (None, ""):
            interval_minutes_value: t.Any = pick(
                "interval_minutes",
                "PGSYNC_STUDIO_DURABLE_OUTBOX_CLEANUP_INTERVAL_MINUTES",
                DEFAULT_DURABLE_OUTBOX_CLEANUP_INTERVAL_SECONDS / 60,
            )
            try:
                interval_seconds_value = float(interval_minutes_value) * 60
            except (TypeError, ValueError):
                interval_seconds_value = (
                    DEFAULT_DURABLE_OUTBOX_CLEANUP_INTERVAL_SECONDS
                )

        return {
            "enabled": enabled,
            "enabled_source": enabled_source,
            "config_error": config_error,
            "retention_hours": _clamped_int(
                pick(
                    "retention_hours",
                    "PGSYNC_STUDIO_DURABLE_OUTBOX_CLEANUP_RETENTION_HOURS",
                    DEFAULT_DURABLE_OUTBOX_CLEANUP_RETENTION_HOURS,
                ),
                default=DEFAULT_DURABLE_OUTBOX_CLEANUP_RETENTION_HOURS,
                minimum=1,
                maximum=24 * 365,
            ),
            "interval_seconds": _clamped_int(
                interval_seconds_value,
                default=DEFAULT_DURABLE_OUTBOX_CLEANUP_INTERVAL_SECONDS,
                minimum=5,
                maximum=24 * 365 * 3600,
            ),
            "batch_size": _clamped_int(
                pick(
                    "batch_size",
                    "PGSYNC_STUDIO_DURABLE_OUTBOX_CLEANUP_BATCH_SIZE",
                    DEFAULT_DURABLE_OUTBOX_CLEANUP_BATCH_SIZE,
                ),
                default=DEFAULT_DURABLE_OUTBOX_CLEANUP_BATCH_SIZE,
                minimum=1,
                maximum=100_000,
            ),
            "max_batches_per_run": _clamped_int(
                pick(
                    "max_batches_per_run",
                    "PGSYNC_STUDIO_DURABLE_OUTBOX_CLEANUP_MAX_BATCHES_PER_RUN",
                    DEFAULT_DURABLE_OUTBOX_CLEANUP_MAX_BATCHES_PER_RUN,
                ),
                default=DEFAULT_DURABLE_OUTBOX_CLEANUP_MAX_BATCHES_PER_RUN,
                minimum=1,
                maximum=1000,
            ),
            "max_rows_per_run": _clamped_int(
                pick(
                    "max_rows_per_run",
                    "PGSYNC_STUDIO_DURABLE_OUTBOX_CLEANUP_MAX_ROWS_PER_RUN",
                    DEFAULT_DURABLE_OUTBOX_CLEANUP_MAX_ROWS_PER_RUN,
                ),
                default=DEFAULT_DURABLE_OUTBOX_CLEANUP_MAX_ROWS_PER_RUN,
                minimum=1,
                maximum=10_000_000,
            ),
            "max_run_seconds": _clamped_int(
                pick(
                    "max_run_seconds",
                    "PGSYNC_STUDIO_DURABLE_OUTBOX_CLEANUP_MAX_RUN_SECONDS",
                    DEFAULT_DURABLE_OUTBOX_CLEANUP_MAX_RUN_SECONDS,
                ),
                default=DEFAULT_DURABLE_OUTBOX_CLEANUP_MAX_RUN_SECONDS,
                minimum=1,
                maximum=3600,
            ),
            "max_rows_per_second": _clamped_int(
                pick(
                    "max_rows_per_second",
                    "PGSYNC_STUDIO_DURABLE_OUTBOX_CLEANUP_MAX_ROWS_PER_SECOND",
                    DEFAULT_DURABLE_OUTBOX_CLEANUP_MAX_ROWS_PER_SECOND,
                ),
                default=DEFAULT_DURABLE_OUTBOX_CLEANUP_MAX_ROWS_PER_SECOND,
                minimum=1,
                maximum=100_000,
            ),
            "batch_pause_ms": _clamped_int(
                pick(
                    "batch_pause_ms",
                    "PGSYNC_STUDIO_DURABLE_OUTBOX_CLEANUP_BATCH_PAUSE_MS",
                    DEFAULT_DURABLE_OUTBOX_CLEANUP_BATCH_PAUSE_MS,
                ),
                default=DEFAULT_DURABLE_OUTBOX_CLEANUP_BATCH_PAUSE_MS,
                minimum=0,
                maximum=60_000,
            ),
            "statement_timeout_ms": _clamped_int(
                pick(
                    "statement_timeout_ms",
                    "PGSYNC_STUDIO_DURABLE_OUTBOX_CLEANUP_STATEMENT_TIMEOUT_MS",
                    DEFAULT_DURABLE_OUTBOX_CLEANUP_STATEMENT_TIMEOUT_MS,
                ),
                default=DEFAULT_DURABLE_OUTBOX_CLEANUP_STATEMENT_TIMEOUT_MS,
                minimum=100,
                maximum=300_000,
            ),
            "lock_timeout_ms": _clamped_int(
                pick(
                    "lock_timeout_ms",
                    "PGSYNC_STUDIO_DURABLE_OUTBOX_CLEANUP_LOCK_TIMEOUT_MS",
                    DEFAULT_DURABLE_OUTBOX_CLEANUP_LOCK_TIMEOUT_MS,
                ),
                default=DEFAULT_DURABLE_OUTBOX_CLEANUP_LOCK_TIMEOUT_MS,
                minimum=100,
                maximum=60_000,
            ),
        }

    def _durable_outbox_cleanup_groups(self) -> list[dict[str, t.Any]]:
        snapshot: dict = self.store.snapshot()
        runtimes: dict = snapshot.get("job_runtime") or {}
        connections: dict[str, dict] = {
            str(connection.get("id") or ""): connection
            for connection in snapshot.get("connections", [])
            if str(connection.get("id") or "")
        }
        groups: dict[tuple[str, str, str], dict[str, t.Any]] = {}
        for raw_job in snapshot.get("jobs", []):
            job: dict = self.normalize_job_definition(raw_job)
            if not job.get("enable_durable_change_events"):
                continue
            job_id: str = str(job.get("id") or "")
            runtime: dict = runtimes.get(job_id) or {}
            cursors: t.Any = runtime.get("native_change_event_cursors") or {}
            if not isinstance(cursors, dict):
                cursors = {}
            try:
                replay_window: int = max(
                    0,
                    int(
                        job.get("durable_change_event_replay_window")
                        or DEFAULT_DURABLE_CHANGE_EVENT_REPLAY_WINDOW
                    ),
                )
            except (TypeError, ValueError):
                replay_window = DEFAULT_DURABLE_CHANGE_EVENT_REPLAY_WINDOW
            job_label: str = str(
                job.get("name") or job.get("index_name") or job_id
            )
            for table in job.get("tables", []):
                connection_id: str = str(table.get("connection_id") or "")
                schema: str = (
                    str(table.get("schema") or "public").strip().lower()
                )
                table_name: str = str(table.get("table") or "").strip()
                if not connection_id or not schema or not table_name:
                    continue
                connection: t.Optional[dict] = connections.get(connection_id)
                physical_identity: str = ""
                connection_error: str = ""
                if connection is None:
                    connection_error = "connection_missing"
                    physical_identity = f"missing:{connection_id}"
                else:
                    try:
                        physical_identity = physical_database_identity(
                            connection
                        )
                    except Exception as exc:
                        connection_error = f"connection_invalid:{exc}"
                        physical_identity = f"invalid:{connection_id}"
                cursor_key: str = f"{connection_id}:{schema}"
                group: dict[str, t.Any] = groups.setdefault(
                    (physical_identity, schema, table_name),
                    {
                        "physical_database_identity": physical_identity,
                        "connection_id": connection_id,
                        "connection": connection,
                        "schema": schema,
                        "table_name": table_name,
                        "connection_ids": set(),
                        "cursor_keys": set(),
                        "safe_event_id": None,
                        "jobs": [],
                        "blocked_jobs": [],
                    },
                )
                group["connection_ids"].add(connection_id)
                group["cursor_keys"].add(cursor_key)
                group["jobs"].append(job_label)
                if connection_error:
                    group["blocked_jobs"].append(job_label)
                    continue
                try:
                    cursor: int = int(cursors.get(cursor_key) or 0)
                except (TypeError, ValueError):
                    cursor = 0
                safe_candidate: int = cursor - replay_window
                if safe_candidate <= 0:
                    group["blocked_jobs"].append(job_label)
                    continue
                current_safe: t.Optional[int] = group.get("safe_event_id")
                group["safe_event_id"] = (
                    safe_candidate
                    if current_safe is None
                    else min(int(current_safe), safe_candidate)
                )

        normalized_groups: list[dict[str, t.Any]] = []
        for group in groups.values():
            normalized_groups.append(
                {
                    **group,
                    "connection_ids": sorted(
                        group.get("connection_ids") or []
                    ),
                    "cursor_keys": sorted(group.get("cursor_keys") or []),
                    "jobs": sorted(set(group.get("jobs") or [])),
                    "blocked_jobs": sorted(
                        set(group.get("blocked_jobs") or [])
                    ),
                    "safe_event_id": int(group.get("safe_event_id") or 0),
                }
            )
        return sorted(
            normalized_groups,
            key=lambda item: (
                item["physical_database_identity"],
                item["schema"],
                item["table_name"],
            ),
        )

    def durable_outbox_capture_reconciliation_config(
        self,
    ) -> dict[str, t.Any]:
        """Return fail-closed capture ownership reconciliation settings."""

        tuning_snapshot: dict[str, t.Any] = self.runtime_tuning.snapshot()
        payload: dict[str, t.Any] = tuning_snapshot.get("payload") or {}
        pgsync_payload: t.Any = payload.get("pgsync")
        root: dict[str, t.Any] = (
            pgsync_payload if isinstance(pgsync_payload, dict) else payload
        )
        raw_config: t.Any = root.get("durable_outbox_capture_reconciliation")
        config: dict[str, t.Any] = (
            raw_config if isinstance(raw_config, dict) else {}
        )

        enabled_env: str = (
            os.environ.get(
                "PGSYNC_STUDIO_DURABLE_OUTBOX_CAPTURE_RECONCILIATION_ENABLED"
            )
            or ""
        ).strip()
        enabled_value: t.Any = config.get("enabled")
        enabled_source: str = "nacos"
        if enabled_value in (None, ""):
            enabled_value = (
                enabled_env
                if enabled_env
                else DEFAULT_DURABLE_OUTBOX_CAPTURE_RECONCILIATION_ENABLED
            )
            enabled_source = "environment" if enabled_env else "default"
        enabled: bool = _as_bool(enabled_value, default=False)
        config_error: str = str(tuning_snapshot.get("last_error") or "")
        if enabled_env and not _as_bool(enabled_env, default=False):
            enabled = False
            enabled_source = "environment_kill_switch"
        elif config_error:
            enabled = False
            enabled_source = "nacos_error_fail_closed"

        apply_value: t.Any = config.get("apply_changes")
        if apply_value in (None, ""):
            apply_value = (
                os.environ.get(
                    "PGSYNC_STUDIO_DURABLE_OUTBOX_CAPTURE_RECONCILIATION_APPLY_CHANGES"
                )
                or DEFAULT_DURABLE_OUTBOX_CAPTURE_RECONCILIATION_APPLY_CHANGES
            )
        lock_timeout_value: t.Any = config.get("lock_timeout_ms")
        if lock_timeout_value in (None, ""):
            lock_timeout_value = (
                os.environ.get(
                    "PGSYNC_STUDIO_DURABLE_OUTBOX_CAPTURE_RECONCILIATION_LOCK_TIMEOUT_MS"
                )
                or DEFAULT_DURABLE_OUTBOX_CAPTURE_RECONCILIATION_LOCK_TIMEOUT_MS
            )
        return {
            "enabled": enabled,
            "enabled_source": enabled_source,
            "config_error": config_error,
            "apply_changes": _as_bool(apply_value, default=False),
            "lock_timeout_ms": _clamped_int(
                lock_timeout_value,
                default=(
                    DEFAULT_DURABLE_OUTBOX_CAPTURE_RECONCILIATION_LOCK_TIMEOUT_MS
                ),
                minimum=100,
                maximum=60_000,
            ),
        }

    def _durable_outbox_capture_reference_groups(
        self,
    ) -> dict[str, t.Any]:
        """Build capture ownership from all persisted native job definitions."""

        snapshot: dict = self.store.snapshot()
        connections: dict[str, dict] = {
            str(connection.get("id") or ""): connection
            for connection in snapshot.get("connections", [])
            if str(connection.get("id") or "")
        }
        groups: dict[tuple[str, str], dict[str, t.Any]] = {}
        errors: list[dict[str, str]] = []
        for raw_job in snapshot.get("jobs", []):
            job: dict = self.normalize_job_definition(raw_job)
            try:
                if self.determine_job_mode(job) != "native":
                    continue
            except Exception as exc:
                errors.append(
                    {
                        "job_id": str(job.get("id") or ""),
                        "reason": f"job_mode_invalid:{exc}",
                    }
                )
                continue
            job_id: str = str(job.get("id") or "")
            job_label: str = str(
                job.get("name") or job.get("index_name") or job_id
            )
            index_name: str = str(job.get("index_name") or "").strip()
            for table in job.get("tables", []):
                connection_id: str = str(table.get("connection_id") or "")
                schema: str = (
                    str(table.get("schema") or "public").strip().lower()
                )
                table_name: str = str(table.get("table") or "").strip()
                connection: t.Optional[dict] = connections.get(connection_id)
                if not connection or not schema or not table_name:
                    errors.append(
                        {
                            "job_id": job_id,
                            "reason": (
                                "capture_reference_invalid:"
                                f"{connection_id}:{schema}:{table_name}"
                            ),
                        }
                    )
                    continue
                try:
                    physical_identity: str = physical_database_identity(
                        connection
                    )
                except Exception as exc:
                    errors.append(
                        {
                            "job_id": job_id,
                            "reason": f"connection_invalid:{exc}",
                        }
                    )
                    continue
                key = (physical_identity, schema)
                group: dict[str, t.Any] = groups.setdefault(
                    key,
                    {
                        "physical_database_identity": physical_identity,
                        "schema": schema,
                        "connection": connection,
                        "connection_id": connection_id,
                        "connection_ids": set(),
                        "referenced_table_names": set(),
                        "desired_indices_by_table": {},
                        "jobs": set(),
                    },
                )
                group["connection_ids"].add(connection_id)
                group["referenced_table_names"].add(table_name)
                group["jobs"].add(job_label)
                group["desired_indices_by_table"].setdefault(table_name, set())
                if index_name:
                    group["desired_indices_by_table"][table_name].add(
                        index_name
                    )

        normalized: list[dict[str, t.Any]] = []
        for group in groups.values():
            normalized.append(
                {
                    **group,
                    "connection_ids": sorted(group["connection_ids"]),
                    "referenced_table_names": sorted(
                        group["referenced_table_names"]
                    ),
                    "desired_indices_by_table": {
                        table_name: sorted(index_names)
                        for table_name, index_names in sorted(
                            group["desired_indices_by_table"].items()
                        )
                    },
                    "jobs": sorted(group["jobs"]),
                }
            )
        return {
            "groups": sorted(
                normalized,
                key=lambda item: (
                    item["physical_database_identity"],
                    item["schema"],
                ),
            ),
            "errors": errors,
        }

    def _capture_ownership_matches(
        self,
        physical_identity: str,
        schema: str,
        referenced_table_names: list[str],
        desired_indices_by_table: dict[str, list[str]],
    ) -> bool:
        """Recheck persisted ownership while the database view lock is held."""

        current = self._durable_outbox_capture_reference_groups()
        if current.get("errors"):
            return False
        expected_references = sorted(referenced_table_names)
        expected_indices = {
            table_name: sorted(index_names)
            for table_name, index_names in sorted(
                desired_indices_by_table.items()
            )
        }
        for group in current.get("groups") or []:
            if (
                str(group["physical_database_identity"]) == physical_identity
                and str(group["schema"]) == schema
            ):
                return bool(
                    group["referenced_table_names"] == expected_references
                    and group["desired_indices_by_table"] == expected_indices
                )
        if expected_references or expected_indices:
            return False
        for connection in self.store.snapshot().get("connections", []):
            try:
                if physical_database_identity(connection) == physical_identity:
                    return True
            except Exception:
                return False
        return False

    def run_durable_outbox_capture_reconciliation_cycle(
        self,
        config: t.Optional[dict[str, t.Any]] = None,
    ) -> dict[str, t.Any]:
        reconciliation_config: dict[str, t.Any] = (
            dict(config)
            if isinstance(config, dict)
            else self.durable_outbox_capture_reconciliation_config()
        )
        summary: dict[str, t.Any] = {
            "ok": True,
            "enabled": _as_bool(
                reconciliation_config.get("enabled"), default=False
            ),
            "enabled_source": reconciliation_config.get(
                "enabled_source", "manual"
            ),
            "config_error": str(
                reconciliation_config.get("config_error") or ""
            ),
            "apply_changes": _as_bool(
                reconciliation_config.get("apply_changes"), default=False
            ),
            "started_at": utcnow(),
            "finished_at": None,
            "reference_errors": [],
            "ownership_safe": False,
            "groups": [],
        }
        if not summary["enabled"]:
            summary["finished_at"] = utcnow()
            self._durable_outbox_capture_reconciliation_last_summary = (
                deepcopy(summary)
            )
            return summary

        references = self._durable_outbox_capture_reference_groups()
        summary["reference_errors"] = list(references.get("errors") or [])
        summary["ownership_safe"] = not summary["reference_errors"]
        reference_groups: list[dict[str, t.Any]] = list(
            references.get("groups") or []
        )
        groups_by_key: dict[tuple[str, str], dict[str, t.Any]] = {
            (
                str(group["physical_database_identity"]),
                str(group["schema"]),
            ): group
            for group in reference_groups
        }
        discovered_physical_identities: set[str] = set()
        for connection in self.store.snapshot().get("connections", []):
            connection_id: str = str(connection.get("id") or "")
            try:
                physical_identity = physical_database_identity(connection)
            except Exception as exc:
                summary["reference_errors"].append(
                    {
                        "job_id": "",
                        "reason": (
                            f"connection_invalid:{connection_id}:{exc}"
                        ),
                    }
                )
                continue
            if physical_identity in discovered_physical_identities:
                continue
            discovered_physical_identities.add(physical_identity)
            try:
                captured_schemas = self.db.list_change_capture_schemas(
                    connection
                )
            except Exception as exc:
                summary["reference_errors"].append(
                    {
                        "job_id": "",
                        "reason": (
                            "capture_schema_discovery_failed:"
                            f"{connection_id}:{exc}"
                        ),
                    }
                )
                continue
            for schema in captured_schemas:
                normalized_schema: str = str(schema or "").strip().lower()
                if not normalized_schema:
                    continue
                key = (physical_identity, normalized_schema)
                if key in groups_by_key:
                    continue
                discovered_group: dict[str, t.Any] = {
                    "physical_database_identity": physical_identity,
                    "schema": normalized_schema,
                    "connection": connection,
                    "connection_id": connection_id,
                    "connection_ids": [connection_id],
                    "referenced_table_names": [],
                    "desired_indices_by_table": {},
                    "jobs": [],
                }
                groups_by_key[key] = discovered_group
                reference_groups.append(discovered_group)
        if summary["reference_errors"]:
            summary["ok"] = False
        for group in sorted(
            reference_groups,
            key=lambda item: (
                item["physical_database_identity"],
                item["schema"],
            ),
        ):
            group_summary: dict[str, t.Any] = {
                "physical_database_identity": group[
                    "physical_database_identity"
                ],
                "schema": group["schema"],
                "connection_id": group["connection_id"],
                "connection_ids": group["connection_ids"],
                "jobs": group["jobs"],
                "referenced_table_names": group["referenced_table_names"],
                "desired_indices_by_table": group["desired_indices_by_table"],
                "safe_for_orphan_cleanup": False,
                "error": "",
            }
            summary["groups"].append(group_summary)
            try:
                result = self.db.reconcile_change_capture(
                    group["connection"],
                    group["schema"],
                    desired_indices_by_table=group["desired_indices_by_table"],
                    referenced_table_names=group["referenced_table_names"],
                    apply_changes=summary["apply_changes"],
                    lock_timeout_ms=int(
                        reconciliation_config.get("lock_timeout_ms")
                        or DEFAULT_DURABLE_OUTBOX_CAPTURE_RECONCILIATION_LOCK_TIMEOUT_MS
                    ),
                    ownership_guard=lambda group=group: (
                        self._capture_ownership_matches(
                            str(group["physical_database_identity"]),
                            str(group["schema"]),
                            list(group["referenced_table_names"]),
                            dict(group["desired_indices_by_table"]),
                        )
                    ),
                )
                group_summary.update(result)
                if not result.get("ok", True):
                    summary["ok"] = False
            except Exception as exc:
                group_summary["error"] = str(exc)
                group_summary["safe_for_orphan_cleanup"] = False
                summary["ok"] = False
                logger.warning(
                    "Durable outbox capture reconciliation failed for "
                    "%s/%s: %s",
                    group["physical_database_identity"],
                    group["schema"],
                    exc,
                )
        summary["finished_at"] = utcnow()
        self._durable_outbox_capture_reconciliation_last_summary = deepcopy(
            summary
        )
        return summary

    def durable_outbox_capture_reconciliation_status(
        self,
    ) -> dict[str, t.Any]:
        return {
            "config": self.durable_outbox_capture_reconciliation_config(),
            "last_cycle": deepcopy(
                self._durable_outbox_capture_reconciliation_last_summary
            ),
        }

    def durable_outbox_partition_maintenance_config(
        self,
    ) -> dict[str, t.Any]:
        tuning_snapshot: dict[str, t.Any] = self.runtime_tuning.snapshot()
        payload: dict[str, t.Any] = tuning_snapshot.get("payload") or {}
        pgsync_payload: t.Any = payload.get("pgsync")
        root: dict[str, t.Any] = (
            pgsync_payload if isinstance(pgsync_payload, dict) else payload
        )
        raw_config: t.Any = root.get("durable_outbox_partition_maintenance")
        config: dict[str, t.Any] = (
            raw_config if isinstance(raw_config, dict) else {}
        )

        def pick(name: str, env_name: str, default: t.Any) -> t.Any:
            value: t.Any = config.get(name)
            if value not in (None, ""):
                return value
            env_value: str = (os.environ.get(env_name) or "").strip()
            return env_value if env_value else default

        enabled_env: str = (
            os.environ.get(
                "PGSYNC_STUDIO_DURABLE_OUTBOX_PARTITION_MAINTENANCE_ENABLED"
            )
            or ""
        ).strip()
        enabled_nacos: t.Any = config.get("enabled")
        enabled_source: str = "default"
        enabled_value: t.Any = (
            DEFAULT_DURABLE_OUTBOX_PARTITION_MAINTENANCE_ENABLED
        )
        if enabled_nacos not in (None, ""):
            enabled_source = "nacos"
            enabled_value = enabled_nacos
        elif enabled_env:
            enabled_source = "environment"
            enabled_value = enabled_env
        enabled: bool = _as_bool(enabled_value, default=False)
        config_error: str = str(tuning_snapshot.get("last_error") or "")
        if enabled_env and not _as_bool(enabled_env, default=False):
            enabled = False
            enabled_source = "environment_kill_switch"
        elif config_error:
            enabled = False
            enabled_source = "nacos_error_fail_closed"

        raw_run_times: t.Any = pick(
            "run_times_utc",
            "PGSYNC_STUDIO_DURABLE_OUTBOX_PARTITION_RUN_TIMES_UTC",
            list(DEFAULT_DURABLE_OUTBOX_PARTITION_RUN_TIMES_UTC),
        )
        if isinstance(raw_run_times, str):
            candidates: list[t.Any] = raw_run_times.split(",")
        elif isinstance(raw_run_times, (list, tuple)):
            candidates = list(raw_run_times)
        else:
            candidates = []
        run_times: list[str] = []
        invalid_run_times: bool = not candidates
        for item in candidates:
            label: str = str(item or "").strip()
            try:
                parsed: datetime = datetime.strptime(label, "%H:%M")
            except ValueError:
                invalid_run_times = True
                continue
            normalized: str = parsed.strftime("%H:%M")
            if normalized not in run_times:
                run_times.append(normalized)
        run_times.sort()
        if invalid_run_times or not run_times:
            run_times = list(DEFAULT_DURABLE_OUTBOX_PARTITION_RUN_TIMES_UTC)
            if enabled:
                enabled = False
                enabled_source = "invalid_schedule_fail_closed"
            if not config_error:
                config_error = "invalid run_times_utc"

        return {
            "enabled": enabled,
            "enabled_source": enabled_source,
            "config_error": config_error,
            "retention_days": _clamped_int(
                pick(
                    "retention_days",
                    "PGSYNC_STUDIO_DURABLE_OUTBOX_PARTITION_RETENTION_DAYS",
                    DEFAULT_DURABLE_OUTBOX_PARTITION_RETENTION_DAYS,
                ),
                default=DEFAULT_DURABLE_OUTBOX_PARTITION_RETENTION_DAYS,
                minimum=1,
                maximum=365,
            ),
            "future_days": _clamped_int(
                pick(
                    "future_days",
                    "PGSYNC_STUDIO_DURABLE_OUTBOX_PARTITION_FUTURE_DAYS",
                    DEFAULT_DURABLE_OUTBOX_PARTITION_FUTURE_DAYS,
                ),
                default=DEFAULT_DURABLE_OUTBOX_PARTITION_FUTURE_DAYS,
                minimum=1,
                maximum=31,
            ),
            "allow_orphan_tables": _as_bool(
                pick(
                    "allow_orphan_tables",
                    "PGSYNC_STUDIO_DURABLE_OUTBOX_PARTITION_ALLOW_ORPHAN_TABLES",
                    DEFAULT_DURABLE_OUTBOX_PARTITION_ALLOW_ORPHAN_TABLES,
                ),
                default=False,
            ),
            "run_times_utc": run_times,
            "lock_timeout_ms": _clamped_int(
                pick(
                    "lock_timeout_ms",
                    "PGSYNC_STUDIO_DURABLE_OUTBOX_PARTITION_LOCK_TIMEOUT_MS",
                    DEFAULT_DURABLE_OUTBOX_PARTITION_LOCK_TIMEOUT_MS,
                ),
                default=DEFAULT_DURABLE_OUTBOX_PARTITION_LOCK_TIMEOUT_MS,
                minimum=100,
                maximum=60_000,
            ),
        }

    def _durable_outbox_partition_groups(self) -> list[dict[str, t.Any]]:
        groups: dict[tuple[str, str], dict[str, t.Any]] = {}
        for table_group in self._durable_outbox_cleanup_groups():
            key = (
                str(table_group["physical_database_identity"]),
                str(table_group["schema"]),
            )
            group: dict[str, t.Any] = groups.setdefault(
                key,
                {
                    "physical_database_identity": key[0],
                    "schema": key[1],
                    "connection": table_group.get("connection"),
                    "connection_id": table_group.get("connection_id"),
                    "connection_ids": set(),
                    "table_names": set(),
                    "jobs": set(),
                    "blocked_jobs": set(),
                    "blocked_table_names": set(),
                    "safe_event_ids_by_table": {},
                    "safe_event_id": None,
                },
            )
            group["connection_ids"].update(
                table_group.get("connection_ids") or []
            )
            group["table_names"].add(table_group.get("table_name"))
            group["jobs"].update(table_group.get("jobs") or [])
            group["blocked_jobs"].update(table_group.get("blocked_jobs") or [])
            table_name: str = str(table_group.get("table_name") or "")
            candidate: int = int(table_group.get("safe_event_id") or 0)
            if table_group.get("blocked_jobs") or candidate <= 0:
                if table_name:
                    group["blocked_table_names"].add(table_name)
            elif table_name:
                current_table_safe: t.Optional[int] = group[
                    "safe_event_ids_by_table"
                ].get(table_name)
                group["safe_event_ids_by_table"][table_name] = (
                    candidate
                    if current_table_safe is None
                    else min(int(current_table_safe), candidate)
                )
            if candidate > 0:
                current_safe: t.Optional[int] = group.get("safe_event_id")
                group["safe_event_id"] = (
                    candidate
                    if current_safe is None
                    else min(int(current_safe), candidate)
                )
        normalized: list[dict[str, t.Any]] = []
        for group in groups.values():
            normalized.append(
                {
                    **group,
                    "connection_ids": sorted(group["connection_ids"]),
                    "table_names": sorted(
                        item for item in group["table_names"] if item
                    ),
                    "jobs": sorted(group["jobs"]),
                    "blocked_jobs": sorted(group["blocked_jobs"]),
                    "blocked_table_names": sorted(
                        group["blocked_table_names"]
                    ),
                    "safe_event_ids_by_table": dict(
                        sorted(group["safe_event_ids_by_table"].items())
                    ),
                    "safe_event_id": int(group.get("safe_event_id") or 0),
                }
            )
        return sorted(
            normalized,
            key=lambda item: (
                item["physical_database_identity"],
                item["schema"],
            ),
        )

    def run_durable_outbox_partition_maintenance_cycle(
        self,
        config: t.Optional[dict[str, t.Any]] = None,
        *,
        now: t.Optional[datetime] = None,
    ) -> dict[str, t.Any]:
        maintenance_config: dict[str, t.Any] = (
            dict(config)
            if isinstance(config, dict)
            else self.durable_outbox_partition_maintenance_config()
        )
        summary: dict[str, t.Any] = {
            "ok": True,
            "enabled": _as_bool(
                maintenance_config.get("enabled"), default=False
            ),
            "enabled_source": maintenance_config.get(
                "enabled_source", "manual"
            ),
            "config_error": str(maintenance_config.get("config_error") or ""),
            "retention_days": int(
                maintenance_config.get("retention_days")
                or DEFAULT_DURABLE_OUTBOX_PARTITION_RETENTION_DAYS
            ),
            "future_days": int(
                maintenance_config.get("future_days")
                or DEFAULT_DURABLE_OUTBOX_PARTITION_FUTURE_DAYS
            ),
            "allow_orphan_tables_requested": _as_bool(
                maintenance_config.get("allow_orphan_tables"),
                default=False,
            ),
            "started_at": utcnow(),
            "finished_at": None,
            "groups": [],
            "dropped_partitions": 0,
            "capture_reconciliation": None,
        }
        if not summary["enabled"]:
            summary["finished_at"] = utcnow()
            with self._durable_outbox_partition_status_lock:
                self._durable_outbox_partition_last_summary = deepcopy(summary)
            return summary

        reconciliation = self.run_durable_outbox_capture_reconciliation_cycle()
        summary["capture_reconciliation"] = reconciliation
        reconciliation_by_group: dict[tuple[str, str], dict[str, t.Any]] = {
            (
                str(group.get("physical_database_identity") or ""),
                str(group.get("schema") or ""),
            ): group
            for group in reconciliation.get("groups") or []
        }

        for group in self._durable_outbox_partition_groups():
            blocked_jobs: list[str] = list(group.get("blocked_jobs") or [])
            blocked_table_names: list[str] = list(
                group.get("blocked_table_names") or []
            )
            safe_event_ids_by_table: dict[str, int] = dict(
                group.get("safe_event_ids_by_table") or {}
            )
            safe_event_id: int = int(group.get("safe_event_id") or 0)
            reconciliation_group: dict[str, t.Any] = (
                reconciliation_by_group.get(
                    (
                        str(group["physical_database_identity"]),
                        str(group["schema"]),
                    ),
                    {},
                )
            )
            referenced_table_names: list[str] = list(
                group.get("table_names") or []
            )
            allow_orphan_tables: bool = bool(
                summary["allow_orphan_tables_requested"]
                and reconciliation.get("enabled")
                and reconciliation.get("ownership_safe")
                and reconciliation_group.get("safe_for_orphan_cleanup")
            )
            group_summary: dict[str, t.Any] = {
                "physical_database_identity": group[
                    "physical_database_identity"
                ],
                "connection_id": group.get("connection_id"),
                "connection_ids": group.get("connection_ids") or [],
                "schema": group["schema"],
                "table_names": group.get("table_names") or [],
                "jobs": group.get("jobs") or [],
                "blocked_jobs": blocked_jobs,
                "blocked_table_names": blocked_table_names,
                "safe_event_ids_by_table": safe_event_ids_by_table,
                "safe_event_id": safe_event_id,
                "referenced_table_names": referenced_table_names,
                "capture_referenced_table_names": list(
                    reconciliation_group.get("referenced_table_names") or []
                ),
                "allow_orphan_tables": allow_orphan_tables,
                "capture_reconciliation_safe": bool(
                    reconciliation_group.get("safe_for_orphan_cleanup")
                ),
                "mode": "",
                "locked": False,
                "ensured_partitions": [],
                "dropped_partitions": [],
                "blocked_partitions": [],
                "orphan_tables": [],
                "stop_reason": "",
                "error": "",
            }
            summary["groups"].append(group_summary)
            connection: t.Optional[dict] = group.get("connection")
            if not isinstance(connection, dict):
                group_summary["error"] = "connection_missing"
                group_summary["stop_reason"] = "connection_missing"
                summary["ok"] = False
                continue
            try:
                result = self.db.maintain_change_event_partitions(
                    connection,
                    group_summary["schema"],
                    retention_days=summary["retention_days"],
                    future_days=summary["future_days"],
                    safe_event_ids_by_table=safe_event_ids_by_table,
                    blocked_table_names=blocked_table_names,
                    allow_orphan_tables=allow_orphan_tables,
                    referenced_table_names=referenced_table_names,
                    lock_timeout_ms=int(
                        maintenance_config.get("lock_timeout_ms")
                        or DEFAULT_DURABLE_OUTBOX_PARTITION_LOCK_TIMEOUT_MS
                    ),
                    lock_key=(
                        "pgsync:durable-outbox-partition:v1:"
                        f"{group_summary['physical_database_identity']}:"
                        f"{group_summary['schema']}"
                    ),
                    now=now,
                )
            except Exception as exc:
                group_summary["error"] = str(exc)
                group_summary["stop_reason"] = "error"
                summary["ok"] = False
                logger.warning(
                    "Durable outbox partition maintenance failed for "
                    "%s/%s: %s",
                    group_summary["physical_database_identity"],
                    group_summary["schema"],
                    exc,
                )
                continue
            for field in (
                "mode",
                "locked",
                "ensured_partitions",
                "dropped_partitions",
                "blocked_partitions",
                "orphan_tables",
                "stop_reason",
            ):
                if field in result:
                    group_summary[field] = result[field]
            summary["dropped_partitions"] += len(
                group_summary["dropped_partitions"]
            )
        summary["finished_at"] = utcnow()
        with self._durable_outbox_partition_status_lock:
            self._durable_outbox_partition_last_summary = deepcopy(summary)
        logger.info(
            "Durable outbox partition maintenance finished: ok=%s, "
            "dropped_partitions=%s, groups=%s.",
            summary["ok"],
            summary["dropped_partitions"],
            len(summary["groups"]),
        )
        return summary

    @staticmethod
    def _next_partition_maintenance_run(
        run_times_utc: list[str],
        now: t.Optional[datetime] = None,
    ) -> datetime:
        current: datetime = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        current = current.astimezone(timezone.utc)
        candidates: list[datetime] = []
        for label in run_times_utc:
            parsed = datetime.strptime(label, "%H:%M")
            candidate = current.replace(
                hour=parsed.hour,
                minute=parsed.minute,
                second=0,
                microsecond=0,
            )
            if candidate <= current:
                candidate += timedelta(days=1)
            candidates.append(candidate)
        return min(candidates)

    def _durable_outbox_partition_maintenance_loop(self) -> None:
        next_run: t.Optional[datetime] = None
        schedule: tuple[str, ...] = ()
        while not self._durable_outbox_partition_stop_event.is_set():
            try:
                config = self.durable_outbox_partition_maintenance_config()
                current_schedule = tuple(config.get("run_times_utc") or [])
                now = datetime.now(timezone.utc)
                if not config.get("enabled"):
                    next_run = None
                    schedule = current_schedule
                    with self._durable_outbox_partition_status_lock:
                        self._durable_outbox_partition_next_run_at = None
                    wait_seconds = float(
                        DEFAULT_DURABLE_OUTBOX_PARTITION_DISABLED_POLL_SECONDS
                    )
                else:
                    if next_run is None or schedule != current_schedule:
                        schedule = current_schedule
                        next_run = self._next_partition_maintenance_run(
                            list(schedule), now
                        )
                    with self._durable_outbox_partition_status_lock:
                        self._durable_outbox_partition_next_run_at = (
                            next_run.isoformat()
                        )
                    if now >= next_run:
                        self.run_durable_outbox_partition_maintenance_cycle(
                            config, now=now
                        )
                        next_run = self._next_partition_maintenance_run(
                            list(schedule), datetime.now(timezone.utc)
                        )
                        with self._durable_outbox_partition_status_lock:
                            self._durable_outbox_partition_next_run_at = (
                                next_run.isoformat()
                            )
                    wait_seconds = min(
                        max(1.0, (next_run - now).total_seconds()), 60.0
                    )
            except Exception:
                logger.exception(
                    "Failed durable outbox partition maintenance cycle"
                )
                wait_seconds = 60.0
            if self._durable_outbox_partition_stop_event.wait(wait_seconds):
                return

    def start_durable_outbox_partition_maintenance_scheduler(self) -> None:
        with self._lock:
            if (
                self._durable_outbox_partition_thread
                and self._durable_outbox_partition_thread.is_alive()
            ):
                return
            self._durable_outbox_partition_stop_event.clear()
            self._durable_outbox_partition_thread = threading.Thread(
                target=self._durable_outbox_partition_maintenance_loop,
                daemon=True,
            )
            self._durable_outbox_partition_thread.start()
        config = self.durable_outbox_partition_maintenance_config()
        logger.info(
            "Durable outbox partition maintenance scheduler started: "
            "enabled=%s, retention=%s days, future=%s days, times=%s UTC.",
            config.get("enabled"),
            config.get("retention_days"),
            config.get("future_days"),
            config.get("run_times_utc"),
        )

    def stop_durable_outbox_partition_maintenance_scheduler(self) -> None:
        with self._lock:
            self._durable_outbox_partition_stop_event.set()
            thread = self._durable_outbox_partition_thread
        if (
            thread
            and thread.is_alive()
            and thread is not threading.current_thread()
        ):
            thread.join(timeout=15)
        with self._lock:
            if not thread or not thread.is_alive():
                self._durable_outbox_partition_thread = None

    def durable_outbox_partition_maintenance_status(
        self,
    ) -> dict[str, t.Any]:
        config = self.durable_outbox_partition_maintenance_config()
        with self._lock:
            scheduler_running = bool(
                self._durable_outbox_partition_thread
                and self._durable_outbox_partition_thread.is_alive()
            )
        with self._durable_outbox_partition_status_lock:
            last_cycle = deepcopy(self._durable_outbox_partition_last_summary)
            next_run_at = self._durable_outbox_partition_next_run_at
        return {
            "config": config,
            "scheduler_running": scheduler_running,
            "next_run_at": next_run_at,
            "last_cycle": last_cycle,
        }

    def run_durable_outbox_cleanup_cycle(
        self,
        config: t.Optional[dict[str, t.Any]] = None,
    ) -> dict[str, t.Any]:
        cleanup_config: dict[str, t.Any] = (
            dict(config)
            if isinstance(config, dict)
            else self.durable_outbox_cleanup_config()
        )
        summary: dict[str, t.Any] = {
            "ok": True,
            "enabled": _as_bool(
                cleanup_config.get("enabled"),
                default=False,
            ),
            "enabled_source": cleanup_config.get("enabled_source", "manual"),
            "config_error": str(cleanup_config.get("config_error") or ""),
            "retention_hours": int(
                cleanup_config.get("retention_hours")
                or DEFAULT_DURABLE_OUTBOX_CLEANUP_RETENTION_HOURS
            ),
            "batch_size": int(
                cleanup_config.get("batch_size")
                or DEFAULT_DURABLE_OUTBOX_CLEANUP_BATCH_SIZE
            ),
            "max_batches_per_run": int(
                cleanup_config.get("max_batches_per_run")
                or DEFAULT_DURABLE_OUTBOX_CLEANUP_MAX_BATCHES_PER_RUN
            ),
            "max_rows_per_run": int(
                cleanup_config.get("max_rows_per_run")
                or DEFAULT_DURABLE_OUTBOX_CLEANUP_MAX_ROWS_PER_RUN
            ),
            "max_run_seconds": int(
                cleanup_config.get("max_run_seconds")
                or DEFAULT_DURABLE_OUTBOX_CLEANUP_MAX_RUN_SECONDS
            ),
            "max_rows_per_second": int(
                cleanup_config.get("max_rows_per_second")
                or DEFAULT_DURABLE_OUTBOX_CLEANUP_MAX_ROWS_PER_SECOND
            ),
            "batch_pause_ms": int(
                cleanup_config.get("batch_pause_ms")
                if cleanup_config.get("batch_pause_ms") is not None
                else DEFAULT_DURABLE_OUTBOX_CLEANUP_BATCH_PAUSE_MS
            ),
            "statement_timeout_ms": int(
                cleanup_config.get("statement_timeout_ms")
                or DEFAULT_DURABLE_OUTBOX_CLEANUP_STATEMENT_TIMEOUT_MS
            ),
            "lock_timeout_ms": int(
                cleanup_config.get("lock_timeout_ms")
                or DEFAULT_DURABLE_OUTBOX_CLEANUP_LOCK_TIMEOUT_MS
            ),
            "started_at": utcnow(),
            "finished_at": None,
            "elapsed_ms": 0,
            "groups": [],
            "deleted_rows": 0,
        }
        if not summary["enabled"]:
            summary["finished_at"] = utcnow()
            with self._durable_outbox_cleanup_status_lock:
                self._durable_outbox_cleanup_last_summary = deepcopy(summary)
            return summary

        started: float = time.time()
        cutoff: datetime = datetime.now(timezone.utc) - timedelta(
            hours=summary["retention_hours"]
        )
        logger.info(
            "Durable outbox cleanup cycle started: retention=%sh, "
            "batch=%s, max_rows/table=%s, max_rate/table=%s rows/s.",
            summary["retention_hours"],
            summary["batch_size"],
            summary["max_rows_per_run"],
            summary["max_rows_per_second"],
        )
        for group in self._durable_outbox_cleanup_groups():
            group_summary: dict[str, t.Any] = {
                "physical_database_identity": group[
                    "physical_database_identity"
                ],
                "connection_id": group["connection_id"],
                "connection_ids": group["connection_ids"],
                "schema": group["schema"],
                "cursor_keys": group["cursor_keys"],
                "table_name": group["table_name"],
                "jobs": group["jobs"],
                "safe_event_id": group["safe_event_id"],
                "blocked_jobs": group["blocked_jobs"],
                "skipped": False,
                "skip_reason": "",
                "deleted_rows": 0,
                "batches": 0,
                "locked": True,
                "error": "",
                "elapsed_ms": 0,
                "rate_limited_ms": 0,
                "stop_reason": "",
            }
            summary["groups"].append(group_summary)
            if group_summary["blocked_jobs"]:
                group_summary["skipped"] = True
                group_summary["skip_reason"] = "missing_cursor"
                continue
            if group_summary["safe_event_id"] <= 0:
                group_summary["skipped"] = True
                group_summary["skip_reason"] = "safe_event_id_not_ready"
                continue
            if not group_summary["table_name"]:
                group_summary["skipped"] = True
                group_summary["skip_reason"] = "no_table"
                continue
            connection: t.Optional[dict] = group.get("connection")
            if not isinstance(connection, dict):
                group_summary["skipped"] = True
                group_summary["skip_reason"] = "connection_missing"
                continue
            try:
                kind_reader = getattr(
                    self.db, "change_event_outbox_kind", None
                )
                if callable(kind_reader):
                    relation_kind = kind_reader(
                        connection, group_summary["schema"]
                    )
                    if relation_kind == "p":
                        group_summary["skipped"] = True
                        group_summary["skip_reason"] = "partitioned_table"
                        group_summary["stop_reason"] = "partitioned_table"
                        continue
                    if relation_kind is None:
                        group_summary["skipped"] = True
                        group_summary["skip_reason"] = "table_missing"
                        group_summary["stop_reason"] = "table_missing"
                        continue
                result: dict[str, t.Any] = self.db.purge_change_events_cycle(
                    connection,
                    group_summary["schema"],
                    table_name_filter=group_summary["table_name"],
                    safe_event_id=group_summary["safe_event_id"],
                    cutoff=cutoff,
                    batch_size=summary["batch_size"],
                    max_batches=summary["max_batches_per_run"],
                    max_rows=summary["max_rows_per_run"],
                    max_run_seconds=summary["max_run_seconds"],
                    max_rows_per_second=summary["max_rows_per_second"],
                    batch_pause_ms=summary["batch_pause_ms"],
                    statement_timeout_ms=summary["statement_timeout_ms"],
                    lock_timeout_ms=summary["lock_timeout_ms"],
                    lock_key=(
                        "pgsync:durable-outbox-cleanup:v2:"
                        f"{group_summary['physical_database_identity']}:"
                        f"{group_summary['schema']}:"
                        f"{group_summary['table_name']}"
                    ),
                    stop_event=self._durable_outbox_cleanup_stop_event,
                )
            except Exception as exc:
                group_summary["error"] = str(exc)
                group_summary["stop_reason"] = "error"
                summary["ok"] = False
                logger.warning(
                    "Durable outbox cleanup failed for %s/%s/%s: %s",
                    group_summary["physical_database_identity"],
                    group_summary["schema"],
                    group_summary["table_name"],
                    exc,
                )
                continue
            group_summary["locked"] = bool(result.get("locked"))
            group_summary["deleted_rows"] = int(
                result.get("deleted_rows") or 0
            )
            group_summary["batches"] = int(result.get("batches") or 0)
            group_summary["elapsed_ms"] = int(result.get("elapsed_ms") or 0)
            group_summary["rate_limited_ms"] = int(
                result.get("rate_limited_ms") or 0
            )
            group_summary["stop_reason"] = str(result.get("stop_reason") or "")
            summary["deleted_rows"] += group_summary["deleted_rows"]

        summary["finished_at"] = utcnow()
        summary["elapsed_ms"] = int((time.time() - started) * 1000)
        with self._durable_outbox_cleanup_status_lock:
            self._durable_outbox_cleanup_last_summary = deepcopy(summary)
        logger.info(
            "Durable outbox cleanup cycle finished: ok=%s, deleted=%s, "
            "tables=%s, elapsed_ms=%s.",
            summary["ok"],
            summary["deleted_rows"],
            len(summary["groups"]),
            summary["elapsed_ms"],
        )
        return summary

    def _durable_outbox_cleanup_loop(self) -> None:
        next_run_at: t.Optional[float] = None
        while not self._durable_outbox_cleanup_stop_event.is_set():
            try:
                config: dict[str, t.Any] = self.durable_outbox_cleanup_config()
                interval_seconds: float = float(
                    config.get("interval_seconds")
                    or DEFAULT_DURABLE_OUTBOX_CLEANUP_INTERVAL_SECONDS
                )
                now: float = time.monotonic()
                if not config.get("enabled"):
                    next_run_at = now + interval_seconds
                    wait_seconds = min(
                        interval_seconds,
                        float(
                            DEFAULT_DURABLE_OUTBOX_CLEANUP_DISABLED_POLL_SECONDS
                        ),
                    )
                else:
                    if next_run_at is None:
                        # Never perform destructive work immediately on Pod
                        # startup.  This also prevents restart loops from
                        # bypassing the configured cadence.
                        next_run_at = now + interval_seconds
                    if now < next_run_at:
                        wait_seconds = min(
                            next_run_at - now,
                            float(
                                DEFAULT_DURABLE_OUTBOX_CLEANUP_DISABLED_POLL_SECONDS
                            ),
                        )
                    else:
                        self.run_durable_outbox_cleanup_cycle(config)
                        next_run_at = time.monotonic() + interval_seconds
                        wait_seconds = min(
                            interval_seconds,
                            float(
                                DEFAULT_DURABLE_OUTBOX_CLEANUP_DISABLED_POLL_SECONDS
                            ),
                        )
            except Exception:
                logger.exception("Failed durable outbox cleanup cycle")
                wait_seconds = float(
                    DEFAULT_DURABLE_OUTBOX_CLEANUP_DISABLED_POLL_SECONDS
                )
            if self._durable_outbox_cleanup_stop_event.wait(wait_seconds):
                return

    def start_durable_outbox_cleanup_scheduler(self) -> None:
        config: dict[str, t.Any] = self.durable_outbox_cleanup_config()
        with self._lock:
            if (
                self._durable_outbox_cleanup_thread
                and self._durable_outbox_cleanup_thread.is_alive()
            ):
                return
            self._durable_outbox_cleanup_stop_event.clear()
            self._durable_outbox_cleanup_thread = threading.Thread(
                target=self._durable_outbox_cleanup_loop,
                daemon=True,
            )
            self._durable_outbox_cleanup_thread.start()
        logger.info(
            "Durable outbox cleanup scheduler started: enabled=%s, "
            "interval=%s seconds, retention=%s hours.",
            config.get("enabled"),
            config.get("interval_seconds"),
            config.get("retention_hours"),
        )

    def stop_durable_outbox_cleanup_scheduler(self) -> None:
        thread: t.Optional[threading.Thread]
        with self._lock:
            self._durable_outbox_cleanup_stop_event.set()
            thread = self._durable_outbox_cleanup_thread
        if (
            thread
            and thread.is_alive()
            and thread is not threading.current_thread()
        ):
            thread.join(timeout=15)
        with self._lock:
            if not thread or not thread.is_alive():
                self._durable_outbox_cleanup_thread = None

    def durable_outbox_cleanup_status(self) -> dict[str, t.Any]:
        config: dict[str, t.Any] = self.durable_outbox_cleanup_config()
        with self._lock:
            scheduler_running: bool = bool(
                self._durable_outbox_cleanup_thread
                and self._durable_outbox_cleanup_thread.is_alive()
            )
        with self._durable_outbox_cleanup_status_lock:
            last_cycle = deepcopy(self._durable_outbox_cleanup_last_summary)
        return {
            "config": config,
            "scheduler_running": scheduler_running,
            "last_cycle": last_cycle,
        }

    def connection_auto_discovery_supported(self) -> bool:
        return (
            isinstance(
                self.store,
                DatabaseConfigStore,
            )
            and self.store.supports_connection_auto_discovery()
        )

    def _seconds_until_next_connection_auto_discovery_run(
        self,
        now: t.Optional[datetime] = None,
    ) -> float:
        current: datetime = now or datetime.now()
        next_run: datetime = current.replace(
            hour=self.connection_auto_discovery_hour,
            minute=self.connection_auto_discovery_minute,
            second=0,
            microsecond=0,
        )
        if next_run <= current:
            next_run += timedelta(days=1)
        return max(1.0, (next_run - current).total_seconds())

    def run_connection_auto_discovery_cycle(self) -> dict[str, t.Any]:
        if not self.connection_auto_discovery_supported():
            return {
                "ok": False,
                "supported": False,
                "reason": "store_not_supported",
            }
        store = self.store
        if not isinstance(store, DatabaseConfigStore):
            return {
                "ok": False,
                "supported": False,
                "reason": "store_not_supported",
            }
        summary: dict[str, t.Any] = store.sync_instance_connections(
            extra_excluded_databases=(
                self._connection_auto_discovery_runtime_excluded_databases
            )
        )
        added_databases: list[str] = list(summary.get("added_databases") or [])
        if added_databases:
            logger.info(
                "Auto-discovered %d new Studio connection(s): %s",
                len(added_databases),
                ", ".join(added_databases),
            )
        else:
            logger.info(
                "Studio connection auto-discovery finished with no new databases."
            )
        return summary

    def _load_connection_auto_discovery_runtime_excluded_databases(
        self,
    ) -> set[str]:
        store = self.store
        if not isinstance(store, DatabaseConfigStore):
            return set()
        try:
            return set(store.list_instance_connection_databases())
        except Exception:
            logger.exception(
                "Failed to load Studio connection auto-discovery blacklist snapshot."
            )
            return set()

    def _connection_auto_discovery_loop(self) -> None:
        while True:
            try:
                self.run_connection_auto_discovery_cycle()
            except Exception:
                logger.exception(
                    "Failed Studio connection auto-discovery cycle"
                )
            wait_seconds: float = (
                self._seconds_until_next_connection_auto_discovery_run()
            )
            if self._connection_auto_discovery_stop_event.wait(wait_seconds):
                return

    def start_connection_auto_discovery_scheduler(self) -> None:
        if not self.connection_auto_discovery_supported():
            return
        with self._lock:
            if (
                self._connection_auto_discovery_thread
                and self._connection_auto_discovery_thread.is_alive()
            ):
                return
            self._connection_auto_discovery_runtime_excluded_databases = (
                self._load_connection_auto_discovery_runtime_excluded_databases()
            )
            self._connection_auto_discovery_stop_event.clear()
            self._connection_auto_discovery_thread = threading.Thread(
                target=self._connection_auto_discovery_loop,
                daemon=True,
            )
            self._connection_auto_discovery_thread.start()
        logger.info(
            "Studio connection auto-discovery scheduled daily at %s.",
            self.connection_auto_discovery_time_label,
        )

    def stop_connection_auto_discovery_scheduler(self) -> None:
        thread: t.Optional[threading.Thread]
        with self._lock:
            self._connection_auto_discovery_stop_event.set()
            thread = self._connection_auto_discovery_thread
            self._connection_auto_discovery_thread = None
            self._connection_auto_discovery_runtime_excluded_databases = set()
        if (
            thread
            and thread.is_alive()
            and thread is not threading.current_thread()
        ):
            thread.join(timeout=1)

    def runtime_should_resume_incremental(
        self, job_id: str, runtime: dict
    ) -> bool:
        if (runtime.get("desired_state") or "running") != "running":
            return False
        job: t.Optional[dict] = self.store.get_job(job_id)
        if not job:
            return False
        normalized_job: dict = self.normalize_job_definition(job)
        expected_resume_strategy: str = self.expected_resume_strategy(
            normalized_job
        )
        return self.runtime_can_resume_incremental(
            runtime,
            expected_resume_strategy,
        )

    def failed_runtime_should_auto_recover(
        self,
        job_id: str,
        runtime: dict,
    ) -> bool:
        """Return whether a failed runner may safely resume incrementally."""
        if runtime.get("status") != "failed":
            return False
        if runtime.get("phase") != "failed":
            return False
        if runtime.get("assembly_recovery_requires_full_sync"):
            return False
        return self.runtime_should_resume_incremental(job_id, runtime)

    def failed_recovery_backoff_seconds(self, attempt_count: int) -> float:
        """Return capped exponential backoff for a failed runner."""
        normalized_attempt_count: int = max(1, int(attempt_count or 1))
        exponent: int = min(20, normalized_attempt_count - 1)
        return min(
            self.failed_recovery_max_delay_seconds,
            self.failed_recovery_initial_delay_seconds * (2**exponent),
        )

    def schedule_failed_runtime_recovery(
        self,
        job_id: str,
        runtime: t.Optional[dict] = None,
    ) -> dict:
        """Persist the next retry for an eligible failed runner."""
        current_runtime: dict = (
            deepcopy(runtime)
            if isinstance(runtime, dict)
            else self.store.get_job_runtime(job_id)
        )
        if not self.failed_runtime_should_auto_recover(
            job_id,
            current_runtime,
        ):
            return current_runtime
        raw_next_attempt_ts: t.Any = current_runtime.get(
            "runner_recovery_next_attempt_ts"
        )
        if raw_next_attempt_ts not in (None, ""):
            try:
                if math.isfinite(float(raw_next_attempt_ts)):
                    return current_runtime
            except (TypeError, ValueError):
                pass
        try:
            previous_attempt_count: int = max(
                0,
                int(current_runtime.get("runner_recovery_attempt_count") or 0),
            )
        except (TypeError, ValueError):
            previous_attempt_count = 0
        attempt_count: int = previous_attempt_count + 1
        delay_seconds: float = self.failed_recovery_backoff_seconds(
            attempt_count
        )
        now_ts: float = utc_timestamp()
        patch: dict = {
            "runner_recovery_attempt_count": attempt_count,
            "runner_recovery_next_attempt_ts": now_ts + delay_seconds,
            "runner_recovery_retry_delay_seconds": delay_seconds,
            "runner_recovery_last_failure_at": utcnow(),
            "runner_recovery_last_error": str(
                current_runtime.get("last_error") or ""
            ),
            "runner_recovery_started_at": None,
            "runner_recovery_started_ts": None,
        }
        updated_runtime: dict = self.store.update_job_runtime(job_id, patch)
        self.append_log(
            job_id,
            "Runner auto-recovery attempt "
            f"{attempt_count} scheduled in {delay_seconds:g}s.",
        )
        return updated_runtime

    def failed_runtime_recovery_is_due(
        self,
        runtime: dict,
        *,
        now_ts: t.Optional[float] = None,
    ) -> bool:
        raw_next_attempt_ts: t.Any = runtime.get(
            "runner_recovery_next_attempt_ts"
        )
        if raw_next_attempt_ts in (None, ""):
            return False
        try:
            next_attempt_ts: float = float(raw_next_attempt_ts)
        except (TypeError, ValueError):
            return False
        return next_attempt_ts <= (
            utc_timestamp() if now_ts is None else float(now_ts)
        )

    def clear_stable_runner_recovery_state(
        self,
        job_id: str,
        runtime: dict,
    ) -> None:
        """Clear consecutive-failure backoff after a stable resumed run."""
        if runtime.get("status") != "running":
            return
        try:
            attempt_count: int = int(
                runtime.get("runner_recovery_attempt_count") or 0
            )
            started_ts: float = float(
                runtime.get("runner_recovery_started_ts") or 0
            )
        except (TypeError, ValueError):
            return
        if attempt_count <= 0 or started_ts <= 0:
            return
        if utc_timestamp() - started_ts < self.failed_recovery_stable_seconds:
            return
        self.store.update_job_runtime(
            job_id,
            {
                "runner_recovery_attempt_count": 0,
                "runner_recovery_next_attempt_ts": None,
                "runner_recovery_retry_delay_seconds": 0,
                "runner_recovery_started_at": None,
                "runner_recovery_started_ts": None,
                "runner_recovery_succeeded_at": utcnow(),
            },
        )
        self.append_log(
            job_id,
            "Runner remained healthy after auto-recovery; retry backoff reset.",
        )

    def build_resumable_stop_runtime_patch(
        self,
        job_id: str,
        runtime: dict,
    ) -> dict:
        resumable_after_restart: bool = self.runtime_should_resume_incremental(
            job_id,
            runtime,
        )
        return self.build_restart_runtime_patch(
            runtime,
            resumable_after_restart=resumable_after_restart,
        )

    def build_restart_runtime_patch(
        self,
        runtime: dict,
        *,
        resumable_after_restart: bool,
    ) -> dict:
        desired_state: str = runtime.get("desired_state") or "running"
        if resumable_after_restart:
            phase: str = PHASE_AWAITING_INCREMENTAL_RESUME
        elif desired_state != "running":
            phase = "stopped"
        else:
            phase = "stopped_after_restart"
        patch: dict = {
            "status": "stopped",
            "phase": phase,
            "desired_state": desired_state,
            "pid": None,
            "native_pid": None,
            "owner": None,
            "heartbeat_at": None,
            "native_bootstrap_pid": None,
            "native_setup_pid": None,
            "assembly_incremental_running": False,
            "assembly_current_batch_started_at": None,
            "assembly_pending_batch_count": 0,
            "assembly_pending_batch_at": None,
            "listener_states": {},
            "listener_total_count": 0,
            "listener_healthy_count": 0,
            "listener_recovering_count": 0,
            "listener_degraded_count": 0,
        }
        if resumable_after_restart:
            patch["last_error"] = ""
        elif runtime.get("status") == "starting":
            patch["last_error"] = (
                runtime.get("last_error")
                or "Studio 重启，之前的任务启动流程已中断。"
            )
        return patch

    def describe_job_holder(
        self,
        job_id: str,
        services: t.Optional[dict] = None,
    ) -> dict:
        resolved_services: dict = (
            deepcopy(services)
            if isinstance(services, dict)
            else self.store.snapshot().get("services", {})
        )
        if not (resolved_services.get("redis", {}) or {}).get("host"):
            return {}
        try:
            lease = RedisJobLease.from_services(
                resolved_services,
                job_id=job_id,
                owner_instance_id=self.instance_id,
            )
        except Exception as exc:
            logger.warning(
                "Could not inspect job lease holder for %s: %s",
                job_id,
                exc,
            )
            return {}
        if isinstance(lease, NoopJobLease):
            return {}
        try:
            return lease.describe_holder()
        except Exception as exc:
            logger.warning(
                "Could not describe job lease holder for %s: %s",
                job_id,
                exc,
            )
            return {}

    def job_has_active_lease(
        self,
        job_id: str,
        services: t.Optional[dict] = None,
    ) -> bool:
        return bool(self.describe_job_holder(job_id, services))

    def run_recovery_cycle(self) -> list[str]:
        with self._lock:
            resumable_job_ids: list[str] = self.reconcile_runtime_states()
        started_job_ids: list[str] = []
        for job_id in resumable_job_ids:
            with self._lock:
                runtime: dict = self.store.get_job_runtime(job_id)
                if not self.runtime_should_resume_incremental(job_id, runtime):
                    continue
                if job_id in self.runners or self.job_has_active_lease(job_id):
                    continue
                try:
                    self.start_job(job_id)
                except Exception as exc:
                    logger.warning(
                        "Could not auto-resume incremental job %s: %s",
                        job_id,
                        exc,
                    )
                    self.append_log(job_id, f"自动恢复增量任务失败：{exc}")
                    failed_runtime: dict = self.update_runtime(
                        job_id,
                        {
                            "status": "failed",
                            "phase": "failed",
                            "last_error": str(exc),
                        },
                    )
                    self.schedule_failed_runtime_recovery(
                        job_id,
                        failed_runtime,
                    )
                    continue
            started_job_ids.append(job_id)
        return started_job_ids

    def _recovery_loop(self) -> None:
        self.run_recovery_cycle()
        while not self._recovery_stop_event.wait(
            self.recovery_poll_interval_seconds
        ):
            self.run_recovery_cycle()

    def start_background_recovery(self) -> None:
        with self._lock:
            if self._recovery_thread and self._recovery_thread.is_alive():
                return
            self._recovery_stop_event.clear()
            self._recovery_thread = threading.Thread(
                target=self._recovery_loop,
                daemon=True,
            )
            self._recovery_thread.start()

    def stop_background_recovery(self) -> None:
        thread: t.Optional[threading.Thread]
        with self._lock:
            self._recovery_stop_event.set()
            thread = self._recovery_thread
            self._recovery_thread = None
        if (
            thread
            and thread.is_alive()
            and thread is not threading.current_thread()
        ):
            thread.join(timeout=1)

    def prepare_for_shutdown(
        self,
        *,
        join_timeout_seconds: float = 15.0,
    ) -> None:
        self.stop_durable_outbox_partition_maintenance_scheduler()
        self.stop_durable_outbox_cleanup_scheduler()
        self.stop_connection_auto_discovery_scheduler()
        self.stop_background_recovery()
        with self._lock:
            runners: list[tuple[str, t.Any]] = list(self.runners.items())
        shutdown_targets: list[tuple[str, t.Any, dict, bool]] = []
        for job_id, runner in runners:
            runtime: dict = self.store.get_job_runtime(job_id)
            resumable_after_restart: bool = (
                self.runtime_should_resume_incremental(
                    job_id,
                    runtime,
                )
            )
            shutdown_targets.append(
                (
                    job_id,
                    runner,
                    runtime,
                    resumable_after_restart,
                )
            )
            try:
                runner.stop()
            except Exception:
                logger.exception(
                    "Failed to stop runner during shutdown: %s",
                    job_id,
                )
        for (
            job_id,
            runner,
            runtime,
            resumable_after_restart,
        ) in shutdown_targets:
            runner_thread: t.Optional[threading.Thread] = getattr(
                runner,
                "thread",
                None,
            )
            thread_stopped: bool = True
            if runner_thread and runner_thread.is_alive():
                runner_thread.join(timeout=join_timeout_seconds)
                thread_stopped = not runner_thread.is_alive()
            current_runtime: dict = self.store.get_job_runtime(job_id)
            patch: dict = self.build_restart_runtime_patch(
                current_runtime or runtime,
                resumable_after_restart=resumable_after_restart,
            )
            self.update_runtime(job_id, patch)
            if thread_stopped:
                self.release_job_lease(job_id, clear_owner=True)
            else:
                logger.warning(
                    "Runner %s did not stop before shutdown timeout; lease will expire naturally.",
                    job_id,
                )
            with self._lock:
                self.runners.pop(job_id, None)

    def attach_index_document_counts(self, payload: dict) -> None:
        jobs: list[dict] = payload.get("jobs", [])
        if not jobs:
            return
        try:
            search = SearchAdapter(payload.get("services", {}))
        except Exception as exc:
            logger.warning(
                "Could not build search adapter for bootstrap payload: %s", exc
            )
            return

        runtimes: dict = payload.setdefault("job_runtime", {})
        for job in jobs:
            job_id: str = job["id"]
            runtime: dict = dict(runtimes.get(job_id, {}))
            try:
                index_exists: bool = search.index_exists(job["index_name"])
                runtime["index_exists"] = index_exists
                runtime["job_locked"] = index_exists
                runtime["index_document_count"] = (
                    search.count_documents(job["index_name"])
                    if index_exists
                    else 0
                )
            except Exception as exc:
                logger.warning(
                    "Could not fetch index document count for %s (%s): %s",
                    job_id,
                    job.get("index_name"),
                    exc,
                )
                runtime.setdefault("index_exists", False)
                runtime.setdefault(
                    "job_locked",
                    bool(runtime.get("index_exists")),
                )
                runtime.setdefault("index_document_count", None)
            runtimes[job_id] = runtime

    def reconcile_runtime_states(self) -> list[str]:
        """Clear stale runtime state left behind after a studio restart."""
        snapshot: dict = self.store.snapshot()
        services: dict = snapshot.get("services", {})
        resumable_job_ids: list[str] = []
        for job_id, runtime in snapshot.get("job_runtime", {}).items():
            if job_id in self.runners:
                self.clear_stable_runner_recovery_state(job_id, runtime)
                continue
            if self.job_has_active_lease(job_id, services):
                continue
            resumable_after_restart: bool = (
                self.runtime_should_resume_incremental(
                    job_id,
                    runtime,
                )
            )
            if runtime.get("status") in {"starting", "running", "stopping"}:
                patch: dict = self.build_restart_runtime_patch(
                    runtime,
                    resumable_after_restart=resumable_after_restart,
                )
                self.store.update_job_runtime(job_id, patch)
                if resumable_after_restart:
                    resumable_job_ids.append(job_id)
                continue
            if runtime.get("status") == "failed":
                if not self.failed_runtime_should_auto_recover(
                    job_id,
                    runtime,
                ):
                    continue
                runtime = self.schedule_failed_runtime_recovery(
                    job_id,
                    runtime,
                )
                if not self.failed_runtime_recovery_is_due(runtime):
                    continue
                last_error: str = str(runtime.get("last_error") or "")
                patch = self.build_restart_runtime_patch(
                    runtime,
                    resumable_after_restart=True,
                )
                patch.update(
                    {
                        "last_error": last_error,
                        "runner_recovery_next_attempt_ts": None,
                        "runner_recovery_started_at": utcnow(),
                        "runner_recovery_started_ts": utc_timestamp(),
                    }
                )
                self.store.update_job_runtime(job_id, patch)
                resumable_job_ids.append(job_id)
                continue
            if runtime.get("status") == "stopped" and resumable_after_restart:
                if runtime.get("phase") != PHASE_AWAITING_INCREMENTAL_RESUME:
                    patch = self.build_restart_runtime_patch(
                        runtime,
                        resumable_after_restart=True,
                    )
                    self.store.update_job_runtime(job_id, patch)
                resumable_job_ids.append(job_id)
        return resumable_job_ids

    def bootstrap_payload(self) -> dict:
        payload: dict = self.store.snapshot()
        payload["connections"] = [
            mask_connection_credentials(connection)
            for connection in payload.get("connections", [])
        ]
        payload["jobs"] = [
            self.normalize_job_definition(job)
            for job in payload.get("jobs", [])
        ]
        self.attach_index_document_counts(payload)
        payload["running_jobs"] = sorted(self.runners.keys())
        payload["durable_outbox_cleanup"] = (
            self.durable_outbox_cleanup_status()
        )
        payload["durable_outbox_capture_reconciliation"] = (
            self.durable_outbox_capture_reconciliation_status()
        )
        payload["durable_outbox_partition_maintenance"] = (
            self.durable_outbox_partition_maintenance_status()
        )
        return payload

    def normalize_job_definition(self, job: dict) -> dict:
        """Normalize persisted job data for UI and runtime compatibility."""
        normalized: dict = deepcopy(job)
        normalized["enable_assembly_fallback"] = _as_bool(
            normalized.get("enable_assembly_fallback"),
            default=False,
        )
        normalized["enable_durable_change_events"] = _as_bool(
            normalized.get("enable_durable_change_events"),
            default=False,
        )
        for table in normalized.get("tables", []):
            table["soft_delete_column"] = normalize_soft_delete_column(
                table.get("soft_delete_column")
            )
        for relation in normalized.get("relations", []):
            relation["type"] = normalize_relation_type(relation.get("type"))
            relation["fixed_conditions"] = [
                (
                    {
                        "field": (condition.get("field") or "").strip(),
                        "operator": (
                            (condition.get("operator") or "eq").strip().lower()
                            or "eq"
                        ),
                        "value": condition.get("value"),
                        **(
                            {"source": self.fixed_condition_source(condition)}
                            if (
                                self.fixed_condition_source(condition)
                                != "child"
                                or "source" in condition
                            )
                            else {}
                        ),
                    }
                )
                for condition in relation.get("fixed_conditions", [])
                if (condition.get("field") or "").strip()
            ]
        return normalized

    def append_log(self, job_id: str, line: str) -> None:
        try:
            self.store.append_job_log(
                job_id,
                f"[{utcnow()}] {line}",
                limit=LOG_TAIL_LIMIT,
            )
        except Exception:
            logger.exception("写入任务运行日志失败: %s", job_id)

    def update_runtime(self, job_id: str, patch: dict) -> dict:
        return self.store.update_job_runtime(job_id, patch)

    def acquire_job_lease(
        self,
        job_id: str,
        services: dict,
    ) -> t.Union[RedisJobLease, NoopJobLease]:
        lease = RedisJobLease.from_services(
            services,
            job_id=job_id,
            owner_instance_id=self.instance_id,
        )
        if lease.acquire():
            return lease
        holder: dict = lease.describe_holder()
        owner: str = holder.get("owner_instance_id") or "unknown"
        raise StudioError(
            f"任务 {job_id} 当前已被实例 {owner} 持有，无法在当前 Studio 重复启动。"
        )

    def _lease_monitor_loop(
        self,
        job_id: str,
        runner: t.Any,
        lease: t.Union[RedisJobLease, NoopJobLease],
        stop_event: threading.Event,
    ) -> None:
        while not stop_event.wait(lease.renew_interval_seconds):
            try:
                renewed: bool = bool(lease.renew())
            except Exception as exc:
                logger.exception("Failed to renew job lease: %s", job_id)
                self.append_log(job_id, f"Redis job lease 续租失败：{exc}")
                renewed = False
            if not renewed:
                self.update_runtime(
                    job_id,
                    {
                        "status": "failed",
                        "phase": "lease_lost",
                        "heartbeat_at": utcnow(),
                        "last_error": (
                            "Redis job lease lost; the current Studio instance no longer owns this task."
                        ),
                    },
                )
                if hasattr(runner, "handle_lease_lost"):
                    runner.handle_lease_lost()
                else:
                    runner.stop()
                return
            self.update_runtime(
                job_id,
                {
                    "owner": self.instance_id,
                    "heartbeat_at": utcnow(),
                },
            )

    def start_job_lease_monitor(
        self,
        job_id: str,
        runner: t.Any,
        lease: t.Union[RedisJobLease, NoopJobLease],
    ) -> None:
        if isinstance(lease, NoopJobLease):
            return
        stop_event = threading.Event()
        thread = threading.Thread(
            target=self._lease_monitor_loop,
            args=(job_id, runner, lease, stop_event),
            daemon=True,
        )
        self.job_lease_threads[job_id] = {
            "stop_event": stop_event,
            "thread": thread,
        }
        thread.start()

    def release_job_lease(
        self,
        job_id: str,
        *,
        clear_owner: bool = True,
    ) -> None:
        state: t.Optional[dict[str, t.Any]] = self.job_lease_threads.pop(
            job_id, None
        )
        current_thread = threading.current_thread()
        if state:
            state["stop_event"].set()
            thread = state.get("thread")
            if thread and thread.is_alive() and thread is not current_thread:
                thread.join(timeout=1)
        lease = self.job_leases.pop(job_id, None)
        if lease:
            try:
                lease.release()
            except Exception:
                logger.exception("Failed to release job lease: %s", job_id)
        if clear_owner:
            job_exists: bool = self.store.get_job(job_id) is not None
            runtime_exists: bool = bool(self.store.get_job_runtime(job_id))
            if not job_exists and not runtime_exists:
                return
            self.update_runtime(
                job_id,
                {
                    "owner": None,
                    "heartbeat_at": None,
                },
            )

    def on_runner_finished(self, job_id: str) -> None:
        self.runners.pop(job_id, None)
        self.release_job_lease(job_id, clear_owner=True)
        self.schedule_failed_runtime_recovery(job_id)

    def validate_connection_payload(self, payload: dict) -> dict:
        connection: dict = {
            "id": payload.get("id") or make_id("conn"),
            "name": (payload.get("name") or "").strip(),
            "type": (payload.get("type") or "postgresql").strip().lower(),
            "host": (payload.get("host") or "").strip(),
            "port": int(payload.get("port") or 5432),
            "database": (payload.get("database") or "").strip(),
            "username": MASKED_SHARED_CREDENTIAL,
            "password": "",
            "url": "",
        }
        if connection["type"] != "postgresql":
            raise StudioError("当前版本仅支持 PostgreSQL 连接。")
        if not connection["url"]:
            required: list[str] = [
                key
                for key in ("name", "host", "database")
                if not connection[key]
            ]
            if required:
                labels: dict[str, str] = {
                    "name": "连接名称",
                    "host": "主机",
                    "database": "数据库",
                }
                raise StudioError(
                    "连接信息缺少必填字段："
                    + ", ".join(labels.get(item, item) for item in required)
                )
        return connection

    def test_connection(self, payload: dict) -> dict:
        connection: dict = self.validate_connection_payload(payload)
        return self.db.test_connection(connection)

    def save_connection(self, payload: dict) -> dict:
        connection: dict = self.validate_connection_payload(payload)
        self.db.test_connection(connection)
        return mask_connection_credentials(
            self.store.upsert_connection(
                mask_connection_credentials(connection)
            )
        )

    def delete_connection(self, connection_id: str) -> bool:
        for job in self.store.list_jobs():
            for table in job.get("tables", []):
                if table.get("connection_id") == connection_id:
                    raise StudioError(
                        f"连接 {connection_id} 仍被任务 {job['name']} 引用，无法删除。"
                    )
        return self.store.delete_connection(connection_id)

    def delete_job(self, job_id: str, delete_index: bool = False) -> dict:
        job: t.Optional[dict] = self.store.get_job(job_id)
        if not job:
            raise StudioError(f"任务 {job_id} 不存在。")

        job = deepcopy(job)
        self.stop_job(job_id)

        index_deleted: bool = False
        if delete_index:
            index_deleted = SearchAdapter(
                self.store.snapshot()["services"]
            ).delete_index(job.get("index_name") or "")

        if not self.store.delete_job(job_id):
            raise StudioError(f"任务 {job_id} 不存在。")

        return {
            "job_id": job_id,
            "job_name": job.get("name") or job_id,
            "index_name": job.get("index_name") or "",
            "delete_index": delete_index,
            "index_deleted": index_deleted,
        }

    def list_schemas(self, connection_id: str) -> list[str]:
        connection: dict = self.require_connection(connection_id)
        return self.db.list_schemas(connection)

    def list_tables(self, connection_id: str, schema: str) -> list[str]:
        connection: dict = self.require_connection(connection_id)
        return self.db.list_tables(connection, schema)

    def get_columns(self, connection_id: str, schema: str, table: str) -> dict:
        connection: dict = self.require_connection(connection_id)
        columns: list[dict] = self.db.get_table_columns(
            connection, schema, table
        )
        primary_key: list[str] = self.db.get_primary_key(
            connection, schema, table
        )
        cursor_column: t.Optional[str] = self.db.suggest_cursor_column(
            connection,
            schema,
            table,
        )
        soft_delete_column: t.Optional[str] = next(
            (
                column["name"]
                for column in columns
                if str(column.get("name") or "").strip().lower() == "deleted"
            ),
            None,
        )
        return {
            "columns": columns,
            "primary_key": primary_key,
            "cursor_column": cursor_column,
            "soft_delete_column": soft_delete_column,
        }

    def preview_job_sql(self, raw_job: dict) -> dict:
        preview_job: dict = deepcopy(raw_job)
        if not (preview_job.get("name") or "").strip():
            preview_job["name"] = "__preview__"
        if not (preview_job.get("index_name") or "").strip():
            preview_job["index_name"] = "__preview_index__"
        job: dict = self.prepare_job(preview_job)
        mode: str = self.determine_job_mode(job)
        if mode == "native":
            steps: list[dict] = self.build_native_capture_sql_preview(job)
        else:
            steps = self.build_assembly_sql_preview(job)
        return {
            "job": job,
            "mode": mode,
            "steps": steps,
        }

    def search_job_documents(self, job_id: str, payload: dict) -> dict:
        job: t.Optional[dict] = self.store.get_job(job_id)
        if not job:
            raise StudioError(f"任务 {job_id} 不存在。")
        runtime: dict = self.store.get_job_runtime(job_id)
        if runtime.get("status") == "failed":
            raise StudioError(
                runtime.get("last_error")
                or "当前任务状态为失败，请先处理失败原因。"
            )

        keyword: str = (payload.get("keyword") or "").strip()
        page: int = max(1, int(payload.get("page") or 1))
        size: int = max(1, min(100, int(payload.get("size") or 10)))
        result: dict = SearchAdapter(
            self.store.snapshot()["services"]
        ).search_documents(
            job["index_name"],
            keyword=keyword,
            page=page,
            size=size,
        )
        result["job"] = {
            "id": job["id"],
            "name": job["name"],
            "index_name": job["index_name"],
            "status": runtime.get("status") or "saved",
        }
        return result

    def _thread_stack_snapshot(
        self,
        thread: t.Optional[threading.Thread],
        *,
        limit: int = 12,
    ) -> list[dict[str, t.Any]]:
        if thread is None or thread.ident is None:
            return []
        frame = sys._current_frames().get(thread.ident)
        if frame is None:
            return []
        extracted = traceback.extract_stack(frame, limit=limit)
        return [
            {
                "path": item.filename,
                "line": item.lineno,
                "name": item.name,
                "code": item.line,
            }
            for item in extracted
        ]

    def _thread_debug_info(
        self,
        thread: t.Optional[threading.Thread],
        *,
        stop_event: t.Optional[threading.Event] = None,
    ) -> dict[str, t.Any]:
        return {
            "present": thread is not None,
            "name": getattr(thread, "name", None),
            "ident": getattr(thread, "ident", None),
            "daemon": getattr(thread, "daemon", None),
            "alive": bool(thread and thread.is_alive()),
            "stop_event_set": (
                bool(stop_event.is_set()) if stop_event is not None else None
            ),
            "stack": self._thread_stack_snapshot(thread),
        }

    def get_job_debug(self, job_id: str) -> dict:
        job: t.Optional[dict] = self.store.get_job(job_id)
        if not job:
            raise StudioError(f"job {job_id} does not exist.")
        runtime: dict = self.store.get_job_runtime(job_id)
        runner: t.Any = self.runners.get(job_id)
        payload: dict[str, t.Any] = {
            "job_id": job_id,
            "job_name": job.get("name"),
            "runtime": runtime,
            "has_runner": runner is not None,
        }
        if runner is None:
            return payload

        native_listener_specs: dict[str, dict[str, t.Any]] = (
            getattr(runner, "native_listener_specs", None) or {}
        )
        native_listener_threads: dict[str, threading.Thread] = (
            getattr(runner, "native_listener_threads", None) or {}
        )
        native_listener_stop_events: dict[str, threading.Event] = (
            getattr(runner, "native_listener_stop_events", None) or {}
        )
        listeners: dict[str, dict[str, t.Any]] = {}
        for connection_id, spec in native_listener_specs.items():
            connection: dict = spec.get("connection") or {}
            listeners[connection_id] = {
                **self._thread_debug_info(
                    native_listener_threads.get(connection_id),
                    stop_event=native_listener_stop_events.get(connection_id),
                ),
                "database": connection.get("database"),
                "host": connection.get("host"),
                "port": connection.get("port"),
                "watched_locations": [
                    list(location)
                    for location in sorted(spec.get("watched_locations") or [])
                ],
                "state": (
                    getattr(runner, "native_listener_states", {})
                    .get(connection_id, {})
                    .get("state")
                ),
                "reason": (
                    getattr(runner, "native_listener_states", {})
                    .get(connection_id, {})
                    .get("reason")
                ),
            }

        root_queue_snapshot: dict[str, t.Any]
        try:
            root_queue_snapshot = getattr(runner, "root_queue").snapshot(
                sample_size=5
            )
        except Exception as exc:
            root_queue_snapshot = {"error": str(exc)}

        payload["runner"] = {
            "type": runner.__class__.__name__,
            "thread": self._thread_debug_info(getattr(runner, "thread", None)),
            "stop_requested": getattr(runner, "stop_requested", None),
            "lease_lost": getattr(runner, "lease_lost", None),
            "native_activity_event_set": (
                bool(runner.native_activity_event.is_set())
                if hasattr(runner, "native_activity_event")
                else None
            ),
            "native_recovery_event_set": (
                bool(runner.native_recovery_event.is_set())
                if hasattr(runner, "native_recovery_event")
                else None
            ),
            "queue_lock_locked": (
                runner.queue_lock.locked()
                if hasattr(runner, "queue_lock")
                else None
            ),
            "incremental_run_lock_locked": (
                runner.incremental_run_lock.locked()
                if hasattr(runner, "incremental_run_lock")
                else None
            ),
            "recovery_scan_thread": self._thread_debug_info(
                getattr(runner, "recovery_scan_thread", None)
            ),
            "native_listener_threads": listeners,
            "root_queue_snapshot": root_queue_snapshot,
        }
        return payload

    def search_index_documents(self, payload: dict) -> dict:
        job_id: str = (payload.get("job_id") or "").strip()
        if not job_id:
            raise StudioError("璇峰厛閫夋嫨瑕佹煡璇㈢殑 ES 绱㈠紩浠诲姟銆?")

        job: t.Optional[dict] = self.store.get_job(job_id)
        if not job:
            raise StudioError(f"浠诲姟 {job_id} 涓嶅瓨鍦ㄣ€?")

        body: t.Any = payload.get("body")
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except json.JSONDecodeError as exc:
                raise StudioError(
                    f"ES 查询条件不是合法 JSON：{exc.msg}"
                ) from exc
        if body is None:
            body = {}
        if not isinstance(body, dict):
            raise StudioError("ES 查询条件必须是 JSON 对象。")

        result: dict = SearchAdapter(
            self.store.snapshot()["services"]
        ).search_documents_by_body(job["index_name"], body)
        runtime: dict = self.store.get_job_runtime(job_id)
        result["job"] = {
            "id": job["id"],
            "name": job["name"],
            "index_name": job["index_name"],
            "status": runtime.get("status") or "saved",
        }
        return result

    def add_index_alias(self, payload: dict) -> dict:
        index_name: str = (payload.get("index") or "").strip()
        alias_name: str = (payload.get("alias") or "").strip()
        return SearchAdapter(
            self.store.snapshot()["services"]
        ).add_index_alias(index_name, alias_name)

    def ordered_relations(self, job: dict) -> list[dict]:
        children_by_parent: dict[str, list[dict]] = {}
        for relation in job.get("relations", []):
            children_by_parent.setdefault(
                relation["parent_table_id"],
                [],
            ).append(relation)

        ordered: list[dict] = []

        def walk(parent_table_id: str) -> None:
            for relation in children_by_parent.get(parent_table_id, []):
                ordered.append(relation)
                walk(relation["child_table_id"])

        walk(job["root_table_id"])
        return ordered

    def sql_alias(self, table: dict) -> str:
        return table.get("alias") or table["table"]

    def sql_column_ref(self, alias: str, column: str) -> str:
        return f"{quote_ident(alias)}.{quote_ident(column)}"

    def sql_order_by(self, alias: str, columns: list[str]) -> str:
        return ", ".join(
            f"{self.sql_column_ref(alias, column)} ASC" for column in columns
        )

    def sql_join_condition(
        self,
        relation: dict,
        parent_alias: str,
        child_alias: str,
    ) -> str:
        clauses: list[str] = [
            (
                f"{self.sql_column_ref(parent_alias, pair['parent_field'])} = "
                f"{self.sql_column_ref(child_alias, pair['child_field'])}"
            )
            for pair in relation["field_pairs"]
        ]
        clauses.extend(
            self.sql_relation_fixed_condition_clause(
                parent_alias,
                child_alias,
                condition,
            )
            for condition in relation.get("fixed_conditions", [])
        )
        return " AND ".join(clauses)

    def sql_literal(self, value: t.Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return str(value)
        return "'" + str(value).replace("'", "''") + "'"

    def fixed_condition_source(self, condition: dict) -> str:
        source: str = str(condition.get("source") or "child").strip().lower()
        return source or "child"

    def relation_fixed_conditions(
        self,
        relation: dict,
        *,
        source: t.Optional[str] = None,
    ) -> list[dict]:
        conditions: list[dict] = []
        for condition in relation.get("fixed_conditions", []):
            normalized_source: str = self.fixed_condition_source(condition)
            if source is not None and normalized_source != source:
                continue
            normalized: dict[str, t.Any] = {
                "field": condition["field"],
                "operator": condition.get("operator", "eq"),
                "value": condition.get("value"),
            }
            if normalized_source != "child" or "source" in condition:
                normalized["source"] = normalized_source
            conditions.append(normalized)
        return conditions

    def sql_fixed_condition_clause_from_ref(
        self,
        column_ref: str,
        condition: dict,
    ) -> str:
        operator: str = (condition.get("operator") or "eq").lower()
        value: t.Any = condition.get("value")
        if operator == "eq":
            return f"{column_ref} = {self.sql_literal(value)}"
        if operator == "ne":
            return f"{column_ref} <> {self.sql_literal(value)}"
        if operator == "is_null":
            return f"{column_ref} IS NULL"
        if operator == "not_null":
            return f"{column_ref} IS NOT NULL"
        if operator == "in":
            values: list[t.Any] = (
                list(value)
                if isinstance(value, (list, tuple, set))
                else [value]
            )
            return (
                f"{column_ref} IN ("
                + ", ".join(self.sql_literal(item) for item in values)
                + ")"
            )
        raise StudioError(f"Unsupported fixed condition operator: {operator}")

    def sql_fixed_condition_clause(
        self,
        alias: str,
        condition: dict,
    ) -> str:
        return self.sql_fixed_condition_clause_from_ref(
            self.sql_column_ref(alias, condition["field"]),
            condition,
        )

    def sql_relation_fixed_condition_clause(
        self,
        parent_alias: str,
        child_alias: str,
        condition: dict,
        *,
        parent_as_parameter: bool = False,
    ) -> str:
        if self.fixed_condition_source(condition) == "parent":
            if parent_as_parameter:
                column_ref = f":{parent_alias}_{condition['field']}"
            else:
                column_ref = self.sql_column_ref(
                    parent_alias, condition["field"]
                )
        else:
            column_ref = self.sql_column_ref(child_alias, condition["field"])
        return self.sql_fixed_condition_clause_from_ref(column_ref, condition)

    def relation_fixed_eq_filters(
        self,
        relation: dict,
        *,
        source: str = "child",
    ) -> dict[str, t.Any]:
        return {
            condition["field"]: condition.get("value")
            for condition in self.relation_fixed_conditions(
                relation,
                source=source,
            )
            if condition.get("operator", "eq") == "eq"
        }

    def relation_row_matches_fixed_conditions(
        self,
        relation: dict,
        row: dict,
        *,
        source: str = "child",
    ) -> bool:
        for field, value in self.relation_fixed_eq_filters(
            relation,
            source=source,
        ).items():
            if row.get(field) != value:
                return False
        return True

    def build_native_sql_preview(self, job: dict) -> list[dict]:
        tables_by_id: dict[str, dict] = {
            table["id"]: table for table in job["tables"]
        }
        root: dict = tables_by_id[job["root_table_id"]]
        root_alias: str = self.sql_alias(root)
        root_connection: dict = self.require_connection(root["connection_id"])
        select_columns: list[str] = [f"{quote_ident(root_alias)}.*"]
        join_blocks: list[str] = []

        for relation in self.ordered_relations(job):
            parent: dict = tables_by_id[relation["parent_table_id"]]
            child: dict = tables_by_id[relation["child_table_id"]]
            parent_alias: str = self.sql_alias(parent)
            child_alias: str = self.sql_alias(child)
            select_columns.append(f"{quote_ident(child_alias)}.*")
            join_blocks.append(
                "\n".join(
                    [
                        (
                            "LEFT JOIN "
                            f"{qualified_table(child['schema'], child['table'])} "
                            f"AS {quote_ident(child_alias)}"
                        ),
                        (
                            "  ON "
                            + self.sql_join_condition(
                                relation,
                                parent_alias,
                                child_alias,
                            )
                        ),
                    ]
                )
            )

        sql_lines: list[str] = [
            "SELECT",
            "  " + ",\n  ".join(select_columns),
            (
                "FROM "
                f"{qualified_table(root['schema'], root['table'])} "
                f"AS {quote_ident(root_alias)}"
            ),
        ]
        if join_blocks:
            sql_lines.extend(join_blocks)
        sql_lines.append(
            "ORDER BY " + self.sql_order_by(root_alias, root["primary_key"])
        )
        return [
            {
                "title": "同源库处理 SQL",
                "kind": "left_join",
                "connection_name": root_connection["name"],
                "database": root_connection["database"],
                "sql": "\n".join(sql_lines) + ";",
            }
        ]

    def build_assembly_sql_preview(self, job: dict) -> list[dict]:
        tables_by_id: dict[str, dict] = {
            table["id"]: table for table in job["tables"]
        }
        root: dict = tables_by_id[job["root_table_id"]]
        root_alias: str = self.sql_alias(root)
        root_connection: dict = self.require_connection(root["connection_id"])
        steps: list[dict] = [
            {
                "title": f"步骤 1：查询根表 {root['alias']}",
                "kind": "root_query",
                "connection_name": root_connection["name"],
                "database": root_connection["database"],
                "sql": "\n".join(
                    [
                        "SELECT *",
                        (
                            "FROM "
                            f"{qualified_table(root['schema'], root['table'])} "
                            f"AS {quote_ident(root_alias)}"
                        ),
                        (
                            "ORDER BY "
                            + self.sql_order_by(
                                root_alias, root["primary_key"]
                            )
                        ),
                        "LIMIT :page_size OFFSET :offset;",
                    ]
                ),
            }
        ]

        for index, relation in enumerate(self.ordered_relations(job), start=2):
            parent: dict = tables_by_id[relation["parent_table_id"]]
            child: dict = tables_by_id[relation["child_table_id"]]
            parent_alias: str = self.sql_alias(parent)
            child_alias: str = self.sql_alias(child)
            child_connection: dict = self.require_connection(
                child["connection_id"]
            )
            where_clause: str = " AND ".join(
                (
                    f"{self.sql_column_ref(child_alias, pair['child_field'])} = "
                    f":{parent_alias}_{pair['parent_field']}"
                )
                for pair in relation["field_pairs"]
            )
            fixed_clause: str = " AND ".join(
                self.sql_relation_fixed_condition_clause(
                    parent_alias,
                    child_alias,
                    condition,
                    parent_as_parameter=True,
                )
                for condition in relation.get("fixed_conditions", [])
            )
            sql_lines: list[str] = [
                "SELECT *",
                (
                    "FROM "
                    f"{qualified_table(child['schema'], child['table'])} "
                    f"AS {quote_ident(child_alias)}"
                ),
                "WHERE "
                + " AND ".join(
                    [part for part in [where_clause, fixed_clause] if part]
                ),
            ]
            if child["primary_key"]:
                sql_lines.append(
                    "ORDER BY "
                    + self.sql_order_by(child_alias, child["primary_key"])
                )
            sql_lines[-1] = sql_lines[-1] + ";"
            steps.append(
                {
                    "title": (
                        f"步骤 {index}：查询子表 {child['alias']} "
                        f"（根据 {parent['alias']} 结果继续组装）"
                    ),
                    "kind": "followup_query",
                    "connection_name": child_connection["name"],
                    "database": child_connection["database"],
                    "same_connection": (
                        parent["connection_id"] == child["connection_id"]
                    ),
                    "sql": "\n".join(sql_lines),
                }
            )
        return steps

    def build_native_capture_sql_preview(self, job: dict) -> list[dict]:
        root_table: dict = next(
            table
            for table in job["tables"]
            if table["id"] == job["root_table_id"]
        )
        root_connection: dict = self.require_connection(
            root_table["connection_id"]
        )
        cross_database: bool = self.job_has_cross_database_tables(job)
        steps: list[dict] = [
            {
                "title": "步骤 1：Native 变更捕获准备说明",
                "kind": "native_setup_checkpoint",
                "connection_name": root_connection["name"],
                "database": root_connection["database"],
                "sql": (
                    "-- 启动同步前，会先在涉及的 PostgreSQL 库中准备 Native 监听所需的 function / view / trigger。\n"
                    "-- 这一步只负责建立变更捕获能力，不直接向 Elasticsearch 写入全量数据。\n"
                    + (
                        "-- 当前任务包含跨库表，监听器会分别覆盖各个参与同步的数据库。"
                        if cross_database
                        else "-- 当前任务为单库场景，监听器会覆盖当前库内参与同步的所有表。"
                    )
                ),
            }
        ]

        assembly_steps: list[dict] = self.build_assembly_sql_preview(job)
        for index, step in enumerate(assembly_steps, start=2):
            next_step: dict = deepcopy(step)
            if index == 2:
                next_step["title"] = "步骤 2：Assembly 全量同步根表查询"
            else:
                next_step["title"] = f"步骤 {index}：Assembly 关联组装查询"
            next_step["kind"] = (
                "cross_database_assembly_full_sync"
                if cross_database
                else "assembly_full_sync_query"
            )
            steps.append(next_step)

        durable_enabled: bool = bool(job.get("enable_durable_change_events"))
        incremental_note: str = (
            "-- 存量阶段由 Assembly full_sync 负责构建完整文档并写入 Elasticsearch。\n"
            "-- 增量阶段由 Native listener 监听数据库变更并解析受影响的 root key。\n"
            "-- 最终仍由 Assembly 按 root key 回源重建整篇文档后写入 Elasticsearch。"
        )
        if durable_enabled:
            incremental_note += "\n-- 已开启 durable outbox：触发器会在同一事务写入 _pgsync_change_event，PG_NOTIFY 只作为低延迟唤醒信号。"
        else:
            incremental_note += "\n-- 当前未开启 durable outbox：PG_NOTIFY 仍作为实时事件来源。"
        if job.get("enable_assembly_fallback"):
            incremental_note += "\n-- 已开启周期性 Recovery Scan 兜底；系统会按轮询间隔执行恢复扫描与补偿重建。"
        else:
            incremental_note += "\n-- 当前未开启周期性 Recovery Scan；系统仍会在重连或异常时触发事件驱动恢复扫描。"

        steps.append(
            {
                "title": f"步骤 {len(steps) + 1}：增量同步说明",
                "kind": (
                    "cross_database_incremental_note"
                    if cross_database
                    else "native_incremental_note"
                ),
                "connection_name": "",
                "database": "",
                "sql": incremental_note,
            }
        )
        return steps

    def build_cross_database_native_sql_preview(self, job: dict) -> list[dict]:
        return self.build_native_capture_sql_preview(job)

    def require_connection(self, connection_id: str) -> dict:
        connection: t.Optional[dict] = self.store.get_connection(connection_id)
        if not connection:
            raise StudioError(f"连接 {connection_id} 不存在。")
        return connection

    def normalize_services(self, payload: dict) -> dict:
        return {
            "search": {
                "enabled": True,
                "url": (payload.get("search", {}).get("url") or "").strip(),
                "username": payload.get("search", {}).get("username") or "",
                "password": payload.get("search", {}).get("password") or "",
                "verify_certs": bool(
                    payload.get("search", {}).get("verify_certs", False)
                ),
            },
            "redis": {
                "enabled": True,
                "host": (payload.get("redis", {}).get("host") or "").strip(),
                "port": int(payload.get("redis", {}).get("port") or 6379),
                "password": payload.get("redis", {}).get("password") or "",
                "db": int(payload.get("redis", {}).get("db") or 0),
            },
        }

    def save_services(self, payload: dict) -> dict:
        services: dict = self.normalize_services(payload)
        return self.store.save_services(services)

    def prepare_job(self, raw_job: dict) -> dict:
        job: dict = deepcopy(raw_job)
        job["id"] = job.get("id") or make_id("job")
        job["name"] = (job.get("name") or "").strip()
        job["index_name"] = (job.get("index_name") or "").strip()
        job["enable_assembly_fallback"] = _as_bool(
            job.get("enable_assembly_fallback"),
            default=False,
        )
        job["enable_durable_change_events"] = _as_bool(
            job.get("enable_durable_change_events"),
            default=False,
        )
        job["poll_interval_seconds"] = int(
            job.get("poll_interval_seconds") or DEFAULT_POLL_INTERVAL_SECONDS
        )
        job["page_size"] = int(job.get("page_size") or 200)
        job["incremental_batch_size"] = int(
            job.get("incremental_batch_size") or 100
        )
        job["full_resync_interval_seconds"] = int(
            job.get("full_resync_interval_seconds") or 1800
        )
        if job.get("assembly_rebuild_batch_size") not in (None, ""):
            job["assembly_rebuild_batch_size"] = int(
                job.get("assembly_rebuild_batch_size")
            )
        if job.get("cross_db_recovery_scan_batch_size") not in (None, ""):
            job["cross_db_recovery_scan_batch_size"] = int(
                job.get("cross_db_recovery_scan_batch_size")
            )
        if job.get("cross_db_recovery_scan_interval_seconds") not in (
            None,
            "",
        ):
            job["cross_db_recovery_scan_interval_seconds"] = int(
                job.get("cross_db_recovery_scan_interval_seconds")
            )
        if job.get("assembly_hot_batch_ratio") not in (None, ""):
            job["assembly_hot_batch_ratio"] = float(
                job.get("assembly_hot_batch_ratio")
            )
        if job.get("assembly_hot_min_batch_size") not in (None, ""):
            job["assembly_hot_min_batch_size"] = int(
                job.get("assembly_hot_min_batch_size")
            )
        if job.get("assembly_backlog_min_batch_size") not in (None, ""):
            job["assembly_backlog_min_batch_size"] = int(
                job.get("assembly_backlog_min_batch_size")
            )
        if job.get("durable_change_event_batch_size") not in (None, ""):
            job["durable_change_event_batch_size"] = int(
                job.get("durable_change_event_batch_size")
            )
        if job.get("durable_change_event_replay_window") not in (None, ""):
            job["durable_change_event_replay_window"] = int(
                job.get("durable_change_event_replay_window")
            )
        if job.get("durable_change_event_processed_window") not in (None, ""):
            job["durable_change_event_processed_window"] = int(
                job.get("durable_change_event_processed_window")
            )

        if not job["name"]:
            raise StudioError("任务名称不能为空。")
        if not job["index_name"]:
            raise StudioError("Elasticsearch 索引名称不能为空。")
        if not job.get("tables"):
            raise StudioError("至少需要配置一张表。")

        tables: list[dict] = []
        seen_ids: set[str] = set()
        for raw_table in job["tables"]:
            table: dict = {
                "id": raw_table.get("id") or make_id("table"),
                "alias": (
                    raw_table.get("alias") or raw_table.get("table") or ""
                ).strip(),
                "connection_id": raw_table.get("connection_id"),
                "schema": (raw_table.get("schema") or "public").strip(),
                "table": (raw_table.get("table") or "").strip(),
                "primary_key": list(raw_table.get("primary_key") or []),
                "cursor_column": (
                    raw_table.get("cursor_column") or ""
                ).strip(),
                "soft_delete_column": normalize_soft_delete_column(
                    raw_table.get("soft_delete_column")
                ),
            }
            if not table["connection_id"] or not table["table"]:
                raise StudioError("每张表都必须配置连接和表名。")
            if table["id"] in seen_ids:
                raise StudioError("表 ID 不能重复。")
            seen_ids.add(table["id"])
            connection: dict = self.require_connection(table["connection_id"])
            if not table["primary_key"]:
                table["primary_key"] = self.db.get_primary_key(
                    connection,
                    table["schema"],
                    table["table"],
                )
            if not table["primary_key"]:
                raise StudioError(
                    f"{table['schema']}.{table['table']} 没有主键，不能用于同步。"
                )
            if not table["cursor_column"]:
                suggested: t.Optional[str] = self.db.suggest_cursor_column(
                    connection,
                    table["schema"],
                    table["table"],
                )
                if suggested:
                    table["cursor_column"] = suggested
            if table["soft_delete_column"]:
                available_columns: set[str] = {
                    str(column.get("name") or "")
                    for column in self.db.get_table_columns(
                        connection,
                        table["schema"],
                        table["table"],
                    )
                }
                if table["soft_delete_column"] not in available_columns:
                    raise StudioError(
                        f"{table['schema']}.{table['table']} 不存在软删除字段 "
                        f"{table['soft_delete_column']}。"
                    )
            tables.append(table)

        job["tables"] = tables
        job["root_table_id"] = job.get("root_table_id") or tables[0]["id"]
        job["enable_assembly_fallback"] = _as_bool(
            job.get("enable_assembly_fallback"),
            default=False,
        )
        job["enable_durable_change_events"] = _as_bool(
            job.get("enable_durable_change_events"),
            default=False,
        )
        table_ids: set[str] = {table["id"] for table in tables}
        if job["root_table_id"] not in table_ids:
            raise StudioError("根表配置无效。")

        relations: list[dict] = []
        child_ids: set[str] = set()
        for raw_relation in job.get("relations", []):
            relation: dict = {
                "id": raw_relation.get("id") or make_id("rel"),
                "parent_table_id": raw_relation.get("parent_table_id"),
                "child_table_id": raw_relation.get("child_table_id"),
                "type": normalize_relation_type(raw_relation.get("type")),
                "field_pairs": [
                    {
                        "parent_field": (
                            pair.get("parent_field") or ""
                        ).strip(),
                        "child_field": (pair.get("child_field") or "").strip(),
                    }
                    for pair in raw_relation.get("field_pairs", [])
                ],
                "fixed_conditions": [
                    (
                        {
                            "field": (condition.get("field") or "").strip(),
                            "operator": (
                                (condition.get("operator") or "eq")
                                .strip()
                                .lower()
                                or "eq"
                            ),
                            "value": condition.get("value"),
                            **(
                                {
                                    "source": (
                                        (condition.get("source") or "child")
                                        .strip()
                                        .lower()
                                        or "child"
                                    )
                                }
                                if (
                                    (
                                        (condition.get("source") or "child")
                                        .strip()
                                        .lower()
                                        or "child"
                                    )
                                    != "child"
                                    or "source" in condition
                                )
                                else {}
                            ),
                        }
                    )
                    for condition in raw_relation.get("fixed_conditions", [])
                    if (condition.get("field") or "").strip()
                ],
            }
            if relation["parent_table_id"] not in table_ids:
                raise StudioError("关联关系中的父表配置无效。")
            if relation["child_table_id"] not in table_ids:
                raise StudioError("关联关系中的子表配置无效。")
            if relation["child_table_id"] == job["root_table_id"]:
                raise StudioError("根表不能被配置成子表。")
            if relation["child_table_id"] in child_ids:
                raise StudioError("每张子表只能归属于一个父表。")
            if relation["type"] not in SUPPORTED_RELATION_TYPES:
                raise StudioError(
                    "关系类型只能是 one_to_one、one_to_many 或 many_to_one。"
                )
            if not relation["field_pairs"]:
                raise StudioError("每条关联关系至少需要配置一个字段映射。")
            for pair in relation["field_pairs"]:
                if not pair["parent_field"] or not pair["child_field"]:
                    raise StudioError("关联关系中的字段映射不能为空。")
            for condition in relation["fixed_conditions"]:
                if condition["operator"] not in {"eq"}:
                    raise StudioError(
                        f"固定条件暂不支持操作符 {condition['operator']}"
                    )
                if self.fixed_condition_source(condition) not in {
                    "child",
                    "parent",
                }:
                    raise StudioError(
                        "固定条件字段来源只能是 child 或 parent。"
                    )
            relations.append(relation)
            child_ids.add(relation["child_table_id"])

        for table in tables:
            if table["id"] == job["root_table_id"]:
                continue
            if table["id"] not in child_ids:
                raise StudioError(
                    f"非根表 {table['alias']} 必须配置父级关联关系。"
                )
        job["relations"] = relations
        return job

    def validate_job(self, raw_job: dict) -> dict:
        job: dict = self.prepare_job(raw_job)
        relations_result: list[dict] = []
        for relation in job["relations"]:
            relations_result.append(self.validate_relation(job, relation))
        return {
            "ok": True,
            "job": job,
            "mode": self.determine_job_mode(job),
            "relations": relations_result,
        }

    def validate_relation(self, job: dict, relation: dict) -> dict:
        tables_by_id: dict[str, dict] = {
            table["id"]: table for table in job["tables"]
        }
        parent: dict = tables_by_id[relation["parent_table_id"]]
        child: dict = tables_by_id[relation["child_table_id"]]
        parent_conn: dict = self.require_connection(parent["connection_id"])
        child_conn: dict = self.require_connection(child["connection_id"])
        sample_size: int = 10
        relation_type: str = normalize_relation_type(relation["type"])
        child_fixed_filters: dict[str, t.Any] = self.relation_fixed_eq_filters(
            relation,
            source="child",
        )
        parent_fixed_filters: dict[str, t.Any] = (
            self.relation_fixed_eq_filters(
                relation,
                source="parent",
            )
        )

        if relation_type == "many_to_one":
            parent_samples = self.db.sample_rows(
                parent_conn,
                parent["schema"],
                parent["table"],
                columns=[
                    pair["parent_field"] for pair in relation["field_pairs"]
                ],
                limit=sample_size,
                not_null_columns=[
                    pair["parent_field"] for pair in relation["field_pairs"]
                ],
                filters=parent_fixed_filters,
            )
            for sample in parent_samples:
                filters = {
                    pair["child_field"]: sample.get(pair["parent_field"])
                    for pair in relation["field_pairs"]
                }
                filters.update(child_fixed_filters)
                count = self.db.count_rows_by_fields(
                    child_conn,
                    child["schema"],
                    child["table"],
                    filters,
                )
                if count > 1:
                    raise StudioError(
                        "多对一关系校验失败："
                        f"父表抽样 {sample} 匹配到了 {count} 条子表数据。"
                    )
            return {
                "relation_id": relation["id"],
                "type": relation_type,
                "checked": True,
                "message": "多对一抽样校验通过。",
            }

        if False:
            return {
                "relation_id": relation["id"],
                "type": relation_type,
                "checked": False,
                "message": "多对多关系暂不做 SQL 抽样校验，保存后直接生效。",
            }

        if relation_type == "one_to_one":
            parent_samples: list[dict] = self.db.sample_rows(
                parent_conn,
                parent["schema"],
                parent["table"],
                columns=[
                    pair["parent_field"] for pair in relation["field_pairs"]
                ],
                limit=sample_size,
                not_null_columns=[
                    pair["parent_field"] for pair in relation["field_pairs"]
                ],
                filters=parent_fixed_filters,
            )
            for sample in parent_samples:
                filters: dict = {
                    pair["child_field"]: sample.get(pair["parent_field"])
                    for pair in relation["field_pairs"]
                }
                filters.update(child_fixed_filters)
                count: int = self.db.count_rows_by_fields(
                    child_conn,
                    child["schema"],
                    child["table"],
                    filters,
                )
                if count > 1:
                    raise StudioError(
                        "一对一关系校验失败："
                        f"父表抽样 {sample} 匹配到了 {count} 条子表数据。"
                    )

            child_samples: list[dict] = self.db.sample_rows(
                child_conn,
                child["schema"],
                child["table"],
                columns=[
                    pair["child_field"] for pair in relation["field_pairs"]
                ],
                limit=sample_size,
                not_null_columns=[
                    pair["child_field"] for pair in relation["field_pairs"]
                ],
                filters=child_fixed_filters,
            )
            for sample in child_samples:
                filters = {
                    pair["parent_field"]: sample.get(pair["child_field"])
                    for pair in relation["field_pairs"]
                }
                filters.update(parent_fixed_filters)
                count = self.db.count_rows_by_fields(
                    parent_conn,
                    parent["schema"],
                    parent["table"],
                    filters,
                )
                if count > 1:
                    raise StudioError(
                        "一对一关系校验失败："
                        f"子表抽样 {sample} 匹配到了 {count} 条父表数据。"
                    )
            return {
                "relation_id": relation["id"],
                "type": relation["type"],
                "checked": True,
                "message": "一对一抽样校验通过。",
            }

        child_samples = self.db.sample_rows(
            child_conn,
            child["schema"],
            child["table"],
            columns=[pair["child_field"] for pair in relation["field_pairs"]],
            limit=sample_size,
            not_null_columns=[
                pair["child_field"] for pair in relation["field_pairs"]
            ],
            filters=child_fixed_filters,
        )
        for sample in child_samples:
            filters = {
                pair["parent_field"]: sample.get(pair["child_field"])
                for pair in relation["field_pairs"]
            }
            filters.update(parent_fixed_filters)
            count = self.db.count_rows_by_fields(
                parent_conn,
                parent["schema"],
                parent["table"],
                filters,
            )
            if count > 1:
                raise StudioError(
                    "一对多关系校验失败："
                    f"子表抽样 {sample} 匹配到了 {count} 条父表数据。"
                )
        return {
            "relation_id": relation["id"],
            "type": relation_type,
            "checked": True,
            "message": "一对多反向抽样校验通过。",
        }

    def determine_job_mode(self, job: dict) -> str:
        if self.job_supports_native_mode(job):
            return "native"
        return "assembly"

    def job_connection_ids(self, job: dict) -> set[str]:
        connection_ids: set[str] = {
            table["connection_id"] for table in job["tables"]
        }
        return connection_ids

    def job_has_cross_database_tables(self, job: dict) -> bool:
        return len(self.job_connection_ids(job)) > 1

    def expected_resume_strategy(
        self,
        job: dict,
        mode: t.Optional[str] = None,
    ) -> str:
        resolved_mode: str = mode or self.determine_job_mode(job)
        if resolved_mode == "native" and self.job_has_cross_database_tables(
            job
        ):
            return RESUME_STRATEGY_CROSS_DATABASE_INCREMENTAL
        if resolved_mode == "native":
            return RESUME_STRATEGY_NATIVE_INCREMENTAL
        return RESUME_STRATEGY_ASSEMBLY_INCREMENTAL

    def runtime_can_resume_incremental(
        self,
        runtime: dict,
        expected_resume_strategy: str,
    ) -> bool:
        if not runtime.get("baseline_completed"):
            return False
        actual_strategy: str = str(
            runtime.get("resume_strategy") or ""
        ).strip()
        return (
            not actual_strategy or actual_strategy == expected_resume_strategy
        )

    def job_supports_native_mode(self, job: dict) -> bool:
        relation_types: set[str] = {
            normalize_relation_type(relation["type"])
            for relation in job["relations"]
        }
        if not relation_types.issubset(SUPPORTED_NATIVE_RELATIONS):
            return False
        return True

    def resumable_job_signature(self, job: dict) -> dict[str, t.Any]:
        """Return the job fields that must stay stable for resume safety."""
        return {
            "index_name": str(job.get("index_name") or "").strip(),
            "root_table_id": job.get("root_table_id"),
            "tables": [
                {
                    "id": table.get("id"),
                    "alias": table.get("alias"),
                    "connection_id": table.get("connection_id"),
                    "schema": table.get("schema"),
                    "table": table.get("table"),
                    "primary_key": list(table.get("primary_key") or []),
                    "cursor_column": table.get("cursor_column"),
                    "soft_delete_column": normalize_soft_delete_column(
                        table.get("soft_delete_column")
                    ),
                }
                for table in job.get("tables", [])
            ],
            "relations": [
                {
                    "id": relation.get("id"),
                    "parent_table_id": relation.get("parent_table_id"),
                    "child_table_id": relation.get("child_table_id"),
                    "type": normalize_relation_type(relation.get("type")),
                    "field_pairs": [
                        {
                            "parent_field": pair.get("parent_field"),
                            "child_field": pair.get("child_field"),
                        }
                        for pair in relation.get("field_pairs", [])
                    ],
                    "fixed_conditions": [
                        {
                            "field": condition.get("field"),
                            "operator": condition.get("operator"),
                            "value": condition.get("value"),
                            "source": self.fixed_condition_source(condition),
                        }
                        for condition in relation.get("fixed_conditions", [])
                    ],
                }
                for relation in job.get("relations", [])
            ],
        }

    def job_requires_fresh_baseline(
        self,
        existing_job: t.Optional[dict],
        new_job: dict,
    ) -> bool:
        """Return whether the edited job must rebuild from scratch."""
        if not existing_job:
            return True
        return self.resumable_job_signature(existing_job) != (
            self.resumable_job_signature(new_job)
        )

    def fresh_baseline_runtime_patch(self) -> dict[str, t.Any]:
        """Return the runtime fields that must be cleared before a rebuild."""
        return {
            "baseline_completed": False,
            "resume_strategy": "",
            "cursors": {},
            "document_count": 0,
            "last_full_sync_at": None,
            "last_full_sync_ts": None,
            "last_incremental_sync_at": None,
            "last_incremental_sync_ts": None,
            "last_native_event_at": None,
            "last_native_event_ts": None,
            "last_native_event_enqueued_count": 0,
            "last_native_event_source": "",
            "last_native_event_root_key_count": 0,
            "last_native_event_root_keys": [],
            "last_native_events_by_source": {},
            "last_recovery_requested_at": None,
            "last_recovery_started_at": None,
            "last_recovery_finished_at": None,
            "last_recovery_reason": "",
            "native_incremental_start_checkpoint": None,
            "native_setup_at": None,
            "native_started_at": None,
            "native_bootstrap_at": None,
            "native_change_event_cursors": {},
            "native_change_event_processed_ids": {},
            "durable_change_events_enabled": False,
            "durable_last_drain_at": None,
            "durable_last_drain_event_count": 0,
            "durable_last_drain_root_key_count": 0,
            "durable_last_drain_enqueued_count": 0,
            "durable_last_event_id": 0,
            "durable_event_lag_by_group": {},
            "durable_event_lag": 0,
            "durable_event_lag_exact_by_group": {},
            "durable_event_lag_exact": False,
            "durable_event_backlog_present_by_group": {},
            "durable_event_backlog_present": False,
            "durable_event_lag_observed_at": None,
            "listener_states": {},
            "listener_total_count": 0,
            "listener_healthy_count": 0,
            "listener_recovering_count": 0,
            "listener_degraded_count": 0,
            "health_status": HEALTH_STATUS_RUNNING,
            "health_reason": "",
            "assembly_last_batch_confirmed_at": None,
            "assembly_last_batch_deleted_count": 0,
            "assembly_last_batch_updated_count": 0,
            "assembly_last_batch_root_key_count": 0,
            "assembly_last_batch_raw_root_key_count": 0,
            "assembly_last_batch_deduped_root_key_count": 0,
            "assembly_last_batch_worker_count": 0,
            "assembly_last_batch_elapsed_ms": 0,
            "assembly_last_batch_chunk_elapsed_ms": [],
            "assembly_last_batch_configured_size": 0,
            "assembly_last_batch_size_source": "",
            "assembly_last_batch_root_keys": [],
            "assembly_last_recovery_enqueue_count": 0,
            "assembly_last_recovery_scan_at": None,
            "assembly_last_recovery_scan_root_count": 0,
            "assembly_last_recovery_scan_row_count": 0,
            "assembly_last_recovery_scan_ts": None,
            "assembly_pending_batch_at": None,
            "assembly_pending_batch_count": 0,
            "assembly_root_queue": [],
            "assembly_root_queue_size": 0,
            "assembly_hot_queue_size": 0,
            "assembly_backlog_queue_size": 0,
            "assembly_recovered_inflight_count": 0,
            "assembly_recovery_requires_full_sync": False,
            "runner_recovery_attempt_count": 0,
            "runner_recovery_next_attempt_ts": None,
            "runner_recovery_retry_delay_seconds": 0,
            "runner_recovery_last_failure_at": None,
            "runner_recovery_last_error": "",
            "runner_recovery_started_at": None,
            "runner_recovery_started_ts": None,
            "runner_recovery_succeeded_at": None,
        }

    def search_index_exists(self, index_name: str) -> t.Optional[bool]:
        """Return whether an Elasticsearch index exists, if searchable."""
        normalized_index_name: str = str(index_name or "").strip()
        if not normalized_index_name:
            return False
        services: dict = self.store.snapshot().get("services", {})
        if not (services.get("search", {}) or {}).get("url"):
            return None
        try:
            return SearchAdapter(services).index_exists(normalized_index_name)
        except Exception as exc:
            logger.warning(
                "Could not resolve Elasticsearch index existence for %s: %s",
                normalized_index_name,
                exc,
            )
            return None

    def job_index_is_materialized(
        self,
        job: dict,
        runtime: t.Optional[dict] = None,
    ) -> bool:
        """Return whether the job's current index has already been created."""
        index_exists: t.Optional[bool] = self.search_index_exists(
            job.get("index_name")
        )
        if index_exists is not None:
            return bool(index_exists)
        runtime = runtime or {}
        return bool(runtime.get("job_locked") or runtime.get("index_exists"))

    def assert_job_name_and_index_available(
        self,
        *,
        name: str,
        index_name: str,
        exclude_job_id: t.Optional[str] = None,
    ) -> None:
        """Reject duplicate task names and index names."""
        normalized_name: str = str(name or "").strip()
        normalized_index_name: str = str(index_name or "").strip()
        for existing_job in self.store.list_jobs():
            if exclude_job_id and existing_job.get("id") == exclude_job_id:
                continue
            if str(existing_job.get("name") or "").strip() == normalized_name:
                raise StudioError(
                    f"任务名称 {normalized_name} 已存在，请更换后重试。"
                )
            if (
                str(existing_job.get("index_name") or "").strip()
                == normalized_index_name
            ):
                existing_label: str = (
                    str(existing_job.get("name") or "").strip()
                    or str(existing_job.get("id") or "").strip()
                    or "unknown"
                )
                raise StudioError(
                    f"索引名称 {normalized_index_name} 已被任务 "
                    f"{existing_label} 使用，请更换后重试。"
                )

        existing_job: t.Optional[dict] = None
        if exclude_job_id:
            existing_job = self.store.get_job(exclude_job_id)
        current_job_owns_index: bool = bool(
            existing_job
            and str(existing_job.get("index_name") or "").strip()
            == normalized_index_name
        )
        if current_job_owns_index:
            return

        if self.search_index_exists(normalized_index_name):
            raise StudioError(
                f"索引名称 {normalized_index_name} 已存在于 Elasticsearch 中，"
                "请使用新的索引名称。"
            )

    def save_job(self, raw_job: dict, start: bool = True) -> dict:
        validation: dict = self.validate_job(raw_job)
        job: dict = validation["job"]
        existing_job: t.Optional[dict] = self.store.get_job(job["id"])
        existing_runtime: dict = self.store.get_job_runtime(job["id"])
        if existing_job and self.job_index_is_materialized(
            existing_job,
            existing_runtime,
        ):
            raise StudioError(
                "当前任务索引已生成，任务已锁定，不允许再修改。"
                "请使用复制功能创建新任务和新索引。"
            )
        self.assert_job_name_and_index_available(
            name=job["name"],
            index_name=job["index_name"],
            exclude_job_id=job["id"] if existing_job else None,
        )
        resume_strategy: str = self.expected_resume_strategy(
            job,
            validation["mode"],
        )
        requires_fresh_baseline: bool = self.job_requires_fresh_baseline(
            existing_job,
            job,
        )
        baseline_completed: bool = bool(
            not requires_fresh_baseline
            and existing_runtime.get("baseline_completed")
            and self.runtime_can_resume_incremental(
                existing_runtime,
                resume_strategy,
            )
        )
        runtime_patch: dict[str, t.Any] = {
            **(
                self.fresh_baseline_runtime_patch()
                if requires_fresh_baseline
                else {}
            ),
            "status": "saved",
            "phase": "saved",
            "desired_state": "running" if start else "stopped",
            "owner": None,
            "heartbeat_at": None,
            "mode": validation["mode"],
            "baseline_completed": baseline_completed,
            "resume_strategy": resume_strategy,
            "cross_database": self.job_has_cross_database_tables(job),
            "assembly_fallback_enabled": bool(
                job.get("enable_assembly_fallback")
            ),
            "health_status": HEALTH_STATUS_RUNNING,
            "health_reason": "",
            "validated_at": utcnow(),
            "validation": validation["relations"],
        }
        self.store.upsert_job(job)
        self.update_runtime(job["id"], runtime_patch)
        if start:
            self.start_job(job["id"])
        return {
            "job": job,
            "validation": validation,
            "runtime": self.store.get_job_runtime(job["id"]),
        }

    def copy_job(self, source_job_id: str, payload: dict) -> dict:
        """Create a new saved job from an existing task definition."""
        source_job: t.Optional[dict] = self.store.get_job(source_job_id)
        if not source_job:
            raise StudioError(f"任务 {source_job_id} 不存在。")

        copied_job: dict = deepcopy(source_job)
        copied_job.pop("id", None)
        copied_job["name"] = (
            payload.get("name") or source_job.get("name") or ""
        )
        copied_job["index_name"] = (
            payload.get("index_name") or source_job.get("index_name") or ""
        )
        result: dict = self.save_job(copied_job, start=False)
        return {
            "source_job_id": source_job_id,
            **result,
        }

    def start_job(self, job_id: str) -> dict:
        with self._lock:
            if job_id in self.runners:
                raise StudioError(f"任务 {job_id} 已在运行中。")
            job: t.Optional[dict] = self.store.get_job(job_id)
            if not job:
                raise StudioError(f"任务 {job_id} 不存在。")
            job = self.normalize_job_definition(job)
            mode: str = self.determine_job_mode(job)
            runtime: dict = self.store.get_job_runtime(job_id)
            resume_strategy: str = self.expected_resume_strategy(job, mode)
            resume_incremental_only: bool = (
                self.runtime_can_resume_incremental(
                    runtime,
                    resume_strategy,
                )
            )
            services: dict = self.store.snapshot()["services"]
            if not services["search"].get("url"):
                raise StudioError("启动同步任务前请先配置 Elasticsearch。")
            lease = self.acquire_job_lease(job_id, services)
            try:
                starting_patch: dict = {
                    "status": "starting",
                    "phase": "queued",
                    "desired_state": "running",
                    "owner": self.instance_id,
                    "heartbeat_at": utcnow(),
                    "resume_strategy": resume_strategy,
                }
                if runtime.get("runner_recovery_attempt_count"):
                    starting_patch.update(
                        {
                            "runner_recovery_next_attempt_ts": None,
                            "runner_recovery_started_at": utcnow(),
                            "runner_recovery_started_ts": utc_timestamp(),
                        }
                    )
                self.update_runtime(
                    job_id,
                    starting_patch,
                )
                if mode == "native":
                    if not services["redis"].get("host"):
                        raise StudioError(
                            "Native change-capture tasks require Redis to persist root-key queues."
                        )
                    runner: t.Union[
                        NativeProcessRunner,
                        AssemblyRunner,
                        CrossDatabaseHybridRunner,
                    ] = CrossDatabaseHybridRunner(
                        self,
                        job,
                        resume_incremental_only=resume_incremental_only,
                    )
                else:
                    runner = AssemblyRunner(
                        self,
                        job,
                        incremental_only=resume_incremental_only,
                        resume_strategy=resume_strategy,
                    )
                self.runners[job_id] = runner
                self.job_leases[job_id] = lease
                self.start_job_lease_monitor(job_id, runner, lease)
                runner.start()
                return self.store.get_job_runtime(job_id)
            except Exception:
                self.release_job_lease(job_id, clear_owner=True)
                raise

    def stop_job(self, job_id: str) -> dict:
        with self._lock:
            runner = self.runners.get(job_id)
            runtime: dict = self.store.get_job_runtime(job_id)
            owner: str = runtime.get("owner") or ""
            holder: dict = self.describe_job_holder(job_id)
            holder_owner: str = holder.get("owner_instance_id") or ""
            if (
                not runner
                and owner
                and owner != self.instance_id
                and holder_owner
                and holder_owner != self.instance_id
            ):
                raise StudioError(
                    f"任务 {job_id} 当前由实例 {owner} 持有，当前 Studio 无法直接停止远端任务。"
                )
            if not runner:
                runtime.setdefault("status", "stopped")
                runtime["desired_state"] = "stopped"
                runtime["phase"] = "stopped"
                runtime["owner"] = None
                runtime["heartbeat_at"] = None
                self.update_runtime(job_id, runtime)
                self.release_job_lease(job_id, clear_owner=True)
                return self.store.get_job_runtime(job_id)
            self.update_runtime(
                job_id,
                {
                    "desired_state": "stopped",
                    "phase": "stopping",
                },
            )
            runner.stop()
            return self.store.get_job_runtime(job_id)

    def build_native_baseline_job(self, job: dict) -> dict:
        tables_by_id: dict[str, dict] = {
            table["id"]: deepcopy(table) for table in job["tables"]
        }
        children_by_parent: dict[str, list[dict]] = {}
        for relation in job["relations"]:
            children_by_parent.setdefault(
                relation["parent_table_id"],
                [],
            ).append(deepcopy(relation))

        root_table_id: str = job["root_table_id"]
        root_connection_id: str = tables_by_id[root_table_id]["connection_id"]
        included_table_ids: set[str] = {root_table_id}
        included_relations: list[dict] = []

        def walk(parent_table_id: str) -> None:
            for relation in children_by_parent.get(parent_table_id, []):
                child_id: str = relation["child_table_id"]
                child_table: dict = tables_by_id[child_id]
                if child_table["connection_id"] != root_connection_id:
                    continue
                included_relations.append(deepcopy(relation))
                included_table_ids.add(child_id)
                walk(child_id)

        walk(root_table_id)

        baseline_job: dict = deepcopy(job)
        baseline_job["tables"] = [
            deepcopy(table)
            for table in job["tables"]
            if table["id"] in included_table_ids
        ]
        baseline_job["relations"] = [
            deepcopy(relation) for relation in included_relations
        ]
        return baseline_job

    def build_native_schema(self, job: dict) -> list[dict]:
        tables_by_id: dict[str, dict] = {
            table["id"]: table for table in job["tables"]
        }
        children_by_parent: dict[str, list[dict]] = {}
        for relation in job["relations"]:
            children_by_parent.setdefault(
                relation["parent_table_id"],
                [],
            ).append(relation)

        def build_node(table_id: str) -> dict:
            table: dict = tables_by_id[table_id]
            node: dict = {
                "schema": table["schema"],
                "table": table["table"],
                "label": table["alias"],
                "primary_key": table["primary_key"],
                "soft_delete_column": normalize_soft_delete_column(
                    table.get("soft_delete_column")
                ),
            }
            children: list[dict] = []
            for relation in children_by_parent.get(table_id, []):
                relation_type: str = normalize_relation_type(relation["type"])
                if relation_type not in SUPPORTED_NATIVE_RELATIONS:
                    raise StudioError(
                        "原生 PGSync 模式仅支持 one_to_one 和 one_to_many 关系。"
                    )
                child_node: dict = build_node(relation["child_table_id"])
                child_node["relationship"] = {
                    "variant": default_relation_variant(relation_type),
                    "type": native_relation_type(relation_type),
                    "foreign_key": {
                        "parent": [
                            pair["parent_field"]
                            for pair in relation["field_pairs"]
                        ],
                        "child": [
                            pair["child_field"]
                            for pair in relation["field_pairs"]
                        ],
                    },
                    "fixed_conditions": relation.get("fixed_conditions", []),
                }
                children.append(child_node)
            if children:
                node["children"] = children
            return node

        root: dict = tables_by_id[job["root_table_id"]]
        connection: dict = self.require_connection(root["connection_id"])
        return [
            {
                "database": connection["database"],
                "index": job["index_name"],
                "nodes": build_node(job["root_table_id"]),
            }
        ]

    def build_search_mapping(self, job: dict) -> dict[str, t.Any]:
        """Build the Elasticsearch mapping for studio-managed indices."""
        tables_by_id: dict[str, dict] = {
            table["id"]: table for table in job["tables"]
        }
        children_by_parent: dict[str, list[dict]] = {}
        for relation in job["relations"]:
            children_by_parent.setdefault(
                relation["parent_table_id"],
                [],
            ).append(relation)

        def build_properties(table_id: str) -> dict[str, t.Any]:
            properties: dict[str, t.Any] = {}
            for relation in children_by_parent.get(table_id, []):
                relation_type: str = normalize_relation_type(relation["type"])
                child_table: dict = tables_by_id[relation["child_table_id"]]
                child_properties: dict[str, t.Any] = build_properties(
                    relation["child_table_id"]
                )
                properties[child_table["alias"]] = (
                    build_relation_container_mapping(
                        relation_type,
                        child_properties,
                    )
                )
            return properties

        return {
            "properties": build_properties(job["root_table_id"]),
        }

    def write_native_schema(
        self,
        job: dict,
        filename: str = "schema.json",
    ) -> str:
        job_dir: str = os.path.join(self.runtime_dir, job["id"])
        os.makedirs(job_dir, exist_ok=True)
        schema_path: str = os.path.join(job_dir, filename)
        with open(schema_path, "w", encoding="utf-8") as fp:
            json.dump(
                self.build_native_schema(job),
                fp,
                ensure_ascii=False,
                indent=2,
            )
        return schema_path

    def build_native_env(self, connection: dict) -> dict[str, str]:
        services: dict = resolve_env_placeholders(
            self.store.snapshot()["services"]
        )
        connection = apply_shared_postgres_credentials(
            resolve_env_placeholders(connection)
        )
        env: dict[str, str] = dict(os.environ)
        env["PG_HOST"] = connection["host"]
        env["PG_PORT"] = str(connection["port"])
        env["PG_USER"] = connection["username"]
        env["PG_PASSWORD"] = connection["password"]
        env["ELASTICSEARCH_URL"] = services["search"]["url"]
        env["ELASTICSEARCH"] = "true"
        env["OPENSEARCH"] = "false"
        env["CHECKPOINT_PATH"] = os.path.join(self.runtime_dir, "checkpoints")
        os.makedirs(env["CHECKPOINT_PATH"], exist_ok=True)
        search_username: str = services["search"].get("username") or ""
        search_password: str = services["search"].get("password") or ""
        if search_username:
            env["ELASTICSEARCH_USERNAME"] = search_username
        if search_password:
            env["ELASTICSEARCH_PASSWORD"] = search_password
        if search_username and search_password:
            env["ELASTICSEARCH_BASIC_AUTH"] = (
                f"{search_username}:{search_password}"
            )
            env["ELASTICSEARCH_HTTP_AUTH"] = (
                f"{search_username},{search_password}"
            )
        if "verify_certs" in services["search"]:
            env["ELASTICSEARCH_VERIFY_CERTS"] = str(
                bool(services["search"]["verify_certs"])
            ).lower()
        if services["redis"].get("host"):
            env["REDIS_HOST"] = services["redis"]["host"]
            env["REDIS_PORT"] = str(services["redis"]["port"])
            env["REDIS_DB"] = str(services["redis"].get("db", 0))
            env["REDIS_CHECKPOINT"] = "true"
            redis_user: str = services["redis"].get("username") or ""
            if redis_user:
                env["REDIS_USER"] = redis_user
            env["REDIS_PASSWORD"] = services["redis"].get("password") or ""
        return env
