"""Local configuration storage for PGSync Studio."""

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import typing as t
from copy import deepcopy
from datetime import date, datetime, time as dt_time, timezone


def utcnow() -> str:
    """Return the current UTC timestamp as an ISO string."""
    return datetime.now(timezone.utc).isoformat()


def to_jsonable(value: t.Any) -> t.Any:
    """Normalize values so they can be safely serialized to JSON."""
    if isinstance(value, dict):
        return {
            str(key): to_jsonable(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [to_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [to_jsonable(item) for item in value]
    if isinstance(value, (datetime, date, dt_time)):
        return value.isoformat()
    return value


def default_config() -> dict:
    """Return the default persisted configuration."""
    timestamp: str = utcnow()
    return {
        "version": 1,
        "created_at": timestamp,
        "updated_at": timestamp,
        "services": {
            "search": {
                "enabled": True,
                "url": "",
                "username": "",
                "password": "",
                "verify_certs": False,
            },
            "redis": {
                "enabled": True,
                "host": "",
                "port": 6379,
                "password": "",
                "db": 0,
            },
        },
        "connections": [],
        "jobs": [],
        "job_runtime": {},
    }


class ConfigStore:
    """A thread-safe JSON-backed config store."""

    REPLACE_RETRY_DELAYS: tuple[float, ...] = (0.05, 0.1, 0.2)
    JOB_LOG_DIRNAME: str = "job_runtime_logs"

    def __init__(self, path: t.Optional[str] = None) -> None:
        base_dir: str = os.environ.get(
            "PGSYNC_STUDIO_HOME",
            os.path.join(os.getcwd(), "runtime", "studio"),
        )
        config_path: str = os.environ.get(
            "PGSYNC_STUDIO_CONFIG",
            path or os.path.join(base_dir, "config.json"),
        )
        self.path: str = os.path.abspath(config_path)
        self.base_dir: str = os.path.dirname(self.path)
        self.logs_dir: str = os.path.join(self.base_dir, self.JOB_LOG_DIRNAME)
        self._lock: threading.RLock = threading.RLock()
        os.makedirs(self.base_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)
        self._bootstrap_storage()

    def _bootstrap_storage(self) -> None:
        if not os.path.exists(self.path):
            self._write(default_config())
            return

        try:
            payload = self._read()
        except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
            self._write(default_config())
            return

        normalized: dict = self._normalize_payload(payload)
        migrated_logs: dict[str, list[str]] = self._extract_inline_runtime_logs(
            normalized
        )
        for job_id, logs in migrated_logs.items():
            self._write_job_logs(job_id, logs)
        if payload != normalized or migrated_logs:
            self._write(normalized)

    def _read(self) -> dict:
        with open(self.path, "r", encoding="utf-8") as fp:
            return json.load(fp)

    def _load_payload(self) -> dict:
        try:
            payload: t.Any = self._read()
        except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
            return default_config()
        return self._normalize_payload(payload)

    def _normalize_payload(self, payload: t.Any) -> dict:
        defaults: dict = default_config()
        if not isinstance(payload, dict):
            return defaults

        normalized: dict = deepcopy(defaults)
        for key, value in payload.items():
            if key not in normalized:
                normalized[key] = deepcopy(value)

        if payload.get("version") is not None:
            normalized["version"] = payload["version"]
        if payload.get("created_at"):
            normalized["created_at"] = payload["created_at"]
        if payload.get("updated_at"):
            normalized["updated_at"] = payload["updated_at"]

        services: t.Any = payload.get("services")
        if isinstance(services, dict):
            search: t.Any = services.get("search")
            if isinstance(search, dict):
                normalized["services"]["search"].update(deepcopy(search))
            redis: t.Any = services.get("redis")
            if isinstance(redis, dict):
                normalized["services"]["redis"].update(deepcopy(redis))

        connections: t.Any = payload.get("connections")
        if isinstance(connections, list):
            normalized["connections"] = deepcopy(connections)

        jobs: t.Any = payload.get("jobs")
        if isinstance(jobs, list):
            normalized["jobs"] = deepcopy(jobs)

        runtimes: t.Any = payload.get("job_runtime")
        if isinstance(runtimes, dict):
            normalized["job_runtime"] = {
                str(job_id): deepcopy(runtime)
                if isinstance(runtime, dict)
                else {}
                for job_id, runtime in runtimes.items()
            }

        return normalized

    def _normalize_logs(self, logs: t.Any) -> list[str]:
        if logs is None:
            return []
        if isinstance(logs, list):
            return [str(item) for item in logs if item is not None]
        return [str(logs)]

    def _extract_inline_runtime_logs(
        self,
        payload: dict,
    ) -> dict[str, list[str]]:
        migrated: dict[str, list[str]] = {}
        for job_id, runtime in payload.get("job_runtime", {}).items():
            if not isinstance(runtime, dict) or "logs" not in runtime:
                continue
            logs: list[str] = self._normalize_logs(runtime.pop("logs", []))
            if logs:
                migrated[job_id] = logs
        return migrated

    def _write_text(self, path: str, text: str) -> None:
        with open(path, "w", encoding="utf-8") as fp:
            fp.write(text)
            fp.flush()
            os.fsync(fp.fileno())

    def _atomic_write_text(
        self,
        path: str,
        text: str,
        temp_prefix: str,
    ) -> None:
        directory: str = os.path.dirname(path)
        os.makedirs(directory, exist_ok=True)
        fd: int
        temp_path: str
        fd, temp_path = tempfile.mkstemp(
            prefix=temp_prefix,
            suffix=".tmp",
            dir=directory,
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fp:
                fp.write(text)
                fp.flush()
                os.fsync(fp.fileno())

            for index, delay in enumerate((0.0, *self.REPLACE_RETRY_DELAYS)):
                if delay:
                    time.sleep(delay)
                try:
                    os.replace(temp_path, path)
                    return
                except PermissionError:
                    if index == len(self.REPLACE_RETRY_DELAYS):
                        break
                except OSError as exc:
                    if exc.errno != 13 or index == len(self.REPLACE_RETRY_DELAYS):
                        raise

            # Windows bind mounts occasionally reject os.replace on a hot file.
            # Fall back to an in-place overwrite so runtime updates do not fail.
            self._write_text(path, text)
        finally:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass

    def _write_json(self, path: str, payload: t.Any, temp_prefix: str) -> None:
        self._atomic_write_text(
            path,
            json.dumps(
                to_jsonable(payload),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            temp_prefix,
        )

    def _runtime_log_path(self, job_id: str) -> str:
        safe_job_id: str = str(job_id).replace(os.sep, "_")
        if os.altsep:
            safe_job_id = safe_job_id.replace(os.altsep, "_")
        return os.path.join(self.logs_dir, f"{safe_job_id}.json")

    def _read_job_logs(self, job_id: str) -> list[str]:
        path: str = self._runtime_log_path(job_id)
        if not os.path.exists(path):
            return []
        try:
            with open(path, "r", encoding="utf-8") as fp:
                payload: t.Any = json.load(fp)
        except (OSError, ValueError, json.JSONDecodeError):
            return []
        return self._normalize_logs(payload)

    def _write_job_logs(self, job_id: str, logs: t.Any) -> None:
        normalized_logs: list[str] = self._normalize_logs(logs)
        path: str = self._runtime_log_path(job_id)
        if not normalized_logs:
            self._remove_job_logs(job_id)
            return
        self._write_json(path, normalized_logs, f"{job_id}.")

    def _remove_job_logs(self, job_id: str) -> None:
        path: str = self._runtime_log_path(job_id)
        if not os.path.exists(path):
            return
        try:
            os.remove(path)
        except OSError:
            pass

    def _attach_runtime_logs(self, payload: dict) -> dict:
        runtimes: dict = payload.setdefault("job_runtime", {})
        for job_id, runtime in list(runtimes.items()):
            merged_runtime: dict = deepcopy(runtime) if isinstance(runtime, dict) else {}
            logs: list[str] = self._read_job_logs(job_id)
            if logs:
                merged_runtime["logs"] = logs
            runtimes[job_id] = merged_runtime
        return payload

    def _write(self, payload: dict) -> None:
        normalized: dict = self._normalize_payload(payload)
        migrated_logs: dict[str, list[str]] = self._extract_inline_runtime_logs(
            normalized
        )
        for job_id, logs in migrated_logs.items():
            self._write_job_logs(job_id, logs)
        normalized["updated_at"] = utcnow()
        self._write_json(self.path, normalized, "config.")

    def snapshot(self) -> dict:
        with self._lock:
            payload: dict = self._load_payload()
            return deepcopy(self._attach_runtime_logs(payload))

    def save_services(self, services: dict) -> dict:
        with self._lock:
            payload: dict = self._load_payload()
            payload["services"] = deepcopy(services)
            self._write(payload)
            return deepcopy(payload["services"])

    def list_connections(self) -> list[dict]:
        return self.snapshot().get("connections", [])

    def get_connection(self, connection_id: str) -> t.Optional[dict]:
        for connection in self.list_connections():
            if connection["id"] == connection_id:
                return connection
        return None

    def upsert_connection(self, connection: dict) -> dict:
        with self._lock:
            payload: dict = self._load_payload()
            connections: list[dict] = payload["connections"]
            connection = deepcopy(connection)
            connection["updated_at"] = utcnow()
            for index, current in enumerate(connections):
                if current["id"] == connection["id"]:
                    connection.setdefault("created_at", current.get("created_at"))
                    connections[index] = connection
                    self._write(payload)
                    return deepcopy(connection)
            connection.setdefault("created_at", utcnow())
            connections.append(connection)
            self._write(payload)
            return deepcopy(connection)

    def delete_connection(self, connection_id: str) -> bool:
        with self._lock:
            payload: dict = self._load_payload()
            before: int = len(payload["connections"])
            payload["connections"] = [
                item
                for item in payload["connections"]
                if item["id"] != connection_id
            ]
            if len(payload["connections"]) == before:
                return False
            self._write(payload)
            return True

    def list_jobs(self) -> list[dict]:
        return self.snapshot().get("jobs", [])

    def get_job(self, job_id: str) -> t.Optional[dict]:
        for job in self.list_jobs():
            if job["id"] == job_id:
                return job
        return None

    def upsert_job(self, job: dict) -> dict:
        with self._lock:
            payload: dict = self._load_payload()
            jobs: list[dict] = payload["jobs"]
            job = deepcopy(job)
            job["updated_at"] = utcnow()
            for index, current in enumerate(jobs):
                if current["id"] == job["id"]:
                    job.setdefault("created_at", current.get("created_at"))
                    jobs[index] = job
                    self._write(payload)
                    return deepcopy(job)
            job.setdefault("created_at", utcnow())
            jobs.append(job)
            self._write(payload)
            return deepcopy(job)

    def delete_job(self, job_id: str) -> bool:
        with self._lock:
            payload: dict = self._load_payload()
            before: int = len(payload["jobs"])
            payload["jobs"] = [
                job for job in payload["jobs"] if job["id"] != job_id
            ]
            payload["job_runtime"].pop(job_id, None)
            self._remove_job_logs(job_id)
            if len(payload["jobs"]) == before:
                return False
            self._write(payload)
            return True

    def append_job_log(
        self,
        job_id: str,
        line: str,
        limit: t.Optional[int] = None,
    ) -> list[str]:
        with self._lock:
            logs: list[str] = self._read_job_logs(job_id)
            logs.append(str(line))
            if limit and limit > 0 and len(logs) > limit:
                logs = logs[-limit:]
            self._write_job_logs(job_id, logs)
            return deepcopy(logs)

    def update_job_runtime(self, job_id: str, patch: dict) -> dict:
        with self._lock:
            payload: dict = self._load_payload()
            runtime: dict = deepcopy(payload["job_runtime"].get(job_id, {}))
            runtime_patch: dict = deepcopy(patch)
            logs: t.Any = runtime_patch.pop("logs", None)
            has_logs_patch: bool = "logs" in patch
            runtime.update(runtime_patch)
            runtime["updated_at"] = utcnow()
            payload["job_runtime"][job_id] = runtime
            self._write(payload)
            if has_logs_patch:
                self._write_job_logs(job_id, logs)
            current_logs: list[str] = self._read_job_logs(job_id)
            if current_logs:
                runtime["logs"] = current_logs
            return deepcopy(runtime)

    def get_job_runtime(self, job_id: str) -> dict:
        with self._lock:
            payload: dict = self._load_payload()
            runtime: dict = deepcopy(payload.get("job_runtime", {}).get(job_id, {}))
            logs: list[str] = self._read_job_logs(job_id)
            if logs:
                runtime["logs"] = logs
            return runtime
