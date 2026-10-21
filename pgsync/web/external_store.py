"""External configuration storage for PGSync Studio."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
import time
import typing as t
from copy import deepcopy

import sqlalchemy as sa

from .config_store import ConfigStore, default_config, to_jsonable, utcnow
from .credentials import resolve_env_placeholders
from .db import (
    MASKED_SHARED_CREDENTIAL,
    config_database_url_from_env,
    mask_connection_credentials,
)


def _default_base_dir() -> str:
    return os.environ.get(
        "PGSYNC_STUDIO_HOME",
        os.path.join(os.getcwd(), "runtime", "studio"),
    )


def _env_flag(name: str, default: bool = False) -> bool:
    """Return a normalized boolean flag from the environment."""
    raw: str = str(os.environ.get(name, "")).strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _csv_set(value: t.Any) -> set[str]:
    raw: str = str(value or "").strip()
    if not raw:
        return set()
    return {
        item.strip().lower()
        for item in raw.split(",")
        if item and item.strip()
    }


class DatabaseConfigStore:
    """PostgreSQL-backed configuration store for PGSync Studio."""

    META_KEY_ROOT = "root"
    META_KEY_SERVICES = "services"
    JOB_LOG_LIMIT = 80
    REPLACE_RETRY_DELAYS: tuple[float, ...] = (0.05, 0.1, 0.2)
    DEFAULT_AUTO_DISCOVER_EXCLUDE_DATABASES: tuple[str, ...] = (
        "postgres",
        "template0",
        "template1",
    )
    AUTO_DISCOVER_EXCLUDE_DATABASES_ENV = (
        "PGSYNC_STUDIO_AUTO_DISCOVER_CONNECTIONS_EXCLUDE_DATABASES"
    )
    TABLE_COMMENTS: dict[str, dict[str, t.Any]] = {
        "system_pgsync_meta": {
            "table": "PGSync Studio 元数据表，存储全局配置版本和服务配置。",
            "columns": {
                "config_key": "元数据键，例如 root、services。",
                "payload": "JSON 序列化后的元数据内容。",
                "created_at": "记录创建时间，UTC ISO8601 字符串。",
                "updated_at": "记录更新时间，UTC ISO8601 字符串。",
            },
        },
        "system_pgsync_connection": {
            "table": "PGSync Studio 数据库连接配置表。",
            "columns": {
                "id": "连接唯一标识。",
                "payload": "JSON 序列化后的连接配置。",
                "created_at": "记录创建时间，UTC ISO8601 字符串。",
                "updated_at": "记录更新时间，UTC ISO8601 字符串。",
            },
        },
        "system_pgsync_job": {
            "table": "PGSync Studio 同步任务定义表。",
            "columns": {
                "id": "任务唯一标识。",
                "payload": "JSON 序列化后的任务定义。",
                "created_at": "记录创建时间，UTC ISO8601 字符串。",
                "updated_at": "记录更新时间，UTC ISO8601 字符串。",
            },
        },
        "system_pgsync_job_runtime": {
            "table": "PGSync Studio 同步任务运行时状态表。",
            "columns": {
                "job_id": "关联的任务唯一标识。",
                "payload": "JSON 序列化后的任务运行时状态。",
                "created_at": "记录创建时间，UTC ISO8601 字符串。",
                "updated_at": "记录更新时间，UTC ISO8601 字符串。",
            },
        },
    }

    def __init__(
        self,
        database_url: str,
        *,
        base_dir: t.Optional[str] = None,
        seed_config_path: t.Optional[str] = None,
    ) -> None:
        self.database_url = database_url
        self.base_dir: str = os.path.abspath(base_dir or _default_base_dir())
        self.path: str = os.path.abspath(
            seed_config_path
            or os.environ.get(
                "PGSYNC_STUDIO_CONFIG",
                os.path.join(self.base_dir, "config.json"),
            )
        )
        self.logs_dir: str = os.path.join(
            self.base_dir,
            ConfigStore.JOB_LOG_DIRNAME,
        )
        # Configuration definitions share this lock. Runtime state and logs
        # use per-job locks so a busy job cannot stall unrelated runners.
        self._lock: threading.RLock = threading.RLock()
        self._job_locks_guard: threading.Lock = threading.Lock()
        self._job_locks: dict[str, threading.RLock] = {}
        os.makedirs(self.base_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)
        self.engine: sa.Engine = sa.create_engine(
            database_url,
            future=True,
            pool_pre_ping=True,
        )
        self.metadata = sa.MetaData()
        self.meta_table = sa.Table(
            "system_pgsync_meta",
            self.metadata,
            sa.Column("config_key", sa.String(128), primary_key=True),
            sa.Column("payload", sa.Text(), nullable=False),
            sa.Column("created_at", sa.String(64), nullable=False),
            sa.Column("updated_at", sa.String(64), nullable=False),
        )
        self.connection_table = sa.Table(
            "system_pgsync_connection",
            self.metadata,
            sa.Column("id", sa.String(128), primary_key=True),
            sa.Column("payload", sa.Text(), nullable=False),
            sa.Column("created_at", sa.String(64), nullable=False),
            sa.Column("updated_at", sa.String(64), nullable=False),
        )
        self.job_table = sa.Table(
            "system_pgsync_job",
            self.metadata,
            sa.Column("id", sa.String(128), primary_key=True),
            sa.Column("payload", sa.Text(), nullable=False),
            sa.Column("created_at", sa.String(64), nullable=False),
            sa.Column("updated_at", sa.String(64), nullable=False),
        )
        self.runtime_table = sa.Table(
            "system_pgsync_job_runtime",
            self.metadata,
            sa.Column("job_id", sa.String(128), primary_key=True),
            sa.Column("payload", sa.Text(), nullable=False),
            sa.Column("created_at", sa.String(64), nullable=False),
            sa.Column("updated_at", sa.String(64), nullable=False),
        )
        self.metadata.create_all(self.engine)
        self._apply_schema_comments()
        self._bootstrap_storage()

    def _job_lock(self, job_id: str) -> threading.RLock:
        """Return the stable in-process lock for one job's runtime data."""
        normalized_job_id: str = str(job_id)
        with self._job_locks_guard:
            lock = self._job_locks.get(normalized_job_id)
            if lock is None:
                lock = threading.RLock()
                self._job_locks[normalized_job_id] = lock
            return lock

    def _bootstrap_storage(self) -> None:
        with self.engine.begin() as conn:
            self._ensure_root_meta(conn)
            self._ensure_services_meta(conn)
            self._sync_services_from_environment(
                conn,
                overwrite=_env_flag(
                    "PGSYNC_STUDIO_SYNC_SERVICES_FROM_ENV",
                    default=False,
                ),
            )
            self._sync_connections_from_environment(
                conn,
                overwrite=_env_flag(
                    "PGSYNC_STUDIO_SYNC_CONNECTIONS_FROM_ENV",
                    default=False,
                ),
            )

        if not os.path.exists(self.path):
            return
        if not self.is_empty():
            return

        snapshot: dict = ConfigStore(self.path).snapshot()
        if not self._snapshot_has_user_data(snapshot):
            return
        self.import_snapshot(snapshot)

    def _snapshot_has_user_data(self, snapshot: dict) -> bool:
        if snapshot.get("connections") or snapshot.get("jobs") or snapshot.get(
            "job_runtime"
        ):
            return True
        services: dict = snapshot.get("services", {})
        search: dict = services.get("search", {})
        redis: dict = services.get("redis", {})
        return bool(search.get("url") or redis.get("host"))

    def is_empty(self) -> bool:
        with self.engine.begin() as conn:
            job_count: int = int(
                conn.execute(
                    sa.select(sa.func.count()).select_from(self.job_table)
                ).scalar_one()
            )
            connection_count: int = int(
                conn.execute(
                    sa.select(sa.func.count()).select_from(self.connection_table)
                ).scalar_one()
            )
            runtime_count: int = int(
                conn.execute(
                    sa.select(sa.func.count()).select_from(self.runtime_table)
                ).scalar_one()
            )
        return job_count == 0 and connection_count == 0 and runtime_count == 0

    def import_snapshot(self, snapshot: dict) -> None:
        with self._lock:
            services: dict = deepcopy(snapshot.get("services") or {})
            self.save_services(services)
            for connection in snapshot.get("connections", []):
                self.upsert_connection(connection)
            for job in snapshot.get("jobs", []):
                self.upsert_job(job)
            for job_id, runtime in (snapshot.get("job_runtime") or {}).items():
                self.update_job_runtime(job_id, runtime)

    def _serialize(self, payload: t.Any) -> str:
        return json.dumps(
            to_jsonable(payload),
            ensure_ascii=False,
            sort_keys=True,
        )

    def _deserialize(self, raw: t.Any, default: t.Any) -> t.Any:
        if raw is None:
            return deepcopy(default)
        if isinstance(raw, (dict, list)):
            return deepcopy(raw)
        try:
            return json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return deepcopy(default)

    def _normalize_logs(self, logs: t.Any) -> list[str]:
        if logs is None:
            return []
        if isinstance(logs, list):
            return [str(item) for item in logs if item is not None]
        return [str(logs)]

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
        directory: str = os.path.dirname(path) or self.base_dir
        os.makedirs(directory, exist_ok=True)
        fd, temp_path = tempfile.mkstemp(
            prefix=temp_prefix,
            suffix=".tmp",
            dir=directory,
        )
        try:
            os.close(fd)
            self._write_text(temp_path, text)
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

    def _apply_schema_comments(self) -> None:
        if self.engine.dialect.name != "postgresql":
            return
        with self.engine.begin() as conn:
            for table_name, comment_spec in self.TABLE_COMMENTS.items():
                table_comment: str = str(comment_spec.get("table") or "")
                if table_comment:
                    conn.execute(
                        sa.text(
                            f"COMMENT ON TABLE {table_name} IS :comment"
                        ),
                        {"comment": table_comment},
                    )
                for column_name, column_comment in (
                    comment_spec.get("columns") or {}
                ).items():
                    conn.execute(
                        sa.text(
                            f"COMMENT ON COLUMN {table_name}.{column_name} IS :comment"
                        ),
                        {"comment": str(column_comment)},
                    )

    def _meta_row(
        self,
        conn: sa.Connection,
        key: str,
    ) -> t.Optional[sa.Row]:
        return conn.execute(
            sa.select(self.meta_table).where(self.meta_table.c.config_key == key)
        ).first()

    def _ensure_root_meta(self, conn: sa.Connection) -> None:
        row = self._meta_row(conn, self.META_KEY_ROOT)
        if row:
            return
        timestamp: str = utcnow()
        conn.execute(
            self.meta_table.insert().values(
                config_key=self.META_KEY_ROOT,
                payload=self._serialize({"version": default_config()["version"]}),
                created_at=timestamp,
                updated_at=timestamp,
            )
        )

    def _ensure_services_meta(self, conn: sa.Connection) -> None:
        row = self._meta_row(conn, self.META_KEY_SERVICES)
        if row:
            return
        timestamp: str = utcnow()
        conn.execute(
            self.meta_table.insert().values(
                config_key=self.META_KEY_SERVICES,
                payload=self._serialize(default_config()["services"]),
                created_at=timestamp,
                updated_at=timestamp,
            )
        )

    def _touch_root_meta(
        self,
        conn: sa.Connection,
        *,
        timestamp: t.Optional[str] = None,
    ) -> None:
        current: t.Optional[sa.Row] = self._meta_row(conn, self.META_KEY_ROOT)
        if current is None:
            self._ensure_root_meta(conn)
            current = self._meta_row(conn, self.META_KEY_ROOT)
        if current is None:
            return
        conn.execute(
            self.meta_table.update()
            .where(self.meta_table.c.config_key == self.META_KEY_ROOT)
            .values(updated_at=timestamp or utcnow())
        )

    def _upsert_payload_row(
        self,
        conn: sa.Connection,
        table: sa.Table,
        key_column: str,
        key_value: str,
        payload: dict,
    ) -> dict:
        timestamp: str = utcnow()
        current: t.Optional[sa.Row] = conn.execute(
            sa.select(table).where(getattr(table.c, key_column) == key_value)
        ).first()
        created_at: str = (
            str(current._mapping["created_at"]) if current is not None else payload.get("created_at") or timestamp
        )
        payload = deepcopy(payload)
        payload.setdefault("created_at", created_at)
        payload["updated_at"] = timestamp
        values: dict = {
            key_column: key_value,
            "payload": self._serialize(payload),
            "created_at": created_at,
            "updated_at": timestamp,
        }
        if current is None:
            conn.execute(table.insert().values(**values))
        else:
            conn.execute(
                table.update()
                .where(getattr(table.c, key_column) == key_value)
                .values(**values)
            )
        self._touch_root_meta(conn, timestamp=timestamp)
        return payload

    def _load_services(self, conn: sa.Connection) -> dict:
        stored: dict = default_config()["services"]
        row = self._meta_row(conn, self.META_KEY_SERVICES)
        if row is None:
            return deepcopy(stored)
        payload: dict = self._deserialize(row._mapping["payload"], stored)
        if isinstance(payload, dict):
            search = payload.get("search")
            if isinstance(search, dict):
                stored["search"].update(deepcopy(search))
            redis = payload.get("redis")
            if isinstance(redis, dict):
                stored["redis"].update(deepcopy(redis))
        return stored

    def _services_from_environment(self) -> dict[str, dict[str, t.Any]]:
        """Build a partial services payload while keeping credentials external."""
        payload: dict[str, dict[str, t.Any]] = {"search": {}, "redis": {}}

        search_url: str = (os.environ.get("ELASTICSEARCH_URL") or "").strip()
        if search_url:
            payload["search"]["enabled"] = True
            payload["search"]["url"] = search_url
        if "ELASTICSEARCH_USERNAME" in os.environ:
            payload["search"]["username"] = "${ELASTICSEARCH_USERNAME}"
        if "ELASTICSEARCH_PASSWORD" in os.environ:
            payload["search"]["password"] = "${ELASTICSEARCH_PASSWORD}"
        if "ELASTICSEARCH_VERIFY_CERTS" in os.environ:
            payload["search"]["verify_certs"] = _env_flag(
                "ELASTICSEARCH_VERIFY_CERTS",
                default=False,
            )

        redis_host: str = (os.environ.get("REDIS_HOST") or "").strip()
        if redis_host:
            payload["redis"]["enabled"] = True
            payload["redis"]["host"] = redis_host
        if "REDIS_PORT" in os.environ:
            payload["redis"]["port"] = int(
                os.environ.get("REDIS_PORT") or 6379
            )
        if "REDIS_USER" in os.environ:
            payload["redis"]["username"] = "${REDIS_USER}"
        if "REDIS_PASSWORD" in os.environ:
            payload["redis"]["password"] = "${REDIS_PASSWORD}"
        if "REDIS_DB" in os.environ:
            payload["redis"]["db"] = int(os.environ.get("REDIS_DB") or 0)
        return payload

    def _sync_services_from_environment(
        self,
        conn: sa.Connection,
        *,
        overwrite: bool,
    ) -> None:
        """Seed or synchronize service settings from environment variables."""
        env_services: dict[str, dict[str, t.Any]] = (
            self._services_from_environment()
        )
        current: dict = self._load_services(conn)
        changed: bool = False

        for group_name in ("search", "redis"):
            current_group: dict = current.get(group_name, {})
            env_group: dict = env_services.get(group_name, {})
            for key, value in env_group.items():
                current_value: t.Any = current_group.get(key)
                should_apply: bool = overwrite or current_value in {
                    None,
                    "",
                }
                if key in {"port", "db"}:
                    should_apply = overwrite or current_value in {None, ""}
                if key == "verify_certs":
                    should_apply = overwrite or "verify_certs" not in current_group
                if should_apply and current_value != value:
                    current_group[key] = value
                    changed = True

        if changed:
            self._set_services(conn, current)

    def _connections_from_environment(self) -> list[dict]:
        """Build connection payloads from environment variables."""
        raw: str = (
            os.environ.get("PGSYNC_STUDIO_CONNECTIONS_JSON") or ""
        ).strip()
        if not raw:
            return []
        try:
            payload: t.Any = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "PGSYNC_STUDIO_CONNECTIONS_JSON must be valid JSON"
            ) from exc
        if not isinstance(payload, list):
            raise RuntimeError(
                "PGSYNC_STUDIO_CONNECTIONS_JSON must be a JSON array"
            )
        connections: list[dict] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            connection = deepcopy(item)
            connection.setdefault("type", "postgresql")
            if not connection.get("id"):
                connection["id"] = str(
                    connection.get("name")
                    or connection.get("database")
                    or ""
                )
            if connection.get("id"):
                connections.append(mask_connection_credentials(connection))
        return connections

    def _sync_connections_from_environment(
        self,
        conn: sa.Connection,
        *,
        overwrite: bool,
    ) -> None:
        """Seed or synchronize database connections from environment."""
        connections: list[dict] = self._connections_from_environment()
        if not connections:
            return
        current_rows: dict[str, dict] = {
            str(item.get("id")): item
            for item in self._list_payload_rows(conn, self.connection_table)
            if item.get("id")
        }
        changed: bool = False
        for connection in connections:
            connection_id: str = str(connection.get("id") or "")
            if not connection_id:
                continue
            if connection_id in current_rows and not overwrite:
                continue
            self._upsert_payload_row(
                conn,
                self.connection_table,
                "id",
                connection_id,
                connection,
            )
            changed = True
        if changed:
            self._touch_root_meta(conn)

    def supports_connection_auto_discovery(self) -> bool:
        return self.engine.dialect.name == "postgresql"

    def _config_instance_connection_template(
        self,
    ) -> t.Optional[dict[str, t.Any]]:
        try:
            url = sa.engine.make_url(self.database_url)
        except Exception:
            return None
        database_name: str = str(url.database or "").strip()
        if not database_name:
            return None
        return {
            "host": str(url.host or "").strip(),
            "port": int(url.port or 5432),
            "username": str(url.username or "").strip(),
            "password": str(url.password or ""),
            "database": database_name,
        }

    def _connection_signature(
        self,
        connection: dict,
    ) -> t.Optional[tuple[str, int, str]]:
        host: str = ""
        port: int = 5432
        database_name: str = ""
        raw_url: str = str(connection.get("url") or "").strip()
        if raw_url:
            try:
                url = sa.engine.make_url(raw_url)
            except Exception:
                url = None
            if url is not None:
                host = str(url.host or "").strip()
                port = int(url.port or 5432)
                database_name = str(url.database or "").strip()
        if not database_name:
            database_name = str(connection.get("database") or "").strip()
        if not host:
            host = str(connection.get("host") or "").strip()
        if connection.get("port") not in {None, ""}:
            try:
                port = int(connection.get("port") or 5432)
            except (TypeError, ValueError):
                port = 5432
        if not host or not database_name:
            return None
        return (host.lower(), port, database_name.lower())

    def _auto_discovered_connection_id(
        self,
        host: str,
        port: int,
        database_name: str,
    ) -> str:
        slug: str = "".join(
            character.lower() if character.isalnum() else "_"
            for character in database_name
        ).strip("_")
        while "__" in slug:
            slug = slug.replace("__", "_")
        if not slug:
            slug = "database"
        digest: str = hashlib.sha1(
            f"{host}:{port}:{database_name}".encode("utf-8")
        ).hexdigest()[:10]
        candidate: str = f"conn_auto_{slug}_{digest}"
        return candidate[:128]

    def _build_auto_discovered_connection(
        self,
        template: dict[str, t.Any],
        database_name: str,
    ) -> dict[str, t.Any]:
        host: str = str(template.get("host") or "").strip()
        port: int = int(template.get("port") or 5432)
        normalized_database: str = str(database_name or "").strip()
        return {
            "id": self._auto_discovered_connection_id(
                host,
                port,
                normalized_database,
            ),
            "name": normalized_database,
            "type": "postgresql",
            "host": host,
            "port": port,
            "database": normalized_database,
            "username": MASKED_SHARED_CREDENTIAL,
            "password": "",
            "url": "",
        }

    def _list_instance_databases(self, conn: sa.Connection) -> list[str]:
        rows = conn.execute(
            sa.text(
                """
                SELECT datname
                FROM pg_database
                WHERE datallowconn = true
                  AND datistemplate = false
                ORDER BY datname
                """
            )
        ).scalars()
        return [
            str(item).strip()
            for item in rows
            if str(item or "").strip()
        ]

    def _excluded_auto_discovery_databases(self) -> set[str]:
        return {
            *{
                item.lower()
                for item in self.DEFAULT_AUTO_DISCOVER_EXCLUDE_DATABASES
            },
            *_csv_set(
                os.environ.get(self.AUTO_DISCOVER_EXCLUDE_DATABASES_ENV)
                or ""
            ),
        }

    def list_instance_connection_databases(self) -> set[str]:
        if not self.supports_connection_auto_discovery():
            return set()
        template: t.Optional[dict[str, t.Any]] = (
            self._config_instance_connection_template()
        )
        if not template:
            return set()
        instance_host: str = str(template.get("host") or "").strip().lower()
        instance_port: int = int(template.get("port") or 5432)
        if not instance_host:
            return set()
        with self._lock, self.engine.begin() as conn:
            return {
                signature[2]
                for signature in (
                    self._connection_signature(item)
                    for item in self._list_payload_rows(
                        conn,
                        self.connection_table,
                    )
                )
                if signature is not None
                and signature[0] == instance_host
                and signature[1] == instance_port
            }

    def sync_instance_connections(
        self,
        *,
        extra_excluded_databases: t.Optional[t.Iterable[str]] = None,
    ) -> dict[str, t.Any]:
        summary: dict[str, t.Any] = {
            "ok": False,
            "supported": False,
            "config_database": None,
            "discovered_databases": [],
            "excluded_databases": [],
            "added_connection_ids": [],
            "added_databases": [],
        }
        if not self.supports_connection_auto_discovery():
            return summary
        template: t.Optional[dict[str, t.Any]] = (
            self._config_instance_connection_template()
        )
        if not template:
            return summary
        config_database: str = str(template["database"])
        summary["supported"] = True
        summary["config_database"] = config_database
        excluded_databases: set[str] = self._excluded_auto_discovery_databases()
        if extra_excluded_databases:
            excluded_databases.update(
                {
                    str(item).strip().lower()
                    for item in extra_excluded_databases
                    if str(item or "").strip()
                }
            )
        summary["excluded_databases"] = sorted(excluded_databases)
        with self._lock, self.engine.begin() as conn:
            discovered_databases: list[str] = [
                database_name
                for database_name in self._list_instance_databases(conn)
                if database_name.lower() != config_database.lower()
                and database_name.lower() not in excluded_databases
            ]
            summary["discovered_databases"] = deepcopy(discovered_databases)
            existing_signatures: set[tuple[str, int, str]] = {
                signature
                for signature in (
                    self._connection_signature(item)
                    for item in self._list_payload_rows(
                        conn,
                        self.connection_table,
                    )
                )
                if signature is not None
            }
            for database_name in discovered_databases:
                payload: dict[str, t.Any] = (
                    self._build_auto_discovered_connection(
                        template,
                        database_name,
                    )
                )
                signature = self._connection_signature(payload)
                if signature is None or signature in existing_signatures:
                    continue
                self._upsert_payload_row(
                    conn,
                    self.connection_table,
                    "id",
                    str(payload["id"]),
                    payload,
                )
                existing_signatures.add(signature)
                summary["added_connection_ids"].append(payload["id"])
                summary["added_databases"].append(database_name)
        summary["ok"] = True
        return summary

    def _set_services(self, conn: sa.Connection, services: dict) -> dict:
        current = self._meta_row(conn, self.META_KEY_SERVICES)
        timestamp: str = utcnow()
        normalized: dict = self._load_services(conn)
        if isinstance(services, dict):
            search = services.get("search")
            if isinstance(search, dict):
                normalized["search"].update(deepcopy(search))
            redis = services.get("redis")
            if isinstance(redis, dict):
                normalized["redis"].update(deepcopy(redis))
        values: dict = {
            "config_key": self.META_KEY_SERVICES,
            "payload": self._serialize(normalized),
            "created_at": (
                str(current._mapping["created_at"])
                if current is not None
                else timestamp
            ),
            "updated_at": timestamp,
        }
        if current is None:
            conn.execute(self.meta_table.insert().values(**values))
        else:
            conn.execute(
                self.meta_table.update()
                .where(self.meta_table.c.config_key == self.META_KEY_SERVICES)
                .values(**values)
            )
        self._touch_root_meta(conn, timestamp=timestamp)
        return normalized

    def _list_payload_rows(self, conn: sa.Connection, table: sa.Table) -> list[dict]:
        rows = conn.execute(
            sa.select(table.c.payload).order_by(table.c.created_at.asc())
        ).all()
        return [
            self._deserialize(row._mapping["payload"], {})
            for row in rows
        ]

    def _get_payload_row(
        self,
        conn: sa.Connection,
        table: sa.Table,
        key_column: str,
        key_value: str,
    ) -> t.Optional[dict]:
        row = conn.execute(
            sa.select(table.c.payload).where(getattr(table.c, key_column) == key_value)
        ).first()
        if row is None:
            return None
        return self._deserialize(row._mapping["payload"], {})

    def snapshot(self) -> dict:
        with self._lock, self.engine.begin() as conn:
            payload: dict = default_config()
            root = self._meta_row(conn, self.META_KEY_ROOT)
            if root is not None:
                root_payload: dict = self._deserialize(
                    root._mapping["payload"],
                    {"version": payload["version"]},
                )
                payload["version"] = int(
                    root_payload.get("version") or payload["version"]
                )
                payload["created_at"] = str(root._mapping["created_at"])
                payload["updated_at"] = str(root._mapping["updated_at"])
            payload["services"] = self._load_services(conn)
            payload["connections"] = self._list_payload_rows(conn, self.connection_table)
            payload["jobs"] = self._list_payload_rows(conn, self.job_table)
            runtime_rows = conn.execute(
                sa.select(
                    self.runtime_table.c.job_id,
                    self.runtime_table.c.payload,
                ).order_by(self.runtime_table.c.created_at.asc())
            ).all()
            runtimes: dict[str, dict] = {}
            for row in runtime_rows:
                job_id: str = str(row._mapping["job_id"])
                runtime: dict = self._deserialize(row._mapping["payload"], {})
                with self._job_lock(job_id):
                    logs: list[str] = self._read_job_logs(job_id)
                if logs:
                    runtime["logs"] = logs
                runtimes[job_id] = runtime
            payload["job_runtime"] = runtimes
            return deepcopy(payload)

    def save_services(self, services: dict) -> dict:
        with self._lock, self.engine.begin() as conn:
            return deepcopy(self._set_services(conn, services))

    def list_connections(self) -> list[dict]:
        with self._lock, self.engine.begin() as conn:
            return deepcopy(self._list_payload_rows(conn, self.connection_table))

    def get_connection(self, connection_id: str) -> t.Optional[dict]:
        with self._lock, self.engine.begin() as conn:
            connection = self._get_payload_row(
                conn,
                self.connection_table,
                "id",
                connection_id,
            )
            return deepcopy(connection) if connection else None

    def upsert_connection(self, connection: dict) -> dict:
        with self._lock, self.engine.begin() as conn:
            payload = self._upsert_payload_row(
                conn,
                self.connection_table,
                "id",
                str(connection["id"]),
                deepcopy(connection),
            )
            return deepcopy(payload)

    def delete_connection(self, connection_id: str) -> bool:
        with self._lock, self.engine.begin() as conn:
            result = conn.execute(
                self.connection_table.delete().where(
                    self.connection_table.c.id == connection_id
                )
            )
            if not result.rowcount:
                return False
            self._touch_root_meta(conn)
            return True

    def list_jobs(self) -> list[dict]:
        with self._lock, self.engine.begin() as conn:
            return deepcopy(self._list_payload_rows(conn, self.job_table))

    def get_job(self, job_id: str) -> t.Optional[dict]:
        with self._lock, self.engine.begin() as conn:
            job = self._get_payload_row(conn, self.job_table, "id", job_id)
            return deepcopy(job) if job else None

    def upsert_job(self, job: dict) -> dict:
        with self._lock, self.engine.begin() as conn:
            payload = self._upsert_payload_row(
                conn,
                self.job_table,
                "id",
                str(job["id"]),
                deepcopy(job),
            )
            return deepcopy(payload)

    def delete_job(self, job_id: str) -> bool:
        # Keep the lock order global -> job whenever both domains are needed.
        # Runtime-only operations never acquire the global lock.
        with self._lock, self._job_lock(job_id), self.engine.begin() as conn:
            result = conn.execute(
                self.job_table.delete().where(self.job_table.c.id == job_id)
            )
            conn.execute(
                self.runtime_table.delete().where(self.runtime_table.c.job_id == job_id)
            )
            if not result.rowcount:
                return False
            self._remove_job_logs(job_id)
            self._touch_root_meta(conn)
            return True

    def append_job_log(
        self,
        job_id: str,
        line: str,
        limit: t.Optional[int] = None,
    ) -> list[str]:
        tail_limit: int = limit or self.JOB_LOG_LIMIT
        with self._job_lock(job_id):
            logs: list[str] = self._read_job_logs(job_id)
            logs.append(str(line))
            logs = logs[-tail_limit:]
            self._write_job_logs(job_id, logs)
            return deepcopy(logs)

    def update_job_runtime(self, job_id: str, patch: dict) -> dict:
        with self._job_lock(job_id), self.engine.begin() as conn:
            runtime: dict = self._get_payload_row(
                conn,
                self.runtime_table,
                "job_id",
                job_id,
            ) or {}
            runtime_patch: dict = deepcopy(patch)
            logs: t.Any = runtime_patch.pop("logs", None)
            has_logs_patch: bool = "logs" in patch
            runtime.update(runtime_patch)
            runtime["updated_at"] = utcnow()
            payload = self._upsert_payload_row(
                conn,
                self.runtime_table,
                "job_id",
                job_id,
                runtime,
            )
            if has_logs_patch:
                self._write_job_logs(job_id, logs)
                current_logs = self._read_job_logs(job_id)
            else:
                current_logs = self._read_job_logs(job_id)
            if current_logs:
                payload["logs"] = current_logs
            return deepcopy(payload)

    def get_job_runtime(self, job_id: str) -> dict:
        with self._job_lock(job_id), self.engine.begin() as conn:
            runtime: dict = self._get_payload_row(
                conn,
                self.runtime_table,
                "job_id",
                job_id,
            ) or {}
            logs: list[str] = self._read_job_logs(job_id)
            if logs:
                runtime["logs"] = logs
            return deepcopy(runtime)


def create_store(path: t.Optional[str] = None) -> t.Union[ConfigStore, DatabaseConfigStore]:
    """Create the configured PGSync Studio store implementation."""

    database_url: str = config_database_url_from_env()
    if not database_url:
        return ConfigStore(path)
    return DatabaseConfigStore(
        database_url,
        base_dir=_default_base_dir(),
        seed_config_path=path,
    )
