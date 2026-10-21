"""Database helpers for PGSync Studio."""

from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
import typing as t
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote_plus, unquote, urlsplit

import sqlalchemy as sa

from ..constants import LEGACY_TRIGGER_FUNC, MATERIALIZED_VIEW, TRIGGER_FUNC
from ..view import replace_view_rows
from .credentials import resolve_env_placeholders
from .metrics import record_pg_query

logger = logging.getLogger(__name__)

COMMON_CURSOR_COLUMNS = (
    "updated_at",
    "update_time",
    "updated_time",
    "gmt_modified",
    "modify_time",
    "modified_at",
    "mtime",
    "write_date",
    "last_modified_at",
)

CHANGE_EVENT_TABLE = "_pgsync_change_event"
CHANGE_EVENT_SEQUENCE = f"{CHANGE_EVENT_TABLE}_id_seq"
CHANGE_EVENT_PARTITION_PREFIX = f"{CHANGE_EVENT_TABLE}_p"
MASKED_SHARED_CREDENTIAL = "******"
SHARED_PG_CREDENTIAL_ENV_PAIRS: tuple[tuple[str, str], ...] = (
    ("PGSYNC_STUDIO_PG_USERNAME", "PGSYNC_STUDIO_PG_PASSWORD"),
    ("PGSYNC_STUDIO_POSTGRES_USERNAME", "PGSYNC_STUDIO_POSTGRES_PASSWORD"),
    (
        "PGSYNC_STUDIO_CONFIG_DATABASE_USERNAME",
        "PGSYNC_STUDIO_CONFIG_DATABASE_PASSWORD",
    ),
    ("PGSQL_USERNAME", "PGSQL_PASSWORD"),
    ("POSTGRES_USER", "POSTGRES_PASSWORD"),
)
CONFIG_DATABASE_URL_ENV_NAMES: tuple[str, ...] = (
    "PGSYNC_STUDIO_CONFIG_DATABASE_URL",
    "PGSYNC_STUDIO_CONFIG_DATABASE_URL_SECRET",
)
CONFIG_DATABASE_USERNAME_ENV = "PGSYNC_STUDIO_CONFIG_DATABASE_USERNAME"
CONFIG_DATABASE_PASSWORD_ENV = "PGSYNC_STUDIO_CONFIG_DATABASE_PASSWORD"

READ_REPLICA_ENABLED_ENV = "PGSYNC_STUDIO_READ_REPLICA_ENABLED"
READ_REPLICA_HOST_ENV = "PGSYNC_STUDIO_READ_REPLICA_HOST"
READ_REPLICA_PORT_ENV = "PGSYNC_STUDIO_READ_REPLICA_PORT"
READ_REPLICA_WAIT_TIMEOUT_MS_ENV = "PGSYNC_STUDIO_READ_REPLICA_WAIT_TIMEOUT_MS"
READ_REPLICA_POLL_INTERVAL_MS_ENV = (
    "PGSYNC_STUDIO_READ_REPLICA_POLL_INTERVAL_MS"
)
READ_REPLICA_FALLBACK_TO_PRIMARY_ENV = (
    "PGSYNC_STUDIO_READ_REPLICA_FALLBACK_TO_PRIMARY"
)
READ_REPLICA_CONNECTION_IDS_ENV = "PGSYNC_STUDIO_READ_REPLICA_CONNECTION_IDS"
READ_REPLICA_COOLDOWN_MS_ENV = "PGSYNC_STUDIO_READ_REPLICA_COOLDOWN_MS"
DEFAULT_READ_REPLICA_WAIT_TIMEOUT_MS = 2000
DEFAULT_READ_REPLICA_POLL_INTERVAL_MS = 100
DEFAULT_READ_REPLICA_COOLDOWN_MS = 30000


def _env_bool(name: str, *, default: bool) -> bool:
    raw_value: t.Optional[str] = os.environ.get(name)
    if raw_value is None or not raw_value.strip():
        return default
    normalized: str = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _env_non_negative_int(name: str, *, default: int) -> int:
    raw_value: str = (os.environ.get(name) or "").strip()
    try:
        return max(0, int(raw_value)) if raw_value else default
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class ReplicaReadConfig:
    """Process-wide read-replica settings loaded without persisting secrets."""

    enabled: bool = False
    host: str = ""
    port: t.Optional[int] = None
    wait_timeout_ms: int = DEFAULT_READ_REPLICA_WAIT_TIMEOUT_MS
    poll_interval_ms: int = DEFAULT_READ_REPLICA_POLL_INTERVAL_MS
    fallback_to_primary: bool = True
    connection_ids: frozenset[str] = dataclass_field(default_factory=frozenset)
    cooldown_ms: int = DEFAULT_READ_REPLICA_COOLDOWN_MS

    @classmethod
    def from_env(cls) -> "ReplicaReadConfig":
        raw_port: str = (os.environ.get(READ_REPLICA_PORT_ENV) or "").strip()
        port: t.Optional[int] = None
        if raw_port:
            try:
                parsed_port: int = int(raw_port)
                if 0 < parsed_port <= 65535:
                    port = parsed_port
            except (TypeError, ValueError):
                port = None
        return cls(
            enabled=_env_bool(READ_REPLICA_ENABLED_ENV, default=False),
            host=(os.environ.get(READ_REPLICA_HOST_ENV) or "").strip(),
            port=port,
            wait_timeout_ms=_env_non_negative_int(
                READ_REPLICA_WAIT_TIMEOUT_MS_ENV,
                default=DEFAULT_READ_REPLICA_WAIT_TIMEOUT_MS,
            ),
            poll_interval_ms=max(
                1,
                _env_non_negative_int(
                    READ_REPLICA_POLL_INTERVAL_MS_ENV,
                    default=DEFAULT_READ_REPLICA_POLL_INTERVAL_MS,
                ),
            ),
            fallback_to_primary=_env_bool(
                READ_REPLICA_FALLBACK_TO_PRIMARY_ENV,
                default=True,
            ),
            connection_ids=frozenset(
                item.strip()
                for item in (
                    os.environ.get(READ_REPLICA_CONNECTION_IDS_ENV) or ""
                ).split(",")
                if item.strip()
            ),
            cooldown_ms=_env_non_negative_int(
                READ_REPLICA_COOLDOWN_MS_ENV,
                default=DEFAULT_READ_REPLICA_COOLDOWN_MS,
            ),
        )


@dataclass(frozen=True)
class ConnectionReadRoute:
    """A fixed primary/replica choice for one connection in one work unit."""

    connection_key: str
    role: str
    target_connection: dict[str, t.Any] = dataclass_field(
        repr=False,
        compare=False,
    )
    required_lsn: t.Optional[str] = None
    replay_lsn: t.Optional[str] = None
    waited_ms: int = 0
    reason: str = ""


@dataclass(frozen=True)
class AssemblyReadPlan:
    """Immutable-by-convention routing decisions shared by assembly workers."""

    routes: dict[str, ConnectionReadRoute] = dataclass_field(
        default_factory=dict,
        repr=False,
    )
    enabled: bool = False
    fallback_to_primary: bool = True
    cooldown_ms: int = DEFAULT_READ_REPLICA_COOLDOWN_MS

    def route_for(self, connection: dict) -> t.Optional[ConnectionReadRoute]:
        return self.routes.get(connection_identity(connection))

    @property
    def uses_replica(self) -> bool:
        return any(route.role == "replica" for route in self.routes.values())

    def summary(self) -> dict[str, t.Any]:
        routes: list[ConnectionReadRoute] = list(self.routes.values())
        return {
            "enabled": self.enabled,
            "fallback_to_primary": self.fallback_to_primary,
            "connection_count": len(routes),
            "replica_count": sum(route.role == "replica" for route in routes),
            "primary_count": sum(route.role == "primary" for route in routes),
            "max_waited_ms": max(
                (route.waited_ms for route in routes),
                default=0,
            ),
            "reasons": {
                route.connection_key: route.reason
                for route in routes
                if route.reason
            },
        }


class ReplicaReadUnavailable(RuntimeError):
    """Raised before a work unit reads data when fallback is disabled."""


class ReplicaReadQueryError(RuntimeError):
    """A selected replica failed while executing a work-unit data query."""


def connection_identity(connection: dict) -> str:
    """Return a stable, non-secret lookup key for a configured connection."""
    connection_id: str = str(connection.get("id") or "").strip()
    if connection_id:
        return connection_id
    resolved: dict = resolve_env_placeholders(connection)
    if resolved.get("url"):
        parsed = urlsplit(str(resolved["url"]))
        return "|".join(
            [
                parsed.hostname or "",
                str(parsed.port or 5432),
                (parsed.path or "").lstrip("/"),
            ]
        )
    return "|".join(
        [
            str(resolved.get("host") or ""),
            str(resolved.get("port") or 5432),
            str(resolved.get("database") or ""),
        ]
    )


def physical_database_identity(connection: dict) -> str:
    """Return a credential-free identity for one physical database target.

    Unlike ``connection_identity``, this deliberately ignores the Studio
    connection id.  Cleanup safety boundaries must be shared by duplicate
    connection definitions that point at the same PostgreSQL database.
    """
    url = sa.engine.make_url(build_postgres_url(connection))
    host: str = str(url.host or "").strip().lower().rstrip(".")
    port: int = int(url.port or 5432)
    database: str = str(url.database or "").strip()
    return "|".join((host, str(port), database))


def _metric_result_row_count(result: t.Any) -> int:
    if isinstance(result, list):
        return len(result)
    if isinstance(result, tuple) and result and isinstance(result[0], list):
        return len(result[0])
    return 0


def quote_ident(value: str) -> str:
    """Return a quoted Postgres identifier."""
    return '"' + value.replace('"', '""') + '"'


def qualified_table(schema: str, table: str) -> str:
    """Return a fully qualified table reference."""
    return f"{quote_ident(schema)}.{quote_ident(table)}"


def _credentials_from_url(raw_url: str) -> tuple[str, str]:
    if not raw_url:
        return "", ""
    try:
        parsed = urlsplit(raw_url)
    except Exception:
        return "", ""
    username = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    return username, password


def config_database_url_from_env() -> str:
    """Build the Studio configuration database URL without persisting credentials."""
    raw_url = next(
        (
            (os.environ.get(name) or "").strip()
            for name in CONFIG_DATABASE_URL_ENV_NAMES
            if (os.environ.get(name) or "").strip()
        ),
        "",
    )
    raw_url = str(resolve_env_placeholders(raw_url)).strip()
    if not raw_url:
        return ""

    username = (os.environ.get(CONFIG_DATABASE_USERNAME_ENV) or "").strip()
    if not username:
        return raw_url

    password = os.environ.get(CONFIG_DATABASE_PASSWORD_ENV) or ""
    url = sa.engine.make_url(raw_url).set(username=username, password=password)
    return url.render_as_string(hide_password=False)


def shared_postgres_credentials() -> tuple[str, str]:
    """Return the shared PostgreSQL credentials loaded from Nacos/env."""
    for username_key, password_key in SHARED_PG_CREDENTIAL_ENV_PAIRS:
        username = (os.environ.get(username_key) or "").strip()
        password = os.environ.get(password_key) or ""
        if username or password:
            return username, password
    return _credentials_from_url(config_database_url_from_env())


def apply_shared_postgres_credentials(connection: dict) -> dict:
    """Return a connection copy with runtime PostgreSQL credentials injected."""
    normalized = dict(connection)
    if normalized.get("url"):
        return normalized

    shared_username, shared_password = shared_postgres_credentials()
    if shared_username:
        normalized["username"] = shared_username
        normalized["password"] = shared_password
    return normalized


def mask_connection_credentials(connection: dict) -> dict:
    """Return a connection copy safe for storage/API responses."""
    masked = dict(connection)
    masked["username"] = MASKED_SHARED_CREDENTIAL
    masked["password"] = ""
    if masked.get("url"):
        masked["url"] = ""
    return masked


def format_order_by(items: list[str]) -> str:
    """Format order by items and support leading '-' for DESC."""
    clauses: list[str] = []
    for item in items:
        if item.startswith("-"):
            clauses.append(f"{quote_ident(item[1:])} DESC")
        else:
            clauses.append(f"{quote_ident(item)} ASC")
    return ", ".join(clauses)


def split_order_by_item(item: str) -> tuple[str, bool]:
    """Return the column name and whether the ordering is descending."""
    if item.startswith("-"):
        return item[1:], True
    return item, False


def build_row_comparison_clause(
    columns: list[str],
    param_names: list[str],
    *,
    descending: bool = False,
) -> str:
    """Build a Postgres row-value comparison clause."""
    comparator: str = "<" if descending else ">"
    quoted_columns: str = ", ".join(quote_ident(column) for column in columns)
    placeholders: str = ", ".join(f":{param}" for param in param_names)
    return f"ROW({quoted_columns}) {comparator} ROW({placeholders})"


def build_seek_pagination_where(
    order_by: list[str],
    seek_after: t.Sequence[t.Any],
) -> tuple[str, dict[str, t.Any]]:
    """Build a lexicographic seek pagination clause for the provided order."""
    if not order_by or not seek_after:
        return "", {}

    normalized_order: list[tuple[str, bool]] = [
        split_order_by_item(item) for item in order_by[: len(seek_after)]
    ]
    params: dict[str, t.Any] = {
        f"seek_{index}": seek_after[index]
        for index in range(len(normalized_order))
    }
    if (
        len(normalized_order) > 1
        and len({descending for _, descending in normalized_order}) == 1
    ):
        return (
            build_row_comparison_clause(
                [column for column, _ in normalized_order],
                [f"seek_{index}" for index in range(len(normalized_order))],
                descending=normalized_order[0][1],
            ),
            params,
        )

    clauses: list[str] = []
    for index, (column, descending) in enumerate(normalized_order):
        comparator: str = "<" if descending else ">"
        prefix_equalities: list[str] = []
        for prefix_index, (prefix_column, _) in enumerate(
            normalized_order[:index]
        ):
            prefix_equalities.append(
                f"{quote_ident(prefix_column)} = :seek_{prefix_index}"
            )
        current_clause: str = (
            f"{quote_ident(column)} {comparator} :seek_{index}"
        )
        if prefix_equalities:
            clauses.append(
                "(" + " AND ".join([*prefix_equalities, current_clause]) + ")"
            )
        else:
            clauses.append(current_clause)
    if not clauses:
        return "", {}
    return "(" + " OR ".join(clauses) + ")", params


def build_postgres_url(
    connection: dict, database: t.Optional[str] = None
) -> str:
    """Build a SQLAlchemy database URL."""
    connection = resolve_env_placeholders(connection)
    if connection.get("url"):
        return connection["url"]
    connection = apply_shared_postgres_credentials(connection)
    host: str = connection["host"]
    port: int = int(connection.get("port") or 5432)
    user: str = connection["username"]
    password: str = connection.get("password") or ""
    db_name: str = database or connection["database"]
    return (
        "postgresql+psycopg2://"
        f"{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{quote_plus(db_name)}"
    )


class DatabaseManager:
    """Manage SQLAlchemy engines for configured database connections."""

    def __init__(self) -> None:
        self._engines: dict[str, sa.Engine] = {}
        self._engines_lock = threading.RLock()
        self._replica_circuit_lock = threading.RLock()
        self._replica_circuit_open_until: float = 0.0
        self._replica_circuit_reason: str = ""

    def _open_replica_circuit(self, cooldown_ms: int, *, reason: str) -> None:
        if cooldown_ms <= 0:
            return
        with self._replica_circuit_lock:
            self._replica_circuit_open_until = max(
                self._replica_circuit_open_until,
                time.monotonic() + (cooldown_ms / 1000.0),
            )
            self._replica_circuit_reason = reason
        logger.warning(
            "Assembly read replica circuit opened for %sms: %s",
            cooldown_ms,
            reason,
        )

    def _close_replica_circuit(self) -> None:
        with self._replica_circuit_lock:
            self._replica_circuit_open_until = 0.0
            self._replica_circuit_reason = ""

    def replica_circuit_snapshot(self) -> dict[str, t.Any]:
        now: float = time.monotonic()
        with self._replica_circuit_lock:
            remaining_ms: int = max(
                0,
                int((self._replica_circuit_open_until - now) * 1000),
            )
            if remaining_ms <= 0:
                self._replica_circuit_open_until = 0.0
                self._replica_circuit_reason = ""
            return {
                "open": remaining_ms > 0,
                "remaining_ms": remaining_ms,
                "reason": self._replica_circuit_reason,
            }

    def _engine_key(self, connection: dict, *, role: str = "primary") -> str:
        return json.dumps(
            {
                "role": role,
                "id": connection.get("id"),
                "url": build_postgres_url(connection),
            },
            sort_keys=True,
        )

    def get_engine(
        self,
        connection: dict,
        *,
        role: str = "primary",
    ) -> sa.Engine:
        key: str = self._engine_key(connection, role=role)
        with self._engines_lock:
            engine: t.Optional[sa.Engine] = self._engines.get(key)
            if engine is None:
                engine = sa.create_engine(
                    build_postgres_url(connection),
                    future=True,
                    pool_pre_ping=True,
                )
                self._engines[key] = engine
            return engine

    def dispose_engine(
        self,
        connection: dict,
        *,
        role: str = "primary",
    ) -> None:
        key: str = self._engine_key(connection, role=role)
        with self._engines_lock:
            engine: t.Optional[sa.Engine] = self._engines.pop(key, None)
        if engine is not None:
            engine.dispose()

    def _replica_connection(
        self,
        connection: dict,
        config: ReplicaReadConfig,
    ) -> dict[str, t.Any]:
        primary_url = sa.engine.make_url(build_postgres_url(connection))
        configured_connect_timeout: int = max(
            1,
            int(math.ceil(config.wait_timeout_ms / 1000.0)),
        )
        existing_connect_timeout: t.Optional[int] = None
        try:
            raw_connect_timeout = primary_url.query.get("connect_timeout")
            if raw_connect_timeout not in (None, ""):
                existing_connect_timeout = max(
                    1,
                    int(str(raw_connect_timeout)),
                )
        except (TypeError, ValueError):
            existing_connect_timeout = None
        connect_timeout: int = (
            min(existing_connect_timeout, configured_connect_timeout)
            if existing_connect_timeout is not None
            else configured_connect_timeout
        )
        replica_url = primary_url.set(
            host=config.host,
            port=config.port or primary_url.port or 5432,
        ).update_query_dict({"connect_timeout": str(connect_timeout)})
        replica_connection: dict[str, t.Any] = dict(connection)
        replica_connection["url"] = replica_url.render_as_string(
            hide_password=False
        )
        return replica_connection

    def _primary_route(
        self,
        connection: dict,
        *,
        reason: str,
        required_lsn: t.Optional[str] = None,
        replay_lsn: t.Optional[str] = None,
        waited_ms: int = 0,
    ) -> ConnectionReadRoute:
        return ConnectionReadRoute(
            connection_key=connection_identity(connection),
            role="primary",
            target_connection=dict(connection),
            required_lsn=required_lsn,
            replay_lsn=replay_lsn,
            waited_ms=waited_ms,
            reason=reason,
        )

    def _fallback_or_raise(
        self,
        connection: dict,
        config: ReplicaReadConfig,
        *,
        reason: str,
        required_lsn: t.Optional[str] = None,
        replay_lsn: t.Optional[str] = None,
        waited_ms: int = 0,
    ) -> ConnectionReadRoute:
        if config.fallback_to_primary:
            logger.warning(
                "Assembly read replica fallback to primary for connection %s: %s",
                connection_identity(connection),
                reason,
            )
            return self._primary_route(
                connection,
                reason=reason,
                required_lsn=required_lsn,
                replay_lsn=replay_lsn,
                waited_ms=waited_ms,
            )
        raise ReplicaReadUnavailable(
            "Read replica unavailable for connection "
            f"{connection_identity(connection)}: {reason}"
        )

    def _current_wal_insert_lsn(self, connection: dict) -> str:
        return str(
            self._run_with_retry(
                connection,
                lambda conn: conn.execute(
                    sa.text("SELECT pg_current_wal_insert_lsn()::text")
                ).scalar_one(),
                label="read_replica required_lsn",
                role="primary",
            )
        )

    def _replica_replay_state(
        self,
        replica_connection: dict,
        required_lsn: str,
    ) -> tuple[bool, t.Optional[str], bool]:
        row: dict[str, t.Any] = self._run_with_retry(
            replica_connection,
            lambda conn: dict(
                conn.execute(
                    sa.text(
                        "SELECT pg_is_in_recovery() AS in_recovery, "
                        "pg_last_wal_replay_lsn()::text AS replay_lsn, "
                        "COALESCE("
                        "pg_last_wal_replay_lsn() >= "
                        "CAST(:required_lsn AS pg_lsn), false"
                        ") AS caught_up"
                    ),
                    {"required_lsn": required_lsn},
                )
                .mappings()
                .one()
            ),
            label="read_replica replay_lsn",
            role="replica",
        )
        return (
            bool(row.get("in_recovery")),
            str(row["replay_lsn"]) if row.get("replay_lsn") else None,
            bool(row.get("caught_up")),
        )

    def _build_connection_read_route(
        self,
        connection: dict,
        config: ReplicaReadConfig,
    ) -> ConnectionReadRoute:
        if not config.enabled:
            return self._primary_route(connection, reason="disabled")
        if not config.host:
            return self._fallback_or_raise(
                connection,
                config,
                reason="missing_replica_host",
            )

        try:
            required_lsn: str = self._current_wal_insert_lsn(connection)
        except Exception as exc:
            return self._fallback_or_raise(
                connection,
                config,
                reason=f"required_lsn_error:{type(exc).__name__}",
            )

        replica_connection: dict[str, t.Any] = self._replica_connection(
            connection,
            config,
        )
        started_at: float = time.monotonic()
        deadline: float = started_at + (config.wait_timeout_ms / 1000.0)
        replay_lsn: t.Optional[str] = None
        while True:
            try:
                in_recovery, replay_lsn, caught_up = (
                    self._replica_replay_state(
                        replica_connection,
                        required_lsn,
                    )
                )
            except Exception as exc:
                waited_ms: int = int((time.monotonic() - started_at) * 1000)
                self._open_replica_circuit(
                    config.cooldown_ms,
                    reason=f"replica_check_error:{type(exc).__name__}",
                )
                return self._fallback_or_raise(
                    connection,
                    config,
                    reason=f"replica_check_error:{type(exc).__name__}",
                    required_lsn=required_lsn,
                    replay_lsn=replay_lsn,
                    waited_ms=waited_ms,
                )

            waited_ms = int((time.monotonic() - started_at) * 1000)
            if not in_recovery:
                self._open_replica_circuit(
                    config.cooldown_ms,
                    reason="replica_not_in_recovery",
                )
                return self._fallback_or_raise(
                    connection,
                    config,
                    reason="replica_not_in_recovery",
                    required_lsn=required_lsn,
                    replay_lsn=replay_lsn,
                    waited_ms=waited_ms,
                )
            if caught_up:
                return ConnectionReadRoute(
                    connection_key=connection_identity(connection),
                    role="replica",
                    target_connection=replica_connection,
                    required_lsn=required_lsn,
                    replay_lsn=replay_lsn,
                    waited_ms=waited_ms,
                    reason="caught_up",
                )
            now: float = time.monotonic()
            if now >= deadline:
                self._open_replica_circuit(
                    config.cooldown_ms,
                    reason="replay_lsn_timeout",
                )
                return self._fallback_or_raise(
                    connection,
                    config,
                    reason="replay_lsn_timeout",
                    required_lsn=required_lsn,
                    replay_lsn=replay_lsn,
                    waited_ms=waited_ms,
                )
            time.sleep(
                min(
                    config.poll_interval_ms / 1000.0,
                    max(0.0, deadline - now),
                )
            )

    def build_assembly_read_plan(
        self,
        connections: t.Iterable[dict],
        *,
        config: t.Optional[ReplicaReadConfig] = None,
    ) -> AssemblyReadPlan:
        """Freeze one read target per source connection before any data read."""
        resolved_config: ReplicaReadConfig = (
            config or ReplicaReadConfig.from_env()
        )
        connection_list: list[dict] = list(connections)
        routes: dict[str, ConnectionReadRoute] = {}
        if not resolved_config.enabled:
            for connection in connection_list:
                route = self._primary_route(connection, reason="disabled")
                routes[route.connection_key] = route
            return AssemblyReadPlan(
                routes=routes,
                enabled=False,
                fallback_to_primary=resolved_config.fallback_to_primary,
                cooldown_ms=resolved_config.cooldown_ms,
            )
        if resolved_config.connection_ids:
            unmatched_connections: list[dict] = [
                connection
                for connection in connection_list
                if str(connection.get("id") or "").strip()
                not in resolved_config.connection_ids
            ]
            if unmatched_connections:
                routes = {
                    connection_identity(connection): self._primary_route(
                        connection,
                        reason="connection_not_allowlisted",
                    )
                    for connection in connection_list
                }
                return AssemblyReadPlan(
                    routes=routes,
                    enabled=True,
                    fallback_to_primary=resolved_config.fallback_to_primary,
                    cooldown_ms=resolved_config.cooldown_ms,
                )
        if not resolved_config.host:
            for connection in connection_list:
                route = self._fallback_or_raise(
                    connection,
                    resolved_config,
                    reason="missing_replica_host",
                )
                routes[route.connection_key] = route
            return AssemblyReadPlan(
                routes=routes,
                enabled=True,
                fallback_to_primary=resolved_config.fallback_to_primary,
                cooldown_ms=resolved_config.cooldown_ms,
            )

        circuit: dict[str, t.Any] = self.replica_circuit_snapshot()
        if circuit["open"]:
            circuit_reason: str = str(circuit.get("reason") or "cooldown")
            if not resolved_config.fallback_to_primary:
                raise ReplicaReadUnavailable(
                    f"Read replica circuit is open: {circuit_reason}"
                )
            routes = {
                connection_identity(connection): self._primary_route(
                    connection,
                    reason=f"circuit_open:{circuit_reason}",
                )
                for connection in connection_list
            }
            return AssemblyReadPlan(
                routes=routes,
                enabled=True,
                fallback_to_primary=True,
                cooldown_ms=resolved_config.cooldown_ms,
            )

        primary_endpoints: set[tuple[t.Optional[str], int]] = set()
        for connection in connection_list:
            primary_url = sa.engine.make_url(build_postgres_url(connection))
            primary_endpoints.add((primary_url.host, primary_url.port or 5432))
        if len(primary_endpoints) > 1:
            for connection in connection_list:
                route = self._fallback_or_raise(
                    connection,
                    resolved_config,
                    reason="multiple_primary_endpoints_unsupported",
                )
                routes[route.connection_key] = route
            return AssemblyReadPlan(
                routes=routes,
                enabled=True,
                fallback_to_primary=resolved_config.fallback_to_primary,
                cooldown_ms=resolved_config.cooldown_ms,
            )
        for connection in connection_list:
            connection_key: str = connection_identity(connection)
            if connection_key in routes:
                continue
            route = self._build_connection_read_route(
                connection,
                resolved_config,
            )
            routes[connection_key] = route
            if route.role == "primary":
                break
        primary_routes: list[ConnectionReadRoute] = [
            route for route in routes.values() if route.role == "primary"
        ]
        if primary_routes:
            fallback_reason: str = primary_routes[0].reason or "not_ready"
            max_waited_ms: int = max(
                (route.waited_ms for route in routes.values()),
                default=0,
            )
            routes = {
                connection_identity(connection): self._primary_route(
                    connection,
                    reason=f"work_unit_fallback:{fallback_reason}",
                    waited_ms=max_waited_ms,
                )
                for connection in connection_list
            }
        else:
            self._close_replica_circuit()
        return AssemblyReadPlan(
            routes=routes,
            enabled=resolved_config.enabled,
            fallback_to_primary=resolved_config.fallback_to_primary,
            cooldown_ms=resolved_config.cooldown_ms,
        )

    def build_primary_assembly_read_plan(
        self,
        connections: t.Iterable[dict],
        *,
        reason: str = "forced_primary",
    ) -> AssemblyReadPlan:
        """Build a no-I/O plan used to restart a whole failed work unit."""
        routes: dict[str, ConnectionReadRoute] = {}
        for connection in connections:
            connection_key: str = connection_identity(connection)
            if connection_key in routes:
                continue
            routes[connection_key] = self._primary_route(
                connection,
                reason=reason,
            )
        return AssemblyReadPlan(
            routes=routes,
            enabled=True,
            fallback_to_primary=True,
        )

    def _resolve_read_target(
        self,
        connection: dict,
        read_plan: t.Optional[AssemblyReadPlan],
    ) -> tuple[dict, str]:
        route: t.Optional[ConnectionReadRoute] = (
            read_plan.route_for(connection) if read_plan is not None else None
        )
        if route is None:
            return connection, "primary"
        return route.target_connection, route.role

    def _assert_replica_session_ready(
        self,
        conn: sa.Connection,
        required_lsn: t.Optional[str],
    ) -> None:
        """Verify the exact pooled session before its assembly SELECT."""
        if not required_lsn:
            raise ReplicaReadQueryError(
                "Selected read replica route has no required LSN"
            )
        row = (
            conn.execute(
                sa.text(
                    "SELECT pg_is_in_recovery() AS in_recovery, "
                    "COALESCE("
                    "pg_last_wal_replay_lsn() >= "
                    "CAST(:required_lsn AS pg_lsn), false"
                    ") AS caught_up"
                ),
                {"required_lsn": required_lsn},
            )
            .mappings()
            .one()
        )
        if not bool(row.get("in_recovery")):
            raise ReplicaReadQueryError(
                "Selected replica session is not a standby"
            )
        if not bool(row.get("caught_up")):
            raise ReplicaReadQueryError(
                "Selected replica session is behind the work-unit LSN"
            )

    def _is_transient_operational_error(
        self,
        error: sa.exc.OperationalError,
    ) -> bool:
        message: str = str(error).lower()
        return bool(error.connection_invalidated) or any(
            snippet in message
            for snippet in (
                "server closed the connection unexpectedly",
                "connection not open",
                "connection was closed",
                "ssl connection has been closed unexpectedly",
            )
        )

    def _run_with_retry(
        self,
        connection: dict,
        operation: t.Callable[[sa.Connection], t.Any],
        *,
        label: str,
        read_plan: t.Optional[AssemblyReadPlan] = None,
        role: t.Optional[str] = None,
    ) -> t.Any:
        read_route: t.Optional[ConnectionReadRoute] = (
            read_plan.route_for(connection) if read_plan is not None else None
        )
        target_connection, resolved_role = self._resolve_read_target(
            connection,
            read_plan,
        )
        if role is not None:
            target_connection = connection
            resolved_role = role
            read_route = None
        last_error: t.Optional[sa.exc.OperationalError] = None
        for attempt in range(2):
            started_at = time.perf_counter()
            try:
                connection_context = (
                    self.connect(target_connection)
                    if resolved_role == "primary"
                    else self.connect(
                        target_connection,
                        role=resolved_role,
                    )
                )
                with connection_context as conn:
                    if resolved_role == "replica" and read_route is not None:
                        self._assert_replica_session_ready(
                            conn,
                            read_route.required_lsn,
                        )
                    result = operation(conn)
                record_pg_query(
                    connection,
                    label,
                    time.perf_counter() - started_at,
                    status="success",
                    rows=_metric_result_row_count(result),
                    role=resolved_role,
                )
                return result
            except ReplicaReadQueryError:
                record_pg_query(
                    connection,
                    label,
                    time.perf_counter() - started_at,
                    status="error",
                    role=resolved_role,
                )
                if read_plan is not None:
                    self._open_replica_circuit(
                        read_plan.cooldown_ms,
                        reason="replica_session_guard_error",
                    )
                raise
            except sa.exc.OperationalError as error:
                record_pg_query(
                    connection,
                    label,
                    time.perf_counter() - started_at,
                    status="error",
                    role=resolved_role,
                )
                last_error = error
                if attempt >= 1 or not self._is_transient_operational_error(
                    error
                ):
                    if resolved_role == "replica" and read_plan is not None:
                        self._open_replica_circuit(
                            read_plan.cooldown_ms,
                            reason="replica_operational_error",
                        )
                        raise ReplicaReadQueryError(
                            "Selected read replica failed during assembly query"
                        ) from error
                    raise
                logger.warning(
                    "Retrying studio database query after transient error: %s",
                    label,
                )
                if resolved_role == "primary":
                    self.dispose_engine(target_connection)
                else:
                    self.dispose_engine(
                        target_connection,
                        role=resolved_role,
                    )
            except sa.exc.DBAPIError as error:
                record_pg_query(
                    connection,
                    label,
                    time.perf_counter() - started_at,
                    status="error",
                    role=resolved_role,
                )
                if resolved_role == "replica" and read_plan is not None:
                    self._open_replica_circuit(
                        read_plan.cooldown_ms,
                        reason="replica_dbapi_error",
                    )
                    raise ReplicaReadQueryError(
                        "Selected read replica failed during assembly query"
                    ) from error
                raise
            except Exception:
                record_pg_query(
                    connection,
                    label,
                    time.perf_counter() - started_at,
                    status="error",
                    role=resolved_role,
                )
                raise
        if last_error is not None:
            raise last_error

    @contextmanager
    def connect(self, connection: dict, *, role: str = "primary"):
        engine: sa.Engine = self.get_engine(connection, role=role)
        with engine.connect() as conn:
            yield conn

    def test_connection(self, connection: dict) -> dict:
        with self.connect(connection) as conn:
            current_db: str = conn.execute(
                sa.text("SELECT current_database()")
            ).scalar_one()
            version: str = conn.execute(
                sa.text("SELECT version()")
            ).scalar_one()
            return {
                "ok": True,
                "database": current_db,
                "version": version,
            }

    def list_schemas(self, connection: dict) -> list[str]:
        with self.connect(connection) as conn:
            inspector = sa.inspect(conn)
            return sorted(
                [
                    schema
                    for schema in inspector.get_schema_names()
                    if schema not in {"information_schema", "pg_catalog"}
                ]
            )

    def list_tables(self, connection: dict, schema: str) -> list[str]:
        with self.connect(connection) as conn:
            inspector = sa.inspect(conn)
            return sorted(inspector.get_table_names(schema=schema))

    def get_table_columns(
        self, connection: dict, schema: str, table: str
    ) -> list[dict]:
        with self.connect(connection) as conn:
            inspector = sa.inspect(conn)
            primary_key: set[str] = set(
                inspector.get_pk_constraint(table, schema=schema).get(
                    "constrained_columns",
                    [],
                )
            )
            columns: list[dict] = []
            for column in inspector.get_columns(table, schema=schema):
                columns.append(
                    {
                        "name": column["name"],
                        "type": str(column["type"]),
                        "nullable": bool(column.get("nullable", True)),
                        "default": column.get("default"),
                        "is_primary_key": column["name"] in primary_key,
                    }
                )
            return columns

    def get_primary_key(
        self, connection: dict, schema: str, table: str
    ) -> list[str]:
        with self.connect(connection) as conn:
            inspector = sa.inspect(conn)
            return (
                inspector.get_pk_constraint(table, schema=schema).get(
                    "constrained_columns",
                    [],
                )
                or []
            )

    def suggest_cursor_column(
        self, connection: dict, schema: str, table: str
    ) -> t.Optional[str]:
        column_names: set[str] = {
            column["name"]
            for column in self.get_table_columns(connection, schema, table)
        }
        for candidate in COMMON_CURSOR_COLUMNS:
            if candidate in column_names:
                return candidate
        return None

    def sample_rows(
        self,
        connection: dict,
        schema: str,
        table: str,
        columns: list[str],
        limit: int = 10,
        not_null_columns: t.Optional[list[str]] = None,
        filters: t.Optional[dict[str, t.Any]] = None,
    ) -> list[dict]:
        projection: str = ", ".join(
            f"{quote_ident(column)} AS {quote_ident(column)}"
            for column in columns
        )
        clauses: list[str] = []
        params: dict[str, t.Any] = {"limit": limit}
        if not_null_columns:
            clauses.extend(
                f"{quote_ident(column)} IS NOT NULL"
                for column in not_null_columns
            )
        for index, (field, value) in enumerate((filters or {}).items()):
            key: str = f"f_{index}"
            if value is None:
                clauses.append(f"{quote_ident(field)} IS NULL")
            else:
                clauses.append(f"{quote_ident(field)} = :{key}")
                params[key] = value
        where_clause: str = (
            f" WHERE {' AND '.join(clauses)}" if clauses else ""
        )
        sql: str = (
            f"SELECT {projection} FROM {qualified_table(schema, table)}"
            f"{where_clause} ORDER BY random() LIMIT :limit"
        )
        return self._run_with_retry(
            connection,
            lambda conn: [
                dict(row)
                for row in conn.execute(sa.text(sql), params).mappings()
            ],
            label=f"{schema}.{table} sample_rows",
        )

    def fetch_rows_by_fields(
        self,
        connection: dict,
        schema: str,
        table: str,
        filters: dict[str, t.Any],
        columns: t.Optional[list[str]] = None,
        order_by: t.Optional[list[str]] = None,
        limit: t.Optional[int] = None,
        read_plan: t.Optional[AssemblyReadPlan] = None,
    ) -> list[dict]:
        selected: str = "*"
        if columns:
            selected = ", ".join(
                f"{quote_ident(column)} AS {quote_ident(column)}"
                for column in columns
            )

        params: dict[str, t.Any] = {}
        clauses: list[str] = []
        for index, (field, value) in enumerate(filters.items()):
            key: str = f"p_{index}"
            if value is None:
                clauses.append(f"{quote_ident(field)} IS NULL")
            else:
                clauses.append(f"{quote_ident(field)} = :{key}")
                params[key] = value

        sql: str = f"SELECT {selected} FROM {qualified_table(schema, table)}"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        if order_by:
            sql += " ORDER BY " + format_order_by(order_by)
        if limit:
            sql += " LIMIT :limit"
            params["limit"] = limit

        return self._run_with_retry(
            connection,
            lambda conn: [
                dict(row)
                for row in conn.execute(sa.text(sql), params).mappings()
            ],
            label=f"{schema}.{table} fetch_rows_by_fields",
            read_plan=read_plan,
        )

    def fetch_rows_by_filter_groups(
        self,
        connection: dict,
        schema: str,
        table: str,
        filter_groups: list[dict[str, t.Any]],
        fixed_filters: t.Optional[dict[str, t.Any]] = None,
        columns: t.Optional[list[str]] = None,
        order_by: t.Optional[list[str]] = None,
        limit: t.Optional[int] = None,
        read_plan: t.Optional[AssemblyReadPlan] = None,
    ) -> list[dict]:
        normalized_groups: list[dict[str, t.Any]] = []
        seen_groups: set[str] = set()
        for group in filter_groups:
            if not group:
                continue
            signature: str = json.dumps(
                {str(key): group[key] for key in sorted(group)},
                sort_keys=True,
                default=str,
            )
            if signature in seen_groups:
                continue
            seen_groups.add(signature)
            normalized_groups.append(group)

        if not normalized_groups:
            return []

        selected: str = "*"
        if columns:
            selected = ", ".join(
                f"{quote_ident(column)} AS {quote_ident(column)}"
                for column in columns
            )

        params: dict[str, t.Any] = {}
        fixed_clauses: list[str] = []
        for index, (field, value) in enumerate((fixed_filters or {}).items()):
            key: str = f"fx_{index}"
            if value is None:
                fixed_clauses.append(f"{quote_ident(field)} IS NULL")
            else:
                fixed_clauses.append(f"{quote_ident(field)} = :{key}")
                params[key] = value

        group_clauses: list[str] = []
        for group_index, group in enumerate(normalized_groups):
            item_clauses: list[str] = []
            for field_index, (field, value) in enumerate(group.items()):
                key: str = f"g_{group_index}_{field_index}"
                if value is None:
                    item_clauses.append(f"{quote_ident(field)} IS NULL")
                else:
                    item_clauses.append(f"{quote_ident(field)} = :{key}")
                    params[key] = value
            if item_clauses:
                group_clauses.append("(" + " AND ".join(item_clauses) + ")")

        if not group_clauses:
            return []

        sql: str = f"SELECT {selected} FROM {qualified_table(schema, table)}"
        where_parts: list[str] = []
        if fixed_clauses:
            where_parts.append("(" + " AND ".join(fixed_clauses) + ")")
        where_parts.append("(" + " OR ".join(group_clauses) + ")")
        sql += " WHERE " + " AND ".join(where_parts)
        if order_by:
            sql += " ORDER BY " + format_order_by(order_by)
        if limit:
            sql += " LIMIT :limit"
            params["limit"] = limit

        return self._run_with_retry(
            connection,
            lambda conn: [
                dict(row)
                for row in conn.execute(sa.text(sql), params).mappings()
            ],
            label=f"{schema}.{table} fetch_rows_by_filter_groups",
            read_plan=read_plan,
        )

    def count_rows_by_fields(
        self,
        connection: dict,
        schema: str,
        table: str,
        filters: dict[str, t.Any],
    ) -> int:
        params: dict[str, t.Any] = {}
        clauses: list[str] = []
        for index, (field, value) in enumerate(filters.items()):
            key: str = f"p_{index}"
            if value is None:
                clauses.append(f"{quote_ident(field)} IS NULL")
            else:
                clauses.append(f"{quote_ident(field)} = :{key}")
                params[key] = value
        sql: str = f"SELECT COUNT(*) FROM {qualified_table(schema, table)}"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        return int(
            self._run_with_retry(
                connection,
                lambda conn: conn.execute(sa.text(sql), params).scalar_one(),
                label=f"{schema}.{table} count_rows_by_fields",
            )
        )

    def fetch_page(
        self,
        connection: dict,
        schema: str,
        table: str,
        order_by: list[str],
        limit: int,
        offset: int = 0,
        seek_after: t.Optional[t.Sequence[t.Any]] = None,
        read_plan: t.Optional[AssemblyReadPlan] = None,
    ) -> list[dict]:
        params: dict[str, t.Any] = {"limit": limit}
        sql: str = f"SELECT * FROM {qualified_table(schema, table)} "
        seek_clause, seek_params = build_seek_pagination_where(
            order_by,
            seek_after or [],
        )
        if seek_clause:
            sql += f"WHERE {seek_clause} "
            params.update(seek_params)
        sql += f"ORDER BY {format_order_by(order_by)} LIMIT :limit"
        if not seek_clause:
            sql += " OFFSET :offset"
            params["offset"] = offset
        return self._run_with_retry(
            connection,
            lambda conn: [
                dict(row)
                for row in conn.execute(
                    sa.text(sql),
                    params,
                ).mappings()
            ],
            label=f"{schema}.{table} fetch_page",
            read_plan=read_plan,
        )

    def fetch_changed_rows(
        self,
        connection: dict,
        schema: str,
        table: str,
        cursor_column: str,
        last_cursor: t.Any,
        last_primary_key: t.Optional[list[t.Any]],
        primary_key: list[str],
        limit: int = 100,
        read_plan: t.Optional[AssemblyReadPlan] = None,
    ) -> tuple[list[dict], t.Optional[dict[str, t.Any]]]:
        params: dict[str, t.Any] = {"limit": limit}
        sql: str = f"SELECT * FROM {qualified_table(schema, table)}"
        if last_cursor is not None:
            params["last_cursor"] = last_cursor
            if last_primary_key and len(last_primary_key) == len(primary_key):
                for index, column in enumerate(primary_key):
                    params[f"last_pk_{index}"] = last_primary_key[index]
                sql += " WHERE " + build_row_comparison_clause(
                    [cursor_column, *primary_key],
                    ["last_cursor"]
                    + [
                        f"last_pk_{index}" for index in range(len(primary_key))
                    ],
                )
            else:
                sql += f" WHERE {quote_ident(cursor_column)} > :last_cursor"
            params["last_cursor"] = last_cursor
        sql += (
            f" ORDER BY {quote_ident(cursor_column)}, "
            + ", ".join(quote_ident(column) for column in primary_key)
            + " LIMIT :limit"
        )
        rows = self._run_with_retry(
            connection,
            lambda conn: [
                dict(row)
                for row in conn.execute(sa.text(sql), params).mappings()
            ],
            label=f"{schema}.{table} fetch_changed_rows",
            read_plan=read_plan,
        )
        next_cursor: t.Optional[dict[str, t.Any]] = None
        if rows:
            last_row: dict = rows[-1]
            next_cursor = {
                "cursor": last_row.get(cursor_column),
                "pk": [last_row.get(column) for column in primary_key],
            }
        return rows, next_cursor

    def ensure_change_event_outbox(
        self,
        connection: dict,
        schema: str,
        *,
        future_days: int = 7,
        now: t.Optional[datetime] = None,
    ) -> dict[str, t.Any]:
        table_name: str = qualified_table(schema, CHANGE_EVENT_TABLE)
        sequence_name: str = qualified_table(schema, CHANGE_EVENT_SEQUENCE)
        sequence_regclass: str = sequence_name.replace("'", "''")
        table_id_idx: str = quote_ident(f"{CHANGE_EVENT_TABLE}_table_id_idx")
        created_at_idx: str = quote_ident(
            f"{CHANGE_EVENT_TABLE}_created_at_idx"
        )
        id_idx: str = quote_ident(f"{CHANGE_EVENT_TABLE}_id_idx")
        current: datetime = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        first_day: date = current.astimezone(timezone.utc).date()
        last_day: date = first_day + timedelta(days=max(0, int(future_days)))

        def operation(conn: sa.Connection) -> dict[str, t.Any]:
            # CREATE TABLE IF NOT EXISTS is not sufficient protection against
            # simultaneous catalog changes from multiple Studio processes.
            # Serialize outbox DDL per database/schema for this transaction.
            conn.execute(
                sa.text(
                    "SELECT PG_ADVISORY_XACT_LOCK("
                    "HASHTEXT('pgsync-outbox-ddl-v1'), "
                    "HASHTEXT(CAST(:schema AS text)))"
                ),
                {"schema": schema},
            )
            relation_kind: t.Optional[str] = conn.execute(
                sa.text(
                    "SELECT c.relkind FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = :schema AND c.relname = :table"
                ),
                {"schema": schema, "table": CHANGE_EVENT_TABLE},
            ).scalar_one_or_none()
            created_parent: bool = relation_kind is None
            if created_parent:
                conn.execute(
                    sa.text(
                        f"CREATE SEQUENCE IF NOT EXISTS {sequence_name} "
                        "AS BIGINT"
                    )
                )
                conn.execute(sa.text(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
  id BIGINT NOT NULL DEFAULT nextval('{sequence_regclass}'::regclass),
  channel TEXT NOT NULL,
  schema_name TEXT NOT NULL,
  table_name TEXT NOT NULL,
  tg_op TEXT NOT NULL,
  event_xmin BIGINT,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CLOCK_TIMESTAMP(),
  PRIMARY KEY (created_at, id)
) PARTITION BY RANGE (created_at)
"""))
                relation_kind = conn.execute(
                    sa.text(
                        "SELECT c.relkind FROM pg_class c "
                        "JOIN pg_namespace n ON n.oid = c.relnamespace "
                        "WHERE n.nspname = :schema AND c.relname = :table"
                    ),
                    {"schema": schema, "table": CHANGE_EVENT_TABLE},
                ).scalar_one_or_none()

            # Existing regular outboxes remain usable until the explicit
            # stop/drop/recreate migration. PostgreSQL cannot convert a
            # regular table to a partitioned table in place.
            if relation_kind == "r":
                conn.execute(
                    sa.text(
                        f"CREATE INDEX IF NOT EXISTS {table_id_idx} "
                        f"ON {table_name}(schema_name, table_name, id)"
                    )
                )
                conn.execute(
                    sa.text(
                        f"CREATE INDEX IF NOT EXISTS {created_at_idx} "
                        f"ON {table_name}(created_at)"
                    )
                )
                conn.commit()
                return {
                    "mode": "regular",
                    "created_parent": False,
                    "created_partitions": [],
                }
            if relation_kind != "p":
                raise RuntimeError(
                    f"{schema}.{CHANGE_EVENT_TABLE} has unsupported "
                    f"relation kind {relation_kind!r}"
                )

            conn.execute(
                sa.text(
                    f"CREATE INDEX IF NOT EXISTS {table_id_idx} "
                    f"ON {table_name}(schema_name, table_name, id)"
                )
            )
            conn.execute(
                sa.text(
                    f"CREATE INDEX IF NOT EXISTS {created_at_idx} "
                    f"ON {table_name}(created_at)"
                )
            )
            conn.execute(
                sa.text(
                    f"CREATE INDEX IF NOT EXISTS {id_idx} "
                    f"ON {table_name}(id)"
                )
            )
            created_partitions: list[str] = []
            partition_day: date = first_day
            while partition_day <= last_day:
                partition_name: str = (
                    f"{CHANGE_EVENT_PARTITION_PREFIX}"
                    f"{partition_day.strftime('%Y%m%d')}"
                )
                partition_table: str = qualified_table(schema, partition_name)
                upper_day: date = partition_day + timedelta(days=1)
                conn.execute(
                    sa.text(
                        f"CREATE TABLE IF NOT EXISTS {partition_table} "
                        f"PARTITION OF {table_name} FOR VALUES FROM "
                        f"('{partition_day.isoformat()} 00:00:00+00') TO "
                        f"('{upper_day.isoformat()} 00:00:00+00')"
                    )
                )
                created_partitions.append(partition_name)
                partition_day = upper_day
            conn.commit()
            return {
                "mode": "partitioned",
                "created_parent": created_parent,
                "ensured_partitions": created_partitions,
            }

        return self._run_with_retry(
            connection,
            operation,
            label=f"{schema}.{CHANGE_EVENT_TABLE} ensure_change_event_outbox",
        )

    def change_event_outbox_kind(
        self,
        connection: dict,
        schema: str,
    ) -> t.Optional[str]:
        return self._run_with_retry(
            connection,
            lambda conn: conn.execute(
                sa.text(
                    "SELECT c.relkind FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = :schema AND c.relname = :table"
                ),
                {"schema": schema, "table": CHANGE_EVENT_TABLE},
            ).scalar_one_or_none(),
            label=f"{schema}.{CHANGE_EVENT_TABLE} relation_kind",
        )

    def list_change_capture_schemas(self, connection: dict) -> list[str]:
        """Return schemas that still contain PGSync capture artifacts."""

        def operation(conn: sa.Connection) -> list[str]:
            rows = conn.execute(
                sa.text(
                    "SELECT DISTINCT n.nspname AS schema_name "
                    "FROM pg_trigger t "
                    "JOIN pg_class c ON c.oid = t.tgrelid "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "JOIN pg_proc p ON p.oid = t.tgfoid "
                    "JOIN pg_namespace fn ON fn.oid = p.pronamespace "
                    "WHERE NOT t.tgisinternal "
                    "AND fn.nspname = n.nspname "
                    "AND p.proname = ANY(:function_names) "
                    "AND t.tgname IN ("
                    "n.nspname || '_' || c.relname || '_notify', "
                    "n.nspname || '_' || c.relname || '_truncate'"
                    ") ORDER BY schema_name"
                ),
                {
                    "function_names": [
                        TRIGGER_FUNC,
                        LEGACY_TRIGGER_FUNC,
                    ],
                },
            ).scalars()
            return [str(schema) for schema in rows if schema]

        return self._run_with_retry(
            connection,
            operation,
            label="list_change_capture_schemas",
        )

    def reconcile_change_capture(
        self,
        connection: dict,
        schema: str,
        *,
        desired_indices_by_table: dict[str, list[str]],
        referenced_table_names: list[str],
        apply_changes: bool,
        lock_timeout_ms: int,
        ownership_guard: t.Optional[t.Callable[[], bool]] = None,
    ) -> dict[str, t.Any]:
        """Reconcile PGSync-owned trigger metadata against persisted jobs.

        Stopped jobs remain in ``referenced_table_names`` and therefore keep
        their capture artifacts.  Only tables with no persisted job reference
        can lose PGSync-owned view metadata or triggers.  The complete check
        and optional mutation run under the same transaction/advisory lock as
        normal view bootstrap.
        """

        desired_indices: dict[str, list[str]] = {
            str(table_name): sorted(
                {
                    str(index_name)
                    for index_name in (index_names or [])
                    if str(index_name or "").strip()
                }
            )
            for table_name, index_names in desired_indices_by_table.items()
            if str(table_name or "").strip()
        }
        referenced_tables: set[str] = {
            str(table_name)
            for table_name in referenced_table_names
            if str(table_name or "").strip()
        }
        result: dict[str, t.Any] = {
            "ok": True,
            "schema": schema,
            "apply_changes": bool(apply_changes),
            "applied": False,
            "view_kind": "",
            "referenced_table_names": sorted(referenced_tables),
            "current_view_indices_by_table": {},
            "desired_view_indices_by_table": desired_indices,
            "stale_view_tables": [],
            "changed_view_indices": [],
            "missing_view_tables": [],
            "orphan_capture_tables": [],
            "managed_triggers": [],
            "dropped_triggers": [],
            "unmanaged_trigger_conflicts": [],
            "pending_changes": False,
            "apply_blocked_reason": "",
            "safe_for_orphan_cleanup": False,
            "ownership_changed": False,
        }

        engine: sa.Engine = self.get_engine(connection, role="primary")
        with engine.begin() as conn:
            conn.execute(
                sa.text("SELECT set_config('lock_timeout', :value, true)"),
                {"value": f"{max(100, int(lock_timeout_ms))}ms"},
            )
            conn.execute(
                sa.text(
                    "SELECT pg_advisory_xact_lock("
                    "hashtext(:namespace), hashtext(:resource))"
                ),
                {
                    "namespace": "pgsync",
                    "resource": f"{schema}.{MATERIALIZED_VIEW}",
                },
            )
            if ownership_guard is not None and not ownership_guard():
                result["ok"] = False
                result["ownership_changed"] = True
                return result
            view_kind = conn.execute(
                sa.text(
                    "SELECT c.relkind FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = :schema AND c.relname = :view_name "
                    "AND c.relkind IN ('m', 'v')"
                ),
                {"schema": schema, "view_name": MATERIALIZED_VIEW},
            ).scalar_one_or_none()
            result["view_kind"] = str(view_kind or "")

            existing_rows: dict[str, dict[str, set[str]]] = {}
            if view_kind:
                rows = conn.execute(
                    sa.text(
                        f"SELECT * FROM "
                        f"{qualified_table(schema, MATERIALIZED_VIEW)}"
                    )
                ).mappings()
                for row in rows:
                    table_name = row.get("table_name")
                    if not table_name:
                        continue
                    existing_rows[str(table_name)] = {
                        "primary_keys": set(row.get("primary_keys") or []),
                        "foreign_keys": set(row.get("foreign_keys") or []),
                        "indices": set(row.get("indices") or []),
                        "columns": set(row.get("columns") or []),
                        "payload_columns": set(
                            row.get("payload_columns") or []
                        ),
                    }

            current_indices: dict[str, list[str]] = {
                table_name: sorted(row.get("indices") or [])
                for table_name, row in existing_rows.items()
            }
            result["current_view_indices_by_table"] = current_indices
            result["stale_view_tables"] = sorted(
                set(existing_rows) - referenced_tables
            )
            result["missing_view_tables"] = sorted(
                referenced_tables - set(existing_rows)
            )

            reconciled_rows: dict[str, dict[str, set[str]]] = {}
            for table_name in sorted(set(existing_rows) & referenced_tables):
                row = {
                    name: set(values)
                    for name, values in existing_rows[table_name].items()
                }
                expected_indices: set[str] = set(
                    desired_indices.get(table_name) or []
                )
                current_table_indices: set[str] = set(row.get("indices") or [])
                if current_table_indices != expected_indices:
                    result["changed_view_indices"].append(
                        {
                            "table_name": table_name,
                            "current": sorted(current_table_indices),
                            "desired": sorted(expected_indices),
                        }
                    )
                row["indices"] = expected_indices
                reconciled_rows[table_name] = row

            trigger_rows = list(
                conn.execute(
                    sa.text(
                        "SELECT t.tgname AS trigger_name, "
                        "c.relname AS table_name, fn.nspname AS "
                        "function_schema, p.proname AS function_name "
                        "FROM pg_trigger t "
                        "JOIN pg_class c ON c.oid = t.tgrelid "
                        "JOIN pg_namespace n ON n.oid = c.relnamespace "
                        "JOIN pg_proc p ON p.oid = t.tgfoid "
                        "JOIN pg_namespace fn ON fn.oid = p.pronamespace "
                        "WHERE NOT t.tgisinternal AND n.nspname = :schema"
                    ),
                    {"schema": schema},
                ).mappings()
            )
            orphan_triggers: list[dict[str, str]] = []
            for trigger_row in trigger_rows:
                table_name = str(trigger_row["table_name"])
                trigger_name = str(trigger_row["trigger_name"])
                function_schema = str(trigger_row["function_schema"])
                function_name = str(trigger_row["function_name"])
                expected_names = {
                    f"{schema}_{table_name}_notify",
                    f"{schema}_{table_name}_truncate",
                }
                if trigger_name not in expected_names:
                    continue
                trigger = {
                    "trigger_name": trigger_name,
                    "table_name": table_name,
                    "function_schema": function_schema,
                    "function_name": function_name,
                }
                managed = function_schema == schema and function_name in {
                    TRIGGER_FUNC,
                    LEGACY_TRIGGER_FUNC,
                }
                if managed:
                    result["managed_triggers"].append(trigger)
                    if table_name not in referenced_tables:
                        orphan_triggers.append(trigger)
                elif table_name not in referenced_tables:
                    result["unmanaged_trigger_conflicts"].append(trigger)

            orphan_tables: set[str] = set(result["stale_view_tables"])
            orphan_tables.update(
                trigger["table_name"] for trigger in orphan_triggers
            )
            result["orphan_capture_tables"] = sorted(orphan_tables)
            view_changes: bool = bool(
                result["stale_view_tables"] or result["changed_view_indices"]
            )
            result["pending_changes"] = bool(view_changes or orphan_triggers)

            if result["unmanaged_trigger_conflicts"]:
                result["apply_blocked_reason"] = "unmanaged_trigger_conflict"
            if (
                apply_changes
                and result["pending_changes"]
                and not result["apply_blocked_reason"]
            ):
                if view_changes and view_kind:
                    replace_view_rows(
                        conn,
                        schema,
                        reconciled_rows,
                        has_materialized_view=view_kind == "m",
                        has_plain_view=view_kind == "v",
                    )
                for trigger in orphan_triggers:
                    conn.execute(
                        sa.text(
                            f"DROP TRIGGER IF EXISTS "
                            f"{quote_ident(trigger['trigger_name'])} ON "
                            f"{qualified_table(schema, trigger['table_name'])}"
                        )
                    )
                    result["dropped_triggers"].append(trigger)
                result["applied"] = True

            result["safe_for_orphan_cleanup"] = bool(
                not result["unmanaged_trigger_conflicts"]
                and (
                    not result["pending_changes"]
                    or (apply_changes and result["applied"])
                )
            )
        return result

    def maintain_change_event_partitions(
        self,
        connection: dict,
        schema: str,
        *,
        retention_days: int,
        future_days: int,
        safe_event_ids_by_table: dict[str, int],
        blocked_table_names: list[str],
        lock_timeout_ms: int,
        lock_key: str,
        allow_orphan_tables: bool = False,
        referenced_table_names: t.Optional[list[str]] = None,
        now: t.Optional[datetime] = None,
    ) -> dict[str, t.Any]:
        current: datetime = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        current = current.astimezone(timezone.utc)
        ensure_result: dict[str, t.Any] = self.ensure_change_event_outbox(
            connection,
            schema,
            future_days=future_days,
            now=current,
        )
        mode: str = str(ensure_result.get("mode") or "")
        result: dict[str, t.Any] = {
            "mode": mode,
            "locked": False,
            "ensured_partitions": list(
                ensure_result.get("ensured_partitions") or []
            ),
            "dropped_partitions": [],
            "blocked_partitions": [],
            "orphan_tables": [],
            "stop_reason": "",
        }
        if mode != "partitioned":
            result["stop_reason"] = "regular_table"
            return result

        parent_table: str = qualified_table(schema, CHANGE_EVENT_TABLE)
        referenced_tables: set[str] = set(referenced_table_names or [])
        cutoff: datetime = current - timedelta(days=max(1, retention_days))
        conn: t.Optional[sa.Connection] = None
        try:
            conn = self.get_engine(connection, role="primary").connect()
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            result["locked"] = bool(
                conn.execute(
                    sa.text(
                        "SELECT PG_TRY_ADVISORY_LOCK("
                        "HASHTEXT(CAST(:lock_key AS text)), "
                        "HASHTEXT(CAST('pgsync-partition-v1:' || "
                        ":lock_key AS text)))"
                    ),
                    {"lock_key": lock_key},
                ).scalar_one()
            )
            if not result["locked"]:
                result["stop_reason"] = "lock_not_acquired"
                return result
            conn.execute(
                sa.text("SELECT set_config('lock_timeout', :value, false)"),
                {"value": f"{max(100, int(lock_timeout_ms))}ms"},
            )
            rows = list(
                conn.execute(
                    sa.text(
                        "SELECT child.relname AS partition_name, "
                        "(i.inhrelid IS NOT NULL) AS attached, "
                        "COALESCE(i.inhdetachpending, false) "
                        "AS detach_pending "
                        "FROM pg_class child "
                        "JOIN pg_namespace cn "
                        "ON cn.oid = child.relnamespace "
                        "LEFT JOIN pg_class parent "
                        "ON parent.relnamespace = cn.oid "
                        "AND parent.relname = :parent "
                        "LEFT JOIN pg_inherits i "
                        "ON i.inhrelid = child.oid "
                        "AND i.inhparent = parent.oid "
                        "WHERE cn.nspname = :schema "
                        "AND child.relkind = 'r' "
                        "AND child.relname LIKE :partition_prefix "
                        "ORDER BY child.relname"
                    ),
                    {
                        "schema": schema,
                        "parent": CHANGE_EVENT_TABLE,
                        "partition_prefix": (
                            f"{CHANGE_EVENT_PARTITION_PREFIX}%"
                        ),
                    },
                ).mappings()
            )
            for row in rows:
                partition_name = str(row["partition_name"])
                if not partition_name.startswith(
                    CHANGE_EVENT_PARTITION_PREFIX
                ):
                    continue
                suffix: str = partition_name[
                    len(CHANGE_EVENT_PARTITION_PREFIX) :
                ]
                try:
                    partition_day: date = datetime.strptime(
                        suffix, "%Y%m%d"
                    ).date()
                except ValueError:
                    continue
                partition_upper: datetime = datetime.combine(
                    partition_day + timedelta(days=1),
                    datetime.min.time(),
                    tzinfo=timezone.utc,
                )
                if partition_upper > cutoff:
                    continue
                partition_table: str = qualified_table(schema, partition_name)
                table_max_rows = list(
                    conn.execute(
                        sa.text(
                            f"SELECT table_name, MAX(id) AS max_event_id "
                            f"FROM {partition_table} GROUP BY table_name"
                        )
                    ).mappings()
                )
                unsafe_tables: list[dict[str, t.Any]] = []
                for table_max_row in table_max_rows:
                    source_table: str = str(table_max_row["table_name"])
                    max_event_id: int = int(table_max_row["max_event_id"] or 0)
                    safe_event_id: int = int(
                        safe_event_ids_by_table.get(source_table) or 0
                    )
                    if (
                        allow_orphan_tables
                        and source_table not in referenced_tables
                    ):
                        result["orphan_tables"].append(
                            {
                                "partition": partition_name,
                                "table_name": source_table,
                                "max_event_id": max_event_id,
                            }
                        )
                        continue
                    if (
                        source_table in blocked_table_names
                        or safe_event_id <= 0
                    ):
                        reason = "safe_cursor_not_ready"
                    elif max_event_id > safe_event_id:
                        reason = "safe_cursor_behind"
                    else:
                        continue
                    unsafe_tables.append(
                        {
                            "table_name": source_table,
                            "max_event_id": max_event_id,
                            "safe_event_id": safe_event_id,
                            "reason": reason,
                        }
                    )
                if unsafe_tables:
                    result["blocked_partitions"].append(
                        {
                            "partition": partition_name,
                            "tables": unsafe_tables,
                            "reason": "unsafe_table_watermark",
                        }
                    )
                    continue
                if bool(row.get("attached")):
                    detach_suffix: str = (
                        "FINALIZE"
                        if bool(row.get("detach_pending"))
                        else "CONCURRENTLY"
                    )
                    conn.execute(
                        sa.text(
                            f"ALTER TABLE {parent_table} DETACH PARTITION "
                            f"{partition_table} {detach_suffix}"
                        )
                    )
                conn.execute(sa.text(f"DROP TABLE {partition_table}"))
                result["dropped_partitions"].append(partition_name)
            result["stop_reason"] = "completed"
            return result
        finally:
            if conn is not None:
                if result.get("locked"):
                    try:
                        conn.execute(
                            sa.text(
                                "SELECT PG_ADVISORY_UNLOCK("
                                "HASHTEXT(CAST(:lock_key AS text)), "
                                "HASHTEXT(CAST('pgsync-partition-v1:' || "
                                ":lock_key AS text)))"
                            ),
                            {"lock_key": lock_key},
                        )
                    except Exception:
                        logger.exception(
                            "Failed to release outbox partition lock for %s",
                            lock_key,
                        )
                conn.close()

    def fetch_change_events(
        self,
        connection: dict,
        schema: str,
        *,
        table_names: list[str],
        after_id: int,
        limit: int = 100,
        exclude_ids: t.Optional[list[int]] = None,
    ) -> list[dict]:
        if not table_names:
            return []
        table_name: str = qualified_table(schema, CHANGE_EVENT_TABLE)
        params: dict[str, t.Any] = {
            "after_id": int(after_id),
            "table_names": list(table_names),
            "limit": int(limit),
        }
        sql: str = (
            "SELECT id, channel, schema_name, table_name, tg_op, event_xmin, "
            "payload, created_at "
            f"FROM {table_name} "
            "WHERE id > :after_id "
            "AND schema_name = :schema_name "
            "AND table_name = ANY(:table_names) "
        )
        params["schema_name"] = schema
        if exclude_ids:
            params["exclude_ids"] = [int(item) for item in exclude_ids]
            sql += "AND NOT (id = ANY(:exclude_ids)) "
        sql += "ORDER BY id ASC LIMIT :limit"
        return self._run_with_retry(
            connection,
            lambda conn: [
                dict(row)
                for row in conn.execute(sa.text(sql), params).mappings()
            ],
            label=f"{schema}.{CHANGE_EVENT_TABLE} fetch_change_events",
        )

    def max_change_event_id(
        self,
        connection: dict,
        schema: str,
        *,
        table_names: list[str],
    ) -> int:
        if not table_names:
            return 0
        table_name: str = qualified_table(schema, CHANGE_EVENT_TABLE)
        params: dict[str, t.Any] = {
            "schema_name": schema,
            "table_names": list(table_names),
        }
        sql: str = (
            f"SELECT COALESCE(MAX(id), 0) FROM {table_name} "
            "WHERE schema_name = :schema_name "
            "AND table_name = ANY(:table_names)"
        )
        return int(
            self._run_with_retry(
                connection,
                lambda conn: conn.execute(sa.text(sql), params).scalar_one(),
                label=f"{schema}.{CHANGE_EVENT_TABLE} max_change_event_id",
            )
        )

    def fetch_change_event_ids(
        self,
        connection: dict,
        schema: str,
        *,
        table_names: list[str],
        after_id: int,
        limit: int = 100,
    ) -> list[int]:
        if not table_names:
            return []
        table_name: str = qualified_table(schema, CHANGE_EVENT_TABLE)
        params: dict[str, t.Any] = {
            "after_id": int(after_id),
            "schema_name": schema,
            "table_names": list(table_names),
            "limit": int(limit),
        }
        sql: str = (
            f"SELECT id FROM {table_name} "
            "WHERE id > :after_id "
            "AND schema_name = :schema_name "
            "AND table_name = ANY(:table_names) "
            "ORDER BY id ASC LIMIT :limit"
        )
        return [
            int(row["id"])
            for row in self._run_with_retry(
                connection,
                lambda conn: [
                    dict(row)
                    for row in conn.execute(sa.text(sql), params).mappings()
                ],
                label=f"{schema}.{CHANGE_EVENT_TABLE} fetch_change_event_ids",
            )
        ]

    def purge_change_events_cycle(
        self,
        connection: dict,
        schema: str,
        *,
        table_name_filter: str,
        safe_event_id: int,
        cutoff: t.Any,
        batch_size: int,
        max_batches: int,
        max_rows: int,
        max_run_seconds: float,
        max_rows_per_second: float,
        batch_pause_ms: int,
        statement_timeout_ms: int,
        lock_timeout_ms: int,
        lock_key: str,
        stop_event: t.Optional[threading.Event] = None,
    ) -> dict[str, t.Any]:
        if (
            not table_name_filter
            or safe_event_id <= 0
            or batch_size <= 0
            or max_batches <= 0
            or max_rows <= 0
            or max_run_seconds <= 0
            or max_rows_per_second <= 0
        ):
            return {
                "locked": False,
                "deleted_rows": 0,
                "batches": 0,
                "elapsed_ms": 0,
                "rate_limited_ms": 0,
                "stop_reason": "invalid_config",
            }
        table_name: str = qualified_table(schema, CHANGE_EVENT_TABLE)
        params: dict[str, t.Any] = {
            "schema_name": schema,
            "table_name_filter": table_name_filter,
            "safe_event_id": int(safe_event_id),
            "cutoff": cutoff,
        }
        sql: str = f"""
WITH doomed AS (
  SELECT id
  FROM {table_name}
  WHERE schema_name = :schema_name
    AND table_name = :table_name_filter
    AND id <= :safe_event_id
    AND created_at < :cutoff
  ORDER BY id ASC
  LIMIT :limit
),
deleted AS (
  DELETE FROM {table_name}
  WHERE id IN (SELECT id FROM doomed)
  RETURNING id
)
SELECT COUNT(*) FROM deleted
"""
        started: float = time.monotonic()
        rate_limited_seconds: float = 0.0
        deleted_total: int = 0
        batches: int = 0
        locked: bool = False
        stop_reason: str = "max_batches"
        conn: t.Optional[sa.Connection] = None
        query_label: str = (
            f"{schema}.{CHANGE_EVENT_TABLE} purge_change_events_cycle"
        )
        try:
            conn = self.get_engine(connection, role="primary").connect()
            locked = bool(
                conn.execute(
                    sa.text(
                        "SELECT PG_TRY_ADVISORY_LOCK("
                        "HASHTEXT(CAST(:lock_key AS text)), "
                        "HASHTEXT(CAST('pgsync-cleanup-v2:' || "
                        ":lock_key AS text)))"
                    ),
                    {"lock_key": lock_key},
                ).scalar_one()
            )
            conn.commit()
            if not locked:
                stop_reason = "lock_not_acquired"
            else:
                for _ in range(max_batches):
                    elapsed: float = time.monotonic() - started
                    if stop_event is not None and stop_event.is_set():
                        stop_reason = "shutdown"
                        break
                    if elapsed >= max_run_seconds:
                        stop_reason = "time_budget"
                        break
                    remaining_rows: int = max_rows - deleted_total
                    if remaining_rows <= 0:
                        stop_reason = "row_budget"
                        break
                    current_limit: int = min(batch_size, remaining_rows)
                    conn.execute(
                        sa.text(
                            "SELECT set_config('statement_timeout', :value, true)"
                        ),
                        {"value": f"{int(statement_timeout_ms)}ms"},
                    )
                    conn.execute(
                        sa.text(
                            "SELECT set_config('lock_timeout', :value, true)"
                        ),
                        {"value": f"{int(lock_timeout_ms)}ms"},
                    )
                    deleted_rows: int = int(
                        conn.execute(
                            sa.text(sql),
                            {**params, "limit": current_limit},
                        ).scalar_one()
                    )
                    conn.commit()
                    batches += 1
                    deleted_total += deleted_rows
                    if deleted_rows < current_limit:
                        stop_reason = "exhausted"
                        break
                    target_elapsed: float = deleted_total / max_rows_per_second
                    throttle_seconds: float = max(
                        0.0,
                        target_elapsed - (time.monotonic() - started),
                    )
                    if batch_pause_ms > 0:
                        throttle_seconds = max(
                            throttle_seconds,
                            batch_pause_ms / 1000.0,
                        )
                    if throttle_seconds > 0:
                        wait_started: float = time.monotonic()
                        if stop_event is None:
                            time.sleep(throttle_seconds)
                        elif stop_event.wait(throttle_seconds):
                            stop_reason = "shutdown"
                        rate_limited_seconds += time.monotonic() - wait_started
                        if stop_reason == "shutdown":
                            break
                else:
                    stop_reason = "max_batches"
            record_pg_query(
                connection,
                query_label,
                time.monotonic() - started,
                status="success",
                rows=deleted_total,
                role="primary",
            )
            return {
                "locked": locked,
                "deleted_rows": deleted_total,
                "batches": batches,
                "elapsed_ms": int((time.monotonic() - started) * 1000),
                "rate_limited_ms": int(rate_limited_seconds * 1000),
                "stop_reason": stop_reason,
            }
        except Exception:
            if conn is not None:
                conn.rollback()
            record_pg_query(
                connection,
                query_label,
                time.monotonic() - started,
                status="error",
                role="primary",
            )
            raise
        finally:
            if conn is not None:
                if locked:
                    try:
                        conn.rollback()
                        released: bool = bool(
                            conn.execute(
                                sa.text(
                                    "SELECT PG_ADVISORY_UNLOCK("
                                    "HASHTEXT(CAST(:lock_key AS text)), "
                                    "HASHTEXT(CAST("
                                    "'pgsync-cleanup-v2:' || "
                                    ":lock_key AS text)))"
                                ),
                                {"lock_key": lock_key},
                            ).scalar_one()
                        )
                        if not released:
                            raise RuntimeError(
                                "durable cleanup advisory lock was not held"
                            )
                        conn.commit()
                    except Exception:
                        logger.exception(
                            "Could not release durable cleanup advisory lock"
                        )
                        conn.invalidate()
                conn.close()
