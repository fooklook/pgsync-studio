"""Bootstrap PGSync Studio configuration from Nacos."""

from __future__ import annotations

import json
import os
import sys
import typing as t
from string import Template
from urllib.parse import urljoin

import requests


def _env_flag(name: str, default: bool = False) -> bool:
    raw: str = str(os.environ.get(name, "")).strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _stringify(value: t.Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return Template(str(value)).safe_substitute(os.environ)


def _set_env(name: str, value: t.Any, *, overwrite: bool) -> None:
    if value is None:
        return
    if not overwrite and os.environ.get(name) not in {None, ""}:
        return
    os.environ[name] = _stringify(value)


def _server_url(server_addr: str) -> str:
    value: str = server_addr.strip().rstrip("/")
    if not value:
        raise RuntimeError("NACOS_SERVER_ADDR is required when Nacos is enabled")
    if "://" not in value:
        value = f"http://{value}"
    return value


def _server_urls(server_addr: str) -> list[str]:
    values: list[str] = [
        _server_url(item)
        for item in server_addr.split(",")
        if item.strip()
    ]
    if not values:
        raise RuntimeError("NACOS_SERVER_ADDR is required when Nacos is enabled")
    return values


def _default_data_id(profile: str) -> str:
    return f"pgsync-studio-{profile}.yaml"


def _data_id_from_env(profile: str) -> str:
    return (
        os.environ.get("PGSYNC_STUDIO_NACOS_DATA_ID")
        or _default_data_id(profile)
    ).strip()


def _login(
    base_url: str,
    *,
    username: str,
    password: str,
    timeout: float,
) -> str:
    if not username and not password:
        return ""
    response = requests.post(
        urljoin(base_url + "/", "nacos/v1/auth/login"),
        data={"username": username, "password": password},
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    return str(payload.get("accessToken") or "")


def _fetch_config(
    base_url: str,
    *,
    data_id: str,
    group: str,
    namespace: str,
    access_token: str,
    timeout: float,
) -> str:
    params: dict[str, str] = {
        "dataId": data_id,
        "group": group,
    }
    if namespace:
        params["tenant"] = namespace
    if access_token:
        params["accessToken"] = access_token
    response = requests.get(
        urljoin(base_url + "/", "nacos/v1/cs/configs"),
        params=params,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text


def _load_yaml(raw: str) -> dict:
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError(
            "Nacos config is YAML, but PyYAML is not installed"
        ) from exc
    payload = yaml.safe_load(raw)
    return payload if isinstance(payload, dict) else {}


def _parse_config(raw: str, *, data_id: str, fmt: str) -> dict:
    normalized_format: str = fmt.strip().lower()
    if normalized_format in {"json"} or data_id.endswith(".json"):
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else {}
    if normalized_format in {"yaml", "yml"} or data_id.endswith(
        (".yaml", ".yml")
    ):
        return _load_yaml(raw)
    try:
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else {}
    except json.JSONDecodeError:
        return _load_yaml(raw)


def _config_root(payload: dict) -> dict:
    pgsync = payload.get("pgsync")
    if isinstance(pgsync, dict):
        return pgsync
    return payload


def _apply_studio_config(
    root: dict,
    *,
    overwrite: bool,
) -> None:
    studio = root.get("studio")
    if not isinstance(studio, dict):
        return
    _set_env(
        "PGSYNC_STUDIO_HOST",
        studio.get("host"),
        overwrite=overwrite,
    )
    _set_env(
        "PGSYNC_STUDIO_PORT",
        studio.get("port"),
        overwrite=overwrite,
    )
    _set_env(
        "PGSYNC_STUDIO_CONFIG_DATABASE_URL",
        studio.get("config_database_url"),
        overwrite=overwrite,
    )
    _set_env(
        "PGSYNC_STUDIO_SYNC_SERVICES_FROM_ENV",
        studio.get("sync_services"),
        overwrite=overwrite,
    )
    _set_env(
        "PGSYNC_STUDIO_SYNC_CONNECTIONS_FROM_ENV",
        studio.get("sync_connections"),
        overwrite=overwrite,
    )
    _set_env(
        "PGSYNC_STUDIO_JOB_LEASE_TTL_SECONDS",
        studio.get("job_lease_ttl_seconds"),
        overwrite=overwrite,
    )
    _set_env(
        "PGSYNC_STUDIO_JOB_LEASE_RENEW_INTERVAL_SECONDS",
        studio.get("job_lease_renew_interval_seconds"),
        overwrite=overwrite,
    )
    _set_env(
        "PGSYNC_STUDIO_JOB_LEASE_KEY_PREFIX",
        studio.get("job_lease_key_prefix"),
        overwrite=overwrite,
    )
    _set_env(
        "PGSYNC_STUDIO_AUTO_DISCOVER_CONNECTIONS_TIME",
        studio.get("auto_discover_connections_time"),
        overwrite=overwrite,
    )
    _set_env(
        "PGSYNC_STUDIO_AUTO_DISCOVER_CONNECTIONS_EXCLUDE_DATABASES",
        studio.get("auto_discover_connections_exclude_databases"),
        overwrite=overwrite,
    )

    read_replica = studio.get("read_replica")
    if not isinstance(read_replica, dict):
        return
    _set_env(
        "PGSYNC_STUDIO_READ_REPLICA_ENABLED",
        read_replica.get("enabled"),
        overwrite=overwrite,
    )
    _set_env(
        "PGSYNC_STUDIO_READ_REPLICA_HOST",
        read_replica.get("host"),
        overwrite=overwrite,
    )
    _set_env(
        "PGSYNC_STUDIO_READ_REPLICA_PORT",
        read_replica.get("port"),
        overwrite=overwrite,
    )
    _set_env(
        "PGSYNC_STUDIO_READ_REPLICA_CONNECTION_IDS",
        read_replica.get("connection_ids"),
        overwrite=overwrite,
    )
    _set_env(
        "PGSYNC_STUDIO_READ_REPLICA_WAIT_TIMEOUT_MS",
        read_replica.get("wait_timeout_ms"),
        overwrite=overwrite,
    )
    _set_env(
        "PGSYNC_STUDIO_READ_REPLICA_POLL_INTERVAL_MS",
        read_replica.get("poll_interval_ms"),
        overwrite=overwrite,
    )
    _set_env(
        "PGSYNC_STUDIO_READ_REPLICA_COOLDOWN_MS",
        read_replica.get("cooldown_ms"),
        overwrite=overwrite,
    )
    _set_env(
        "PGSYNC_STUDIO_READ_REPLICA_FALLBACK_TO_PRIMARY",
        read_replica.get("fallback_to_primary"),
        overwrite=overwrite,
    )


def _apply_services_config(
    root: dict,
    *,
    overwrite: bool,
) -> None:
    services = root.get("services")
    if not isinstance(services, dict):
        return

    search = services.get("search")
    if isinstance(search, dict):
        _set_env("ELASTICSEARCH_URL", search.get("url"), overwrite=overwrite)
        _set_env(
            "ELASTICSEARCH_USERNAME",
            search.get("username"),
            overwrite=overwrite,
        )
        _set_env(
            "ELASTICSEARCH_PASSWORD",
            search.get("password"),
            overwrite=overwrite,
        )
        _set_env(
            "ELASTICSEARCH_VERIFY_CERTS",
            search.get("verify_certs"),
            overwrite=overwrite,
        )

    redis = services.get("redis")
    if isinstance(redis, dict):
        _set_env("REDIS_HOST", redis.get("host"), overwrite=overwrite)
        _set_env("REDIS_PORT", redis.get("port"), overwrite=overwrite)
        _set_env("REDIS_DB", redis.get("db"), overwrite=overwrite)
        _set_env("REDIS_USER", redis.get("username"), overwrite=overwrite)
        _set_env("REDIS_PASSWORD", redis.get("password"), overwrite=overwrite)


def _apply_connections_config(
    root: dict,
    *,
    overwrite: bool,
) -> None:
    connections = root.get("connections")
    if not isinstance(connections, list):
        return
    _set_env(
        "PGSYNC_STUDIO_CONNECTIONS_JSON",
        json.dumps(connections, ensure_ascii=False),
        overwrite=overwrite,
    )
    if connections and "PGSYNC_STUDIO_SYNC_CONNECTIONS_FROM_ENV" not in os.environ:
        os.environ["PGSYNC_STUDIO_SYNC_CONNECTIONS_FROM_ENV"] = "true"


def apply_nacos_config(payload: dict, *, overwrite: bool = True) -> None:
    """Apply a decoded Nacos payload to process environment variables."""

    root: dict = _config_root(payload)
    _apply_studio_config(root, overwrite=overwrite)
    _apply_services_config(root, overwrite=overwrite)
    _apply_connections_config(root, overwrite=overwrite)


def _ensure_dev_config_path() -> None:
    if os.environ.get("PGSYNC_STUDIO_CONFIG"):
        return
    os.environ["PGSYNC_STUDIO_CONFIG"] = os.path.join(
        os.getcwd(),
        "runtime",
        "studio",
        "config.dev.json",
    )


def bootstrap_from_nacos() -> bool:
    """Load Nacos config before importing modules that read settings."""

    profile: str = (
        os.environ.get("PGSYNC_STUDIO_PROFILE")
        or os.environ.get("APP_ENV")
        or "dev"
    ).strip().lower()
    if profile == "dev":
        _ensure_dev_config_path()
        return False

    enabled: bool = _env_flag("NACOS_ENABLED", default=profile != "dev")
    if not enabled:
        return False

    server_addr: str = os.environ.get("NACOS_SERVER_ADDR", "")
    data_id: str = _data_id_from_env(profile)
    group: str = (os.environ.get("NACOS_GROUP") or "DEFAULT_GROUP").strip()
    namespace: str = (
        os.environ.get("NACOS_NAMESPACE")
        or os.environ.get("NACOS_TENANT")
        or ""
    )
    username: str = os.environ.get("NACOS_USERNAME", "")
    password: str = os.environ.get("NACOS_PASSWORD", "")
    overwrite: bool = _env_flag("PGSYNC_STUDIO_NACOS_OVERWRITE_ENV", default=True)
    required: bool = _env_flag("PGSYNC_STUDIO_NACOS_REQUIRED", default=True)

    try:
        payload: dict = fetch_nacos_payload_from_env()
        apply_nacos_config(payload, overwrite=overwrite)
        print(
            f"Loaded PGSync Studio config from Nacos dataId={data_id} "
            f"group={group} namespace={namespace or '-'}",
            file=sys.stderr,
        )
        return True
    except Exception as exc:
        if required:
            raise RuntimeError(f"Failed to load Nacos config: {exc}") from exc
        print(f"Skipping Nacos config: {exc}", file=sys.stderr)
        return False


def fetch_nacos_payload_from_env() -> dict:
    """Fetch and decode the configured Nacos payload without applying it."""

    profile: str = (
        os.environ.get("PGSYNC_STUDIO_PROFILE")
        or os.environ.get("APP_ENV")
        or "dev"
    ).strip().lower()
    if profile == "dev":
        return {}

    enabled: bool = _env_flag("NACOS_ENABLED", default=profile != "dev")
    if not enabled:
        return {}

    server_addr: str = os.environ.get("NACOS_SERVER_ADDR", "")
    data_id: str = _data_id_from_env(profile)
    group: str = (os.environ.get("NACOS_GROUP") or "DEFAULT_GROUP").strip()
    namespace: str = (
        os.environ.get("NACOS_NAMESPACE")
        or os.environ.get("NACOS_TENANT")
        or ""
    )
    username: str = os.environ.get("NACOS_USERNAME", "")
    password: str = os.environ.get("NACOS_PASSWORD", "")
    fmt: str = (os.environ.get("PGSYNC_STUDIO_NACOS_FORMAT") or "").strip()
    timeout: float = float(
        os.environ.get("PGSYNC_STUDIO_NACOS_TIMEOUT_SECONDS") or "10"
    )

    last_error: Exception | None = None
    for base_url in _server_urls(server_addr):
        try:
            access_token: str = _login(
                base_url,
                username=username,
                password=password,
                timeout=timeout,
            )
            raw: str = _fetch_config(
                base_url,
                data_id=data_id,
                group=group,
                namespace=namespace,
                access_token=access_token,
                timeout=timeout,
            )
            return _parse_config(raw, data_id=data_id, fmt=fmt)
        except Exception as exc:
            last_error = exc
    if last_error:
        raise last_error
    return {}
