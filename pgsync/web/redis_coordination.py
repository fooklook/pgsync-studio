"""Redis-based coordination helpers for PGSync Studio."""

from __future__ import annotations

import json
import os
import typing as t
import uuid
from urllib.parse import quote_plus

from redis import Redis

from .config_store import utcnow
from .credentials import resolve_env_placeholders

DEFAULT_LEASE_TTL_SECONDS = 300
DEFAULT_LEASE_RENEW_INTERVAL_SECONDS = 60
DEFAULT_LEASE_KEY_PREFIX = "pgsync:studio:job-lock"

JOB_LEASE_TTL_ENV = "PGSYNC_STUDIO_JOB_LEASE_TTL_SECONDS"
JOB_LEASE_RENEW_INTERVAL_ENV = "PGSYNC_STUDIO_JOB_LEASE_RENEW_INTERVAL_SECONDS"
JOB_LEASE_KEY_PREFIX_ENV = "PGSYNC_STUDIO_JOB_LEASE_KEY_PREFIX"

RELEASE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
end
return 0
"""

RENEW_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('expire', KEYS[1], ARGV[2])
end
return 0
"""


def _env_positive_int(name: str, default: int) -> int:
    raw: str = str(os.environ.get(name, "")).strip()
    if not raw:
        return int(default)
    try:
        value = int(raw)
    except ValueError:
        return int(default)
    return max(1, value)


def resolve_job_lease_ttl_seconds() -> int:
    return _env_positive_int(JOB_LEASE_TTL_ENV, DEFAULT_LEASE_TTL_SECONDS)


def resolve_job_lease_renew_interval_seconds(
    ttl_seconds: t.Optional[int] = None,
) -> int:
    resolved_ttl: int = int(ttl_seconds or resolve_job_lease_ttl_seconds())
    renew_interval: int = _env_positive_int(
        JOB_LEASE_RENEW_INTERVAL_ENV,
        DEFAULT_LEASE_RENEW_INTERVAL_SECONDS,
    )
    if renew_interval >= resolved_ttl:
        return max(1, resolved_ttl - 1)
    return renew_interval


def resolve_job_lease_key_prefix() -> str:
    raw: str = str(os.environ.get(JOB_LEASE_KEY_PREFIX_ENV, "")).strip()
    if not raw:
        return DEFAULT_LEASE_KEY_PREFIX
    return raw.rstrip(":") or DEFAULT_LEASE_KEY_PREFIX


class NoopJobLease:
    """Fallback lease used when Redis is not configured."""

    def __init__(self) -> None:
        self.ttl_seconds: int = resolve_job_lease_ttl_seconds()
        self.renew_interval_seconds: int = (
            resolve_job_lease_renew_interval_seconds(self.ttl_seconds)
        )
        self.token: str = ""

    def acquire(self) -> bool:
        return True

    def renew(self) -> bool:
        return True

    def is_owned(self) -> bool:
        return True

    def release(self) -> bool:
        return True

    def describe_holder(self) -> dict:
        return {}


class RedisJobLease:
    """A Redis-backed lease for coordinating job execution."""

    def __init__(
        self,
        client: Redis,
        *,
        job_id: str,
        owner_instance_id: str,
        ttl_seconds: t.Optional[int] = None,
        renew_interval_seconds: t.Optional[int] = None,
    ) -> None:
        self.client = client
        self.job_id = str(job_id)
        self.owner_instance_id = owner_instance_id
        self.ttl_seconds = int(ttl_seconds or resolve_job_lease_ttl_seconds())
        self.renew_interval_seconds = int(
            renew_interval_seconds
            or resolve_job_lease_renew_interval_seconds(self.ttl_seconds)
        )
        self.token = uuid.uuid4().hex
        self.key_prefix = resolve_job_lease_key_prefix()
        self.key = f"{self.key_prefix}:{self.job_id}"
        self.value = json.dumps(
            {
                "token": self.token,
                "job_id": self.job_id,
                "owner_instance_id": self.owner_instance_id,
                "acquired_at": utcnow(),
                "pid": os.getpid(),
            },
            ensure_ascii=False,
            sort_keys=True,
        )

    @classmethod
    def from_services(
        cls,
        services: dict,
        *,
        job_id: str,
        owner_instance_id: str,
    ) -> t.Union["RedisJobLease", NoopJobLease]:
        services = resolve_env_placeholders(services)
        redis_cfg: dict = services.get("redis", {})
        if not redis_cfg.get("host"):
            return NoopJobLease()
        username: str = redis_cfg.get("username") or ""
        password: str = redis_cfg.get("password") or ""
        auth: str = ""
        if username and password:
            auth = f"{quote_plus(username)}:{quote_plus(password)}@"
        elif password:
            auth = f":{quote_plus(password)}@"
        redis_url: str = (
            "redis://"
            f"{auth}{redis_cfg.get('host')}:{int(redis_cfg.get('port') or 6379)}/"
            f"{int(redis_cfg.get('db') or 0)}"
        )
        client = Redis.from_url(
            redis_url,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
            decode_responses=True,
        )
        client.ping()
        return cls(
            client,
            job_id=job_id,
            owner_instance_id=owner_instance_id,
        )

    def acquire(self) -> bool:
        return bool(
            self.client.set(
                self.key,
                self.value,
                nx=True,
                ex=self.ttl_seconds,
            )
        )

    def renew(self) -> bool:
        return bool(
            self.client.eval(
                RENEW_SCRIPT,
                1,
                self.key,
                self.value,
                str(self.ttl_seconds),
            )
        )

    def is_owned(self) -> bool:
        return self.client.get(self.key) == self.value

    def release(self) -> bool:
        return bool(
            self.client.eval(
                RELEASE_SCRIPT,
                1,
                self.key,
                self.value,
            )
        )

    def describe_holder(self) -> dict:
        raw: t.Optional[str] = self.client.get(self.key)
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {"raw": raw}
