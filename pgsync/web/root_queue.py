"""Redis-backed root queue helpers for cross-database assembly rebuild."""

from __future__ import annotations

import json
import typing as t
import uuid
from urllib.parse import quote_plus

from redis import Redis

from .config_store import utcnow
from .credentials import resolve_env_placeholders

ENQUEUE_SCRIPT = """
if redis.call('SADD', KEYS[1], ARGV[1]) == 1 then
  redis.call('RPUSH', KEYS[2], ARGV[1])
  return 1
end
return 0
"""

CLAIM_SCRIPT = """
local claimed = {}
local limit = tonumber(ARGV[1])
for i = 1, limit do
  local item = redis.call('LPOP', KEYS[2])
  if not item then
    break
  end
  redis.call('SREM', KEYS[1], item)
  redis.call('RPUSH', KEYS[3], item)
  table.insert(claimed, item)
end
return claimed
"""

RECOVER_SCRIPT = """
if ARGV[1] and redis.call('HGET', KEYS[4], 'inflight_batch_id') ~= ARGV[1] then
  return {}
end
local moved = {}
local items = redis.call('LRANGE', KEYS[3], 0, -1)
redis.call('HDEL', KEYS[4], 'inflight_batch_id', 'inflight_claimed_at',
  'inflight_hot_count', 'inflight_backlog_count', 'inflight_size')
if #items == 0 then
  return moved
end
redis.call('DEL', KEYS[3])
for i = 1, #items do
  local item = items[i]
  if redis.call('SADD', KEYS[2], item) == 1 then
    redis.call('RPUSH', KEYS[1], item)
    table.insert(moved, item)
  end
end
return moved
"""

ACK_SCRIPT = """
if redis.call('HGET', KEYS[2], 'inflight_batch_id') ~= ARGV[1] then
  return 0
end
redis.call('DEL', KEYS[1])
redis.call('HDEL', KEYS[2], 'inflight_batch_id', 'inflight_claimed_at',
  'inflight_hot_count', 'inflight_backlog_count', 'inflight_size')
return 1
"""


class NoopRootQueue:
    """Fallback queue used when Redis is unavailable."""

    backend: str = "noop"

    def clear(self) -> None:
        return None

    def recover_inflight(self, *, to_hot: bool = True) -> int:
        return 0

    def enqueue_hot(
        self,
        root_keys: t.Iterable[tuple[t.Any, ...]],
    ) -> int:
        return 0

    def enqueue_backlog(
        self,
        root_keys: t.Iterable[tuple[t.Any, ...]],
    ) -> int:
        return 0

    def claim_batch(
        self,
        *,
        hot_limit: int,
        backlog_limit: int,
    ) -> dict[str, t.Any]:
        return {
            "batch_id": None,
            "root_keys": [],
            "hot_count": 0,
            "backlog_count": 0,
        }

    def ack_batch(self, batch_id: t.Optional[str]) -> None:
        return None

    def requeue_inflight(
        self,
        batch_id: t.Optional[str],
        *,
        to_hot: bool = True,
    ) -> int:
        return 0

    def queued_size(self) -> int:
        return 0

    def snapshot(self, sample_size: int = 10) -> dict[str, t.Any]:
        return {
            "backend": self.backend,
            "queued_size": 0,
            "hot_size": 0,
            "backlog_size": 0,
            "inflight_size": 0,
            "inflight_batch_id": None,
            "sample": [],
        }


class RedisRootQueue:
    """Persistent root queue with hot/backlog/inflight segments."""

    backend: str = "redis"

    def __init__(self, client: Redis, *, job_id: str) -> None:
        self.client = client
        self.job_id = str(job_id)
        key_prefix = f"pgsync:studio:root-queue:{self.job_id}"
        self.hot_key = f"{key_prefix}:hot"
        self.backlog_key = f"{key_prefix}:backlog"
        self.inflight_key = f"{key_prefix}:inflight"
        self.queued_set_key = f"{key_prefix}:queued"
        self.meta_key = f"{key_prefix}:meta"
        self._enqueue_script = self.client.register_script(ENQUEUE_SCRIPT)
        self._claim_script = self.client.register_script(CLAIM_SCRIPT)
        self._recover_script = self.client.register_script(RECOVER_SCRIPT)
        self._ack_script = self.client.register_script(ACK_SCRIPT)

    @classmethod
    def from_services(
        cls,
        services: dict,
        *,
        job_id: str,
    ) -> t.Union["RedisRootQueue", NoopRootQueue]:
        services = resolve_env_placeholders(services)
        redis_cfg: dict = services.get("redis", {})
        if not redis_cfg.get("host"):
            return NoopRootQueue()
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
        return cls(client, job_id=job_id)

    def _serialize(self, root_key: tuple[t.Any, ...]) -> str:
        return json.dumps(
            list(root_key), ensure_ascii=False, separators=(",", ":")
        )

    def _deserialize(self, payload: str) -> tuple[t.Any, ...]:
        parsed: t.Any = json.loads(payload)
        if isinstance(parsed, list):
            return tuple(parsed)
        if isinstance(parsed, tuple):
            return tuple(parsed)
        return (parsed,)

    def _enqueue(
        self,
        target_key: str,
        root_keys: t.Iterable[tuple[t.Any, ...]],
    ) -> int:
        added: int = 0
        for root_key in root_keys:
            normalized: tuple[t.Any, ...] = tuple(root_key)
            if not normalized or not any(
                value is not None for value in normalized
            ):
                continue
            if int(
                self._enqueue_script(
                    keys=[self.queued_set_key, target_key],
                    args=[self._serialize(normalized)],
                )
                or 0
            ):
                added += 1
        return added

    def clear(self) -> None:
        self.client.delete(
            self.hot_key,
            self.backlog_key,
            self.inflight_key,
            self.queued_set_key,
            self.meta_key,
        )

    def recover_inflight(self, *, to_hot: bool = True) -> int:
        return self._recover_inflight(to_hot=to_hot)

    def _recover_inflight(
        self, *, to_hot: bool, batch_id: t.Optional[str] = None
    ) -> int:
        target_key: str = self.hot_key if to_hot else self.backlog_key
        moved: list[str] = list(
            self._recover_script(
                keys=[
                    target_key,
                    self.queued_set_key,
                    self.inflight_key,
                    self.meta_key,
                ],
                args=[batch_id] if batch_id else [],
            )
            or []
        )
        return len(moved)

    def enqueue_hot(
        self,
        root_keys: t.Iterable[tuple[t.Any, ...]],
    ) -> int:
        return self._enqueue(self.hot_key, root_keys)

    def enqueue_backlog(
        self,
        root_keys: t.Iterable[tuple[t.Any, ...]],
    ) -> int:
        return self._enqueue(self.backlog_key, root_keys)

    def _claim_from(self, source_key: str, limit: int) -> list[str]:
        if limit <= 0:
            return []
        claimed: t.Any = self._claim_script(
            keys=[self.queued_set_key, source_key, self.inflight_key],
            args=[int(limit)],
        )
        return list(claimed or [])

    def claim_batch(
        self,
        *,
        hot_limit: int,
        backlog_limit: int,
    ) -> dict[str, t.Any]:
        inflight_size: int = int(self.client.llen(self.inflight_key) or 0)
        if inflight_size:
            return {
                "batch_id": self.client.hget(
                    self.meta_key, "inflight_batch_id"
                ),
                "root_keys": [
                    self._deserialize(item)
                    for item in self.client.lrange(self.inflight_key, 0, -1)
                ],
                "hot_count": int(
                    self.client.hget(self.meta_key, "inflight_hot_count") or 0
                ),
                "backlog_count": int(
                    self.client.hget(self.meta_key, "inflight_backlog_count")
                    or 0
                ),
            }

        hot_claimed: list[str] = self._claim_from(self.hot_key, hot_limit)
        backlog_claimed: list[str] = self._claim_from(
            self.backlog_key,
            backlog_limit,
        )
        claimed: list[str] = [*hot_claimed, *backlog_claimed]
        if not claimed:
            return {
                "batch_id": None,
                "root_keys": [],
                "hot_count": 0,
                "backlog_count": 0,
            }

        batch_id: str = uuid.uuid4().hex
        self.client.hset(
            self.meta_key,
            mapping={
                "inflight_batch_id": batch_id,
                "inflight_claimed_at": utcnow(),
                "inflight_hot_count": len(hot_claimed),
                "inflight_backlog_count": len(backlog_claimed),
                "inflight_size": len(claimed),
            },
        )
        return {
            "batch_id": batch_id,
            "root_keys": [self._deserialize(item) for item in claimed],
            "hot_count": len(hot_claimed),
            "backlog_count": len(backlog_claimed),
        }

    def ack_batch(self, batch_id: t.Optional[str]) -> bool:
        if not batch_id:
            return False
        return bool(
            self._ack_script(
                keys=[self.inflight_key, self.meta_key],
                args=[batch_id],
            )
        )

    def requeue_inflight(
        self,
        batch_id: t.Optional[str],
        *,
        to_hot: bool = True,
    ) -> int:
        if not batch_id:
            return 0
        return self._recover_inflight(to_hot=to_hot, batch_id=batch_id)

    def queued_size(self) -> int:
        return int(self.client.llen(self.hot_key) or 0) + int(
            self.client.llen(self.backlog_key) or 0
        )

    def snapshot(self, sample_size: int = 10) -> dict[str, t.Any]:
        hot_size: int = int(self.client.llen(self.hot_key) or 0)
        backlog_size: int = int(self.client.llen(self.backlog_key) or 0)
        inflight_size: int = int(self.client.llen(self.inflight_key) or 0)
        remaining: int = max(0, int(sample_size))
        sample_raw: list[str] = []
        if remaining > 0:
            hot_sample: list[str] = list(
                self.client.lrange(self.hot_key, 0, remaining - 1) or []
            )
            sample_raw.extend(hot_sample)
            remaining -= len(hot_sample)
        if remaining > 0:
            backlog_sample: list[str] = list(
                self.client.lrange(self.backlog_key, 0, remaining - 1) or []
            )
            sample_raw.extend(backlog_sample)
        return {
            "backend": self.backend,
            "queued_size": hot_size + backlog_size,
            "hot_size": hot_size,
            "backlog_size": backlog_size,
            "inflight_size": inflight_size,
            "inflight_batch_id": self.client.hget(
                self.meta_key,
                "inflight_batch_id",
            ),
            "sample": [list(self._deserialize(item)) for item in sample_raw],
        }
