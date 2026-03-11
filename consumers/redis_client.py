"""
Redis feature store for the fraud detection enrichment consumer.
All enrichment lookups for a single transaction are pipelined into one round-trip.

Environment variables:
    REDIS_HOST  (default: redis)
    REDIS_PORT  (default: 6379)
"""

import logging
import math
import os
import time
from typing import Any

import pybreaker
import redis

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}',
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Connection pool — module-level singleton, shared across all threads
# ---------------------------------------------------------------------------

_pool: redis.ConnectionPool | None = None


def _get_pool() -> redis.ConnectionPool:
    global _pool
    if _pool is None:
        _pool = redis.ConnectionPool(
            host=os.getenv("REDIS_HOST", "redis"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            max_connections=50,
            decode_responses=True,
        )
        log.info("Redis connection pool created (max_connections=50)")
    return _pool


def _get_client() -> redis.Redis:
    return redis.Redis(connection_pool=_get_pool())


# ---------------------------------------------------------------------------
# Defaults used when Redis is unavailable or card has no history
# ---------------------------------------------------------------------------

DEFAULT_AVG_AMOUNT   = 85.0
DEFAULT_STDDEV_AMOUNT = 42.0

# ---------------------------------------------------------------------------
# Circuit breaker listener — logs state transitions
# ---------------------------------------------------------------------------

class _BreakerListener(pybreaker.CircuitBreakerListener):
    def state_change(self, cb: pybreaker.CircuitBreaker, old_state, new_state) -> None:
        if new_state.name == "open":
            log.warning(
                "Redis circuit breaker OPENED after %d failures — "
                "enrichment will use degraded defaults for %ds",
                cb.fail_counter,
                cb.reset_timeout,
            )
        elif new_state.name == "closed":
            log.info("Redis circuit breaker CLOSED — normal enrichment resumed")


# ---------------------------------------------------------------------------
# Circuit breaker — wraps all Redis calls
# ---------------------------------------------------------------------------

_breaker = pybreaker.CircuitBreaker(
    fail_max=5,
    reset_timeout=30,
    listeners=[_BreakerListener()],
    name="redis",
)

# ---------------------------------------------------------------------------
# RedisFeatureStore
# ---------------------------------------------------------------------------

class RedisFeatureStore:
    """
    All Redis data access for the enrichment consumer.

    Key patterns:
        velocity:{card_id}   — Sorted set: score=timestamp_ms, member=txn_id
        stats:{card_id}      — Hash: avg, std, count_30d
        blacklist:cards      — Set of blacklisted card IDs
    """

    # ------------------------------------------------------------------
    # Velocity tracking
    # ------------------------------------------------------------------

    def add_transaction(
        self, card_id: str, txn_id: str, timestamp_ms: int
    ) -> None:
        """
        Add a transaction to the velocity sorted set and evict entries
        older than 5 minutes. Runs as a single pipeline round-trip.
        """
        key = f"velocity:{card_id}"
        cutoff_ms = timestamp_ms - 5 * 60 * 1000  # 5 minutes ago

        @_breaker
        def _run() -> None:
            client = _get_client()
            pipe = client.pipeline(transaction=False)
            pipe.zadd(key, {txn_id: timestamp_ms})
            pipe.zremrangebyscore(key, "-inf", cutoff_ms)
            pipe.expire(key, 600)  # 10-minute TTL
            pipe.execute()

        try:
            _run()
        except pybreaker.CircuitBreakerError:
            log.warning("add_transaction skipped — circuit is open (card=%s)", card_id)
        except redis.RedisError as exc:
            log.error("add_transaction failed: %s (card=%s)", exc, card_id)
            raise

    def get_velocity(self, card_id: str, window_seconds: int) -> int:
        """
        Return the number of transactions for a card in the last
        `window_seconds` seconds. Returns 0 if the key doesn't exist.
        """
        key = f"velocity:{card_id}"
        now_ms = int(time.time() * 1000)
        min_score = now_ms - window_seconds * 1000

        @_breaker
        def _run() -> int:
            return _get_client().zcount(key, min_score, "+inf")

        try:
            return int(_run())
        except pybreaker.CircuitBreakerError:
            return 0
        except redis.RedisError as exc:
            log.error("get_velocity failed: %s (card=%s)", exc, card_id)
            return 0

    # ------------------------------------------------------------------
    # Card statistics cache
    # ------------------------------------------------------------------

    def get_card_stats(self, card_id: str) -> dict[str, Any] | None:
        """
        Return card stats hash or None if the key doesn't exist
        (caller should fall back to PostgreSQL).
        """
        key = f"stats:{card_id}"

        @_breaker
        def _run() -> dict[str, Any] | None:
            raw = _get_client().hgetall(key)
            if not raw:
                return None
            return {
                "avg":        float(raw["avg"]),
                "std":        float(raw["std"]),
                "count_30d":  int(raw["count_30d"]),
            }

        try:
            return _run()
        except pybreaker.CircuitBreakerError:
            return None
        except redis.RedisError as exc:
            log.error("get_card_stats failed: %s (card=%s)", exc, card_id)
            return None

    def set_card_stats(
        self, card_id: str, avg: float, std: float, count_30d: int
    ) -> None:
        """Write card stats and set a 1-hour TTL."""
        key = f"stats:{card_id}"

        @_breaker
        def _run() -> None:
            client = _get_client()
            pipe = client.pipeline(transaction=False)
            pipe.hset(key, mapping={"avg": avg, "std": std, "count_30d": count_30d})
            pipe.expire(key, 3600)
            pipe.execute()

        try:
            _run()
        except pybreaker.CircuitBreakerError:
            log.warning("set_card_stats skipped — circuit open (card=%s)", card_id)
        except redis.RedisError as exc:
            log.error("set_card_stats failed: %s (card=%s)", exc, card_id)
            raise

    def bulk_set_card_stats(self, stats: list[dict[str, Any]]) -> int:
        """
        Bulk-write card stats in batches of 500.
        Each item must have keys: card_id, avg, std, count_30d.
        Returns the number of cards successfully written.
        """
        BATCH = 500
        written = 0

        @_breaker
        def _write_batch(batch: list[dict[str, Any]]) -> int:
            client = _get_client()
            pipe = client.pipeline(transaction=False)
            for item in batch:
                key = f"stats:{item['card_id']}"
                pipe.hset(
                    key,
                    mapping={
                        "avg":       item["avg"],
                        "std":       item["std"],
                        "count_30d": item["count_30d"],
                    },
                )
                pipe.expire(key, 3600)
            pipe.execute()
            return len(batch)

        for i in range(0, len(stats), BATCH):
            batch = stats[i : i + BATCH]
            try:
                written += _write_batch(batch)
            except pybreaker.CircuitBreakerError:
                log.warning("bulk_set_card_stats aborted at offset %d — circuit open", i)
                break
            except redis.RedisError as exc:
                log.error("bulk_set_card_stats batch failed at offset %d: %s", i, exc)
                raise

        return written

    # ------------------------------------------------------------------
    # Card blacklist
    # ------------------------------------------------------------------

    def is_card_blacklisted(self, card_id: str) -> bool:
        @_breaker
        def _run() -> bool:
            return bool(_get_client().sismember("blacklist:cards", card_id))

        try:
            return _run()
        except pybreaker.CircuitBreakerError:
            return False
        except redis.RedisError as exc:
            log.error("is_card_blacklisted failed: %s (card=%s)", exc, card_id)
            return False

    def add_to_blacklist(self, card_id: str) -> None:
        @_breaker
        def _run() -> None:
            _get_client().sadd("blacklist:cards", card_id)

        try:
            _run()
        except pybreaker.CircuitBreakerError:
            log.warning("add_to_blacklist skipped — circuit open (card=%s)", card_id)
        except redis.RedisError as exc:
            log.error("add_to_blacklist failed: %s (card=%s)", exc, card_id)
            raise

    def bulk_add_to_blacklist(self, card_ids: list[str]) -> int:
        """Add multiple card IDs in a single SADD call. Returns number added."""
        if not card_ids:
            return 0

        @_breaker
        def _run() -> int:
            return int(_get_client().sadd("blacklist:cards", *card_ids))

        try:
            return _run()
        except pybreaker.CircuitBreakerError:
            log.warning("bulk_add_to_blacklist skipped — circuit open")
            return 0
        except redis.RedisError as exc:
            log.error("bulk_add_to_blacklist failed: %s", exc)
            raise

    # ------------------------------------------------------------------
    # Enrichment — the hot path, called on every transaction event
    # ------------------------------------------------------------------

    def enrich_transaction(
        self, card_id: str, txn_id: str, timestamp_ms: int
    ) -> dict[str, Any]:
        """
        Fetch all enrichment features for a card in a SINGLE pipeline round-trip,
        then asynchronously update the velocity set.

        Returns a dict matching the EnrichedTransaction Avro schema fields
        that come from Redis.
        """
        now_ms = timestamp_ms
        min_1m  = now_ms - 60 * 1000
        min_5m  = now_ms - 300 * 1000
        vel_key = f"velocity:{card_id}"
        stats_key = f"stats:{card_id}"

        @_breaker
        def _pipeline_lookup() -> tuple[int, int, dict, bool]:
            client = _get_client()
            pipe = client.pipeline(transaction=False)
            pipe.zcount(vel_key, min_1m,  "+inf")   # 0: velocity_1m
            pipe.zcount(vel_key, min_5m,  "+inf")   # 1: velocity_5m
            pipe.hgetall(stats_key)                  # 2: card stats
            pipe.sismember("blacklist:cards", card_id)  # 3: blacklist
            results = pipe.execute()
            return results[0], results[1], results[2], results[3]

        try:
            vel_1m, vel_5m, raw_stats, blacklisted = _pipeline_lookup()

            # Parse stats — fall back to defaults if key was missing
            if raw_stats:
                avg_amount   = float(raw_stats.get("avg", DEFAULT_AVG_AMOUNT))
                stddev_amount = float(raw_stats.get("std", DEFAULT_STDDEV_AMOUNT))
            else:
                avg_amount   = DEFAULT_AVG_AMOUNT
                stddev_amount = DEFAULT_STDDEV_AMOUNT

            # Update velocity set (fire-and-forget — errors logged but not raised)
            try:
                self.add_transaction(card_id, txn_id, timestamp_ms)
            except Exception as exc:
                log.warning("add_transaction failed after enrich: %s", exc)

            return {
                "velocity_1m":          int(vel_1m),
                "velocity_5m":          int(vel_5m),
                "avg_amount_30d":       avg_amount,
                "stddev_amount_30d":    stddev_amount,
                "is_blacklisted_card":  bool(blacklisted),
                "is_blacklisted_ip":    False,   # set by bloom_filter.py
                "is_enrichment_degraded": False,
            }

        except pybreaker.CircuitBreakerError:
            log.warning(
                "enrich_transaction returning degraded result — circuit open (card=%s)",
                card_id,
            )
            return _degraded_result()
        except redis.RedisError as exc:
            log.error("enrich_transaction pipeline failed: %s (card=%s)", exc, card_id)
            return _degraded_result()


def _degraded_result() -> dict[str, Any]:
    return {
        "velocity_1m":          0,
        "velocity_5m":          0,
        "avg_amount_30d":       DEFAULT_AVG_AMOUNT,
        "stddev_amount_30d":    DEFAULT_STDDEV_AMOUNT,
        "is_blacklisted_card":  False,
        "is_blacklisted_ip":    False,
        "is_enrichment_degraded": True,
    }


# ---------------------------------------------------------------------------
# Module-level singleton — import and use directly
# ---------------------------------------------------------------------------

feature_store = RedisFeatureStore()