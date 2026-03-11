"""
Pure-Python Bloom filter for IP blacklisting.
Stored in Redis as a single binary blob at key 'bloom:ips'.

Falls back to a Redis SET (bloom:ips:fallback) if the filter
has not been initialised yet.

Environment variables:
    REDIS_HOST  (default: redis)
    REDIS_PORT  (default: 6379)
"""

import logging
import math
import os
from typing import TYPE_CHECKING

import mmh3
from bitarray import bitarray

import redis as redis_lib

log = logging.getLogger(__name__)

BLOOM_KEY          = "bloom:ips"
FALLBACK_SET_KEY   = "bloom:ips:fallback"
BLOOM_CAPACITY     = 10_000_000
BLOOM_ERROR_RATE   = 0.01


def _optimal_params(capacity: int, error_rate: float) -> tuple[int, int]:
    """Return (bit_array_size, num_hash_functions) for the given capacity and error rate."""
    m = -capacity * math.log(error_rate) / (math.log(2) ** 2)
    k = (m / capacity) * math.log(2)
    return int(math.ceil(m)), int(math.ceil(k))


class BloomFilter:
    """
    Bloom filter backed by Redis binary string storage.

    Usage:
        bf = BloomFilter(redis_client)
        bf.load_from_redis()   # call on startup
        bf.add("192.168.1.1")
        bf.contains("192.168.1.1")  # True
        bf.save_to_redis()
    """

    def __init__(self, redis_client: redis_lib.Redis) -> None:
        self._redis = redis_client
        self._size, self._num_hashes = _optimal_params(BLOOM_CAPACITY, BLOOM_ERROR_RATE)
        self._bits: bitarray | None = None
        self._initialized = False

    # ------------------------------------------------------------------
    # Redis persistence
    # ------------------------------------------------------------------

    def load_from_redis(self) -> bool:
        """
        Load the Bloom filter bit array from Redis.
        Returns True if loaded successfully, False if key doesn't exist yet.
        """
        raw: bytes | None = self._redis.get(BLOOM_KEY)
        if raw is None:
            log.info(
                "Bloom filter key '%s' not found in Redis — filter not initialised",
                BLOOM_KEY,
            )
            return False

        self._bits = bitarray()
        self._bits.frombytes(raw)

        # Trim or pad to exact size (protects against partial writes)
        if len(self._bits) > self._size:
            self._bits = self._bits[: self._size]
        elif len(self._bits) < self._size:
            self._bits.extend([False] * (self._size - len(self._bits)))

        self._initialized = True
        log.info("Bloom filter loaded from Redis (%d bits, %d hash fns)", self._size, self._num_hashes)
        return True

    def save_to_redis(self) -> None:
        """Persist the current bit array to Redis."""
        if self._bits is None:
            raise RuntimeError("Bloom filter has not been initialised; call add() first")
        self._redis.set(BLOOM_KEY, self._bits.tobytes())
        log.info("Bloom filter saved to Redis (%d bits)", self._size)

    def _ensure_initialised(self) -> None:
        if self._bits is None:
            self._bits = bitarray(self._size)
            self._bits.setall(False)
            self._initialized = True

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def _hash_positions(self, item: str) -> list[int]:
        return [
            mmh3.hash(item, seed, signed=False) % self._size
            for seed in range(self._num_hashes)
        ]

    def add(self, ip: str) -> None:
        self._ensure_initialised()
        for pos in self._hash_positions(ip):
            self._bits[pos] = True

    def contains(self, ip: str) -> bool:
        """
        Returns True if the IP *might* be in the set.
        False guarantees the IP is definitely NOT in the set.

        Falls back to Redis SET lookup if the filter is not initialised.
        """
        if not self._initialized or self._bits is None:
            # Fallback: check the Redis SET directly
            result = self._redis.sismember(FALLBACK_SET_KEY, ip)
            return bool(result)

        return all(self._bits[pos] for pos in self._hash_positions(ip))

    # ------------------------------------------------------------------
    # Fallback SET (used before bloom filter is initialised)
    # ------------------------------------------------------------------

    def add_to_fallback(self, ip: str) -> None:
        """Add an IP to the fallback Redis SET (used during initialisation)."""
        self._redis.sadd(FALLBACK_SET_KEY, ip)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def get_bloom_filter() -> BloomFilter:
    """Return a BloomFilter connected to the module-level Redis pool."""
    from consumers.redis_client import _get_client
    client = _get_client()
    bf = BloomFilter(client)
    if not bf.load_from_redis():
        log.warning(
            "Bloom filter not in Redis — IP lookups will use fallback SET until seeded"
        )
    return bf