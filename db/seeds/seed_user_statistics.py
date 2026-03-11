"""
Seed script — Redis card statistics.
Generates realistic avg/std/count stats for 100,000 cards and bulk-loads them into Redis.

Usage:
    REDIS_HOST=localhost python3 db/seeds/seed_user_statistics.py
"""

import logging
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import numpy as np
import redis

from consumers.redis_client import RedisFeatureStore

logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
)
log = logging.getLogger(__name__)

REDIS_HOST  = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT  = int(os.getenv("REDIS_PORT", 6379))
NUM_CARDS   = 100_000
BATCH_SIZE  = 500
REPORT_EVERY = 10_000

rng = np.random.default_rng(seed=42)


def generate_stats(n: int) -> list[dict]:
    avgs      = rng.lognormal(mean=4.0, sigma=0.9, size=n).clip(2.0, 5000.0)
    stds      = avgs * rng.uniform(0.3, 0.8, size=n)
    counts    = rng.integers(5, 200, size=n)

    return [
        {
            "card_id":   f"CARD_{i:07d}",
            "avg":       round(float(avgs[i]), 4),
            "std":       round(float(stds[i]), 4),
            "count_30d": int(counts[i]),
        }
        for i in range(n)
    ]


def main() -> None:
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        client.ping()
    except redis.ConnectionError as exc:
        log.error("Cannot connect to Redis: %s", exc)
        sys.exit(1)

    log.info("Generating stats for %d cards...", NUM_CARDS)
    all_stats = generate_stats(NUM_CARDS)

    store = RedisFeatureStore()
    total_written = 0
    start = time.time()

    for offset in range(0, NUM_CARDS, REPORT_EVERY):
        chunk = all_stats[offset : offset + REPORT_EVERY]
        written = store.bulk_set_card_stats(chunk)
        total_written += written
        elapsed = time.time() - start
        log.info(
            "Progress: %d / %d cards written (%.1fs elapsed)",
            total_written, NUM_CARDS, elapsed,
        )

    elapsed = time.time() - start
    info = client.info("memory")

    log.info(
        "Done — %d cards written in %.2fs | Redis memory: %s",
        total_written,
        elapsed,
        info.get("used_memory_human", "unknown"),
    )


if __name__ == "__main__":
    main()