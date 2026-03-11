"""
Seed script — Redis blacklists.
Adds 1000 fake card IDs to the card blacklist and 10000 fake IPs to the Bloom filter.
Safe to re-run (idempotent).

Usage:
    REDIS_HOST=localhost python3 db/seeds/seed_blacklist.py
"""

import logging
import os
import sys
import time

# Allow running from project root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import redis

from consumers.bloom_filter import BloomFilter

logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
)
log = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

NUM_BLACKLISTED_CARDS = 1_000
NUM_BLACKLISTED_IPS   = 10_000


def seed_card_blacklist(client: redis.Redis) -> int:
    card_ids = [f"CARD_{n:07d}" for n in range(NUM_BLACKLISTED_CARDS)]
    added = client.sadd("blacklist:cards", *card_ids)
    log.info("Card blacklist: %d new cards added (%d already existed)", added, NUM_BLACKLISTED_CARDS - added)
    return added


def seed_ip_bloom_filter(client: redis.Redis) -> int:
    bf = BloomFilter(client)
    # Load existing filter if present; otherwise start fresh
    bf.load_from_redis()

    ips = [f"192.168.{i // 256}.{i % 256}" for i in range(NUM_BLACKLISTED_IPS)]
    for ip in ips:
        bf.add(ip)
        bf.add_to_fallback(ip)  # keep fallback SET in sync

    bf.save_to_redis()
    log.info("Bloom filter: %d IPs seeded and saved to Redis", NUM_BLACKLISTED_IPS)
    return NUM_BLACKLISTED_IPS


def main() -> None:
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    try:
        client.ping()
    except redis.ConnectionError as exc:
        log.error("Cannot connect to Redis at %s:%d — %s", REDIS_HOST, REDIS_PORT, exc)
        sys.exit(1)

    log.info("Connected to Redis at %s:%d", REDIS_HOST, REDIS_PORT)

    seed_card_blacklist(client)
    seed_ip_bloom_filter(client)

    # Report Redis memory after seeding
    info = client.info("memory")
    log.info(
        "Seeding complete — Redis memory used: %s",
        info.get("used_memory_human", "unknown"),
    )


if __name__ == "__main__":
    main()