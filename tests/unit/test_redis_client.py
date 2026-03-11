"""
Unit tests for RedisFeatureStore.
Uses fakeredis — no real Redis required.

Run:
    python3 -m pytest tests/unit/test_redis_client.py -v
"""

import time
from unittest.mock import patch, MagicMock

import fakeredis
import pytest
import pybreaker
import redis as redis_lib

# ---------------------------------------------------------------------------
# Patch the connection pool before importing the module under test so that
# all Redis calls go to fakeredis instead of a real server.
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def fake_redis_pool(monkeypatch):
    """Replace the real Redis connection pool with a fakeredis instance."""
    server = fakeredis.FakeServer()
    fake_client = fakeredis.FakeRedis(server=server, decode_responses=True)

    import consumers.redis_client as rc

    # Reset module-level state between tests
    rc._pool = None

    monkeypatch.setattr(rc, "_get_client", lambda: fake_client)

    # Also reset the circuit breaker so each test starts with a closed circuit
    rc._breaker.close()

    yield fake_client


@pytest.fixture()
def store():
    from consumers.redis_client import RedisFeatureStore
    return RedisFeatureStore()


# ---------------------------------------------------------------------------
# Velocity tests
# ---------------------------------------------------------------------------

class TestVelocity:

    def test_get_velocity_empty(self, store):
        """Unknown card returns 0 — key does not exist."""
        assert store.get_velocity("CARD_UNKNOWN", 60) == 0

    def test_add_and_get_velocity(self, store):
        """Add 5 transactions within the window — velocity must be 5."""
        now_ms = int(time.time() * 1000)
        for i in range(5):
            store.add_transaction("CARD_0000001", f"TXN_{i}", now_ms + i)

        assert store.get_velocity("CARD_0000001", 60) == 5

    def test_velocity_window_eviction(self, store):
        """Transaction added 120 seconds ago must not appear in a 60s window."""
        now_ms   = int(time.time() * 1000)
        old_ms   = now_ms - 120_000  # 2 minutes ago

        store.add_transaction("CARD_0000002", "TXN_OLD", old_ms)
        # Now add a recent one so the key exists
        store.add_transaction("CARD_0000002", "TXN_NEW", now_ms)

        # The old entry should be evicted by ZREMRANGEBYSCORE inside add_transaction
        assert store.get_velocity("CARD_0000002", 60) == 1  # only TXN_NEW

    def test_velocity_5m_window(self, store):
        """Transactions within 5 minutes are counted; older ones are not."""
        now_ms = int(time.time() * 1000)
        store.add_transaction("CARD_0000003", "TXN_A", now_ms - 240_000)  # 4 min ago
        store.add_transaction("CARD_0000003", "TXN_B", now_ms - 60_000)   # 1 min ago
        store.add_transaction("CARD_0000003", "TXN_C", now_ms)

        assert store.get_velocity("CARD_0000003", 300) == 3
        assert store.get_velocity("CARD_0000003", 60)  == 1


# ---------------------------------------------------------------------------
# Blacklist tests
# ---------------------------------------------------------------------------

class TestBlacklist:

    def test_card_blacklist_miss(self, store):
        assert store.is_card_blacklisted("CARD_CLEAN") is False

    def test_card_blacklist_hit(self, store):
        store.add_to_blacklist("CARD_BAD")
        assert store.is_card_blacklisted("CARD_BAD") is True

    def test_bulk_add_to_blacklist(self, store):
        cards = [f"CARD_{i:07d}" for i in range(10)]
        added = store.bulk_add_to_blacklist(cards)
        assert added == 10
        for card in cards:
            assert store.is_card_blacklisted(card) is True

    def test_bulk_add_idempotent(self, store):
        store.add_to_blacklist("CARD_DUP")
        added = store.bulk_add_to_blacklist(["CARD_DUP"])
        assert added == 0  # already in set


# ---------------------------------------------------------------------------
# Card stats tests
# ---------------------------------------------------------------------------

class TestCardStats:

    def test_get_stats_missing(self, store):
        assert store.get_card_stats("CARD_NO_STATS") is None

    def test_set_and_get_stats(self, store):
        store.set_card_stats("CARD_0000010", avg=75.5, std=22.3, count_30d=45)
        stats = store.get_card_stats("CARD_0000010")
        assert stats is not None
        assert stats["avg"]        == pytest.approx(75.5)
        assert stats["std"]        == pytest.approx(22.3)
        assert stats["count_30d"]  == 45

    def test_bulk_set_stats(self, store):
        records = [
            {"card_id": f"CARD_{i:07d}", "avg": 50.0 + i, "std": 10.0, "count_30d": i + 1}
            for i in range(10)
        ]
        written = store.bulk_set_card_stats(records)
        assert written == 10
        stats = store.get_card_stats("CARD_0000005")
        assert stats["avg"] == pytest.approx(55.0)


# ---------------------------------------------------------------------------
# Enrichment pipeline test
# ---------------------------------------------------------------------------

class TestEnrichTransaction:

    def test_enrich_returns_correct_shape(self, store):
        """enrich_transaction must return a dict with all expected keys."""
        now_ms = int(time.time() * 1000)
        result = store.enrich_transaction("CARD_0000099", "TXN_XYZ", now_ms)

        assert set(result.keys()) == {
            "velocity_1m",
            "velocity_5m",
            "avg_amount_30d",
            "stddev_amount_30d",
            "is_blacklisted_card",
            "is_blacklisted_ip",
            "is_enrichment_degraded",
        }
        assert isinstance(result["velocity_1m"],       int)
        assert isinstance(result["velocity_5m"],       int)
        assert isinstance(result["avg_amount_30d"],    float)
        assert isinstance(result["stddev_amount_30d"], float)
        assert result["is_enrichment_degraded"] is False

    def test_enrich_uses_defaults_when_no_stats(self, store):
        """Cards with no stats in Redis should receive default avg/std."""
        from consumers.redis_client import DEFAULT_AVG_AMOUNT, DEFAULT_STDDEV_AMOUNT
        now_ms = int(time.time() * 1000)
        result = store.enrich_transaction("CARD_NOSTATS", "TXN_001", now_ms)
        assert result["avg_amount_30d"]    == DEFAULT_AVG_AMOUNT
        assert result["stddev_amount_30d"] == DEFAULT_STDDEV_AMOUNT

    def test_enrich_uses_real_stats(self, store):
        """Cards with stats in Redis should use those values."""
        store.set_card_stats("CARD_WITHSTATS", avg=120.0, std=30.0, count_30d=100)
        now_ms = int(time.time() * 1000)
        result = store.enrich_transaction("CARD_WITHSTATS", "TXN_002", now_ms)
        assert result["avg_amount_30d"]    == pytest.approx(120.0)
        assert result["stddev_amount_30d"] == pytest.approx(30.0)

    def test_enrich_detects_blacklisted_card(self, store):
        store.add_to_blacklist("CARD_FLAGGED")
        now_ms = int(time.time() * 1000)
        result = store.enrich_transaction("CARD_FLAGGED", "TXN_003", now_ms)
        assert result["is_blacklisted_card"] is True

    def test_enrich_increments_velocity(self, store):
        """Calling enrich_transaction twice should increment velocity_1m."""
        now_ms = int(time.time() * 1000)
        store.enrich_transaction("CARD_VEL", "TXN_A", now_ms)
        result = store.enrich_transaction("CARD_VEL", "TXN_B", now_ms + 100)
        # After two calls, velocity should be at least 1 (TXN_A was added by first call)
        assert result["velocity_1m"] >= 1


# ---------------------------------------------------------------------------
# Circuit breaker test
# ---------------------------------------------------------------------------

class TestCircuitBreaker:

    def test_circuit_breaker_opens_after_failures(self, monkeypatch):
        """
        Force 5 consecutive Redis errors; the circuit must open and
        enrich_transaction must return is_enrichment_degraded=True.
        """
        import consumers.redis_client as rc

        # Reset breaker to a known closed state
        rc._breaker.close()

        call_count = 0

        def failing_client():
            nonlocal call_count
            call_count += 1
            mock = MagicMock()
            mock.pipeline.return_value.__enter__ = MagicMock(return_value=mock)
            mock.pipeline.return_value.execute.side_effect = redis_lib.ConnectionError("forced failure")
            mock.pipeline.return_value.zcount.side_effect = redis_lib.ConnectionError("forced failure")
            # Make pipeline() return an object whose execute() raises
            pipe = MagicMock()
            pipe.execute.side_effect = redis_lib.ConnectionError("forced failure")
            mock.pipeline.return_value = pipe
            return mock

        monkeypatch.setattr(rc, "_get_client", failing_client)

        store = rc.RedisFeatureStore()
        now_ms = int(time.time() * 1000)

        results = []
        for i in range(7):
            result = store.enrich_transaction(f"CARD_{i}", f"TXN_{i}", now_ms)
            results.append(result)

        # At least the last results (after circuit opened) must be degraded
        degraded = [r for r in results if r["is_enrichment_degraded"]]
        assert len(degraded) >= 1, "Circuit breaker should have produced at least one degraded result"

    def test_degraded_result_has_correct_defaults(self, monkeypatch):
        """Degraded result must use default avg/std and flag is_enrichment_degraded."""
        import consumers.redis_client as rc
        from consumers.redis_client import DEFAULT_AVG_AMOUNT, DEFAULT_STDDEV_AMOUNT, _degraded_result

        result = _degraded_result()
        assert result["is_enrichment_degraded"]  is True
        assert result["avg_amount_30d"]          == DEFAULT_AVG_AMOUNT
        assert result["stddev_amount_30d"]        == DEFAULT_STDDEV_AMOUNT
        assert result["velocity_1m"]             == 0
        assert result["velocity_5m"]             == 0
        assert result["is_blacklisted_card"]     is False