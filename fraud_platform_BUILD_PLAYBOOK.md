# Real-Time Fraud Detection Platform
## Build Playbook & LLM Prompt Guide

> **15 Build Phases • Production-Grade Prompts • Step-by-Step Implementation**
>
> Use each prompt with Claude, GPT-4, or any capable LLM

---

## How to Use This Playbook

This guide breaks the fraud detection platform into **15 self-contained build phases**. Each phase can be given to an LLM as a single prompt. The prompts are written to be:

- **Self-contained** — they include all the context the LLM needs without requiring the LLM to have read anything else.
- **Prescriptive** — they specify exact library versions, file names, config values, and interfaces so the output integrates cleanly with adjacent phases.
- **Production-grade** — they ask for error handling, logging, health checks, and tests, not just happy-path code.
- **Anti-hallucination** — they include explicit constraints ("do not use X", "the function signature must be Y") to prevent the LLM from drifting.

### Recommended Workflow

1. Read the phase overview and prerequisites before running the prompt. Verify the listed dependencies are running.
2. Paste the prompt into your LLM of choice. Claude Sonnet, GPT-4o, and Gemini 1.5 Pro all work well.
3. Review the output. Check that file names, imports, and interfaces match what is listed in the **Outputs** box.
4. Place the files in the repo at the paths specified. Run the verification commands before moving to the next phase.
5. If the output is incomplete, use the follow-up prompt at the bottom of each phase to get the missing parts.

---

## Build Sequence Map

```
PHASE 1  → Environment & Docker Foundation
PHASE 2  → Schema Registry & Avro Schemas
PHASE 3  → Redis Feature Store
PHASE 4  → PostgreSQL Schema & Migrations
PHASE 5  → High-Throughput Producer
PHASE 6  → Enrichment Consumer
PHASE 7  → Spark Structured Streaming — Detection Engine
PHASE 8  → Analytics & Alert Consumers
PHASE 9  → FastAPI Backend
PHASE 10 → React Dashboard
PHASE 11 → Prometheus + Grafana Observability
PHASE 12 → Failure Testing & Chaos Engineering
PHASE 13 → End-to-End Integration Testing
PHASE 14 → Event Replay & Operational Runbooks
PHASE 15 → ML Extension — XGBoost + GPU Acceleration
```

---

## 🐳 PHASE 1 — Environment & Docker Foundation

### Overview

This phase creates the entire Docker Compose skeleton — all services defined, networked, and health-checked — before writing any application code. Every subsequent phase drops files into a repo structure that is already running. You will verify the environment works by checking that all containers reach a healthy state.

- **Prerequisites:** Docker Desktop (or Docker Engine) installed. WSL2 configured with 32GB RAM allocation. Node.js, Python 3.11+, and npm installed in WSL2.
- **Goal:** A fully running `docker-compose.yml` with all 13 services healthy, an init script that creates Kafka topics, and a repo directory skeleton.
- **Outputs:** `docker/docker-compose.yml` | `scripts/init_platform.sh` | `scripts/health_check.sh` | `.env` | Full directory tree scaffold

### Files This Phase Produces

```
fraud-detection-platform/
├── docker/
│   ├── docker-compose.yml
│   └── .env                    # All credentials + port vars
├── scripts/
│   ├── init_platform.sh        # Topic creation + seeding
│   └── health_check.sh         # Verify all services
├── producers/   consumers/   spark_jobs/   schemas/
├── api/   react-dashboard/   ml/   tests/   db/
└── README.md
```

### LLM Prompt

```
You are a senior DevOps and distributed systems engineer.
Build the complete Docker Compose foundation for a real-time fraud detection platform.

## Context
The platform runs entirely on a single developer machine:
  - OS: Windows + WSL2 / Linux
  - RAM: 40GB total (allocate 32GB to WSL2 via .wslconfig)
  - CPU: i7-class (12 logical cores available)
  - All Docker volumes MUST be stored inside the WSL2 filesystem,
    NOT on /mnt/c/... (cross-filesystem I/O is 10-20x slower)

## Services to define (with exact resource limits)
  kafka1, kafka2, kafka3:      2 CPU, 2GB RAM each  (KRaft mode, NO Zookeeper)
  schema-registry:             0.5 CPU, 512MB RAM
  spark-master:                1 CPU, 2GB RAM
  spark-worker-1, worker-2:    4 CPU, 6GB RAM each
  redis:                       0.5 CPU, 4GB RAM
  postgres:                    1 CPU, 4GB RAM
  prometheus:                  0.5 CPU, 1GB RAM
  grafana:                     0.5 CPU, 512MB RAM
  redis-exporter:              0.2 CPU, 128MB RAM
  postgres-exporter:           0.2 CPU, 128MB RAM

## Exact image versions to use
  confluentinc/cp-kafka:7.6.1
  confluentinc/cp-schema-registry:7.6.1
  bitnami/spark:3.4.2
  redis:7.2-alpine
  postgres:15.5-alpine
  prom/prometheus:v2.48.0
  grafana/grafana:10.2.2
  oliver006/redis_exporter:v1.55.0
  prometheuscommunity/postgres-exporter:v0.15.0

## Kafka KRaft configuration requirements
  - 3-broker cluster in KRaft mode (KAFKA_PROCESS_ROLES: broker,controller)
  - CLUSTER_ID must be a valid base64-encoded UUID (generate one)
  - CONTROLLER_QUORUM_VOTERS lists all 3 brokers
  - Each broker has a unique KAFKA_NODE_ID (1, 2, 3)
  - KAFKA_ADVERTISED_LISTENERS must use the container hostname
  - auto.create.topics.enable: 'false'
  - default.replication.factor: 3
  - min.insync.replicas: 2
  - log.retention.hours: 24
  - JMX port 9101 exposed for Prometheus scraping
  - Use an x-kafka-common YAML anchor to avoid repeating config

## Spark configuration requirements
  - spark-master exposes ports 8080 (UI) and 7077 (cluster)
  - Each worker: SPARK_WORKER_MEMORY=5G, SPARK_WORKER_CORES=4
  - Workers connect to spark://spark-master:7077
  - Mount a named volume 'spark-checkpoints' at /tmp/checkpoints on workers
  - Set SPARK_WORKER_OPTS with backpressure enabled

## Redis configuration requirements
  - maxmemory 3gb
  - maxmemory-policy allkeys-lru
  - appendonly no  (we don't need AOF persistence for this workload)
  - Expose port 6379

## PostgreSQL configuration requirements
  - DB: frauddb, user: fraud_user, password: fraud_pass
  - postgres command flags: shared_buffers=1GB, effective_cache_size=2GB,
    wal_buffers=64MB, max_connections=200, checkpoint_completion_target=0.9
  - Mount ./db/migrations as /docker-entrypoint-initdb.d (auto-runs on first start)
  - Named volume postgres-data

## Networking
  - Two networks: kafka-net (172.20.0.0/24) and data-net (172.21.0.0/24)
  - Kafka brokers and Spark are on kafka-net
  - Redis and PostgreSQL are on data-net
  - Spark workers are on BOTH networks (need Kafka + DB access)
  - Prometheus and Grafana are on BOTH networks

## Health checks (required on every service)
  - Kafka: kafka-broker-api-versions --bootstrap-server localhost:9092
  - Redis: redis-cli ping
  - PostgreSQL: pg_isready -U fraud_user -d frauddb
  - All health checks: interval 10s, timeout 5s, retries 10, start_period 30s

## depends_on with condition: service_healthy
  - schema-registry depends_on kafka1 (healthy)
  - spark-worker depends_on spark-master
  - api and producers depend_on kafka1 (healthy) and postgres (healthy)

## .env file
  Extract ALL credentials, ports, and cluster IDs into a .env file.
  docker-compose.yml must reference them via ${VAR_NAME}.
  Include: KAFKA_CLUSTER_ID, POSTGRES_PASSWORD, GRAFANA_ADMIN_PASSWORD,
  SCHEMA_REGISTRY_URL, all broker hostnames.

## scripts/init_platform.sh
  Write a bash script that:
  1. Waits for kafka1 to be healthy (poll with retries)
  2. Creates all 5 Kafka topics with correct partition/retention config:
     - transactions.raw:      12 partitions, RF 3, retention 43200000ms
     - transactions.enriched: 12 partitions, RF 3, retention 86400000ms
     - fraud.alerts:           3 partitions, RF 3, retention 604800000ms
     - fraud.scores:           6 partitions, RF 3, retention 21600000ms
     - dead-letter-queue:      1 partition,  RF 3, retention 2592000000ms
  3. Verifies topic creation with kafka-topics --describe
  4. Prints colored status output (green = success, red = failure)
  5. Uses --if-not-exists on all kafka-topics commands

## scripts/health_check.sh
  Write a bash script that checks every service and prints:
  [OK] / [FAIL] for: kafka1, kafka2, kafka3, schema-registry,
  spark-master, redis, postgres, prometheus, grafana
  Exit code 0 if all healthy, 1 if any fail.

## Directory scaffold
  Create empty placeholder files (touch) for every directory in this tree:
  producers/ consumers/ spark_jobs/ schemas/ api/routes/ api/websocket/
  react-dashboard/src/components/ react-dashboard/src/hooks/
  ml/ tests/unit/ tests/integration/ tests/load/ db/migrations/ db/seeds/
  docker/prometheus/ docker/grafana/provisioning/ scripts/ monitoring/

## README.md
  Write a clear README with:
  - Prerequisites section
  - Quick start (3 commands to get the platform running)
  - Service URL table (all ports)
  - WSL2 performance setup section with .wslconfig example
  - Troubleshooting section for the 5 most common startup errors

## Constraints
  - Do NOT use Zookeeper anywhere
  - Do NOT use host network mode
  - Do NOT expose any port that isn't needed for local development
  - All secrets must come from .env, never hardcoded in docker-compose.yml
  - Use named volumes (not bind mounts) for Kafka and PostgreSQL data
  - The docker-compose.yml must pass 'docker-compose config' validation

Output all files in clearly labeled code blocks.
After the files, provide a 'Verification' section with the exact commands
to run and what output to expect if everything is correct.
```

> ⚠️ **Watch out:** On WSL2, after running `docker-compose up -d`, wait 60–90 seconds before running `init_platform.sh`. KRaft leader election takes longer than a simple health check suggests.

---

## 📋 PHASE 2 — Schema Registry & Avro Schemas

### Overview

Define the three canonical Avro schemas (`TransactionEvent`, `EnrichedTransaction`, `FraudAlert`) and register them with the Schema Registry. Write a setup script that is idempotent — it can be re-run safely and will update schemas if they have changed. This schema layer is the contract between all producers and consumers and must be locked in before any application code is written.

- **Prerequisites:** Phase 1 complete. Schema Registry container healthy at `http://localhost:8081`. Python 3.11+ with pip available.
- **Goal:** Three Avro schemas registered in Schema Registry under correct subject names. A Python schema client module usable by producers and consumers. Schema evolution tested.
- **Outputs:** `schemas/transaction_event.avsc` | `schemas/enriched_transaction.avsc` | `schemas/fraud_alert.avsc` | `schemas/schema_registry_setup.py` | `schemas/client.py`

### LLM Prompt

```
You are a senior data engineer specializing in Kafka schema management and Apache Avro.
Build the complete schema layer for a real-time financial fraud detection platform.

## Schema Registry details
  URL: http://localhost:8081
  Compatibility mode: BACKWARD (consumers can read data from newer schema versions
  because all new fields have defaults)

## Required Avro schemas

### 1. TransactionEvent (subject: transactions.raw-value)
Fields:
  transaction_id: string (UUID v4, required)
  card_id:        string (partition key for Kafka, required)
  user_id:        string (required)
  amount:         double (USD, required)
  merchant_id:    string (required)
  merchant_name:  string (required)
  merchant_cat:   string (ISO 18245 MCC code, required)
  location:       record { lat: double, lon: double } (required)
  country:        string (ISO 3166-1 alpha-2, required)
  timestamp:      long   (epoch milliseconds, required)
  is_online:      boolean (required)
  device_id:      union [null, string] (nullable, default null)

### 2. EnrichedTransaction (subject: transactions.enriched-value)
All TransactionEvent fields PLUS:
  velocity_1m:          int    (tx count in last 60s for this card)
  velocity_5m:          int    (tx count in last 5 min)
  avg_amount_30d:       double (30-day rolling mean, default 85.0)
  stddev_amount_30d:    double (30-day std dev, default 42.0)
  is_blacklisted_card:  boolean (default false)
  is_blacklisted_ip:    boolean (default false)
  is_enrichment_degraded: boolean (true if Redis was unavailable, default false)
  enrichment_ts:        long   (epoch ms when enrichment was applied)

### 3. FraudAlert (subject: fraud.alerts-value)
Fields:
  alert_id:       string (UUID v4)
  txn_id:         string
  card_id:        string
  user_id:        string
  fraud_type:     enum { VELOCITY, DEVIATION, BLACKLIST_HIT, COMPOSITE }
  risk_score:     double (0.0 to 1.0)
  severity:       enum { LOW, MEDIUM, HIGH, CRITICAL }
  recommendation: string (Block | Challenge | Monitor | Allow)
  trigger_reason: string (human-readable explanation)
  window_start:   union [null, long] (default null, for velocity alerts)
  window_end:     union [null, long] (default null)
  triggered_at:   long (epoch ms)

## schemas/schema_registry_setup.py
Write a Python script that:
  1. Connects to Schema Registry at SCHEMA_REGISTRY_URL (env var)
  2. Reads each .avsc file from the schemas/ directory
  3. Checks if the schema already exists (GET /subjects/{subject}/versions/latest)
  4. If it exists and is unchanged, skips with a log message
  5. If it exists and has changed, checks BACKWARD compatibility before registering
     (POST /compatibility/subjects/{subject}/versions/latest)
  6. If compatibility check fails, raises a clear exception (do not proceed)
  7. Registers the schema (POST /subjects/{subject}/versions)
  8. Prints the registered schema ID for each schema
  9. Is fully idempotent — safe to re-run at any time
  10. Reads SCHEMA_REGISTRY_URL from environment, falls back to http://localhost:8081

## schemas/client.py
Write a Python module with these exports:
  get_avro_serializer(schema_name: str, registry_url: str) -> AvroSerializer
    Uses confluent_kafka.schema_registry.avro.AvroSerializer
    schema_name: 'TransactionEvent' | 'EnrichedTransaction' | 'FraudAlert'
    Caches serializer instances (do not re-create on every call)
  get_avro_deserializer(schema_name: str, registry_url: str) -> AvroDeserializer
    Uses confluent_kafka.schema_registry.avro.AvroDeserializer
    Caches deserializer instances
  class SchemaClient:
    __init__(self, registry_url: str)
    serialize(self, schema_name: str, data: dict, topic: str) -> bytes
    deserialize(self, schema_name: str, data: bytes, topic: str) -> dict

## requirements.txt for schemas/
  confluent-kafka[avro]==2.3.0
  requests==2.31.0
  python-dotenv==1.0.0

## Schema evolution test
Write a test file schemas/test_evolution.py that:
  1. Registers TransactionEvent v1
  2. Adds a new optional field 'network_type' (union [null, string], default null)
  3. Verifies the new schema is BACKWARD compatible
  4. Registers v2 and verifies it gets a new schema ID
  5. Verifies a v1 consumer can deserialize a v2 message (the new field is ignored)
  6. Verifies that removing a field raises a compatibility error (test the guard)

## Constraints
  - Use only the confluent-kafka library for Schema Registry interaction,
    not fastavro or PyAvro directly
  - All .avsc files must be valid JSON parseable by json.loads()
  - Subject naming convention: {topic_name}-value for all subjects
  - Include the 'namespace': 'com.fraudplatform.events' in all schemas
  - Do not hardcode Schema Registry URL anywhere; always read from env

Output all files as labeled code blocks.
End with a verification section showing how to confirm schemas are registered:
  curl http://localhost:8081/subjects
  curl http://localhost:8081/subjects/transactions.raw-value/versions/latest
```

---

## ⚡ PHASE 3 — Redis Feature Store

### Overview

Implement the Redis data access layer used by the enrichment consumer. This includes velocity sorted sets, per-card statistics hashes, card and IP blacklists, and a circuit breaker pattern for handling Redis unavailability gracefully. The goal is a clean, well-tested module that the enrichment consumer imports rather than duplicating Redis logic.

- **Prerequisites:** Phase 1 complete. Redis running at `localhost:6379`. Python 3.11+ with pip.
- **Goal:** A production-grade Redis client module with all data access patterns, TTL management, pipelining, and a circuit breaker. Seed scripts to populate initial data.
- **Outputs:** `consumers/redis_client.py` | `consumers/bloom_filter.py` | `db/seeds/seed_blacklist.py` | `db/seeds/seed_user_statistics.py` (Redis portion) | `tests/unit/test_redis_client.py`

### LLM Prompt

```
You are a senior backend engineer with deep Redis expertise.
Build the complete Redis feature store module for a real-time fraud detection platform.
This module is called by the Kafka enrichment consumer on every transaction event.
Latency is critical: each enrichment call must complete in under 2ms (Redis p99).

## Redis connection details
  host: redis (Docker container name), port: 6379
  Read from env: REDIS_HOST, REDIS_PORT
  Use redis-py 5.x with connection pooling (max_connections=50)

## consumers/redis_client.py — Main module
Implement the class RedisFeatureStore with these methods:

### Velocity tracking (Sorted Set per card)
  Key pattern: velocity:{card_id}
  Value:       sorted set with score=timestamp_ms, member=txn_id
  add_transaction(card_id: str, txn_id: str, timestamp_ms: int) -> None
    - ZADD velocity:{card_id} timestamp_ms txn_id
    - ZREMRANGEBYSCORE to evict entries older than 5 minutes
    - EXPIRE key 600 (10 minutes TTL)
    - Run as a pipeline (single round-trip)
  get_velocity(card_id: str, window_seconds: int) -> int
    - ZCOUNT velocity:{card_id} (now_ms - window_seconds*1000) +inf
    - window_seconds options: 60, 300 (1 min and 5 min)
    - Returns 0 if key does not exist

### Card statistics cache (Hash per card)
  Key pattern: stats:{card_id}
  Fields:      avg (float), std (float), count_30d (int)
  get_card_stats(card_id: str) -> dict | None
    - HGETALL stats:{card_id}
    - Returns None if key missing (triggers PostgreSQL fallback in consumer)
    - Returns {'avg': float, 'std': float, 'count_30d': int}
  set_card_stats(card_id: str, avg: float, std: float, count_30d: int) -> None
    - HSET stats:{card_id} avg X std Y count_30d Z
    - EXPIRE stats:{card_id} 3600 (1 hour TTL)
  bulk_set_card_stats(stats: list[dict]) -> int
    - Pipeline multiple HSET+EXPIRE calls (batch size 500)
    - Returns count of cards updated

### Card blacklist (Set)
  Key: blacklist:cards
  is_card_blacklisted(card_id: str) -> bool
    - SISMEMBER blacklist:cards card_id
  add_to_blacklist(card_id: str) -> None
    - SADD blacklist:cards card_id
  bulk_add_to_blacklist(card_ids: list[str]) -> int
    - SADD blacklist:cards *card_ids (single call)
    - Returns number added

### Enrichment batch method (most important — called on every event)
  enrich_transaction(card_id: str, txn_id: str, timestamp_ms: int) -> dict
    - Runs ALL lookups in a SINGLE pipeline round-trip:
      * ZCOUNT for velocity_1m (60s window)
      * ZCOUNT for velocity_5m (300s window)
      * HGETALL for card stats
      * SISMEMBER for blacklist check
    - After pipeline, call add_transaction() to update the velocity set
    - Returns:
      {
        'velocity_1m': int,
        'velocity_5m': int,
        'avg_amount_30d': float,      # from stats or default 85.0
        'stddev_amount_30d': float,   # from stats or default 42.0
        'is_blacklisted_card': bool,
        'is_blacklisted_ip': False,   # placeholder, set by bloom filter
        'is_enrichment_degraded': False
      }

### Circuit breaker
  Wrap the RedisFeatureStore with a circuit breaker:
  - Use the 'pybreaker' library (pybreaker==1.0.1)
  - Threshold: open circuit after 5 consecutive failures
  - Recovery timeout: 30 seconds (try again after 30s of open state)
  - When circuit is OPEN, enrich_transaction() returns a degraded result:
    all stats at default values, is_enrichment_degraded=True
  - Log a WARNING when circuit opens, INFO when it closes

## consumers/bloom_filter.py — IP blacklist
  NOTE: Standard Redis (without RedisBloom module) does not support BF commands.
  Implement a pure-Python Bloom filter using mmh3 + bitarray that is:
  - Loaded into Redis as a single binary string (GET/SET bloom:ips)
  - Initialized with capacity=10_000_000 IPs, error_rate=0.01
  - Provides: add(ip: str), contains(ip: str) -> bool, save_to_redis(), load_from_redis()
  - Falls back to a Redis SET if the bloom filter is not initialized

## db/seeds/seed_blacklist.py
  Script that:
  1. Adds 1000 fake card IDs to the Redis blacklist (format: CARD_{n:07d} for n in range(1000))
  2. Initializes the Bloom filter and adds 10000 fake IPs
  3. Logs count of items added to each data structure
  4. Is idempotent (safe to re-run)

## db/seeds/seed_user_statistics.py (Redis portion)
  Script that:
  1. Generates realistic statistics for 100,000 cards:
     avg from lognormal(4.0, 0.9), std from avg*uniform(0.3, 0.8), count_30d from randint(5, 200)
  2. bulk_set_card_stats() them into Redis in batches of 500
  3. Prints progress every 10,000 cards
  4. Measures and prints total time and Redis memory used after seeding

## tests/unit/test_redis_client.py
  Use pytest + fakeredis (fakeredis==2.20.0) to test WITHOUT a real Redis:
  - test_get_velocity_empty: returns 0 for unknown card
  - test_add_and_get_velocity: add 5 txns, get_velocity(60) returns 5
  - test_velocity_window_eviction: add txn at t-120s, get_velocity(60) returns 0
  - test_enrich_transaction_pipeline: full enrich call returns correct dict shape
  - test_circuit_breaker_opens: force 5 Redis errors, verify degraded result returned
  - test_card_blacklist: add card, check SISMEMBER returns True

## requirements.txt additions
  redis==5.0.1
  pybreaker==1.0.1
  mmh3==4.0.1
  bitarray==2.8.3
  fakeredis==2.20.0  (dev/test only)

## Constraints
  - NEVER call Redis more than once per enrich_transaction() invocation
    (all lookups must be pipelined into exactly 1 round-trip)
  - All methods must have Python type hints
  - Use structured JSON logging (import logging; logging.basicConfig(format=json))
  - Never swallow exceptions silently — always log before returning fallback
  - Connection pool must be shared across threads (module-level singleton)

Output all files as labeled code blocks with full implementations.
Include a 'Running the tests' section at the end.
```

---

## 🗄️ PHASE 4 — PostgreSQL Schema & Migrations

### Overview

Create the full PostgreSQL schema with partitioned tables, indexes optimized for fraud detection query patterns, PostGIS for geographic queries, and a statistics refresh procedure. Also write the seed scripts that pre-populate historical user statistics — critical for the Z-score anomaly detection to work from day one.

- **Prerequisites:** Phase 1 complete. PostgreSQL running at `localhost:5432` with database `frauddb`. Python 3.11+ with psycopg2 available.
- **Goal:** Five migration files creating all tables, indexes, partitions, and stored procedures. Seed data for 500K users. A statistics refresh job runnable as a cron or standalone script.
- **Outputs:** `db/migrations/001-005.sql` | `db/seeds/seed_user_statistics.py` (PostgreSQL portion) | `db/stats_refresh.py` | `tests/unit/test_db_schema.py`

### LLM Prompt

```
You are a senior PostgreSQL database engineer. Build the complete database schema
for a real-time financial fraud detection platform.

## Connection details
  host: postgres (Docker), port: 5432
  database: frauddb, user: fraud_user, password: fraud_pass
  Read from env: POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

## db/migrations/001_create_extensions.sql
  CREATE EXTENSION IF NOT EXISTS 'uuid-ossp';
  CREATE EXTENSION IF NOT EXISTS postgis;
  CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
  CREATE EXTENSION IF NOT EXISTS btree_gin;

## db/migrations/002_create_transactions.sql
  Table: transactions
  Columns:
    transaction_id  UUID PRIMARY KEY DEFAULT uuid_generate_v4()
    card_id         VARCHAR(20) NOT NULL
    user_id         VARCHAR(20) NOT NULL
    amount          DECIMAL(15,2) NOT NULL CHECK (amount > 0)
    merchant_id     VARCHAR(20)
    merchant_cat    VARCHAR(10)
    geo_lat         DOUBLE PRECISION
    geo_lon         DOUBLE PRECISION
    geo_point       GEOGRAPHY(Point, 4326)  -- PostGIS spatial column
    country         CHAR(2)
    is_online       BOOLEAN DEFAULT FALSE
    is_fraud        BOOLEAN DEFAULT FALSE
    fraud_type      VARCHAR(20)  -- VELOCITY | DEVIATION | BLACKLIST_HIT
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
  Partitioning:
    PARTITION BY RANGE (created_at)
    Create partitions for: current month, next month
    Include a comment explaining how to automate monthly partition creation
  Indexes:
    idx_txn_card_time:    (card_id, created_at DESC)
    idx_txn_created:      (created_at DESC)
    idx_txn_is_fraud:     (is_fraud) WHERE is_fraud = TRUE  -- partial index
    idx_txn_geo:          USING GIST (geo_point)  -- spatial index
    idx_txn_country_time: (country, created_at DESC)
  Add a TRIGGER that automatically populates geo_point from geo_lat/geo_lon:
    BEFORE INSERT OR UPDATE ON transactions
    SET geo_point = ST_SetSRID(ST_MakePoint(NEW.geo_lon, NEW.geo_lat), 4326)

## db/migrations/003_create_fraud_alerts.sql
  Table: fraud_alerts
  Columns:
    alert_id        UUID PRIMARY KEY DEFAULT uuid_generate_v4()
    transaction_id  UUID REFERENCES transactions(transaction_id) ON DELETE SET NULL
    card_id         VARCHAR(20) NOT NULL
    user_id         VARCHAR(20) NOT NULL
    fraud_type      VARCHAR(20) NOT NULL
    severity        VARCHAR(10) NOT NULL CHECK (severity IN ('LOW','MEDIUM','HIGH','CRITICAL'))
    risk_score      DECIMAL(5,4) NOT NULL CHECK (risk_score BETWEEN 0 AND 1)
    recommendation  VARCHAR(20) CHECK (recommendation IN ('Block','Challenge','Monitor','Allow'))
    trigger_reason  TEXT
    window_start    TIMESTAMPTZ
    window_end      TIMESTAMPTZ
    triggered_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
  Indexes:
    idx_alert_time:     (triggered_at DESC)
    idx_alert_card:     (card_id, triggered_at DESC)
    idx_alert_severity: (severity, triggered_at DESC)
    idx_alert_type:     (fraud_type, triggered_at DESC)
    idx_alert_score:    (risk_score DESC) WHERE risk_score > 0.7  -- partial

## db/migrations/004_create_user_statistics.sql
  Table: user_statistics
  Columns:
    user_id                 VARCHAR(20) PRIMARY KEY
    avg_txn_amount_30d      DECIMAL(12,2)
    stddev_txn_amount_30d   DECIMAL(12,2)
    txn_count_7d            INTEGER DEFAULT 0
    txn_count_30d           INTEGER DEFAULT 0
    total_spend_30d         DECIMAL(15,2) DEFAULT 0
    last_txn_country        CHAR(2)
    home_country            CHAR(2),
    fraud_flag_count_30d    INTEGER DEFAULT 0
    last_active_at          TIMESTAMPTZ
    updated_at              TIMESTAMPTZ DEFAULT NOW()
  Index: idx_user_stats_active (last_active_at DESC)

## db/migrations/005_create_helper_views.sql
  View: v_recent_alerts — last 24 hours of fraud_alerts with card info
  View: v_fraud_geo — geo coordinates and alert count for heatmap
    SELECT geo_lat, geo_lon, country, COUNT(*) as alert_count
    FROM transactions t JOIN fraud_alerts fa ON t.transaction_id = fa.transaction_id
    WHERE fa.triggered_at > NOW() - INTERVAL '1 hour'
    GROUP BY 1, 2, 3
  View: v_top_flagged_cards — cards with most alerts in last hour
    SELECT card_id, COUNT(*) as alert_count, MAX(risk_score) as max_score
    FROM fraud_alerts
    WHERE triggered_at > NOW() - INTERVAL '1 hour'
    GROUP BY card_id ORDER BY alert_count DESC

## db/seeds/seed_user_statistics.py
  Write a Python script that:
  1. Connects to PostgreSQL using psycopg2 + connection pooling
  2. Generates 500,000 user records with realistic stats:
     - user_id: USER_{n:06d}
     - avg_txn_amount_30d: np.random.lognormal(4.0, 0.9) clipped to [2, 5000]
     - stddev_txn_amount_30d: avg * np.random.uniform(0.2, 0.8)
     - txn_count_30d: np.random.randint(5, 250)
     - last_txn_country: random choice from ['US','GB','DE','FR','CA','AU']
     - home_country: same as last_txn_country with 80% probability
     - last_active_at: random datetime within last 30 days
  3. Uses COPY (psycopg2 copy_from with StringIO) for bulk inserts — NOT individual INSERTs
     This is 50x faster than row-by-row inserts
  4. Runs in batches of 10,000; prints progress bar
  5. Total time should be under 30 seconds for 500K rows
  6. Uses INSERT ... ON CONFLICT (user_id) DO UPDATE (upsert) for idempotency

## db/stats_refresh.py
  Write a script (runnable standalone or as a cron job every hour) that:
  1. Queries PostgreSQL for active cards (transaction in last 2 hours)
  2. Computes fresh avg/stddev from transactions in last 30 days
  3. Updates both PostgreSQL user_statistics AND Redis stats:{card_id} hashes
  4. Logs: cards_processed, redis_updates, pg_updates, duration_seconds
  5. Runs in a single SQL query using window functions (do not use Python for aggregation)
     SQL shape:
       SELECT user_id,
              AVG(amount) as avg_30d,
              STDDEV(amount) as std_30d,
              COUNT(*) as count_30d
       FROM transactions
       WHERE created_at > NOW() - INTERVAL '30 days'
       GROUP BY user_id
  6. Measures Redis pipeline efficiency (should be < 5ms per 100 cards)

## tests/unit/test_db_schema.py
  Use pytest with a temporary PostgreSQL database (use testcontainers-python):
  - test_transaction_insert: insert a transaction, verify geo_point trigger fires
  - test_duplicate_transaction_id: verify PRIMARY KEY constraint
  - test_fraud_alert_risk_score_constraint: verify risk_score CHECK constraint
  - test_partition_routing: insert transaction in current month, verify correct partition
  - test_geo_query: insert 2 transactions, verify PostGIS distance query works

## Constraints
  - All SQL must be idempotent (IF NOT EXISTS, ON CONFLICT DO NOTHING/UPDATE)
  - No ORM — raw SQL only (psycopg2 directly)
  - All database credentials from environment variables, never hardcoded
  - Migration files must run in numeric order and be safe to re-run
  - Include EXPLAIN ANALYZE output in comments for each index,
    showing what query it optimizes and expected speedup

Output all files as labeled code blocks.
Verification section: show psql commands to verify each table, index, and view exists.
```

---

## 🚀 PHASE 5 — High-Throughput Producer

### Overview

Build the Python multiprocessing producer that generates 1M+ events per hour. This is the data source for the entire pipeline. It must generate statistically realistic transactions (not just random data), inject synthetic flash fraud patterns, and be observable — exposing metrics on how many events were sent, error rates, and per-worker throughput.

- **Prerequisites:** Phases 1–2 complete. Kafka cluster healthy. Schema Registry running. Avro schemas registered. Python confluent-kafka and numpy installed.
- **Goal:** A multiprocessing producer that sustains 400+ events/second with idempotent delivery, realistic distributions, and a 0.5% flash fraud injection rate. Observable via a built-in Prometheus metrics endpoint.
- **Outputs:** `producers/main.py` | `producers/generator.py` | `producers/distributions.py` | `producers/config.py` | `producers/metrics.py` | `tests/unit/test_generator.py`

### LLM Prompt

```
You are a senior Python engineer specializing in high-throughput data generation
and Apache Kafka producer design.
Build the complete transaction producer for a real-time fraud detection platform.

## Target throughput: 400 events/second (1.44M per hour)
## Workers: 8 Python processes, each producing ~50 events/second
## Fraud injection: 0.5% of cards are 'fraud cards'; they emit 15-25 tx bursts with 2% probability

## producers/config.py
  Dataclass ProducerConfig with fields (all readable from env vars with defaults):
    KAFKA_BROKERS:       str  = 'kafka1:9092,kafka2:9092,kafka3:9092'
    SCHEMA_REGISTRY_URL: str  = 'http://schema-registry:8081'
    TOPIC_RAW:           str  = 'transactions.raw'
    TOPIC_DLQ:           str  = 'dead-letter-queue'
    NUM_WORKERS:         int  = 8
    TPS_PER_WORKER:      int  = 50
    CARD_POOL_SIZE:      int  = 1_000_000
    FRAUD_CARD_PCT:      float = 0.005
    BURST_PROBABILITY:   float = 0.02
    BURST_MIN:           int  = 15
    BURST_MAX:           int  = 25
    LINGER_MS:           int  = 50
    BATCH_SIZE_BYTES:    int  = 131072
    ENABLE_METRICS:      bool = True
    METRICS_PORT:        int  = 8100

## producers/distributions.py
  Implement realistic statistical distributions using numpy.
  All functions are vectorized (return numpy arrays, not scalars):
  generate_amounts(n: int) -> np.ndarray
    Lognormal with mu=4.0, sigma=0.9, clipped to [0.50, 8000.00]
    Round to 2 decimal places
  sample_cards(pool: np.ndarray, n: int, fraud_cards: set) -> tuple[np.ndarray, np.ndarray]
    Returns (card_ids, is_fraud_flags)
    Non-uniform: weight fraud cards 3x more likely to be sampled
  generate_geo_points(n: int) -> tuple[np.ndarray, np.ndarray]
    Zipf distribution over 100 pre-defined city lat/lon pairs
    City list must include major global financial centers:
      New York (40.71, -74.01), London (51.51, -0.13),
      Singapore (1.35, 103.82), Dubai (25.20, 55.27),
      Tokyo (35.68, 139.69), Frankfurt (50.11, 8.68),
      Hong Kong (22.32, 114.17), Paris (48.86, 2.35),
      Sydney (-33.87, 151.21), Toronto (43.65, -79.38)
      + 90 more realistic cities
    Top 10 cities get ~70% of traffic (realistic hub concentration)
  sample_merchant_categories(n: int) -> np.ndarray
    Weighted: Grocery 25%, Restaurant 20%, Gas 15%, E-commerce 20%, Other 20%
    Returns ISO MCC codes: ['5411','5812','5541','5999','4829']
  generate_time_offset(n: int) -> np.ndarray
    Gaussian mixture model for time-of-day realism:
    Peak 1: noon (12:00) with sigma=2hrs
    Peak 2: evening (18:00) with sigma=1.5hrs
    Trough: 3 AM
    Returns sleep duration in seconds (used to pace event generation)

## producers/generator.py
  class TransactionGenerator:
    __init__(self, config: ProducerConfig, worker_id: int)
    _build_card_pool() -> tuple[np.ndarray, set]
      Creates CARD_POOL_SIZE card IDs: [f'CARD_{i:07d}' for i in range(N)]
      FRAUD_CARD_PCT of them are designated fraud cards (random sample, fixed seed=42)
    generate_normal_batch(self, n: int) -> list[dict]
      Returns list of n TransactionEvent dicts using vectorized distributions
      Each dict has all fields required by TransactionEvent Avro schema
    generate_flash_fraud_burst(self, card_id: str) -> list[dict]
      Returns 15-25 transactions from the same card within a 500ms window
      Small amounts: uniform [1.00, 29.99]
      Same merchant (random selection)
      Timestamps: now_ms + i for i in range(burst_size)
    next_batch(self) -> list[dict]
      Called in the producer loop.
      For each transaction in a normal batch:
        If card is a fraud card AND random() < BURST_PROBABILITY:
          return flash fraud burst instead of normal transaction
        Else: return normal transaction

## producers/metrics.py
  Use the prometheus_client library to expose per-worker metrics:
  Counter:   transactions_produced_total{worker_id, status}  (status: success|error)
  Counter:   fraud_bursts_injected_total{worker_id}
  Histogram: produce_latency_seconds{worker_id}   (time from generate to delivery callback)
  Gauge:     producer_queue_depth{worker_id}       (messages buffered, not yet sent)
  Start an HTTP server on port METRICS_PORT + worker_id
  (worker 0 on 8100, worker 1 on 8101, etc.)

## producers/main.py
  Entry point for the multiprocessing producer:
  def make_kafka_producer(config: ProducerConfig) -> Producer:
    confluent_kafka Producer with:
      bootstrap.servers:                    config.KAFKA_BROKERS
      enable.idempotence:                   True
      acks:                                 'all'
      compression.type:                     'lz4'
      batch.size:                           config.BATCH_SIZE_BYTES
      linger.ms:                            config.LINGER_MS
      retries:                              5
      max.in.flight.requests.per.connection: 5
      delivery.timeout.ms:                  30000
      on_delivery callback that: logs errors, increments metrics counter
      sends failed messages to DLQ topic on permanent failure
  def worker_main(worker_id: int, config: ProducerConfig):
    1. Create producer and generator
    2. Start metrics HTTP server if ENABLE_METRICS
    3. Main loop:
       - Get next_batch() from generator
       - For each event in batch:
           serialize with AvroSerializer (schema: TransactionEvent)
           producer.produce(topic, key=card_id.encode(), value=serialized,
                            on_delivery=delivery_callback)
           producer.poll(0)
       - Sleep to maintain target TPS (non-busy-wait using time.sleep)
    4. Handle KeyboardInterrupt: call producer.flush(30) before exit
    5. Structured JSON log every 10 seconds: worker_id, events_sent, errors, tps
  if __name__ == '__main__':
    Parse args (--workers, --tps, optional --duration for testing)
    Spawn NUM_WORKERS processes
    Main process prints aggregate stats every 5 seconds (sum across workers)
    Handle SIGINT: graceful shutdown (send poison pill to workers, wait for flush)

## tests/unit/test_generator.py
  - test_amount_distribution: generate 10000 amounts, verify mean between 60-110
  - test_burst_size: generate burst, verify len between 15-25
  - test_fraud_card_sampling: verify fraud cards appear 3x more often than normal cards
  - test_geo_concentration: top 10 cities get 65-75% of traffic
  - test_card_id_format: all card IDs match pattern CARD_\d{7}
  - test_avro_schema_compliance: generate 100 events, verify each serializes without error

## requirements.txt additions
  confluent-kafka[avro]==2.3.0
  numpy==1.26.2
  prometheus-client==0.19.0
  faker==20.1.0

## Constraints
  - Do NOT use the kafka-python library — only confluent-kafka
  - Do NOT use threading — only multiprocessing (GIL bypass)
  - Each worker process must be completely independent (no shared memory)
  - The main process must NOT produce events — only orchestrate workers
  - All Faker usage must have a fixed seed per worker for reproducibility
  - generator.py must have zero Kafka dependencies (pure data generation logic)

Output all files as labeled code blocks.
Verification: show how to run with 2 workers for 30 seconds and verify
events appear in Kafka using kafka-console-consumer.
```

---

## 🔗 PHASE 6 — Enrichment Consumer

### Overview

The enrichment consumer reads raw transactions from `transactions.raw`, calls Redis for behavioral features, and publishes `EnrichedTransaction` events to `transactions.enriched`. It is the most latency-critical Python component — Redis lookups must be pipelined and the consumer must handle Redis failure gracefully via the circuit breaker built in Phase 3.

- **Prerequisites:** Phases 1–5 complete. Kafka topics created. Redis seeded with blacklists and card stats. Avro schemas registered. `consumers/redis_client.py` exists.
- **Goal:** A production-grade consumer that processes 400+ events/second with < 2ms Redis enrichment latency, manual offset commits, and proper rebalance handling.
- **Outputs:** `consumers/enrichment_consumer.py` | `consumers/base_consumer.py` | `tests/integration/test_enrichment.py`

### LLM Prompt

```
You are a senior Python engineer specializing in Apache Kafka consumer design.
Build the enrichment consumer for a real-time fraud detection platform.

## Role of this consumer
  Reads from: transactions.raw (Avro, TransactionEvent schema)
  Writes to:  transactions.enriched (Avro, EnrichedTransaction schema)
  Consumer group: enrichment-cg
  Parallelism: 4 threads (one per assigned partition)

## consumers/base_consumer.py
  Abstract base class BaseConsumer with:
  __init__(self, group_id: str, topics: list[str], config_overrides: dict = {})
    - Creates confluent_kafka Consumer with these base settings:
        bootstrap.servers:               from env KAFKA_BROKERS
        group.id:                        group_id
        auto.offset.reset:               'latest'
        enable.auto.commit:              False  (manual commits only)
        max.poll.interval.ms:            30000
        session.timeout.ms:              10000
        heartbeat.interval.ms:           3000
        partition.assignment.strategy:   'cooperative-sticky'
        fetch.min.bytes:                 1024
        fetch.wait.max.ms:               100
    - Merges config_overrides on top
  on_assign(consumer, partitions): abstract — called on partition assignment
  on_revoke(consumer, partitions):
    - Flush any in-progress batch
    - Commit current offsets synchronously before returning
    - Log: partitions being revoked
  process_batch(messages: list) -> int: abstract — returns count processed
  run(batch_size: int = 500):
    Main loop:
    1. consumer.consume(num_messages=batch_size, timeout=0.1)
    2. Filter out None messages and messages with errors (log errors, send to DLQ)
    3. Call process_batch(messages)
    4. consumer.commit(asynchronous=False)  # AFTER successful processing
    5. Log batch stats every 100 batches
    6. Handle KeyboardInterrupt: final commit + consumer.close()

## consumers/enrichment_consumer.py
  Class EnrichmentConsumer(BaseConsumer):
  __init__(self):
    - Call super().__init__('enrichment-cg', ['transactions.raw'])
    - Initialize SchemaClient (from schemas/client.py) for deserialization/serialization
    - Initialize RedisFeatureStore (from consumers/redis_client.py)
    - Initialize Kafka Producer for transactions.enriched output
    - Producer config: idempotent=True, linger_ms=10, acks=all
  process_batch(self, messages: list) -> int:
    For each message in batch:
    1. Deserialize Avro bytes -> dict (TransactionEvent)
    2. Call redis_store.enrich_transaction(card_id, txn_id, timestamp_ms)
       This is a SINGLE pipeline call returning all enrichment fields
    3. Build EnrichedTransaction dict by merging raw event + enrichment result
    4. Serialize to Avro (EnrichedTransaction schema)
    5. Produce to 'transactions.enriched' with same card_id as key
    After all messages:
    6. Call output_producer.flush()  (wait for all enriched events to be acked)
    7. Update metrics: batch_size, processing_time, redis_degraded_count
    8. Return count of successfully processed messages
  Error handling in process_batch:
    - Avro deserialization failure: send raw bytes to DLQ with error metadata, continue
    - Redis failure (circuit open): enrich with degraded defaults, set is_enrichment_degraded=True
    - Output produce failure: retry once, then send to DLQ
    - NEVER let an exception bubble up and crash the consumer loop
  on_assign(self, consumer, partitions):
    - Log: 'Assigned partitions: {[p.partition for p in partitions]}'
    - Seek each partition to its committed offset (or beginning if none)
  DLQ publishing:
    def _send_to_dlq(self, raw_value: bytes, raw_key: bytes, reason: str, error: str):
      Publishes to 'dead-letter-queue' with value:
      {
        'original_topic': 'transactions.raw',
        'original_key': raw_key.decode(),
        'error_reason': reason,
        'error_detail': str(error),
        'failed_at': int(time.time() * 1000),
        'raw_payload_b64': base64.b64encode(raw_value).decode()
      }
  Metrics (prometheus_client):
    Counter: enrichment_processed_total{status}  (status: success|dlq|degraded)
    Histogram: enrichment_latency_ms  (time from poll to output produce)
    Histogram: redis_lookup_latency_ms
    Gauge: consumer_lag{partition}

## Scaling: how to run 4 workers
  The enrichment consumer is designed to run as 4 independent processes.
  Kafka automatically assigns 3 of the 12 partitions to each process.
  Provide a run_enrichment.py script that launches N workers as subprocesses:
    python run_enrichment.py --workers 4

## tests/integration/test_enrichment.py
  Use pytest with a real Kafka (running in Docker) and fakeredis:
  test_enrichment_happy_path:
    1. Produce 10 TransactionEvent messages to transactions.raw
    2. Start EnrichmentConsumer in a thread (run for 5 seconds)
    3. Consume from transactions.enriched
    4. Assert 10 EnrichedTransaction messages received
    5. Assert each has velocity_1m, avg_amount_30d, is_blacklisted_card fields
  test_enrichment_redis_failure:
    1. Configure RedisFeatureStore to fail (wrong port)
    2. Produce 5 transactions
    3. Assert 5 enriched messages received with is_enrichment_degraded=True
    4. Assert 0 messages in DLQ
  test_enrichment_bad_avro:
    1. Produce raw bytes (not valid Avro) to transactions.raw
    2. Assert message appears in dead-letter-queue
    3. Assert DLQ message contains 'error_reason' and 'raw_payload_b64'
  test_offset_commit_after_process:
    Verify that if consumer crashes mid-batch, restarted consumer
    re-processes from last committed offset (not from beginning)

## Constraints
  - enable.auto.commit MUST be False — manual commit only
  - Offsets must only be committed AFTER output producer.flush() succeeds
  - The consumer must never drop a message silently (DLQ or retry, always)
  - partition.assignment.strategy MUST be cooperative-sticky
  - Log at INFO level: every batch processed (batch_size, duration_ms)
  - Log at WARNING level: every DLQ event, every Redis circuit open/close
  - Do not import Spark or Flink — this is a plain Python consumer

Output all files as labeled code blocks.
Include a section explaining how to verify enrichment is working:
  kafka-console-consumer on transactions.enriched and grep for 'velocity_1m'
```

---

## ⚙️ PHASE 7 — Spark Structured Streaming — Detection Engine

### Overview

The heart of the platform. This Spark job reads from `transactions.enriched`, applies velocity (sliding window) and Z-score (statistical deviation) fraud detection, emits `FraudAlert` events to `fraud.alerts`, and writes all data to PostgreSQL. Watermarking, backpressure, checkpointing, and graceful restart must all be production-ready.

- **Prerequisites:** Phases 1–6 complete. `transactions.enriched` receiving data. PostgreSQL tables created. Spark cluster running. PySpark 3.4 with Kafka package available.
- **Goal:** A Spark Structured Streaming job that detects Flash Fraud and Anomalous Spending in real time, survives restarts via checkpointing, and writes idempotently to PostgreSQL.
- **Outputs:** `spark_jobs/fraud_detection_job.py` | `spark_jobs/velocity_detection.py` | `spark_jobs/zscore_anomaly.py` | `spark_jobs/output_sinks.py` | `spark_jobs/session.py`

### LLM Prompt

```
You are a senior data engineer specializing in Apache Spark Structured Streaming.
Build the complete fraud detection Spark job for a real-time financial fraud platform.

## Environment
  Spark version: 3.4.2
  Python: 3.11
  Kafka package: org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2
  JDBC driver: org.postgresql:postgresql:42.6.0
  Kafka brokers: kafka1:9092,kafka2:9092,kafka3:9092
  Source topic: transactions.enriched (12 partitions)
  PostgreSQL JDBC URL: jdbc:postgresql://postgres:5432/frauddb
  Checkpoint base dir: /tmp/checkpoints

## spark_jobs/session.py
  Function get_spark_session() -> SparkSession:
    builder config:
      appName: 'FraudDetectionEngine'
      master: from env SPARK_MASTER_URL, default 'spark://spark-master:7077'
      spark.jars.packages: kafka + postgresql jars (comma-separated)
      spark.sql.streaming.checkpointLocation: /tmp/checkpoints
      spark.streaming.backpressure.enabled: true
      spark.sql.streaming.forceDeleteTempCheckpointLocation: true
      spark.executor.memory: 4g
      spark.executor.cores: 3
      spark.sql.adaptive.enabled: true
      spark.sql.shuffle.partitions: 12
    setLogLevel('WARN')
    return session

## spark_jobs/velocity_detection.py
  Function detect_flash_fraud(parsed_df: DataFrame) -> DataFrame:
  Apply a sliding window to count transactions per card:
    .withWatermark('event_time', '10 minutes')
    .groupBy(
        window(col('event_time'), '1 minute', '10 seconds'),
        col('card_id')
    )
    .agg(
        count('transaction_id').alias('txn_count'),
        F.sum('amount').alias('total_amount'),
        F.max('amount').alias('max_amount'),
        F.first('user_id').alias('user_id'),
        F.first('geo_lat').alias('geo_lat'),
        F.first('geo_lon').alias('geo_lon'),
        F.first('country').alias('country'),
        F.first('transaction_id').alias('txn_id'),
    )
    .filter(col('txn_count') > 5)
  Then add columns:
    fraud_type = 'VELOCITY'
    risk_score = least(1.0, txn_count / 30.0)  -- capped at 1.0
    severity = CRITICAL if txn_count>=20, HIGH if >=15, MEDIUM if >=10, else LOW
    recommendation = 'Block' if severity in [CRITICAL, HIGH] else 'Challenge'
    trigger_reason = concat('Card swiped ', txn_count, ' times in 60 seconds')
    alert_id = uuid()  -- use F.expr('uuid()')
    window_start = col('window.start').cast('long') * 1000  -- epoch ms
    window_end   = col('window.end').cast('long') * 1000
    triggered_at = F.current_timestamp().cast('long') * 1000
  Return only the columns present in FraudAlert schema.

## spark_jobs/zscore_anomaly.py
  Function detect_anomalous_spending(parsed_df: DataFrame) -> DataFrame:
  Filter rows where stddev_amount_30d > 0 (can't compute Z if no history)
  Compute:
    z_score = abs(amount - avg_amount_30d) / stddev_amount_30d
  Filter where z_score > 3.0
  Add columns:
    fraud_type = 'DEVIATION'
    risk_score = least(1.0, z_score / 10.0)
    severity = CRITICAL if z_score>7, HIGH if >5, MEDIUM if >3.5, else LOW
    recommendation = 'Block' if z_score > 5.0 else 'Challenge'
    trigger_reason = concat('Amount $', amount, ' is ', round(z_score,1),
                            ' sigma from card mean $', avg_amount_30d)
    alert_id, txn_id (= transaction_id), card_id, user_id,
    triggered_at, window_start=null, window_end=null
  Also implement:
  Function detect_blacklist_hits(parsed_df: DataFrame) -> DataFrame:
    Filter where is_blacklisted_card == true OR is_blacklisted_ip == true
    Add: fraud_type='BLACKLIST_HIT', risk_score=0.95, severity='CRITICAL',
         recommendation='Block', trigger_reason='Card or IP on blacklist'

## spark_jobs/output_sinks.py
  Function write_alerts_to_kafka(alerts_df: DataFrame, checkpoint: str) -> StreamingQuery
  Function write_transactions_to_postgres(parsed_df: DataFrame, checkpoint: str) -> StreamingQuery
    Use foreachBatch — idempotent because PostgreSQL uses INSERT ... ON CONFLICT DO NOTHING
  Function write_alerts_to_postgres(alerts_df: DataFrame, checkpoint: str) -> StreamingQuery

## spark_jobs/fraud_detection_job.py
  Main entry point:
  1. Get SparkSession
  2. Read from Kafka with maxOffsetsPerTrigger=50000 (backpressure)
  3. Parse JSON value with full EnrichedTransaction StructType schema
  4. Apply all three detectors
  5. Union all alerts with unionByName(allowMissingColumns=True)
  6. Start 3 streaming queries (Kafka alerts, PG transactions, PG alerts)
  7. Monitor queries every 60s; restart any that terminate unexpectedly
  8. spark.streams.awaitAnyTermination()
  9. Handle SIGTERM: stop all queries, call spark.stop()

## Constraints
  - withWatermark MUST be applied before groupBy — never after
  - foreachBatch MUST check for empty DataFrames before writing to JDBC
  - outputMode MUST be 'append' for all queries with watermarks
  - Checkpoint directories must be unique per query
  - Use F.expr('uuid()') not Python uuid.uuid4() for alert_id generation
  - All column names must exactly match PostgreSQL column names

Output all files as labeled code blocks.
Include instructions for submitting the job:
  spark-submit --packages ... spark_jobs/fraud_detection_job.py
```

---

## 📊 PHASE 8 — Analytics & Alert Consumers

### Overview

Two additional consumer groups sit alongside Spark: the analytics consumer refreshes PostgreSQL user statistics from the transaction stream, and the alert consumer processes `FraudAlert` events from `fraud.alerts` to trigger notifications and update operational state. Both use the `BaseConsumer` from Phase 6.

- **Prerequisites:** Phases 1–7 complete. `consumers/base_consumer.py` exists. PostgreSQL tables exist. Kafka topics receiving data.
- **Goal:** Analytics consumer keeping `user_statistics` table fresh. Alert consumer logging alerts and triggering webhooks. DLQ monitor consumer raising alerts on dead letters.
- **Outputs:** `consumers/analytics_consumer.py` | `consumers/alert_consumer.py` | `consumers/dlq_consumer.py`

### LLM Prompt

```
You are a senior Python/Kafka engineer. Build three additional consumer services
for a real-time fraud detection platform, all extending the BaseConsumer class
already defined in consumers/base_consumer.py.

## consumers/analytics_consumer.py
  Consumer group: analytics-cg
  Source topic: transactions.enriched
  Purpose: keep user_statistics table in PostgreSQL up to date
  Class AnalyticsConsumer(BaseConsumer):
  process_batch(self, messages: list) -> int:
    1. Deserialize each EnrichedTransaction Avro message
    2. Group messages by user_id
    3. For each user_id, compute running stats from the batch
    4. Upsert to user_statistics using ON CONFLICT (user_id) DO UPDATE
    5. Use batch UPSERT (executemany with psycopg2, NOT row-by-row)
    6. Log: batch_size, unique_users, pg_upsert_count, duration_ms
  PostgreSQL connection: use psycopg2 with ThreadedConnectionPool (minconn=2, maxconn=10)

## consumers/alert_consumer.py
  Consumer group: alert-cg
  Source topic: fraud.alerts
  Class AlertConsumer(BaseConsumer):
  process_batch(self, messages: list) -> int:
    For each FraudAlert message (JSON, not Avro — Spark writes JSON):
    1. Log at appropriate level: CRITICAL/HIGH -> logger.error(), MEDIUM -> logger.warning()
    2. If recommendation == 'Block': SADD blacklist:cards card_id
       (feedback loop: Spark detects fraud -> alert consumer blocks card -> future txns flagged)
    3. If WEBHOOK_URL env var is set: POST alert as JSON (timeout=5s, non-critical)
    4. LPUSH recent_alerts <json_alert> + LTRIM recent_alerts 0 999
       (feeds React dashboard without querying PostgreSQL)
  Metrics: alerts_processed_total{fraud_type, severity}, cards_blocked_total, webhooks_sent_total

## consumers/dlq_consumer.py
  Consumer group: dlq-monitor-cg
  Source topic: dead-letter-queue
  Class DLQConsumer(BaseConsumer):
  process_batch(self, messages: list) -> int:
    1. Parse DLQ envelope (error_reason, original_topic, failed_at, raw_payload_b64)
    2. Log at ERROR level with full envelope
    3. Maintain counter of errors by original_topic and error_reason
    4. If DLQ depth > 100 in last 5 minutes: send PagerDuty alert or log CRITICAL
    5. Write summary to Redis: HSET dlq:stats total_count X last_error_at Y
    6. Save sample of raw payloads to /tmp/dlq_samples/ (keep last 100)

## run_consumers.py
  python run_consumers.py --consumer enrichment --workers 4
  python run_consumers.py --consumer analytics  --workers 2
  python run_consumers.py --consumer alert      --workers 2
  python run_consumers.py --consumer dlq        --workers 1
  python run_consumers.py --consumer all        # runs all in subprocesses

## Constraints
  - All consumers extend BaseConsumer — do not copy the offset commit logic
  - PostgreSQL connections must use connection pooling, not a new connection per batch
  - Redis operations in alert_consumer must be fire-and-forget
  - DLQ consumer must never itself produce to the DLQ (infinite loop risk)
  - Webhook calls must have a timeout and run in a background thread
```

---

## 🔌 PHASE 9 — FastAPI Backend

### Overview

The FastAPI backend bridges the stream processing layer and the React dashboard. It exposes REST endpoints for polling-based metrics (TPS, Kafka lag, geographic data) and a WebSocket endpoint that pushes fraud alerts in real time.

- **Prerequisites:** Phases 1–8 complete. PostgreSQL has data. Redis has `recent_alerts` list. Kafka `fraud.alerts` topic receiving messages.
- **Goal:** A FastAPI application with 8 REST endpoints, 1 WebSocket endpoint, Kafka consumer background task, and OpenAPI documentation auto-generated.
- **Outputs:** `api/main.py` | `api/db.py` | `api/routes/metrics.py` | `api/routes/alerts.py` | `api/routes/kafka_status.py` | `api/routes/cards.py` | `api/websocket/alert_stream.py`

### LLM Prompt

```
You are a senior Python backend engineer specializing in FastAPI and async programming.
Build the complete API backend for a real-time fraud detection dashboard.

## Tech stack
  FastAPI 0.104+, asyncpg 0.29+, redis.asyncio, confluent_kafka, uvicorn, pydantic v2

## api/db.py
  async def get_pool() -> asyncpg.Pool: min_size=5, max_size=20, command_timeout=10.0
  async def get_redis() -> redis.asyncio.Redis

## api/websocket/alert_stream.py
  class ConnectionManager:
    active_connections: list[WebSocket]
    async connect / disconnect / broadcast_json (skip failed clients without raising)
  async def kafka_alert_pusher(): background task, polls fraud.alerts, broadcasts JSON

## api/routes/metrics.py
  GET /api/metrics/tps — TPS per second for last 5 minutes. Redis cache: 2s
  GET /api/metrics/fraud-rate — alert count per minute for last 30 minutes
  GET /api/metrics/system — total_transactions_24h, total_alerts_24h, critical_alerts_1h, avg_risk_score_1h

## api/routes/alerts.py
  GET /api/alerts/recent?limit=50 — from Redis LRANGE, fallback to PostgreSQL
  GET /api/alerts/history?days=30 — daily count by fraud_type and severity
  GET /api/alerts/geo?window_hours=1 — lat/lon/country/alert_count. Redis cache: 30s

## api/routes/kafka_status.py
  GET /api/kafka/lag — consumer group lag per partition with status (healthy/warning/critical)
  GET /api/kafka/groups — summary of consumer group health
  GET /api/kafka/topics — topic details

## api/routes/cards.py
  GET /api/cards/flagged?top=10 — top flagged cards in last hour
  GET /api/cards/{card_id}/history — last 50 transactions + all alerts in 24h

## api/main.py
  CORS: allow origins=['http://localhost:3001']
  GZip middleware for responses > 1KB
  Lifespan: init DB pool, Redis, start kafka_alert_pusher() background task
  WebSocket /ws/alerts: connect, receive_text loop, disconnect on WebSocketDisconnect
  /health endpoint: 200 with DB and Redis connection status
  OpenAPI docs at /docs

## Constraints
  - ALL database calls must be async (asyncpg, redis.asyncio)
  - Kafka AdminClient is sync — run it in asyncio.run_in_executor
  - All endpoints must return within 500ms under normal load
  - Cache all expensive queries in Redis; log cache hits/misses
  - WebSocket broadcast must not raise if one client is slow

Output all files as labeled code blocks. Include curl examples for every endpoint.
```

---

## 📱 PHASE 10 — React Dashboard

### Overview

The React dashboard provides real-time operational visibility with six panels: live TPS chart, WebSocket fraud alert feed, Kafka consumer lag, consumer group health, deck.gl geographic heatmap, and historical statistics.

- **Prerequisites:** Phase 9 complete. FastAPI backend running at `localhost:8000`. WebSocket endpoint `/ws/alerts` working.
- **Goal:** A React SPA with six live panels, auto-reconnecting WebSocket, GPU-rendered fraud heatmap, and proper handling of API failures and loading states.
- **Outputs:** `react-dashboard/src/` (all components, hooks, App.jsx) | `react-dashboard/Dockerfile` | `react-dashboard/nginx.conf`

### LLM Prompt

```
You are a senior React engineer specializing in real-time data visualization.
Build the complete monitoring dashboard for a real-time fraud detection platform.

## Tech stack
  React 18 + Vite, Recharts, deck.gl 8.9 + react-map-gl 7,
  Tailwind CSS, lucide-react

## Dashboard layout
  Dark theme: background #0D1117, cards #161B22, borders #30363D
  Header: title, connection status, last-updated timestamp, pulsing LIVE badge
  6 panels in 12-column CSS grid:
    Row 1: TPS chart (8 cols) | System stats (4 cols)
    Row 2: Fraud Alert Feed (6 cols) | Kafka Lag (6 cols)
    Row 3: Geographic Heatmap (8 cols) | Consumer Health + Top Flagged Cards (4 cols)

## src/hooks/useWebSocket.js
  Auto-reconnect with exponential backoff: 1s → 2s → 4s → ... → 60s cap
  Send 'ping' every 30s to keep connection alive
  Returns: { status: 'connecting'|'connected'|'disconnected'|'error', reconnectCount }

## src/hooks/usePollingData.js
  Generic hook: fetch on mount and every intervalMs, optional transform fn
  Returns: { data, loading, error, lastUpdated }
  Cancel pending fetches on unmount (AbortController)

## src/components/TpsChart.jsx
  Recharts LineChart — TPS for last 5 minutes, polled every 2s
  Two series: all transactions (blue) + fraud alerts (red)
  Reference line at current average TPS; min/max/current stat badges

## src/components/AlertFeed.jsx
  Real-time scrolling table via WebSocket, keeps last 200 alerts (FIFO)
  Each row: severity badge, fraud type icon, truncated card_id, amount,
             risk score progress bar, relative time, recommendation
  New alerts animate in from top (0.3s fade-in)
  Connection status badge (🟢/🔴 + reconnect count)

## src/components/KafkaLagChart.jsx
  Recharts BarChart — consumer lag per group, polled every 5s
  Log scale Y-axis; green/yellow/red color coding; per-partition tooltip
  Flashing alert banner if any group is RED

## src/components/ConsumerHealth.jsx
  Status table: Group Name | State | Total Lag | 🟢🟡🔴 Status
  Below: Top 5 Flagged Cards from /api/cards/flagged

## src/components/GeoHeatmap.jsx
  deck.gl HeatmapLayer on MapLibre (open-source, no token needed)
  basemap: 'https://demotiles.maplibre.org/style.json'
  getWeight: d => d.alert_count, radiusPixels: 40
  Color gradient: transparent-blue to opaque-red; legend; window alert count

## src/components/SystemStats.jsx
  4 stat cards: Total transactions 24h, Total alerts 24h,
                Critical alerts 1h, Avg risk score 1h (colored gauge)

## react-dashboard/nginx.conf
  Proxy /api/* → http://api-backend:8000/api/*
  Proxy /ws/* → http://api-backend:8000/ws/* (WebSocket)
  Gzip compression; no-cache for index.html; max-age=31536000 for static assets

## Constraints
  - Use MapLibre — NOT Mapbox
  - All panels: loading skeleton (not spinner) + error state with retry button
  - No useState anti-patterns — use useReducer for AlertFeed
  - No prop drilling — use React Context for WebSocket status
  - Dynamic import for deck.gl (bundle size < 2MB)
  - All data polling cleaned up on unmount

Output all files as labeled code blocks with full package.json.
```

---

## 📈 PHASE 11 — Prometheus + Grafana Observability

### Overview

Configure the complete observability stack: Prometheus scrape targets for Kafka (JMX exporter), Spark, Redis, PostgreSQL, and application custom metrics. Five pre-built Grafana dashboards covering the NOC overview, Kafka health, Spark streaming, infrastructure, and fraud analytics.

- **Prerequisites:** Phases 1–10 complete. Prometheus and Grafana containers running. JMX Exporter configured on Kafka brokers.
- **Goal:** Five fully functional Grafana dashboards auto-provisioned via dashboard JSON. All metrics scraped. Alerting rules configured.
- **Outputs:** `docker/prometheus/prometheus.yml` | `docker/prometheus/alert_rules.yml` | `docker/grafana/provisioning/` | `docker/grafana/dashboards/*.json`

### LLM Prompt

```
You are a senior SRE/observability engineer specializing in Prometheus and Grafana.
Build the complete monitoring stack for a real-time fraud detection platform.

## docker/prometheus/prometheus.yml
  global: scrape_interval 15s, evaluation_interval 15s
  scrape_configs:
    - kafka1/2/3: targets kafkaN:9101 (JMX exporter)
    - schema-registry: target schema-registry:8081/metrics
    - spark-master: target spark-master:8080/metrics/prometheus
    - redis: target redis-exporter:9121
    - postgres: target postgres-exporter:9187
    - node-exporter: target node-exporter:9100
    - producers: targets localhost:8100–8107 (8 workers)
    - enrichment-consumer: target enrichment-consumer:8200
    - api-backend: target api-backend:8000/metrics

## docker/prometheus/alert_rules.yml
  KafkaBrokerDown:           up{job='kafka'} == 0 — for 30s — CRITICAL
  KafkaConsumerLagCritical:  sum(kafka_consumergroup_lag) by (consumergroup) > 50000 — 2m — CRITICAL
  KafkaUnderReplicatedPartitions: > 0 — 1m — WARNING
  SparkBatchDelayed:         spark_streaming_processing_delay > 1000 — 3m — WARNING
  RedisMemoryHigh:           redis_memory_used_bytes / redis_memory_max_bytes > 0.85 — 5m — WARNING
  PostgresConnectionsHigh:   pg_stat_activity_count > 150 — 2m — WARNING
  DLQDepthSpike:             increase(...dead-letter-queue...[5m]) > 100 — 0s — CRITICAL

## Five Grafana Dashboard JSON files
  01-noc-overview.json: Current TPS, fraud alerts/min, TPS graph, consumer lag table, broker health
  02-kafka-health.json: Broker throughput, messages/sec, consumer lag heatmap, under-replicated partitions
  03-spark-streaming.json: Batch duration, input/processed rows/sec, scheduling delay, JVM heap
  04-infrastructure.json: CPU/memory per container, Redis commands, PostgreSQL query stats, disk I/O
  05-fraud-analytics.json: Daily alert volume by type, fraud rate, top countries, severity distribution, risk score histogram

## Constraints
  - All dashboard JSON must be valid and importable without modification
  - Use rate() not irate() for smoother graphs on 15s scrape interval
  - Use ${DS_PROMETHEUS} template variable — do not hardcode datasource UIDs
  - All dashboards: title, description, tags, default time range 15m
```

---

## 💥 PHASE 12 — Failure Testing & Chaos Engineering

### Overview

Systematically test the fault tolerance of every component. Each scenario includes a trigger command, expected system behavior, observation method, and pass/fail criteria.

- **Prerequisites:** Phases 1–11 complete. All services running. Grafana dashboards showing live data. Producer generating 400 events/second.
- **Goal:** A chaos test runbook (automated scripts + manual steps) covering 7 failure scenarios with pass/fail criteria scripts.
- **Outputs:** `scripts/chaos/` (7 scenario scripts) | `scripts/chaos/verify_recovery.py` | `tests/integration/test_chaos.py`

### LLM Prompt

```
You are a senior SRE and distributed systems engineer specializing in chaos engineering.
Build a complete chaos engineering test suite for a real-time Kafka fraud detection platform.

## scripts/chaos/ — One script per failure scenario

### 01_kafka_broker_crash.sh
  Kill kafka2, wait for KRaft leader re-election, watch lag stabilize, restart broker.
  PASS: re-election < 15s AND lag stabilized AND kafka2 rejoined ISR

### 02_consumer_group_rebalance.sh
  Add 5th enrichment worker, observe partition reassignment, kill worker, verify recovery.
  PASS: rebalance < 30s AND no duplicate messages

### 03_producer_retry_storm.sh
  Add 500ms latency via tc netem, watch producer retries, remove delay, verify no duplicates.
  PASS: COUNT(*) == COUNT(DISTINCT transaction_id) in PostgreSQL

### 04_spark_backpressure.sh
  Throttle PostgreSQL max_connections=5, observe lag plateau, restore, verify recovery.
  PASS: lag plateaued below 200K AND recovered within 5 min of restoration

### 05_redis_failure_circuit_breaker.sh
  Stop Redis, verify circuit opens (is_enrichment_degraded=True), restart, verify circuit closes.
  PASS: zero messages lost AND throughput > 80% of baseline during outage

### 06_schema_mismatch_dlq.sh
  Publish 10 invalid messages to transactions.raw, verify DLQ receives them.
  PASS: DLQ has >= 10 messages AND enrichment consumer throughput unaffected

### 07_full_pipeline_recovery.sh
  docker-compose stop, docker-compose start, verify Spark resumes from checkpoint.
  PASS: all services healthy AND data persisted AND Spark resumed from checkpoint

## scripts/chaos/verify_recovery.py
  Check invariants: no duplicate transaction_ids, consumer lag < 50K for all groups,
  all Kafka brokers in ISR, no OOM events, enrichment throughput > 200 events/sec
  Prints color-coded PASS/FAIL report

## Constraints
  - All scripts must be idempotent and restore system to full health before exiting
  - Use set -euo pipefail at the top of all bash scripts
  - Print timestamps with each step: echo '[HH:MM:SS] Step X: ...'
  - PASS/FAIL output prefixed with RESULT: for machine parsing
```

---

## 🧪 PHASE 13 — End-to-End Integration Testing

### Overview

A comprehensive integration test suite that verifies the entire pipeline from producer to dashboard, with a Locust load test for the API.

- **Prerequisites:** Phases 1–12 complete. All services healthy. Producers running. Spark job running.
- **Goal:** A pytest-based test suite with 15+ integration tests. A Locust load test. CI-friendly with configurable timeouts.
- **Outputs:** `tests/integration/test_pipeline.py` | `tests/integration/test_api.py` | `tests/load/locustfile.py` | `tests/conftest.py`

### LLM Prompt

```
You are a senior test engineer specializing in distributed system integration testing.
Build the complete test suite for a real-time Kafka fraud detection platform.

## tests/conftest.py
  Fixtures: kafka_producer, kafka_consumer_raw, async_http_client, pg_conn (with rollback)
  Helper: wait_for_condition(condition_fn, timeout=30, interval=1)

## tests/integration/test_pipeline.py
  test_transaction_flows_through_pipeline: verify geo_point trigger fires
  test_enrichment_adds_velocity: 3 txns same card → velocity_1m >= 3
  test_flash_fraud_triggers_alert: 15 txns same card → VELOCITY alert, severity >= MEDIUM
  test_zscore_triggers_alert: avg=50, std=10, amount=500 → DEVIATION alert, risk_score > 0.9
  test_blacklisted_card_triggers_alert: SADD blacklist → BLACKLIST_HIT alert, risk_score = 0.95
  test_malformed_message_goes_to_dlq: raw bytes → DLQ with error_reason
  test_alert_consumer_blacklists_blocked_card: alert with 'Block' → card appears in blacklist
  test_event_replay: reset offsets → counts increase, no duplicate transaction_ids

## tests/integration/test_api.py
  test_health_endpoint, test_tps_endpoint, test_kafka_lag_endpoint,
  test_recent_alerts, test_geo_alerts, test_flagged_cards,
  test_websocket_receives_alert (publish to fraud.alerts, assert WS receives within 5s)

## tests/load/locustfile.py
  50 concurrent users, wait 0.5–2s between tasks:
  40%: GET /api/metrics/tps
  20%: GET /api/kafka/lag
  20%: GET /api/alerts/recent
  10%: GET /api/alerts/geo?window_hours=1
  10%: GET /api/metrics/system
  Expected: all endpoints p95 < 500ms at 50 concurrent users

## requirements-test.txt
  pytest==7.4.3, pytest-asyncio==0.21.1, pytest-timeout==2.2.0,
  httpx==0.25.2, locust==2.19.1, testcontainers[kafka,postgresql]==3.7.1

## Constraints
  - All integration tests: @pytest.mark.timeout(90)
  - Use unique card_ids per test: f'TEST_CARD_{uuid4().hex[:8]}'
  - Tests must be runnable in any order (no implicit dependencies)
  - Tests that mutate shared state must clean up in teardown
```

---

## 🔄 PHASE 14 — Event Replay & Operational Runbooks

### Overview

Implement the event replay system and write operational runbooks for the most common failure and maintenance scenarios.

- **Prerequisites:** Phases 1–13 complete. Full platform running. PostgreSQL seeded with data.
- **Goal:** A replay CLI tool, a DLQ remediation tool, and 5 operational runbooks covering the most critical operational procedures.
- **Outputs:** `scripts/replay.py` | `scripts/dlq_replay.py` | `docs/runbooks/*.md`

### LLM Prompt

```
You are a senior data engineering operations lead.
Build the operational tooling and runbooks for a real-time Kafka fraud detection platform.

## scripts/replay.py — Click-based CLI
  replay offsets --group --topic [--to-datetime | --to-earliest | --to-offset] [--dry-run]
  replay status --group: show committed offsets, log-end-offset, lag as table
  replay verify --group --expected-count: wait up to 5 min for N messages processed
  replay rebuild-postgres --from-datetime [--dry-run]: rename tables, re-run migrations,
                                                       reset consumer group offsets

## scripts/dlq_replay.py — DLQ Remediation CLI
  dlq list [--limit 50]: print offset/original_topic/error_reason/failed_at table
  dlq inspect --offset N: fetch message, decode, show validation errors
  dlq replay --offset-range start end [--fix-script file] [--target-topic topic]
  dlq purge --before-datetime ISO_TIMESTAMP [--confirm]

## docs/runbooks/ — 5 Operational Runbooks (each with Overview, When to Use,
  Prerequisites, Step-by-step Procedure, Verification, Rollback)
  01-kafka-broker-failure.md: single broker crash and recovery
  02-consumer-lag-runbook.md: lag spike investigation decision tree
  03-schema-evolution-runbook.md: safely adding new Avro field
  04-dlq-investigation.md: investigating and replaying DLQ events
  05-monthly-partition-maintenance.md: creating partitions, archiving old ones + cron job

## Constraints
  - All CLI commands must have --help documentation (Click decorators)
  - All destructive operations must require --confirm flag
  - Runbooks written for an on-call engineer who has never seen this system
  - Every step must include the exact command to run + expected output
```

---

## 🤖 PHASE 15 — ML Extension — XGBoost + GPU Acceleration

### Overview

Replace rule-based thresholds with trained XGBoost and autoencoder models. Includes feature engineering, GPU-accelerated training on the RTX 2060, MLflow experiment tracking, and deploying the model as a Spark UDF in the streaming pipeline.

- **Prerequisites:** Phases 1–14 complete. PostgreSQL has at least 100K labeled transactions. RTX 2060 with CUDA 11.8+. PyTorch 2.0+ and XGBoost 2.0+ installed. MLflow running.
- **Goal:** Trained XGBoost fraud classifier with > 0.92 ROC-AUC, autoencoder for unsupervised anomaly detection, both tracked in MLflow, and an updated Spark job that applies ML scoring.
- **Outputs:** `ml/feature_engineering.py` | `ml/train_xgboost.py` | `ml/train_autoencoder.py` | `ml/model_registry.py` | `spark_jobs/ml_inference.py` | `ml/evaluate.py`

### LLM Prompt

```
You are a senior ML engineer specializing in fraud detection and real-time model serving.
Build the machine learning extension for a real-time Kafka fraud detection platform.
The training environment has an RTX 2060 GPU (6GB VRAM) with CUDA 11.8.

## ml/feature_engineering.py
  build_feature_matrix(pg_conn, lookback_days=60) -> tuple[pd.DataFrame, pd.Series]
  Features: amount, log_amount, z_score_30d, amount_vs_cat_mean, is_round_amount,
            velocity_1m, velocity_5m, velocity_1h, time_since_last_txn_s,
            hour_of_day, day_of_week, is_weekend, is_online,
            is_new_country, is_new_merchant_cat, fraud_flag_rate_30d
  class FraudFeaturePipeline: fit / transform / fit_transform / save / load
  Report class imbalance; return scale_pos_weight for XGBoost

## ml/train_xgboost.py
  Split 70/15/15 stratified. XGBoost with device='cuda', tree_method='hist',
  scale_pos_weight, early_stopping_rounds=20, eval_set on validation.
  Evaluate: ROC-AUC, PR-AUC, F1, confusion matrix, precision/recall/FPR at threshold 0.85
  SHAP analysis: top 100 test samples, summary plot, top 5 features
  MLflow tracking: all hyperparameters, metrics, confusion matrix, model, feature_pipeline.pkl

## ml/train_autoencoder.py
  Architecture: Input → 64 → 32 → 16 → 32 → 64 → Input (ReLU, BatchNorm)
  Train on legitimate transactions only (y==0)
  Loss: MSE; Optimizer: Adam lr=1e-3; batch_size=512; 50 epochs; early stopping patience=5
  Threshold: 99th percentile of reconstruction errors on legitimate holdout set
  MLflow: log threshold, val_reconstruction_error_p99, fraud_recall_at_threshold

## ml/evaluate.py
  Load both models from MLflow. Run on test set.
  Combined score: 0.7 * xgb_score + 0.3 * autoencoder_score
  Print comparison: rule-based vs XGBoost vs Autoencoder vs Ensemble
  Plot ROC curves for all three models

## spark_jobs/ml_inference.py
  load_model_for_spark(spark, model_uri): broadcast to all executors
  @pandas_udf('double') ml_fraud_score(*cols) -> pd.Series
  Updated fraud_detection_job.py: add ml_score column, filter ml_score > 0.85 as ML_SCORE alerts
  Controlled by ENABLE_ML_SCORING=true/false env var

## ml/requirements.txt
  torch==2.1.1+cu118, xgboost==2.0.2, shap==0.43.0, mlflow==2.8.1,
  scikit-learn==1.3.2, pandas==2.1.3, numpy==1.26.2, matplotlib==3.8.2

## Constraints
  - model_broadcast.value must be called inside the UDF, not outside
  - Autoencoder threshold must be serialized alongside the model (not hardcoded)
  - Training must fall back to CPU with a warning if GPU unavailable
  - All training scripts must be runnable standalone
```

---

## Appendix: Common LLM Follow-Up Prompts

Use these prompts when the LLM output is incomplete or needs adjustment.

### If the output is missing error handling:

```
The code above is missing proper error handling. For every function or method,
add try/except blocks that:
1. Catch specific exceptions (not bare except: or except Exception:)
2. Log the error with context using structured JSON: logger.error(json.dumps({...}))
3. Either re-raise, return a safe default, or send to DLQ depending on the context
4. Never silently swallow an exception

Please rewrite [FUNCTION_NAME] with complete error handling.
```

### If the output is missing tests:

```
The implementation above has no tests. Write pytest unit tests for [FILE_NAME] that:
1. Use mocks/fakes for all external dependencies (Kafka, Redis, PostgreSQL)
2. Test the happy path (correct input -> correct output)
3. Test at least 3 failure/edge cases
4. Are completely independent (no shared state between tests)
5. Have descriptive test names that read as sentences:
   test_velocity_returns_zero_for_unknown_card
```

### If the code has hardcoded values:

```
The code above has hardcoded values that should come from configuration.
Refactor [FILE_NAME] so that:
1. All hostnames, ports, credentials, and thresholds are read from environment variables
2. Each env var has a sensible default for local development
3. Add a validate_config() function that checks all required env vars are set
   and raises a clear ConfigurationError if any are missing
4. Document each env var in a comment: # KAFKA_BROKERS: comma-separated list of brokers
```

### If Kafka connection is not production-ready:

```
The Kafka producer/consumer config above is missing production-grade settings.
Update the config to add:
1. For producers: enable.idempotence=True, acks=all, retries=5,
   max.in.flight.requests.per.connection=5, delivery.timeout.ms=30000
2. For consumers: enable.auto.commit=False, partition.assignment.strategy=cooperative-sticky,
   max.poll.interval.ms=30000, session.timeout.ms=10000
3. For both: security.protocol=PLAINTEXT (for local Docker; change to SSL for production)
4. Add a connection retry loop that waits for the broker to be ready before starting
   (retry every 5 seconds for up to 2 minutes)
```

### If the Spark job is missing checkpointing:

```
The Spark Structured Streaming job above is not checkpoint-safe.
Update it to ensure:
1. Every writeStream has a unique .option('checkpointLocation', '/tmp/checkpoints/X')
   where X is a descriptive name (e.g., 'fraud-alerts-kafka', 'transactions-postgres')
2. The checkpoint directory is NOT deleted on restart (remove any rm -rf commands)
3. The Spark session has spark.sql.streaming.checkpointLocation set as a fallback
4. If a query fails, the job restarts it from the checkpoint rather than crashing
5. Add a monitoring loop that checks query.lastProgress every 60 seconds
   and logs inputRowsPerSecond and processingRate
```

---

*End of Build Playbook — 15 Phases | Real-Time Fraud Detection Platform*
