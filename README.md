# Fraud Detection Platform

A real-time fraud detection platform built on Kafka (KRaft), Apache Spark, Redis, and PostgreSQL — containerised with Docker Compose and designed to run entirely inside WSL2 on a developer machine.

> **Built & tested on:** Windows 11 + WSL2 (Ubuntu 22.04), Docker Desktop 4.26+, 40 GB RAM

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Prerequisites](#prerequisites)
- [WSL2 Performance Setup](#wsl2-performance-setup)
- [Quick Start](#quick-start)
- [Image Version Notes](#image-version-notes)
- [Service URLs](#service-urls)
- [Resource Budget](#resource-budget)
- [Kafka Topics](#kafka-topics)
- [Network Topology](#network-topology)
- [Project Structure](#project-structure)
- [Day-to-Day Operations](#day-to-day-operations)
- [Troubleshooting](#troubleshooting)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  kafka-net  (172.20.0.0/24)                                     │
│                                                                  │
│  ┌────────┐  ┌────────┐  ┌────────┐   ┌─────────────────────┐  │
│  │ kafka1 │  │ kafka2 │  │ kafka3 │   │   schema-registry   │  │
│  │  :9092 │  │  :9093 │  │  :9094 │   │        :8081        │  │
│  └────────┘  └────────┘  └────────┘   └─────────────────────┘  │
│                                                                  │
│  ┌──────────────┐   ┌───────────────┐  ┌───────────────┐       │
│  │ spark-master │   │ spark-worker-1│  │ spark-worker-2│       │
│  │  :8080/:7077 │   │    (5G/4CPU)  │  │    (5G/4CPU)  │       │
│  └──────────────┘   └───────┬───────┘  └───────┬───────┘       │
└──────────────────────────────┼──────────────────┼───────────────┘
                               │   data-net        │
┌──────────────────────────────┼──────────────────┼───────────────┐
│  data-net  (172.21.0.0/24)   │                  │               │
│                              └─────────┬─────────┘               │
│  ┌──────────────┐  ┌───────────────┐   │                         │
│  │    redis     │  │   postgres    │   │  (workers span both)    │
│  │    :6379     │  │    :5432      │   │                         │
│  └──────────────┘  └───────────────┘                             │
│  ┌────────────────┐  ┌──────────────────┐                        │
│  │ redis-exporter │  │ postgres-exporter│                        │
│  │     :9121      │  │      :9187       │                        │
│  └────────────────┘  └──────────────────┘                        │
└───────────────────────────────────────────────────────────────────┘

Prometheus (:9090) and Grafana (:3000) span BOTH networks.
```

---

## Prerequisites

| Tool | Minimum Version | Install |
|------|----------------|---------|
| Docker Desktop | 4.26+ | https://docs.docker.com/desktop/windows/install/ |
| Docker Compose | V2 (built-in) | Included with Docker Desktop |
| WSL2 | Ubuntu 22.04+ | `wsl --install -d Ubuntu` in PowerShell |
| RAM | 40 GB total (32 GB to WSL2) | See WSL2 setup below |
| CPU | i7-class, 12 logical cores | — |
| Disk | 50 GB free inside WSL2 | — |

Verify your setup:

```bash
docker compose version      # must show >= 2.20
docker info | grep -i memory
free -h                     # should show ~32 GB after .wslconfig
```

---

## WSL2 Performance Setup

**Critical:** All project files and Docker volumes MUST live inside the WSL2 filesystem. Cross-filesystem I/O from `/mnt/c/...` is 10–20× slower and will cause Kafka and PostgreSQL to time out on startup.

### 1. Configure WSL2 memory

Create or edit `C:\Users\<YourUser>\.wslconfig` in Windows:

```ini
[wsl2]
memory=32GB
processors=8
swap=8GB
swapfile=C:\\Users\\<YourUser>\\AppData\\Local\\Temp\\wsl-swap.vhdx
sparseVhd=false
pageReporting=false

[experimental]
networkingMode=mirrored
autoMemoryReclaim=dropcache
```

After saving, restart WSL2 from PowerShell:

```powershell
wsl --shutdown
```

### 2. Verify Docker is using WSL2 filesystem

```bash
docker info | grep "Docker Root Dir"
# Expected: /var/lib/docker  (NOT /mnt/c/...)
```

### 3. Always work inside WSL2 home directory

```bash
# CORRECT
cd ~/projects/fraud-detection-platform

# WRONG — 10-20x slower
cd /mnt/c/Users/yourname/projects
```

---

## Quick Start

```bash
# 1. Start the stack (pulls images on first run — ~10 min, ~6 GB)
docker compose up -d

# 2. Create Kafka topics and verify the cluster
bash scripts/init_platform.sh

# 3. Confirm every service is green
bash scripts/health_check.sh
```

Open Grafana at http://localhost:3000 — `admin` / password is in `.env`.

**Daily workflow:**
```bash
docker compose start    # resume — preserves all data
docker compose stop     # pause — preserves all data
docker compose down -v  # DANGER: deletes all data volumes
```

---

## Image Version Notes

| Service | Originally Specified | Actual Image Used | Reason |
|---------|---------------------|-------------------|--------|
| Spark | `bitnami/spark:3.4.2` | `spark:3.5.7-java17-python3` | Bitnami removed all versioned tags from Docker Hub in late 2024. Using the official Apache Spark image instead. |
| Kafka JMX | Enabled on port 9101 | **Disabled** | `cp-kafka:7.6.1` crashes on startup without a `jmxremote.password` file. JMX can be re-added later via the jmx_prometheus_javaagent sidecar. |
| All others | As specified | As specified | — |

---

## Service URLs

| Service | Host URL | Internal URL | Notes |
|---------|----------|--------------|-------|
| Kafka broker 1 | `localhost:9092` | `kafka1:9092` | Primary bootstrap |
| Kafka broker 2 | `localhost:9093` | `kafka2:9092` | |
| Kafka broker 3 | `localhost:9094` | `kafka3:9092` | |
| Schema Registry | http://localhost:8081 | `schema-registry:8081` | REST API |
| Spark Master UI | http://localhost:8080 | `spark-master:8080` | Job monitoring |
| Spark cluster | — | `spark://spark-master:7077` | Internal only |
| Redis | `localhost:6379` | `redis:6379` | |
| PostgreSQL | `localhost:5432` | `postgres:5432` | DB: frauddb |
| Prometheus | http://localhost:9090 | `prometheus:9090` | |
| Grafana | http://localhost:3000 | `grafana:3000` | admin / see `.env` |
| Redis exporter | http://localhost:9121/metrics | `redis-exporter:9121` | |
| PG exporter | http://localhost:9187/metrics | `postgres-exporter:9187` | |

---

## Resource Budget

| Service | CPU limit | RAM limit | Notes |
|---------|-----------|-----------|-------|
| kafka1 | 2 | 2 GB | JVM heap 1.5 GB |
| kafka2 | 2 | 2 GB | |
| kafka3 | 2 | 2 GB | |
| schema-registry | 0.5 | 512 MB | |
| spark-master | 1 | 2 GB | |
| spark-worker-1 | 4 | 6 GB | 5 GB worker memory |
| spark-worker-2 | 4 | 6 GB | 5 GB worker memory |
| redis | 0.5 | 4 GB | 3 GB maxmemory |
| postgres | 1 | 4 GB | 1 GB shared_buffers |
| prometheus | 0.5 | 1 GB | |
| grafana | 0.5 | 512 MB | |
| redis-exporter | 0.2 | 128 MB | |
| postgres-exporter | 0.2 | 128 MB | |
| **Total** | **≈18.4** | **≈31 GB** | Fits in 32 GB WSL2 alloc |

---

## Kafka Topics

| Topic | Partitions | RF | Retention | Purpose |
|-------|-----------|-----|-----------|---------|
| `transactions.raw` | 12 | 3 | 12 hours | Raw inbound transactions |
| `transactions.enriched` | 12 | 3 | 24 hours | Enriched after lookup joins |
| `fraud.alerts` | 3 | 3 | 7 days | Confirmed fraud alerts |
| `fraud.scores` | 6 | 3 | 6 hours | ML model scoring results |
| `dead-letter-queue` | 1 | 3 | 30 days | Failed/unprocessable messages |

Topics are created by `scripts/init_platform.sh`. Auto-create is disabled.

---

## Network Topology

| Network | Subnet | Members |
|---------|--------|---------|
| `kafka-net` | 172.20.0.0/24 | kafka1-3, schema-registry, spark-master |
| `data-net` | 172.21.0.0/24 | redis, postgres, redis-exporter, postgres-exporter |
| both | — | spark-worker-1/2, prometheus, grafana |

Spark workers span both networks — they read from Kafka and write results to PostgreSQL/Redis.

---

## Project Structure

```
fraud-detection-platform/
├── .env                          # Secrets & config (never commit)
├── docker-compose.yml
├── README.md
├── scripts/
│   ├── init_platform.sh          # Creates Kafka topics, verifies cluster
│   └── health_check.sh          # Checks every service, exits 0/1
├── docker/
│   ├── prometheus/prometheus.yml          # Scrape targets
│   └── grafana/provisioning/
│       ├── datasources/prometheus.yml     # Auto-registers Prometheus
│       └── dashboards/dashboard.yml       # Drop *.json dashboards here
├── producers/                    # Kafka producer microservices
├── consumers/                    # Kafka consumer microservices
├── spark_jobs/                   # PySpark streaming jobs
├── schemas/                      # Avro / JSON Schema definitions
├── api/
│   ├── routes/                   # REST API handlers
│   └── websocket/                # WebSocket push handlers
├── react-dashboard/src/
│   ├── components/
│   └── hooks/
├── ml/                           # Model training & inference
├── tests/
│   ├── unit/
│   ├── integration/
│   └── load/
├── db/
│   ├── migrations/               # Auto-run by PostgreSQL on first start
│   └── seeds/
└── monitoring/                   # Alert rules, runbooks
```

---

## Day-to-Day Operations

```bash
# Start everything
docker compose up -d

# health checkup after 30 secs
watch -n3 "docker compose ps --format 'table {{.Name}}\t{{.Status}}'"

# Stop without deleting volumes
docker compose stop

# Destroy everything including volumes (data loss!)
docker compose down -v

# Tail logs for a service
docker compose logs -f kafka1

# Open a Kafka shell
docker exec -it kafka1 bash

# List topics
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 --list

# Consume messages
docker exec kafka1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic transactions.raw \
  --from-beginning --max-messages 10

# Connect to PostgreSQL
docker exec -it postgres psql -U fraud_user -d frauddb

# Check Redis memory
docker exec redis redis-cli INFO memory | grep used_memory_human

# Check Schema Registry subjects
curl http://localhost:8081/subjects

# Reload Prometheus config without restart
curl -X POST http://localhost:9090/-/reload

# Restart a single service
docker compose restart schema-registry
```

---

## Troubleshooting

### 1. Kafka crashes on startup — JMX password file error

**Symptom:** `docker logs kafka1` shows:
```
Error: Password file not found: .../jmxremote.password
jdk.internal.agent.AgentConfigurationError
```

**Cause:** `cp-kafka:7.6.1` tries to enable JMX with authentication but the password file doesn't exist in the container. Setting `authenticate=false` in `KAFKA_JMX_OPTS` is silently ignored by this image.

**Fix:** Remove all `KAFKA_JMX_PORT`, `KAFKA_JMX_OPTS`, and `KAFKA_JMX_HOSTNAME` lines from `docker-compose.yml`. Already done in this repo.

---

### 2. `bitnami/spark` image not found

**Symptom:**
```
Error response from daemon: failed to resolve reference "docker.io/bitnami/spark:3.4.2": not found
```

**Cause:** Bitnami removed all tags from Docker Hub in late 2024. Even `bitnami/spark:latest` no longer exists there.

**Fix:** Already resolved — this repo uses `spark:3.5.7-java17-python3` (official Apache image).

---

### 3. Docker can't pull images — all show "Interrupted"

**Symptom:** Every image pull fails, but `curl https://google.com` works fine from WSL2.

**Cause:** Docker Desktop's DNS breaks in WSL2 after sleep or restart.

**Fix:**
```bash
sudo mkdir -p /etc/docker
sudo bash -c 'echo "{\"dns\": [\"8.8.8.8\", \"8.8.4.4\"]}" > /etc/docker/daemon.json'
```
Right-click Docker Desktop tray → Restart Docker Desktop → wait 60 seconds → retry.

---

### 4. Kafka brokers stuck in "starting" — CLUSTER_ID mismatch

**Symptom:** Brokers won't become healthy after a restart, especially if `.env` was edited.

**Cause:** Volumes were already formatted with a different `CLUSTER_ID`.

**Fix:**
```bash
docker compose down -v    # wipes volumes
docker compose up -d
```

Verify CLUSTER_ID is exactly 22 characters:
```bash
echo -n "$(grep KAFKA_CLUSTER_ID .env | cut -d= -f2)" | wc -c
# Must print: 22
```

---

### 5. `redis-exporter` shows unhealthy

**Symptom:** `bash scripts/health_check.sh` reports `[FAIL] redis-exporter`.

**Cause:** The `redis_exporter:v1.55.0` image has no shell, `curl`, or `wget` — Docker can't run a healthcheck command inside it.

**Verify it's actually working:**
```bash
curl http://localhost:9121/metrics | head -5
# Returns metrics = exporter is fine
```

The healthcheck is disabled for this container in `docker-compose.yml`. `health_check.sh` checks it via HTTP from the host instead.

---

### 6. Schema Registry — "Leader not available"

**Symptom:** `docker compose logs schema-registry` shows repeated `LeaderNotAvailableException`.

**Cause:** Schema Registry started before Kafka elected partition leaders for its `_schemas` topic.

**Fix:**
```bash
docker compose restart schema-registry
```

---

### 7. Out-of-memory — container killed (exit code 137)

**Symptom:** Container exits immediately, `docker inspect <name>` shows `OOMKilled: true`.

**Fix:** Confirm `.wslconfig` has `memory=32GB` and you ran `wsl --shutdown` after editing it:
```bash
free -h    # should show ~32 GB
```
If Spark workers are the culprit, temporarily lower `SPARK_WORKER_MEMORY` from `5G` to `3G` in `docker-compose.yml`.

---

*Platform built for local development. Not hardened for production.*
