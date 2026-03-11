#!/usr/bin/env bash
# =============================================================================
# init_platform.sh — Create Kafka topics for the Fraud Detection Platform
# Usage: bash scripts/init_platform.sh
# =============================================================================

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

info()    { echo -e "${CYAN}[INFO]${RESET}  $*"; }
success() { echo -e "${GREEN}[OK]${RESET}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
error()   { echo -e "${RED}[FAIL]${RESET}  $*" >&2; }
banner()  { echo -e "\n${BOLD}${CYAN}═══════════════════════════════════════${RESET}"; \
            echo -e "${BOLD}${CYAN}  $*${RESET}"; \
            echo -e "${BOLD}${CYAN}═══════════════════════════════════════${RESET}\n"; }

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka1}"
BOOTSTRAP_SERVER="localhost:9092"
MAX_WAIT_SECONDS=300
POLL_INTERVAL=5

# name|partitions|replication-factor|retention-ms
TOPICS=(
  "transactions.raw|12|3|43200000"
  "transactions.enriched|12|3|86400000"
  "fraud.alerts|3|3|604800000"
  "fraud.scores|6|3|21600000"
  "dead-letter-queue|1|3|2592000000"
)

# ── Step 1: Wait for kafka1 healthy ──────────────────────────────────────────
banner "Step 1 · Waiting for ${KAFKA_CONTAINER} to be healthy"

waited=0
while true; do
  STATUS=$(docker inspect --format='{{.State.Health.Status}}' \
           "${KAFKA_CONTAINER}" 2>/dev/null || echo "not_found")
  case "${STATUS}" in
    healthy)   success "${KAFKA_CONTAINER} healthy (${waited}s)"; break ;;
    not_found) error "Container not found. Run: docker compose up -d"; exit 1 ;;
    *)
      [[ ${waited} -ge ${MAX_WAIT_SECONDS} ]] && \
        { error "Timed out after ${MAX_WAIT_SECONDS}s (status: ${STATUS})"; exit 1; }
      info "Status: ${STATUS} — retrying in ${POLL_INTERVAL}s… (${waited}/${MAX_WAIT_SECONDS}s)"
      sleep "${POLL_INTERVAL}"; waited=$(( waited + POLL_INTERVAL )) ;;
  esac
done
sleep 3

# ── Step 2: Create topics ─────────────────────────────────────────────────────
banner "Step 2 · Creating Kafka topics"
CREATED=0; SKIPPED=0; FAILED=0

for topic_def in "${TOPICS[@]}"; do
  IFS='|' read -r topic_name partitions replication retention_ms <<< "${topic_def}"
  info "Creating: ${BOLD}${topic_name}${RESET} (p=${partitions} rf=${replication} ret=${retention_ms}ms)"

  OUTPUT=$(docker exec "${KAFKA_CONTAINER}" \
      kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}" \
      --create --if-not-exists \
      --topic "${topic_name}" \
      --partitions "${partitions}" \
      --replication-factor "${replication}" \
      --config "retention.ms=${retention_ms}" \
      --config "min.insync.replicas=2" \
      --config "compression.type=lz4" 2>&1 || true)

  if echo "${OUTPUT}" | grep -q "Created topic"; then
    success "  ✓  ${topic_name}"; CREATED=$(( CREATED + 1 ))
  elif echo "${OUTPUT}" | grep -q "already exists"; then
    warn "  ~  ${topic_name} already exists"; SKIPPED=$(( SKIPPED + 1 ))
  else
    error "  ✗  ${topic_name}: ${OUTPUT}"; FAILED=$(( FAILED + 1 ))
  fi
done

# ── Step 3: Verify ────────────────────────────────────────────────────────────
banner "Step 3 · Verifying topics"
ALL_OK=true
for topic_def in "${TOPICS[@]}"; do
  IFS='|' read -r topic_name _ _ _ <<< "${topic_def}"
  OUT=$(docker exec "${KAFKA_CONTAINER}" kafka-topics \
        --bootstrap-server "${BOOTSTRAP_SERVER}" --describe \
        --topic "${topic_name}" 2>&1)
  if echo "${OUT}" | grep -q "Topic: ${topic_name}"; then
    LEADERS=$(echo "${OUT}" | grep -c "Leader:" || true)
    success "${topic_name}  (${LEADERS} leaders confirmed)"
  else
    error "${topic_name} — describe failed"; ALL_OK=false
  fi
done

# ── Summary ───────────────────────────────────────────────────────────────────
banner "Summary"
echo -e "  Created: ${GREEN}${CREATED}${RESET}  Skipped: ${YELLOW}${SKIPPED}${RESET}  Failed: ${RED}${FAILED}${RESET}"

[[ "${ALL_OK}" == "true" && "${FAILED}" -eq 0 ]] && \
  { success "Platform ready! 🎉"; exit 0; } || \
  { error "Finished with errors."; exit 1; }