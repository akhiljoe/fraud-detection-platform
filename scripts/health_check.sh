#!/usr/bin/env bash
# =============================================================================
# health_check.sh — Verify every service.  Exit 0 = all healthy, 1 = failures.
# Usage: bash scripts/health_check.sh
# =============================================================================

set -uo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'; PAD=24

ok()   { printf "  ${GREEN}[OK]${RESET}   %-${PAD}s %s\n" "$1" "${2:-}"; }
fail() { printf "  ${RED}[FAIL]${RESET} %-${PAD}s %s\n" "$1" "${2:-}" >&2; }
skip() { printf "  ${YELLOW}[SKIP]${RESET} %-${PAD}s %s\n" "$1" "${2:-}"; }

container_health() { docker inspect --format='{{.State.Health.Status}}' "$1" 2>/dev/null || echo "missing"; }
http_ok() { curl -sf --max-time 5 "$1" > /dev/null 2>&1; }

check_service() {
  local label="$1" container="$2" extra="${3:-}"
  case "$(container_health "${container}")" in
    healthy)   ok   "${label}" "${extra}"; return 0 ;;
    unhealthy) fail "${label}" "unhealthy — docker logs ${container}"; return 1 ;;
    starting)  fail "${label}" "still starting"; return 1 ;;
    *)         fail "${label}" "not found"; return 1 ;;
  esac
}

# Load .env
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -f "${SCRIPT_DIR}/../.env" ]] && { set -o allexport; source "${SCRIPT_DIR}/../.env"; set +o allexport; }

FAILED=0

echo -e "\n${BOLD}${CYAN}╔══════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}${CYAN}║  Fraud Detection Platform — Health Check  ║${RESET}"
echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════╝${RESET}\n"
echo -e "  Timestamp: $(date -u '+%Y-%m-%dT%H:%M:%SZ')\n"

echo -e "${BOLD}  ── Kafka (KRaft) ──────────────────────────${RESET}"
check_service kafka1 kafka1 || FAILED=$(( FAILED + 1 ))
check_service kafka2 kafka2 || FAILED=$(( FAILED + 1 ))
check_service kafka3 kafka3 || FAILED=$(( FAILED + 1 ))
echo -n "  "
docker exec kafka1 kafka-broker-api-versions --bootstrap-server localhost:9092 \
  > /dev/null 2>&1 && ok "kafka-cluster-api" || { fail "kafka-cluster-api"; FAILED=$(( FAILED + 1 )); }

echo -e "\n${BOLD}  ── Schema Registry ─────────────────────────${RESET}"
check_service schema-registry schema-registry || FAILED=$(( FAILED + 1 ))

echo -e "\n${BOLD}  ── Spark ───────────────────────────────────${RESET}"
check_service spark-master   spark-master   || FAILED=$(( FAILED + 1 ))
check_service spark-worker-1 spark-worker-1 || FAILED=$(( FAILED + 1 ))
check_service spark-worker-2 spark-worker-2 || FAILED=$(( FAILED + 1 ))

echo -e "\n${BOLD}  ── Redis ───────────────────────────────────${RESET}"
check_service redis redis || FAILED=$(( FAILED + 1 ))
echo -n "  "
docker exec redis redis-cli ping 2>/dev/null | grep -q PONG \
  && ok "redis-ping" "PONG" || { fail "redis-ping"; FAILED=$(( FAILED + 1 )); }

echo -e "\n${BOLD}  ── PostgreSQL ──────────────────────────────${RESET}"
check_service postgres postgres || FAILED=$(( FAILED + 1 ))
echo -n "  "
docker exec postgres pg_isready -U "${POSTGRES_USER:-fraud_user}" \
  -d "${POSTGRES_DB:-frauddb}" > /dev/null 2>&1 \
  && ok "postgres-isready" || { fail "postgres-isready"; FAILED=$(( FAILED + 1 )); }

echo -e "\n${BOLD}  ── Monitoring ──────────────────────────────${RESET}"
check_service prometheus prometheus || FAILED=$(( FAILED + 1 ))
echo -n "  "
http_ok "http://localhost:${PROMETHEUS_PORT:-9090}/-/healthy" \
  && ok "prometheus-healthy" || { fail "prometheus-healthy"; FAILED=$(( FAILED + 1 )); }
check_service grafana grafana || FAILED=$(( FAILED + 1 ))

echo -e "\n${BOLD}  ── Exporters ───────────────────────────────${RESET}"
echo -n "  "
if curl -sf --max-time 5 "http://localhost:${REDIS_EXPORTER_PORT:-9121}/metrics" -o /dev/null; then
  ok "redis-exporter" "metrics endpoint responding"
else
  fail "redis-exporter" "http://localhost:${REDIS_EXPORTER_PORT:-9121}/metrics not responding"
  FAILED=$(( FAILED + 1 ))
fi
check_service postgres-exporter postgres-exporter || FAILED=$(( FAILED + 1 ))

echo -e "\n${BOLD}${CYAN}────────────────────────────────────────────${RESET}"
[[ "${FAILED}" -eq 0 ]] \
  && echo -e "  ${GREEN}${BOLD}ALL SERVICES HEALTHY${RESET}  🎉\n" && exit 0 \
  || echo -e "  ${RED}${BOLD}${FAILED} SERVICE(S) FAILING${RESET}\n" && exit 1