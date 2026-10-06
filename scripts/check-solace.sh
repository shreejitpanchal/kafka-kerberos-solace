#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Pre-flight checks against your EXISTING Solace PubSub+ container:
#   - SEMP reachable the way the toolboxes will reach it (https://localhost:1943 from the host)
#   - the broker can resolve/reach the KDC and Kafka through your PC's published ports
#     (host.docker.internal:88 / :9093 by default)
#   - Kafka bridging scaling parameters (> 0 required; broker default is 0)
# Nothing is changed by this script.
# ---------------------------------------------------------------------------
set -uo pipefail
# shellcheck disable=SC1091
source "$(dirname "$0")/lib.sh"

SEMP_SCHEME="${SOLACE_SEMP_SCHEME:-https}"; SEMP_PORT="${SOLACE_SEMP_PORT:-1943}"
KDC_ADDR="${KDC_ADDRESS_FOR_SOLACE:-host.docker.internal:88}"
KAFKA_ADDR="${KAFKA_BOOTSTRAP_FOR_SOLACE:-host.docker.internal:9093}"
ok=0; bad=0
pass() { echo "  OK    $*"; ok=$((ok+1)); }
fail() { echo "  FAIL  $*"; bad=$((bad+1)); }

echo "1) SEMP from this PC: ${SEMP_SCHEME}://localhost:${SEMP_PORT}  (user ${SOLACE_ADMIN_USER:-admin})"
code=$(curl -sk -o /dev/null -w '%{http_code}' -u "${SOLACE_ADMIN_USER:-admin}:${SOLACE_ADMIN_PASSWORD:-admin}" \
       "${SEMP_SCHEME}://localhost:${SEMP_PORT}/SEMP/v2/config/about/api" 2>/dev/null || echo 000)
case "${code}" in
  200) ver=$(curl -sk -u "${SOLACE_ADMIN_USER:-admin}:${SOLACE_ADMIN_PASSWORD:-admin}" \
             "${SEMP_SCHEME}://localhost:${SEMP_PORT}/SEMP/v2/config/about/api" 2>/dev/null | sed -n 's/.*"sempVersion":"\([^"]*\)".*/\1/p')
       pass "SEMP answers (version ${ver:-?})" ;;
  401|403) fail "SEMP rejects the credentials (HTTP ${code}) - fix SOLACE_ADMIN_USER/PASSWORD in .env" ;;
  *)   fail "SEMP not reachable (HTTP ${code}) - is the broker running and publishing ${SEMP_PORT}? (curl missing on this PC also shows 000)" ;;
esac

echo
echo "2) Solace container"
if resolve_solace_container; then
  state=$(docker inspect -f '{{.State.Status}}' "${SOLACE_CONTAINER}")
  [ "${state}" = "running" ] && pass "${SOLACE_CONTAINER} is running" || fail "${SOLACE_CONTAINER} is '${state}'"

  echo
  echo "3) From inside the broker: can it reach the KDC (${KDC_ADDR}) and Kafka (${KAFKA_ADDR})?"
  echo "   (ports are open only once 'docker compose up -d' has started kdc and kafka)"
  for addr in "${KDC_ADDR}" "${KAFKA_ADDR}"; do
    h="${addr%%:*}"; p="${addr##*:}"
    if docker exec "${SOLACE_CONTAINER}" getent hosts "${h}" >/dev/null 2>&1; then
      if docker exec "${SOLACE_CONTAINER}" bash -c "exec 3<>/dev/tcp/${h}/${p}" 2>/dev/null; then
        pass "${addr} resolves and accepts TCP connections"
      else
        fail "${addr}: name resolves but port ${p} is closed (stack not up yet, or KDC_HOST_PORT/KAFKA_HOST_PORT differ)"
      fi
    else
      fail "${h} does not resolve inside ${SOLACE_CONTAINER} (not Docker Desktop? use the network mode, README section 7)"
    fi
  done

  echo
  echo "4) Kafka bridging scaling parameters (must be > 0)"
  env_scaling=$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "${SOLACE_CONTAINER}" \
                | grep -E '^system_scaling_maxkafka' || true)
  if [ -n "${env_scaling}" ]; then
    echo "${env_scaling}" | sed 's/^/  container env: /'
  else
    echo "  container env: no system_scaling_maxkafka* keys (fine if you raised them via CLI)"
  fi
  echo "  live values ('show kafka'):"
  if out=$(solace_cli "show kafka" 2>/dev/null) && [ -n "${out}" ]; then
    echo "${out}" | grep -iE "max|bridge|connection" | sed 's/^/    /'
    if echo "${out}" | grep -iE "max.*kafka.*bridges?\b.*\b0\b|bridges?.*max.*\b0\b" >/dev/null 2>&1; then
      fail "Max Kafka Bridges looks like 0 - run scripts/solace-enable-kafka-scaling.sh"
    fi
  else
    echo "    (could not run the CLI non-interactively; check with: docker exec -it ${SOLACE_CONTAINER} ${SOLACE_CLI} -A  ->  show kafka)"
  fi
else
  fail "Solace container not found - CLI-based checks skipped (set SOLACE_CONTAINER in .env)"
fi

echo
echo "Result: ${ok} ok, ${bad} failed"
[ "${bad}" -eq 0 ]
