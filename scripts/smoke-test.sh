#!/usr/bin/env bash
# End-to-end test in both directions through the Kerberos-authenticated bridge.
#   Kafka  -> Solace : produce to $KAFKA_TOPIC_IN, expect it on Solace topic kafka/<topic> + queue
#   Solace -> Kafka  : publish to solace/orders/..., expect it on Kafka topic $KAFKA_TOPIC_OUT
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
set -a; # shellcheck disable=SC1091
source .env; set +a

DC="docker compose"
STAMP="smoke-$(date +%Y%m%dT%H%M%S)"
PASS=0; FAIL=0
ok()   { echo "  PASS  $*"; PASS=$((PASS+1)); }
bad()  { echo "  FAIL  $*"; FAIL=$((FAIL+1)); }

echo "=============================================================="
echo " 1) Kafka -> Solace   (${KAFKA_TOPIC_IN} -> kafka/${KAFKA_TOPIC_IN} -> ${SOLACE_QUEUE_FROM_KAFKA})"
echo "=============================================================="
before=$(${DC} run --rm -T solace-tools qcount "${SOLACE_QUEUE_FROM_KAFKA}" 2>/dev/null | tail -n1)
echo "  queue ${SOLACE_QUEUE_FROM_KAFKA} spooled before: ${before:-?}"

PEEK_OUT=$(mktemp)
${DC} run --rm -T solace-tools peek "kafka/#" 20 > "${PEEK_OUT}" 2>/dev/null &
PEEK_PID=$!
sleep 4   # let the MQTT subscription settle

if ${DC} exec -T kafka-client produce "${KAFKA_TOPIC_IN}" "${STAMP} hello-from-kafka"; then
  echo "  produced to Kafka topic ${KAFKA_TOPIC_IN}"
else
  bad "could not produce to Kafka"
fi

sleep 6
after=$(${DC} run --rm -T solace-tools qcount "${SOLACE_QUEUE_FROM_KAFKA}" 2>/dev/null | tail -n1)
echo "  queue ${SOLACE_QUEUE_FROM_KAFKA} spooled after:  ${after:-?}"
if [ -n "${before}" ] && [ -n "${after}" ] && [ "${after}" -gt "${before}" ]; then
  ok "message spooled on ${SOLACE_QUEUE_FROM_KAFKA} (+$((after-before)))"
else
  bad "queue count did not increase - check: docker compose run --rm solace-tools status"
fi

wait "${PEEK_PID}" 2>/dev/null || true
if grep -q "${STAMP}" "${PEEK_OUT}"; then
  ok "payload seen on Solace topic: $(grep "${STAMP}" "${PEEK_OUT}" | head -n1)"
else
  echo "  (no live MQTT capture of the payload - not fatal; peek output follows)"; cat "${PEEK_OUT}"
fi
rm -f "${PEEK_OUT}"

echo
echo "=============================================================="
echo " 2) Solace -> Kafka   (solace/orders/smoke -> ${SOLACE_QUEUE_TO_KAFKA} -> ${KAFKA_TOPIC_OUT})"
echo "=============================================================="
${DC} run --rm -T solace-tools publish "solace/orders/smoke" "${STAMP} hello-from-solace" \
  || bad "REST publish to Solace failed"
sleep 4
consumed=$(${DC} exec -T kafka-client consume "${KAFKA_TOPIC_OUT}" 15 2>/dev/null || true)
if echo "${consumed}" | grep -q "${STAMP}"; then
  ok "record received on Kafka topic ${KAFKA_TOPIC_OUT}: $(echo "${consumed}" | grep "${STAMP}" | head -n1)"
else
  bad "record not found on ${KAFKA_TOPIC_OUT}"; echo "${consumed}" | tail -n 5
fi

echo
echo "=============================================================="
echo " Result: ${PASS} passed, ${FAIL} failed"
echo "=============================================================="
[ "${FAIL}" -eq 0 ]
