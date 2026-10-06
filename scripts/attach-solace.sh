#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# OPTIONAL "network mode" (not needed in the default setup).
# Connects your EXISTING Solace container to the compose network so that the broker can
# resolve "kdc" and "kafka" by name and the toolboxes reach it as "solace". Use it together
# with these .env settings:
#   SOLACE_HOST=solace   HOST_ALIAS=kafka
#   KDC_ADDRESS_FOR_SOLACE=kdc:88   KAFKA_BOOTSTRAP_FOR_SOLACE=kafka:9093
# Default setup instead uses your PC's published ports via host.docker.internal
# (see scripts/check-solace.sh). Idempotent.
# ---------------------------------------------------------------------------
set -euo pipefail
# shellcheck disable=SC1091
source "$(dirname "$0")/lib.sh"

resolve_solace_container
state=$(docker inspect -f '{{.State.Status}}' "${SOLACE_CONTAINER}")
[ "${state}" = "running" ] || echo "WARNING: ${SOLACE_CONTAINER} is '${state}', not running."

docker network inspect "${NET}" >/dev/null 2>&1 || docker network create "${NET}" >/dev/null

ALIAS="${SOLACE_HOST}"
if [ "${ALIAS}" = "host.docker.internal" ]; then ALIAS="solace"; fi
if solace_on_network; then
  echo "${SOLACE_CONTAINER} is already attached to network ${NET}"
else
  docker network connect --alias "${ALIAS}" "${NET}" "${SOLACE_CONTAINER}"
  echo "Attached ${SOLACE_CONTAINER} to ${NET} with alias '${ALIAS}'"
fi

echo
echo "Name resolution from inside the Solace container:"
for h in kdc kafka; do
  if out=$(docker exec "${SOLACE_CONTAINER}" getent hosts "${h}" 2>/dev/null); then
    echo "  ${out}"
  else
    echo "  ${h}: not resolvable yet (the kdc/kafka containers are not up, or not on ${NET})"
  fi
done
echo
echo "Now set in .env:  SOLACE_HOST=${ALIAS}  HOST_ALIAS=kafka  KDC_ADDRESS_FOR_SOLACE=kdc:88  KAFKA_BOOTSTRAP_FOR_SOLACE=kafka:9093"
echo "then run:         scripts/reset.sh   (the Kafka SPN changes, so keytabs must be regenerated)"
