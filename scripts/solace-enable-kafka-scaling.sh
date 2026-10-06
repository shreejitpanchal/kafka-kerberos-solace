#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Raise the Kafka bridging scaling parameters on the EXISTING Solace broker.
# A fresh software broker has max-kafka-bridges = 0 and
# max-kafka-broker-connections = 0 -> no Kafka receiver/sender can start.
#
# Two ways to fix it:
#   A) (this script) CLI:  configure > system > scaling >
#        scale max-kafka-bridges 10 max-kafka-broker-connections 300
#      -> the broker reloads its services automatically (short outage; on
#         10.25.1+ the container may restart depending on its restart policy)
#   B) recreate the Solace container with
#        -e system_scaling_maxkafkabridgecount=10
#        -e system_scaling_maxkafkabrokerconnectioncount=300
# Docs: https://docs.solace.com/Software-Broker/Set-Scaling-Params-Standalone.htm
# ---------------------------------------------------------------------------
set -euo pipefail
# shellcheck disable=SC1091
source "$(dirname "$0")/lib.sh"

BRIDGES="${SOLACE_MAX_KAFKA_BRIDGES:-10}"
CONNS="${SOLACE_MAX_KAFKA_BROKER_CONNECTIONS:-300}"
resolve_solace_container

cat <<EOF

About to run on ${SOLACE_CONTAINER} (docker exec -it ${SOLACE_CONTAINER} ${SOLACE_CLI} -A):

  enable
  configure
  system
  scaling
  scale max-kafka-bridges ${BRIDGES} max-kafka-broker-connections ${CONNS}
  end
  show kafka

This makes the broker reload (messaging is interrupted for a short time).
EOF
read -r -p "Continue? [y/N] " ans
[ "${ans}" = "y" ] || [ "${ans}" = "Y" ] || { echo "aborted"; exit 1; }

# the extra "y" answers a confirmation prompt if the CLI asks for one
solace_cli "enable" "configure" "system" "scaling" \
           "scale max-kafka-bridges ${BRIDGES} max-kafka-broker-connections ${CONNS}" "y" \
           "end" "show kafka" || true

echo
echo "Waiting for the broker to come back ..."
for _ in $(seq 1 60); do
  if docker exec "${SOLACE_CONTAINER}" bash -c "exec 3<>/dev/tcp/127.0.0.1/${SOLACE_SEMP_PORT:-1943}" 2>/dev/null; then
    echo "SEMP port is open again."; break
  fi
  sleep 5
done
echo "Verify with:  scripts/solace-cli.sh \"show kafka\"    (Max Kafka Bridges / Max Kafka Broker Connections)"
echo "Then re-run:  docker compose up -d   (re-applies the bridge configuration)"
