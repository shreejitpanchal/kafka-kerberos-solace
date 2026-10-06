#!/usr/bin/env bash
# Build + start KDC/Kafka/toolboxes, check the existing Solace broker is reachable the way
# the stack expects (https://localhost:1943, host.docker.internal:88 / :9093 from the broker),
# then follow the Solace configuration job until it finishes.
set -euo pipefail
# shellcheck disable=SC1091
source "$(dirname "$0")/lib.sh"

docker compose up -d --build

echo
echo "=== Checking the existing Solace broker ==="
if ! "${SCRIPT_DIR}/check-solace.sh"; then
  echo
  echo "Some checks failed. The solace-config job keeps polling SEMP for 10 minutes; fix the problem"
  echo "(see messages above / README section 8) and run 'docker compose up -d' again if the job gave up."
fi

echo
echo "=== Solace configuration job (realm -> keytab -> Kafka bridge) ==="
docker compose logs -f solace-config &
LOGPID=$!
while :; do
  st=$(docker inspect -f '{{.State.Status}}' solace-config 2>/dev/null || echo missing)
  [ "${st}" = "running" ] || [ "${st}" = "created" ] || break
  sleep 2
done
sleep 1; kill "${LOGPID}" 2>/dev/null || true
rc=$(docker inspect -f '{{.State.ExitCode}}' solace-config 2>/dev/null || echo 1)
echo
if [ "${rc}" = "0" ]; then
  echo "Done. Bridge configured on VPN '${SOLACE_VPN:-default}' (PubSub+ Manager: https://localhost:${SOLACE_SEMP_PORT:-1943})."
  echo
  echo "=== Keeping a copy of the bridge keytabs in scripts/keytabs/ ==="
  "${SCRIPT_DIR}/export-keytabs.sh" || echo "WARNING: keytab export failed - run scripts/export-keytabs.sh later"
  echo "Next: scripts/smoke-test.sh   |   docker compose run --rm solace-tools status"
else
  echo "solace-config exited with code ${rc} - see: docker compose logs solace-config"
  exit "${rc}"
fi
