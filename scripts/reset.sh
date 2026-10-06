#!/usr/bin/env bash
# Tear down KDC/Kafka INCLUDING volumes (KDC database + keytabs) and start again.
# New keytabs are generated and re-uploaded to the Solace broker (its own config
# is left in place and simply re-applied). The Solace container itself is untouched.
set -euo pipefail
# shellcheck disable=SC1091
source "$(dirname "$0")/lib.sh"

# detach the broker first, otherwise the network cannot be removed
if resolve_solace_container >/dev/null 2>&1 && solace_on_network; then
  docker network disconnect "${NET}" "${SOLACE_CONTAINER}" && echo "Detached ${SOLACE_CONTAINER} from ${NET}"
fi
docker compose down -v --remove-orphans
exec "${SCRIPT_DIR}/up.sh"
