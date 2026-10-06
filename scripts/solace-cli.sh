#!/usr/bin/env bash
# Run one or more Solace CLI commands non-interactively inside your Solace container.
#   scripts/solace-cli.sh "show message-vpn default kafka"
#   scripts/solace-cli.sh "show message-vpn default kafka kafka-receiver kr-kafka-kerberos keytab" \
#                         "show message-vpn default kerberos realm @SOLACE.LOCAL"
# Interactive alternative:  docker exec -it <solace-container> /usr/sw/loads/currentload/bin/cli -A
set -euo pipefail
# shellcheck disable=SC1091
source "$(dirname "$0")/lib.sh"
[ $# -gt 0 ] || { echo "usage: $0 \"<cli command>\" [\"<cli command>\" ...]" >&2; exit 2; }
resolve_solace_container >/dev/null
solace_cli "$@"
