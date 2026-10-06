#!/usr/bin/env bash
# Shared helpers for the host-side scripts (sourced, not executed).
# - loads .env
# - locates the existing Solace container (SOLACE_CONTAINER or auto-detect)
# - runs Solace CLI commands inside it

# Git Bash (MSYS) rewrites arguments that look like POSIX paths before docker sees them
# (/usr/sw/loads/... -> C:/Program Files/Git/usr/sw/loads/...). Disable that for everything
# these scripts run inside containers.
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*"

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# absolute path of this scripts/ folder: lib.sh cd-s to PROJECT_DIR below, so sibling scripts
# must be called as "${SCRIPT_DIR}/<name>.sh" (a relative $(dirname "$0") no longer resolves)
SCRIPT_DIR="${PROJECT_DIR}/scripts"
cd "${PROJECT_DIR}" || exit 1
# .env is git-ignored (holds your broker credentials); seed it from the committed sample on first use
if [ ! -f .env ]; then
  cp env.example .env
  echo "Created .env from env.example - review it (SOLACE_*, REALM, ports) and re-run if you change anything." >&2
fi
set -a
# shellcheck disable=SC1091
source .env
set +a

NET="${COMPOSE_NETWORK:-kafka-kerberos-net}"
SOLACE_HOST="${SOLACE_HOST:-solace}"
SOLACE_CLI=/usr/sw/loads/currentload/bin/cli

# Resolve SOLACE_CONTAINER: use .env value or auto-detect a single solace-pubsub container
resolve_solace_container() {
  if [ -n "${SOLACE_CONTAINER:-}" ]; then
    if ! docker inspect "${SOLACE_CONTAINER}" >/dev/null 2>&1; then
      echo "ERROR: container '${SOLACE_CONTAINER}' (SOLACE_CONTAINER in .env) not found. docker ps -a:" >&2
      docker ps -a --format '  {{.Names}}  {{.Image}}  {{.Status}}' >&2
      return 1
    fi
    return 0
  fi
  local found=()
  while IFS= read -r n; do [ -n "$n" ] && found+=("$n"); done < <(
    docker ps --format '{{.Names}}\t{{.Image}}' | awk -F'\t' '$2 ~ /solace-pubsub/ {print $1}')
  case ${#found[@]} in
    0) echo "ERROR: no running container with a solace/solace-pubsub-* image found." >&2
       echo "       Start your Solace broker, or set SOLACE_CONTAINER=<name> in .env" >&2
       return 1 ;;
    1) SOLACE_CONTAINER="${found[0]}"
       echo "Using Solace container: ${SOLACE_CONTAINER} (auto-detected; pin it with SOLACE_CONTAINER= in .env)" ;;
    *) echo "ERROR: several Solace containers are running: ${found[*]}" >&2
       echo "       Set SOLACE_CONTAINER=<name> in .env" >&2
       return 1 ;;
  esac
}

# Run Solace CLI commands non-interactively inside the broker container.
#   solace_cli "show version" "show message-vpn default kafka"
# Tries `cli -A -es <script>` first, falls back to feeding the commands on stdin.
solace_cli() {
  local script=/usr/sw/jail/cliscripts/_adhoc.cli
  { printf '%s\n' "$@"; echo "exit"; } | docker exec -i "${SOLACE_CONTAINER}" bash -c "cat > ${script}"
  if ! docker exec "${SOLACE_CONTAINER}" ${SOLACE_CLI} -A -es _adhoc.cli 2>/dev/null; then
    printf '%s\n' "$@" | docker exec -i "${SOLACE_CONTAINER}" ${SOLACE_CLI} -A
  fi
}

# Is the broker already connected to our network?
solace_on_network() {
  docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{println $k}}{{end}}' "${SOLACE_CONTAINER}" \
    | grep -qx "${NET}"
}
