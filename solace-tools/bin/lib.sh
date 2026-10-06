#!/bin/bash
# Shared helpers for the Solace toolbox (SEMP v2 over curl + jq)

SEMP_URL="${SEMP_URL:-https://host.docker.internal:1943}"
SEMP_INSECURE="${SEMP_INSECURE:-true}"
SEMP_USER="${SEMP_USER:-admin}"
SEMP_PASSWORD="${SEMP_PASSWORD:-admin}"
SOLACE_REST_URL="${SOLACE_REST_URL:-http://host.docker.internal:9000}"

# TLS: the broker's default server certificate is self-signed -> accept it when asked to
CURL_TLS=()
if [ "${SEMP_INSECURE}" = "true" ] || [ "${SEMP_INSECURE}" = "1" ] || [ "${SEMP_INSECURE}" = "yes" ]; then
  CURL_TLS=(-k)
fi
SOLACE_MQTT_HOST="${SOLACE_MQTT_HOST:-host.docker.internal}"
SOLACE_MQTT_PORT="${SOLACE_MQTT_PORT:-1883}"

VPN="${VPN:-default}"
REALM="${REALM:-SOLACE.LOCAL}"
KDC_ADDRESS="${KDC_ADDRESS:-kdc:88}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9093}"
KAFKA_SERVICE_NAME="${KAFKA_SERVICE_NAME:-kafka}"
SOLACE_PRINCIPAL="${SOLACE_PRINCIPAL:-solace}"
KEYTAB_FILE="${KEYTAB_FILE:-/keytabs/solace.keytab}"

RECEIVER="${KAFKA_RECEIVER_NAME:-kr-kafka-kerberos}"
SENDER="${KAFKA_SENDER_NAME:-ks-kafka-kerberos}"
CONSUMER_GROUP="${KAFKA_CONSUMER_GROUP:-solace-bridge}"
TOPIC_IN="${KAFKA_TOPIC_IN:-orders-in}"
TOPIC_OUT="${KAFKA_TOPIC_OUT:-orders-out}"
Q_FROM_KAFKA="${SOLACE_QUEUE_FROM_KAFKA:-q.from-kafka}"
Q_TO_KAFKA="${SOLACE_QUEUE_TO_KAFKA:-q.to-kafka}"
TOPIC_TO_KAFKA="${SOLACE_TOPIC_TO_KAFKA:-solace/orders/>}"

# Solace topic generated for each Kafka record (substitution expression,
# evaluated by the broker - the single quotes keep bash away from it)
LOCAL_TOPIC_EXPR='kafka/${kafkaTopic()}'

# Keytab file names as stored in the broker's keytabs folder
RECEIVER_KEYTAB_NAME="${RECEIVER_KEYTAB_NAME:-solace-kafka-receiver.keytab}"
SENDER_KEYTAB_NAME="${SENDER_KEYTAB_NAME:-solace-kafka-sender.keytab}"

CFG="${SEMP_URL}/SEMP/v2/config"
MON="${SEMP_URL}/SEMP/v2/monitor"

log()  { echo "[solace-tools] $*"; }
die()  { echo "[solace-tools] ERROR: $*" >&2; exit 1; }
hr()   { printf '%s\n' "------------------------------------------------------------------"; }

# curl wrapper: sets globals  RESP (body)  and  HTTP_CODE  (never call in a subshell)
#   semp <METHOD> <url> [json-data]
RESP=""; HTTP_CODE="000"
semp() {
  local method="$1" url="$2" data="${3:-}"
  local tmp; tmp=$(mktemp)
  if [ -n "${data}" ]; then
    HTTP_CODE=$(curl -sS "${CURL_TLS[@]}" -o "${tmp}" -w '%{http_code}' -u "${SEMP_USER}:${SEMP_PASSWORD}" \
        -H 'Content-Type: application/json' -X "${method}" "${url}" -d "${data}") || HTTP_CODE="000"
  else
    HTTP_CODE=$(curl -sS "${CURL_TLS[@]}" -o "${tmp}" -w '%{http_code}' -u "${SEMP_USER}:${SEMP_PASSWORD}" \
        -H 'Content-Type: application/json' -X "${method}" "${url}") || HTTP_CODE="000"
  fi
  RESP=$(cat "${tmp}" 2>/dev/null || true); rm -f "${tmp}"
}

# error details of the last SEMP call, one line
semp_err() { echo "${RESP}" | jq -c '.meta.error // .' 2>/dev/null || echo "${RESP}"; }

urlenc() { jq -rn --arg v "$1" '$v|@uri'; }

SOLACE_CONTAINER="${SOLACE_CONTAINER:-<solace-container>}"
SEMP_HOST=$(echo "${SEMP_URL}" | sed -E 's#^https?://##; s#[:/].*$##')

wait_for_semp() {
  local i hint
  local code
  for i in $(seq 1 120); do
    code=$(curl -sS "${CURL_TLS[@]}" -o /dev/null -w '%{http_code}' -u "${SEMP_USER}:${SEMP_PASSWORD}" "${CFG}/about/api" 2>/dev/null || true)
    code="${code:-000}"
    case "${code}" in
      200) return 0 ;;
      401|403) die "SEMP at ${SEMP_URL} rejected user '${SEMP_USER}' (HTTP ${code}) - check SOLACE_ADMIN_USER/SOLACE_ADMIN_PASSWORD in .env" ;;
    esac
    hint=""
    if ! getent hosts "${SEMP_HOST}" >/dev/null 2>&1; then
      if [ "${SEMP_HOST}" = "host.docker.internal" ]; then
        hint=" - 'host.docker.internal' does not resolve here (not Docker Desktop?) - see README section 7 (network mode)"
      else
        hint=" - '${SEMP_HOST}' does not resolve: attach the Solace container with scripts/attach-solace.sh or fix SOLACE_HOST"
      fi
    elif [ "${code}" = "000" ] && [ -z "${CURL_TLS[*]}" ] && [[ "${SEMP_URL}" == https://* ]]; then
      hint=" - TLS handshake failing? set SOLACE_SEMP_INSECURE=true for the broker's self-signed certificate"
    fi
    log "waiting for SEMP at ${SEMP_URL} ... (${i}/120, last HTTP code ${code})${hint}"
    sleep 5
  done
  die "SEMP not reachable at ${SEMP_URL}. Run scripts/check-solace.sh, fix SOLACE_HOST/SOLACE_SEMP_SCHEME/SOLACE_SEMP_PORT in .env if needed, then re-run 'docker compose up -d'."
}

# Verify the broker exposes the Kerberos attributes for Kafka bridging.
# SEMP 2.40 (broker 10.7.1) introduced them; newer brokers report the SEMP
# version as the broker version itself (e.g. 10.26.7.12655).
check_semp_version() {
  local ver
  semp GET "${CFG}/about/api"
  ver=$(echo "${RESP}" | jq -r '.data.sempVersion // "unknown"')
  log "SEMP version: ${ver}  platform: $(echo "${RESP}" | jq -r '.data.platform // "?"')"
  case "${ver}" in
    2.*)
      local minor; minor=$(echo "${ver}" | cut -d. -f2)
      if [ "${minor}" -lt 40 ]; then
        die "SEMP ${ver} is older than 2.40 - Kerberos for Kafka bridging needs broker 10.7.1+"
      fi ;;
    unknown) die "could not read SEMP version from ${CFG}/about/api" ;;
    *) : ;;   # 10.x.y.z style - new enough
  esac
}

# Create-or-update an object.
#   semp_upsert <collection-path> <key-attribute> <json-body>
# POST to the collection; on ALREADY_EXISTS -> PATCH the object (key removed).
semp_upsert() {
  local coll="$1" keyattr="$2" body="$3"
  local name enc status patch
  name=$(echo "${body}" | jq -r ".${keyattr}")
  enc=$(urlenc "${name}")

  semp POST "${CFG}/${coll}" "${body}"
  if [ "${HTTP_CODE}" = "200" ]; then
    log "created  ${coll}/${name}"
    return 0
  fi
  status=$(echo "${RESP}" | jq -r '.meta.error.status // empty' 2>/dev/null || true)
  if [ "${status}" = "ALREADY_EXISTS" ]; then
    patch=$(echo "${body}" | jq -c "del(.${keyattr})")
    if [ "${patch}" = "{}" ]; then
      log "exists   ${coll}/${name}"
      return 0
    fi
    semp PATCH "${CFG}/${coll}/${enc}" "${patch}"
    if [ "${HTTP_CODE}" = "200" ]; then
      log "updated  ${coll}/${name}"
      return 0
    fi
  fi
  semp_err >&2
  die "HTTP ${HTTP_CODE} while configuring ${coll}/${name}"
}

# Fetch a monitor object and print selected / all attributes
mon_get() {   # mon_get <path> [jq-filter]
  local path="$1" filter="${2:-.data // .meta}"
  semp GET "${MON}/${path}"
  if [ "${HTTP_CODE}" = "200" ]; then
    echo "${RESP}" | jq -r "${filter}"
  else
    echo "  (HTTP ${HTTP_CODE}) $(semp_err)"
  fi
}
