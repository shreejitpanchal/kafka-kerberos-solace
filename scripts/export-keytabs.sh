#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Copy the Solace bridge keytab out of the KDC container into scripts/keytabs/
# under the two file names the broker knows it by:
#   scripts/keytabs/solace-kafka-receiver.keytab   (Kafka receiver)
#   scripts/keytabs/solace-kafka-sender.keytab     (Kafka sender)
# Both are byte-identical copies of /keytabs/solace.keytab (UPN solace@REALM) -
# the receiver and sender share one principal, the broker just references the
# keytab by a per-object file name.
#
# Kept for reference / manual upload (scripts/upload-keytab-manual.sh, PubSub+
# Manager, a corporate broker). Keytabs are secrets: *.keytab is git-ignored.
# Re-run after scripts/reset.sh (new KDC database = new keys).
# ---------------------------------------------------------------------------
set -euo pipefail
# shellcheck disable=SC1091
source "$(dirname "$0")/lib.sh"

# relative to the repo root (lib.sh cd-s there): docker cp must not see a /c/... MSYS path
KT_DIR="scripts/keytabs"
KT_RECV="${RECEIVER_KEYTAB_NAME:-solace-kafka-receiver.keytab}"
KT_SEND="${SENDER_KEYTAB_NAME:-solace-kafka-sender.keytab}"

if [ "$(docker inspect -f '{{.State.Health.Status}}' kdc 2>/dev/null || echo missing)" != "healthy" ]; then
  echo "ERROR: kdc container is not healthy - start the stack first (scripts/up.sh)" >&2
  exit 1
fi

mkdir -p "${KT_DIR}"
docker compose cp kdc:/keytabs/solace.keytab "${KT_DIR}/${KT_RECV}"
cp -f "${KT_DIR}/${KT_RECV}" "${KT_DIR}/${KT_SEND}"
chmod 600 "${KT_DIR}/${KT_RECV}" "${KT_DIR}/${KT_SEND}"

echo "Exported ${SOLACE_PRINCIPAL}@${REALM} keytab ($(stat -c %s "${KT_DIR}/${KT_RECV}") bytes) to:"
echo "  ${PROJECT_DIR}/${KT_DIR}/${KT_RECV}"
echo "  ${PROJECT_DIR}/${KT_DIR}/${KT_SEND}"
echo "Entries:"
docker compose exec -T kdc klist -kte /keytabs/solace.keytab
