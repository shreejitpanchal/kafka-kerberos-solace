#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# ALTERNATIVE to the automated SEMP upload: the "manual" procedure described by
# Solace Support, i.e. copy the keytab file into the broker's keytabs folder
# (/usr/sw/jail/keytabs) and reference it by file name on the receiver/sender.
#
# Normally you do NOT need this - `solace-config` already pushes the keytab via
# SEMP (authenticationKerberosKeytabContent). Use it when you prefer to manage
# keytab files yourself, or to compare behaviour while troubleshooting.
# ---------------------------------------------------------------------------
set -euo pipefail
# shellcheck disable=SC1091
source "$(dirname "$0")/lib.sh"
resolve_solace_container

UPN="${SOLACE_PRINCIPAL}@${REALM}"
KT_RECV="solace-kafka-receiver.keytab"
KT_SEND="solace-kafka-sender.keytab"

echo "1) Export the keytab from the KDC container to scripts/keytabs/"
"${SCRIPT_DIR}/export-keytabs.sh"

echo "2) Copy the files into the broker's keytabs folder (/usr/sw/jail/keytabs)"
docker cp "scripts/keytabs/${KT_RECV}" "${SOLACE_CONTAINER}:/usr/sw/jail/keytabs/${KT_RECV}"
docker cp "scripts/keytabs/${KT_SEND}" "${SOLACE_CONTAINER}:/usr/sw/jail/keytabs/${KT_SEND}"
docker exec "${SOLACE_CONTAINER}" bash -c "chmod 644 /usr/sw/jail/keytabs/*.keytab && ls -l /usr/sw/jail/keytabs/"

echo "3) Realm + receiver/sender referencing the files BY NAME (no content upload)"
docker compose run --rm solace-tools configure --keytab-by-name

cat <<EOF

Equivalent Solace CLI (docker exec -it ${SOLACE_CONTAINER} ${SOLACE_CLI} -A):
  enable
  configure
  message-vpn ${SOLACE_VPN}
    authentication kerberos
      create realm @${REALM}
        kdc-address ${KDC_ADDRESS_FOR_SOLACE}
        no shutdown
        exit
      exit
    kafka
      kafka-receiver ${KAFKA_RECEIVER_NAME}
        shutdown
        bootstrap-addresses ${KAFKA_BOOTSTRAP_FOR_SOLACE}
        authentication auth-scheme kerberos
        authentication kerberos service-name ${KAFKA_SERVICE_NAME}
        authentication kerberos user-principal-name ${UPN} ${KT_RECV}
        no shutdown
        exit
      kafka-sender ${KAFKA_SENDER_NAME}
        shutdown
        bootstrap-addresses ${KAFKA_BOOTSTRAP_FOR_SOLACE}
        authentication auth-scheme kerberos
        authentication kerberos service-name ${KAFKA_SERVICE_NAME}
        authentication kerberos user-principal-name ${UPN} ${KT_SEND}
        no shutdown
        end
  show message-vpn ${SOLACE_VPN} kafka kafka-receiver ${KAFKA_RECEIVER_NAME} keytab
EOF
