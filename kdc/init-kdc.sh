#!/bin/bash
# ---------------------------------------------------------------------------
# KDC bootstrap + run script.
#  - generates krb5.conf / kdc.conf from environment
#  - on first start: creates the realm, principals and keytabs
#  - exports krb5.conf + keytabs through shared volumes
#  - runs krb5kdc in the foreground
#
# Principals created:
#   admin/admin@REALM                              (password: $KADMIN_PASSWORD)
#   $KAFKA_SERVICE_NAME/$KAFKA_HOST@REALM          -> /keytabs/kafka.keytab  (Kafka SPN, in-network name)
#   $KAFKA_SERVICE_NAME/$KAFKA_EXTERNAL_HOST@REALM -> /keytabs/kafka.keytab  (Kafka SPN, name used by Solace)
#   $SOLACE_PRINCIPAL@REALM                        -> /keytabs/solace.keytab (Solace Kafka bridge UPN)
#   $CLIENT_PRINCIPAL@REALM                        -> /keytabs/client.keytab (test producer/consumer)
# ---------------------------------------------------------------------------
set -euo pipefail

REALM="${REALM:-SOLACE.LOCAL}"
KDC_HOST="${KDC_HOST:-kdc}"
KAFKA_HOST="${KAFKA_HOST:-kafka}"
KAFKA_EXTERNAL_HOST="${KAFKA_EXTERNAL_HOST:-${KAFKA_HOST}}"
KAFKA_SERVICE_NAME="${KAFKA_SERVICE_NAME:-kafka}"
SOLACE_PRINCIPAL="${SOLACE_PRINCIPAL:-solace}"
CLIENT_PRINCIPAL="${CLIENT_PRINCIPAL:-client}"
KDC_MASTER_PASSWORD="${KDC_MASTER_PASSWORD:-MasterKey-ChangeMe}"
KADMIN_PASSWORD="${KADMIN_PASSWORD:-admin}"

KDB_DIR="${KDB_DIR:-/var/lib/krb5kdc}"
KEYTAB_DIR="${KEYTAB_DIR:-/keytabs}"
KRB5_OUT_DIR="${KRB5_OUT_DIR:-/krb5}"
MARKER="${KDB_DIR}/.realm-created"

# Solace event brokers support aes256-cts-hmac-sha1-96 and aes128-cts-hmac-sha1-96
# (plus legacy des3/arcfour). Keep the realm on the AES-SHA1 pair so every party
# (MIT, Java/Kafka, Solace) negotiates the same key types.
ENCTYPES_KDC="aes256-cts-hmac-sha1-96:normal aes128-cts-hmac-sha1-96:normal"
ENCTYPES_LIB="aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96"

log() { echo "[kdc] $*"; }

mkdir -p /etc/krb5kdc "${KDB_DIR}" "${KEYTAB_DIR}" "${KRB5_OUT_DIR}"

# ---------------------------------------------------------------- krb5.conf
cat > /etc/krb5.conf <<EOF
[libdefaults]
    default_realm = ${REALM}
    dns_lookup_realm = false
    dns_lookup_kdc = false
    dns_canonicalize_hostname = false
    rdns = false
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true
    udp_preference_limit = 1
    default_tkt_enctypes = ${ENCTYPES_LIB}
    default_tgs_enctypes = ${ENCTYPES_LIB}
    permitted_enctypes = ${ENCTYPES_LIB}

[realms]
    ${REALM} = {
        kdc = ${KDC_HOST}:88
        admin_server = ${KDC_HOST}:749
    }

[domain_realm]
    ${KAFKA_HOST} = ${REALM}
    ${KAFKA_EXTERNAL_HOST} = ${REALM}
    ${KDC_HOST} = ${REALM}
    solace = ${REALM}
    localhost = ${REALM}

[logging]
    default = STDERR
EOF
cp /etc/krb5.conf "${KRB5_OUT_DIR}/krb5.conf"
chmod 644 "${KRB5_OUT_DIR}/krb5.conf"

# ----------------------------------------------------------------- kdc.conf
cat > /etc/krb5kdc/kdc.conf <<EOF
[kdcdefaults]
    kdc_ports = 88
    kdc_tcp_ports = 88

[realms]
    ${REALM} = {
        database_name = ${KDB_DIR}/principal
        key_stash_file = ${KDB_DIR}/.k5.${REALM}
        acl_file = /etc/krb5kdc/kadm5.acl
        admin_keytab = ${KDB_DIR}/kadm5.keytab
        max_life = 24h 0m 0s
        max_renewable_life = 7d 0h 0m 0s
        supported_enctypes = ${ENCTYPES_KDC}
        default_principal_flags = +preauth
    }

[logging]
    kdc = STDERR
    admin_server = STDERR
    default = STDERR
EOF

echo "*/admin@${REALM} *" > /etc/krb5kdc/kadm5.acl

# ------------------------------------------------------- realm bootstrap
if [ ! -f "${MARKER}" ]; then
    log "Creating realm ${REALM} (database in ${KDB_DIR})"
    rm -f "${KDB_DIR}"/principal* "${KDB_DIR}"/.k5.* 2>/dev/null || true
    kdb5_util create -s -r "${REALM}" -P "${KDC_MASTER_PASSWORD}"

    log "Creating principals"
    kadmin.local -q "addprinc -pw ${KADMIN_PASSWORD} admin/admin@${REALM}"
    kadmin.local -q "addprinc -randkey ${KAFKA_SERVICE_NAME}/${KAFKA_HOST}@${REALM}"
    if [ "${KAFKA_EXTERNAL_HOST}" != "${KAFKA_HOST}" ]; then
        kadmin.local -q "addprinc -randkey ${KAFKA_SERVICE_NAME}/${KAFKA_EXTERNAL_HOST}@${REALM}"
    fi
    kadmin.local -q "addprinc -randkey ${SOLACE_PRINCIPAL}@${REALM}"
    kadmin.local -q "addprinc -randkey ${CLIENT_PRINCIPAL}@${REALM}"

    log "Exporting keytabs to ${KEYTAB_DIR}"
    rm -f "${KEYTAB_DIR}"/*.keytab "${KEYTAB_DIR}/.ready"
    kadmin.local -q "ktadd -k ${KEYTAB_DIR}/kafka.keytab  ${KAFKA_SERVICE_NAME}/${KAFKA_HOST}@${REALM}"
    if [ "${KAFKA_EXTERNAL_HOST}" != "${KAFKA_HOST}" ]; then
        kadmin.local -q "ktadd -k ${KEYTAB_DIR}/kafka.keytab  ${KAFKA_SERVICE_NAME}/${KAFKA_EXTERNAL_HOST}@${REALM}"
    fi
    kadmin.local -q "ktadd -k ${KEYTAB_DIR}/solace.keytab ${SOLACE_PRINCIPAL}@${REALM}"
    kadmin.local -q "ktadd -k ${KEYTAB_DIR}/client.keytab ${CLIENT_PRINCIPAL}@${REALM}"
    chmod 644 "${KEYTAB_DIR}"/*.keytab

    date -u +%Y-%m-%dT%H:%M:%SZ > "${MARKER}"
else
    log "Realm ${REALM} already initialised ($(cat "${MARKER}")) - reusing database and keytabs"
    if [ ! -f "${KEYTAB_DIR}/solace.keytab" ]; then
        log "WARNING: keytabs volume is empty but the KDC database exists."
        log "         Run 'docker compose down -v' to reset both volumes together."
    fi
fi

log "Keytab contents:"
for kt in "${KEYTAB_DIR}"/*.keytab; do
    klist -kte "${kt}" || true
done

touch "${KEYTAB_DIR}/.ready"
log "Starting krb5kdc for ${REALM} on ${KDC_HOST}:88 (tcp+udp)"
exec krb5kdc -n
