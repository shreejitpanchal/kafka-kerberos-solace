#!/bin/bash
# Healthy when the keytabs exist AND the KDC issues a TGT for the test client.
set -e
REALM="${REALM:-SOLACE.LOCAL}"
CLIENT_PRINCIPAL="${CLIENT_PRINCIPAL:-client}"
[ -f /keytabs/.ready ] || exit 1
[ -f /keytabs/client.keytab ] || exit 1
KRB5CCNAME=MEMORY:healthcheck kinit -kt /keytabs/client.keytab "${CLIENT_PRINCIPAL}@${REALM}" >/dev/null 2>&1
