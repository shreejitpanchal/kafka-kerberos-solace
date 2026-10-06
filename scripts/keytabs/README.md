# scripts/keytabs

Exported copies of the Solace Kafka-bridge keytab, written by `scripts/export-keytabs.sh`
(also run at the end of `scripts/up.sh`):

| File | Used by | Principal |
|---|---|---|
| `solace-kafka-receiver.keytab` | Kafka receiver (`authenticationKerberosKeytabFileName`) | `solace@SOLACE.LOCAL` |
| `solace-kafka-sender.keytab`   | Kafka sender   (`authenticationKerberosKeytabFileName`) | `solace@SOLACE.LOCAL` |

Both files are identical copies of the KDC's `/keytabs/solace.keytab`; the broker only
distinguishes them by file name. They are **secrets** and git-ignored (`*.keytab`). After
`scripts/reset.sh` the KDC generates new keys, so re-run the export (`up.sh` does it for you).

Inspect: `docker compose exec kdc klist -kte /keytabs/solace.keytab`
