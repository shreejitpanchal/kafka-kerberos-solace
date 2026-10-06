# CLAUDE.md — kafka-kerberos-solace

Context for Claude Code sessions working in this repository. Read fully before changing anything.

## What this project is

**Purpose: create and validate a Solace *native* Kafka Bridge** — the broker's built-in Kafka
Bridging feature (Kafka Receiver + Kafka Sender on the `default` Message VPN), *not* Kafka Connect
or the Solace PubSub+ Connector for Kafka. A Docker Compose lab bridges an **existing,
separately-running Solace PubSub+ container** to a lightweight **Apache Kafka (KRaft, single node)**
that way, authenticated with **Kerberos (SASL/GSSAPI) via a keytab**. An **MIT Kerberos KDC**
container issues the tickets. Keep the README's opening "Purpose" paragraph in sync with this.

Owner: Shreejit Panchal (Solace Professional Services). Customer context: Zain Group
(folder `C:\Development\2_Customer_Projects\Zain-Group\kafka-kerberos-solace`). Host OS is
Windows with Docker Desktop; shell scripts are run from Git Bash/WSL, PowerShell variants exist
where it matters.

## Architecture (one paragraph)

Compose network `kafka-kerberos-net` holds: `kdc` (Alpine + MIT krb5, realm `SOLACE.LOCAL`,
creates principals + keytabs on first start, exports them through the `keytabs` and `krb5conf`
volumes, publishes host port 88), `kafka` (image `apache/kafka:4.3.1`, GSSAPI only, two listeners:
`INTERNAL` `kafka:9093` with principal `kafka/kafka@REALM` for inter-broker + in-network clients,
`EXTERNAL` container 9095 → host port 9093 advertised as `host.docker.internal:9093` with principal
`kafka/host.docker.internal@REALM` for the Solace broker; per-listener JAAS), `kafka-init` (creates
topics `orders-in`/`orders-out`), `kafka-client` (idle Kerberos-enabled Kafka CLI toolbox),
`solace-config` (one-shot SEMP v2 job) and `solace-tools` (profile `tools`, run on demand).

The user's Solace container is **not** in the compose file and is reached only through the ports it
publishes on the PC: PubSub+ Manager/SEMP is `https://localhost:1943` on the host, which from inside
a container is **`https://host.docker.internal:1943`** (`HOST_ALIAS`/`SOLACE_HOST`; self-signed cert
→ `SEMP_INSECURE=true` → `curl -k`), REST `http://host.docker.internal:9000`, MQTT
`host.docker.internal:1883`. The broker in turn reaches the KDC as `host.docker.internal:88` and
Kafka as `host.docker.internal:9093` (`*_FOR_SOLACE` in `.env`). SEMP credentials are
`admin` / `admin`. `scripts/check-solace.sh` is the read-only pre-flight; `scripts/attach-solace.sh`
is an *optional* alternative "network mode" (attach the broker to the compose network, then
`SOLACE_HOST=solace`, `HOST_ALIAS=kafka`, `kdc:88`, `kafka:9093`).

Flows: Kafka `orders-in` → receiver → Solace topic `kafka/${kafkaTopic()}` → queue `q.from-kafka`
(subscription `kafka/>`); Solace `solace/orders/>` → queue `q.to-kafka` → sender → Kafka `orders-out`.

## Key facts that must stay true (hard-won, verified against docs)

- **Order of operations** (confirmed by Solace Support): (1) Kerberos realm on the VPN
  (`@REALM`, `kdcAddress`, enabled) → (2) keytab into the broker's keytabs folder
  (`/usr/sw/jail/keytabs`) → (3) receiver/sender with `authenticationScheme: kerberos`,
  `authenticationKerberosServiceName`, `authenticationKerberosUserPrincipalName` (must include
  `@REALM`), `authenticationKerberosKeytabFileName` (+ optional `authenticationKerberosKeytabContent`
  base64 to upload via SEMP). `solace-tools/bin/configure` implements exactly this; keep the order.
  **With the SEMP content upload the broker imports the keytab into its configuration and
  `/usr/sw/jail/keytabs` stays empty** (verified on 10.26.0.9000; the thumbprint in the monitor
  API and `show ... keytab` prove it is there). Only `scripts/upload-keytab-manual.sh` puts files in
  that folder.
- **SEMP v2 ≥ 2.40 (broker 10.7.1+)** is required for the Kerberos attributes on
  `msgVpns/{vpn}/kafkaReceivers|kafkaSenders`. Newer brokers report SEMP version as `10.x.y.z`;
  `check_semp_version` handles both formats.
- **Broker scaling parameters default to 0**: `max-kafka-bridges` and `max-kafka-broker-connections`.
  Without raising them (CLI `configure > system > scaling > scale max-kafka-bridges 10
  max-kafka-broker-connections 300`, or container env `system_scaling_maxkafkabridgecount` /
  `system_scaling_maxkafkabrokerconnectioncount`) no receiver/sender ever comes up.
  `scripts/solace-enable-kafka-scaling.sh` does the CLI variant (broker reloads).
- **SPN = `<service-name>/<bootstrap-host>@REALM`.** Solace requests a ticket for exactly the host
  typed in `bootstrapAddressList`. The KDC therefore creates **two** Kafka SPNs in `kafka.keytab`:
  `kafka/kafka` (INTERNAL listener) and `kafka/${HOST_ALIAS}` (EXTERNAL listener, default
  `host.docker.internal`). The host part of `KAFKA_BOOTSTRAP_FOR_SOLACE` must equal `HOST_ALIAS`;
  changing `HOST_ALIAS` requires `scripts/reset.sh` (new keytabs).
- **`localhost` inside a container is the container itself.** Whenever the user says "localhost"
  for the broker, that is `host.docker.internal` from the containers' point of view (Docker Desktop
  resolves it natively; `extra_hosts: host.docker.internal:host-gateway` covers Linux engines for the
  toolboxes). Do not put `localhost` into `SEMP_URL`/`*_FOR_SOLACE`.
- **Encryption types**: Solace supports `aes256-cts-hmac-sha1-96`, `aes128-cts-hmac-sha1-96`
  (plus legacy des3/arcfour). The KDC is pinned to the two AES-SHA1 types. Do not add SHA2 types.
- **apache/kafka image env rules** (verified in `KafkaDockerWrapper.scala`): `KAFKA_` prefix
  stripped, lowercased, `_`→`.`, `__`→`_`, `___`→`-`; `KAFKA_OPTS` is passed to the JVM, not to
  server.properties; **`CLUSTER_ID` is mandatory**. Hence
  `KAFKA_LISTENER_NAME_INTERNAL_GSSAPI_SASL_JAAS_CONFIG` → `listener.name.internal.gssapi.sasl.jaas.config`
  (and `..._EXTERNAL_...` for the second listener).
- Substitution expressions use `${...}` evaluated by the **broker**: `${kafkaTopic()}`,
  `${kafkaPartitionKeyAsString()}`, `${topic()}`, … Keep them in single quotes in bash.
- Solace CLI inside the container: `/usr/sw/loads/currentload/bin/cli -A`. Non-interactive use via
  `-es <script in /usr/sw/jail/cliscripts>` is best-effort with a stdin fallback (`scripts/lib.sh`).

## Conventions

- **LF line endings everywhere** (`.gitattributes`, `.editorconfig`); the Dockerfiles also strip CRs
  at build time. Never introduce CRLF.
- Shell: `bash`, `set -euo pipefail`, shellcheck-clean (SC1091/SC2034 for sourced libs are accepted).
- In `solace-tools/bin/lib.sh`, `semp METHOD URL [json]` sets globals `HTTP_CODE` and `RESP`
  — **never call it inside `$(...)`** (subshell loses the globals). Build JSON with `jq -n --arg`.
  Every curl to the broker must include `"${CURL_TLS[@]}"` (adds `-k` when `SEMP_INSECURE=true`).
- All configuration is **idempotent**: `semp_upsert` = POST, on `ALREADY_EXISTS` → PATCH;
  bridge objects use `upsert_bridge` = disable → PATCH → enabled:true.
- **jq `//` treats `false` like `null`.** Never read boolean monitor fields with `.data.up // "n/a"`
  (a receiver that is still connecting looks like "field missing"); use `has("up")` + `tostring`
  as `UP_FILTER` in `configure` does.
- `scripts/lib.sh` exports `MSYS_NO_PATHCONV=1` / `MSYS2_ARG_CONV_EXCL="*"`: Git Bash otherwise
  rewrites container paths such as `/usr/sw/loads/currentload/bin/cli` into `C:/Program Files/Git/...`
  before `docker exec` sees them. Keep that export above any docker call.
- `scripts/lib.sh` **`cd`s to the repo root when sourced**, so host scripts call siblings as
  `"${SCRIPT_DIR}/<name>.sh"`, never `"$(dirname "$0")/<name>.sh"` after the `source` line.
- **Host paths given to `docker cp` / `docker compose cp` must be repo-relative** (`scripts/keytabs/...`),
  never `"${SCRIPT_DIR}/..."`: with `MSYS_NO_PATHCONV=1` Git Bash hands `/c/Development/...` to Docker
  unconverted and it fails with *invalid output path: directory "C:\\c\\..." does not exist*.
- `.env` is the single place for names/ports/realm; `env.example` mirrors it (remote tools cannot
  write `.env`, so keep both in sync when editing). Keep comments on their own lines in `.env`.
- Never add the Solace broker back into `docker-compose.yml`; never create/restart/remove the
  user's Solace container from scripts (`scripts/reset.sh` only detaches it from the network).
- Keytabs are secrets: `*.keytab` is git-ignored. `scripts/export-keytabs.sh` (run by `up.sh`)
  keeps reference copies as `scripts/keytabs/solace-kafka-{receiver,sender}.keytab` (both =
  KDC `solace.keytab`). Don't print keytab contents except in `export-keytab` (base64, user-invoked).

## How to run / verify

```bash
scripts/up.sh                                   # compose up --build + check-solace + follow solace-config
scripts/check-solace.sh                         # read-only: SEMP reachable, broker->KDC/Kafka ports, scaling
docker compose run --rm solace-tools status     # realm / receiver / sender / bindings / queues
scripts/smoke-test.sh | scripts\smoke-test.ps1  # both directions end-to-end
scripts/solace-cli.sh "show kafka"              # scaling values; "show message-vpn default kafka ..."
docker compose exec kafka-client check-auth     # GSSAPI handshake as client@REALM (debug on retry)
docker exec <solace> grep -E "VPN_KAFKA|KERBEROS" /usr/sw/jail/logs/event.log | tail
scripts/reset.sh                                # wipe KDC db + keytabs, regenerate, re-upload
scripts/export-keytabs.sh                       # KDC solace.keytab -> scripts/keytabs/solace-kafka-{receiver,sender}.keytab (up.sh does this too)
```

## Validation status (what has and has not been proven)

Proven in the authoring session (no Docker daemon with registry access was available there):
`docker compose config` renders; all scripts shellcheck-clean; `kdc/init-kdc.sh` executed against
real MIT Kerberos (TGTs for client/solace, service tickets for both `kafka/kafka` and
`kafka/host.docker.internal`, AES-SHA1 only, idempotent re-run path); a Java 21 GSSAPI
client↔server handshake with the generated keytabs + krb5.conf succeeded for **both** Kafka SPNs
(same code path Kafka uses); `solace-tools` scripts exercised against a mock SEMP server (plain and
TLS with a self-signed cert, 401 handling) in both create/update paths and both keytab modes; host
scripts exercised against a fake `docker` CLI.

**Proven on the user's machine (2026-10-04, first real `scripts/up.sh`)**: image pulls, KDC and
Kafka (two GSSAPI listeners) healthy, `kafka-init` created the topics, `solace-config` created realm,
queues, receiver and sender on `solace-ent-10-26` (SEMP 10.26.0.9000), and the broker's event log
shows `VPN_KAFKA_SENDER_UP` and `VPN_KAFKA_RECEIVER_UP` ~5 s after the objects were enabled, i.e.
Kerberos via `host.docker.internal:88` and the SPN `kafka/host.docker.internal` work. They went
`DOWN (No remote-broker in UP state)` only when the stack was taken down. Monitor fields confirmed on
10.26: `up`, `failureReason` (not `lastFailureReason`), `connectionCount`, `spooledMsgCount`,
`bindCount` (null); `remoteKafkaBrokers` is **not** a valid monitor path on 10.26 (HTTP 400 code 535),
`status` prints that error line and continues.

**Verified 2026-10-06 (stack UP, after `scripts/reset.sh` → new keys, KVNO 2)**: the user's broker
`solace-ent-10-26` is `solace-pubsub-enterprise:10.26.0.9000` **created with**
`system_scaling_maxkafkabridgecount=10` / `system_scaling_maxkafkabrokerconnectioncount=300` (no CLI
`scale` needed; `show kafka` → 5 of 300 connections, 2 of 10 bridges, receiver/sender 1/1 operational).
`status`: realm enabled, receiver `up` (`connectionCount` 3, `groupId solace-bridge`, topic binding
`orders-in → kafka/${kafkaTopic()}` up) and sender `up` (`connectionCount` 2, queue binding
`q.to-kafka → orders-out` up); both carry the **same keytab thumbprint** (one UPN `solace@REALM`).
From `kafka-client`, `kafka-consumer-groups --describe --group solace-bridge` lists the Solace
consumer `fe6ccdc774f8/default/kr-kafka-kerberos` with `HOST /172.19.0.1` (compose gateway) = proof the
broker enters through `host.docker.internal:9093` / the `EXTERNAL` listener. Recovery seen in the event
log: Solace container restart → `DOWN (Msg-backbone shutdown)` → both `UP` ~40 s after container start;
keytab re-upload in PubSub+ Manager → `DOWN (Shutdown)` → `UP` ~5 s later. Event-log timestamps are in
the broker's local time (UTC+8). `scripts/export-keytabs.sh` ran successfully (140-byte keytab,
AES256 + AES128 SHA1 entries) into `scripts/keytabs/`.

**Not yet proven**: `scripts/smoke-test.sh` (messages actually flowing in both directions — as of
2026-10-06 11:30 `orders-in`/`orders-out` log-end offsets are all 0, i.e. nothing has been produced
since Kafka started) and the `--keytab-by-name` path against the real broker.

## Likely next steps / ideas

- Stack is UP and the bridge is `up` on both sides (2026-10-06): next is `scripts/smoke-test.sh`
  (or `scripts\smoke-test.ps1`), then `docker compose run --rm solace-tools qcount q.from-kafka` and
  `docker compose exec kafka-client consume orders-out 15`.
- Drop or replace the `remoteKafkaBrokers` lookups in `status` with whatever 10.26 exposes
  (CLI `show message-vpn default kafka kafka-receiver <name> remote-broker` works).
- Optional: TLS (`SASL_SSL` + `transportTlsEnabled`), Kafka ACLs for `solace@REALM`,
  separate principals for receiver and sender, corporate KDC/AD instead of the lab KDC
  (see README §7), additional receivers/senders (copy the `upsert_bridge` blocks).

## References (primary sources used)

Solace docs: Kafka-Bridging-Setup-Overview, config-kafka-bridge (Broker Manager),
Kafka-Bridging-Monitor, Kafka-Bridging-Example, Substitution-Expressions-Overview,
Configuring-Client-Authentication (Kerberos realm/keytabs), Client-Authentication-Overview
(enctypes), System-Scaling-Parameters, Set-Scaling-Params-Standalone, Configuration-Keys-Reference,
SEMP-API-Versions, Using-Solace-CLI. SEMP attribute names: SolaceProducts/terraform-provider-solacebroker
docs. Kafka image: apache/kafka `docker/examples/README.md` and `core/.../docker/KafkaDockerWrapper.scala`.
Full URLs are in README §10.
