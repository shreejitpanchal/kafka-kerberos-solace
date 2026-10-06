# Solace native Kafka Bridge ↔ Kafka with Kerberos (keytab)

**Purpose of this repository: create and validate a *Solace native Kafka Bridge* on a Solace
PubSub+ event broker.** The bridge is the broker's built-in Kafka Bridging feature (a *Kafka
Receiver* and a *Kafka Sender* configured on the Message VPN) — no Kafka Connect, no separate
Solace PubSub+ Connector for Kafka, no extra process: the Solace broker itself connects to the
Kafka cluster as a Kerberos-authenticated Kafka client and moves messages in both directions.
Everything else in this repo (the KDC, the Kafka broker, the toolboxes, the scripts) exists only
to give that bridge something to talk to and to configure/verify it reproducibly.

A self-contained Docker Compose lab: a lightweight Apache Kafka (KRaft, single node) that only
accepts **SASL/GSSAPI (Kerberos)**, an **MIT Kerberos KDC** that issues the tickets, and the
automation that configures the `default` Message VPN of **your already-running Solace PubSub+
container** to bridge to that Kafka in both directions with the broker's **native Kafka Bridge**,
authenticated with a **keytab**.

The Solace broker is **not** part of the compose file and is **not modified as a container**.
Everything talks to it through the ports it already publishes on your PC — PubSub+ Manager is
`https://localhost:1943`. Inside a container "localhost" means the container itself, so Docker
Desktop's name for your PC is used instead: **`host.docker.internal` = localhost on your PC**.
The KDC and Kafka publish host ports as well, so the broker reaches them the same way.

```
 your PC (Windows, Docker Desktop)                 published ports: 1943 (SEMP/TLS) 9000 (REST) 1883 (MQTT)
 ┌───────────────────────────────────────────┐                      88 (KDC)  9093 (Kafka EXTERNAL)
 │  existing Solace PubSub+ container        │
 │  VPN "default"                            │   KDC  host.docker.internal:88   ─────────────┐
 │    realm @SOLACE.LOCAL  kdc-address ──────┼──────────────────────────────────────────────┐│
 │    keytab solace@REALM                    │                                              ││
 │    kafka-receiver ── bootstrap ───────────┼─► host.docker.internal:9093 (SASL/GSSAPI) ──┐││
 │    kafka-sender   ── bootstrap ───────────┼─► SPN kafka/host.docker.internal@REALM     │││
 └───────────────────────────────────────────┘                                            │││
        ▲ https://host.docker.internal:1943  (SEMP v2: realm, keytab, bridge)             │││
        │ REST :9000 / MQTT :1883              (publish / peek)                            │││
 ┌──────┴─────────────────────── compose network kafka-kerberos-net ───────────────────────┼┼┼─┐
 │  ┌──────────────────┐   ┌──────────────┐        ┌───────────────────────────────────┐  │││ │
 │  │ solace-config /  │   │ kdc :88 ◄────┼────────┼── TGT / service tickets           │◄─┘││ │
 │  │ solace-tools     │   │ MIT krb5     │        │ kafka  (KRaft)                    │   ││ │
 │  └──────────────────┘   │ realm        │◄───────┤  INTERNAL kafka:9093  kafka/kafka │   ││ │
 │  ┌──────────────────┐   │ SOLACE.LOCAL │        │  EXTERNAL :9095 → host port 9093  │◄──┘│ │
 │  │ kafka-client     ├───┼──────────────┼───────►│           kafka/host.docker.internal│   │ │
 │  │ client@REALM     │   └──────────────┘        └───────────────────────────────────┘    │ │
 │  └──────────────────┘    volumes: keytabs (kafka/solace/client.keytab), krb5conf          │ │
 └────────────────────────────────────────────────────────────────────────────────────────────┘
```

Data flows that are wired up out of the box:

* **Kafka → Solace**: Kafka topic `orders-in` → Kafka Receiver → Solace topic `kafka/orders-in`
  (substitution expression `kafka/${kafkaTopic()}`) → queue `q.from-kafka` (subscription `kafka/>`).
* **Solace → Kafka**: any message on `solace/orders/>` → queue `q.to-kafka` → Kafka Sender → Kafka topic `orders-out`.

---

## 1. Prerequisites

* **Docker Desktop** (Windows/macOS) — it provides `host.docker.internal`. (Plain Docker Engine on
  Linux: the toolboxes get it via `extra_hosts`; for the broker side use the network mode in §7.)
  Kafka + KDC need < 1.5 GB RAM.
* **A running Solace PubSub+ software broker container, version 10.7.1 or newer** (SEMP v2 ≥ 2.40
  is where the Kerberos attributes for Kafka bridging appeared; the script checks the version),
  publishing SEMP over TLS on **1943** (your `https://localhost:1943`), REST on 9000 and MQTT on
  1883 (change `SOLACE_*_PORT` in `.env` if yours differ). Credentials `admin` / `admin`
  (`SOLACE_ADMIN_USER` / `SOLACE_ADMIN_PASSWORD`); the broker's self-signed certificate is accepted
  (`SOLACE_SEMP_INSECURE=true`).
* **Kafka bridging scaling parameters on that broker must be > 0.** A broker started without
  `system_scaling_maxkafkabridgecount` / `system_scaling_maxkafkabrokerconnectioncount` has both
  at **0** and no receiver/sender can ever start. Either run `scripts/solace-enable-kafka-scaling.sh`
  (CLI `scale` — the broker reloads, see §3) or recreate the container with
  `-e system_scaling_maxkafkabridgecount=10 -e system_scaling_maxkafkabrokerconnectioncount=300`.
* Free host ports **88** (KDC) and **9093** (Kafka) — change `KDC_HOST_PORT` / `KAFKA_HOST_PORT`
  (and the matching `*_FOR_SOLACE` addresses) in `.env` if taken.
* Windows: run the `scripts/*.sh` helpers from **Git Bash or WSL**, or use `scripts/smoke-test.ps1`
  from PowerShell. Keep files **LF**-terminated (`.gitattributes`/`.editorconfig` included; the
  images also strip CRs at build time).

## 2. Quick start

```bash
cd kafka-kerberos-solace
scripts/up.sh
```

`scripts/up.sh` = `docker compose up -d --build` + `scripts/check-solace.sh` (SEMP reachable, broker
can reach KDC/Kafka through your PC's ports, scaling values) + follow the `solace-config` job until
it prints the bridge status + `scripts/export-keytabs.sh` (reference copies of the bridge keytab as
`scripts/keytabs/solace-kafka-{receiver,sender}.keytab`, e.g. for a manual upload in PubSub+ Manager).
Equivalent by hand:

```bash
docker compose up -d --build                 # kdc, kafka, kafka-init, kafka-client, solace-config (polls SEMP up to 10 min)
scripts/check-solace.sh                      # pre-flight checks against your broker (read-only)
docker compose logs -f solace-config         # wait for "receiver up=true, sender up=true"
docker compose run --rm solace-tools status
```

Open PubSub+ Manager at <https://localhost:1943> → Message VPN `default` → **Kafka Bridges**.
Then run the round-trip test:

```bash
scripts/smoke-test.sh          # bash (Git Bash / WSL / macOS / Linux)
scripts\smoke-test.ps1         # PowerShell
```

## 3. What the automation does (and in which order)

The order matches the procedure confirmed by Solace Support:

| Step | Where | What |
|------|-------|------|
| 0 | `kdc` container | Creates realm `SOLACE.LOCAL`; principals `kafka/kafka@REALM` and `kafka/host.docker.internal@REALM` (both Kafka SPNs, in one `kafka.keytab`), `solace@REALM` (bridge UPN), `client@REALM` (test clients), `admin/admin@REALM`; exports the keytabs and `krb5.conf` into shared volumes; publishes host port 88. Only AES-SHA1 enctypes (`aes256-cts-hmac-sha1-96`, `aes128-cts-hmac-sha1-96`), which are the ones Solace supports. |
| 0 | `kafka` container | KRaft single node with two GSSAPI listeners: `INTERNAL` (`kafka:9093`, principal `kafka/kafka`, inter-broker + in-network clients) and `EXTERNAL` (container 9095 → host port 9093, advertised as `host.docker.internal:9093`, principal `kafka/host.docker.internal`, used by Solace). Per-listener JAAS via `listener.name.<listener>.gssapi.sasl.jaas.config`. No anonymous/PLAINTEXT client listener exists. |
| 0 | `kafka-init` | Authenticates as `client@REALM` on `kafka:9093` and creates topics `orders-in`, `orders-out`. |
| 0 | `scripts/check-solace.sh` | Read-only: SEMP answers at `https://localhost:1943`; from inside your broker `host.docker.internal:88` and `:9093` resolve and accept connections; `show kafka` scaling values. |
| 1 | `solace-config` → SEMP v2 | **Kerberos realm on the VPN**: `POST /msgVpns/default/authenticationKerberosRealms` `{kerberosRealmName:"@SOLACE.LOCAL", kdcAddress:"host.docker.internal:88", enabled:true}` (CLI: `message-vpn default > authentication > kerberos > create realm @SOLACE.LOCAL > kdc-address host.docker.internal:88 > no shutdown`). |
| 2 | `solace-config` → SEMP v2 | **Keytab onto the broker**: sent base64-encoded in `authenticationKerberosKeytabContent` together with `authenticationKerberosKeytabFileName`. The broker imports it into its own configuration — `/usr/sw/jail/keytabs` stays **empty** in this mode (verified on 10.26); the monitor API shows `authenticationKerberosKeytabThumbprint` / `...KeytabConfigTime`, and the CLI `show message-vpn default kafka kafka-receiver <name> keytab` lists it. (Manual alternative: `scripts/upload-keytab-manual.sh` copies the files from `scripts/keytabs/` into `/usr/sw/jail/keytabs` with `docker cp` and runs `configure --keytab-by-name`, which only references the file name — same as the CLI `user-principal-name <upn> <keytab-file>`.) |
| 3 | `solace-config` → SEMP v2 | **Kafka receiver + sender** with `authenticationScheme:"kerberos"`, `authenticationKerberosServiceName:"kafka"`, `authenticationKerberosUserPrincipalName:"solace@SOLACE.LOCAL"`, keytab file name, `bootstrapAddressList:"host.docker.internal:9093"`, then the topic binding (`orders-in` → `kafka/${kafkaTopic()}`) and the queue binding (`q.to-kafka` → `orders-out`). Queues `q.from-kafka` / `q.to-kafka` and their subscriptions are created in between. |

Everything is idempotent — rerunning `docker compose up -d` re-applies the configuration
(bridge objects are disabled, patched, re-enabled). Nothing else on your broker is touched.

### Raising the Kafka scaling parameters on an existing broker

```bash
scripts/solace-enable-kafka-scaling.sh      # asks for confirmation, then runs via CLI:
```
```
enable
configure
system
scaling
scale max-kafka-bridges 10 max-kafka-broker-connections 300
end
show kafka
```

The broker reloads its services automatically (short messaging outage; on 10.25.1+ the container
may restart depending on its restart policy). Afterwards run `docker compose up -d` again so the
bridge configuration is re-applied. Reference: *Scaling Up an Existing Single Software Event Broker*.

## 4. Verifying that Kerberos works

### From the toolbox (SEMP v2 monitor API)

```bash
docker compose run --rm solace-tools status
```

Shows the realm, receiver/sender (`enabled`, `up`, `failureReason`, keytab file name + thumbprint +
config time), topic/queue bindings and queue depths. The `remoteKafkaBrokers` monitor path does not
exist on 10.26 (`HTTP 400`, code 535 `INVALID_PATH`) — `status` prints that line and continues; use the
CLI `... remote-broker` or `show kafka` (connection counts) instead.

### On the broker CLI (the checks Solace Support suggested)

```bash
docker exec -it <solace-container> /usr/sw/loads/currentload/bin/cli -A
```
```
solace> show kafka
solace> show message-vpn default kerberos realm @SOLACE.LOCAL
solace> show message-vpn default kafka
solace> show message-vpn default kafka kafka-receiver kr-kafka-kerberos keytab
solace> show message-vpn default kafka kafka-receiver kr-kafka-kerberos remote-broker
solace> show message-vpn default kafka kafka-receiver kr-kafka-kerberos topic-binding
solace> show message-vpn default kafka kafka-sender   ks-kafka-kerberos keytab
solace> show message-vpn default kafka kafka-sender   ks-kafka-kerberos queue-binding
```

Non-interactive helper: `scripts/solace-cli.sh "show message-vpn default kafka"`.

### Event log

```bash
docker exec <solace-container> grep -E "VPN_KAFKA|KERBEROS" /usr/sw/jail/logs/event.log | tail -n 30
```

Look for `VPN_KAFKA_RECEIVER_UP` / `VPN_KAFKA_SENDER_UP` (and `*_DOWN` with a reason when it fails).

### On the Kafka / KDC side

```bash
docker compose exec kafka-client check-auth          # GSSAPI handshake as client@REALM (INTERNAL listener)
docker compose exec kafka-client topics describe
docker compose exec kdc kadmin.local -q "listprincs"
docker compose exec kdc klist -kte /keytabs/kafka.keytab      # both SPNs
docker compose logs kdc | grep -E "TGS_REQ|AS_REQ" | tail   # ticket requests from solace / kafka
docker compose logs kafka | grep -i -E "sasl|gssapi|kerberos" | tail
```

### What a healthy lab looks like (verified 2026-10-06)

Broker `solace-ent-10-26` = `solace-pubsub-enterprise:10.26.0.9000`, created with
`system_scaling_maxkafkabridgecount=10` / `system_scaling_maxkafkabrokerconnectioncount=300`
(so no CLI `scale` step was needed); stack from this repo with `apache/kafka:4.3.1`.

```
show kafka                                     (scripts/solace-cli.sh "show kafka")
  Number of Kafka Broker Connections : 5 (of max 300)
  Number of Configured Kafka Bridges : 2 (of max 10)
  Kafka Receivers  Configured 1  Operational 1
  Kafka Senders    Configured 1  Operational 1

docker compose run --rm solace-tools status    (abridged)
  realm      kerberosRealmName "@SOLACE.LOCAL"  kdcAddress "host.docker.internal:88"  enabled true
  receiver   kr-kafka-kerberos  up true  connectionCount 3  groupId solace-bridge
             authenticationKerberosKeytabFileName "solace-kafka-receiver.keytab"
             authenticationKerberosKeytabThumbprint 1dfeccf0...  (same thumbprint on the sender)
             topic binding orders-in -> kafka/${kafkaTopic()}  up true  initialOffset beginning
  sender     ks-kafka-kerberos  up true  connectionCount 2
             queue binding q.to-kafka -> orders-out  ackMode all  up true

event.log (broker local time, UTC+8)
  VPN_KAFKA_SENDER_UP   ... Kafka-Sender ks-kafka-kerberos up      ~5 s after the keytab/enable PATCH
  VPN_KAFKA_RECEIVER_UP ... Kafka-Receiver kr-kafka-kerberos up

kafka-consumer-groups --describe --group solace-bridge   (from kafka-client, as client@REALM)
  solace-bridge  orders-in  0..2  CONSUMER-ID fe6ccdc774f8/default/kr-kafka-kerberos-...  HOST /172.19.0.1
```

The consumer `HOST /172.19.0.1` is the compose network's gateway, i.e. the Solace broker really
comes in through `host.docker.internal:9093` → the `EXTERNAL` listener (SPN `kafka/host.docker.internal`).
Both bridge objects report the same keytab thumbprint because they share `solace@SOLACE.LOCAL`.

Observed recovery behaviour: after `docker compose down` the objects go `DOWN (No remote-broker in
UP state)` and come back by themselves once `scripts/up.sh` has re-applied the configuration; after a
restart of the Solace container itself (`DOWN (Msg-backbone shutdown)`) both were `UP` again ~40 s
after the container started, with no action on the Kafka/KDC side. Re-uploading a keytab in PubSub+
Manager shows as `DOWN (Shutdown)` followed by `UP` a few seconds later.

## 5. Exercising the bridge by hand

Kafka → Solace:

```bash
# terminal 1: watch what arrives on Solace (MQTT subscription on kafka/#)
docker compose run --rm solace-tools peek "kafka/#" 60
# terminal 2: produce to Kafka with Kerberos auth
docker compose exec kafka-client produce orders-in "hello from kafka"
docker compose exec kafka-client produce --key orders-in "order-42:{\"id\":42}"
docker compose run --rm solace-tools qcount q.from-kafka
```

Solace → Kafka:

```bash
docker compose run --rm solace-tools publish solace/orders/new "hello from solace"
docker compose exec kafka-client consume orders-out 15
# or straight from your PC through the broker's REST port:
curl -X POST http://localhost:9000/TOPIC/solace/orders/new -H "Content-Type: text/plain" -d "hello from solace"
```

PubSub+ Manager → `default` → **Try Me!** also works for publishing/subscribing interactively.

## 6. Project layout

```
CLAUDE.md                     project context for Claude Code sessions (architecture, invariants, how to verify)
.env / env.example            realm, image tag, your broker's host/ports/credentials, names — the file you edit
docker-compose.yml            kdc, kafka, kafka-init, kafka-client, solace-config, solace-tools
kdc/                          Alpine + MIT krb5; init-kdc.sh builds realm/principals/keytabs/krb5.conf
kafka-client/                 Kafka CLI toolbox with Kerberos client.properties (produce/consume/topics/check-auth)
solace-tools/                 curl+jq toolbox: configure [--keytab-by-name] (SEMP v2), status, publish, peek, qcount, export-keytab
scripts/lib.sh                shared: loads .env, finds the Solace container, runs CLI commands in it
scripts/up.sh                 compose up + check-solace + follow the configuration job
scripts/check-solace.sh       read-only pre-flight checks against your broker
scripts/solace-enable-kafka-scaling.sh   CLI `scale max-kafka-bridges ... max-kafka-broker-connections ...`
scripts/solace-cli.sh         run Solace CLI show commands non-interactively
scripts/upload-keytab-manual.sh  the "copy keytab to /usr/sw/jail/keytabs" variant + CLI transcript
scripts/export-keytabs.sh     copy the bridge keytab out of the KDC -> scripts/keytabs/ (also run by up.sh)
scripts/keytabs/              solace-kafka-receiver.keytab + solace-kafka-sender.keytab (git-ignored secrets) + README
scripts/smoke-test.sh|.ps1    both-direction end-to-end test
scripts/reset.sh              docker compose down -v + fresh start (new keytabs, re-upload)
scripts/attach-solace.sh      optional network mode (see §7)
```

Volumes: `kdc-data` (KDC database), `keytabs`, `krb5conf`. Kafka data is intentionally ephemeral.
Your Solace container and its volumes are never created, removed or restarted by this project.

## 7. Customising

* **Realm / principals / topics / names**: edit `.env`, then `scripts/reset.sh` (realm or host-name
  changes need a fresh KDC database and new keytabs). Topic or queue renames only need `docker compose up -d`.
* **Different host ports**: `KDC_HOST_PORT`, `KAFKA_HOST_PORT` in `.env`, and keep
  `KDC_ADDRESS_FOR_SOLACE` / `KAFKA_BOOTSTRAP_FOR_SOLACE` in step (`host.docker.internal:<port>`).
  The Kafka SPN is derived from `HOST_ALIAS`, not from the port, so ports can change freely.
* **Network mode** (no `host.docker.internal`, e.g. Linux Docker Engine, or if you prefer one shared
  network): `scripts/attach-solace.sh` connects your broker container to `kafka-kerberos-net` with the
  alias `solace`; then set `SOLACE_HOST=solace`, `HOST_ALIAS=kafka`, `KDC_ADDRESS_FOR_SOLACE=kdc:88`,
  `KAFKA_BOOTSTRAP_FOR_SOLACE=kafka:9093` in `.env` and run `scripts/reset.sh`. The published ports
  then simply go unused.
* **Use an existing corporate KDC / Active Directory**: drop the `kdc` service, set
  `KDC_ADDRESS_FOR_SOLACE` to the DC (`dc01.corp.example.com:88`), `REALM` to the AD realm (uppercase),
  export a keytab for the bridge account with `ktpass`/`ktutil`, and mount it at `/keytabs/solace.keytab`
  for `solace-config`. Keep AES enctypes enabled on the account (`msDS-SupportedEncryptionTypes`).
* **TLS to Kafka (SASL_SSL)**: switch the listeners to `SASL_SSL`, add keystore/truststore, and set
  `transportTlsEnabled:true` on the receiver/sender (plus a trusted CA on the broker).
* **Extra receivers/senders**: copy the two `upsert_bridge` blocks in `solace-tools/bin/configure`.
  Useful substitution expressions: `${kafkaTopic()}`, `${kafkaPartitionKeyAsString()}`,
  `${kafkaHeaderAsString("name")}` on receivers; `${topic()}`, `${topic(2)}`, `${partitionKey()}` on senders.

## 8. Troubleshooting

| Symptom | Likely cause / fix |
|---------|--------------------|
| `solace-config` logs *last HTTP code 000* | The toolbox cannot reach `https://host.docker.internal:1943`. Is the broker up and publishing 1943? Does `curl -k -u admin:admin https://localhost:1943/SEMP/v2/config/about/api` work on your PC? If your broker only serves plaintext SEMP use `SOLACE_SEMP_SCHEME=http` + `SOLACE_SEMP_PORT=8080`. |
| `solace-config` logs *'host.docker.internal' does not resolve* | Not Docker Desktop (or an old version). Use the network mode (§7). |
| `solace-config` dies with *rejected user 'admin' (HTTP 401)* | Wrong SEMP credentials — `SOLACE_ADMIN_USER` / `SOLACE_ADMIN_PASSWORD` in `.env`. |
| `solace-config` never starts (`docker compose ps` shows `kafka-init` exited ≠ 0) | `solace-config` is gated on `kafka-init` succeeding, i.e. on a working Kerberos login to Kafka. `docker compose logs kafka-init kafka kdc`. Fix, then `docker compose up -d` again. |
| `solace-config` dies with *SEMP … older than 2.40* | Broker too old for Kerberos on Kafka bridges — upgrade your Solace container to 10.7.1+. |
| Receiver/sender stay `up=false`; event log mentions bridge/connection limits, or `show kafka` prints `Max Kafka Bridges 0` | Scaling parameters are 0. `scripts/solace-enable-kafka-scaling.sh` (or recreate the container with the two `system_scaling_*` env vars), then `docker compose up -d`. |
| `check-solace.sh`: *port 88/9093 is closed* from inside the broker | The stack isn't up yet, or `KDC_HOST_PORT`/`KAFKA_HOST_PORT` differ from `*_FOR_SOLACE`. `docker compose ps` must show `kdc` healthy and `kafka` healthy. |
| `docker compose up` fails with *port is already allocated* (88 or 9093) | Change `KDC_HOST_PORT` / `KAFKA_HOST_PORT` and the matching `*_FOR_SOLACE` values in `.env`. |
| Kerberos failure *Server not found in Kerberos database* | SPN mismatch. Solace requests `kafka/<host in KAFKA_BOOTSTRAP_FOR_SOLACE>@REALM`; the keytab has `kafka/kafka` and `kafka/<HOST_ALIAS>`. Keep the host part equal to `HOST_ALIAS`, or change `HOST_ALIAS` and `scripts/reset.sh`. |
| *Cannot contact any KDC* in the event log | Broker cannot reach `host.docker.internal:88` — `scripts/check-solace.sh`, step 3. |
| *Clock skew too great* | Docker Desktop VM clock drifted after sleep. `wsl --shutdown` (Windows) or restart Docker Desktop. |
| *KDC has no support for encryption type* / *Unsupported key type* | Something negotiates a non-AES-SHA1 enctype. Keep `supported_enctypes` in `kdc/init-kdc.sh` as is; check the keytab with `klist -kte`. |
| Kafka container restarts, logs *Cannot locate KDC* / *krb5.conf* | `krb5conf` volume empty — `kdc` must be healthy first. `docker compose logs kdc`. |
| Kafka client `check-auth` fails but KDC is fine | Run it again — the second attempt prints `sun.security.krb5.debug` output showing which step fails (TGT, TGS for `kafka/kafka`, or the AP-REQ). |
| Keytab regenerated (`scripts/reset.sh`) but broker still has the old one | `docker compose up -d` re-uploads it (`solace-config` is idempotent). |
| `/usr/sw/jail/keytabs` inside the broker is empty | Expected with the default SEMP upload (`authenticationKerberosKeytabContent`): the broker keeps the keytab in its configuration, not as a file. Proof it is there: `authenticationKerberosKeytabThumbprint` in `status`, or CLI `show message-vpn default kafka kafka-receiver kr-kafka-kerberos keytab`. Only `scripts/upload-keytab-manual.sh` puts files in that folder. |
| `status` right after `configure` shows `up:false`, *No remote-broker connection* — but the event log has `VPN_KAFKA_RECEIVER_UP` / `VPN_KAFKA_SENDER_UP` a few seconds later | Normal: the Kerberos handshake (TGT + service ticket + metadata) takes ~5 s after the objects are enabled. Re-run `docker compose run --rm solace-tools status`. |
| Event log: `VPN_KAFKA_*_DOWN … (No remote-broker in UP state)` after it was UP | The broker lost Kafka: the stack was stopped (`docker compose down`/`stop`), the Kafka container restarted, or the broker itself was restarted while the stack was down. `docker compose ps` — if `kdc`/`kafka` are missing run `scripts/up.sh` again; the receiver/sender reconnect by themselves. |
| Event log: `VPN_KAFKA_*_DOWN … (Msg-backbone shutdown)` | The Solace container itself was restarted. Nothing to do — both objects came back `UP` ~40 s after the container start (verified on 10.26). |
| Event log: `VPN_KAFKA_*_DOWN … (Shutdown)` then `UP` seconds later | Someone (re-)applied the object: `solace-config`/`configure` (disable → PATCH → enable) or a keytab upload in PubSub+ Manager. Harmless. |
| `docker cp` / `docker compose cp` from a host script fails with *invalid output path: directory "C:\\c\\…" does not exist* | `scripts/lib.sh` disables MSYS path conversion, so an absolute `/c/...` path reaches Docker unconverted. Pass paths **relative to the repo root** (`lib.sh` already `cd`s there), as `scripts/export-keytabs.sh` does. |
| Scripts fail with `$'\r': command not found` | CRLF line endings. `git config core.autocrlf false` + re-checkout, or `dos2unix`. Inside the images this is handled automatically. |

## 9. Hardening notes before this leaves the lab

* Replace `SASL_PLAINTEXT` by `SASL_SSL`; Kerberos protects authentication, not the payload.
* Use dedicated principals for receiver and sender, rotate keytabs (`ktadd` bumps the KVNO — the old
  key keeps working until tickets expire), and store them outside the repo (`*.keytab` is git-ignored).
  `scripts/up.sh` keeps a reference copy of the bridge keytab as `scripts/keytabs/solace-kafka-{receiver,sender}.keytab`
  (`scripts/export-keytabs.sh`); treat that folder like any other secret.
* Enable a Kafka authorizer and grant `solace@REALM` only `Read` on the source topics /
  consumer group and `Write` on the sink topics.
* Give the Solace bridge its own client profile/ACLs and size `max-kafka-broker-connections`
  to (#receivers + #senders) × (#Kafka brokers + 1).
* Use a persistent, backed-up KDC (or your corporate KDC) so keytabs never silently change under the broker.

## 10. References

* Solace — Configuring Kafka Bridging (CLI, Kerberos section): <https://docs.solace.com/Features/Kafka-Bridging/Kafka-Bridging-Setup-Overview.htm>
* Solace — Kafka Bridging in Broker Manager: <https://docs.solace.com/Admin/Broker-Manager/config-kafka-bridge.htm>
* Solace — Monitoring Kafka Bridging: <https://docs.solace.com/Features/Kafka-Bridging/Kafka-Bridging-Monitor.htm>
* Solace — Kafka Bridging Example: <https://docs.solace.com/Features/Kafka-Bridging/Kafka-Bridging-Example.htm>
* Solace — Substitution Expressions: <https://docs.solace.com/Messaging/Substitution-Expressions-Overview.htm>
* Solace — Client Authentication / Kerberos realm & keytabs: <https://docs.solace.com/Security/Configuring-Client-Authentication.htm>
* Solace — Kerberos supported encryption types: <https://docs.solace.com/Security/Client-Authentication-Overview.htm>
* Solace — System scaling parameters (Kafka bridge counts): <https://docs.solace.com/Software-Broker/System-Scaling-Parameters.htm>
* Solace — Scaling up an existing single software event broker (`scale` command): <https://docs.solace.com/Software-Broker/Set-Scaling-Params-Standalone.htm>
* Solace — Configuration keys reference: <https://docs.solace.com/Software-Broker/Configuration-Keys-Reference.htm>
* Solace — SEMP API versions ↔ broker releases: <https://docs.solace.com/Admin/SEMP/SEMP-API-Versions.htm>
* Solace — Using the CLI (container access): <https://docs.solace.com/Solace-CLI/Using-Solace-CLI.htm>
* SEMP v2 attribute names (Terraform provider docs): <https://github.com/SolaceProducts/terraform-provider-solacebroker/tree/main/docs/resources>
* Apache Kafka Docker image (env-var rules, JAAS, /mnt/shared/config): <https://github.com/apache/kafka/blob/trunk/docker/examples/README.md>
* MIT Kerberos — encryption types: <https://web.mit.edu/kerberos/krb5-latest/doc/admin/enctypes.html>
