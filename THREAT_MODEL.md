<!--
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Threat Model — Apache Kvrocks Controller

## §1 Header

- **Project:** Apache Kvrocks Controller — a Go cluster-management control plane for Apache Kvrocks.
  It probes managed Kvrocks nodes and performs **failover**, scales clusters out/in, manages many
  clusters from one (itself-clustered) controller, and stores cluster metadata in a pluggable backend
  (ETCD by default; ZooKeeper / Consul / Raft optional) *(documented — README, `config/config.yaml`)*.
- **Modelled against:** `apache/kvrocks-controller` `unstable`/HEAD (2026-05-31).
- **Status:** **DRAFT — v0, not yet reviewed by the Kvrocks PMC.** Produced by the ASF Security team via
  the `threat-model-producer` rubric
  (<https://gist.github.com/potiuk/da14a826283038ddfe38cc9fe6310573>). Companion to the
  `apache/kvrocks` model; this one covers the **control plane**, whose trust surface differs.
- **Version binding:** versioned with the project.
- **Reporting cross-reference:** §8-property violations → report privately per `SECURITY.md` /
  <https://www.apache.org/security/>; §3 / §9 findings closed citing this document.
- **Provenance legend:** *(documented)* / *(maintainer)* / *(inferred)* as in the sibling model; each
  *(inferred)* routes to a §14 question.
- **Draft confidence:** ~12 documented / 0 maintainer / ~40 inferred.

The controller exposes an **HTTP API** (default `addr: 127.0.0.1:9379`) and a bundled **web UI**, through
which operators create clusters, add/remove nodes, migrate slots, and trigger or automate failover
*(documented — README, config)*. It is, in effect, the **administrative authority** over every Kvrocks
cluster it manages: whoever controls the controller controls those clusters' topology and data placement.

## §2 Scope and intended use

Primary intended use *(documented)*: an operations control plane that a cluster administrator runs to
manage one or more Kvrocks clusters, backed by a consensus/metadata store for its own state and HA.

Caller roles:

- **API / UI client** — whoever can reach `:9379` or the web UI. **The config exposes no built-in
  authentication knob**, so by default this role is *unauthenticated* and gated only by `addr` /
  network reachability *(inferred — `config/config.yaml` has no API-auth section; default `addr` is
  localhost; `server/middleware` present but role unconfirmed)*. This is the load-bearing §14 wave-1 item.
- **Metadata store (ETCD/…)** — trusted source of truth for cluster topology; the controller believes it.
- **Managed Kvrocks node** — a data node the controller connects to and issues admin/cluster commands to.
- **Peer controller** — another controller instance in the same HA group, coordinating via the store.
- **Operator / deployer** — controls `config.yaml`, the store, TLS material, network exposure, and the
  managed-node credentials. Fully trusted; **out of model** as adversary (§3).

**Component-family table:**

| Family | Entry point | Touches outside process | In model? |
| --- | --- | --- | --- |
| HTTP control-plane API | `:9379`, `server/` | network; store; managed nodes | **Yes** |
| Web UI / dashboard | `webui/` | browser; the API | **Yes** |
| Metadata store engine | `store/engine` (etcd/zk/consul/raft) | network (the store) | **Yes** |
| Controller → Kvrocks-node client | probing, cluster ops, failover | network (managed nodes) | **Yes** |
| Controller HA / leader election | via the store | network | **Yes** |
| Build / tooling | `cmd`, `scripts`, `x.py`, `Makefile` | — | No → §3 |

## §3 Out of scope (explicit non-goals)

- **The operator / deployer as adversary** — controls `config.yaml`, the metadata store, and the
  deployment (§9) *(inferred)*.
- **Hardening of the metadata store itself** (ETCD/ZK/Consul auth, ACLs, TLS) — the controller consumes
  it; securing it is the operator's job, though the controller must use the credentials/TLS the operator
  supplies *(documented — etcd `username`/`password`/`tls` fields exist, default empty/off)*.
- **The managed Kvrocks nodes' own threat model** — covered by the `apache/kvrocks` model; this document
  covers the controller's *use* of them.
- **Network/transport hardening** beyond the TLS the controller supports.
- **Build tooling and tests.**

## §4 Trust boundaries and data flow

The primary trust boundary is the **HTTP API / web-UI surface**. A request that reaches it can read or
mutate cluster topology — create/delete clusters, add/remove nodes, migrate slots, force failover
*(documented — feature list)*. With no built-in auth (pending §14), the boundary is effectively the
network ACL around `:9379` *(inferred)*.

Secondary boundaries:

- **Controller ↔ metadata store:** the controller trusts store contents as ground truth for topology and
  leadership. An actor who can write the store can redirect the controller *(inferred)*.
- **Controller → managed node:** the controller connects out to node addresses taken from its
  metadata/API input and issues admin/cluster commands, holding whatever node credentials the operator
  configured *(inferred)*. Node addresses supplied via the API are an **SSRF / connect-to-arbitrary-host**
  vector if the caller is untrusted (§9).

**Reachability preconditions:**

- A finding in the **API/UI** path is in-model if reachable by a client the deployment treats as untrusted
  (which, absent built-in auth, is "anyone who can reach the port" — see §14 wave-1).
- A finding in **store handling** is in-model if reachable from API input or from store contents an
  attacker could influence.
- A finding in the **node client** (e.g. SSRF, credential mishandling) is in-model if a node address or
  connection parameter is attacker-influenceable via the API.
- A finding reachable only from `config.yaml` or operator-held credentials is out of model (§3).

## §5 Assumptions about the environment

- **Runtime:** a Go binary on a POSIX host; default API bind `127.0.0.1:9379` *(documented)*.
- **Metadata store:** an operator-run ETCD (default) or alternative, reachable at the configured addr,
  with operator-chosen auth/TLS (defaults: no auth, `tls.enable: false`) *(documented — config)*.
- **Managed nodes:** reachable Kvrocks instances the operator has provisioned; the controller is assumed
  to be authorized to administer them *(inferred)*.
- **Network:** the operator controls who can reach the API, the store, and the nodes *(inferred)*.
- **What the controller does to its host/network (*(inferred)* — wave-2):** binds the API port; connects
  out to the store and to managed nodes; reads `config.yaml`; may write logs/metrics. Not assumed to
  execute arbitrary host commands.

## §5a Build-time and configuration variants

| Knob | Default *(documented — config)* | Effect | Insecure-default ruling |
| --- | --- | --- | --- |
| API `addr` | `127.0.0.1:9379` | Localhost-only by default | Safe default; exposing it without a fronting auth proxy is the risk |
| API authentication | **none present in config** | Control plane appears unauthenticated | **Open (wave-1):** is there *any* API/UI auth, and is "network-trust only" the supported posture? |
| API/UI TLS | *(unconfirmed)* | Plaintext control-plane traffic | **Open (wave-1)** |
| `storage_type` | `etcd` | Choice of metadata backend (etcd/zk/consul/raft; raft = experimental, "not recommended for production") | raft-in-prod is an operator misuse (§11) |
| etcd `username`/`password`/`tls.enable` | empty / `false` | Store auth + encryption off unless set | Operator responsibility (§10) |

## §6 Assumptions about inputs

| Entry point | Parameter | Attacker-controllable? | Caller/operator must enforce |
| --- | --- | --- | --- |
| API: create/scale/failover/migrate | cluster/node/slot args | **yes** (absent auth, any API client) | front with authn/authz; network ACL |
| API: add node | **node address/host:port** | **yes** | restrict who may register nodes (SSRF surface) |
| Web UI | form/query fields | **yes** | output encoding; CSRF protection; auth |
| metadata store reads | topology/leadership records | from a **trusted** store *(inferred)* | store auth/TLS (operator) |
| node responses | probe/command replies | from a **trusted** node *(inferred)* | node authenticity (TLS/credentials) |
| `config.yaml` | all keys | **no — operator-trusted** | never sourced from a request |

## §7 Adversary model

- **Primary adversary:** an actor who can reach the controller API or web UI but is not the operator —
  realistically present whenever the API is exposed beyond a trusted network without a fronting auth layer.
  Capabilities: call any API endpoint (create/delete clusters, add/remove nodes, **force failover**,
  migrate slots), register node addresses (SSRF), drive the UI.
- **Goals:** seize administrative control of managed Kvrocks clusters; cause data movement/loss via slot
  migration or failover; make the controller connect to attacker-chosen hosts; read/modify topology.
- **Out of model:** the operator, anyone with `config.yaml` / store / managed-node credentials, and
  (pending §14) a malicious peer controller or a compromised metadata store.

## §8 Security properties the project provides

*(Thin by design for a control plane assumed to run on a trusted network; all *(inferred)* pending §14.)*

1. **Failover correctness under trusted operation.** Given a healthy store and honest peers, failover
   promotes a valid replica rather than corrupting topology *(inferred — core function)*. *Symptom:* split
   brain / promotion of a stale or wrong node / topology corruption. *Severity:* high (availability/integrity).
2. **Leader-election safety (single active controller).** HA controllers coordinate via the store so that
   conflicting administrative actions are serialized *(inferred)*. *Symptom:* two controllers issuing
   conflicting cluster ops. *Severity:* high.
3. **Memory/handler safety on API input.** Malformed API/UI requests do not crash or corrupt the controller
   *(inferred)*. *Symptom:* panic/crash/OOB from crafted input. *Severity:* medium–high.
4. **Authentication / authorization — UNRESOLVED.** Whether the API/UI authenticate callers at all is the
   central open question (§14 wave-1); absent that, the controller claims **no** access-control property and
   relies entirely on §10 network controls.

## §9 Security properties the project does NOT provide

- **No built-in control-plane authentication by default** *(inferred)* — exposing `:9379` (or the UI) to an
  untrusted network without a fronting auth proxy yields an **unauthenticated administrative control plane**
  over every managed cluster.
- **No transport encryption guarantee by default** for the API/UI; store TLS is off unless configured.
- **No defence against a malicious operator** or anyone holding store/managed-node credentials (§3).
- **The controller trusts its metadata store and its peers** — it is not a defence against a tampered store
  or a Byzantine peer controller (pending §14).
- **No SSRF protection on registered node addresses** *(inferred)* — the controller connects to host:port
  values it is given; an untrusted caller who can add nodes can aim those connections.

**False friends:**

- *A bundled web UI looks like an admin console with a login, but (pending §14) may have no authentication at
  all* — its presence does not imply an access-control boundary.
- *Managing "clusters" implies isolation, but the controller is a single authority across all managed
  clusters* — compromising it is not contained to one cluster.

**Well-known attack classes left to the operator:** unauthenticated admin-API exposure; CSRF/XSS on the web
UI; SSRF via node registration; plaintext control traffic; an unsecured ETCD that anyone can rewrite.

## §10 Downstream (operator) responsibilities

- **Do not expose the API/UI to an untrusted network without an authenticating reverse proxy / network ACL**
  (until/unless built-in auth is confirmed).
- Enable TLS for the API/UI, for the store connection (`etcd.tls`), and for managed-node connections on
  untrusted networks.
- Secure the metadata store (ETCD/ZK/Consul) with its own auth + ACLs; supply those credentials to the
  controller; do not run the experimental `raft` engine in production.
- Restrict and protect the credentials the controller uses to administer managed nodes.
- Restrict who may register node addresses (SSRF surface).

## §11 Known misuse patterns

- Binding the controller API to `0.0.0.0` on a shared network without fronting authentication.
- Running the experimental `raft` store engine in production (the README warns against it).
- Pointing the controller at an ETCD with no auth/TLS on a reachable network.
- Treating the web UI as an authenticated admin console when it is not.

## §11a Known non-findings (recurring false positives)

*(v0 seed — PMC's real list is the key §14 input.)*

- **"Unauthenticated admin API / no login on the UI"** against the default config — by design *if* the PMC
  rules network-trust the supported posture (else it is `VALID`); resolved in §14 wave-1.
- **"ETCD has no auth/TLS"** — operator-configured store, defaults documented (§3/§10).
- **Findings in `cmd/`, `scripts/`, `x.py`, build tooling, tests** — out of scope (§3).
- **Controller can delete/failover clusters** — that is its purpose; an authorized call is not a finding (§7).

## §12 Conditions that would change this model

- Introduction (or confirmation) of built-in API/UI authentication/authorization — would add §8 properties
  and shrink §9/§10.
- A change to the default `addr` / TLS posture.
- A new API surface or store engine, or promotion of the `raft` engine to supported.
- Treating peer controllers or the metadata store as untrusted (pulls them into §7).
- Any report not cleanly routable to a §13 disposition.

## §13 Triage dispositions

| Disposition | Meaning | Licensed by |
| --- | --- | --- |
| `VALID` | Violates a claimed property via an in-scope adversary/input. | §8, §6, §7 |
| `VALID-HARDENING` | No §8 property broken, but a §11 misuse warrants hardening (e.g. add CSRF tokens, SSRF allow-list). | §11 |
| `OUT-OF-MODEL: trusted-input` | Requires control of config / store / node credentials. | §6 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Requires operator / store / peer capability. | §7, §3 |
| `OUT-OF-MODEL: unsupported-component` | Lands in tooling/tests, or the experimental `raft` engine. | §3, §5a |
| `OUT-OF-MODEL: non-default-build` | Only under a discouraged/non-default config. | §5a |
| `BY-DESIGN: property-disclaimed` | Concerns a §9-disclaimed property (no built-in auth/TLS default, single cross-cluster authority). | §9 |
| `KNOWN-NON-FINDING` | Matches a §11a entry. | §11a |
| `MODEL-GAP` | Routes to none of the above → revise the model. | §12 |

## §14 Open questions for the maintainers

**Wave 1 — the central auth question (§2/§5a/§8/§9):**
1. Does the controller API have **any built-in authentication/authorization** (token, mTLS, basic-auth), or
   is it **network-trust only**? If the latter, is exposing it behind an operator's own auth proxy the
   *supported* posture, so an "unauthenticated API" report is `BY-DESIGN`? *Proposed:* network-trust only
   today; operator fronts it; unauth-on-trusted-network is by design, unauth-on-untrusted-network is misuse.
2. Does the **web UI** authenticate, and does it have **CSRF** protection on state-changing calls?
   *Proposed:* same as the API; CSRF is a hardening item.
3. Is **TLS** supported/expected for the API and managed-node connections? *Proposed:* operator-configurable;
   plaintext acceptable only on trusted networks.

**Wave 2 — store, nodes, SSRF (§4/§6/§9):**
4. How are **managed-node admin credentials** stored and used (in the store? in config?), and what protects
   them at rest? *Proposed:* operator-supplied; stored in the metadata store; protected at the store layer.
5. Are **node addresses** registered via the API validated/allow-listed, or will the controller connect to
   any host:port given (SSRF)? *Proposed:* currently connects as given; allow-listing is a hardening item.
6. Does the controller **trust the metadata store and peer controllers** as honest (out of §7), or should a
   tampered store / Byzantine peer be modelled? *Proposed:* trusted; out of scope.

**Wave 3 — properties & §11a (§8/§11a):**
7. What **failover-correctness / split-brain** guarantees does the controller make, and under what store/peer
   assumptions? *Proposed:* correct given a healthy store and a quorum of honest peers.
8. What do scanners/researchers most often report here that the PMC considers a **non-finding**? (Seeds §11a.)

**Meta:**
9. Confirm this model lives as root `THREAT_MODEL.md` referenced from a new `SECURITY.md`, separate from the
   `apache/kvrocks` model. *Proposed:* yes.

## §15 Machine-readable companion

Deferred for v0; a `threat-model.yaml` can later encode the §6 trust table, §2/§3 scoping, §8 rows, §9 false
friends, §11a non-findings, and §13 dispositions.
