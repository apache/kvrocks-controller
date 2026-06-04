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
- **Status:** **DRAFT — v0, maintainer-reviewed (git-hulk), pending PMC sign-off.** Produced by the ASF
  Security team via the `threat-model-producer` rubric
  (<https://gist.github.com/potiuk/da14a826283038ddfe38cc9fe6310573>). Companion to the
  `apache/kvrocks` model; this one covers the **control plane**, whose trust surface differs.
- **Version binding:** versioned with the project.
- **Reporting cross-reference:** §8-property violations → report privately per `SECURITY.md` /
  <https://www.apache.org/security/>; §3 / §9 findings closed citing this document.
- **Provenance legend:** *(documented)* / *(maintainer)* / *(inferred)* as in the sibling model; each
  *(inferred)* routes to a §14 question.
- **Draft confidence:** ~12 documented / ~10 maintainer / ~14 inferred.

The controller exposes an **HTTP API** (default `addr: 127.0.0.1:9379`) and a bundled **web UI**, through
which operators create clusters, add/remove nodes, migrate slots, and trigger or automate failover
*(documented — README, config)*. It is, in effect, the **administrative authority** over every Kvrocks
cluster it manages: whoever controls the controller controls those clusters' topology and data placement.

## §2 Scope and intended use

Primary intended use *(documented)*: an operations control plane that a cluster administrator runs to
manage one or more Kvrocks clusters, backed by a consensus/metadata store for its own state and HA. The
controller is **required to be deployed inside a trusted environment** *(maintainer — git-hulk)*.

Caller roles:

- **API / UI client** — whoever can reach `:9379` or the web UI. **There is no built-in
  authentication or authorization** *(maintainer — git-hulk)*, so this role is *unauthenticated* and
  gated only by `addr` / network reachability. Exposing an unauthenticated API is **expected behaviour**
  today; authentication/authorization is **planned** (tracking issue
  [apache/kvrocks-controller#390](https://github.com/apache/kvrocks-controller/issues/390)) *(maintainer
  — git-hulk)*.
- **Metadata store (ETCD/…)** — trusted source of truth for cluster topology; the controller believes it,
  and is expected to *(maintainer — git-hulk; trusted environment)*.
- **Managed Kvrocks node** — a data node the controller connects to and issues admin/cluster commands to;
  trusted as honest *(maintainer — git-hulk)*.
- **Peer controller** — another controller instance in the same HA group, coordinating via the store;
  trusted as honest *(maintainer — git-hulk)*.
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
- **Network/transport hardening** — the controller does **not** provide built-in TLS; operators front it
  with a TLS proxy such as Nginx *(maintainer — git-hulk)*.
- **A tampered metadata store or a Byzantine peer controller** — the store and peer nodes are trusted as
  honest because the deployment is required to be in a trusted environment *(maintainer — git-hulk)*.
- **Build tooling and tests.**

## §4 Trust boundaries and data flow

The primary trust boundary is the **HTTP API / web-UI surface**. A request that reaches it can read or
mutate cluster topology — create/delete clusters, add/remove nodes, migrate slots, force failover
*(documented — feature list)*. With **no built-in auth** *(maintainer — git-hulk)*, the boundary is
effectively the network ACL around `:9379`, and the deployment is required to sit in a trusted
environment.

Secondary boundaries:

- **Controller ↔ metadata store:** the controller trusts store contents as ground truth for topology and
  leadership, and is expected to — the store is trusted as honest *(maintainer — git-hulk)*.
- **Controller → managed node:** the controller connects out to node addresses taken from its
  metadata/API input and issues admin/cluster commands, holding whatever node credentials the operator
  configured (expected to be **stored inside the metadata store**) *(maintainer — git-hulk)*. Node
  addresses supplied via the API are accepted but **validated to be a real Kvrocks node**; no further
  SSRF hardening is considered necessary *(maintainer — git-hulk)* (§9).

**Reachability preconditions:**

- A finding in the **API/UI** path is in-model only when it breaks a §8 property; "the API is
  unauthenticated" is by-design (§9), since the deployment is a trusted environment.
- A finding in **store handling** is in-model if reachable from API input; the store contents themselves
  are trusted (§7).
- A finding in the **node client** (e.g. credential mishandling) is in-model if a connection parameter is
  attacker-influenceable via the API; arbitrary node addresses are accepted but Kvrocks-validated
  *(maintainer — git-hulk)*.
- A finding reachable only from `config.yaml` or operator-held credentials is out of model (§3).

## §5 Assumptions about the environment

- **Runtime:** a Go binary on a POSIX host; default API bind `127.0.0.1:9379` *(documented)*.
- **Deployment:** the controller, its store, its peers and its managed nodes are deployed together in a
  **trusted environment** *(maintainer — git-hulk)*.
- **Metadata store:** an operator-run ETCD (default) or alternative, reachable at the configured addr,
  with operator-chosen auth/TLS (defaults: no auth, `tls.enable: false`) *(documented — config)*; trusted
  as honest *(maintainer — git-hulk)*.
- **Managed nodes:** reachable Kvrocks instances the operator has provisioned; the controller is assumed
  to be authorized to administer them, and the nodes are trusted as honest *(maintainer — git-hulk)*.
- **Network:** the operator controls who can reach the API, the store, and the nodes; there is no
  built-in API auth or TLS, so network controls (and an optional TLS proxy) are the boundary *(maintainer
  — git-hulk)*.
- **What the controller does to its host/network (*(inferred)* — wave-2):** binds the API port; connects
  out to the store and to managed nodes; reads `config.yaml`; may write logs/metrics. Not assumed to
  execute arbitrary host commands.

## §5a Build-time and configuration variants

| Knob | Default *(documented — config)* | Effect | Insecure-default ruling |
| --- | --- | --- | --- |
| API `addr` | `127.0.0.1:9379` | Localhost-only by default | Safe default; exposing it on an untrusted network is operator misuse (§11) |
| API authentication | **none** *(maintainer — git-hulk)* | Control plane is unauthenticated by design | By-design today; auth/authz planned in #390. Trusted-environment deployment is the supported posture |
| API/UI TLS | **none built-in** *(maintainer — git-hulk)* | Plaintext control-plane traffic | By-design; operators front with a TLS proxy (e.g. Nginx) |
| `storage_type` | `etcd` | Choice of metadata backend (etcd/zk/consul/raft; raft = experimental, "not recommended for production") | raft-in-prod is an operator misuse (§11) |
| etcd `username`/`password`/`tls.enable` | empty / `false` | Store auth + encryption off unless set | Operator responsibility (§10) |

## §6 Assumptions about inputs

| Entry point | Parameter | Attacker-controllable? | Caller/operator must enforce |
| --- | --- | --- | --- |
| API: create/scale/failover/migrate | cluster/node/slot args | only if API reachable; no built-in auth *(maintainer)* | deploy in a trusted environment; network ACL / TLS proxy |
| API: add node | **node address/host:port** | accepted, but Kvrocks-validated *(maintainer)* | trusted-environment deployment; restrict network access |
| Web UI | form/query fields | only if reachable; no auth/CSRF *(maintainer)* | front with auth proxy; trusted-environment deployment |
| metadata store reads | topology/leadership records | from a **trusted** store *(maintainer)* | store auth/TLS (operator) |
| node responses | probe/command replies | from a **trusted** node *(maintainer)* | node authenticity (TLS/credentials) |
| `config.yaml` | all keys | **no — operator-trusted** | never sourced from a request |

## §7 Adversary model

- **Primary adversary:** because the controller is required to be deployed in a **trusted environment**
  with no built-in auth *(maintainer — git-hulk)*, the modelled adversary is **not** an arbitrary network
  client but an actor who can subvert a §8 safety property of the control plane (e.g. crafted API input
  that crashes or corrupts the controller despite trusted callers).
- **Goals against a §8 property:** crash/corrupt the controller via malformed input; induce incorrect
  failover or split-brain through a §8 logic flaw (not through a tampered store).
- **Out of model:** the operator; anyone with `config.yaml` / store / managed-node credentials; a
  malicious peer controller or a tampered/Byzantine metadata store (the store and peers are trusted as
  honest — *(maintainer — git-hulk)*); and an unauthenticated network client reaching `:9379`, since
  exposing an unauthenticated API in a trusted environment is by-design (§9).

## §8 Security properties the project provides

*(Thin by design for a control plane required to run in a trusted environment.)*

1. **Failover correctness under trusted operation.** Given a healthy store and honest peers, failover
   promotes a valid replica rather than corrupting topology *(inferred — core function)*. *Symptom:* split
   brain / promotion of a stale or wrong node / topology corruption. *Severity:* high (availability/integrity).
2. **Leader-election / split-brain safety — store-dependent.** HA controllers coordinate via the store;
   split-brain resolution **depends on the store engine** and is **majority-win** when using Raft / ETCD /
   ZooKeeper *(maintainer — git-hulk)*. *Symptom:* two controllers issuing conflicting cluster ops under a
   non-majority store. *Severity:* high.
3. **Memory/handler safety on API input.** Malformed API/UI requests do not crash or corrupt the controller
   *(inferred)*. *Symptom:* panic/crash/OOB from crafted input. *Severity:* medium–high.
4. **Authentication / authorization — NOT PROVIDED (by design).** The API/UI have **no built-in
   authentication or authorization** *(maintainer — git-hulk)*; the controller claims **no** access-control
   property and relies entirely on §10 network controls and a trusted-environment deployment. Auth/authz is
   planned in [#390](https://github.com/apache/kvrocks-controller/issues/390).

## §9 Security properties the project does NOT provide

- **No built-in control-plane authentication or authorization** *(maintainer — git-hulk)* — exposing
  `:9379` (or the UI) yields an unauthenticated administrative control plane; this is **expected behaviour**
  in the required trusted-environment deployment, with auth/authz **planned in #390**.
- **No transport encryption** for the API/UI *(maintainer — git-hulk)* — operators front the controller
  with a TLS proxy (e.g. Nginx); store TLS is off unless configured.
- **No web-UI authentication or CSRF protection** *(maintainer — git-hulk)*.
- **No defence against a malicious operator** or anyone holding store/managed-node credentials (§3).
- **The controller trusts its metadata store, its peers and its managed nodes as honest** *(maintainer —
  git-hulk)* — it is not a defence against a tampered store, a Byzantine peer, or a hostile node; the
  trusted-environment requirement covers this.
- **No SSRF allow-listing of registered node addresses** — any address is accepted, but the controller
  **validates that it is a real Kvrocks node**, which the PMC considers sufficient *(maintainer —
  git-hulk)*.

**False friends:**

- *A bundled web UI looks like an admin console with a login, but it has **no authentication and no CSRF**
  protection* *(maintainer — git-hulk)* — its presence does not imply an access-control boundary.
- *Managing "clusters" implies isolation, but the controller is a single authority across all managed
  clusters* — compromising it is not contained to one cluster.

**Well-known attack classes left to the operator / the trusted-environment requirement:** unauthenticated
admin-API exposure; CSRF/XSS on the web UI; plaintext control traffic; an unsecured ETCD that anyone can
rewrite. These are accepted given the trusted-environment deployment model.

## §10 Downstream (operator) responsibilities

- **Deploy the controller, store, peers and managed nodes in a trusted environment** — the supported
  posture, since there is no built-in API/UI auth *(maintainer — git-hulk)*.
- **Do not expose the API/UI to an untrusted network**; front it with an authenticating reverse proxy /
  network ACL and a TLS proxy (e.g. Nginx) for transport encryption *(maintainer — git-hulk)*.
- Enable TLS for the store connection (`etcd.tls`) and for managed-node connections on untrusted networks.
- Secure the metadata store (ETCD/ZK/Consul) with its own auth + ACLs; supply those credentials to the
  controller; do not run the experimental `raft` engine in production.
- Restrict and protect the credentials the controller uses to administer managed nodes (these are
  **expected to be stored inside the metadata store** *(maintainer — git-hulk)*).

## §11 Known misuse patterns

- Deploying the controller outside a trusted environment / exposing its API or UI to an untrusted network
  (there is no built-in auth) *(maintainer — git-hulk)*.
- Binding the controller API to `0.0.0.0` on a shared network without a fronting auth + TLS proxy.
- Running the experimental `raft` store engine in production (the README warns against it).
- Pointing the controller at an ETCD with no auth/TLS on a reachable network.
- Treating the web UI as an authenticated admin console when it has no auth or CSRF protection.

## §11a Known non-findings (recurring false positives)

*(Seed list; the PMC reports **no scanner/researcher findings yet** *(maintainer — git-hulk)*, so this
list is provisional — §14 Q8.)*

- **"Unauthenticated admin API / no login on the UI / no CSRF"** against any config — **by design**; the
  controller deliberately ships no auth and is deployed in a trusted environment; auth/authz is planned in
  #390 *(maintainer — git-hulk)*. Disposition `BY-DESIGN`.
- **"Controller connects to arbitrary node addresses (SSRF)"** — addresses are accepted but Kvrocks-node
  validated; the PMC considers this sufficient and not in need of hardening *(maintainer — git-hulk)*.
- **"Tampered ETCD / Byzantine peer redirects the controller"** — store and peers are trusted as honest in
  the required trusted environment *(maintainer — git-hulk)*. Disposition `OUT-OF-MODEL`.
- **"No transport encryption on the API"** — by design; operators front with a TLS proxy *(maintainer —
  git-hulk)*.
- **Findings in `cmd/`, `scripts/`, `x.py`, build tooling, tests** — out of scope (§3).
- **Controller can delete/failover clusters** — that is its purpose; an authorized call is not a finding (§7).

## §12 Conditions that would change this model

- Landing of built-in API/UI authentication/authorization (tracked in #390) — would add §8 properties and
  shrink §9/§10, and remove the trusted-environment requirement from several rows.
- A change to the default `addr` / TLS posture, or built-in TLS support.
- A new API surface or store engine, or promotion of the `raft` engine to supported.
- Treating peer controllers or the metadata store as untrusted (pulls them into §7).
- Any report not cleanly routable to a §13 disposition.

## §13 Triage dispositions

| Disposition | Meaning | Licensed by |
| --- | --- | --- |
| `VALID` | Violates a claimed property via an in-scope adversary/input. | §8, §6, §7 |
| `VALID-HARDENING` | No §8 property broken, but a §11 misuse warrants hardening. | §11 |
| `OUT-OF-MODEL: trusted-input` | Requires control of config / store / node credentials. | §6 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Requires operator / store / peer capability, or an untrusted-network client (trusted-environment deployment assumed). | §7, §3 |
| `OUT-OF-MODEL: unsupported-component` | Lands in tooling/tests, or the experimental `raft` engine. | §3, §5a |
| `OUT-OF-MODEL: non-default-build` | Only under a discouraged/non-default config. | §5a |
| `BY-DESIGN: property-disclaimed` | Concerns a §9-disclaimed property (no built-in auth/TLS, no web-UI CSRF, single cross-cluster authority). | §9 |
| `KNOWN-NON-FINDING` | Matches a §11a entry. | §11a |
| `MODEL-GAP` | Routes to none of the above → revise the model. | §12 |

## §14 Open questions for the maintainers

*Wave-1 and most of wave-2/3 were answered by git-hulk on PR #397 (folded above as *(maintainer)*); they are
retained here for traceability with their resolutions. Genuinely open items remain tagged *(inferred)*.*

**Wave 1 — auth, TLS (§2/§5a/§8/§9):**
1. **[ANSWERED — git-hulk]** Built-in API authentication/authorization? **No** — none today; an
   unauthenticated API is expected behaviour; auth/authz is planned in
   [#390](https://github.com/apache/kvrocks-controller/issues/390). Supported posture is a
   trusted-environment deployment.
2. **[ANSWERED — git-hulk]** Web-UI authentication / CSRF? **None of them** — no auth, no CSRF.
3. **[ANSWERED — git-hulk]** TLS for the API / managed-node connections? **Not built-in** — operators use a
   TLS proxy such as Nginx.

**Wave 2 — store, nodes, SSRF (§4/§6/§9):**
4. **[ANSWERED — git-hulk]** How are managed-node admin credentials stored? **Expected to be stored inside
   the (metadata) store.** *(Open *(inferred)* sub-item: what protects them at rest within the store is the
   store layer's responsibility — confirm if any controller-side protection is intended.)*
5. **[ANSWERED — git-hulk]** Are node addresses validated/allow-listed? **All addresses are allowed, but the
   controller validates that the target is a Kvrocks node**; no further hardening considered necessary.
6. **[ANSWERED — git-hulk]** Trust the metadata store and peers as honest? **Yes** — required to be deployed
   inside a trusted environment.

**Wave 3 — properties & §11a (§8/§11a):**
7. **[ANSWERED — git-hulk]** Failover-correctness / split-brain guarantees? **Depends on the store engine** —
   majority-win with Raft / ETCD / ZooKeeper.
8. **[ANSWERED — git-hulk]** Most common scanner/researcher non-findings? **No report yet** — §11a stays a
   provisional seed *(inferred)* until real reports arrive.
9. **[ANSWERED — git-hulk]** Confirm this model lives as root `THREAT_MODEL.md` referenced from a new
   `SECURITY.md`, separate from the `apache/kvrocks` model? **Yes.**

**Still open *(inferred)* — for the PMC:**
10. *(inferred)* Confirm the operator/deployer is the only out-of-scope adversary class, and that no
    in-process privilege boundary is claimed between API roles (none exist today).
11. *(inferred)* Memory/handler safety on malformed API/UI input (§8.3): is a panic/crash from crafted
    input considered `VALID`, given callers are nominally trusted?
12. *(inferred)* Wave-2 host/network behaviour (§5): confirm the controller never executes arbitrary host
    commands and only binds the API port + connects out to the store and nodes.
13. *(inferred)* Within-store protection of node credentials at rest (follow-up to Q4): is any
    controller-side encryption of stored credentials intended, or is it wholly the store's responsibility?

## §15 Machine-readable companion

Deferred for v0; a `threat-model.yaml` can later encode the §6 trust table, §2/§3 scoping, §8 rows, §9 false
friends, §11a non-findings, and §13 dispositions.
