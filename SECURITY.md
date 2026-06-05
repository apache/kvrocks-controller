# Security Policy

## Reporting a Vulnerability

Apache Kvrocks Controller follows the [Apache Software Foundation security process](https://www.apache.org/security/).
Please report suspected vulnerabilities **privately** to `security@apache.org` (the Kvrocks PMC is reachable
at `private@kvrocks.apache.org`). Do **not** open public GitHub issues or pull requests for security reports.

## Threat Model

What the controller treats as in/out of scope, the security properties it claims and disclaims (the
control-plane API/UI authentication posture, its failover authority over managed clusters, metadata-store
trust, and the SSRF surface of node registration), the adversary model, and how findings are triaged are
documented in [THREAT_MODEL.md](./THREAT_MODEL.md). Reporters and triagers should consult it alongside this policy.
