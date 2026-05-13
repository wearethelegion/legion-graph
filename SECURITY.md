# Security Policy

## Reporting a vulnerability

**Please do NOT report security vulnerabilities through public GitHub issues, discussions, or pull requests.** Public disclosure of an unpatched vulnerability exposes every operator of legion-graph to risk.

Instead, report security vulnerabilities to:

> **yubozhenko@icloud.com**

Please include, where possible:

- A clear description of the vulnerability and its potential impact.
- The component(s) affected (e.g., `kgrag-auth`, `kgrag-cognee`, a specific REST/gRPC endpoint).
- Steps to reproduce, ideally with a minimal proof-of-concept.
- Your assessment of severity (CVSS score is welcome but not required).
- Whether you intend to publish your findings, and your preferred disclosure timeline.

If you require encrypted communication, request a PGP key in your first message and one will be provided.

## What to expect

This is a best-effort, no-SLA security policy. The maintainers will:

- **Acknowledge** receipt of a report when capacity permits, typically within a few business days but with no guaranteed timeline.
- **Triage** the report and assess severity at their sole discretion.
- **Address** validated vulnerabilities on a best-effort basis at their sole discretion, with no commitment to a specific timeline, scope, or backport policy.
- **Credit** the reporter in any subsequent advisory unless the reporter requests anonymity.

No representation or warranty of any kind is made regarding response time, severity assessment, scope of remediation, or availability of a fix. This is a small open-source project; please calibrate expectations accordingly.

If you require formal vulnerability-handling commitments — guaranteed response windows, SLA-bound remediation timelines, indemnification, or coordinated disclosure under a written agreement — that is a separate commercial relationship. Contact the same address above to discuss commercial-support terms.

## Supported versions

| Version    | Security reports accepted |
|------------|---------------------------|
| `main` branch (HEAD) | ✅ Yes |
| Tagged releases (latest) | ✅ Yes |
| Older tagged releases | ❌ No — upgrade to current |
| Any forked/modified version | ❌ No — the fork operator is responsible |

If you operate a fork or a derivative work, your fork is *your* responsibility under Apache License 2.0 §7 (Disclaimer of Warranty) and §8 (Limitation of Liability). Security issues unique to your modifications will not be triaged here.

## Out of scope

The following are NOT considered vulnerabilities in legion-graph itself:

- Misconfiguration of operator-controlled secrets (`JWT_SECRET_KEY`, `NEO4J_PASSWORD`, `POSTGRES_PASSWORD`, `REDIS_PASSWORD`, `GEMINI_API_KEY`) — operators are responsible for their own secret hygiene.
- Issues in third-party services that legion-graph references (Postgres, Neo4j/DozerDB, Qdrant, Redpanda, Redis, LLM providers) — report those to their respective upstreams.
- Issues that require pre-existing operator-level (root, database superuser) access to the deployment.
- `SKIP_EMAIL_VERIFICATION=true` is a documented developer-only flag; running it in production is operator misconfiguration, not a vulnerability.
- Behaviour differences between LLM providers other than Gemini 3.1 Flash Lite Preview (the only validated configuration — see "Validation status" in README.md).

## Commercial licensing and support

For commercial licensing, warranties, indemnification, or formal support agreements that go beyond the Apache License 2.0 open-source grant:

> **yubozhenko@icloud.com**

State clearly in the subject line whether your inquiry is about **security** or **commercial licensing** so it can be routed appropriately.
