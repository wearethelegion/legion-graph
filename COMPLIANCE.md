# Compliance Notes

This document collects compliance-related statements that may be requested by enterprise procurement, open-source program offices (OSPOs), or legal review teams evaluating legion-graph for inclusion in their environments. Nothing in this document modifies the [Apache License 2.0](LICENSE); see in particular §7 (Disclaimer of Warranty) and §8 (Limitation of Liability), which govern.

## Export control

legion-graph is general-purpose open-source software for building knowledge-graph retrieval systems. It has not been classified for export under the U.S. Export Administration Regulations (EAR), the EU Dual-Use Regulation, or any equivalent regime. The authors make no representation that the software is subject to, or exempt from, any particular export-control classification.

Operators who download, deploy, redistribute, or integrate legion-graph are solely responsible for determining their own compliance obligations under applicable export-control laws, sanctions regimes, and embargo lists in their jurisdiction(s) of operation.

## Cryptography

legion-graph uses cryptographic primitives only through standard, widely-deployed libraries:

- TLS termination is delegated to operator-controlled infrastructure (ingress, reverse proxy, cloud load balancer); legion-graph itself does not implement TLS.
- JWT signing/verification uses HMAC-SHA256 via the standard `python-jose` / `pyjwt` library.
- Password hashing uses `bcrypt` via `passlib`.
- TOTP (RFC 6238) is implemented via the `pyotp` library.
- All other cryptographic operations are delegated to standard Python and third-party libraries identified in the per-service `requirements*.txt` files.

legion-graph does not implement its own cryptographic primitives. No part of legion-graph is intended for use as encryption software within the meaning of EAR Category 5 Part 2 or equivalent classifications; any such classification of bundled dependencies is the responsibility of those upstream projects.

## Data handling

legion-graph stores user-provided content (code, documents, brain knowledge entries) in operator-controlled databases (PostgreSQL, Neo4j, Qdrant). It does not transmit user content to any third party other than:

- The configured LLM provider, when ingestion or search invokes an LLM call (see "LLM provider choice" in the [README](README.md)).
- The configured embedding provider, when a new chunk is embedded.

Operators are responsible for ensuring their chosen LLM and embedding providers are compatible with their data-residency, privacy, and regulatory obligations (GDPR, HIPAA, SOC 2, etc.). The default Gemini configuration sends content to Google's Gemini API; operators with stricter data-handling requirements should switch to a self-hosted LLM (Ollama, vLLM) and local embeddings (FastEmbed) — see the README "LLM provider choice" section for the recipe.

legion-graph has not undergone formal SOC 2, ISO 27001, HIPAA, or PCI-DSS audit. No representation of compliance with any of these frameworks is made.

## Software Bill of Materials (SBOM)

A formal SBOM is not bundled with the repository. To generate one against the pinned dependencies:

```bash
pip install pip-licenses cyclonedx-bom
pip-licenses --format=markdown --output-file=SBOM.md
cyclonedx-py -o sbom.json
```

The per-service `requirements*.txt` files in this repository pin direct dependencies. Transitive dependencies are resolved by `pip` at install time inside the Docker images and are not separately pinned. Operators requiring reproducible, fully-pinned builds should generate and commit a lockfile (`pip-compile`, `uv pip compile`, or equivalent) before deployment.

## Vulnerability and CVE management

legion-graph does not maintain a formal CVE-management process or a published security advisory feed. Vulnerability reports are accepted per [SECURITY.md](SECURITY.md) on a best-effort basis with no SLA. Operators requiring formal CVE response — guaranteed advisory publication, coordinated disclosure, backported patches to old versions — should treat that as part of a separate commercial support relationship; see the contact in SECURITY.md.

## Governing law

The Apache License 2.0 is silent on governing law by design; it is intended to be jurisdiction-neutral. legion-graph follows that convention. Any commercial agreement (paid licence, support contract, indemnification, or warranty) entered into separately between an operator and the project maintainers is governed by terms to be agreed at that time, in a separate written agreement, and is not addressed by this open-source release.

## Updates to this document

This file is informational and may be updated without notice. The version in the `main` branch is authoritative. If you require a specific compliance representation in writing, that must form part of a separate commercial agreement; the contents of this file alone do not constitute legal advice, a warranty, or a representation of fitness for any particular regulatory framework.
