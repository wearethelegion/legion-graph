# Contributing to legion-graph

Thank you for considering a contribution. legion-graph is the open-source Knowledge pillar of the [LEGION](https://wearethelegion.com) ecosystem and is maintained on a part-time basis. Please read this document end-to-end before opening a pull request — it covers how contributions are governed, the legal grant you make by contributing, and what the maintainers can realistically commit to.

## Before you start

- **Read the [README](README.md)** end-to-end, including the "Validation status" callout and the `## License & Warranty` section. Make sure you understand the scope of what's been tested vs claimed.
- **Open an issue first for non-trivial changes.** Small bug fixes, typo corrections, and documentation polish can go straight to a PR. Anything that touches architecture, public APIs, schema, ingestion pipeline behaviour, or LLM provider plumbing should be discussed in an issue first so we can avoid wasted work.
- **Search existing issues and PRs** before opening a new one.

## Developer Certificate of Origin (DCO)

legion-graph requires every commit to be signed off under the [Developer Certificate of Origin (DCO) v1.1](https://developercertificate.org/). The DCO is a lightweight attestation — no paperwork to sign — that says, in essence:

> *I have the right to contribute this code under the project's license.*

To sign off, add the `-s` flag when you commit:

```bash
git commit -s -m "your commit message"
```

This appends a `Signed-off-by: Your Name <your.email@example.com>` line to your commit message, using the name and email configured in your git config. **By signing off, you attest to all four clauses of the DCO**:

> 1. The contribution was created in whole or in part by me and I have the right to submit it under the open-source license indicated in the file; or
> 2. The contribution is based upon previous work that, to the best of my knowledge, is covered under an appropriate open-source license and I have the right under that license to submit that work with modifications, whether created in whole or in part by me, under the same open-source license (unless I am permitted to submit under a different license), as indicated in the file; or
> 3. The contribution was provided directly to me by some other person who certified (1), (2), or (3) and I have not modified it.
> 4. I understand and agree that this project and the contribution are public and that a record of the contribution (including all personal information I submit with it, including my sign-off) is maintained indefinitely and may be redistributed consistent with this project or the open-source license(s) involved.

Commits without a valid `Signed-off-by:` line will not be merged.

By contributing, you also agree that your contribution is licensed under the [Apache License 2.0](LICENSE) — the same license that covers the rest of the project. You retain copyright in your contribution; you grant a license under Apache 2.0 §2 (copyright) and §3 (patent) to the project and downstream users.

## AI-assisted contributions

If any portion of your contribution was generated, suggested, or refactored using AI coding tools (GitHub Copilot, Claude, ChatGPT, Cursor, etc.), you warrant that:

1. You have **personally reviewed** the AI-generated code and understand what it does.
2. The code does not reproduce, in substantial part, copyleft-licensed (GPL, AGPL, SSPL) material from the AI tool's training data.
3. You take **personal responsibility** for the correctness, security, and licensing of the contribution, the same as if you had written it from scratch.
4. Your DCO sign-off applies to the AI-assisted code exactly as it would to handwritten code.

You are not required to disclose AI tool usage in PRs, but you may not use AI tool involvement to disclaim responsibility for what you submit.

## Code style and quality

- Match the existing style in the file you're editing. Don't reformat unrelated code.
- Python: follow PEP 8 with the line-length and formatter conventions already in use in the touched module.
- Protocol buffers: regenerate generated stubs with the existing `make protos` (or equivalent) target rather than hand-editing generated files.
- Tests: if you fix a bug, add a regression test. If you add a feature, add coverage. The Postman E2E suite (`postman/`) is the canonical "does it actually work" check — running it locally before opening a PR is encouraged.
- Commit messages: imperative mood ("Add X", "Fix Y"), not past tense. One logical change per commit where practical.

## Pull request process

1. Fork the repository and create a feature branch from `main`.
2. Make your changes, including tests and documentation.
3. Sign off every commit (`git commit -s`).
4. Open a PR against `main` with a clear description: what you changed, why, and how to verify.
5. The maintainers will review on a best-effort basis. There is no guaranteed review timeline. Polite reminders after two weeks of inactivity are welcome.
6. Address review feedback in additional commits (don't force-push a rebase mid-review — it makes incremental review harder). Final squash, if requested, will be discussed before merge.

## What contributions are likely to be accepted

- Bug fixes with regression tests.
- Documentation improvements that match the existing structure and the forensic pipeline map (see README "Further reading").
- Additional LLM provider configurations with documentation of which models you tested.
- Performance improvements with before/after measurements.
- Test coverage improvements.

## What contributions are unlikely to be accepted

- Sweeping reformatting or "modernisation" of working code.
- Replacing well-established dependencies with personal favourites.
- New features that significantly expand surface area without aligning with the existing four-pillar architecture (Agents / Knowledge / Memory / Workflows).
- Changes that re-introduce stripped-out coupling with the rest of the (proprietary) LEGION stack.
- Renames of public-facing identifiers (env vars, container names, DB names) without a strong reason — these have downstream operator cost.

## Commercial relationships

If you are contributing on behalf of an employer and your employer requires a signed Contributor License Agreement (CLA) rather than a DCO sign-off, contact yubozhenko@icloud.com and we can discuss. The default contribution model is DCO; CLA is available on request for organisations whose legal teams require it.

If you wish to discuss commercial licensing, warranties, indemnification, or formal support of legion-graph (beyond the open-source Apache 2.0 grant), that is a **separate commercial relationship**, not addressed by this Apache 2.0 release or by this CONTRIBUTING document. Contact yubozhenko@icloud.com to discuss; any resulting commercial agreement is governed by terms to be agreed at that time.

## Code of conduct

All contributors are expected to follow the [Code of Conduct](CODE_OF_CONDUCT.md). Report violations to yubozhenko@icloud.com.

## Questions

Open a GitHub Discussion if your question is open-ended or design-related. Open an issue if you've found a bug or have a concrete feature proposal. Email yubozhenko@icloud.com only for security vulnerabilities (see [SECURITY.md](SECURITY.md)) or commercial inquiries.
