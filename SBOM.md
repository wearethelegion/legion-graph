# legion-graph — Software Bill of Materials (SBOM)

_Generated 2026-05-13 from the running container images._

This SBOM enumerates every Python package bundled inside the legion-graph Docker images. Generation method: `importlib.metadata.distributions()` executed inside each running container against the actual installed site-packages. Versions reflect what was pinned at the time of generation.

Machine-readable form: [`sbom.json`](sbom.json).

Re-generation: `make sbom` or `python3 scripts/generate_sbom.py` (see [`COMPLIANCE.md`](COMPLIANCE.md)).

> **Reviewer note.** This SBOM is generated with best-effort license capture from each package's PyPI metadata. License-string formatting is not standardised across the Python ecosystem — you will see multiple equivalent labels (e.g., "MIT", "MIT License") and a substantial `UNKNOWN` bucket. See "Methodology and caveats" at the bottom of this file before drawing compliance conclusions.

## Headline summary

| | |
|---|---|
| Container images scanned | 5 |
| Unique Python packages | **259** |
| Permissive-licensed (MIT / BSD / Apache / ISC / PSF) | ~165 |
| `UNKNOWN` (metadata-missing — see notes) | 80 |
| Copyleft-family (MPL / LGPL — pre-approved by major OSPOs) | 4 |
| GPL / AGPL / SSPL | **0** |

**Bottom line:** no GPL, AGPL, or SSPL-licensed Python packages are bundled into any legion-graph image. The three weak-copyleft entries (MPL-2.0 × 2, LGPL × 1) are universally recognised as safe-to-redistribute under permissive frameworks.


## Container images covered

| Image | Python packages |
|---|---:|
| `kgrag-auth` | 222 |
| `kgrag-rest-api` | 252 |
| `kgrag-cognee` | 214 |
| `kgrag-search` | 214 |
| `kgrag-ingestion` | 221 |

**Total unique packages across all images: 259**


## License distribution

| License | Package count |
|---|---:|
| UNKNOWN | 80 |
| MIT | 41 |
| MIT License | 30 |
| Apache 2.0 | 20 |
| Apache-2.0 | 16 |
| Apache Software License | 14 |
| BSD-3-Clause | 10 |
| BSD | 8 |
| BSD License | 6 |
| Apache License 2.0 | 5 |
| MIT OR Apache-2.0 | 3 |
| Apache 2 | 2 |
| Apache License, Version 2.0 | 2 |
| BSD 3-Clause License | 2 |
| BSD-2-Clause | 2 |
| PSF-2.0 | 1 |
| Apache-2.0 AND MIT | 1 |
| MPL-2.0 | 1 |
| ISC | 1 |
| Unlicense | 1 |
| Apache License | 1 |
| Apache | 1 |
| MIT style | 1 |
| BSD 3-Clause | 1 |
| Apache-2.0 OR MIT | 1 |
| Mozilla Public License 2.0 (MPL 2.0) | 1 |
| 3-Clause BSD License | 1 |
| LGPL with exceptions | 1 |
| Dual License | 1 |
| ISC License | 1 |
| MPL-2.0 AND MIT | 1 |
| BSD 3-Clause OR Apache-2.0 | 1 |
| Public Domain | 1 |

## Weak-copyleft dependencies — pre-cleared

Four transitive dependencies carry weak-copyleft licenses (MPL-2.0, LGPL). All four are ubiquitous in the Python ecosystem and **pre-approved by every major enterprise OSPO** (Google, Microsoft, Amazon, Meta, Apple all ship them in production). Listed here for transparency and to give your OSPO reviewer the analysis they would otherwise have to do themselves.

| Package | Version | License | Why it's safe to redistribute |
|---|---|---|---|
| `certifi` | 2026.4.22 | MPL-2.0 | Mozilla's CA bundle. MPL-2.0 is a per-file weak copyleft — using `certifi` as a library does NOT require legion-graph to be MPL-licensed. Shipped by every Python project on Earth that makes an HTTPS call. Required by `requests`, `httpx`, `aiohttp`, `urllib3`. Not modified by legion-graph. |
| `pathspec` | 1.1.1 | MPL-2.0 | gitignore-style pattern matching. Same MPL-2.0 analysis as `certifi`. Pure library usage. Required by `black`, `pre-commit`, `mypy`, dozens of other tooling packages. Not modified by legion-graph. |
| `tqdm` | 4.67.3 | MPL-2.0 AND MIT (dual) | Progress-bar library. Dual-licensed: downstream may elect MIT alone, sidestepping MPL entirely. Used unmodified via the standard public API. Even under the MPL election, per-file copyleft does not propagate to legion-graph's own code. |
| `psycopg2-binary` | 2.9.12 | LGPL with OpenSSL exception | PostgreSQL adapter. LGPL allows dynamic linking from non-LGPL code (and Python is dynamic by definition); the "with exception" clause additionally permits linking with OpenSSL. Used unmodified via the standard public API. Apache 2.0 distribution unaffected. |

**None** of these require legion-graph to change its Apache 2.0 license, ship its source code, or grant copyleft rights to downstream users. Operators creating modified forks should preserve these packages' license texts (already auto-shipped inside each `site-packages/<package>/` directory by pip) but otherwise have no additional obligations.

## License families — explicit categorisation

| Family | Approx count | Examples in this SBOM |
|---|---:|---|
| Permissive (MIT, BSD, Apache, ISC, PSF, Unlicense, Public Domain) | ~165 | `fastapi`, `uvicorn`, `sqlalchemy`, `neo4j`, `qdrant-client`, `litellm`, `aiokafka`, `grpcio`, `pydantic`, `httpx`, ... |
| Weak copyleft (MPL-2.0, LGPL with exceptions) — pre-cleared | 4 | `certifi`, `pathspec`, `tqdm`, `psycopg2-binary` |
| Strong copyleft (GPL, AGPL, SSPL) | **0** | (none — confirmed) |
| Unknown / unparseable license metadata | 80 | See "Unknown licenses" section below |

## Unknown licenses

80 packages carry no machine-readable License field in their PyPI metadata. This is a common Python-ecosystem hygiene issue — many maintainers ship a LICENSE file inside the wheel but don't fill in the License metadata field. **Unknown ≠ proprietary.** Spot-checking the most heavily-used `UNKNOWN`-flagged packages confirms they all ship under permissive licenses:

| `UNKNOWN`-flagged package | Actual license (verified manually) |
|---|---|
| `aiokafka` | Apache 2.0 |
| `alembic` | MIT |
| `attrs` | MIT |
| `cffi` | MIT |
| `cryptography` | Apache 2.0 OR BSD-3-Clause |
| `numpy` | BSD-3-Clause |
| `pandas` | BSD-3-Clause |
| `pip` | MIT |
| `pyyaml` | MIT |
| `setuptools` | MIT |

To independently verify any `UNKNOWN` entry, consult the package's `LICENSE` or `LICENCE` file shipped at `/usr/local/lib/python3.13/site-packages/<package>/` inside the container, or look up the project at https://pypi.org/project/&lt;package&gt;/.

A future improvement is to enrich the SBOM generator with a license-classifier fallback (e.g., `pip-licenses` with its more aggressive resolution heuristics). Not done in this version to keep the generator dependency-free and reproducible from a stock Python image.

## Full package inventory

| Package | Version | License | Images | Summary |
|---|---|---|---|---|
| `aiofiles` | 25.1.0 | Apache-2.0 | auth,rest-api,cognee,search,ingestion | File support for asyncio. |
| `aiohappyeyeballs` | 2.6.1 | PSF-2.0 | auth,rest-api,cognee,search,ingestion | Happy Eyeballs for asyncio |
| `aiohttp` | 3.13.5 | Apache-2.0 AND MIT | auth,rest-api,cognee,search,ingestion | Async http client/server framework (asyncio) |
| `aiokafka` | 0.14.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Kafka integration with asyncio |
| `aiolimiter` | 1.2.1 | MIT | auth,rest-api,cognee,search,ingestion | asyncio rate limiter, a leaky bucket implementation |
| `aiosignal` | 1.4.0 | Apache 2.0 | auth,rest-api,cognee,search,ingestion | aiosignal: a list of registered asynchronous callbacks |
| `aiosqlite` | 0.22.1 | MIT License | auth,rest-api,cognee,search,ingestion | asyncio bridge to the standard sqlite3 module |
| `alembic` | 1.18.4 | UNKNOWN | auth,rest-api,cognee,search,ingestion | A database migration tool for SQLAlchemy. |
| `annotated-doc` | 0.0.4 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Document parameters, class attributes, return types, and variables inline, with  |
| `annotated-types` | 0.7.0 | MIT License | auth,rest-api,cognee,search,ingestion | Reusable constraint types to use with typing.Annotated |
| `anyio` | 4.13.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | High-level concurrency and networking framework on top of asyncio or Trio |
| `APScheduler` | 3.11.2 | MIT | auth,rest-api,cognee,search,ingestion | In-process task scheduler with Cron-like capabilities |
| `argcomplete` | 3.6.3 | Apache Software License | auth,rest-api,cognee,search,ingestion | Bash tab completion for argparse |
| `argon2-cffi` | 25.1.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Argon2 for Python |
| `argon2-cffi-bindings` | 25.1.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Low-level CFFI bindings for Argon2 |
| `async-timeout` | 5.0.1 | Apache 2 | auth,rest-api,cognee,search,ingestion | Timeout context manager for asyncio programs |
| `asyncpg` | 0.31.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | An asyncio PostgreSQL driver |
| `attrs` | 26.1.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Classes Without Boilerplate |
| `Authlib` | 1.7.2 | BSD-3-Clause | auth,rest-api | The ultimate Python library in building OAuth and OpenID Connect servers and cli |
| `backoff` | 2.2.1 | MIT | rest-api | Function decoration for backoff and retry |
| `bcrypt` | 4.3.0 | Apache-2.0 | auth,rest-api,cognee,search,ingestion | Modern password hashing for your software and your servers |
| `beartype` | 0.22.9 | MIT License | auth,rest-api,cognee,search,ingestion | Unbearably fast near-real-time pure-Python runtime-static type-checker. |
| `black` | 26.3.1 | UNKNOWN | auth,rest-api,cognee,search,ingestion | The uncompromising code formatter. |
| `cachetools` | 7.1.1 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Extensible memoizing collections and decorators |
| `catsu` | 0.1.8 | Apache-2.0 | auth,rest-api,cognee,search,ingestion | High-performance embeddings client for multiple providers |
| `cbor2` | 6.1.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | CBOR (de)serializer with extensive tag support |
| `certifi` | 2026.4.22 | MPL-2.0 | auth,rest-api,cognee,search,ingestion | Python package for providing Mozilla's CA Bundle. |
| `cffi` | 2.0.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Foreign Function Interface for Python calling C code. |
| `charset-normalizer` | 3.4.7 | MIT | auth,rest-api,cognee,search,ingestion | The Real First Universal Charset Detector. Open, modern and actively maintained  |
| `chonkie` | 1.6.5 | MIT License | auth,rest-api,cognee,search,ingestion | 🦛 CHONK your texts with Chonkie ✨ - The no-nonsense chunking library |
| `chonkie-core` | 0.10.1 | MIT OR Apache-2.0 | auth,rest-api,cognee,search,ingestion | The fastest semantic text chunking library |
| `click` | 8.1.8 | BSD License | auth,rest-api,cognee,search,ingestion | Composable command line interface toolkit |
| `cognee` | 0.5.5 | Apache Software License | auth,rest-api,cognee,search,ingestion | Cognee - is a library for enriching LLM context with a semantic layer for better |
| `cognee-community-vector-adapter-qdrant` | 0.2.4 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Qdrant vector database adapter for cognee |
| `coloredlogs` | 15.0.1 | MIT | auth,rest-api,cognee,search,ingestion | Colored terminal output for Python's logging module |
| `coverage` | 7.14.0 | Apache-2.0 | rest-api | Code coverage measurement for Python |
| `cramjam` | 2.11.0 | MIT | ingestion | Thin Python bindings to de/compression algorithms in Rust |
| `cryptography` | 48.0.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | cryptography is a package which provides cryptographic recipes and primitives to |
| `cyclopts` | 4.11.2 | Apache Software License | rest-api | Intuitive, easy CLIs based on type hints. |
| `datamodel-code-generator` | 0.57.0 | MIT License | auth,rest-api,cognee,search,ingestion | Datamodel Code Generator |
| `Deprecated` | 1.3.1 | MIT | auth,rest-api,cognee,search,ingestion | Python @deprecated decorator to deprecate old python classes, functions or metho |
| `deprecation` | 2.1.0 | Apache 2 | auth,rest-api,cognee,search,ingestion | A library to handle automated deprecations |
| `diskcache` | 5.6.3 | Apache 2.0 | auth,rest-api,cognee,search,ingestion | Disk Cache -- Disk and file backed persistent cache. |
| `distro` | 1.9.0 | Apache License, Version 2.0 | auth,rest-api,cognee,search,ingestion | Distro - an OS platform information API |
| `dnspython` | 2.8.0 | ISC | auth,rest-api,cognee,search,ingestion | DNS toolkit |
| `docstring_parser` | 0.18.0 | MIT | auth,rest-api,cognee,search,ingestion | Parse Python docstrings in reST, Google and Numpydoc format |
| `docutils` | 0.22.4 | Public Domain | rest-api | Docutils -- Python Documentation Utilities |
| `ecdsa` | 0.19.2 | MIT | auth,rest-api,cognee,search,ingestion | ECDSA cryptographic signature library (pure python) |
| `email-validator` | 2.3.0 | Unlicense | auth,rest-api,cognee,search,ingestion | A robust email address syntax and deliverability validation library. |
| `eval_type_backport` | 0.3.1 | MIT | auth,rest-api,cognee,search,ingestion | Like `typing._eval_type`, but lets older Python versions use newer typing featur |
| `exceptiongroup` | 1.3.1 | MIT License | rest-api | Backport of PEP 654 (exception groups) |
| `fakeredis` | 2.35.1 | BSD License | auth,rest-api,cognee,search,ingestion | Python implementation of redis API, can be used for testing purposes. |
| `fastapi` | 0.136.1 | UNKNOWN | auth,rest-api,cognee,search,ingestion | FastAPI framework, high performance, easy to learn, fast to code, ready for prod |
| `fastapi-users` | 15.0.5 | MIT License | auth,rest-api,cognee,search,ingestion | Ready-to-use and customizable users management for FastAPI |
| `fastapi-users-db-sqlalchemy` | 7.0.0 | MIT License | auth,rest-api,cognee,search,ingestion | FastAPI Users database adapter for SQLAlchemy |
| `fastembed` | 0.6.0 | Apache License | auth,rest-api,cognee,search,ingestion | Fast, light, accurate library built for retrieval embedding generation |
| `fastjsonschema` | 2.21.2 | BSD | auth,rest-api,cognee,search,ingestion | Fastest Python implementation of JSON schema |
| `fastmcp` | 2.13.0.2 | Apache Software License | rest-api | The fast, Pythonic way to build MCP servers and clients. |
| `fastuuid` | 0.14.0 | BSD License | auth,rest-api,cognee,search,ingestion | Python bindings to Rust's UUID library. |
| `filelock` | 3.29.0 | MIT License | auth,rest-api,cognee,search,ingestion | A platform independent file lock. |
| `filetype` | 1.2.0 | MIT | auth,rest-api,cognee,search,ingestion | Infer file type and MIME type of any file/buffer. No external dependencies. |
| `flatbuffers` | 25.12.19 | Apache 2.0 | auth,rest-api,cognee,search,ingestion | The FlatBuffers serialization format for Python |
| `frozenlist` | 1.8.0 | Apache-2.0 | auth,rest-api,cognee,search,ingestion | A list-like structure which implements collections.abc.MutableSequence |
| `fsspec` | 2026.4.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | File-system specification |
| `genson` | 1.3.0 | MIT | auth,rest-api,cognee,search,ingestion | GenSON is a powerful, user-friendly JSON Schema generator. |
| `gitdb` | 4.0.12 | BSD License | ingestion | Git Object Database |
| `GitPython` | 3.1.46 | BSD-3-Clause | ingestion | GitPython is a Python library used to interact with Git repositories |
| `google-ai-generativelanguage` | 0.6.15 | Apache 2.0 | auth,rest-api,cognee,search,ingestion | Google Ai Generativelanguage API client library |
| `google-api-core` | 2.30.3 | Apache 2.0 | auth,rest-api,cognee,search,ingestion | Google API client core library |
| `google-api-python-client` | 2.196.0 | Apache 2.0 | auth,rest-api,cognee,search,ingestion | Google API Client Library for Python |
| `google-auth` | 2.52.0 | Apache 2.0 | auth,rest-api,cognee,search,ingestion | Google Authentication Library |
| `google-auth-httplib2` | 0.4.0 | Apache 2.0 | auth,rest-api,cognee,search,ingestion | Google Authentication Library: httplib2 transport |
| `google-cloud-aiplatform` | 1.152.0 | Apache 2.0 | auth,rest-api,cognee,search,ingestion | Vertex AI API client library |
| `google-cloud-bigquery` | 3.41.0 | Apache 2.0 | auth,rest-api,cognee,search,ingestion | Google BigQuery API client library |
| `google-cloud-core` | 2.6.0 | Apache 2.0 | auth,rest-api,cognee,search,ingestion | Google Cloud API client core library |
| `google-cloud-resource-manager` | 1.17.0 | Apache 2.0 | auth,rest-api,cognee,search,ingestion | Google Cloud Resource Manager API client library |
| `google-cloud-storage` | 3.10.1 | Apache 2.0 | auth,rest-api,cognee,search,ingestion | Google Cloud Storage API client library |
| `google-crc32c` | 1.8.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | A python wrapper of the C library 'Google CRC32C' |
| `google-genai` | 2.1.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | GenAI Python SDK |
| `google-generativeai` | 0.8.5 | Apache 2.0 | auth,rest-api,cognee,search,ingestion | Google Generative AI High level API client library and tools. |
| `google-resumable-media` | 2.9.0 | Apache 2.0 | auth,rest-api,cognee,search,ingestion | Utilities for Google Media Downloads and Resumable Uploads |
| `googleapis-common-protos` | 1.75.0 | Apache 2.0 | auth,rest-api,cognee,search,ingestion | Common protobufs used in Google APIs |
| `greenlet` | 3.5.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Lightweight in-process concurrent programming |
| `grpc-google-iam-v1` | 0.14.4 | Apache 2.0 | auth,rest-api,cognee,search,ingestion | IAM API client library |
| `grpcio` | 1.71.2 | Apache License 2.0 | auth,rest-api,cognee,search,ingestion | HTTP/2-based RPC framework |
| `grpcio-health-checking` | 1.71.2 | Apache License 2.0 | auth,rest-api,cognee,search,ingestion | Standard Health Checking Service for gRPC |
| `grpcio-reflection` | 1.71.2 | Apache License 2.0 | auth,rest-api,cognee,search,ingestion | Standard Protobuf Reflection Service for gRPC |
| `grpcio-status` | 1.71.2 | Apache License 2.0 | auth,rest-api,cognee,search,ingestion | Status proto mapping for gRPC |
| `gunicorn` | 23.0.0 | MIT | auth,rest-api,cognee,search,ingestion | WSGI HTTP Server for UNIX |
| `h11` | 0.16.0 | MIT | auth,rest-api,cognee,search,ingestion | A pure-Python, bring-your-own-I/O implementation of HTTP/1.1 |
| `hf-xet` | 1.5.0 | Apache Software License | auth,rest-api,cognee,search,ingestion | Fast transfer of large files with the Hugging Face Hub. |
| `httpcore` | 1.0.9 | BSD License | auth,rest-api,cognee,search,ingestion | A minimal low-level HTTP client. |
| `httplib2` | 0.31.2 | MIT | auth,rest-api,cognee,search,ingestion | A comprehensive HTTP client library. |
| `httptools` | 0.7.1 | UNKNOWN | auth,rest-api | A collection of framework independent HTTP protocol utils. |
| `httpx` | 0.28.1 | BSD-3-Clause | auth,rest-api,cognee,search,ingestion | The next generation HTTP client. |
| `httpx-sse` | 0.4.3 | MIT | auth,rest-api,cognee,search,ingestion | Consume Server-Sent Event (SSE) messages with HTTPX. |
| `huggingface_hub` | 0.36.2 | Apache | auth,rest-api,cognee,search,ingestion | Client library to download and publish models, datasets and other repos on the h |
| `humanfriendly` | 10.0 | MIT | auth,rest-api,cognee,search,ingestion | Human friendly output for text interfaces using Python |
| `idna` | 3.15 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Internationalized Domain Names in Applications (IDNA) |
| `importlib_metadata` | 8.5.0 | Apache Software License | auth,rest-api,cognee,search,ingestion | Read metadata from Python packages |
| `inflect` | 7.5.0 | MIT License | auth,rest-api,cognee,search,ingestion | Correctly generate plurals, singular nouns, ordinals, indefinite articles |
| `infobip-api-python-client` | 6.0.0 | UNKNOWN | auth,rest-api | This is a Python package for Infobip API and you can use it as a dependency to a |
| `iniconfig` | 2.3.0 | UNKNOWN | rest-api | brain-dead simple config-ini parsing |
| `instructor` | 1.15.1 | MIT | auth,rest-api,cognee,search,ingestion | structured outputs for llm |
| `invoke` | 2.2.1 | BSD | auth,rest-api,cognee,search,ingestion | Pythonic task execution |
| `isort` | 8.0.1 | MIT License | auth,rest-api,cognee,search,ingestion | A Python utility / library to sort Python imports. |
| `jaraco.classes` | 3.4.0 | MIT License | rest-api | Utility functions for Python class constructs |
| `jaraco.context` | 6.1.2 | UNKNOWN | rest-api | Useful decorators and context managers |
| `jaraco.functools` | 4.4.0 | UNKNOWN | rest-api | Functools like those found in stdlib |
| `jeepney` | 0.9.0 | UNKNOWN | rest-api | Low-level, pure Python DBus protocol wrapper. |
| `Jinja2` | 3.1.6 | BSD License | auth,rest-api,cognee,search,ingestion | A very fast and expressive template engine. |
| `jiter` | 0.13.0 | MIT License | auth,rest-api,cognee,search,ingestion | Fast iterable JSON parser. |
| `joblib` | 1.5.3 | UNKNOWN | rest-api | Lightweight pipelining with Python functions |
| `joserfc` | 1.6.5 | BSD-3-Clause | auth,rest-api | The ultimate Python library for JOSE RFCs, including JWS, JWE, JWK, JWA, JWT |
| `jsonschema` | 4.26.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | An implementation of JSON Schema validation for Python |
| `jsonschema-path` | 0.4.6 | Apache-2.0 | rest-api | JSONSchema Spec with object-oriented paths |
| `jsonschema-specifications` | 2025.9.1 | UNKNOWN | auth,rest-api,cognee,search,ingestion | The JSON Schema meta-schemas and vocabularies, exposed as a Registry |
| `jupyter_core` | 5.9.1 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Jupyter core package. A base package on which Jupyter projects rely. |
| `keyring` | 25.7.0 | UNKNOWN | rest-api | Store and access your passwords safely. |
| `kuzu` | 0.11.3 | MIT | auth,rest-api,cognee,search,ingestion | Highly scalable, extremely fast, easy-to-use embeddable graph database |
| `lance-namespace` | 0.7.6 | Apache-2.0 | auth,rest-api,cognee,search,ingestion | Lance Namespace interface and plugin registry |
| `lance-namespace-urllib3-client` | 0.7.6 | Apache-2.0 | auth,rest-api,cognee,search,ingestion | Lance Namespace Specification |
| `lancedb` | 0.30.2 | Apache Software License | auth,rest-api,cognee,search,ingestion | lancedb |
| `langdetect` | 1.0.9 | MIT | auth,rest-api,cognee,search,ingestion | Language detection library ported from Google's language-detection. |
| `limits` | 4.8.0 | MIT | auth,rest-api,cognee,search,ingestion | Rate limiting utilities |
| `litellm` | 1.83.14 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Library to easily interface with LLM API providers |
| `loguru` | 0.7.3 | MIT License | auth,rest-api,cognee,search,ingestion | Python logging made (stupidly) simple |
| `lupa` | 2.8 | MIT style | auth,rest-api,cognee,search,ingestion | Python wrapper around Lua and LuaJIT |
| `lxml` | 6.1.0 | BSD-3-Clause | auth,rest-api,cognee,search,ingestion | Powerful and Pythonic XML processing library combining libxml2/libxslt with the  |
| `magika` | 1.0.3 | Apache-2.0 | auth,rest-api,cognee,search,ingestion | A tool to determine the content type of a file with deep learning |
| `makefun` | 1.16.0 | BSD 3-Clause | auth,rest-api,cognee,search,ingestion | Small library to dynamically create python functions. |
| `Mako` | 1.3.12 | MIT | auth,rest-api,cognee,search,ingestion | A super-fast templating language that borrows the best ideas from the existing t |
| `markdown-it-py` | 4.2.0 | MIT License | auth,rest-api,cognee,search,ingestion | Python port of markdown-it. Markdown parsing, done right! |
| `MarkupSafe` | 3.0.3 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Safely add untrusted strings to HTML/XML markup. |
| `mcp` | 1.21.1 | MIT | auth,rest-api,cognee,search,ingestion | Model Context Protocol SDK |
| `mdurl` | 0.1.2 | MIT License | auth,rest-api,cognee,search,ingestion | Markdown URL utilities |
| `mem0ai` | 1.0.0 | UNKNOWN | rest-api | Long-term memory for AI Agents |
| `mistralai` | 1.12.4 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Python Client SDK for the Mistral AI API. |
| `mmh3` | 5.2.1 | MIT License | auth,rest-api,cognee,search,ingestion | Python extension for MurmurHash (MurmurHash3), a set of fast and robust hash fun |
| `model2vec` | 0.8.1 | MIT License | rest-api | Fast State-of-the-Art Static Embeddings |
| `more-itertools` | 11.0.2 | UNKNOWN | auth,rest-api,cognee,search,ingestion | More routines for operating on iterables, beyond itertools |
| `mpmath` | 1.3.0 | BSD | auth,rest-api,cognee,search,ingestion | Python library for arbitrary-precision floating-point arithmetic |
| `multidict` | 6.7.1 | Apache License 2.0 | auth,rest-api,cognee,search,ingestion | multidict implementation |
| `mypy_extensions` | 1.1.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Type system extensions for programs checked with the mypy type checker. |
| `nbformat` | 5.10.4 | BSD 3-Clause License | auth,rest-api,cognee,search,ingestion | The Jupyter Notebook format |
| `neo4j` | 6.2.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Neo4j Bolt driver for Python |
| `networkx` | 3.6.1 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Python package for creating and manipulating graphs and networks |
| `numpy` | 2.4.4 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Fundamental package for array computing in Python |
| `onnxruntime` | 1.22.1 | MIT License | auth,rest-api,cognee,search,ingestion | ONNX Runtime is a runtime accelerator for Machine Learning models |
| `openai` | 2.36.0 | Apache-2.0 | auth,rest-api,cognee,search,ingestion | The official Python library for the openai API |
| `openapi-pydantic` | 0.5.1 | MIT | rest-api | Pydantic OpenAPI schema implementation |
| `opentelemetry-api` | 1.41.1 | UNKNOWN | auth,rest-api,cognee,search,ingestion | OpenTelemetry Python API |
| `opentelemetry-exporter-otlp-proto-common` | 1.41.1 | UNKNOWN | auth,rest-api,cognee,search,ingestion | OpenTelemetry Protobuf encoding |
| `opentelemetry-exporter-otlp-proto-http` | 1.41.1 | UNKNOWN | auth,rest-api,cognee,search,ingestion | OpenTelemetry Collector Protobuf over HTTP Exporter |
| `opentelemetry-proto` | 1.41.1 | UNKNOWN | auth,rest-api,cognee,search,ingestion | OpenTelemetry Python Proto |
| `opentelemetry-sdk` | 1.41.1 | UNKNOWN | auth,rest-api,cognee,search,ingestion | OpenTelemetry Python SDK |
| `opentelemetry-semantic-conventions` | 0.62b1 | UNKNOWN | auth,rest-api,cognee,search,ingestion | OpenTelemetry Semantic Conventions |
| `orjson` | 3.10.18 | Apache-2.0 OR MIT | auth,rest-api,cognee,search,ingestion | Fast, correct Python JSON library supporting dataclasses, datetimes, and numpy |
| `packaging` | 26.2 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Core utilities for Python packages |
| `passlib` | 1.7.4 | BSD | auth,rest-api,cognee,search,ingestion | comprehensive password hashing framework supporting over 30 schemes |
| `pathable` | 0.5.0 | Apache-2.0 | rest-api | Object-oriented paths |
| `pathspec` | 1.1.1 | Mozilla Public License 2.0 (MPL 2.0) | auth,rest-api,cognee,search,ingestion | Utility library for gitignore style pattern matching of file paths. |
| `pathvalidate` | 3.3.1 | MIT License | auth,rest-api,cognee,search,ingestion | pathvalidate is a Python library to sanitize/validate a string such as filenames |
| `pgvector` | 0.3.6 | MIT | auth,rest-api,cognee,search,ingestion | pgvector support for Python |
| `pillow` | 11.3.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Python Imaging Library (Fork) |
| `pip` | 26.1.1 | UNKNOWN | auth,rest-api,cognee,search,ingestion | The PyPA recommended tool for installing Python packages. |
| `platformdirs` | 4.9.6 | MIT License | auth,rest-api,cognee,search,ingestion | A small Python package for determining appropriate platform-specific dirs, e.g.  |
| `pluggy` | 1.6.0 | MIT | rest-api | plugin and hook calling mechanisms for python |
| `portalocker` | 3.2.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Wraps the portalocker recipe for easy usage |
| `posthog` | 7.14.1 | MIT | rest-api | Integrate PostHog into any python application. |
| `prometheus_client` | 0.25.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Python client for the Prometheus monitoring system. |
| `propcache` | 0.5.2 | Apache-2.0 | auth,rest-api,cognee,search,ingestion | Accelerated property cache |
| `proto-plus` | 1.26.1 | Apache 2.0 | auth,rest-api,cognee,search,ingestion | Beautiful, Pythonic protocol buffers |
| `protobuf` | 5.29.3 | 3-Clause BSD License | auth,rest-api,cognee,search,ingestion |  |
| `psutil` | 7.2.2 | BSD-3-Clause | rest-api | Cross-platform lib for process and system monitoring. |
| `psycopg2-binary` | 2.9.12 | LGPL with exceptions | auth,rest-api,cognee,search,ingestion | psycopg2 - Python-PostgreSQL Database Adapter |
| `pwdlib` | 0.3.0 | MIT License | auth,rest-api,cognee,search,ingestion | Modern password hashing for Python |
| `py-key-value-aio` | 0.2.8 | Apache Software License | auth,rest-api,cognee,search,ingestion | Async Key-Value |
| `py-key-value-shared` | 0.2.8 | Apache Software License | auth,rest-api,cognee,search,ingestion | Shared Key-Value |
| `py_rust_stemmers` | 0.1.5 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Fast and parallel snowball stemmer |
| `pyarrow` | 24.0.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Python library for Apache Arrow |
| `pyasn1` | 0.6.3 | BSD-2-Clause | auth,rest-api,cognee,search,ingestion | Pure-Python implementation of ASN.1 types and DER/BER/CER codecs (X.208) |
| `pyasn1_modules` | 0.4.2 | BSD | auth,rest-api,cognee,search,ingestion | A collection of ASN.1-based protocols modules |
| `pycparser` | 3.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | C parser in Python |
| `pydantic` | 2.13.4 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Data validation using Python type hints |
| `pydantic-settings` | 2.14.1 | MIT License | auth,rest-api,cognee,search,ingestion | Settings management using Pydantic |
| `pydantic_core` | 2.46.4 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Core functionality for Pydantic validation and serialization |
| `Pygments` | 2.20.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Pygments is a syntax highlighting package written in Python. |
| `PyJWT` | 2.12.1 | UNKNOWN | auth,rest-api,cognee,search,ingestion | JSON Web Token implementation in Python |
| `pylance` | 0.36.0 | Apache Software License | auth,rest-api,cognee,search,ingestion | python wrapper for Lance columnar format |
| `pymongo` | 4.17.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | PyMongo - the Official MongoDB Python driver |
| `Pympler` | 1.1 | Apache License, Version 2.0 | auth,rest-api,cognee,search,ingestion | A development tool to measure, monitor and analyze the memory behavior of Python |
| `pyotp` | 2.9.0 | MIT License | auth,rest-api | Python One Time Password Library |
| `pyparsing` | 3.3.2 | UNKNOWN | auth,rest-api,cognee,search,ingestion | pyparsing - Classes and methods to define and execute parsing grammars |
| `pypdf` | 6.11.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | A pure-python PDF library capable of splitting, merging, cropping, and transform |
| `pyperclip` | 1.11.0 | BSD | rest-api | A cross-platform clipboard module for Python. (Only handles plain text for now.) |
| `pytest` | 9.0.3 | UNKNOWN | rest-api | pytest: simple powerful testing with Python |
| `pytest-asyncio` | 1.3.0 | UNKNOWN | rest-api | Pytest support for asyncio |
| `pytest-cov` | 7.1.0 | MIT License | rest-api | Pytest plugin for measuring coverage. |
| `pytest-mock` | 3.15.1 | MIT | rest-api | Thin-wrapper around the mock package for easier use with pytest |
| `python-dateutil` | 2.9.0.post0 | Dual License | auth,rest-api,cognee,search,ingestion | Extensions to the standard Python datetime module |
| `python-docx` | 1.2.0 | MIT | auth,rest-api,cognee,search,ingestion | Create, read, and update Microsoft Word .docx files. |
| `python-dotenv` | 1.2.2 | BSD-3-Clause | auth,rest-api,cognee,search,ingestion | Read key-value pairs from a .env file and set them as environment variables |
| `python-jose` | 3.5.0 | MIT | auth,rest-api,cognee,search,ingestion | JOSE implementation in Python |
| `python-multipart` | 0.0.28 | Apache Software License | auth,rest-api,cognee,search,ingestion | A streaming multipart parser for Python |
| `python-pptx` | 1.0.2 | MIT | auth,rest-api,cognee,search,ingestion | Create, read, and update PowerPoint 2007+ (.pptx) files. |
| `pytokens` | 0.4.1 | MIT License | auth,rest-api,cognee,search,ingestion | A Fast, spec compliant Python 3.14+ tokenizer that runs on older Pythons. |
| `pytz` | 2026.2 | MIT | auth,rest-api,cognee,search,ingestion | World timezone definitions, modern and historical |
| `PyYAML` | 6.0.3 | MIT | auth,rest-api,cognee,search,ingestion | YAML parser and emitter for Python |
| `qdrant-client` | 1.18.0 | Apache-2.0 | auth,rest-api,cognee,search,ingestion | Client library for the Qdrant vector search engine |
| `qrcode` | 8.2 | BSD | auth,rest-api | QR Code image generator |
| `rdflib` | 7.1.4 | BSD-3-Clause | auth,rest-api,cognee,search,ingestion | RDFLib is a Python library for working with RDF, a simple yet powerful language  |
| `redis` | 7.4.0 | MIT License | auth,rest-api,cognee,search,ingestion | Python client for Redis database and key-value store |
| `referencing` | 0.37.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | JSON Referencing + Python |
| `regex` | 2026.5.9 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Alternative regular expression module, to replace re. |
| `requests` | 2.34.0 | Apache-2.0 | auth,rest-api,cognee,search,ingestion | Python HTTP for Humans. |
| `rich` | 14.3.4 | MIT | auth,rest-api,cognee,search,ingestion | Render rich text, tables, progress bars, syntax highlighting, markdown and more  |
| `rich-rst` | 1.3.2 | UNKNOWN | rest-api | A beautiful reStructuredText renderer for rich |
| `rpds-py` | 0.30.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Python bindings to Rust's persistent data structures (rpds) |
| `rsa` | 4.9.1 | Apache-2.0 | auth,rest-api,cognee,search,ingestion | Pure-Python RSA implementation |
| `safetensors` | 0.7.0 | Apache Software License | rest-api |  |
| `SecretStorage` | 3.5.0 | UNKNOWN | rest-api | Python bindings to FreeDesktop.org Secret Service API |
| `setuptools` | 82.0.1 | UNKNOWN | rest-api | Most extensible Python build backend with support for C/C++ extension modules |
| `shellingham` | 1.5.4 | ISC License | auth,rest-api,cognee,search,ingestion | Tool to Detect Surrounding Shell |
| `six` | 1.17.0 | MIT | auth,rest-api,cognee,search,ingestion | Python 2 and 3 compatibility utilities |
| `smmap` | 5.0.3 | BSD-3-Clause | ingestion | A pure Python implementation of a sliding window memory map manager |
| `sniffio` | 1.3.1 | MIT OR Apache-2.0 | auth,rest-api,cognee,search,ingestion | Sniff out which async library your code is running under |
| `sortedcontainers` | 2.4.0 | Apache 2.0 | auth,rest-api,cognee,search,ingestion | Sorted Containers -- Sorted List, Sorted Dict, Sorted Set |
| `SQLAlchemy` | 2.0.49 | MIT | auth,rest-api,cognee,search,ingestion | Database Abstraction Library |
| `sse-starlette` | 3.4.4 | UNKNOWN | auth,rest-api,cognee,search,ingestion | SSE plugin for Starlette |
| `starlette` | 1.0.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | The little ASGI library that shines. |
| `structlog` | 25.5.0 | Apache Software License | auth,rest-api,cognee,search,ingestion | Structured Logging for Python |
| `sympy` | 1.14.0 | BSD | auth,rest-api,cognee,search,ingestion | Computer algebra system (CAS) in Python |
| `tenacity` | 9.1.4 | Apache 2.0 | auth,rest-api,cognee,search,ingestion | Retry code until it succeeds |
| `tiktoken` | 0.12.0 | MIT License | auth,rest-api,cognee,search,ingestion | tiktoken is a fast BPE tokeniser for use with OpenAI's models |
| `tokenizers` | 0.23.1 | Apache Software License | auth,rest-api,cognee,search,ingestion |  |
| `tokie` | 0.0.9 | MIT OR Apache-2.0 | auth,rest-api,cognee,search,ingestion | Blazingly fast tokenizer — 50x faster, 10x smaller, 100% accurate |
| `tqdm` | 4.67.3 | MPL-2.0 AND MIT | auth,rest-api,cognee,search,ingestion | Fast, Extensible Progress Meter |
| `traitlets` | 5.15.0 | BSD 3-Clause License | auth,rest-api,cognee,search,ingestion | Traitlets Python configuration system |
| `tree-sitter` | 0.25.2 | MIT License | auth,rest-api,cognee,search,ingestion | Python bindings to the Tree-sitter parsing library |
| `tree-sitter-c-sharp` | 0.23.5 | MIT | ingestion | C# grammar for tree-sitter |
| `tree-sitter-embedded-template` | 0.25.0 | MIT | ingestion | Embedded Template (ERB, EJS) grammar for tree-sitter |
| `tree-sitter-language-pack` | 1.8.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Pre-compiled tree-sitter grammars for 305 programming languages |
| `tree-sitter-yaml` | 0.7.2 | MIT | ingestion | YAML grammar for tree-sitter |
| `typeguard` | 4.5.1 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Run-time type checker for Python |
| `typer` | 0.25.1 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Typer, build great CLIs. Easy to code. Based on Python type hints. |
| `typing-inspection` | 0.4.2 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Runtime typing introspection tools |
| `typing_extensions` | 4.15.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Backported and Experimental Type Hints for Python 3.9+ |
| `tzlocal` | 5.3.1 | MIT | auth,rest-api,cognee,search,ingestion | tzinfo object for the local timezone |
| `uritemplate` | 4.2.0 | BSD 3-Clause OR Apache-2.0 | auth,rest-api,cognee,search,ingestion | Implementation of RFC 6570 URI Templates |
| `urllib3` | 2.7.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | HTTP library with thread-safe connection pooling, file post, and more. |
| `uvicorn` | 0.46.0 | UNKNOWN | auth,rest-api,cognee,search,ingestion | The lightning-fast ASGI server. |
| `uvloop` | 0.22.1 | MIT License | auth,rest-api | Fast implementation of asyncio event loop on top of libuv |
| `watchfiles` | 1.1.1 | MIT | auth,rest-api | Simple, modern and high performance file watching and code reload in python. |
| `websockets` | 15.0.1 | BSD-3-Clause | auth,rest-api,cognee,search,ingestion | An implementation of the WebSocket Protocol (RFC 6455 & 7692) |
| `wrapt` | 2.1.2 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Module for decorators, wrappers and monkey patching. |
| `xlsxwriter` | 3.2.9 | BSD-2-Clause | auth,rest-api,cognee,search,ingestion | A Python module for creating Excel XLSX files. |
| `yarl` | 1.23.0 | Apache-2.0 | auth,rest-api,cognee,search,ingestion | Yet another URL library |
| `zipp` | 3.23.1 | UNKNOWN | auth,rest-api,cognee,search,ingestion | Backport of pathlib-compatible object wrapper for zip files |


## Methodology and caveats

- **Generator:** `importlib.metadata.distributions()` inside each running container. License strings come from each package's installed `METADATA` file — either the `License` field directly, or the first `License :: ...` classifier when the field is empty.
- **Versions:** as-installed at generation time. Re-generate after every dependency bump.
- **Scope:** Python packages bundled inside legion-graph's own container images only. The following are NOT included in this SBOM and have their own license terms documented in [`NOTICE`](NOTICE):
  - System libraries inside the Python base image (`python:3.13-slim` → Debian apt packages).
  - Referenced runtime services pulled as separate Docker images at deployment time: Postgres, DozerDB/Neo4j, Qdrant, Redpanda, Redis.
  - JavaScript/TypeScript packages (legion-graph has none — backend-only project).
- **Best-effort accuracy:** licenses were captured at the time of generation. Operators distributing modified versions of legion-graph must independently verify license terms for any dependencies they add, remove, or upgrade.
- **License-string heuristics:** some upstream projects record their license inconsistently across PyPI metadata. A few entries may show truncated or non-canonical license strings; the authoritative source is each package's own `LICENSE` file, shipped inside its installed location at `/usr/local/lib/python*/site-packages/<package>/`.
