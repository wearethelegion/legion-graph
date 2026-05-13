"""
CodeSearch Prometheus metrics — T7, updated T9.A.

Exposes per-RPC latency histograms and request counters for the
CodeSearchService.  Follows the same pattern as cognee_code_service/metrics.py
but uses a small helper (_observe) instead of a MetricsInterceptor — this
makes it easy to retrofit into the existing servicer without refactoring its
class structure.

Metrics exposed:
    kgrag_code_search_rpc_latency_seconds{rpc, status}
        — latency histogram per RPC, buckets tuned for normal (~1s) RPCs.

    kgrag_code_search_rpc_requests_total{rpc, status}
        — monotonic counter per RPC (status: ok | error).

Usage in servicer (see code_search_servicer.py):
    from grpc_server.servicers.code_search_metrics import observe_rpc

    async def MyRpc(self, request, context):
        t0 = time.monotonic()
        status = "ok"
        try:
            ...
            return result
        except Exception:
            status = "error"
            raise
        finally:
            observe_rpc("MyRpc", status, time.monotonic() - t0)

Metrics HTTP endpoint:
    The main gRPC server (server.py) should call start_metrics_server() once at
    startup.  By default the endpoint is at http://localhost:9091/metrics to
    avoid colliding with the existing cognee_code_service endpoint on 9090.

    Verify:
        curl localhost:9091/metrics | grep kgrag_code_search
"""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Generator

from loguru import logger

# ── Lazy import to avoid hard dep at import time ──────────────────────────────

try:
    from prometheus_client import Counter, Histogram, start_http_server

    _PROM_AVAILABLE = True
except ImportError:
    _PROM_AVAILABLE = False
    logger.warning(
        "prometheus_client not installed — CodeSearch metrics disabled. "
        "Install with: pip install prometheus-client"
    )


# ── RPC names (canonical) — 7 unary RPCs + Health ────────────────────────────

RPC_NAMES = [
    "GetDomains",
    "SearchEntities",
    "SearchSummaries",
    "GetCodeForEntity",
    "TraverseGraph",
    "GetEntityGraph",
    "FullSearch",
    "Health",
]

# ── Prometheus metric definitions ─────────────────────────────────────────────

# Buckets tuned for deterministic primitive RPCs (no LLM calls — max ~5s)
_LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)

if _PROM_AVAILABLE:
    _LATENCY = Histogram(
        "kgrag_code_search_rpc_latency_seconds",
        "End-to-end latency of CodeSearch gRPC RPCs",
        labelnames=["rpc", "status"],
        buckets=_LATENCY_BUCKETS,
    )

    _REQUESTS = Counter(
        "kgrag_code_search_rpc_requests_total",
        "Total CodeSearch gRPC requests",
        labelnames=["rpc", "status"],
    )
else:
    _LATENCY = None
    _REQUESTS = None


# ── Public observation helpers ────────────────────────────────────────────────


def observe_rpc(rpc_name: str, status: str, elapsed_seconds: float) -> None:
    """
    Record latency + request count for one completed RPC.

    Args:
        rpc_name: One of the RPC_NAMES above.
        status: "ok" or "error".
        elapsed_seconds: Wall-clock time from start to finish.
    """
    if not _PROM_AVAILABLE:
        return
    try:
        _LATENCY.labels(rpc=rpc_name, status=status).observe(elapsed_seconds)
        _REQUESTS.labels(rpc=rpc_name, status=status).inc()
    except Exception as exc:
        logger.debug("metrics.observe_rpc failed (non-fatal): %s", exc)


@contextmanager
def rpc_timer(rpc_name: str) -> Generator[dict, None, None]:
    """
    Context manager that times an RPC body and calls observe_rpc on exit.

    Yields a mutable dict {"status": "ok"} — the body can set status="error"
    before raising to mark the metric correctly.

    Usage:
        with rpc_timer("GetDomains") as ctx:
            try:
                ...
            except Exception:
                ctx["status"] = "error"
                raise
    """
    ctx = {"status": "ok"}
    t0 = time.monotonic()
    try:
        yield ctx
    finally:
        observe_rpc(rpc_name, ctx["status"], time.monotonic() - t0)


# ── Metrics HTTP server ───────────────────────────────────────────────────────

_METRICS_PORT = int(os.environ.get("CODE_SEARCH_METRICS_PORT", "9091"))
_metrics_started = False


def start_metrics_server(port: int | None = None) -> None:
    """
    Start the Prometheus HTTP metrics server on a background thread.

    Safe to call multiple times — idempotent.

    Default port: 9091 (to avoid colliding with cognee_code_service on 9090).
    Override with CODE_SEARCH_METRICS_PORT env var.
    """
    global _metrics_started
    if not _PROM_AVAILABLE:
        logger.warning("prometheus_client not available — metrics server not started")
        return
    if _metrics_started:
        return

    p = port or _METRICS_PORT
    try:
        start_http_server(p)
        _metrics_started = True
        logger.info("CodeSearch metrics server started on :%d", p)
    except OSError as exc:
        logger.warning("CodeSearch metrics server failed to start on :%d — %s", p, exc)
