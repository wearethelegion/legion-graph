"""Gemini API primary, Vertex AI regional fallback for LLM calls.

Primary: gemini/gemini-3.1-flash-lite-preview (Gemini API, fast, uses GEMINI_API_KEY)
Fallback: vertex_ai/gemini-2.5-flash-lite (Vertex AI, regional round-robin)
         — kept on 2.5 because preview models are not yet available on Vertex AI.

On rate limit from Gemini API, switches to Vertex AI for GEMINI_COOLDOWN_SECONDS.
After cooldown, tries Gemini again. No requests are dropped.

Environment:
    GEMINI_API_KEY: Gemini API key (required for primary route)
    GEMINI_COOLDOWN_SECONDS: Cooldown duration after rate limit (default: 60)
    VERTEXAI_LLM_REGIONS: Comma-separated Vertex AI regions (default: europe-west1)

Usage:
    from cognee_service.gemini_fallback_router import get_fallback_router

    router = get_fallback_router()
    response = await router.acompletion(
        messages=[{"role": "user", "content": "..."}],
        temperature=0.1,
    )
"""

import itertools
import logging
import os
import threading
import time
from typing import Any

import litellm

logger = logging.getLogger(__name__)


class GeminiFallbackRouter:
    """Thread-safe LLM router with Gemini API primary and Vertex AI fallback.

    Automatically switches to Vertex AI regional round-robin on rate limits,
    then recovers to Gemini API after cooldown.
    """

    def __init__(self):
        """Initialize router with environment configuration."""
        # Gemini API configuration (primary)
        self._gemini_api_key = os.environ.get("GEMINI_API_KEY")
        self._gemini_model = "gemini/gemini-3.1-flash-lite-preview"

        # Vertex AI configuration (fallback)
        # Note: kept on 2.5 because preview models are not yet on Vertex AI.
        self._vertex_model = "vertex_ai/gemini-2.5-flash-lite"
        vertex_regions_str = os.environ.get("VERTEXAI_LLM_REGIONS", "europe-west1")
        self._vertex_regions = [r.strip() for r in vertex_regions_str.split(",") if r.strip()]
        self._region_cycle = itertools.cycle(self._vertex_regions)

        # Cooldown configuration
        self._cooldown_seconds = int(os.environ.get("GEMINI_COOLDOWN_SECONDS", "60"))
        self._gemini_cooldown_until: float = 0.0  # Timestamp when cooldown ends

        # Thread safety
        self._lock = threading.Lock()

        logger.info(
            "GeminiFallbackRouter initialized: gemini_model=%s, vertex_model=%s, "
            "vertex_regions=%s, cooldown_seconds=%d",
            self._gemini_model,
            self._vertex_model,
            self._vertex_regions,
            self._cooldown_seconds,
        )

        if not self._gemini_api_key:
            logger.warning("GEMINI_API_KEY not set — router will always use Vertex AI fallback")

    async def acompletion(self, **kwargs) -> Any:
        """Drop-in replacement for litellm.acompletion with automatic failover.

        Args:
            **kwargs: Arguments passed to litellm.acompletion

        Returns:
            litellm completion response

        Raises:
            Any exception except RateLimitError (which triggers failover)
        """
        # Try Gemini API if not in cooldown and API key is available
        if self._gemini_api_key and not self._in_cooldown():
            try:
                return await self._call_gemini(**kwargs)
            except litellm.RateLimitError as exc:
                # Rate limited — enter cooldown and fall through to Vertex
                self._enter_cooldown()
                logger.warning(
                    "gemini_fallback.rate_limited error=%s cooldown=%ds",
                    str(exc)[:200],
                    self._cooldown_seconds,
                )
                # Continue to Vertex AI fallback below
            except Exception as exc:
                # Non-rate-limit error — log it and fall through to Vertex
                logger.error(
                    "gemini_fallback.gemini_error type=%s error=%s",
                    type(exc).__name__,
                    str(exc)[:300],
                )
                # Fall through to Vertex — don't enter cooldown for non-rate-limit errors

        # Vertex AI fallback (or if no Gemini API key / in cooldown)
        return await self._call_vertex(**kwargs)

    async def _call_gemini(self, **kwargs) -> Any:
        """Make LLM call via Gemini API."""
        # Override model and inject API key
        kwargs["model"] = self._gemini_model
        kwargs["api_key"] = self._gemini_api_key

        # Remove Vertex-specific kwargs if present
        kwargs.pop("vertex_location", None)

        logger.debug(
            "gemini_fallback.using_gemini",
            extra={"model": self._gemini_model},
        )

        return await litellm.acompletion(**kwargs)

    async def _call_vertex(self, **kwargs) -> Any:
        """Make LLM call via Vertex AI with regional round-robin."""
        region = self._next_vertex_region()

        # Override model and inject region
        kwargs["model"] = self._vertex_model
        kwargs["vertex_location"] = region

        # Remove Gemini-specific kwargs if present
        kwargs.pop("api_key", None)  # Vertex uses service account, not API key

        logger.info(
            "gemini_fallback.using_vertex",
            extra={
                "region": region,
                "model": self._vertex_model,
                "in_cooldown": self._in_cooldown(),
            },
        )

        return await litellm.acompletion(**kwargs)

    def _in_cooldown(self) -> bool:
        """Check if currently in cooldown period."""
        return time.time() < self._gemini_cooldown_until

    def _enter_cooldown(self):
        """Enter cooldown state (thread-safe)."""
        with self._lock:
            self._gemini_cooldown_until = time.time() + self._cooldown_seconds

        logger.info(
            "gemini_fallback.cooldown_entered",
            extra={
                "cooldown_seconds": self._cooldown_seconds,
                "cooldown_until_timestamp": self._gemini_cooldown_until,
            },
        )

    def _next_vertex_region(self) -> str:
        """Get next Vertex AI region in round-robin order (thread-safe)."""
        with self._lock:
            return next(self._region_cycle)


# ── Singleton Instance ─────────────────────────────────────────────────────────

_ROUTER: GeminiFallbackRouter | None = None
_ROUTER_LOCK = threading.Lock()


def get_fallback_router() -> GeminiFallbackRouter:
    """Get or create the global fallback router instance (thread-safe).

    Returns:
        Singleton GeminiFallbackRouter instance
    """
    global _ROUTER
    if _ROUTER is None:
        with _ROUTER_LOCK:
            # Double-check after acquiring lock
            if _ROUTER is None:
                _ROUTER = GeminiFallbackRouter()
    return _ROUTER
