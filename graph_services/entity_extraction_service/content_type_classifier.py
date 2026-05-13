"""Re-export shim — content_type_classifier moved to shared/.

The canonical implementation is now ``shared.content_type_classifier``.
This module re-exports the public API so any stale in-tree imports continue
to resolve without a traceback.

Do NOT add new logic here.  All changes go in ``shared/content_type_classifier.py``.
"""

from shared.content_type_classifier import (  # noqa: F401
    classify_content_type,
    _default_content_type_for_language,
    _is_ruby_spec,
)

__all__ = ["classify_content_type", "_default_content_type_for_language", "_is_ruby_spec"]
