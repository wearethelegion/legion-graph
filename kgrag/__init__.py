"""
Kgrag vendor package — local copy of selected modules from kgrag-backend/kgrag/.

Vendored modules: config, secrets, embeddings, fusion, database.

This package contains only what is needed by the kept services in backend-services.
Phase 2 will rename this to kgrag/.
"""

from kgrag.config import config

__version__ = "0.1.0"

__all__ = [
    "config",
]
