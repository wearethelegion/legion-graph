"""
KGRAG API Routes Module
Modular route definitions for the KGRAG REST API.

Phase 1 STRIP: Removed stripped-capability routes.
Phase 4 PRUNE: Removed dead documents_router (api/routes/documents.py deleted).
"""

from api.routes.instructions import router as instructions_router
from api.routes.ingestions import router as ingestions_router
from api.routes.agent_workflows import router as agent_workflows_router

__all__ = [
    "instructions_router",
    "ingestions_router",
    "agent_workflows_router",
]
