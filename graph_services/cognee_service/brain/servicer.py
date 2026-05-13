"""
Brain v2 — BrainServicer

gRPC servicer for all 31 RPCs.  Each domain delegates to its handler module.

Domain handlers:
  - knowledge_handler.py   → Knowledge CRUD  (wired)
  - expertise_handler.py   → Expertise CRUD + chunk + link  (wired)
  - lessons_handler.py     → Lessons CRUD  (wired)
  - engagement_handler.py  → Engagement CRUD  (wired)
  - entry_handler.py       → Entry CRUD + search  (wired)
  - unified_search_handler.py → UnifiedSearch  (wired)
"""

import grpc
import structlog

from cognee_service.generated import brain_pb2
from cognee_service.generated import brain_pb2_grpc
from cognee_service.brain import knowledge_handler
from cognee_service.brain import expertise_handler
from cognee_service.brain import lessons_handler
from cognee_service.brain import engagement_handler
from cognee_service.brain import entry_handler
from cognee_service.brain import unified_search_handler

logger = structlog.get_logger(__name__)


class BrainServicer(brain_pb2_grpc.BrainServiceServicer):
    """BrainService gRPC servicer — delegates to domain handler modules."""

    # ── Knowledge (delegated to knowledge_handler) ─────────────────────────

    async def CreateKnowledge(self, request, context):
        return await knowledge_handler.create_knowledge(request, context)

    async def GetKnowledge(self, request, context):
        return await knowledge_handler.get_knowledge(request, context)

    async def ListKnowledge(self, request, context):
        return await knowledge_handler.list_knowledge(request, context)

    async def UpdateKnowledge(self, request, context):
        return await knowledge_handler.update_knowledge(request, context)

    async def DeleteKnowledge(self, request, context):
        return await knowledge_handler.delete_knowledge(request, context)

    # ── Expertise (delegated to expertise_handler) ─────────────────────────

    async def CreateExpertise(self, request, context):
        return await expertise_handler.create_expertise(request, context)

    async def GetExpertise(self, request, context):
        return await expertise_handler.get_expertise(request, context)

    async def ListExpertise(self, request, context):
        return await expertise_handler.list_expertise(request, context)

    async def UpdateExpertise(self, request, context):
        return await expertise_handler.update_expertise(request, context)

    async def DeleteExpertise(self, request, context):
        return await expertise_handler.delete_expertise(request, context)

    async def AddExpertiseChunk(self, request, context):
        return await expertise_handler.add_expertise_chunk(request, context)

    async def LinkExpertiseToAgent(self, request, context):
        return await expertise_handler.link_expertise_to_agent(request, context)

    async def UnlinkExpertiseFromAgent(self, request, context):
        return await expertise_handler.unlink_expertise_from_agent(request, context)

    # ── Lessons (delegated to lessons_handler) ────────────────────────────────

    async def RecordLesson(self, request, context):
        return await lessons_handler.record_lesson(request, context)

    async def GetLesson(self, request, context):
        return await lessons_handler.get_lesson(request, context)

    async def ListLessons(self, request, context):
        return await lessons_handler.list_lessons(request, context)

    async def UpdateLesson(self, request, context):
        return await lessons_handler.update_lesson(request, context)

    async def DeleteLesson(self, request, context):
        return await lessons_handler.delete_lesson(request, context)

    # ── Engagements (delegated to engagement_handler) ─────────────────────────

    async def CreateEngagement(self, request, context):
        return await engagement_handler.create_engagement(request, context)

    async def GetEngagement(self, request, context):
        return await engagement_handler.get_engagement(request, context)

    async def ListEngagements(self, request, context):
        return await engagement_handler.list_engagements(request, context)

    async def UpdateEngagement(self, request, context):
        return await engagement_handler.update_engagement(request, context)

    async def DeleteEngagement(self, request, context):
        return await engagement_handler.delete_engagement(request, context)

    # ── Entries (delegated to entry_handler) ───────────────────────────────────

    async def AddEntry(self, request, context):
        return await entry_handler.add_entry(request, context)

    async def GetEntry(self, request, context):
        return await entry_handler.get_entry(request, context)

    async def ListEntries(self, request, context):
        return await entry_handler.list_entries(request, context)

    async def UpdateEntry(self, request, context):
        return await entry_handler.update_entry(request, context)

    async def DeleteEntry(self, request, context):
        return await entry_handler.delete_entry(request, context)

    async def SearchEntries(self, request, context):
        return await entry_handler.search_entries(request, context)

    async def ResumeEngagement(self, request, context):
        """Format engagement context for session resumption."""
        await context.abort(grpc.StatusCode.UNIMPLEMENTED, "ResumeEngagement not yet implemented")

    # ── Search ───────────────────────────────────────────────────────────────

    async def UnifiedSearch(self, request, context):
        """Unified search across all knowledge entity types via Cognee graph."""
        return await unified_search_handler.handle_unified_search(request, context)
