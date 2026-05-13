"""Webhook signature validation and payload processing."""

import hmac
import hashlib
from typing import Optional, Dict, Any
from pydantic import BaseModel
from loguru import logger


class WebhookConfig(BaseModel):
    """Configuration for a repository webhook."""
    repository: str  # "org/repo"
    project_id: str
    company_id: str
    framework: str
    target_branch: str = "develop"


class GitHubWebhookService:
    """Service for GitHub webhook validation and processing."""

    def __init__(self, secret: str):
        self.default_secret = secret

    def verify_signature(
        self,
        payload_body: bytes,
        signature_header: str,
        secret: Optional[str] = None
    ) -> bool:
        """
        Verify GitHub webhook signature using HMAC-SHA256.

        Args:
            payload_body: Raw request body (bytes)
            signature_header: Value of X-Hub-Signature-256 header
            secret: Project-specific secret (falls back to default)

        Returns:
            True if signature is valid
        """
        if not signature_header or not signature_header.startswith("sha256="):
            logger.warning("Invalid signature header format")
            return False

        # Use provided secret or default
        secret_to_use = secret or self.default_secret
        if not secret_to_use:
            logger.error("No webhook secret available for validation")
            return False

        # Extract hex digest from header
        expected_signature = signature_header[7:]  # Remove "sha256=" prefix

        # Compute HMAC-SHA256
        computed_hmac = hmac.new(
            key=secret_to_use.encode('utf-8'),
            msg=payload_body,
            digestmod=hashlib.sha256
        )
        computed_signature = computed_hmac.hexdigest()

        # Constant-time comparison (prevents timing attacks)
        return hmac.compare_digest(expected_signature, computed_signature)

    def extract_push_metadata(
        self,
        push_event: dict
    ) -> Optional[Dict[str, Any]]:
        """
        Extract metadata from GitHub push event.

        Returns None if event should be ignored.
        """
        # Extract branch from ref (refs/heads/develop → develop)
        ref = push_event.get("ref", "")
        if not ref.startswith("refs/heads/"):
            logger.debug(f"Ignoring non-branch ref: {ref}")
            return None  # Ignore tag pushes

        branch = ref.replace("refs/heads/", "")

        # Extract repository details
        repo = push_event.get("repository", {})
        repository_full_name = repo.get("full_name")  # e.g., "org/repo"

        if not repository_full_name:
            logger.warning("Missing repository full_name in push event")
            return None  # Invalid payload

        return {
            "repository": repository_full_name,
            "branch": branch,
            "pusher_name": push_event.get("pusher", {}).get("name"),
            "pusher_email": push_event.get("pusher", {}).get("email"),
            "sender_login": push_event.get("sender", {}).get("login"),
            "deleted": push_event.get("deleted", False),
            "forced": push_event.get("forced", False),
            "head_commit_sha": push_event.get("after"),
        }

    def should_process(
        self,
        push_event: dict,
        target_branch: str = "develop"
    ) -> bool:
        """
        Determine if push event should trigger ingestion.

        Args:
            push_event: GitHub push event payload
            target_branch: Branch to monitor (default: "develop")

        Returns:
            True if event should trigger ingestion
        """
        # Extract branch from ref
        ref = push_event.get("ref", "")
        if not ref.startswith("refs/heads/"):
            return False  # Ignore tag pushes

        branch = ref.replace("refs/heads/", "")

        # Check if target branch
        if branch != target_branch:
            logger.debug(f"Ignoring push to non-target branch: {branch}")
            return False

        # Ignore deleted branches
        if push_event.get("deleted", False):
            logger.debug(f"Ignoring deleted branch: {branch}")
            return False

        return True


def get_github_webhook_service() -> GitHubWebhookService:
    """Dependency injection for webhook service."""
    from ..core.config import get_settings
    settings = get_settings()
    return GitHubWebhookService(secret=settings.github_webhook_secret)
