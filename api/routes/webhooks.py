"""GitHub webhook endpoints for automated code ingestion."""

from fastapi import APIRouter, Request, HTTPException, Depends, status
from fastapi.responses import JSONResponse
from loguru import logger
from datetime import datetime, timezone
from typing import Dict, Any
import asyncpg

from ..services.webhook_service import GitHubWebhookService, get_github_webhook_service
from ..services.kafka_service import KafkaProducerService, get_kafka_service, KafkaPublishError
from ..repositories.company_repository import CompanyRepository
from ..repositories.project_repository import ProjectRepository
from ..database import get_db_pool

router = APIRouter(prefix="/api/v1/webhooks", tags=["webhooks"])


@router.post("/github/{company_name}/{project_name}")
async def github_webhook(
    company_name: str,
    project_name: str,
    request: Request,
    webhook_service: GitHubWebhookService = Depends(get_github_webhook_service),
    kafka_service: KafkaProducerService = Depends(get_kafka_service),
    pool: asyncpg.Pool = Depends(get_db_pool),
):
    """
    Receive GitHub push event webhooks for a specific project.

    Path parameters identify which project's webhook secret to use.
    No authentication required - validates HMAC-SHA256 signature instead.
    Only processes pushes to 'develop' branch.
    """
    # Extract headers
    event_type = request.headers.get("X-GitHub-Event")
    delivery_id = request.headers.get("X-GitHub-Delivery")
    signature_header = request.headers.get("X-Hub-Signature-256")

    # Validate event type
    if event_type != "push":
        logger.info(
            f"Ignoring non-push event: {event_type} "
            f"(company={company_name}, project={project_name}, delivery={delivery_id})"
        )
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": f"Unsupported event type: {event_type}"}
        )

    # Lookup company
    company_repo = CompanyRepository(pool)
    company = await company_repo.get_by_name(company_name)
    if not company:
        logger.warning("Company not found: {} (delivery={})", company_name, delivery_id)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Company not found: {company_name}"
        )

    # Lookup project
    project_repo = ProjectRepository(pool)
    project = await project_repo.get_by_name_and_company(
        name=project_name,
        company_id=company["id"]
    )
    if not project:
        logger.warning(
            f"Project not found: {project_name} under {company_name} "
            f"(delivery={delivery_id})"
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project not found: {project_name}"
        )

    # Validate webhook configuration
    github_webhook_secret = project.get("github_webhook_secret")
    if not github_webhook_secret:
        logger.error(
            f"Project webhook not configured: {company_name}/{project_name} "
            f"(project_id={project['id']}, delivery={delivery_id})"
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Project webhook not configured (missing secret)"
        )

    # Read raw body for signature validation
    body = await request.body()

    # Verify HMAC-SHA256 signature using project secret
    if not webhook_service.verify_signature(body, signature_header, github_webhook_secret):
        logger.warning(
            f"Invalid webhook signature for {company_name}/{project_name} "
            f"(delivery={delivery_id})"
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid webhook signature"
        )

    # Parse JSON payload
    try:
        payload = await request.json()
    except Exception as exc:
        logger.error(
            f"Failed to parse webhook payload: {exc} "
            f"(company={company_name}, project={project_name}, delivery={delivery_id})"
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid JSON payload"
        ) from exc

    # Extract push metadata
    metadata = webhook_service.extract_push_metadata(payload)
    if not metadata:
        logger.warning(
            f"Invalid push event metadata "
            f"(company={company_name}, project={project_name}, delivery={delivery_id})"
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid push event payload"
        )

    repository = metadata["repository"]
    branch = metadata["branch"]

    # Skip deleted branches only
    if metadata.get("deleted", False):
        logger.info(
            f"Ignoring deleted branch: {repository}:{branch} "
            f"(company={company_name}, project={project_name}, delivery={delivery_id})"
        )
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={
                "status": "ignored",
                "reason": "Branch deleted",
                "repository": repository,
                "branch": branch
            }
        )

    # Publish to Kafka
    try:
        await kafka_service.publish_repository(
            repository=repository,
            branch=branch,
            framework="",  # Auto-detect or configure later
            project_id=project["id"],
            company_id=company["id"],
            user_id="webhook-github",
            force_full_refresh=False,
        )

        logger.info(
            f"Webhook queued for ingestion: {repository}:{branch} "
            f"(company={company_name}, project={project_name}, "
            f"delivery={delivery_id}, sender={metadata['sender_login']})"
        )

        return {
            "status": "queued",
            "repository": repository,
            "branch": branch,
            "company_name": company_name,
            "project_name": project_name,
            "delivery_id": delivery_id,
            "queued_at": datetime.now(timezone.utc).isoformat()
        }

    except KafkaPublishError as exc:
        logger.error(
            f"Failed to publish webhook to Kafka: {exc} "
            f"(company={company_name}, project={project_name}, delivery={delivery_id})"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to queue ingestion"
        ) from exc
