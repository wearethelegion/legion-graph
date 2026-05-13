"""
Ingestion Pydantic models for REST API.
Request and response models for ingestion status operations.
"""

from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List


class FailedFile(BaseModel):
    """Information about a failed file during ingestion."""

    file_path: str = Field(..., description="Path to the failed file")
    error: str = Field(..., description="Error message")
    timestamp: str = Field("", description="When the failure occurred")


class IngestionStatusResponse(BaseModel):
    """Response model for ingestion status."""

    ingestion_id: str = Field(..., description="Ingestion UUID")
    status: str = Field(..., description="Ingestion status (pending, running, completed, failed)")
    repository: str = Field(..., description="Repository name/path")
    branch: str = Field(..., description="Branch being ingested")
    total_files: int = Field(0, description="Total files to process")
    files_processed: int = Field(0, description="Files successfully processed")
    files_failed: int = Field(0, description="Files that failed processing")
    files_skipped: int = Field(0, description="Files skipped")
    current_file: Optional[str] = Field(None, description="File currently being processed")
    failed_files: List[FailedFile] = Field(
        default_factory=list, description="Details of failed files (max 50)"
    )
    started_at: Optional[datetime] = Field(None, description="When ingestion started")
    completed_at: Optional[datetime] = Field(None, description="When ingestion completed")
    percentage: float = Field(0.0, description="Completion percentage")
    # LLM processing metrics (Observability v2)
    files_llm_fallback: int = Field(0, description="Files that used text chunker fallback")
    files_filtered_size: int = Field(0, description="Files filtered due to size limits")
    files_filtered_extension: int = Field(0, description="Files filtered due to extension")
    files_filtered_directory: int = Field(0, description="Files filtered due to directory rules")
    # LLM analysis metrics (user-facing summary)
    llm_successful: int = Field(0, description="Files successfully analyzed by LLM")
    llm_errors: int = Field(0, description="Files that failed LLM analysis")
    llm_fallback: int = Field(
        0, description="Files that used text chunker fallback (alias for files_llm_fallback)"
    )


class IngestionSummary(BaseModel):
    """Summary model for ingestion list items."""

    ingestion_id: str = Field(..., description="Ingestion UUID")
    repository: str = Field(..., description="Repository name/path")
    branch: str = Field(..., description="Branch being ingested")
    status: str = Field(..., description="Ingestion status")
    total_files: int = Field(0, description="Total files to process")
    files_processed: int = Field(0, description="Files successfully processed")
    started_at: Optional[datetime] = Field(None, description="When ingestion started")
    percentage: float = Field(0.0, description="Completion percentage")


class ProjectIngestionSummary(BaseModel):
    ingestion_id: str
    repository: str
    branch: str
    status: str
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    total_chunks: int


class IngestionStageProgress(BaseModel):
    name: str
    total: int
    processed: int
    percentage: float


class IngestionProgressResponse(BaseModel):
    ingestion_id: str
    status: str
    total_chunks: int
    stages: List[IngestionStageProgress]
    updated_at: datetime


class IngestionListResponse(BaseModel):
    """Response model for listing ingestions."""

    total_count: int = Field(..., description="Total number of ingestions matching filters")
    ingestions: List[ProjectIngestionSummary] = Field(
        ..., description="List of ingestion summaries"
    )
