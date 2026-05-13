"""Pydantic models for document ingestion API endpoints."""

from typing import List, Optional
from pydantic import BaseModel, Field


class DocumentUploadResponse(BaseModel):
    """Response model for document upload endpoint."""

    document_id: str = Field(..., description="Unique identifier for the uploaded document")
    filename: str = Field(..., description="Original filename of the uploaded document")
    file_size: int = Field(..., description="Size of the file in bytes", gt=0)
    file_type: str = Field(..., description="MIME type or file extension")
    upload_status: str = Field(..., description="Upload status (e.g., 'success', 'pending', 'failed')")
    processing_job_id: str = Field(..., description="ID of the background processing job")


class DocumentUploadMultiResponse(BaseModel):
    """Response wrapper for multiple document uploads."""

    documents: List[DocumentUploadResponse] = Field(
        ..., description="List of upload responses for each document"
    )


class DocumentStatusResponse(BaseModel):
    """Response model for document processing status check."""

    document_id: str = Field(..., description="Unique identifier for the document")
    filename: str = Field(..., description="Original filename of the document")
    upload_status: str = Field(..., description="Upload status")
    processing_status: str = Field(
        ..., description="Current processing status (e.g., 'processing', 'completed', 'failed')"
    )
    progress: float = Field(..., description="Processing progress percentage (0-100)", ge=0, le=100)
    stages: dict = Field(
        ...,
        description="Status of each processing stage",
        json_schema_extra={
            "example": {
                "text_extraction": "completed",
                "chunking": "completed",
                "entity_extraction": "processing",
                "embedding": "pending",
                "storage": "pending",
            }
        },
    )
    stats: dict = Field(
        ...,
        description="Processing statistics",
        json_schema_extra={
            "example": {
                "chunks_created": 42,
                "entities_extracted": 15,
                "processing_time_seconds": 12.5,
            }
        },
    )


class DocumentListItem(BaseModel):
    """Single document item in paginated list."""

    document_id: str = Field(..., description="Unique identifier for the document")
    filename: str = Field(..., description="Original filename")
    file_type: str = Field(..., description="MIME type or file extension")
    chunk_count: int = Field(..., description="Number of chunks created from this document", ge=0)
    entity_count: int = Field(..., description="Number of entities extracted from this document", ge=0)
    uploaded_at: str = Field(..., description="ISO 8601 timestamp of upload")
    uploaded_by: str = Field(..., description="User ID or identifier of uploader")


class DocumentListResponse(BaseModel):
    """Paginated response for document list endpoint."""

    documents: List[DocumentListItem] = Field(..., description="List of documents in current page")
    total: int = Field(..., description="Total number of documents matching query", ge=0)
    limit: int = Field(..., description="Maximum number of items per page", gt=0)
    offset: int = Field(..., description="Number of items skipped", ge=0)


class DocumentSummary(BaseModel):
    """Document summary structure with multiple granularities."""

    one_sentence: str = Field(..., description="Single sentence summary of the document")
    paragraph: str = Field(..., description="Paragraph-length summary of the document")
    key_points: List[str] = Field(..., description="List of key points extracted from the document")
    topics: List[str] = Field(..., description="Main topics covered in the document")


class DocumentStatsResponse(BaseModel):
    """Statistical information about a document."""

    chunk_count: int = Field(..., description="Number of chunks created", ge=0)
    entity_count: int = Field(..., description="Number of entities extracted", ge=0)
    word_count: int = Field(..., description="Total word count in document", ge=0)
    page_count: int = Field(..., description="Number of pages in document", ge=0)


class DocumentDetailResponse(BaseModel):
    """Complete details for a single document."""

    document_id: str = Field(..., description="Unique identifier for the document")
    filename: str = Field(..., description="Original filename")
    file_type: str = Field(..., description="MIME type or file extension")
    file_size: int = Field(..., description="Size of the file in bytes", gt=0)
    company_id: str = Field(..., description="Company/tenant identifier")
    project_id: str = Field(..., description="Project identifier")
    summary: DocumentSummary = Field(..., description="Multi-level summary of the document")
    stats: DocumentStatsResponse = Field(..., description="Statistical information")
    metadata: dict = Field(default_factory=dict, description="Additional metadata key-value pairs")
    uploaded_at: str = Field(..., description="ISO 8601 timestamp of upload")
    processed_at: Optional[str] = Field(None, description="ISO 8601 timestamp of processing completion")


class SearchRequest(BaseModel):
    """Request model for semantic search endpoint."""

    query: str = Field(..., description="Search query text", min_length=1)
    company_id: str = Field(..., description="Company/tenant identifier for scoping")
    project_id: Optional[str] = Field(None, description="Optional project identifier for narrower scoping")
    limit: int = Field(10, description="Maximum number of results to return", gt=0, le=100)
    filters: Optional[dict] = Field(None, description="Additional filters for search results")


class SearchResultItem(BaseModel):
    """Single search result item."""

    chunk_id: str = Field(..., description="Unique identifier for the chunk")
    document_id: str = Field(..., description="Document this chunk belongs to")
    filename: str = Field(..., description="Original filename of the document")
    text: str = Field(..., description="Text content of the chunk")
    topic: str = Field(..., description="Topic or category of the chunk")
    section: str = Field(..., description="Section or heading of the chunk")
    score: float = Field(..., description="Relevance score (0-1)", ge=0, le=1)
    page: Optional[int] = Field(None, description="Page number if applicable", ge=1)
    chunk_index: int = Field(..., description="Index of chunk within document", ge=0)
    keywords: List[str] = Field(..., description="Extracted keywords from the chunk")


class SearchResponse(BaseModel):
    """Response model for search endpoint."""

    results: List[SearchResultItem] = Field(..., description="List of search results ordered by relevance")
    total_results: int = Field(..., description="Total number of results found", ge=0)
    search_time_ms: int = Field(..., description="Search execution time in milliseconds", ge=0)
