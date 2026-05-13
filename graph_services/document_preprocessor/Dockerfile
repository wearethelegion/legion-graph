FROM python:3.13-slim

WORKDIR /app

# Copy Python requirements first for better caching
COPY document_preprocessor/requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

WORKDIR /app

# Copy application code
COPY document_preprocessor/ ./document_preprocessor/
COPY shared/ ./shared/

# Set Python path
ENV PYTHONPATH=/app

# Create non-root user
RUN useradd -m -u 1000 appuser \
    && chown -R appuser:appuser /app
USER appuser

# Run the application
CMD ["python", "-m", "document_preprocessor.main"]
