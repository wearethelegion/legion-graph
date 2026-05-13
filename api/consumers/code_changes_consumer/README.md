# Code Changes Consumer 🚀

Kafka consumer that processes code change events from the `data_enrichment` topic, decodes base64 content, and analyzes code using CodeServiceV2.

## Architecture

```
Kafka (data_enrichment topic)
  ↓
CodeChangesConsumer
  ↓
MessageHandler
  ├─ Decode base64 content
  ├─ Detect language
  ├─ Create CodeFile
  └─ CodeServiceV2.analyze_files()
      ├─ GeminiClient (with caching)
      ├─ ParallelExecutor
      └─ StorageOrchestrator
          ├─ Neo4j (graph storage)
          └─ Qdrant (vector storage)
```

## Features

- ✅ **Auto base64 decoding**: Handles `content_encoding: "base64"`
- ✅ **Language detection**: From `file_extension` or `language` field
- ✅ **Smart filtering**: Skips deleted files, unsupported languages
- ✅ **Retry logic**: 3 retries with exponential backoff
- ✅ **Graceful shutdown**: Handles SIGINT/SIGTERM
- ✅ **Multi-tenant**: Extracts project/company IDs from event
- ✅ **Comprehensive logging**: Structured logs with loguru

## Quick Start

### Environment Setup

```bash
# Required
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export GEMINI_API_KEY="your-gemini-api-key"

# Optional (with defaults)
export KAFKA_DATA_ENRICHMENT_TOPIC="data_enrichment"
export KAFKA_CONSUMER_GROUP_ID="code-changes-consumer"
export KAFKA_AUTO_COMMIT="true"
export KAFKA_AUTO_OFFSET_RESET="earliest"  # or "latest"
export SKIP_DELETED_FILES="true"
export LOG_LEVEL="INFO"

# Multi-tenant defaults (override with actual lookup)
export DEFAULT_PROJECT_ID="00000000-0000-0000-0000-000000000000"
export DEFAULT_COMPANY_ID="00000000-0000-0000-0000-000000000000"
export DEFAULT_USER_ID="system"
```

### Run Consumer

```bash
# Run directly
python -m api.consumers.code_changes_consumer.runner

# Or with Docker
docker-compose up code-changes-consumer
```

### Programmatic Usage

```python
from api.consumers.code_changes_consumer import ConsumerRunner

runner = ConsumerRunner()
await runner.run()
```

## Configuration

All settings in `config.py`:

```python
from api.consumers.code_changes_consumer import CodeChangesConsumerConfig

# Kafka settings
print(f"Topic: {CodeChangesConsumerConfig.KAFKA_TOPIC}")
print(f"Group: {CodeChangesConsumerConfig.KAFKA_CONSUMER_GROUP_ID}")
print(f"Bootstrap: {CodeChangesConsumerConfig.KAFKA_BOOTSTRAP_SERVERS}")

# Processing settings
print(f"Skip deleted: {CodeChangesConsumerConfig.SKIP_DELETED_FILES}")
print(f"Max retries: {CodeChangesConsumerConfig.MAX_RETRIES}")
```

## Message Format

Expected message from `data_enrichment` topic:

```json
{
  "event_id": "b22dad6e-d8b4-4291-ae7e-ec3cedd9b19a",
  "event_timestamp": "2025-11-18 18:04:27.553745",
  "schema_version": "1.0.0",
  "repository": "",
  "branch": "develop",
  "framework": "react",
  "commit_sha": "eb740947cf9fa2fbc53d65965d359f8519413fb7",
  "workspace": "default",
  "document_id": "8ec46ccd592675ca1e4cdb034bde33fa",
  "canonical_path": "default::::develop::src__utils__capitalize__ts",
  "file_path": "src/utils/capitalize.ts",
  "file_extension": ".ts",
  "language": null,
  "change_type": "modified",
  "previous_commit_sha": null,
  "content_encoding": "base64",
  "content_b64": "ZXhwb3J0IGNvbnN0IGNhcGl0YWxpemUgPSAoc3RyOiBzdHJpbmcpOiBzdHJpbmcgPT4gewogIHJldHVybiBzdHIuY2hhckF0KDApLnRvVXBwZXJDYXNlKCkgKyBzdHIuc2xpY2UoMSk7Cn07Cg==",
  "content_hash": "0e34aad2c560752af370e6313c28de3cb4bc37066a43fac949083bfa06226e85",
  "content_size_bytes": 109,
  "parser_nodes": null,
  "parser_relationships": null,
  "parser_metadata": null,
  "stage": "PREPROCESSING",
  "force_full_refresh": true,
  "document_version": 1,
  "is_latest": true
}
```

## Processing Flow

1. **Receive Message**: Poll from Kafka topic
2. **Deserialize**: JSON → DataEnrichmentEvent (Pydantic)
3. **Validate**: Check if should process (skip deleted, check content)
4. **Decode**: Base64 → UTF-8 string
5. **Detect Language**: From extension (`.ts` → `typescript`)
6. **Create CodeFile**: Prepare for analysis
7. **Extract Tenant IDs**: Get project/company/user IDs
8. **Analyze**: Call CodeServiceV2.analyze_files()
9. **Store**: Neo4j (graph) + Qdrant (vectors)
10. **Log Result**: Success/skip/error
11. **Commit Offset**: Mark as processed

## Supported Languages

Inherits from CodeServiceV2:

```
python, typescript, javascript, ruby, go, java,
kotlin, rust, cpp, c, csharp, php, swift
```

## Error Handling

### Transient Errors (Retry)
- Network errors
- API rate limits
- Temporary service unavailability

**Action**: Retry 3 times with exponential backoff (2s, 4s, 8s)

### Permanent Errors (Skip)
- Invalid base64 encoding
- Unsupported language
- Empty content
- Deleted files (configurable)

**Action**: Log error, skip message, commit offset

### Fatal Errors (Exit)
- Kafka connection lost
- Database connection lost
- Invalid configuration

**Action**: Log error, exit with code 1, restart by orchestrator

## Monitoring

Key metrics to track:

```python
# From processing result
result = {
    "status": "success",  # or "skipped", "error"
    "event_id": "...",
    "file_path": "src/utils/capitalize.ts",
    "successful": 1,
    "failed": 0,
    "skipped": 0,
    "neo4j_nodes": 5,
    "neo4j_relationships": 3,
    "qdrant_points": 6,
    "duration": 2.34,  # seconds
}
```

### Log Levels

- **INFO**: Message received, processing complete, startup/shutdown
- **DEBUG**: Detailed processing steps, retries, tenant ID lookup
- **WARNING**: Retries, unsupported languages
- **ERROR**: Processing failures, decode errors
- **SUCCESS**: Successful processing with stats

## Graceful Shutdown

Consumer handles `SIGINT` (Ctrl+C) and `SIGTERM` gracefully:

1. Stop accepting new messages
2. Finish processing current message
3. Commit offsets
4. Close Kafka connection
5. Cleanup resources
6. Exit cleanly

```bash
# Send SIGTERM
kill <pid>

# Or Ctrl+C
^C
```

## Production Checklist

- [ ] `KAFKA_BOOTSTRAP_SERVERS` configured
- [ ] `GEMINI_API_KEY` set
- [ ] Neo4j connection working
- [ ] Qdrant connection working
- [ ] Consumer group ID unique per environment
- [ ] Logging configured (level, format, destination)
- [ ] Health checks configured
- [ ] Restart policy configured (Docker/K8s)
- [ ] Monitoring alerts configured
- [ ] Multi-tenant ID lookup implemented

## Troubleshooting

### Consumer not receiving messages

```bash
# Check Kafka topic exists
kafka-topics.sh --list --bootstrap-server localhost:9092

# Check topic has messages
kafka-console-consumer.sh --topic data_enrichment --from-beginning --max-messages 1
```

### Decode errors

```
Failed to decode content for src/file.ts: Invalid base64-encoded string
```

**Solution**: Check `content_encoding` field is `"base64"`, verify base64 is valid

### Unsupported language

```
Skipping src/file.xyz: unsupported_language
```

**Solution**: File extension not in supported languages list. Add to `CodeServiceConfig.SUPPORTED_LANGUAGES`

### Connection errors

```
Error: KAFKA_BOOTSTRAP_SERVERS is required
```

**Solution**: Set environment variable

## Development

Run locally with test message:

```python
import asyncio
import base64
from shared.kafka_schemas import DataEnrichmentEvent, ChangeType
from api.consumers.code_changes_consumer import MessageHandler

# Create test event
code = "export const hello = () => console.log('hello');"
event = DataEnrichmentEvent(
    event_id="test-123",
    repository="test-repo",
    branch="main",
    framework="node",
    commit_sha="abc123",
    workspace="default",
    document_id="doc123",
    canonical_path="test::main::index.ts",
    file_path="index.ts",
    file_extension=".ts",
    change_type=ChangeType.MODIFIED,
    content_b64=base64.b64encode(code.encode()).decode(),
    content_hash="...",
    content_size_bytes=len(code),
)

# Process
handler = MessageHandler(code_analyzer_service)
result = await handler.handle_message(event)
print(result)
```

## Support

- Architecture: `CLAUDE.md`
- CodeServiceV2: `api/services/code_service_v2/README.md`
- Kafka Schemas: `shared/kafka_schemas.py`
