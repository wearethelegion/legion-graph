# KGRAG gRPC Protocol Buffers

This directory contains Protocol Buffer definitions and generated Python code for KGRAG's gRPC services.

## Proto Files (Source Definitions)

### 1. `common.proto`
Shared message types used across all services:
- `ErrorResponse` - Standard error response format
- `QueryResult` - Common query result structure

### 2. `auth.proto`
Authentication service:
- **Service**: `AuthService`
  - `Authenticate(AuthRequest) -> AuthResponse`
  - `GetProjects(GetProjectsRequest) -> GetProjectsResponse`

### 3. `knowledge.proto`
Knowledge management service:
- **Service**: `KnowledgeService`
  - `CreateKnowledge(CreateKnowledgeRequest) -> CreateKnowledgeResponse`
  - `QueryKnowledge(QueryKnowledgeRequest) -> QueryKnowledgeResponse`
  - `FastQuery(FastQueryRequest) -> QueryKnowledgeResponse`
  - `SearchByTags(SearchByTagsRequest) -> QueryKnowledgeResponse`
  - `ExploreGraph(ExploreGraphRequest) -> QueryKnowledgeResponse`

### 4. `code.proto`
Code management service:
- **Service**: `CodeService`
  - `CreateCode(CreateCodeRequest) -> CreateCodeResponse`
  - `FindSimilarCode(FindSimilarCodeRequest) -> FindSimilarCodeResponse`
  - `AnalyzeImpact(AnalyzeImpactRequest) -> AnalyzeImpactResponse`
  - `TraceExecutionFlow(TraceExecutionFlowRequest) -> TraceExecutionFlowResponse`

### 5. `expertise.proto`
Expertise management service:
- **Service**: `ExpertiseService`
  - `CreateExpertise(CreateExpertiseRequest) -> CreateExpertiseResponse`
  - `AddExpertiseChunk(AddExpertiseChunkRequest) -> AddExpertiseChunkResponse`
  - `QueryExpertise(QueryExpertiseRequest) -> QueryExpertiseResponse`

## Generated Python Files

Each `.proto` file generates two Python files:

1. **`*_pb2.py`** - Protocol Buffer message classes
2. **`*_pb2_grpc.py`** - gRPC service stubs and servicers

### Generated Files:
- `common_pb2.py`, `common_pb2_grpc.py`
- `auth_pb2.py`, `auth_pb2_grpc.py`
- `knowledge_pb2.py`, `knowledge_pb2_grpc.py`
- `code_pb2.py`, `code_pb2_grpc.py`
- `expertise_pb2.py`, `expertise_pb2_grpc.py`

## Regenerating Stubs

If you modify any `.proto` file, regenerate Python stubs:

```bash
python3 -m grpc_tools.protoc \
  --proto_path=kgrag/protos \
  --python_out=kgrag/protos \
  --grpc_python_out=kgrag/protos \
  kgrag/protos/*.proto
```

**Important**: After regeneration, fix imports in generated files:
- Change `import <module>_pb2` to `from . import <module>_pb2`
- This is required for proper Python package imports

## Usage Example

```python
from kgrag.protos import auth_pb2, knowledge_pb2
from kgrag.protos import auth_pb2_grpc, knowledge_pb2_grpc

# Create message
request = knowledge_pb2.CreateKnowledgeRequest(
    text="Example knowledge",
    user_token="your-token",
    metadata={"type": "documentation"}
)

# Use with gRPC stub
# stub = knowledge_pb2_grpc.KnowledgeServiceStub(channel)
# response = await stub.CreateKnowledge(request)
```

## Service Coverage

Total: **4 gRPC services** with **13 RPC methods** (matching 13 MCP tools)

| Service | RPCs | MCP Tools Covered |
|---------|------|-------------------|
| AuthService | 2 | authenticateUser, getProjects |
| KnowledgeService | 5 | createKnowledge, queryKnowledge, fastQuery, searchByTags, exploreGraph |
| CodeService | 4 | createCode, findSimilarCode, analyzeImpact, traceExecutionFlow |
| ExpertiseService | 3 | createExpertise, addExpertiseChunk, queryExpertise |

## Next Steps

1. ✅ Proto definitions created
2. ✅ Python stubs generated
3. ✅ Imports verified
4. ⏳ Implement gRPC servicers (Epic 2)
5. ⏳ Add interceptors (Epic 3)
6. ⏳ Deploy gRPC server (Epic 4)

## References

- Design Document: `/Users/yubozhenko/Vetlyx/clean_versions/KGRAG/GRPC_MIGRATION_DESIGN.md`
- gRPC Python Guide: https://grpc.io/docs/languages/python/
- Protocol Buffers: https://protobuf.dev/
