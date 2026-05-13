"""
Enhanced Configuration for Code Intelligence System
Replaces the old LightRAG configuration with direct database connections
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

try:
    from google import genai
    from google.genai import types
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

# Load environment variables
load_dotenv()


@dataclass
class EnhancedConfig:
    """Enhanced configuration for direct database access without LightRAG"""
    
    # Database connections
    neo4j_uri: str = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
    neo4j_user: str = os.getenv('NEO4J_USER', 'neo4j')
    neo4j_password: str = os.getenv('NEO4J_PASSWORD', 'password')
    
    qdrant_url: str = os.getenv('QDRANT_URL', 'http://localhost:6333')
    qdrant_api_key: Optional[str] = os.getenv('QDRANT_API_KEY')
    
    redis_host: str = os.getenv('REDIS_HOST', 'localhost')
    redis_port: int = int(os.getenv('REDIS_PORT', '6379'))
    redis_db: int = int(os.getenv('REDIS_DB', '0'))
    redis_password: Optional[str] = os.getenv('REDIS_PASSWORD')
    redis_uri: str = os.getenv('REDIS_URI', 'redis://localhost:6379/0')
    
    mongo_uri: str = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
    mongo_database: str = os.getenv('MONGO_DATABASE', 'code_intel')
    
    # Processing settings
    batch_size: int = int(os.getenv('BATCH_SIZE', '10'))
    max_workers: int = int(os.getenv('MAX_WORKERS', '4'))
    chunk_size: int = int(os.getenv('CHUNK_SIZE', '1000'))
    overlap: int = int(os.getenv('CHUNK_OVERLAP', '200'))
    
    # LLM settings
    gemini_api_key: str = os.getenv('GEMINI_API_KEY', '')
    openai_api_key: Optional[str] = os.getenv('OPENAI_API_KEY')
    gemini_timeout: int = int(os.getenv('GEMINI_REQUESTTIMEOUT', '600'))  # ✅ Reduced from 300s to 60s for Gemini 2.5 Flash
    embedding_model: str = os.getenv('EMBEDDING_MODEL', 'text-embedding-004')
    model_name: str = os.getenv('MODEL_NAME', 'gemini-2.5-flash')
    context7_token: Optional[str] = os.getenv('CONTEXT7_TOKEN')
    
    # Paths
    output_dir: str = os.getenv('OUTPUT_DIR', './output')
    temp_dir: str = os.getenv('TEMP_DIR', '/tmp')
    
    # Workspace settings (unified configuration)
    workspace: str = os.getenv('WORKSPACE', 'code_intel')
    
    # Feature flags
    dry_run: bool = os.getenv('DRY_RUN', 'false').lower() == 'true'
    debug_output: bool = os.getenv('DEBUG_OUTPUT', 'false').lower() == 'true'
    enable_cache: bool = os.getenv('ENABLE_CACHE', 'true').lower() == 'true'
    
    # API settings (for backward compatibility)
    api_url: str = os.getenv('API_URL', 'http://localhost:8000')
    api_timeout: int = int(os.getenv('API_TIMEOUT', '30'))
    
    def __post_init__(self):
        """Create directories if they don't exist and build redis_uri if needed"""
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.temp_dir).mkdir(parents=True, exist_ok=True)
        
        # Build redis_uri from components if not provided via environment
        if self.redis_uri == 'redis://localhost:6379/0':  # default value
            if self.redis_password:
                self.redis_uri = f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
            else:
                self.redis_uri = f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"
    
    def get_collection_name(self, workspace: str, repository: str, branch: str = "main") -> str:
        """Generate collection name for Qdrant"""
        # Clean the names to be valid collection names
        workspace = workspace.replace('/', '_').replace('-', '_').lower()
        repository = repository.replace('/', '_').replace('-', '_').lower()
        branch = branch.replace('/', '_').replace('-', '_').lower()
        return f"{workspace}_{repository}_{branch}"
    
    def get_neo4j_connection(self):
        """Get Neo4j connection parameters"""
        return {
            "uri": self.neo4j_uri,
            "auth": (self.neo4j_user, self.neo4j_password)
        }
    
    def get_redis_connection(self):
        """Get Redis connection parameters"""
        return {
            "host": self.redis_host,
            "port": self.redis_port,
            "db": self.redis_db,
            "password": self.redis_password
        }
    
    def get_qdrant_connection(self):
        """Get Qdrant connection parameters"""
        params = {"url": self.qdrant_url}
        if self.qdrant_api_key:
            params["api_key"] = self.qdrant_api_key
        return params
    
    def _create_gemini_llm_func(self):
        """Create a Gemini LLM function for code analysis"""

        if not self.gemini_api_key or self.gemini_api_key == 'your-gemini-api-key-here':
            # Return a mock function that doesn't require API key
            def mock_llm_func(prompt: str) -> str:
                """Mock LLM function for testing without API key"""
                return '{"summary": "Code analysis placeholder", "complexity": "medium", "key_features": []}'
            return mock_llm_func

        if not HAS_GENAI:
            raise ImportError("google-genai package not installed. Run: pip install google-genai")

        client = genai.Client(api_key=self.gemini_api_key)

        async def llm_func(prompt: str) -> str:
            """✅ ASYNC with retry logic for 503/504 errors"""
            import asyncio

            max_retries = 3
            base_delay = 2

            for attempt in range(max_retries):
                try:
                    response = client.models.generate_content(
                        model=self.model_name,
                        contents=prompt,
                        config=types.GenerateContentConfig(
                            temperature=0.1,
                        )
                    )
                    return response.text

                except Exception as e:
                    error_msg = str(e)
                    is_retryable = (
                        "503" in error_msg or "overloaded" in error_msg.lower() or
                        "504" in error_msg or "timed out" in error_msg.lower()
                    )

                    if is_retryable and attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        error_type = "overload" if "503" in error_msg else "timeout"
                        print(f"⚠️ Gemini API {error_type}, retry {attempt + 1}/{max_retries} after {delay}s...")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        print(f"❌ Error calling Gemini API: {e}")
                        return '{"error": "' + str(e) + '"}'

            return '{"error": "Max retries exceeded"}'

        return llm_func
    
    def _create_gemini_embed_func(self):
        """Create a Gemini embedding function for vector operations"""

        if not self.gemini_api_key or self.gemini_api_key == 'your-gemini-api-key-here':
            # Return a mock function that doesn't require API key
            def mock_embed_func(texts) -> list:
                """Mock embedding function for testing without API key (supports batch processing)"""
                import hashlib

                # Handle both single text and batch of texts
                if isinstance(texts, str):
                    texts = [texts]
                elif isinstance(texts, list):
                    pass  # Already a list
                else:
                    texts = list(texts)  # Convert sequence to list

                embeddings = []
                for text in texts:
                    # Return a simple hash-based vector for testing
                    hash_val = int(hashlib.md5(text.encode()).hexdigest()[:8], 16)
                    vector = [float((hash_val >> i) & 0xFF) / 255.0 for i in range(0, 32, 4)] * 96  # 768 dims
                    embeddings.append(vector)

                return embeddings
            return mock_embed_func

        if not HAS_GENAI:
            raise ImportError("google-genai package not installed. Run: pip install google-genai")

        client = genai.Client(api_key=self.gemini_api_key)

        def embed_func(texts) -> list:
            """Function to create embeddings with Gemini (supports batch processing)"""
            try:
                # Handle both single text and batch of texts
                if isinstance(texts, str):
                    texts = [texts]
                elif isinstance(texts, list):
                    pass  # Already a list
                else:
                    texts = list(texts)  # Convert sequence to list

                embeddings = []
                for text in texts:
                    try:
                        # Use new SDK embed method
                        response = client.models.embed_content(
                            model=self.embedding_model,
                            contents=text
                        )
                        # Extract embedding from response
                        if hasattr(response, 'embeddings') and response.embeddings:
                            embeddings.append(response.embeddings[0].values)
                        elif hasattr(response, 'embedding'):
                            embeddings.append(response.embedding)
                        else:
                            # Fallback: assume response is the embedding directly
                            embeddings.append(response if isinstance(response, list) else [0.0] * 768)
                    except Exception as e:
                        print(f"Error creating embedding for text: {e}")
                        # Return zero vector on error for this text
                        embeddings.append([0.0] * 768)

                return embeddings

            except Exception as e:
                print(f"Error in batch embedding: {e}")
                # Return zero vectors for all texts on error
                num_texts = len(texts) if hasattr(texts, '__len__') else 1
                return [[0.0] * 768] * num_texts

        return embed_func