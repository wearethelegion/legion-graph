"""
Code-specific LLM prompts for code processing pipeline.
Optimized for extracting classes, functions, patterns, and dependencies from source code.
"""

CODE_ENTITY_EXTRACTION_PROMPT = """
You are a code analysis expert. Extract entities (classes, functions, patterns) and their relationships from this code.

**Entity Types to Extract:**
- **class**: Classes, interfaces, types, structs
- **function**: Functions, methods, procedures
- **module**: Files, packages, namespaces, modules
- **pattern**: Design patterns (Repository, Factory, Singleton, etc.)
- **concept**: Architectural concepts (dependency injection, async/await, ORM)
- **technology**: Libraries, frameworks, APIs used (FastAPI, React, PostgreSQL)
- **api**: REST endpoints, GraphQL queries, RPC methods

**Relationship Types:**
- INHERITS: Class A inherits from Class B
- IMPLEMENTS: Class A implements Pattern/Interface B
- CALLS: Function A calls Function B
- USES: Component A uses Technology/Library B
- DEPENDS_ON: Module A depends on Module B
- EXPOSES: Module A exposes API endpoint B
- FOLLOWS: Code follows Pattern/Principle B

**Output Format** (JSON):
{{
  "entities": [
    {{
      "name": "EntityName",  // canonical name (e.g., "UserService", "authenticate()")
      "type": "class|function|module|pattern|concept|technology|api",
      "description": "Brief 1-sentence description of what this entity is",
      "attributes": {{  // optional metadata
        "visibility": "public|private",
        "async": true,
        "decorators": ["@property", "@cached"],
        "parameters": ["user_id", "token"],
        "return_type": "Optional[User]"
      }},
      "confidence": 0.9  // 0.0-1.0 confidence score (>=0.5 for entities)
    }}
  ],
  "relationships": [
    {{
      "source": "EntityA",
      "target": "EntityB",
      "type": "INHERITS|IMPLEMENTS|CALLS|USES|DEPENDS_ON|EXPOSES|FOLLOWS",
      "context": "Brief explanation of WHY/HOW this relationship exists",
      "confidence": 0.85  // 0.0-1.0 confidence score (>=0.4 for relationships)
    }}
  ]
}}

**Guidelines:**
- Extract only entities explicitly present in the code (no assumptions)
- Use canonical names (e.g., "FastAPI" not "fastapi", "PostgreSQL" not "pg")
- Include relationships that are clearly defined (imports, inheritance, function calls)
- Confidence >= 0.5 for entities, >= 0.4 for relationships
- Description should explain WHAT the entity IS, not what it does
- Context should explain WHY/HOW the relationship exists
- For functions: include key parameters and return types in attributes
- For classes: include inheritance and decorators in attributes

**Code to analyze:**
**Filename:** {filename}
**Language:** {language}

```{language}
{code}
```

**Important:** Return ONLY valid JSON, no markdown formatting.

**Extracted Knowledge:**
"""

CODE_CHUNKING_PROMPT = """
You are a code chunking expert. Split the code into logical chunks (functions, classes, sections).

**Rules:**
1. Keep each function/method as a separate chunk (include docstring + implementation)
2. Keep related helper functions together with main function if tightly coupled
3. Preserve docstrings, comments, and type hints with code
4. Include critical imports if essential to understanding the chunk
5. For classes: chunk by method, but include class definition with first method
6. Extract metadata for each chunk (function signature, complexity, purpose)

**Output Format** (JSON array):
[
  {{
    "content": "full chunk text including docstrings, comments, and code",
    "summary": "1-sentence summary of what this chunk does",
    "chunk_type": "class|function|config|test|utility",  // auto-detect from content
    "complexity": "low|medium|high",  // based on control flow, nesting
    "entry_point": true,  // true if main entry point (if __name__ == "__main__", main(), etc.)
    "keywords": ["authentication", "jwt", "validation"],  // 3-5 technical terms
    "function_signature": "async def authenticate(user_id: str) -> Optional[User]",  // if function/method
    "class_name": "UserService",  // if part of a class
    "decorators": ["@staticmethod", "@cache"]  // if present
  }}
]

**Detection Rules:**
- chunk_type = "class" if contains class definition
- chunk_type = "function" if contains def/async def
- chunk_type = "test" if filename contains "test_" or function name starts with "test_"
- chunk_type = "config" if contains configuration/constants
- chunk_type = "utility" for helper functions
- complexity = "low" if simple logic (1-2 branches)
- complexity = "medium" if moderate logic (3-5 branches, some nesting)
- complexity = "high" if complex logic (6+ branches, deep nesting, recursion)
- entry_point = true if function is main(), __main__, or app entry

**Code to chunk:**
**Language:** {language}

```{language}
{code}
```

**Important:**
- Return ONLY valid JSON array, no markdown formatting
- Preserve original code exactly in "content" field
- Keep syntax highlighting context (triple backticks)
- Include whitespace and indentation

**Chunks:**
"""

CODE_TITLE_GENERATION_PROMPT = """
Generate a concise, descriptive title for this code file.

**Rules:**
- 8-12 words maximum
- Capture the main purpose and key components
- Include class/function names if they are central to the file
- Use clear, technical language
- Format: "Module: Purpose and Key Components"

**Examples:**
- "UserService: Authentication and Authorization Logic with JWT"
- "Neo4jRepository: Database Operations and Query Building for Graph Storage"
- "KnowledgeProcessor: LLM-Based Entity Extraction and Chunking Pipeline"
- "CompanyRoutes: REST API Endpoints for Company Management"
- "ConfigLoader: Environment Variables and Application Settings Parser"

**Code:**
**Filename:** {filename}

```
{code}
```

**Important:** Return ONLY the title text, no quotes, no JSON, no markdown.

**Title:**
"""

CODE_SUMMARY_GENERATION_PROMPT = """
Generate a 2-3 sentence summary of this code file.

**Rules:**
- Focus on WHAT the code does and WHY it exists
- Include key classes, functions, or patterns used
- Mention design patterns explicitly (Repository, Factory, Service Layer, etc.)
- Use clear, professional technical language
- 40-60 words total
- Explain the role in the larger system if apparent

**Example:**
"Implements user authentication using JWT tokens with refresh token rotation. Follows Repository Pattern for database access and Service Layer Pattern for business logic. Includes password hashing with bcrypt, rate limiting, and comprehensive error handling for security."

**Code:**
**Filename:** {filename}

```
{code}
```

**Important:** Return ONLY the summary text, no JSON, no markdown, no labels.

**Summary:**
"""
