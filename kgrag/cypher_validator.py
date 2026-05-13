"""
Cypher Query Validator
Security validation for user-provided Cypher queries to prevent injection attacks.
"""

from typing import List, Tuple, Optional
import re
from loguru import logger


class CypherQueryValidator:
    """
    Validate Cypher queries for read-only operations.

    Blocks all write operations to prevent data corruption and injection attacks.
    """

    # Forbidden commands that modify data
    FORBIDDEN_COMMANDS = {
        "DELETE",
        "DETACH DELETE",
        "CREATE",
        "MERGE",
        "SET",
        "REMOVE",
        "DROP",
        "CALL",  # Block procedure calls (can be dangerous)
        "LOAD CSV",  # Block file operations
        "FOREACH",  # Block loops (can contain writes)
    }

    # Forbidden patterns (regex)
    FORBIDDEN_PATTERNS = [
        r"\bDELETE\b",
        r"\bCREATE\b",
        r"\bMERGE\b",
        r"\bSET\b",
        r"\bREMOVE\b",
        r"\bDROP\b",
        r"\bCALL\b",
        r"\bLOAD\s+CSV\b",
        r"\bFOREACH\b",
        r"\bDETACH\s+DELETE\b",
    ]

    # Maximum query length (prevent DoS)
    MAX_QUERY_LENGTH = 5000

    # Maximum RETURN limit (prevent large result sets)
    MAX_RETURN_LIMIT = 1000

    @classmethod
    def validate(cls, cypher: str) -> Tuple[bool, Optional[str]]:
        """
        Validate Cypher query for read-only operations.

        Args:
            cypher: Cypher query string

        Returns:
            Tuple of (is_valid, error_message)
            - (True, None) if valid
            - (False, "error message") if invalid
        """
        # Check query length
        if len(cypher) > cls.MAX_QUERY_LENGTH:
            return False, f"Query too long ({len(cypher)} chars). Maximum: {cls.MAX_QUERY_LENGTH} chars"

        # Normalize query for checking
        cypher_upper = cypher.upper()

        # Check for forbidden patterns (regex with word boundaries)
        # This prevents false positives like ":calls" matching "CALL"
        for pattern in cls.FORBIDDEN_PATTERNS:
            match = re.search(pattern, cypher_upper)
            if match:
                forbidden_cmd = match.group(0)
                logger.warning(f"Blocked forbidden pattern: {forbidden_cmd} in query: {cypher[:100]}")
                return False, f"Forbidden command: {forbidden_cmd}. Only read-only queries allowed (MATCH, RETURN, WHERE, WITH, etc.)"

        # Validate query starts with allowed keywords
        # Strip leading whitespace and comments
        stripped_query = re.sub(r"^\s*(//.*\n\s*)*", "", cypher, flags=re.MULTILINE).strip()
        if not stripped_query:
            return False, "Empty query"

        allowed_start_keywords = ["MATCH", "RETURN", "WITH", "OPTIONAL MATCH", "UNWIND"]
        query_start = stripped_query.split()[0].upper()

        if query_start not in allowed_start_keywords:
            logger.warning(f"Query must start with {allowed_start_keywords}, got: {query_start}")
            return False, f"Query must start with {', '.join(allowed_start_keywords)}. Got: {query_start}"

        # Validate RETURN limit (prevent excessive results)
        if "LIMIT" in cypher_upper:
            # Extract LIMIT value
            limit_match = re.search(r"LIMIT\s+(\d+)", cypher_upper)
            if limit_match:
                limit_value = int(limit_match.group(1))
                if limit_value > cls.MAX_RETURN_LIMIT:
                    return False, f"LIMIT {limit_value} exceeds maximum {cls.MAX_RETURN_LIMIT}"
        else:
            # No LIMIT specified - warn but allow (will be capped by Neo4j)
            logger.debug(f"Query has no LIMIT clause: {cypher[:100]}")

        logger.info(f"Cypher query validated: {cypher[:100]}")
        return True, None

    @classmethod
    def sanitize_limit(cls, cypher: str, default_limit: int = 100) -> str:
        """
        Add LIMIT clause if missing, or cap existing LIMIT.

        Args:
            cypher: Cypher query string
            default_limit: Default limit to add if missing

        Returns:
            Modified query with LIMIT clause
        """
        cypher_upper = cypher.upper()

        # Check if LIMIT already exists
        if "LIMIT" in cypher_upper:
            # Extract and cap limit
            limit_match = re.search(r"LIMIT\s+(\d+)", cypher_upper, re.IGNORECASE)
            if limit_match:
                current_limit = int(limit_match.group(1))
                if current_limit > cls.MAX_RETURN_LIMIT:
                    # Replace with max limit
                    cypher = re.sub(
                        r"LIMIT\s+\d+",
                        f"LIMIT {cls.MAX_RETURN_LIMIT}",
                        cypher,
                        flags=re.IGNORECASE
                    )
                    logger.info(f"Capped LIMIT from {current_limit} to {cls.MAX_RETURN_LIMIT}")
            return cypher
        else:
            # Add default LIMIT
            cypher = f"{cypher.rstrip()} LIMIT {min(default_limit, cls.MAX_RETURN_LIMIT)}"
            logger.info(f"Added LIMIT {min(default_limit, cls.MAX_RETURN_LIMIT)} to query")
            return cypher

    # ─── Tenant Scoping Injection ───────────────────────────────────

    # Regex: captures (OPTIONAL MATCH | MATCH) followed by the first node variable
    _MATCH_CLAUSE_RE = re.compile(
        r"((?:OPTIONAL\s+)?MATCH\s*\(\s*)(\w+)",
        re.IGNORECASE,
    )

    # Clause-level boundary keywords that signal the end of a MATCH + WHERE scope.
    # WHERE is intentionally excluded — it belongs *inside* the current MATCH scope.
    _CLAUSE_BOUNDARY_RE = re.compile(
        r"\b(?:RETURN|WITH|UNION|ORDER\s+BY|UNWIND)\b|(?:(?:OPTIONAL\s+)?MATCH\b)",
        re.IGNORECASE,
    )

    @classmethod
    def inject_company_id_scope(cls, cypher: str) -> str:
        """
        Auto-inject ``WHERE <var>.company_id = $company_id`` into every
        MATCH / OPTIONAL MATCH clause in a user-supplied Cypher query.

        Security contract:
        1. If the query already contains the string ``company_id``
           (case-insensitive), it is **rejected** with ``ValueError``.
           Users must never reference ``company_id`` — the server injects it.
        2. For each MATCH clause the first node variable is extracted and a
           tenant-scoping predicate is injected:
           - No existing WHERE → ``WHERE var.company_id = $company_id``
           - Existing WHERE     → ``AND var.company_id = $company_id``
        3. The query must contain at least one MATCH with a named node variable.

        Args:
            cypher: Raw user-supplied Cypher (must NOT reference company_id).

        Returns:
            Modified Cypher with tenant scoping injected into every MATCH clause.

        Raises:
            ValueError: If the query references ``company_id`` or has no valid
                        MATCH clause with a node variable.
        """
        # ── Step 1: reject queries that mention company_id at all ──────
        if re.search(r"company_id", cypher, re.IGNORECASE):
            raise ValueError(
                "Queries must NOT reference 'company_id'. "
                "Tenant scoping is automatically injected by the server."
            )

        # ── Step 2: locate every MATCH clause + first node variable ────
        matches: List[re.Match] = list(cls._MATCH_CLAUSE_RE.finditer(cypher))
        if not matches:
            raise ValueError(
                "Query must contain at least one MATCH clause with a "
                "named node variable, e.g. MATCH (n:Label) RETURN n"
            )

        # ── Step 3: inject scope, processing right→left to keep positions valid
        for match_obj in reversed(matches):
            var_name: str = match_obj.group(2)
            search_start: int = match_obj.end()  # after the variable name

            # Find the clause boundary for this MATCH (next RETURN/WITH/MATCH/…)
            boundary_hit = cls._CLAUSE_BOUNDARY_RE.search(cypher, pos=search_start)
            boundary_pos: int = boundary_hit.start() if boundary_hit else len(cypher)

            # Does a WHERE already exist between this MATCH and its boundary?
            segment = cypher[search_start:boundary_pos]
            has_where = re.search(r"\bWHERE\b", segment, re.IGNORECASE)

            scope_predicate = f"{var_name}.company_id = $company_id"

            if has_where:
                # Append AND before the boundary
                cypher = (
                    cypher[:boundary_pos]
                    + f" AND {scope_predicate}"
                    + cypher[boundary_pos:]
                )
            else:
                # Insert WHERE before the boundary
                cypher = (
                    cypher[:boundary_pos]
                    + f" WHERE {scope_predicate} "
                    + cypher[boundary_pos:]
                )

        logger.info(f"Injected company_id scope into {len(matches)} MATCH clause(s)")
        return cypher
