"""Universal file chunking: TreeSitter for code, recursive text split for everything else."""

import os
from typing import Optional

from tree_sitter_language_pack import get_parser
from code_preprocessor.skeleton_extractor import _detect_language

MIN_CHUNK_SIZE = 50  # Filter out chunks smaller than this (in characters)

# TreeSitter emits `singleton_method` for `def self.foo`; treat it the same as `method`.
_NODE_TYPE_ALIASES: dict[str, str] = {
    "singleton_method": "method",
}


def chunk_file(
    file_path: str,
    content: str,
    max_chars: int = 1000,
    chunker_config: Optional[dict] = None,
) -> list[tuple[str, int, int]]:
    """Chunk a file using the best strategy for its type.

    Code files (supported by TreeSitter): AST-aware recursive chunking.
    Everything else: recursive text chunking (paragraphs → lines → sentences).

    When *chunker_config* is provided (Phase 2.1 / V3 pipeline), the config
    drives chunk boundaries using AST node types and per-construct min/max
    sizes.  When None, falls back to the legacy size-based strategy (backward
    compatible with V2).

    Supports two config shapes:
    - Gemini / V3 verbose shape: ``ast_chunk_boundaries`` list of dicts with
      ``node_type``, ``max_size_chars``, ``min_size_chars``.
    - Legacy flat shape: ``boundaries`` dict + ``ast_node_types`` list.

    Also honours ``file_type_overrides`` (Gemini shape): a list of dicts with
    ``extension``, ``strategy`` (``ast_based`` | ``recursive_text``), and
    ``chunk_size``.

    Returns:
        list of (chunk_text, start_line, end_line) tuples.
        Line numbers are 1-indexed (human-readable).
        Chunks smaller than MIN_CHUNK_SIZE are filtered out.
    """
    language = _detect_language(file_path)

    if chunker_config:
        # --- honour file_type_overrides (Gemini shape) -----------------------
        ext = os.path.splitext(file_path)[1].lower()
        override = _get_file_type_override(chunker_config, ext)

        if override:
            strategy = override.get("strategy", "ast_based")
            override_size = override.get("chunk_size", max_chars)
            if strategy == "recursive_text":
                chunks = _chunk_text(content, override_size)
                return [c for c in chunks if len(c[0].strip()) >= MIN_CHUNK_SIZE]
            # strategy == "ast_based": fall through with override_size
            effective_max = override_size
        else:
            effective_max = max_chars

        if language:
            try:
                chunks = _chunk_code_with_config(content, language, chunker_config, effective_max)
                if chunks:
                    return [c for c in chunks if len(c[0].strip()) >= MIN_CHUNK_SIZE]
            except Exception:
                pass  # fall through to size-based chunking
        # Config provided but no TreeSitter language — use config max as max_chars
        text_max = _config_max_chars(chunker_config, effective_max)
        chunks = _chunk_text(content, text_max)
        return [c for c in chunks if len(c[0].strip()) >= MIN_CHUNK_SIZE]

    # Legacy path (no config)
    if language:
        try:
            chunks = _chunk_code(content, language, max_chars)
            if chunks:
                return [c for c in chunks if len(c[0].strip()) >= MIN_CHUNK_SIZE]
        except Exception:
            pass  # fall through to text chunking
    chunks = _chunk_text(content, max_chars)
    return [c for c in chunks if len(c[0].strip()) >= MIN_CHUNK_SIZE]


# ── Config normalisation ──────────────────────────────────────────────────────


def _build_boundary_map(config: dict) -> dict[str, dict[str, int]]:
    """Normalise config into a canonical boundary_map keyed by node_type.

    Accepts both config shapes:

    *Gemini / V3 verbose shape*::

        "ast_chunk_boundaries": [
            {"node_type": "class",  "max_size_chars": 2000, "min_size_chars": 100},
            ...
        ]

    *Legacy flat shape*::

        "boundaries": {
            "class":    {"max": 2000, "min": 100},
            ...
        }

    Returns a dict ``{node_type: {"max": int, "min": int}}``.
    ``singleton_method`` is automatically added as an alias for ``method``
    (TreeSitter emits ``singleton_method`` for ``def self.foo``).
    """
    boundary_map: dict[str, dict[str, int]] = {}

    # Gemini / V3 verbose shape
    ast_boundaries: list[dict] = config.get("ast_chunk_boundaries", [])
    for entry in ast_boundaries:
        node_type = entry.get("node_type", "")
        if not node_type:
            continue
        boundary_map[node_type] = {
            "max": entry.get("max_size_chars", entry.get("max", 1000)),
            "min": entry.get("min_size_chars", entry.get("min", MIN_CHUNK_SIZE)),
        }

    # Legacy flat shape (only used when Gemini shape absent)
    if not boundary_map:
        legacy: dict = config.get("boundaries", {})
        for node_type, sizes in legacy.items():
            boundary_map[node_type] = {
                "max": sizes.get("max", 1000),
                "min": sizes.get("min", MIN_CHUNK_SIZE),
            }

    # Inject TreeSitter aliases
    for ts_alias, canonical in _NODE_TYPE_ALIASES.items():
        if canonical in boundary_map and ts_alias not in boundary_map:
            boundary_map[ts_alias] = boundary_map[canonical]

    return boundary_map


def _get_file_type_override(config: dict, ext: str) -> Optional[dict]:
    """Return the file_type_override entry matching *ext*, or None."""
    for override in config.get("file_type_overrides", []):
        if override.get("extension", "").lower() == ext:
            return override
    return None


# ── Config helpers ────────────────────────────────────────────────────────────


def _config_max_chars(config: dict, default: int) -> int:
    """Derive a single max_chars value from the config.

    Prefers ``fallback_chunk_size`` when present, otherwise takes the maximum
    of all per-construct maximums.  Falls back to *default* if neither exists.

    Accepts both config shapes (Gemini verbose and legacy flat).
    """
    if "fallback_chunk_size" in config:
        return config["fallback_chunk_size"]
    boundary_map = _build_boundary_map(config)
    if not boundary_map:
        return default
    return max((v["max"] for v in boundary_map.values()), default=default)


def _config_boundary_max(config: dict, node_type: str, default: int) -> int:
    """Return the max_chars for a specific AST node type based on config.

    Performs an exact lookup first, then falls back to substring match
    (e.g. "class_declaration" matches key "class") for backward compat.
    Accepts both config shapes.
    """
    boundary_map = _build_boundary_map(config)
    if node_type in boundary_map:
        return boundary_map[node_type]["max"]
    # Substring fallback for legacy keys like "class" matching "class_definition"
    for key, sizes in boundary_map.items():
        if key in node_type:
            return sizes["max"]
    return default


def _config_boundary_min(config: dict, node_type: str, default: int = MIN_CHUNK_SIZE) -> int:
    """Return the min size for a specific AST node type based on config.

    Accepts both config shapes.
    """
    boundary_map = _build_boundary_map(config)
    if node_type in boundary_map:
        return boundary_map[node_type]["min"]
    for key, sizes in boundary_map.items():
        if key in node_type:
            return sizes["min"]
    return default


def _is_boundary_node(node_type: str, ast_node_types: list[str]) -> bool:
    """Return True when *node_type* is listed as a primary chunk boundary."""
    return node_type in ast_node_types


def _get_boundary_types(config: dict) -> set[str]:
    """Return the set of boundary node types from the config (both shapes)."""
    boundary_map = _build_boundary_map(config)
    if boundary_map:
        return set(boundary_map.keys())
    # Legacy: ast_node_types is an explicit list
    return set(config.get("ast_node_types", []))


# ── Config-driven code chunking ───────────────────────────────────────────────


def _chunk_code_with_config(
    content: str, language: str, config: dict, default_max: int
) -> list[tuple[str, int, int]]:
    """Chunk code guided by chunker_config AST node types and size limits.

    Accepts both config shapes (Gemini verbose ``ast_chunk_boundaries`` and
    legacy flat ``boundaries`` + ``ast_node_types``).

    Strategy:
    - Normalise config once into a ``boundary_map`` at function entry.
    - Walk root children (and one level into ``call`` nodes to catch
      ``do_block`` nodes used in Ruby DSLs / RSpec).
    - If a child is a *boundary node*:
        - Use its per-type max as the size cap.
        - If it fits → emit as its own chunk.
        - If too large → recurse with _chunk_node (size-based).
        - If smaller than per-type min → accumulate into a neighbour group.
    - Non-boundary children are grouped together up to the global max.
    """
    parser = get_parser(language)
    tree = parser.parse(content.encode("utf-8"))
    content_bytes = content.encode("utf-8")

    # Normalise config once — works for both Gemini and legacy shapes
    boundary_map = _build_boundary_map(config)
    boundary_types: set[str] = set(boundary_map.keys())
    global_max = _config_max_chars(config, default_max)

    chunks: list[tuple[str, int, int]] = []
    # Accumulator for non-boundary / too-small nodes
    acc_parts: list[bytes] = []
    acc_start: int = 1
    acc_end: int = 1
    acc_len: int = 0

    def _flush_acc() -> None:
        nonlocal acc_parts, acc_start, acc_end, acc_len
        if acc_parts:
            text = b"".join(acc_parts).decode("utf-8", errors="replace")
            chunks.append((text, acc_start, acc_end))
            acc_parts = []
            acc_len = 0

    def _emit_boundary(node) -> None:
        """Process a confirmed boundary node: flush acc, then emit or recurse."""
        nonlocal acc_start, acc_end
        _flush_acc()
        node_text = content_bytes[node.start_byte : node.end_byte]
        node_len = len(node_text)
        node_start = node.start_point[0] + 1
        node_end = node.end_point[0] + 1
        acc_start = node_end + 1
        acc_end = node_end + 1

        node_type = _NODE_TYPE_ALIASES.get(node.type, node.type)
        sizes = boundary_map.get(node_type, boundary_map.get(node.type, {}))
        node_max = sizes.get("max", global_max)
        node_min = sizes.get("min", MIN_CHUNK_SIZE)

        if node_len <= node_max:
            if node_len >= node_min:
                chunks.append((node_text.decode("utf-8", errors="replace"), node_start, node_end))
            else:
                # Too small — accumulate with neighbours
                if not acc_parts:
                    acc_start = node_start
                acc_parts.append(node_text)
                acc_end = node_end
                if (acc_len + node_len) >= global_max:
                    _flush_acc()
        else:
            sub_chunks = _chunk_node(node, content_bytes, node_max)
            chunks.extend(sub_chunks)

    for child in tree.root_node.children:
        # Resolve aliases (e.g. singleton_method → method)
        effective_type = _NODE_TYPE_ALIASES.get(child.type, child.type)

        if effective_type in boundary_types or child.type in boundary_types:
            _emit_boundary(child)
        elif child.type == "call":
            # `do_block` is nested one level inside `call` nodes in Ruby.
            # Check whether the call's do_block is itself a boundary type.
            do_block_child = next((c for c in child.children if c.type == "do_block"), None)
            if do_block_child is not None and "do_block" in boundary_types:
                _emit_boundary(do_block_child)
            else:
                # Accumulate the whole call node as non-boundary content
                child_text = content_bytes[child.start_byte : child.end_byte]
                child_len = len(child_text)
                child_start = child.start_point[0] + 1
                child_end = child.end_point[0] + 1
                if acc_parts and acc_len + child_len > global_max:
                    _flush_acc()
                    acc_start = child_start
                    acc_end = child_start
                if not acc_parts:
                    acc_start = child_start
                acc_parts.append(child_text)
                acc_end = child_end
                acc_len += child_len
        else:
            # Non-boundary node: accumulate
            child_text = content_bytes[child.start_byte : child.end_byte]
            child_len = len(child_text)
            child_start = child.start_point[0] + 1
            child_end = child.end_point[0] + 1
            if acc_parts and acc_len + child_len > global_max:
                _flush_acc()
                acc_start = child_start
                acc_end = child_start

            if not acc_parts:
                acc_start = child_start
            acc_parts.append(child_text)
            acc_end = child_end
            acc_len += child_len

    _flush_acc()
    # Do not re-merge the final list: boundary nodes were intentionally emitted
    # as separate chunks.  Only filter out sub-MIN_CHUNK_SIZE remnants.
    return [c for c in chunks if len(c[0].strip()) >= MIN_CHUNK_SIZE]


# ── Legacy size-based code chunking ──────────────────────────────────────────


def _chunk_code(content: str, language: str, max_chars: int) -> list[tuple[str, int, int]]:
    """Chunk code using TreeSitter AST-aware recursive splitting."""
    parser = get_parser(language)
    tree = parser.parse(content.encode("utf-8"))
    content_bytes = content.encode("utf-8")

    chunks = _chunk_node(tree.root_node, content_bytes, max_chars)
    return _merge_small_chunks(chunks, max_chars)


def _chunk_node(node, content_bytes: bytes, max_chars: int) -> list[tuple[str, int, int]]:
    """Recursively chunk a TreeSitter node.

    Strategy:
    - If node fits in max_chars → return as single chunk
    - If node has children → group children into chunks
    - If single child too large → recurse into that child
    - If leaf too large → fall back to text chunking

    Returns list of (text, start_line, end_line) tuples.
    start_line and end_line are 1-indexed.
    TreeSitter node.start_point and node.end_point are (row, col), 0-indexed.
    """
    node_text = content_bytes[node.start_byte : node.end_byte]
    node_len = len(node_text)
    # TreeSitter rows are 0-indexed; convert to 1-indexed
    node_start_line = node.start_point[0] + 1
    node_end_line = node.end_point[0] + 1

    # Base case: node fits
    if node_len <= max_chars:
        return [(node_text.decode("utf-8", errors="replace"), node_start_line, node_end_line)]

    # Has children: try grouping them
    if node.child_count > 0:
        chunks: list[tuple[str, int, int]] = []
        current_group: list[bytes] = []
        current_len = 0
        current_start_line = node_start_line
        current_end_line = node_start_line

        for child in node.children:
            child_text = content_bytes[child.start_byte : child.end_byte]
            child_len = len(child_text)
            child_start_line = child.start_point[0] + 1
            child_end_line = child.end_point[0] + 1

            # Single child exceeds max → recurse
            if child_len > max_chars:
                # Flush current group
                if current_group:
                    group_text = b"".join(current_group).decode("utf-8", errors="replace")
                    chunks.append((group_text, current_start_line, current_end_line))
                    current_group = []
                    current_len = 0
                    current_start_line = child_start_line
                    current_end_line = child_start_line

                # Recurse into large child
                chunks.extend(_chunk_node(child, content_bytes, max_chars))
                # Update current tracking for next group
                if chunks:
                    current_start_line = chunks[-1][2] + 1
                    current_end_line = chunks[-1][2] + 1
            else:
                # Try adding to current group
                if current_len + child_len > max_chars and current_group:
                    # Flush group
                    group_text = b"".join(current_group).decode("utf-8", errors="replace")
                    chunks.append((group_text, current_start_line, current_end_line))
                    current_group = []
                    current_len = 0
                    current_start_line = child_start_line
                    current_end_line = child_end_line

                if not current_group:
                    current_start_line = child_start_line

                current_group.append(child_text)
                current_end_line = child_end_line
                current_len += child_len

        # Flush remaining
        if current_group:
            group_text = b"".join(current_group).decode("utf-8", errors="replace")
            chunks.append((group_text, current_start_line, current_end_line))

        return (
            chunks
            if chunks
            else [(node_text.decode("utf-8", errors="replace"), node_start_line, node_end_line)]
        )

    # Leaf node too large: fall back to text chunking with node's start line as base
    text = node_text.decode("utf-8", errors="replace")
    fallback = _chunk_text(text, max_chars, base_line=node_start_line)
    return fallback


def _chunk_text(text: str, max_chars: int, base_line: int = 1) -> list[tuple[str, int, int]]:
    """Recursive text chunking: paragraphs → lines → sentences → hard cut.

    Args:
        text: Text to chunk.
        max_chars: Maximum chunk size in characters.
        base_line: 1-indexed line number of the first line in `text`.
                   Used when called as fallback from TreeSitter leaf nodes.

    Returns list of (text, start_line, end_line) tuples.
    """
    blocks = text.split("\n\n")
    pieces: list[tuple[str, int, int]] = []
    current_line = base_line

    for block in blocks:
        block_lines = block.count("\n") + 1
        block_start = current_line
        block_end = current_line + block_lines - 1

        if len(block) <= max_chars:
            pieces.append((block, block_start, block_end))
        else:
            # Split by line
            lines = block.split("\n")
            line_offset = 0
            for line in lines:
                line_start = block_start + line_offset
                if len(line) <= max_chars:
                    pieces.append((line, line_start, line_start))
                else:
                    # Split by sentence
                    sentences = line.split(". ")
                    sent_char_offset = 0
                    for sentence in sentences:
                        if len(sentence) <= max_chars:
                            pieces.append((sentence, line_start, line_start))
                        else:
                            # Hard cut
                            for i in range(0, len(sentence), max_chars):
                                pieces.append((sentence[i : i + max_chars], line_start, line_start))
                        sent_char_offset += len(sentence) + 2  # +2 for ". "
                line_offset += 1

        # +1 for the blank line between paragraphs
        current_line = block_end + 2

    # Merge small adjacent pieces
    return _merge_small_chunks(pieces, max_chars)


def _merge_small_chunks(
    chunks: list[tuple[str, int, int]], max_chars: int
) -> list[tuple[str, int, int]]:
    """Merge adjacent chunks if they fit within max_chars.

    Filters out chunks smaller than MIN_CHUNK_SIZE during merging.
    When merging, the start_line of the first chunk and end_line of the
    last chunk are preserved to give the correct line range.
    """
    if not chunks:
        return []

    merged: list[tuple[str, int, int]] = []
    cur_text, cur_start, cur_end = chunks[0]

    for next_text, next_start, next_end in chunks[1:]:
        if len(cur_text) + len(next_text) + 1 <= max_chars:  # +1 for separator
            cur_text = cur_text + "\n" + next_text
            cur_end = next_end
        else:
            if len(cur_text.strip()) >= MIN_CHUNK_SIZE:
                merged.append((cur_text, cur_start, cur_end))
            cur_text, cur_start, cur_end = next_text, next_start, next_end

    if len(cur_text.strip()) >= MIN_CHUNK_SIZE:
        merged.append((cur_text, cur_start, cur_end))

    return merged
