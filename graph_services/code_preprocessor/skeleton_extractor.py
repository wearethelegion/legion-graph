"""Universal file skeleton extractor using TreeSitter.

Extracts class/function/method/import declarations from any supported language.
No bodies, no logic — just the structural skeleton.
"""

import os
from tree_sitter_language_pack import get_parser

EXTENSION_TO_LANGUAGE = {
    ".rb": "ruby",
    ".py": "python",
    ".ts": "typescript",
    ".tsx": "tsx",
    ".js": "javascript",
    ".jsx": "javascript",
    ".java": "java",
    ".go": "go",
    ".rs": "rust",
    ".cs": "csharp",
    ".kt": "kotlin",
    ".swift": "swift",
    ".php": "php",
}

DECLARATION_TYPES = {
    "ruby": {"module", "class", "method", "singleton_method", "call"},
    "python": {
        "class_definition",
        "function_definition",
        "import_statement",
        "import_from_statement",
    },
    "typescript": {
        "class_declaration",
        "function_declaration",
        "method_definition",
        "interface_declaration",
        "type_alias_declaration",
        "enum_declaration",
        "import_statement",
        "export_statement",
        "lexical_declaration",
    },
    "tsx": {
        "class_declaration",
        "function_declaration",
        "method_definition",
        "interface_declaration",
        "type_alias_declaration",
        "enum_declaration",
        "import_statement",
        "export_statement",
        "lexical_declaration",
    },
    "javascript": {
        "class_declaration",
        "function_declaration",
        "method_definition",
        "import_statement",
        "export_statement",
        "lexical_declaration",
    },
    "java": {
        "class_declaration",
        "method_declaration",
        "interface_declaration",
        "enum_declaration",
        "import_declaration",
    },
    "go": {"function_declaration", "method_declaration", "type_declaration", "import_declaration"},
    "rust": {
        "function_item",
        "struct_item",
        "impl_item",
        "enum_item",
        "trait_item",
        "use_declaration",
    },
    "csharp": {
        "class_declaration",
        "method_declaration",
        "interface_declaration",
        "namespace_declaration",
    },
    "kotlin": {"class_declaration", "function_declaration", "object_declaration"},
    "swift": {
        "class_declaration",
        "function_declaration",
        "protocol_declaration",
        "struct_declaration",
    },
    "php": {
        "class_declaration",
        "method_declaration",
        "function_definition",
        "namespace_definition",
    },
}


def _detect_language(file_path: str) -> str | None:
    ext = os.path.splitext(file_path)[1].lower()
    return EXTENSION_TO_LANGUAGE.get(ext)


_ANNOTATION_LANGUAGES = {"java", "csharp", "kotlin"}

_DECL_KEYWORDS = (
    "class ",
    "interface ",
    "enum ",
    "struct ",
    "func ",
    "function ",
    "def ",
    "fn ",
    "pub ",
    "private ",
    "public ",
    "protected ",
    "internal ",
    "override ",
    "abstract ",
    "async ",
    "static ",
    "val ",
    "var ",
)


def _first_line(node, language: str = "") -> str:
    """Extract the declaration line from a node.

    For annotation-heavy languages (Java, C#, Kotlin), skips annotations
    to find the actual keyword line.
    """
    text = node.text.decode("utf-8", errors="replace")
    lines = text.split("\n")

    if language in _ANNOTATION_LANGUAGES and len(lines) > 1:
        for line in lines:
            stripped = line.strip()
            if any(stripped.startswith(kw) for kw in _DECL_KEYWORDS):
                return stripped
    return lines[0].strip()


_RUBY_CALL_PREFIXES = ("include ", "extend ", "require ", "require_relative ")


def _is_declaration(node, language: str) -> bool:
    """Check if a node is a real declaration (not a keyword token)."""
    # Ruby: 'module' and 'class' appear as both keyword tokens (0 children)
    # and compound declaration nodes (>0 children). Skip keyword tokens.
    if language == "ruby" and node.type in ("module", "class"):
        return node.child_count > 0
    # Ruby: only keep call nodes that are include/extend/require
    if language == "ruby" and node.type == "call":
        line = _first_line(node, language)
        return any(line.startswith(p) for p in _RUBY_CALL_PREFIXES)
    return True


_SHALLOW_ONLY_TYPES = {"lexical_declaration", "export_statement"}


def _walk(node, target_types: set, declarations: list, language: str, depth: int = 0):
    if node.type in target_types and _is_declaration(node, language):
        # lexical_declaration / export_statement: only capture at top-level
        # or first nesting level (e.g. inside a function component body)
        if node.type in _SHALLOW_ONLY_TYPES and depth > 4:
            pass  # skip deeply nested const/let/export
        else:
            line = _first_line(node, language)
            if line:
                declarations.append(line)
    for child in node.children:
        _walk(child, target_types, declarations, language, depth + 1)


def extract_skeleton(file_path: str, content: str | None = None) -> dict | None:
    """Extract structural skeleton from a source file.

    Args:
        file_path: Path to source file (used to detect language from extension).
        content: Optional file content. If None, reads from file_path.

    Returns:
        Dict with file_path, language, and declarations list. None if language unsupported.
    """
    language = _detect_language(file_path)
    if not language:
        return None

    if content is None:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()

    parser = get_parser(language)
    tree = parser.parse(content.encode("utf-8"))
    target_types = DECLARATION_TYPES.get(language, set())

    declarations = []
    _walk(tree.root_node, target_types, declarations, language)

    return {
        "file_path": file_path,
        "language": language,
        "declarations": declarations,
    }


def extract_skeletons_from_directory(
    root_dir: str, extensions: set[str] | None = None
) -> list[dict]:
    """Walk a directory and extract skeletons from all source files.
    Returns list of skeleton dicts. Skips files with unsupported extensions.
    """
    if extensions is None:
        extensions = set(EXTENSION_TO_LANGUAGE.keys())

    results = []
    for dirpath, _, filenames in os.walk(root_dir):
        for fname in sorted(filenames):
            ext = os.path.splitext(fname)[1].lower()
            if ext in extensions:
                full_path = os.path.join(dirpath, fname)
                skeleton = extract_skeleton(full_path)
                if skeleton:
                    results.append(skeleton)
    return results


if __name__ == "__main__":
    import sys
    import json

    path = sys.argv[1] if len(sys.argv) > 1 else "."

    if os.path.isdir(path):
        skeletons = extract_skeletons_from_directory(path)
    else:
        result = extract_skeleton(path)
        skeletons = [result] if result else []

    for sk in skeletons:
        print(f"\n=== {sk['file_path']} ({sk['language']}) ===")
        for decl in sk["declarations"]:
            print(f"  {decl}")

    if not skeletons:
        print("No supported source files found.")
