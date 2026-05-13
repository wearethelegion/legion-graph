"""
Code Extraction Utilities

Pure functions for extracting code segments from source files.
No dependencies, easy to test.

Author: Code Intelligence Team
"""

from typing import List


LANG_MAP = {
    'py': 'python', 'rb': 'ruby', 'js': 'javascript',
    'ts': 'typescript', 'java': 'java', 'go': 'go',
    'rs': 'rust', 'cpp': 'cpp', 'c': 'c', 'cs': 'csharp',
    'php': 'php', 'swift': 'swift',
}


def extract_by_lines(code: str, start_line: int, end_line: int) -> str:
    """
    Extract code using precise line boundaries.

    Args:
        code: Source code as string
        start_line: 1-based start line
        end_line: 1-based end line (inclusive)

    Returns:
        Extracted code or empty string if invalid range
    """
    lines = code.split('\n')
    if start_line < 1 or end_line > len(lines) or start_line > end_line:
        return ""
    return '\n'.join(lines[start_line-1:end_line])


def extract_by_indent(code: str, start_line: int) -> str:
    """
    Extract code block using indentation (for symbols without end_line).

    Args:
        code: Source code as string
        start_line: 1-based line where block starts

    Returns:
        Extracted code block
    """
    lines = code.split('\n')
    if start_line < 1 or start_line > len(lines):
        return ""

    result = [lines[start_line-1]]
    base_indent = len(lines[start_line-1]) - len(lines[start_line-1].lstrip())

    for i in range(start_line, len(lines)):
        line = lines[i]

        # Include empty lines
        if not line.strip():
            result.append(line)
            continue

        # Stop at same or lower indentation
        current_indent = len(line) - len(line.lstrip())
        if current_indent <= base_indent:
            break

        result.append(line)

    return '\n'.join(result)


def extract_variable(code: str, line: int) -> str:
    """
    Extract variable declaration (may span multiple lines).

    Handles multi-line declarations that end with continuation characters
    or opening brackets.

    Args:
        code: Source code as string
        line: 1-based line where variable is declared

    Returns:
        Complete variable declaration
    """
    lines = code.split('\n')
    if line < 1 or line > len(lines):
        return ""

    result = [lines[line-1]]
    idx = line

    # Check for multi-line continuations
    while idx < len(lines):
        prev_line = lines[idx-1].rstrip()

        # Continue if line ends with continuation character or opening bracket
        if prev_line.endswith(('\\', ',', '{', '[')):
            result.append(lines[idx])
            idx += 1
        else:
            break

    return '\n'.join(result)


def detect_language(file_path: str) -> str:
    """
    Detect programming language from file extension.

    Args:
        file_path: Path to source file

    Returns:
        Language name or 'unknown'
    """
    if not file_path or '.' not in file_path:
        return 'unknown'

    ext = file_path.split('.')[-1].lower()
    return LANG_MAP.get(ext, 'unknown')


def split_code_once(code: str) -> List[str]:
    """
    Split code into lines once for reuse.

    Performance optimization to avoid repeated splitting.

    Args:
        code: Source code as string

    Returns:
        List of code lines
    """
    return code.split('\n')


def find_signature_line(code: str, signature: str, hint_line: int = None) -> int:
    """
    Find line number where method signature appears in code.

    More reliable than LLM-provided line numbers. Searches for exact
    signature string with whitespace normalization.

    Args:
        code: Full source code
        signature: Method signature from LLM (e.g., "def update")
        hint_line: Optional LLM line number for disambiguation

    Returns:
        1-based line number, or None if not found
    """
    lines = code.split('\n')
    normalized_sig = ' '.join(signature.strip().split())

    matches = []
    for i, line in enumerate(lines):
        stripped = line.strip()

        # Skip comment-only lines
        if stripped.startswith('#') or stripped.startswith('//'):
            continue

        # Normalize and check for match (substring)
        normalized_line = ' '.join(stripped.split())
        if normalized_sig in normalized_line:
            matches.append(i + 1)  # Store 1-based line number

    if not matches:
        return None

    if len(matches) == 1:
        return matches[0]

    # Multiple matches - use hint_line to pick closest
    if hint_line:
        return min(matches, key=lambda x: abs(x - hint_line))

    # No hint - return first occurrence
    return matches[0]


def extract_ruby_method(code: str, start_line: int) -> str:
    """
    Extract Ruby method by counting def/end keywords.

    Handles nested blocks properly by tracking depth.
    More reliable than trusting LLM end_line for Ruby code.

    Args:
        code: Source code as string
        start_line: 1-based line where 'def' starts (may be inaccurate)

    Returns:
        Complete method code including final 'end'
    """
    lines = code.split('\n')
    if start_line < 1 or start_line > len(lines):
        return ""

    # First, find the actual 'def' line if start_line is wrong
    actual_start = start_line - 1
    for i in range(start_line - 1, min(start_line + 5, len(lines))):
        if lines[i].strip().startswith('def '):
            actual_start = i
            break

    result = []
    depth = 0

    for i in range(actual_start, len(lines)):
        line = lines[i]
        stripped = line.strip()

        # Track def/class/module/begin/do keywords (increase depth)
        if any(stripped.startswith(kw) for kw in ['def ', 'class ', 'module ', 'begin']):
            depth += 1
        elif ' do' in stripped or '\tdo' in stripped or stripped.endswith(' do'):
            depth += 1

        result.append(line)

        # Track 'end' keyword (decrease depth only if we're inside a block)
        if (stripped == 'end' or stripped.startswith('end ')) and depth > 0:
            depth -= 1
            if depth == 0:
                # Found matching end for the method
                break

    return '\n'.join(result)


def extract_brace_method(code: str, start_line: int) -> str:
    """
    Extract method/function by counting braces for C-style languages.

    Handles: JavaScript, TypeScript, Java, C, C++, C#, Go, Rust, PHP, Swift.
    Properly handles nested blocks and ignores braces in strings/comments.

    Args:
        code: Source code as string
        start_line: 1-based line where function starts

    Returns:
        Complete function code including closing brace
    """
    lines = code.split('\n')
    if start_line < 1 or start_line > len(lines):
        return ""

    result = []
    depth = 0
    started = False
    in_string = False
    in_comment = False
    string_char = None

    for i in range(start_line - 1, len(lines)):
        line = lines[i]
        result.append(line)

        # Parse character by character to handle strings/comments
        j = 0
        while j < len(line):
            char = line[j]

            # Handle single-line comments
            if not in_string and j < len(line) - 1:
                if line[j:j+2] == '//':
                    break  # Rest of line is comment
                elif line[j:j+2] == '/*':
                    in_comment = True
                    j += 2
                    continue
                elif line[j:j+2] == '*/' and in_comment:
                    in_comment = False
                    j += 2
                    continue

            # Skip if in comment
            if in_comment:
                j += 1
                continue

            # Handle strings
            if char in ['"', "'", '`'] and (j == 0 or line[j-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None

            # Count braces only outside strings/comments
            if not in_string and not in_comment:
                if char == '{':
                    depth += 1
                    started = True
                elif char == '}':
                    depth -= 1
                    if started and depth == 0:
                        # Found matching closing brace
                        return '\n'.join(result)

            j += 1

    return '\n'.join(result)
