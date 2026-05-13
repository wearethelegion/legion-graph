"""
Line Number Corrector

Corrects LLM-generated line numbers by matching symbols with AST parser output.
Injects accurate line numbers from parsers (libcst, Prism, ts-morph) into AI analysis.

Author: Code Intelligence Team
"""

import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from difflib import SequenceMatcher

from shared.parser_runner import ParserRunner

logger = logging.getLogger(__name__)


@dataclass
class LineMapping:
    """Represents a symbol's line number mapping from parser"""
    name: str
    fqn: str
    start_line: int
    end_line: int
    kind: str  # function, class, method, etc.
    nested_symbols: List[Dict[str, Any]] = None  # Nested functions/blocks from AST parser
    
    def __post_init__(self):
        if self.nested_symbols is None:
            self.nested_symbols = []  # function, class, method, etc.


@dataclass
class CorrectionStats:
    """Statistics for correction operation"""
    total_symbols: int = 0
    matched_symbols: int = 0
    unmatched_symbols: int = 0
    corrected_fields: int = 0
    
    @property
    def match_rate(self) -> float:
        """Calculate match percentage"""
        if self.total_symbols == 0:
            return 0.0
        return (self.matched_symbols / self.total_symbols) * 100


class LineNumberCorrector:
    """
    Corrects LLM-generated line numbers using accurate AST parser data.
    
    Workflow:
    1. Parse source file with appropriate parser (Python/Ruby/TypeScript)
    2. Build symbol name → line number mapping from parser output
    3. Match LLM symbols to parser symbols by name
    4. Correct all line number fields in LLM analysis
    """

    def __init__(self, parser_runner: Optional[ParserRunner] = None):
        """
        Initialize line number corrector.
        
        Args:
            parser_runner: Optional custom ParserRunner instance
        """
        self.parser_runner = parser_runner or ParserRunner()
        self.stats = CorrectionStats()

    async def correct_line_numbers(
        self,
        document: Any,  # AIEnrichedDocument
        source_code: str,
        file_path: str
    ) -> Any:
        """
        Correct line numbers in AIEnrichedDocument using AST parser data.

        V2 documents with chunks (SCIP-based) already have accurate line numbers,
        so correction is skipped for V2.

        Also injects nested symbols from AST for intelligent chunking.

        Args:
            document: AIEnrichedDocument with LLM analysis
            source_code: Original source code
            file_path: File path (for parser selection)

        Returns:
            Document with corrected line numbers and nested symbols
        """
        # Reset stats
        self.stats = CorrectionStats()

        # V2 documents use SCIP which provides accurate line numbers - skip correction
        if hasattr(document, 'chunks') and getattr(document, 'chunks', None):
            logger.info(f"✓ Skipping line correction for V2 document: {file_path} (SCIP-based)")
            return document
        
        # Parse source code with appropriate parser
        parser_result = await self.parser_runner.parse_code(
            code=source_code,
            file_path=file_path
        )
        
        if not parser_result:
            logger.warning(
                f"Parser failed for {file_path}, keeping LLM line numbers (fallback mode)"
            )
            return document
        
        # Build symbol → line mapping from parser output
        line_mappings = self._build_line_mappings(parser_result)
        
        if not line_mappings:
            logger.warning(
                f"No symbols extracted from parser for {file_path}, "
                f"keeping LLM line numbers"
            )
            return document
        
        logger.info(
            f"📊 Built {len(line_mappings)} line mappings from parser for {file_path}"
        )
        
        # Correct line numbers in document
        self._correct_document_lines(document, line_mappings)
        
        # Inject nested symbols into methods for intelligent chunking
        self._inject_nested_symbols(document, line_mappings)
        
        # Log correction statistics
        logger.info(
            f"✅ Line correction complete for {file_path}: "
            f"{self.stats.matched_symbols}/{self.stats.total_symbols} symbols matched "
            f"({self.stats.match_rate:.1f}%), "
            f"{self.stats.corrected_fields} fields corrected"
        )
        
        if self.stats.unmatched_symbols > 0:
            logger.warning(
                f"⚠️ {self.stats.unmatched_symbols} symbols unmatched in {file_path}, "
                f"kept LLM line numbers"
            )
        
        return document

    def _build_line_mappings(
        self,
        parser_result: Dict[str, Any]
    ) -> Dict[str, LineMapping]:
        """
        Build symbol name → line mapping from parser output.
        
        Also stores nested symbols for intelligent chunking.
        
        Args:
            parser_result: Parser output with symbols
            
        Returns:
            Dictionary mapping symbol names to LineMapping objects
        """
        mappings: Dict[str, LineMapping] = {}
        symbols = parser_result.get('symbols', [])
        
        for symbol in symbols:
            name = symbol.get('name')
            span = symbol.get('span', {})
            
            if not name or not span:
                continue
            
            # Extract line numbers (handle different parser output formats)
            start_line = span.get('start_line') or span.get('start', {}).get('line')
            end_line = span.get('end_line') or span.get('end', {}).get('line')
            
            if start_line is None or end_line is None:
                logger.debug(f"Skipping symbol without line numbers: {name}")
                continue
            
            # Create mapping
            mapping = LineMapping(
                name=name,
                fqn=symbol.get('fqn', '') or symbol.get('symbol_fqn', ''),
                start_line=start_line,
                end_line=end_line,
                kind=symbol.get('kind', 'unknown')
            )
            
            # Store nested symbols for intelligent chunking
            nested_symbols = symbol.get('nested_symbols', [])
            if nested_symbols:
                mapping.nested_symbols = nested_symbols
            
            # Store by name (primary key) and FQN (secondary key)
            mappings[name] = mapping
            if mapping.fqn:
                mappings[mapping.fqn] = mapping
        
        return mappings

    def _correct_document_lines(
        self,
        document: Any,
        line_mappings: Dict[str, LineMapping]
    ) -> None:
        """
        Correct all line number fields in document.
        
        Args:
            document: AIEnrichedDocument to correct
            line_mappings: Symbol name → LineMapping dictionary
        """
        # Access declared_elements (different attribute access patterns)
        declared_elements = None
        if hasattr(document, 'declared_elements'):
            declared_elements = document.declared_elements
        elif isinstance(document, dict):
            declared_elements = document.get('declared_elements', {})
        
        if not declared_elements:
            logger.warning("No declared_elements found in document")
            return
        
        # Correct each category of symbols
        self._correct_symbols(declared_elements, line_mappings)
        self._correct_methods(declared_elements, line_mappings)
        self._correct_instance_fields(declared_elements, line_mappings)
        self._correct_internal_calls(declared_elements, line_mappings)
        self._correct_control_blocks(declared_elements, line_mappings)
        
        # Correct external dependencies if present
        external_deps = None
        if hasattr(document, 'external_dependencies'):
            external_deps = document.external_dependencies
        elif isinstance(document, dict):
            external_deps = document.get('external_dependencies', {})
        
        if external_deps:
            self._correct_external_symbols(external_deps, line_mappings)
        
        # Correct side effects if present
        side_effects = None
        if hasattr(document, 'declared_elements'):
            side_effects = getattr(document.declared_elements, 'side_effects', None)
        elif isinstance(declared_elements, dict):
            side_effects = declared_elements.get('side_effects', [])
        
        if side_effects:
            self._correct_side_effects(side_effects, line_mappings)

    def _correct_symbols(
        self,
        declared_elements: Any,
        line_mappings: Dict[str, LineMapping]
    ) -> None:
        """Correct declared_elements.symbols[].line"""
        symbols = self._get_list(declared_elements, 'symbols')
        if not symbols:
            return
        
        for symbol in symbols:
            self.stats.total_symbols += 1
            name = symbol.get('name')
            
            if not name:
                continue
            
            mapping = self._match_symbol(name, line_mappings)
            if mapping:
                self._update_field(symbol, 'line', mapping.start_line)
                self.stats.matched_symbols += 1
            else:
                self.stats.unmatched_symbols += 1
                logger.debug(f"Unmatched symbol: {name}")

    def _correct_methods(
        self,
        declared_elements: Any,
        line_mappings: Dict[str, LineMapping]
    ) -> None:
        """Correct declared_elements.methods[].start_line and end_line"""
        methods = self._get_list(declared_elements, 'methods')
        if not methods:
            return
        
        for method in methods:
            self.stats.total_symbols += 1
            name = method.get('name')
            
            if not name:
                continue
            
            mapping = self._match_symbol(name, line_mappings)
            if mapping:
                self._update_field(method, 'start_line', mapping.start_line)
                self._update_field(method, 'end_line', mapping.end_line)
                # Also update 'line' if present
                if 'line' in method:
                    self._update_field(method, 'line', mapping.start_line)
                self.stats.matched_symbols += 1
            else:
                self.stats.unmatched_symbols += 1
                logger.debug(f"Unmatched method: {name}")

    def _correct_instance_fields(
        self,
        declared_elements: Any,
        line_mappings: Dict[str, LineMapping]
    ) -> None:
        """Correct declared_elements.instance_fields_variables[].line"""
        fields = self._get_list(declared_elements, 'instance_fields_variables')
        if not fields:
            return
        
        for field in fields:
            self.stats.total_symbols += 1
            name = field.get('name')
            
            if not name:
                continue
            
            # Try matching without @ prefix for instance variables
            clean_name = name.lstrip('@')
            mapping = self._match_symbol(clean_name, line_mappings) or self._match_symbol(name, line_mappings)
            
            if mapping:
                self._update_field(field, 'line', mapping.start_line)
                self.stats.matched_symbols += 1
            else:
                self.stats.unmatched_symbols += 1
                logger.debug(f"Unmatched field: {name}")

    def _correct_internal_calls(
        self,
        declared_elements: Any,
        line_mappings: Dict[str, LineMapping]
    ) -> None:
        """Correct declared_elements.internal_methods_calls[].line"""
        calls = self._get_list(declared_elements, 'internal_methods_calls')
        if not calls:
            return
        
        for call in calls:
            caller = call.get('caller')
            
            if not caller:
                continue
            
            mapping = self._match_symbol(caller, line_mappings)
            if mapping:
                # Calls should be within the caller's range
                # Keep original line if available, otherwise use start
                if 'line' not in call or call['line'] is None:
                    self._update_field(call, 'line', mapping.start_line)
                    self.stats.corrected_fields += 1

    def _correct_control_blocks(
        self,
        declared_elements: Any,
        line_mappings: Dict[str, LineMapping]
    ) -> None:
        """Correct declared_elements.control_blocks[].start_line and end_line"""
        blocks = self._get_list(declared_elements, 'control_blocks')
        if not blocks:
            return
        
        for block in blocks:
            containing_method = block.get('containing_method')
            
            if not containing_method:
                continue
            
            mapping = self._match_symbol(containing_method, line_mappings)
            if mapping:
                # Control blocks should be within method range
                # Validate current lines are within bounds, otherwise adjust
                start = block.get('start_line')
                end = block.get('end_line')
                
                if start and (start < mapping.start_line or start > mapping.end_line):
                    logger.debug(
                        f"Control block start_line {start} out of bounds "
                        f"for {containing_method} [{mapping.start_line}-{mapping.end_line}]"
                    )

    def _correct_external_symbols(
        self,
        external_deps: Any,
        line_mappings: Dict[str, LineMapping]
    ) -> None:
        """Correct external_dependencies.symbols[].line"""
        symbols = self._get_list(external_deps, 'symbols')
        if not symbols:
            return
        
        for symbol in symbols:
            name = symbol.get('name')
            
            if not name:
                continue
            
            mapping = self._match_symbol(name, line_mappings)
            if mapping:
                self._update_field(symbol, 'line', mapping.start_line)
                self.stats.corrected_fields += 1

    def _correct_side_effects(
        self,
        side_effects: List[Dict[str, Any]],
        line_mappings: Dict[str, LineMapping]
    ) -> None:
        """Correct side_effects[].start_line and end_line"""
        if not side_effects:
            return
        
        for effect in side_effects:
            name = effect.get('name')
            
            if not name:
                continue
            
            mapping = self._match_symbol(name, line_mappings)
            if mapping:
                self._update_field(effect, 'start_line', mapping.start_line)
                self._update_field(effect, 'end_line', mapping.end_line)
                self.stats.corrected_fields += 1

    def _inject_nested_symbols(
        self,
        document: Any,
        line_mappings: Dict[str, LineMapping]
    ) -> None:
        """
        Inject nested symbols from AST parser into methods for intelligent chunking.
        
        This allows the chunker to access nested functions, try/catch blocks, etc.
        for size-based splitting decisions.
        
        Args:
            document: AIEnrichedDocument to enhance
            line_mappings: Symbol name → LineMapping dictionary with nested symbols
        """
        # Access declared_elements
        declared_elements = None
        if hasattr(document, 'declared_elements'):
            declared_elements = document.declared_elements
        elif isinstance(document, dict):
            declared_elements = document.get('declared_elements', {})
        
        if not declared_elements:
            return
        
        methods = self._get_list(declared_elements, 'methods')
        if not methods:
            return
        
        nested_injected = 0
        
        for method in methods:
            name = method.get('name')
            if not name:
                continue
            
            # Find matching line mapping with nested symbols
            mapping = self._match_symbol(name, line_mappings)
            if mapping and mapping.nested_symbols:
                # Inject nested symbols into method
                method['nested_symbols'] = mapping.nested_symbols
                nested_injected += 1
                logger.debug(
                    f"💉 Injected {len(mapping.nested_symbols)} nested symbols "
                    f"into {name}"
                )
        
        if nested_injected > 0:
            logger.info(
                f"✅ Injected nested symbols into {nested_injected} methods "
                f"for intelligent chunking"
            )

    def _match_symbol(
        self,
        name: str,
        line_mappings: Dict[str, LineMapping]
    ) -> Optional[LineMapping]:
        """
        Match symbol name to line mapping using multiple strategies.
        
        Strategies:
        1. Direct name match
        2. FQN match
        3. Case-insensitive match
        4. Fuzzy match (similarity > 0.9)
        
        Args:
            name: Symbol name to match
            line_mappings: Available line mappings
            
        Returns:
            LineMapping if match found, None otherwise
        """
        if not name:
            return None
        
        # Strategy 1: Direct name match
        if name in line_mappings:
            return line_mappings[name]
        
        # Strategy 2: Case-insensitive match
        name_lower = name.lower()
        for key, mapping in line_mappings.items():
            if key.lower() == name_lower:
                return mapping
        
        # Strategy 3: Fuzzy match (for similar names like camelCase vs snake_case)
        best_match = None
        best_ratio = 0.85  # Minimum similarity threshold
        
        for key, mapping in line_mappings.items():
            ratio = SequenceMatcher(None, name.lower(), key.lower()).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_match = mapping
        
        if best_match:
            logger.debug(f"Fuzzy matched '{name}' with ratio {best_ratio:.2f}")
            return best_match
        
        return None

    def _get_list(self, obj: Any, key: str) -> List[Dict[str, Any]]:
        """Safely get list from object (handles both dict and attribute access)"""
        if hasattr(obj, key):
            result = getattr(obj, key)
        elif isinstance(obj, dict):
            result = obj.get(key, [])
        else:
            return []
        
        return result if isinstance(result, list) else []

    def _update_field(self, obj: Dict[str, Any], field: str, value: int) -> None:
        """Update field and increment stats"""
        if field in obj:
            obj[field] = value
            self.stats.corrected_fields += 1
