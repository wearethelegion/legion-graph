"""
Parser Runner

Executes appropriate AST parser based on file extension to extract accurate line numbers.
Supports Python (libcst), Ruby (Prism), and TypeScript (ts-morph).

Author: Code Intelligence Team
"""

import asyncio
import json
import logging
import subprocess
import tempfile
from pathlib import Path
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# Global persistent TypeScript service process
_ts_service_process: Optional[asyncio.subprocess.Process] = None
_ts_service_lock = asyncio.Lock()


class ParserRunner:
    """Executes language-specific parsers to extract accurate symbol line numbers"""

    # Extension to parser mapping
    PARSER_MAP = {
        '.py': ('python3', 'parsers/parse_py.py'),
        '.rb': ('ruby', 'parsers/parse_rb.rb'),
        '.ts': ('npx', 'parsers/parse_ts.ts', 'tsx'),  # (command, script, extra_arg)
        '.tsx': ('npx', 'parsers/parse_ts.ts', 'tsx'),
        '.js': ('npx', 'parsers/parse_ts.ts', 'tsx'),
        '.jsx': ('npx', 'parsers/parse_ts.ts', 'tsx'),
    }

    # TypeScript extensions that use persistent service
    TS_EXTENSIONS = {'.ts', '.tsx', '.js', '.jsx'}

    def __init__(self, parsers_dir: Optional[Path] = None, use_persistent_ts: bool = True):
        """
        Initialize parser runner.

        Args:
            parsers_dir: Optional custom path to parsers directory
            use_persistent_ts: Use persistent TypeScript service for better performance
        """
        if parsers_dir is None:
            # Default to parsers/ directory relative to project root
            project_root = Path(__file__).parent.parent
            self.parsers_dir = project_root / 'shared' / 'parsers'
        else:
            self.parsers_dir = Path(parsers_dir)

        if not self.parsers_dir.exists():
            logger.warning(f"Parsers directory not found: {self.parsers_dir}")

        self.use_persistent_ts = use_persistent_ts

    def detect_parser(self, file_path: str) -> Optional[tuple]:
        """
        Detect appropriate parser based on file extension.
        
        Args:
            file_path: Path to source file
            
        Returns:
            Tuple of (command, parser_script_path, [extra_args]) or None if unsupported
        """
        file_ext = Path(file_path).suffix.lower()
        
        if file_ext not in self.PARSER_MAP:
            logger.debug(f"No parser available for extension: {file_ext}")
            return None
        
        parser_config = self.PARSER_MAP[file_ext]
        
        # Handle both old format (command, script) and new format (command, script, extra_arg)
        if len(parser_config) == 2:
            command, parser_script = parser_config
            extra_args = []
        else:
            command, parser_script, *extra_args = parser_config
        
        parser_path = self.parsers_dir / parser_script.split('/')[-1]
        
        if not parser_path.exists():
            logger.error(f"Parser script not found: {parser_path}")
            return None
        
        return command, parser_path, extra_args

    async def _start_ts_service(self) -> bool:
        """
        Start persistent TypeScript parser service.

        Returns:
            True if service started successfully, False otherwise
        """
        global _ts_service_process

        async with _ts_service_lock:
            # Check if already running
            if _ts_service_process is not None:
                if _ts_service_process.returncode is None:
                    return True
                # Process died, clean up
                _ts_service_process = None

            try:
                service_path = self.parsers_dir / 'parse_ts_service.ts'
                if not service_path.exists():
                    logger.error(f"TypeScript service not found: {service_path}")
                    return False

                # Start persistent service
                _ts_service_process = await asyncio.create_subprocess_exec(
                    'npx', 'tsx', str(service_path),
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=self.parsers_dir.parent
                )

                logger.info("✅ Started persistent TypeScript parser service")
                return True

            except Exception as e:
                logger.error(f"Failed to start TypeScript service: {e}", exc_info=True)
                _ts_service_process = None
                return False

    async def _parse_with_ts_service(
        self,
        temp_path: str,
        original_path: str,
        timeout: int = 30
    ) -> Optional[Dict[str, Any]]:
        """
        Parse TypeScript file using persistent service.

        Args:
            temp_path: Path to temporary source file
            original_path: Original file path (for error messages)
            timeout: Parser execution timeout in seconds

        Returns:
            Parsed JSON output or None on failure
        """
        global _ts_service_process

        # Ensure service is running
        if not await self._start_ts_service():
            logger.warning("TypeScript service unavailable, falling back to subprocess")
            return None

        try:
            # Send file path to service via stdin
            if _ts_service_process.stdin:
                _ts_service_process.stdin.write(f"{temp_path}\n".encode('utf-8'))
                await _ts_service_process.stdin.drain()

            # Read JSON response from stdout
            if _ts_service_process.stdout:
                output_bytes = await asyncio.wait_for(
                    _ts_service_process.stdout.readline(),
                    timeout=timeout
                )
                output_text = output_bytes.decode('utf-8', errors='ignore').strip()

                if not output_text:
                    logger.warning(f"TypeScript service produced empty output for: {original_path}")
                    return None

                try:
                    result = json.loads(output_text)
                except json.JSONDecodeError as e:
                    logger.error(
                        f"TypeScript service output is not valid JSON for {original_path}: {e}\n"
                        f"Output: {output_text[:500]}"
                    )
                    return None

                # Check for parser errors
                if isinstance(result, dict) and 'error' in result:
                    logger.error(f"TypeScript service error for {original_path}: {result['error']}")
                    return None

                return result

        except asyncio.TimeoutError:
            logger.error(f"TypeScript service timeout for: {original_path}")
            # Service may be hung, restart it
            async with _ts_service_lock:
                if _ts_service_process:
                    _ts_service_process.kill()
                    _ts_service_process = None
            return None
        except Exception as e:
            logger.error(f"TypeScript service error for {original_path}: {e}", exc_info=True)
            return None

    async def parse_code(
        self,
        code: str,
        file_path: str,
        timeout: int = 30
    ) -> Optional[Dict[str, Any]]:
        """
        Parse code using appropriate language parser.

        Args:
            code: Source code content
            file_path: Original file path (for extension detection)
            timeout: Parser execution timeout in seconds

        Returns:
            Parsed AST data with symbols or None on failure
        """
        parser_info = self.detect_parser(file_path)
        if not parser_info:
            logger.warning(f"Unsupported file type for parsing: {file_path}")
            return None

        command, parser_path, extra_args = parser_info

        # Create temporary file with source code
        with tempfile.NamedTemporaryFile(
            mode='w',
            suffix=Path(file_path).suffix,
            delete=False,
            encoding='utf-8'
        ) as temp_file:
            temp_file.write(code)
            temp_path = temp_file.name

        try:
            # Use persistent TypeScript service if enabled and applicable
            file_ext = Path(file_path).suffix.lower()
            if self.use_persistent_ts and file_ext in self.TS_EXTENSIONS:
                result = await asyncio.wait_for(
                    self._parse_with_ts_service(temp_path, file_path, timeout),
                    timeout=timeout
                )

                # Fall back to subprocess if service fails
                if result is None:
                    logger.info(f"Falling back to subprocess for: {file_path}")
                    result = await asyncio.wait_for(
                        self._run_parser(command, parser_path, temp_path, file_path, extra_args),
                        timeout=timeout
                    )
            else:
                # Execute parser subprocess for non-TypeScript files
                result = await asyncio.wait_for(
                    self._run_parser(command, parser_path, temp_path, file_path, extra_args),
                    timeout=timeout
                )

            if result is None:
                logger.warning(f"Parser returned no data for: {file_path}")
                return None

            # Validate parser output structure
            if not isinstance(result, dict) or 'symbols' not in result:
                logger.error(f"Invalid parser output structure for: {file_path}")
                return None

            logger.info(
                f"✅ Parsed {len(result.get('symbols', []))} symbols from {file_path} "
                f"using {command}"
            )
            return result

        except asyncio.TimeoutError:
            logger.error(f"Parser timeout ({timeout}s) for: {file_path}")
            return None
        except Exception as e:
            logger.error(f"Parser execution failed for {file_path}: {e}", exc_info=True)
            return None
        finally:
            # Clean up temporary file
            try:
                Path(temp_path).unlink()
            except Exception as e:
                logger.warning(f"Failed to clean up temp file {temp_path}: {e}")

    async def _run_parser(
        self,
        command: str,
        parser_path: Path,
        temp_path: str,
        original_path: str,
        extra_args: List[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Execute parser subprocess and capture output.
        
        Args:
            command: Parser command (python, ruby, npx)
            parser_path: Path to parser script
            temp_path: Path to temporary source file
            original_path: Original file path (for error messages)
            extra_args: Optional extra arguments (e.g., ['tsx'] for npx)
            
        Returns:
            Parsed JSON output or None on failure
        """
        try:
            # Build command
            extra_args = extra_args or []
            cmd = [command] + extra_args + [str(parser_path), temp_path]
            
            # Execute subprocess
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.parsers_dir.parent  # Run from project root
            )
            
            stdout, stderr = await process.communicate()
            
            # Check return code
            if process.returncode != 0:
                stderr_text = stderr.decode('utf-8', errors='ignore')
                logger.error(
                    f"Parser failed with code {process.returncode} for {original_path}:\n"
                    f"{stderr_text[:500]}"
                )
                return None
            
            # Parse JSON output
            output_text = stdout.decode('utf-8', errors='ignore')
            
            if not output_text.strip():
                logger.warning(f"Parser produced empty output for: {original_path}")
                return None
            
            try:
                result = json.loads(output_text)
            except json.JSONDecodeError as e:
                logger.error(
                    f"Parser output is not valid JSON for {original_path}: {e}\n"
                    f"Output (first 500 chars): {output_text[:500]}"
                )
                return None
            
            # Check for parser errors in output
            if isinstance(result, dict) and 'error' in result:
                logger.error(f"Parser error for {original_path}: {result['error']}")
                return None
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to execute parser for {original_path}: {e}", exc_info=True)
            return None

    async def shutdown(self):
        """Cleanup resources, especially persistent TypeScript service"""
        global _ts_service_process

        async with _ts_service_lock:
            if _ts_service_process is not None:
                try:
                    # Gracefully terminate the service
                    if _ts_service_process.stdin:
                        _ts_service_process.stdin.close()

                    # Wait for process to exit (with timeout)
                    try:
                        await asyncio.wait_for(_ts_service_process.wait(), timeout=2.0)
                    except asyncio.TimeoutError:
                        # Force kill if doesn't exit gracefully
                        _ts_service_process.kill()
                        await _ts_service_process.wait()

                    logger.info("✅ Stopped TypeScript parser service")
                except Exception as e:
                    logger.warning(f"Error stopping TypeScript service: {e}")
                finally:
                    _ts_service_process = None


# Convenience function for single-file parsing
async def parse_file(
    code: str,
    file_path: str,
    parsers_dir: Optional[Path] = None
) -> Optional[Dict[str, Any]]:
    """
    Convenience function to parse a single file.

    Args:
        code: Source code content
        file_path: File path (for extension detection)
        parsers_dir: Optional custom parsers directory

    Returns:
        Parsed AST data with symbols or None on failure
    """
    runner = ParserRunner(parsers_dir=parsers_dir)
    return await runner.parse_code(code, file_path)


# Cleanup function for graceful shutdown
async def shutdown_parsers():
    """Shutdown all parser services gracefully"""
    global _ts_service_process

    async with _ts_service_lock:
        if _ts_service_process is not None:
            try:
                if _ts_service_process.stdin:
                    _ts_service_process.stdin.close()

                try:
                    await asyncio.wait_for(_ts_service_process.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    _ts_service_process.kill()
                    await _ts_service_process.wait()

                logger.info("✅ Stopped TypeScript parser service")
            except Exception as e:
                logger.warning(f"Error stopping TypeScript service: {e}")
            finally:
                _ts_service_process = None
