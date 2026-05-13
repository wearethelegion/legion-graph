"""Sensitive data logging filter to prevent credential leakage."""
import re
import logging
from typing import List, Tuple


class SensitiveDataFilter(logging.Filter):
    """Filter that redacts sensitive data from log messages."""

    SENSITIVE_PATTERNS: List[Tuple[str, str]] = [
        # Verification codes (6 digits after key patterns)
        (r'(code|verification_code)["\s:=]+\d{6}', r'\1=[REDACTED]'),
        # TOTP secrets (32-char Base32)
        (r'(secret|totp_secret)["\s:=]+[A-Z2-7]{32}', r'\1=[REDACTED]'),
        # Backup codes (12 chars with dashes)
        (r'(backup_code)["\s:=]+[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}', r'\1=[REDACTED]'),
        # OAuth tokens
        (r'(access_token|refresh_token)["\s:=]+[^\s"]+', r'\1=[REDACTED]'),
        # Passwords
        (r'(password|password_hash)["\s:=]+[^\s"]+', r'\1=[REDACTED]'),
    ]

    def filter(self, record: logging.LogRecord) -> bool:
        """Redact sensitive data from log record message."""
        if isinstance(record.msg, str):
            for pattern, replacement in self.SENSITIVE_PATTERNS:
                record.msg = re.sub(pattern, replacement, record.msg, flags=re.IGNORECASE)
        return True


def configure_sensitive_logging() -> None:
    """Apply sensitive data filter to all auth-related loggers."""
    sensitive_filter = SensitiveDataFilter()
    # Apply to root and auth loggers
    for logger_name in ['', 'auth', 'uvicorn', 'fastapi']:
        logger = logging.getLogger(logger_name)
        logger.addFilter(sensitive_filter)
