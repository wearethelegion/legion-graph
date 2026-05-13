"""
KGRAG Auth Service - TOTP 2FA Service
RFC 6238 compliant TOTP implementation with Fernet encryption.

Security requirements (from Ragen's review):
- TOTP secrets MUST be encrypted at rest using Fernet
- Backup codes are bcrypt hashed, single-use
- Never log TOTP secrets or codes
"""

import io
import os
import base64
import secrets
import string
import logging
from typing import List, Tuple, Optional

import pyotp
import qrcode
from cryptography.fernet import Fernet, InvalidToken
from passlib.context import CryptContext

logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

TOTP_ISSUER = os.getenv("TOTP_ISSUER", "KGRAG")

# Fernet instance for TOTP encryption - lazy initialized
_fernet: Optional[Fernet] = None


def _get_fernet() -> Fernet:
    """
    Get or initialize the Fernet encryption instance.

    Lazy initialization allows env vars to be set after module import (useful for tests).

    Returns:
        Fernet instance for encryption/decryption

    Raises:
        ValueError: If TOTP_ENCRYPTION_KEY is not configured or invalid
    """
    global _fernet

    if _fernet is not None:
        return _fernet

    encryption_key = os.getenv("TOTP_ENCRYPTION_KEY")
    if not encryption_key:
        raise ValueError(
            "TOTP_ENCRYPTION_KEY not configured. "
            "Generate with: python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'"
        )

    try:
        _fernet = Fernet(encryption_key.encode())
        return _fernet
    except Exception as e:
        logger.error(f"Invalid TOTP_ENCRYPTION_KEY: {e}")
        raise ValueError("TOTP_ENCRYPTION_KEY is invalid") from e

# Backup codes use bcrypt (same as password hashing pattern from jwt_utils.py)
backup_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def _reset_fernet() -> None:
    """Reset Fernet instance. Used for testing when env vars change between tests."""
    global _fernet
    _fernet = None


# =============================================================================
# Encryption Functions (Ragen's CRITICAL requirement)
# =============================================================================

def encrypt_totp_secret(secret: str) -> str:
    """
    Encrypt TOTP secret using Fernet symmetric encryption.

    Args:
        secret: Plain Base32 TOTP secret

    Returns:
        Encrypted secret (base64 encoded)

    Raises:
        ValueError: If encryption key is not configured
    """
    fernet = _get_fernet()
    return fernet.encrypt(secret.encode()).decode()


def decrypt_totp_secret(encrypted: str) -> str:
    """
    Decrypt TOTP secret.

    Args:
        encrypted: Fernet-encrypted secret (base64 encoded)

    Returns:
        Plain Base32 TOTP secret

    Raises:
        ValueError: If decryption fails or key is not configured
    """
    fernet = _get_fernet()

    try:
        return fernet.decrypt(encrypted.encode()).decode()
    except InvalidToken as e:
        logger.error("Failed to decrypt TOTP secret - invalid token")
        raise ValueError("Failed to decrypt TOTP secret") from e


# =============================================================================
# TOTP Service Class
# =============================================================================

class TOTPService:
    """
    TOTP 2FA operations service.

    All secrets are encrypted at rest using Fernet.
    Backup codes are bcrypt hashed.

    Usage:
        service = TOTPService()

        # Setup flow
        secret = service.generate_secret()
        encrypted = encrypt_totp_secret(secret)  # Store this in DB
        uri = service.get_provisioning_uri(secret, "user@example.com")
        qr = service.generate_qr_code(uri)

        # Verification flow
        decrypted = decrypt_totp_secret(stored_encrypted_secret)
        is_valid = service.verify_totp(decrypted, user_code)
    """

    def __init__(self, issuer: str = TOTP_ISSUER):
        """
        Initialize TOTP service.

        Args:
            issuer: Application name shown in authenticator apps
        """
        self.issuer = issuer

    def generate_secret(self) -> str:
        """
        Generate 32-character Base32 TOTP secret.

        Uses pyotp's CSPRNG-based generator (os.urandom internally).

        Returns:
            32-char Base32 encoded secret (160 bits entropy)
        """
        return pyotp.random_base32(32)

    def get_provisioning_uri(self, secret: str, email: str) -> str:
        """
        Generate otpauth:// URI for authenticator apps.

        Args:
            secret: Plain Base32 TOTP secret (NOT encrypted)
            email: User's email address (used as account name)

        Returns:
            otpauth://totp/ISSUER:email?secret=XXX&issuer=ISSUER
        """
        totp = pyotp.TOTP(secret)
        return totp.provisioning_uri(
            name=email,
            issuer_name=self.issuer
        )

    def generate_qr_code(self, provisioning_uri: str) -> str:
        """
        Generate QR code as base64-encoded PNG.

        Args:
            provisioning_uri: otpauth:// URI from get_provisioning_uri

        Returns:
            Base64-encoded PNG image data
        """
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_L,
            box_size=10,
            border=4
        )
        qr.add_data(provisioning_uri)
        qr.make(fit=True)

        img = qr.make_image(fill_color="black", back_color="white")
        buffer = io.BytesIO()
        img.save(buffer, format="PNG")
        buffer.seek(0)

        return base64.b64encode(buffer.getvalue()).decode()

    def verify_totp(
        self,
        secret: str,
        code: str,
        valid_window: int = 1
    ) -> bool:
        """
        Verify TOTP code against secret.

        Args:
            secret: Plain Base32 TOTP secret (NOT encrypted)
            code: 6-digit code from authenticator app
            valid_window: Number of time steps to check before/after current
                          (default: 1 = allows +-30 seconds drift)

        Returns:
            True if code is valid, False otherwise
        """
        if not secret or not code:
            return False

        # Sanitize code input
        code = code.strip().replace(" ", "")

        # Validate code format
        if not code.isdigit() or len(code) != 6:
            return False

        try:
            totp = pyotp.TOTP(secret)
            return totp.verify(code, valid_window=valid_window)
        except Exception as e:
            # Log error but don't expose details
            logger.error(f"TOTP verification error: {type(e).__name__}")
            return False

    def generate_backup_codes(self, count: int = 10) -> Tuple[List[str], List[str]]:
        """
        Generate backup codes for recovery.

        Generates alphanumeric codes formatted as XXXX-XXXX-XXXX.
        Plain codes are shown to user once; hashed codes are stored.

        Args:
            count: Number of backup codes to generate (default: 10)

        Returns:
            Tuple of (plain_codes, hashed_codes)
            - plain_codes: Formatted codes to display to user (once)
            - hashed_codes: Bcrypt hashed codes to store in database
        """
        # Use uppercase letters + digits (excludes confusing chars like 0/O, 1/I)
        alphabet = string.ascii_uppercase + string.digits
        # Remove potentially confusing characters
        alphabet = alphabet.replace("O", "").replace("0", "").replace("I", "").replace("1", "")

        plain_codes: List[str] = []
        hashed_codes: List[str] = []

        for _ in range(count):
            # Generate 12 random characters
            code = "".join(secrets.choice(alphabet) for _ in range(12))

            # Format as XXXX-XXXX-XXXX for display
            formatted = f"{code[:4]}-{code[4:8]}-{code[8:]}"
            plain_codes.append(formatted)

            # Hash the raw code (without dashes) for storage
            hashed_codes.append(backup_context.hash(code))

        return plain_codes, hashed_codes

    def verify_backup_code(
        self,
        code: str,
        hashed_codes: List[str]
    ) -> Tuple[bool, int]:
        """
        Verify a backup code against stored hashes.

        Backup codes are single-use. The index returned should be used
        to remove the code from storage.

        Args:
            code: User-provided backup code (with or without dashes)
            hashed_codes: List of bcrypt-hashed backup codes from database

        Returns:
            Tuple of (is_valid, index)
            - is_valid: True if code matches any stored hash
            - index: Index of matched code (-1 if no match)
                     Use this to remove the code from storage
        """
        if not code or not hashed_codes:
            return False, -1

        # Normalize: remove dashes, convert to uppercase
        code_clean = code.replace("-", "").replace(" ", "").upper()

        # Validate format
        if not code_clean.isalnum() or len(code_clean) != 12:
            return False, -1

        # Check against each stored hash
        # Note: This has timing variation based on match position (low risk per security review)
        for i, hashed in enumerate(hashed_codes):
            try:
                if backup_context.verify(code_clean, hashed):
                    return True, i
            except Exception:
                # Skip invalid hash entries
                continue

        return False, -1


# =============================================================================
# Module-level convenience functions
# =============================================================================

# Default service instance
_default_service: Optional[TOTPService] = None


def get_totp_service() -> TOTPService:
    """Get or create the default TOTPService instance."""
    global _default_service
    if _default_service is None:
        _default_service = TOTPService()
    return _default_service
