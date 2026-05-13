"""
Email Service using Infobip.

Location: auth/services/email_service.py

Provides email verification functionality for KGRAG authentication:
- Generate cryptographically secure 6-digit verification codes
- Constant-time code verification using bcrypt
- Send verification emails via Infobip API
"""

import os
import secrets
import logging
from typing import Tuple

from infobip_api_client import ApiClient, Configuration
from infobip_api_client.api.email_api import EmailApi
from infobip_api_client import (
    EmailRequest,
    EmailMessage,
    EmailGroupDestination,
    EmailToDestination,
    EmailMessageContent,
)
from passlib.context import CryptContext

logger = logging.getLogger(__name__)

# Infobip Configuration
INFOBIP_API_KEY = os.getenv("INFOBIP_API_KEY")
INFOBIP_BASE_URL = os.getenv("INFOBIP_BASE_URL")
FROM_EMAIL = os.getenv("VERIFICATION_FROM_EMAIL", "noreply@wearethelegion.com")

# Use bcrypt for code hashing (same as password hashing pattern in jwt_utils.py)
code_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class EmailService:
    """
    Infobip email service for verification codes.

    Handles:
    - Generation of 6-digit CSPRNG verification codes
    - Secure code hashing with bcrypt
    - Constant-time code verification
    - Email delivery via Infobip
    """

    def __init__(self) -> None:
        """Initialize EmailService with Infobip client."""
        if INFOBIP_API_KEY and INFOBIP_BASE_URL:
            config = Configuration(host=INFOBIP_BASE_URL, api_key={"APIKeyHeader": INFOBIP_API_KEY})
            api_client = ApiClient(config)
            self.email_api = EmailApi(api_client)
        else:
            self.email_api = None
            logger.warning(
                "INFOBIP_API_KEY or INFOBIP_BASE_URL not configured - email sending disabled"
            )

    def generate_verification_code(self) -> Tuple[str, str]:
        """
        Generate a 6-digit verification code.

        Uses secrets.randbelow() for cryptographically secure random generation.
        Code is zero-padded to ensure 6 digits (e.g., "000123").

        Returns:
            Tuple[str, str]: (plain_code, hashed_code)
                - plain_code: The 6-digit code to send to user
                - hashed_code: bcrypt hash to store in database
        """
        # CSPRNG - cryptographically secure random number generation
        code = str(secrets.randbelow(1000000)).zfill(6)
        hashed = code_context.hash(code)
        return code, hashed

    def verify_code(self, plain_code: str, hashed_code: str) -> bool:
        """
        Verify a verification code against its hash.

        Uses bcrypt's constant-time comparison to prevent timing attacks.

        Args:
            plain_code: The 6-digit code entered by user
            hashed_code: The bcrypt hash stored in database

        Returns:
            bool: True if code matches, False otherwise
        """
        return code_context.verify(plain_code, hashed_code)

    async def send_verification_email(self, to_email: str, code: str, username: str) -> bool:
        """
        Send verification email via Infobip.

        Args:
            to_email: Recipient's email address
            code: 6-digit verification code (plain text)
            username: User's display name for personalization

        Returns:
            bool: True if email sent successfully (status PENDING),
                  False otherwise
        """
        if not self.email_api:
            logger.error("Cannot send email: Infobip client not initialized")
            return False

        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
             line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
    <h2 style="color: #2563eb;">Welcome to KGRAG, {username}!</h2>

    <p>Your verification code is:</p>

    <div style="background: #f3f4f6; border-radius: 8px; padding: 24px;
                text-align: center; margin: 24px 0;">
        <span style="font-size: 36px; letter-spacing: 8px; font-family: 'SF Mono', Monaco,
                     'Courier New', monospace; font-weight: bold; color: #1f2937;">
            {code}
        </span>
    </div>

    <p style="color: #6b7280; font-size: 14px;">
        This code expires in <strong>15 minutes</strong>.
    </p>

    <hr style="border: none; border-top: 1px solid #e5e7eb; margin: 24px 0;">

    <p style="color: #9ca3af; font-size: 12px;">
        If you didn't create a KGRAG account, you can safely ignore this email.
    </p>
</body>
</html>
"""

        try:
            # Infobip SDK v5.0+ uses EmailRequest with nested objects
            email_request = EmailRequest(
                messages=[
                    EmailMessage(
                        sender=FROM_EMAIL,
                        destinations=[
                            EmailGroupDestination(to=[EmailToDestination(destination=to_email)])
                        ],
                        content=EmailMessageContent(
                            subject="Verify your KGRAG account", html=html_content
                        ),
                    )
                ]
            )
            response = self.email_api.send_email(email_request)

            # Check success: response.messages[0].status.group_name == "PENDING"
            try:
                status_group = response.messages[0].status.group_name
                success = status_group == "PENDING"
            except (AttributeError, IndexError, TypeError):
                # Response structure unexpected
                success = False

            if success:
                logger.info(f"Verification email sent to {to_email}")
            else:
                logger.warning(f"Infobip returned unexpected response for {to_email}: {response}")

            return success

        except Exception as e:
            logger.error(f"Infobip error sending to {to_email}: {e}")
            return False


# Singleton instance for dependency injection
_email_service: EmailService | None = None


def get_email_service() -> EmailService:
    """
    Get or create EmailService singleton.

    Use with FastAPI Depends:
        email_service: EmailService = Depends(get_email_service)
    """
    global _email_service
    if _email_service is None:
        _email_service = EmailService()
    return _email_service
