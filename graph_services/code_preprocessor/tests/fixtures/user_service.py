"""User service fixture for E2E pipeline tests."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class User:
    """Represents an authenticated user."""

    id: str
    email: str
    name: str
    roles: list = field(default_factory=list)
    active: bool = True


class UserRepository:
    """In-memory user repository."""

    def __init__(self):
        self._users: dict[str, User] = {}

    def save(self, user: User) -> User:
        """Persist a user and return it."""
        self._users[user.id] = user
        return user

    def find_by_id(self, user_id: str) -> Optional[User]:
        """Return user by ID or None."""
        return self._users.get(user_id)

    def find_by_email(self, email: str) -> Optional[User]:
        """Return user by email or None."""
        for user in self._users.values():
            if user.email == email:
                return user
        return None


class UserService:
    """Business logic for user management."""

    def __init__(self, repo: UserRepository):
        self._repo = repo

    def create_user(self, user_id: str, email: str, name: str) -> User:
        """Create and persist a new user."""
        user = User(id=user_id, email=email, name=name)
        return self._repo.save(user)

    def deactivate(self, user_id: str) -> bool:
        """Deactivate a user. Returns True if found and deactivated."""
        user = self._repo.find_by_id(user_id)
        if user is None:
            return False
        user.active = False
        self._repo.save(user)
        return True
