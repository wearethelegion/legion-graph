"""Git repository utilities used by the Kafka consumer."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Protocol
import asyncpg
import asyncio

from git import Repo, exc

logger = logging.getLogger(__name__)


@dataclass
class GitChange:
    """Represents a single file change between two commits."""

    change_type: str
    file_path: str
    previous_path: Optional[str] = None


@dataclass
class GitDiffResult:
    """Represents the result of synchronising a repository."""

    repository: str
    branch: str
    repo_path: Path
    old_commit: Optional[str]
    new_commit: Optional[str]
    changes: List[GitChange]
    is_initial_clone: bool = False
    force_full_refresh: bool = False


class GitRepositorySettings(Protocol):
    repo_storage_root: Path
    github_base_url: str
    github_token: str
    default_branch: str


class GitRepositoryManager:
    """Manage local clones and compute diffs between updates."""

    def __init__(self, settings: GitRepositorySettings, db_pool: Optional[asyncpg.Pool] = None) -> None:
        self._settings = settings
        self._storage_root = settings.repo_storage_root
        self._storage_root.mkdir(parents=True, exist_ok=True)
        self._db_pool = db_pool

    async def _get_project_github_token(self, project_id: str) -> Optional[str]:
        """
        Fetch github_token for a project from database.

        Args:
            project_id: Project UUID

        Returns:
            GitHub token or None if not configured
        """
        if not self._db_pool:
            logger.debug("No database pool configured, cannot fetch project token")
            return None

        query = "SELECT github_token FROM projects WHERE id = $1"

        try:
            async with self._db_pool.acquire() as conn:
                row = await conn.fetchrow(query, project_id)
                return row["github_token"] if row else None
        except Exception as exc:
            logger.error("Failed to fetch github_token for project %s: %s", project_id, exc)
            return None

    def sync_repository(
        self,
        repository: str,
        branch: Optional[str] = None,
        github_token: Optional[str] = None,
        *,
        force_full_refresh: bool = False,
    ) -> GitDiffResult:
        """Clone or update the repository and return the commit diff."""
        remote_url = self._build_remote_url(repository, github_token)
        resolved_branch = None
        if not branch:
            resolved_branch = self._determine_default_branch(remote_url)
        branch_name = branch or resolved_branch or self._settings.default_branch
        if resolved_branch:
            logger.debug("Resolved default branch for %s: %s", repository, resolved_branch)
        repo_path = self._storage_root / self._sanitize_repository_name(repository)

        if self._is_valid_repo(repo_path):
            self._cleanup_git_lock_files(repo_path)
            logger.info("Updating repository %s (branch=%s)", repository, branch_name)
            repo = Repo(repo_path)
            return self._update_existing_repo(
                repo,
                repository,
                branch_name,
                force_full_refresh=force_full_refresh,
            )

        if repo_path.exists():
            logger.warning(
                "Removing stale repository directory before cloning %s into %s",
                repository,
                repo_path,
            )
            self._cleanup_repository_path(repo_path)

        logger.info("Cloning repository %s into %s", repository, repo_path)
        is_initial = True
        repo = self._clone_repository(remote_url, repo_path, branch_name)
        new_commit = repo.head.commit.hexsha if repo.head.is_valid() else None
        changes = self._collect_initial_changes(repo)
        return GitDiffResult(
            repository=repository,
            branch=branch_name,
            repo_path=repo_path,
            old_commit=None,
            new_commit=new_commit,
            changes=changes,
            is_initial_clone=is_initial,
            force_full_refresh=force_full_refresh,
        )

    def _clone_repository(self, remote_url: str, path: Path, branch: str) -> Repo:
        """Clone the repository to the provided path."""
        try:
            return Repo.clone_from(
                remote_url,
                to_path=path,
                branch=branch,
                env=self._git_env(),
            )
        except exc.GitCommandError as git_error:
            logger.error("Failed to clone %s: %s", remote_url, git_error)
            self._cleanup_repository_path(path)
            raise

    def _update_existing_repo(
        self,
        repo: Repo,
        repository: str,
        branch: str,
        *,
        force_full_refresh: bool = False,
    ) -> GitDiffResult:
        """Fetch latest changes and return diff between commits."""
        worktree_path = Path(repo.working_tree_dir or "")
        self._cleanup_git_lock_files(worktree_path)
        old_commit = repo.head.commit.hexsha if repo.head.is_valid() else None

        try:
            repo.git.fetch("origin", branch)
        except exc.GitCommandError as git_error:
            logger.error("Failed to fetch updates for %s: %s", repository, git_error)
            raise

        # Ensure we are on the requested branch
        try:
            repo.git.checkout(branch)
        except exc.GitCommandError as checkout_error:
            logger.debug(
                "Checkout for %s failed (%s); recreating local branch from origin",
                branch,
                checkout_error,
            )
            try:
                repo.git.checkout("-B", branch, f"origin/{branch}")
            except exc.GitCommandError as recreate_error:
                logger.error(
                    "Failed to checkout branch %s for %s after retry: %s",
                    branch,
                    repository,
                    recreate_error,
                )
                raise

        try:
            repo.git.pull("origin", branch)
        except exc.GitCommandError as git_error:
            logger.error("Failed to pull updates for %s: %s", repository, git_error)
            raise

        new_commit = repo.head.commit.hexsha if repo.head.is_valid() else None

        repo_path = Path(repo.working_tree_dir or "")

        if (not old_commit or not new_commit or old_commit == new_commit) and not force_full_refresh:
            return GitDiffResult(
                repository=repository,
                branch=branch,
                repo_path=repo_path,
                old_commit=old_commit,
                new_commit=new_commit,
                changes=[],
                is_initial_clone=False,
                force_full_refresh=False,
            )

        changes: List[GitChange] = []
        if old_commit and new_commit and old_commit != new_commit:
            try:
                diff_output = repo.git.diff("--name-status", f"{old_commit}..{new_commit}")
                changes = self._parse_diff_output(diff_output)
            except exc.GitCommandError as git_error:
                logger.error("Failed to compute diff for %s: %s", repository, git_error)
                raise

        if force_full_refresh:
            try:
                tracked_files = self._collect_all_files(repo, change_type="M")
            except exc.GitCommandError as git_error:
                logger.error("Failed to list files for full refresh %s: %s", repository, git_error)
                raise

            existing_paths = {
                change.file_path
                for change in changes
                if not change.change_type.upper().startswith("D")
            }
            for tracked_change in tracked_files:
                if tracked_change.file_path not in existing_paths:
                    changes.append(tracked_change)

        return GitDiffResult(
            repository=repository,
            branch=branch,
            repo_path=repo_path,
            old_commit=old_commit,
            new_commit=new_commit,
            changes=changes,
            is_initial_clone=False,
            force_full_refresh=force_full_refresh,
        )

    def _collect_initial_changes(self, repo: Repo) -> List[GitChange]:
        """Collect all tracked files as added changes for initial clone."""
        return self._collect_all_files(repo, change_type="A")

    def _collect_all_files(self, repo: Repo, *, change_type: str) -> List[GitChange]:
        """Collect all tracked files and emit them with the provided change code."""
        try:
            files = repo.git.ls_files().splitlines()
        except exc.GitCommandError as git_error:
            logger.error("Failed to list tracked files: %s", git_error)
            raise

        return [
            GitChange(change_type=change_type, file_path=file_path)
            for file_path in files
            if file_path
        ]

    @staticmethod
    def _parse_diff_output(diff_output: str) -> List[GitChange]:
        changes: List[GitChange] = []
        for line in diff_output.splitlines():
            line = line.strip()
            if not line:
                continue

            parts = line.split("\t")
            change_code = parts[0]

            if change_code.startswith("R") and len(parts) >= 3:
                changes.append(
                    GitChange(
                        change_type="R",
                        file_path=parts[2],
                        previous_path=parts[1],
                    )
                )
            elif len(parts) >= 2:
                changes.append(
                    GitChange(
                        change_type=change_code,
                        file_path=parts[1],
                    )
                )
        return changes

    def _build_remote_url(self, repository: str, github_token: Optional[str] = None) -> str:
        """
        Return a remote URL that optionally embeds credentials.

        Args:
            repository: Repository full name (org/repo)
            github_token: Project-specific GitHub token

        Returns:
            Authenticated or public clone URL
        """
        base_url = self._settings.github_base_url.rstrip("/")
        repo_slug = repository.strip("/")
        if repo_slug.endswith(".git"):
            repo_slug = repo_slug[:-4]

        # Use project token if available, fallback to env token
        token = github_token or self._settings.github_token

        if token:
            if base_url.startswith("https://"):
                base_url_with_auth = f"https://{token}@{base_url[len('https://'):]}"
            elif base_url.startswith("http://"):
                base_url_with_auth = f"http://{token}@{base_url[len('http://'):]}"
            else:
                base_url_with_auth = f"https://{token}@{base_url}"
            return f"{base_url_with_auth}/{repo_slug}.git"

        return f"{base_url}/{repo_slug}.git"

    def _determine_default_branch(self, remote_url: str) -> Optional[str]:
        """Discover the default branch for a remote repository."""
        try:
            result = subprocess.run(
                ["git", "ls-remote", "--symref", remote_url, "HEAD"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
                env=self._git_env(),
                text=True,
            )
        except subprocess.CalledProcessError as exc:
            logger.debug("Failed to determine default branch for %s: %s", remote_url, exc.stderr.strip())
            return None

        for line in result.stdout.splitlines():
            line = line.strip()
            if line.startswith("ref:") and "HEAD" in line:
                parts = line.split()
                if len(parts) >= 2:
                    ref = parts[1]
                    if ref.startswith("refs/heads/"):
                        return ref.split("/", 2)[-1]
        return None

    @staticmethod
    def _sanitize_repository_name(repository: str) -> str:
        return repository.replace("/", "__")

    def _git_env(self) -> dict:
        """Provide git environment variables for authenticated operations."""
        env = os.environ.copy()
        if self._settings.github_token:
            env.setdefault("GIT_TERMINAL_PROMPT", "0")
        return env

    @staticmethod
    def _is_valid_repo(path: Path) -> bool:
        if not path.exists():
            return False
        try:
            Repo(path)
        except (exc.InvalidGitRepositoryError, exc.NoSuchPathError):
            return False
        except OSError:
            return False
        return (path / ".git").exists()

    @staticmethod
    def _cleanup_repository_path(path: Path) -> None:
        if not path.exists():
            return

        try:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
        except OSError as exc:
            logger.error("Failed to clean repository path %s: %s", path, exc)
            raise

    @staticmethod
    def _cleanup_git_lock_files(repo_path: Path) -> None:
        if not repo_path.exists():
            return

        lock_files = [
            repo_path / ".git" / "index.lock",
            repo_path / ".git" / "config.lock",
        ]

        for lock_file in lock_files:
            if lock_file.exists():
                try:
                    lock_file.unlink()
                    logger.warning("Removed stale git lock file %s", lock_file)
                except OSError as exc:
                    logger.error("Failed to remove git lock file %s: %s", lock_file, exc)
                    raise
