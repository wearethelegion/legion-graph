"""
Cognee service — per-dataset concurrency primitives.

DatasetLockManager provides one asyncio.Lock per (company_id, dataset) pair,
so cognify operations on *different* datasets can proceed in parallel while
concurrent calls to the *same* dataset are still serialised.
"""

import asyncio


class DatasetLockManager:
    """Per-key async lock manager for dataset isolation."""

    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}
        self._meta_lock = asyncio.Lock()

    async def acquire(self, company_id: str, dataset: str) -> asyncio.Lock:
        """Return (and acquire) the lock for the given company+dataset key."""
        key = f"{company_id}:{dataset}"
        async with self._meta_lock:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
            lock = self._locks[key]
        await lock.acquire()
        return lock


dataset_locks = DatasetLockManager()
