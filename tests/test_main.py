import asyncio
from collections.abc import Callable
from contextlib import suppress
from typing import cast

from memory_vault.main import _stop_outbox_worker
from memory_vault.workers.outbox_worker import OutboxWorker


class _FakeWorker:
    def __init__(self, on_stop: Callable[[], None] | None = None) -> None:
        self.stopped = False
        self._on_stop = on_stop

    def stop(self) -> None:
        self.stopped = True

        if self._on_stop is not None:
            self._on_stop()


async def test_stop_outbox_worker_waits_for_graceful_stop() -> None:
    stopped = asyncio.Event()
    worker = _FakeWorker(on_stop=stopped.set)

    async def run() -> None:
        await stopped.wait()

    task = asyncio.create_task(run())

    await _stop_outbox_worker(cast(OutboxWorker, worker), task, 1.0)

    assert worker.stopped
    assert task.done()
    assert not task.cancelled()


async def test_stop_outbox_worker_bounds_non_cooperative_shutdown() -> None:
    worker = _FakeWorker()
    cancellations = 0

    async def run() -> None:
        nonlocal cancellations

        while cancellations < 2:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cancellations += 1

    task = asyncio.create_task(run())
    await asyncio.sleep(0)

    start = asyncio.get_running_loop().time()

    try:
        # The outer bound turns a regression into a failure instead of a hang.
        await asyncio.wait_for(
            _stop_outbox_worker(cast(OutboxWorker, worker), task, 0.05),
            timeout=2,
        )
        elapsed = asyncio.get_running_loop().time() - start

        # Both bounded waits (graceful + post-cancel) must return even though
        # the task keeps absorbing cancellation.
        assert elapsed < 1.0
        assert not task.done()
    finally:
        task.cancel()

        with suppress(asyncio.CancelledError):
            await task
