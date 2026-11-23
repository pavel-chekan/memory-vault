import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import cast

import pytest

import memory_vault.main as main_module
from memory_vault.main import _stop_outbox_worker, _supervise_outbox_worker
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


class _FakeSupervisedWorker:
    """Stand-in for ``OutboxWorker`` in supervisor tests.

    ``run()`` delegates to ``_run_impl``; ``wait_for_stop`` mirrors the real
    event-based implementation so ``stop()`` wakes it immediately.
    """

    def __init__(self) -> None:
        self.run_calls = 0
        self._stop_event = asyncio.Event()
        self._run_impl: Callable[[], Awaitable[None]] = self._noop

    async def _noop(self) -> None:
        return None

    @property
    def stop_requested(self) -> bool:
        return self._stop_event.is_set()

    def stop(self) -> None:
        self._stop_event.set()

    async def wait_for_stop(self) -> None:
        await self._stop_event.wait()

    async def run(self) -> None:
        self.run_calls += 1
        await self._run_impl()


def _fast_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(main_module, "_OUTBOX_RESTART_INITIAL_BACKOFF_SECONDS", 0.01)
    monkeypatch.setattr(main_module, "_OUTBOX_RESTART_MAX_BACKOFF_SECONDS", 0.02)


async def test_supervise_outbox_worker_returns_when_worker_returns_gracefully(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _fast_backoff(monkeypatch)
    worker = _FakeSupervisedWorker()

    async def run() -> None:
        return None

    worker._run_impl = run

    with caplog.at_level(logging.ERROR):
        await _supervise_outbox_worker(cast(OutboxWorker, worker))

    assert worker.run_calls == 1
    assert not [record for record in caplog.records if record.levelno >= logging.ERROR]


async def test_supervise_outbox_worker_restarts_after_crash(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _fast_backoff(monkeypatch)
    worker = _FakeSupervisedWorker()

    async def run() -> None:
        if worker.run_calls <= 2:
            raise RuntimeError("boom")

    worker._run_impl = run

    with caplog.at_level(logging.ERROR):
        await _supervise_outbox_worker(cast(OutboxWorker, worker))

    assert worker.run_calls == 3
    crash_logs = [record for record in caplog.records if "crashed" in record.message.lower()]
    assert len(crash_logs) == 2


async def test_supervise_outbox_worker_backs_off_between_restarts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(main_module, "_OUTBOX_RESTART_INITIAL_BACKOFF_SECONDS", 0.05)
    monkeypatch.setattr(main_module, "_OUTBOX_RESTART_MAX_BACKOFF_SECONDS", 60.0)
    worker = _FakeSupervisedWorker()
    timestamps: list[float] = []

    async def run() -> None:
        timestamps.append(asyncio.get_running_loop().time())

        if len(timestamps) >= 2:
            worker.stop()

        raise RuntimeError("boom")

    worker._run_impl = run

    task = asyncio.create_task(_supervise_outbox_worker(cast(OutboxWorker, worker)))
    await asyncio.wait_for(task, timeout=5)

    assert worker.run_calls == 2
    assert timestamps[1] - timestamps[0] >= 0.04


async def test_supervise_outbox_worker_doubles_backoff_between_crashes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(main_module, "_OUTBOX_RESTART_INITIAL_BACKOFF_SECONDS", 0.05)
    monkeypatch.setattr(main_module, "_OUTBOX_RESTART_MAX_BACKOFF_SECONDS", 60.0)
    worker = _FakeSupervisedWorker()
    timestamps: list[float] = []

    async def run() -> None:
        timestamps.append(asyncio.get_running_loop().time())

        if len(timestamps) >= 3:
            worker.stop()

        raise RuntimeError("boom")

    worker._run_impl = run

    task = asyncio.create_task(_supervise_outbox_worker(cast(OutboxWorker, worker)))
    await asyncio.wait_for(task, timeout=5)

    assert worker.run_calls == 3
    assert timestamps[1] - timestamps[0] >= 0.04
    assert timestamps[2] - timestamps[1] >= 0.08


async def test_supervise_outbox_worker_stop_during_backoff_returns_immediately(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(main_module, "_OUTBOX_RESTART_INITIAL_BACKOFF_SECONDS", 5.0)
    worker = _FakeSupervisedWorker()

    async def run() -> None:
        raise RuntimeError("boom")

    worker._run_impl = run

    task = asyncio.create_task(_supervise_outbox_worker(cast(OutboxWorker, worker)))

    for _ in range(100):
        if worker.run_calls == 1:
            break
        await asyncio.sleep(0.001)

    worker.stop()

    start = asyncio.get_running_loop().time()
    await asyncio.wait_for(task, timeout=2)
    elapsed = asyncio.get_running_loop().time() - start

    assert worker.run_calls == 1
    assert elapsed < 1.0


async def test_supervise_outbox_worker_does_not_restart_when_stop_races_crash(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _fast_backoff(monkeypatch)
    worker = _FakeSupervisedWorker()

    async def run() -> None:
        worker.stop()
        raise RuntimeError("boom")

    worker._run_impl = run

    with caplog.at_level(logging.ERROR):
        await _supervise_outbox_worker(cast(OutboxWorker, worker))

    assert worker.run_calls == 1
    assert any("after stop was requested" in record.message for record in caplog.records)


async def test_supervise_outbox_worker_propagates_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fast_backoff(monkeypatch)
    worker = _FakeSupervisedWorker()

    async def run() -> None:
        raise asyncio.CancelledError()

    worker._run_impl = run

    with pytest.raises(asyncio.CancelledError):
        await _supervise_outbox_worker(cast(OutboxWorker, worker))

    assert worker.run_calls == 1
