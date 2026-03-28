"""Tests for piline.line — Line feed queue + consumer."""

from __future__ import annotations

import time
from pathlib import Path

from piline.line import Line
from piline.pi import Pi, Result
from piline.runner import Runner


class TestLineEnqueue:
    """Enqueueing and queue properties."""

    def test_put_returns_id(self, run_dir: Path, pass_script: str) -> None:
        """put() returns the Pi's ID."""
        runner = Runner(base_dir=run_dir, max_workers=1)
        line = Line(runner)
        pi = Pi(name="a", script=pass_script)

        pi_id = line.put(pi)
        assert pi_id == pi.id

    def test_put_many(self, run_dir: Path, pass_script: str) -> None:
        """put_many() returns a list of IDs matching the input Pi's."""
        runner = Runner(base_dir=run_dir, max_workers=1)
        line = Line(runner)
        pis = [Pi(name=f"j{i}", script=pass_script) for i in range(3)]

        ids = line.put_many(pis)
        assert ids == [p.id for p in pis]

    def test_size_and_empty(self, run_dir: Path, pass_script: str) -> None:
        """size reflects queued items; empty is True when nothing is queued."""
        runner = Runner(base_dir=run_dir, max_workers=1)
        line = Line(runner)

        assert line.empty is True
        assert line.size == 0

        line.put(Pi(name="a", script=pass_script))
        assert line.size >= 1
        assert line.empty is False

    def test_total_enqueued(self, run_dir: Path, pass_script: str) -> None:
        """total_enqueued increments with every put, even after consumption."""
        runner = Runner(base_dir=run_dir, max_workers=1)
        with Line(runner) as line:
            for i in range(5):
                line.put(Pi(name=f"t{i}", script=pass_script))
            assert line.total_enqueued == 5


class TestLineConsumer:
    """Background consumer and result storage."""

    def test_auto_consume(self, run_dir: Path, pass_script: str) -> None:
        """Pi's placed on a started Line get executed automatically."""
        runner = Runner(base_dir=run_dir, max_workers=2)
        with Line(runner) as line:
            pi = Pi(name="auto", script=pass_script)
            line.put(pi)

            # Wait for the consumer to process
            _wait_for_results(line, 1)

            r = line.get(pi.id)
            assert r is not None
            assert r.succeeded is True

    def test_get_returns_none_for_unknown(
        self, run_dir: Path, pass_script: str
    ) -> None:
        """get() returns None for a pi_id that hasn't been processed."""
        runner = Runner(base_dir=run_dir, max_workers=1)
        line = Line(runner)
        assert line.get("nonexistent") is None

    def test_result_count(self, run_dir: Path, pass_script: str) -> None:
        """result_count reflects the number of results stored."""
        runner = Runner(base_dir=run_dir, max_workers=2)
        with Line(runner) as line:
            pis = [Pi(name=f"rc{i}", script=pass_script) for i in range(3)]
            line.put_many(pis)
            _wait_for_results(line, 3)

            assert line.result_count == 3

    def test_batching(self, run_dir: Path, pass_script: str) -> None:
        """Line dispatches Pi's to Runner in batches up to max_workers."""
        runner = Runner(base_dir=run_dir, max_workers=2)
        with Line(runner) as line:
            pis = [Pi(name=f"b{i}", script=pass_script) for i in range(5)]
            line.put_many(pis)
            _wait_for_results(line, 5)

            assert line.result_count == 5
            assert all(line.get(p.id) is not None for p in pis)

    def test_context_manager(self, run_dir: Path, pass_script: str) -> None:
        """Line works as a context manager starting/stopping the consumer."""
        runner = Runner(base_dir=run_dir, max_workers=1)

        with Line(runner) as line:
            assert line.running is True
            line.put(Pi(name="ctx", script=pass_script))
            _wait_for_results(line, 1)

        # After exiting the context, consumer is stopped
        assert line.running is False

    def test_double_start_raises(self, run_dir: Path, pass_script: str) -> None:
        """Calling start() twice raises RuntimeError."""
        runner = Runner(base_dir=run_dir, max_workers=1)
        line = Line(runner)
        line.start()
        try:
            import pytest

            with pytest.raises(RuntimeError, match="already running"):
                line.start()
        finally:
            line.stop()

    def test_running_property(self, run_dir: Path, pass_script: str) -> None:
        """running is False before start, True after start, False after stop."""
        runner = Runner(base_dir=run_dir, max_workers=1)
        line = Line(runner)

        assert line.running is False
        line.start()
        assert line.running is True
        line.stop()
        assert line.running is False


class TestLineCallbacks:
    """on_pi_complete and on_batch_complete callbacks."""

    def test_on_pi_complete(self, run_dir: Path, pass_script: str) -> None:
        """on_pi_complete is called once per completed Pi."""
        completed: list[Result] = []
        runner = Runner(base_dir=run_dir, max_workers=2)

        with Line(runner, on_pi_complete=lambda r: completed.append(r)) as line:
            pis = [Pi(name=f"cb{i}", script=pass_script) for i in range(3)]
            line.put_many(pis)
            _wait_for_results(line, 3)

        assert len(completed) == 3
        assert all(r.succeeded for r in completed)

    def test_on_batch_complete(self, run_dir: Path, pass_script: str) -> None:
        """on_batch_complete is called with each batch of results."""
        batches: list[list[Result]] = []
        runner = Runner(base_dir=run_dir, max_workers=2)

        with Line(
            runner, on_batch_complete=lambda b: batches.append(b)
        ) as line:
            pis = [Pi(name=f"bc{i}", script=pass_script) for i in range(3)]
            line.put_many(pis)
            _wait_for_results(line, 3)

        total = sum(len(b) for b in batches)
        assert total == 3


class TestLineEviction:
    """max_results cap and drain."""

    def test_max_results_eviction(self, run_dir: Path, pass_script: str) -> None:
        """When results exceed max_results, oldest entries are evicted."""
        runner = Runner(base_dir=run_dir, max_workers=2)
        with Line(runner, max_results=3) as line:
            pis = [Pi(name=f"ev{i}", script=pass_script) for i in range(6)]
            line.put_many(pis)
            _wait_for_results(line, 6, max_stored=3)

            assert line.result_count <= 3

    def test_drain_results(self, run_dir: Path, pass_script: str) -> None:
        """drain_results returns and clears all stored results."""
        runner = Runner(base_dir=run_dir, max_workers=2)
        with Line(runner) as line:
            pis = [Pi(name=f"dr{i}", script=pass_script) for i in range(3)]
            line.put_many(pis)
            _wait_for_results(line, 3)

            drained = line.drain_results()
            assert len(drained) == 3
            assert line.result_count == 0

    def test_drain_results_empty(self, run_dir: Path) -> None:
        """drain_results on a fresh Line returns an empty dict."""
        runner = Runner(base_dir=run_dir, max_workers=1)
        line = Line(runner)
        assert line.drain_results() == {}


# ── helpers ──────────────────────────────────────────────────────────────

def _wait_for_results(
    line: Line,
    expected: int,
    *,
    timeout: float = 30.0,
    max_stored: int | None = None,
) -> None:
    """Poll until the line has processed *expected* results (or timeout).

    When *max_stored* is set, wait until total_enqueued reaches expected
    instead of checking result_count (results may be evicted).
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if max_stored is not None:
            # Can't rely on result_count due to eviction; use total_enqueued
            # plus a small sleep to let the last batch finish writing results
            if line.total_enqueued >= expected and line.empty:
                time.sleep(0.3)
                return
        elif line.result_count >= expected:
            return
        time.sleep(0.05)
    msg = f"Timed out waiting for {expected} results (got {line.result_count})"
    raise TimeoutError(msg)
