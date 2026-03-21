"""Thread-safe feed queue for a Runner."""

from __future__ import annotations

import collections
import queue
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING

from piline.pi import Pi, PiResult

if TYPE_CHECKING:
    from piline.runner import Runner

_DEFAULT_MAX_RESULTS = 2_000


class Line:
    """Feeds Pi's to a :class:`Runner` in batches from a background thread.

    Results are stored in a dict keyed by ``pi_id``.  When *max_results* is
    reached the oldest entries are evicted automatically.

    Usage::

        runner = Runner(max_workers=4)
        with Line(runner) as line:
            line.put(Pi(...))     # gets picked up automatically
            line.get("abc123")    # lookup by pi_id
        # line.stop() called automatically
    """

    def __init__(
        self,
        runner: Runner,
        maxsize: int = 0,
        max_results: int = _DEFAULT_MAX_RESULTS,
        on_batch_complete: Callable[[list[PiResult]], None] | None = None,
        on_pi_complete: Callable[[PiResult], None] | None = None,
    ) -> None:
        self._queue: queue.Queue[Pi] = queue.Queue(maxsize=maxsize)
        self._lock = threading.Lock()
        self._total_enqueued = 0

        self.runner = runner
        self.max_results = max_results
        self.on_batch_complete = on_batch_complete
        self.on_pi_complete = on_pi_complete
        self.results: collections.OrderedDict[str, PiResult] = collections.OrderedDict()
        self._consumer_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    # ── context manager ──────────────────────────────────────────────

    def __enter__(self) -> Line:
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()

    # ── enqueue ──────────────────────────────────────────────────────

    def put(self, pi: Pi) -> str:
        """Enqueue a Pi. Returns its ID."""
        self._queue.put(pi)
        with self._lock:
            self._total_enqueued += 1
        return pi.id

    def put_many(self, pis: list[Pi]) -> list[str]:
        """Enqueue multiple Pi's. Returns list of IDs."""
        return [self.put(p) for p in pis]

    @property
    def size(self) -> int:
        return self._queue.qsize()

    @property
    def empty(self) -> bool:
        return self._queue.empty()

    @property
    def total_enqueued(self) -> int:
        with self._lock:
            return self._total_enqueued

    # ── results ──────────────────────────────────────────────────────

    def get(self, pi_id: str) -> PiResult | None:
        """Look up a result by *pi_id*, or ``None`` if not found."""
        with self._lock:
            return self.results.get(pi_id)

    @property
    def result_count(self) -> int:
        """Number of results currently retained."""
        with self._lock:
            return len(self.results)

    def drain_results(self) -> dict[str, PiResult]:
        """Remove and return all stored results, clearing the internal dict."""
        with self._lock:
            drained = dict(self.results)
            self.results.clear()
            return drained

    # ── consumer lifecycle ───────────────────────────────────────────

    @property
    def running(self) -> bool:
        return self._consumer_thread is not None and self._consumer_thread.is_alive()

    def start(self) -> None:
        """Start consuming tasks in a background thread."""
        if self.running:
            raise RuntimeError("Consumer is already running")
        self._stop_event.clear()
        self._consumer_thread = threading.Thread(
            target=self._consume_loop, daemon=True, name="qu-consumer",
        )
        self._consumer_thread.start()

    def stop(self, timeout: float | None = None) -> None:
        """Signal the consumer to stop and wait for it to finish."""
        self._stop_event.set()
        if self._consumer_thread is not None:
            self._consumer_thread.join(timeout=timeout)

    def _consume_loop(self) -> None:
        """Poll the queue, collect batches up to max_workers, run them."""
        batch_size = self.runner.max_workers

        while not self._stop_event.is_set():
            batch = self._collect_batch(batch_size, poll_interval=0.1)
            if not batch:
                continue
            batch_results = self.runner.run(batch)
            with self._lock:
                for r in batch_results:
                    self.results[r.pi_id] = r
                # Evict oldest if over limit
                while len(self.results) > self.max_results:
                    self.results.popitem(last=False)
            if self.on_pi_complete:
                for r in batch_results:
                    self.on_pi_complete(r)
            if self.on_batch_complete:
                self.on_batch_complete(batch_results)

    def _collect_batch(self, max_size: int, poll_interval: float) -> list[Pi]:
        """Gather up to *max_size* items, blocking briefly on the first."""
        batch: list[Pi] = []
        try:
            first = self._queue.get(timeout=poll_interval)
        except queue.Empty:
            return batch
        batch.append(first)
        while len(batch) < max_size:
            try:
                batch.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return batch
