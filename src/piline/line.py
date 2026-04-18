"""Thread-safe feed queue that dispatches Pi's to a Runner."""

from __future__ import annotations

import collections
import json
import queue
import threading
import warnings
from collections.abc import Callable
from typing import TYPE_CHECKING

from piline.pi import Pi
from piline.result import Result

if TYPE_CHECKING:
    from piline.runner import Runner

_DEFAULT_MAX_RESULTS = 2_000


class Line:
    """Thread-safe queue that feeds Pi's to a Runner from a background thread.

    Put Pi's onto the Line and they get batched and dispatched to the
    Runner in groups of up to ``runner.max_workers``.  Results are stored
    in an ordered dict keyed by ``pi_id``.  When the dict exceeds
    *max_results* entries, the oldest results are dropped (FIFO).

    Use as a context manager to handle start/stop:

    Example::

        runner = Runner(max_workers=4)
        with Line(runner, max_results=5000) as line:
            line.put(Pi(name="job1", script="job.py"))
            line.put(Pi(name="job2", script="job.py"))

            result = line.get(some_pi_id)
            batch = line.drain_results()
        # consumer stops when the with-block exits

    Parameters
    ----------
    runner:
        The Runner that executes batches of Pi's.
    maxsize:
        Maximum queue depth.  ``0`` (default) means unlimited.  If the
        queue is full, :meth:`put` blocks until space is available.
    max_results:
        Maximum number of results to keep in memory.  Oldest results
        are evicted when this limit is exceeded.  Defaults to 2000.
    on_batch_complete:
        Called after each batch finishes, with the list of Results from
        that batch.
    on_pi_complete:
        Called once per finished Pi, with its Result.
    """

    def __init__(
        self,
        runner: Runner,
        maxsize: int = 0,
        max_results: int = _DEFAULT_MAX_RESULTS,
        on_batch_complete: Callable[[list[Result]], None] | None = None,
        on_pi_complete: Callable[[Result], None] | None = None,
    ) -> None:
        self._queue: queue.Queue[Pi] = queue.Queue(maxsize=maxsize)
        self._lock = threading.Lock()
        self._total_enqueued = 0

        self.runner = runner
        self.max_results = max_results
        self.on_batch_complete = on_batch_complete
        self.on_pi_complete = on_pi_complete
        self.results: collections.OrderedDict[str, Result] = collections.OrderedDict()
        self._consumer_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    # ── context manager ──────────────────────────────────────────────

    def __enter__(self) -> Line:
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()

    # ── enqueue ──────────────────────────────────────────────────────

    def put(self, pi: Pi | list[Pi]) -> str | list[str]:
        """Add one or more Pi's to the queue.

        Accepts a single Pi or a list.  If the consumer is running,
        enqueued Pi's will be picked up and executed in the next batch.

        Parameters
        ----------
        pi:
            A single Pi or a list of Pi's to enqueue.

        Returns
        -------
        str | list[str]
            The Pi's unique ID when given a single Pi, or a list of
            IDs (in the same order) when given a list.
        """
        if isinstance(pi, list):
            return [self.put(p) for p in pi]
        self._queue.put(pi)
        with self._lock:
            self._total_enqueued += 1
        return pi.id

    def put_many(self, pis: list[Pi]) -> list[str]:
        """Add several Pi's to the queue at once.

        .. deprecated::
            Use :meth:`put` with a list instead.  Will be removed in
            the next major release.

        Parameters
        ----------
        pis:
            List of Pi's to enqueue.

        Returns
        -------
        list[str]
            IDs of the enqueued Pi's, in the same order as *pis*.
        """
        # TODO: remove put_many in next major release
        warnings.warn(
            "put_many() is deprecated — pass a list to put() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        ids = self.put(pis)
        assert isinstance(ids, list)
        return ids

    @property
    def size(self) -> int:
        """Number of Pi's waiting in the queue (not yet dispatched)."""
        return self._queue.qsize()

    @property
    def empty(self) -> bool:
        """``True`` when nothing is queued."""
        return self._queue.empty()

    @property
    def total_enqueued(self) -> int:
        """Cumulative count of Pi's added since creation.

        Keeps incrementing even after Pi's leave the queue, so it
        works as a progress counter.
        """
        with self._lock:
            return self._total_enqueued

    # ── results ──────────────────────────────────────────────────────

    def get(self, pi_id: str) -> Result | None:
        """Look up a result by its Pi's ID.

        Parameters
        ----------
        pi_id:
            The ID returned by :meth:`put`.

        Returns
        -------
        Result | None
            The matching Result, or ``None`` if the Pi hasn't finished
            yet or has been evicted from the results dict.
        """
        with self._lock:
            return self.results.get(pi_id)

    @property
    def result_count(self) -> int:
        """Number of results currently stored.

        May be less than :attr:`total_enqueued` if results were drained
        or evicted.
        """
        with self._lock:
            return len(self.results)

    def drain_results(self) -> dict[str, Result]:
        """Remove and return all stored results.

        Useful for periodic flushing to a database or file without
        losing data.  After this call, :attr:`result_count` is 0.

        Returns
        -------
        dict[str, Result]
            Mapping of ``pi_id`` to Result for every result that was
            stored.  Empty dict if nothing was stored.
        """
        with self._lock:
            drained = dict(self.results)
            self.results.clear()
            return drained

    # ── consumer lifecycle ───────────────────────────────────────────

    @property
    def running(self) -> bool:
        """``True`` while the background consumer thread is alive."""
        return self._consumer_thread is not None and self._consumer_thread.is_alive()

    def start(self) -> None:
        """Start the background consumer thread.

        The consumer polls the queue, collects batches of up to
        ``runner.max_workers`` Pi's, and dispatches them to the Runner.
        Call :meth:`stop` (or use the context manager) to shut it down.

        Raises
        ------
        RuntimeError
            If the consumer is already running.
        """
        if self.running:
            raise RuntimeError("Consumer is already running")
        self._stop_event.clear()
        self._consumer_thread = threading.Thread(
            target=self._consume_loop,
            daemon=True,
            name="qu-consumer",
        )
        self._consumer_thread.start()

    def stop(self, timeout: float | None = None) -> None:
        """Stop the background consumer and wait for it to finish.

        Blocks until the consumer thread exits.  Any batch currently in
        progress will complete before the thread ends.

        Parameters
        ----------
        timeout:
            Maximum seconds to wait for the thread to join.  ``None``
            (default) waits indefinitely.
        """
        self._stop_event.set()
        if self._consumer_thread is not None:
            self._consumer_thread.join(timeout=timeout)

    def _consume_loop(self) -> None:
        """Drive the consumer: poll, batch, execute, store, callback — repeat.

        Runs inside the background thread started by :meth:`start`.  Each
        iteration pulls up to ``runner.max_workers`` Pi's from the queue,
        hands them to the Runner, and stores each Result keyed by its
        ``pi_id``.  Oldest results are evicted when the dict exceeds
        :attr:`max_results`.  Registered callbacks fire after every batch.
        Exits cleanly once the stop event is set and the queue drains.
        """
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
        """Gather up to *max_size* Pi's without blocking the thread indefinitely.

        Blocks for at most *poll_interval* seconds waiting for the first
        item, then drains additional items non-blocking until *max_size*
        is reached or the queue runs dry.  The short timeout on the first
        item keeps the consumer thread responsive to stop requests.

        Parameters
        ----------
        max_size:
            Upper bound on batch size; typically ``runner.max_workers``.
        poll_interval:
            Seconds to block waiting for the first Pi.  Should be short
            enough that :meth:`stop` returns promptly.

        Returns
        -------
        list[Pi]
            Between zero and *max_size* Pi's.  An empty list means the
            queue was idle for the full *poll_interval*.
        """
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

    def to_dict(self) -> dict:
        """Serialise Line configuration and stored results to a dict.

        Runtime state (thread, queue contents, callbacks) is not
        included — only the settings needed to reconstruct an
        equivalent Line and any results collected so far.

        Returns
        -------
        dict
            JSON-serialisable dictionary.
        """
        with self._lock:
            results = {pid: r.to_dict() for pid, r in self.results.items()}
        return {
            "runner": self.runner.to_dict(),
            "maxsize": self._queue.maxsize,
            "max_results": self.max_results,
            "results": results,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Line:
        """Create a Line from a dict (e.g. one produced by :meth:`to_dict`).

        Callbacks cannot be serialised and must be re-attached after
        construction.  Stored results are restored into the new
        instance.

        Parameters
        ----------
        data:
            Dictionary with at least a ``runner`` key.

        Returns
        -------
        Line
        """
        from piline.runner import Runner

        runner = Runner.from_dict(data["runner"])
        line = cls(
            runner=runner,
            maxsize=data.get("maxsize", 0),
            max_results=data.get("max_results", _DEFAULT_MAX_RESULTS),
        )
        for pid, rd in data.get("results", {}).items():
            line.results[pid] = Result.from_dict(rd)
        return line

    def to_json(self, **kwargs: object) -> str:
        """Serialise this Line to a JSON string.

        Extra keyword arguments are forwarded to :func:`json.dumps`.
        """
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, s: str) -> Line:
        """Create a Line from a JSON string."""
        return cls.from_dict(json.loads(s))
