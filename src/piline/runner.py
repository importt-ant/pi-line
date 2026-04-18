"""Parallel Pi executor using ProcessPoolExecutor."""

from __future__ import annotations

import json
import os
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from piline.pi import Pi, Result
from piline.worker import execute_pi


class Runner:
    """Runs a batch of Pi's in parallel using a process pool.

    Each Pi gets its own output directory under *base_dir*:
    ``<base_dir>/<pi_name>/<pi_id>/``, containing ``stdout.log``,
    ``stderr.log``, and an ``artefact/`` subdirectory.

    ``results`` is reset on every call to :meth:`run`.

    Example::

        runner = Runner(base_dir="/tmp/runs", max_workers=4)
        results = runner.run([
            Pi(name="train", script="train.py"),
            Pi(name="eval", script="eval.py"),
        ])
        for r in results:
            print(r.pi_name, r.succeeded)

    Parameters
    ----------
    base_dir:
        Root directory for task output.  Defaults to ``.piline/runs``.
    max_workers:
        Maximum number of parallel processes.  Defaults to
        ``os.cpu_count()``, falling back to 4.
    """

    def __init__(
        self,
        base_dir: Path | str = ".piline/runs",
        max_workers: int | None = None,
    ) -> None:
        self.base_dir = Path(base_dir)
        self.max_workers = max_workers or os.cpu_count() or 4
        self.results: list[Result] = []

    def run(self, pis: list[Pi]) -> list[Result]:
        """Execute *pis* in parallel and return one Result per Pi.

        Clears ``self.results`` before starting. The number of worker
        processes is capped at ``min(max_workers, len(pis))`` so short
        lists don't spawn idle processes.

        Parameters
        ----------
        pis:
            Pi's to execute.  An empty list returns ``[]`` immediately.

        Returns
        -------
        list[Result]
            One Result per Pi, in completion order (not input order).
        """
        if not pis:
            return []

        self.results = []
        max_w = min(self.max_workers, len(pis))
        futures: dict[Future[Result], Pi] = {}

        with ProcessPoolExecutor(max_workers=max_w) as pool:
            for pi in pis:
                task_dir, artefact_dir = pi.resolve_dirs(self.base_dir)
                f = pool.submit(execute_pi, pi, task_dir, str(artefact_dir))
                futures[f] = pi

            for future in as_completed(futures):
                pi = futures[future]
                result = self._collect(future, pi)
                self.results.append(result)

        return self.results

    def _collect(self, future: Future[Result], pi: Pi) -> Result:
        """Return the Result from a completed future, or a crash Result on error.

        Wraps the future's result in a try/except so that worker crashes
        do not propagate as unhandled exceptions — they are captured as a
        ``Result`` with ``exit_code=None`` and a descriptive
        ``error_message`` instead, keeping the rest of the batch intact.

        Parameters
        ----------
        future:
            A completed future returned by the process pool.
        pi:
            The Pi that the future was executing.  Used to populate the
            failure Result when an exception is caught.

        Returns
        -------
        Result
            The Result produced by the worker, or a synthetic failure
            Result if the worker process raised an exception.
        """
        try:
            return future.result()
        except Exception as exc:
            task_dir, artefact_dir = pi.resolve_dirs(self.base_dir)
            return Result(
                pi_id=pi.id,
                pi_name=pi.name,
                exit_code=None,
                started_at=datetime.now(timezone.utc),
                finished_at=datetime.now(timezone.utc),
                duration_s=0,
                task_dir=str(task_dir),
                artefact_dir=str(artefact_dir),
                error_message=f"Worker crashed: {exc}",
            )

    def to_dict(self) -> dict:
        """Serialise Runner configuration (and results) to a dict.

        Returns
        -------
        dict
            JSON-serialisable dictionary containing ``base_dir``,
            ``max_workers``, and any stored ``results``.
        """
        return {
            "base_dir": str(self.base_dir),
            "max_workers": self.max_workers,
            "results": [r.to_dict() for r in self.results],
        }

    @classmethod
    def from_dict(cls, data: dict) -> Runner:
        """Create a Runner from a dict (e.g. one produced by :meth:`to_dict`).

        Stored results, if present, are restored onto the instance.

        Parameters
        ----------
        data:
            Dictionary with at least ``base_dir`` and ``max_workers``
            keys.

        Returns
        -------
        Runner
        """
        runner = cls(
            base_dir=data.get("base_dir", ".piline/runs"),
            max_workers=data.get("max_workers"),
        )
        for rd in data.get("results", []):
            runner.results.append(Result.from_dict(rd))
        return runner

    def to_json(self, **kwargs: object) -> str:
        """Serialise this Runner to a JSON string.

        Extra keyword arguments are forwarded to :func:`json.dumps`.
        """
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, s: str) -> Runner:
        """Create a Runner from a JSON string."""
        return cls.from_dict(json.loads(s))
