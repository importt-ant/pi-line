"""Runner — parallel Pi executor."""

from __future__ import annotations

import os
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from piline.pi import Pi, PiResult
from piline.worker import execute_pi


class Runner:
    """Executes a list of Pi's in parallel.

    Output layout::

        <base_dir>/<pi_name>/<pi_id>/
            stdout.log
            stderr.log
            artefact/        # scripts write artifacts here

    Usage::

        runner = Runner()
        results = runner.run([Pi(name="a", script="a.py")])
    """

    def __init__(
        self,
        base_dir: Path | str = ".piline/runs",
        max_workers: int | None = None,
    ) -> None:
        self.base_dir = Path(base_dir)
        self.max_workers = max_workers or os.cpu_count() or 4
        self.results: list[PiResult] = []

    def run(self, pis: list[Pi]) -> list[PiResult]:
        """Execute *pis* in parallel and return results."""
        if not pis:
            return []

        self.results = []
        max_w = min(self.max_workers, len(pis))
        futures: dict[Future[PiResult], Pi] = {}

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

    def _collect(self, future: Future[PiResult], pi: Pi) -> PiResult:
        try:
            return future.result()
        except Exception as exc:
            task_dir, artefact_dir = pi.resolve_dirs(self.base_dir)
            return PiResult(
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
