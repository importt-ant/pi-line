"""Tests for piline.runner — Runner parallel executor."""

from __future__ import annotations

from pathlib import Path

from piline.pi import Pi
from piline.runner import Runner


class TestRunner:
    """Tests for the Runner class."""

    def test_single_pi(self, run_dir: Path, pass_script: str) -> None:
        """Runner can execute a single Pi successfully."""
        runner = Runner(base_dir=run_dir, max_workers=2)
        results = runner.run([Pi(name="one", script=pass_script)])

        assert len(results) == 1
        assert results[0].succeeded is True

    def test_multiple_pis(self, run_dir: Path, pass_script: str) -> None:
        """Runner executes several Pi's and returns one result per Pi."""
        pis = [Pi(name=f"job{i}", script=pass_script) for i in range(4)]
        runner = Runner(base_dir=run_dir, max_workers=2)

        results = runner.run(pis)

        assert len(results) == 4
        assert all(r.succeeded for r in results)

    def test_empty_list(self, run_dir: Path) -> None:
        """Runner.run([]) returns an empty list without error."""
        runner = Runner(base_dir=run_dir)
        results = runner.run([])
        assert results == []

    def test_mixed_pass_fail(
        self, run_dir: Path, pass_script: str, fail_script: str
    ) -> None:
        """Results correctly distinguish passing and failing Pi's."""
        pis = [
            Pi(name="ok", script=pass_script),
            Pi(name="bad", script=fail_script),
        ]
        runner = Runner(base_dir=run_dir, max_workers=2)

        results = runner.run(pis)
        by_name = {r.pi_name: r for r in results}

        assert by_name["ok"].succeeded is True
        assert by_name["bad"].succeeded is False

    def test_timeout_in_batch(
        self, run_dir: Path, pass_script: str, slow_script: str
    ) -> None:
        """A timeout in one Pi doesn't block the others from completing."""
        pis = [
            Pi(name="fast", script=pass_script),
            Pi(name="slow", script=slow_script, timeout=1),
        ]
        runner = Runner(base_dir=run_dir, max_workers=2)

        results = runner.run(pis)
        by_name = {r.pi_name: r for r in results}

        assert by_name["fast"].succeeded is True
        assert by_name["slow"].timed_out is True

    def test_dir_layout(self, run_dir: Path, pass_script: str) -> None:
        """Runner creates <base_dir>/<name>/<id>/ with stdout.log, stderr.log, artefact/."""
        pi = Pi(name="layout", script=pass_script)
        runner = Runner(base_dir=run_dir, max_workers=1)
        results = runner.run([pi])

        task_dir = Path(results[0].task_dir)
        assert task_dir.exists()
        assert (task_dir / "stdout.log").exists()
        assert (task_dir / "stderr.log").exists()
        assert (task_dir / "artefact").is_dir()

        # Verify path structure: run_dir / name / id
        assert task_dir.parent.name == pi.name
        assert task_dir.name == pi.id

    def test_same_name_different_ids(self, run_dir: Path, pass_script: str) -> None:
        """Two Pi's with the same name get separate directories (different IDs)."""
        pis = [Pi(name="dup", script=pass_script) for _ in range(3)]
        runner = Runner(base_dir=run_dir, max_workers=2)

        results = runner.run(pis)

        task_dirs = {r.task_dir for r in results}
        assert len(task_dirs) == 3  # all unique directories

    def test_results_reset_between_runs(
        self, run_dir: Path, pass_script: str
    ) -> None:
        """runner.results is reset at the start of each run() call."""
        runner = Runner(base_dir=run_dir, max_workers=1)

        runner.run([Pi(name="r1", script=pass_script)])
        assert len(runner.results) == 1

        runner.run([Pi(name="r2a", script=pass_script), Pi(name="r2b", script=pass_script)])
        assert len(runner.results) == 2  # not 3

    def test_max_workers_default(self, run_dir: Path) -> None:
        """max_workers defaults to os.cpu_count() when not specified."""
        import os

        runner = Runner(base_dir=run_dir)
        assert runner.max_workers == (os.cpu_count() or 4)
