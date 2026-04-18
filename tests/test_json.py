"""Tests for JSON serialisation round-trips on all public classes."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from piline.line import Line
from piline.pi import Pi, Result
from piline.runner import Runner


# ── Pi ───────────────────────────────────────────────────────────────


class TestPiJson:
    """Pi.to_dict / from_dict / to_json / from_json."""

    def test_round_trip_dict(self) -> None:
        """to_dict → from_dict restores an identical Pi."""
        original = Pi(name="train", script="train.py", args=["--lr", "0.01"], env={"K": "V"}, timeout=30)
        restored = Pi.from_dict(original.to_dict())

        assert restored.name == original.name
        assert restored.script == original.script
        assert restored.args == original.args
        assert restored.env == original.env
        assert restored.timeout == original.timeout
        assert restored.id == original.id

    def test_round_trip_json(self) -> None:
        """to_json → from_json restores an identical Pi."""
        original = Pi(name="eval", script="eval.py")
        restored = Pi.from_json(original.to_json())

        assert restored.name == original.name
        assert restored.id == original.id

    def test_from_dict_without_id(self) -> None:
        """from_dict without an 'id' key generates a new ID."""
        pi = Pi.from_dict({"name": "x", "script": "x.py"})
        assert len(pi.id) == 12

    def test_from_dict_with_custom_id(self) -> None:
        """from_dict restores the provided 'id'."""
        pi = Pi.from_dict({"name": "x", "script": "x.py", "id": "aabbccddeeff"})
        assert pi.id == "aabbccddeeff"

    def test_from_dict_defaults(self) -> None:
        """from_dict uses correct defaults for optional fields."""
        pi = Pi.from_dict({"name": "a", "script": "a.py"})
        assert pi.args == []
        assert pi.env == {}
        assert pi.timeout is None

    def test_to_dict_types(self) -> None:
        """to_dict returns only JSON-native types."""
        d = Pi(name="t", script="t.py").to_dict()
        # Should be valid JSON without a custom encoder
        json.dumps(d)

    def test_to_json_kwargs(self) -> None:
        """Extra kwargs are passed through to json.dumps."""
        s = Pi(name="t", script="t.py").to_json(indent=2)
        assert "\n" in s  # pretty-printed


# ── Result ───────────────────────────────────────────────────────────


class TestResultJson:
    """Result.to_dict / from_dict / to_json / from_json."""

    def _make_result(self) -> Result:
        started = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        finished = datetime(2025, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        return Result(
            pi_id="abc123",
            pi_name="job",
            exit_code=0,
            started_at=started,
            finished_at=finished,
            duration_s=5.0,
            task_dir="/tmp/runs/job/abc123",
            artefact_dir="/tmp/runs/job/abc123/artefact",
            error_message=None,
            timed_out=False,
        )

    def test_round_trip_dict(self) -> None:
        """to_dict → from_dict restores identical scalar fields."""
        original = self._make_result()
        restored = Result.from_dict(original.to_dict())

        assert restored.pi_id == original.pi_id
        assert restored.pi_name == original.pi_name
        assert restored.exit_code == original.exit_code
        assert restored.duration_s == original.duration_s
        assert restored.task_dir == original.task_dir
        assert restored.artefact_dir == original.artefact_dir
        assert restored.error_message == original.error_message
        assert restored.timed_out == original.timed_out

    def test_round_trip_datetimes(self) -> None:
        """Datetime fields survive the dict round-trip."""
        original = self._make_result()
        restored = Result.from_dict(original.to_dict())

        assert restored.started_at == original.started_at
        assert restored.finished_at == original.finished_at

    def test_round_trip_json(self) -> None:
        """to_json → from_json restores an identical Result."""
        original = self._make_result()
        restored = Result.from_json(original.to_json())

        assert restored.pi_id == original.pi_id
        assert restored.started_at == original.started_at

    def test_from_dict_minimal(self) -> None:
        """from_dict works with only the required keys."""
        r = Result.from_dict({"pi_id": "x", "pi_name": "y"})
        assert r.pi_id == "x"
        assert r.pi_name == "y"
        assert r.exit_code is None
        assert r.timed_out is False

    def test_none_finished_at(self) -> None:
        """None finished_at round-trips correctly."""
        r = Result(pi_id="x", pi_name="y", finished_at=None)
        d = r.to_dict()
        assert d["finished_at"] is None
        restored = Result.from_dict(d)
        assert restored.finished_at is None

    def test_to_dict_types(self) -> None:
        """to_dict returns only JSON-native types."""
        d = self._make_result().to_dict()
        json.dumps(d)

    def test_error_result_round_trip(self) -> None:
        """A timed-out result with an error message round-trips."""
        r = Result(
            pi_id="t", pi_name="slow", exit_code=None,
            timed_out=True, error_message="timed out after 5s",
        )
        restored = Result.from_dict(r.to_dict())
        assert restored.timed_out is True
        assert restored.error_message == "timed out after 5s"
        assert restored.exit_code is None


# ── Runner ───────────────────────────────────────────────────────────


class TestRunnerJson:
    """Runner.to_dict / from_dict / to_json / from_json."""

    def test_round_trip_dict(self, run_dir: Path) -> None:
        """to_dict → from_dict restores base_dir and max_workers."""
        original = Runner(base_dir=run_dir, max_workers=8)
        restored = Runner.from_dict(original.to_dict())

        assert str(restored.base_dir) == str(original.base_dir)
        assert restored.max_workers == original.max_workers

    def test_round_trip_json(self, run_dir: Path) -> None:
        """to_json → from_json round-trips."""
        original = Runner(base_dir=run_dir, max_workers=4)
        restored = Runner.from_json(original.to_json())

        assert str(restored.base_dir) == str(original.base_dir)
        assert restored.max_workers == 4

    def test_results_included(self, run_dir: Path, pass_script: str) -> None:
        """Stored results are included in the dict and restored."""
        runner = Runner(base_dir=run_dir, max_workers=1)
        runner.run([Pi(name="j", script=pass_script)])

        d = runner.to_dict()
        assert len(d["results"]) == 1

        restored = Runner.from_dict(d)
        assert len(restored.results) == 1
        assert restored.results[0].pi_name == "j"

    def test_empty_results(self, run_dir: Path) -> None:
        """A fresh Runner with no results round-trips fine."""
        runner = Runner(base_dir=run_dir, max_workers=2)
        restored = Runner.from_dict(runner.to_dict())
        assert restored.results == []


# ── Line ─────────────────────────────────────────────────────────────


class TestLineJson:
    """Line.to_dict / from_dict / to_json / from_json."""

    def test_round_trip_dict(self, run_dir: Path) -> None:
        """to_dict → from_dict restores runner config and max_results."""
        runner = Runner(base_dir=run_dir, max_workers=4)
        original = Line(runner, maxsize=10, max_results=500)
        restored = Line.from_dict(original.to_dict())

        assert str(restored.runner.base_dir) == str(runner.base_dir)
        assert restored.runner.max_workers == 4
        assert restored.max_results == 500

    def test_round_trip_json(self, run_dir: Path) -> None:
        """to_json → from_json round-trips."""
        runner = Runner(base_dir=run_dir, max_workers=2)
        original = Line(runner, max_results=100)
        restored = Line.from_json(original.to_json())

        assert restored.max_results == 100

    def test_results_included(self, run_dir: Path, pass_script: str) -> None:
        """Results stored on the Line survive the round-trip."""
        runner = Runner(base_dir=run_dir, max_workers=2)
        with Line(runner) as line:
            pi = Pi(name="rj", script=pass_script)
            line.put(pi)
            _wait_for_results(line, 1)

            d = line.to_dict()
            assert pi.id in d["results"]

        restored = Line.from_dict(d)
        assert pi.id in restored.results
        assert restored.results[pi.id].pi_name == "rj"

    def test_callbacks_not_serialised(self, run_dir: Path) -> None:
        """Callbacks are excluded; from_dict creates a Line without them."""
        runner = Runner(base_dir=run_dir, max_workers=1)
        original = Line(runner, on_pi_complete=lambda r: None)

        restored = Line.from_dict(original.to_dict())
        assert restored.on_pi_complete is None
        assert restored.on_batch_complete is None


# ── helper ───────────────────────────────────────────────────────────


def _wait_for_results(line: Line, count: int, timeout: float = 10.0) -> None:
    """Spin until *line* has at least *count* results, or time out."""
    import time

    deadline = time.monotonic() + timeout
    while line.result_count < count:
        if time.monotonic() > deadline:
            raise TimeoutError(f"Expected {count} results, got {line.result_count}")
        time.sleep(0.05)
