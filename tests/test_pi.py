"""Tests for piline.pi — Pi and Result data models."""

from __future__ import annotations

from pathlib import Path

from piline.pi import Pi
from piline.result import Result


class TestPi:
    """Tests for the Pi dataclass."""

    def test_unique_ids(self) -> None:
        """Two Pi instances get different auto-generated IDs."""
        a = Pi(name="a", script="a.py")
        b = Pi(name="b", script="b.py")
        assert a.id != b.id

    def test_id_format(self) -> None:
        """Auto-generated ID is a 12-char hex string."""
        pi = Pi(name="x", script="x.py")
        assert len(pi.id) == 12
        assert all(c in "0123456789abcdef" for c in pi.id)

    def test_repr(self) -> None:
        """__repr__ includes name and id only."""
        pi = Pi(name="train", script="train.py")
        r = repr(pi)
        assert r.startswith("Pi(name='train', id='")
        assert r.endswith("')")

    def test_defaults(self) -> None:
        """Default args/env/timeout are empty/None."""
        pi = Pi(name="d", script="d.py")
        assert pi.args == []
        assert pi.env == {}
        assert pi.timeout is None

    def test_resolve_dirs(self, tmp_path: Path) -> None:
        """resolve_dirs produces correct task_dir and artefact_dir."""
        pi = Pi(name="job", script="job.py")
        task_dir, artefact_dir = pi.resolve_dirs(tmp_path)

        assert task_dir == tmp_path / "job" / pi.id
        assert artefact_dir == task_dir / "artefact"

    def test_resolve_dirs_str_input(self, tmp_path: Path) -> None:
        """resolve_dirs accepts a str path as well as a Path."""
        pi = Pi(name="job", script="job.py")
        task_dir, artefact_dir = pi.resolve_dirs(str(tmp_path))

        assert isinstance(task_dir, Path)
        assert isinstance(artefact_dir, Path)

    def test_id_not_settable_via_constructor(self) -> None:
        """id cannot be passed to the constructor."""
        import pytest

        with pytest.raises(TypeError):
            Pi(name="job", script="job.py", id="custom123")  # type: ignore[call-arg]


class TestResult:
    """Tests for the Result dataclass."""

    def test_succeeded_true(self) -> None:
        """succeeded is True when exit_code == 0 and not timed out."""
        r = Result(pi_id="abc", pi_name="x", exit_code=0)
        assert r.succeeded is True

    def test_succeeded_false_nonzero(self) -> None:
        """succeeded is False when exit_code != 0."""
        r = Result(pi_id="abc", pi_name="x", exit_code=1)
        assert r.succeeded is False

    def test_succeeded_false_timeout(self) -> None:
        """succeeded is False when timed_out is True."""
        r = Result(pi_id="abc", pi_name="x", exit_code=0, timed_out=True)
        assert r.succeeded is False

    def test_succeeded_none_exit(self) -> None:
        """succeeded is False when exit_code is None (e.g. crash)."""
        r = Result(pi_id="abc", pi_name="x", exit_code=None)
        assert r.succeeded is False
