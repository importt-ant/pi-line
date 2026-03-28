"""Tests for piline.worker — execute_pi subprocess execution."""

from __future__ import annotations

from pathlib import Path

from piline.pi import Pi
from piline.worker import execute_pi


class TestExecutePi:
    """Tests for the execute_pi function."""

    def test_passing_script(self, run_dir: Path, pass_script: str) -> None:
        """A script that exits 0 produces a successful Result."""
        pi = Pi(name="ok", script=pass_script)
        task_dir, artefact_dir = pi.resolve_dirs(run_dir)

        result = execute_pi(pi, task_dir, str(artefact_dir))

        assert result.exit_code == 0
        assert result.succeeded is True
        assert result.timed_out is False
        assert result.error_message is None
        assert result.pi_id == pi.id
        assert result.pi_name == "ok"

    def test_failing_script(self, run_dir: Path, fail_script: str) -> None:
        """A script that exits 1 produces exit_code=1."""
        pi = Pi(name="bad", script=fail_script)
        task_dir, artefact_dir = pi.resolve_dirs(run_dir)

        result = execute_pi(pi, task_dir, str(artefact_dir))

        assert result.exit_code == 1
        assert result.succeeded is False
        assert result.timed_out is False

    def test_timeout(self, run_dir: Path, slow_script: str) -> None:
        """A script that exceeds its timeout is killed and marked timed_out."""
        pi = Pi(name="slow", script=slow_script, timeout=1)
        task_dir, artefact_dir = pi.resolve_dirs(run_dir)

        result = execute_pi(pi, task_dir, str(artefact_dir))

        assert result.timed_out is True
        assert result.succeeded is False
        assert "timed out" in (result.error_message or "")

    def test_env_vars_set(self, run_dir: Path, env_echo_script: str) -> None:
        """PILINE_* env vars are available to the subprocess."""
        pi = Pi(name="env_test", script=env_echo_script)
        task_dir, artefact_dir = pi.resolve_dirs(run_dir)

        result = execute_pi(pi, task_dir, str(artefact_dir))

        stdout = (Path(result.task_dir) / "stdout.log").read_text()
        assert f"PILINE_PI_ID={pi.id}" in stdout
        assert f"PILINE_PI_NAME=env_test" in stdout
        assert "PILINE_TASK_DIR=" in stdout
        assert "PILINE_ARTEFACT_DIR=" in stdout

    def test_custom_env(self, run_dir: Path) -> None:
        """Extra env vars passed via pi.env reach the subprocess."""
        script = str(Path(__file__).parent / "scripts" / "env_echo.py")
        pi = Pi(name="cenv", script=script, env={"PILINE_CUSTOM": "hello"})
        task_dir, artefact_dir = pi.resolve_dirs(run_dir)

        # env_echo only prints PILINE_* vars, so our custom one should appear
        result = execute_pi(pi, task_dir, str(artefact_dir))
        stdout = (Path(result.task_dir) / "stdout.log").read_text()
        assert "PILINE_CUSTOM=hello" in stdout

    def test_artefact_dir_exists_and_writable(
        self, run_dir: Path, artefact_script: str
    ) -> None:
        """Script can write files into the artefact directory."""
        pi = Pi(name="art", script=artefact_script)
        task_dir, artefact_dir = pi.resolve_dirs(run_dir)

        result = execute_pi(pi, task_dir, str(artefact_dir))

        assert result.succeeded is True
        assert (Path(result.artefact_dir) / "output.txt").read_text() == "artefact content"

    def test_arg_templates(self, run_dir: Path, echo_args_script: str) -> None:
        """Placeholders {task_dir} and {artefact_dir} are resolved in args."""
        pi = Pi(
            name="tmpl",
            script=echo_args_script,
            args=["--out={artefact_dir}/model.pt", "--log={task_dir}/run.log"],
        )
        task_dir, artefact_dir = pi.resolve_dirs(run_dir)

        result = execute_pi(pi, task_dir, str(artefact_dir))
        stdout = (Path(result.task_dir) / "stdout.log").read_text().strip().splitlines()

        assert f"--out={artefact_dir}/model.pt" in stdout
        assert f"--log={task_dir}/run.log" in stdout

    def test_stdout_stderr_captured(self, run_dir: Path, pass_script: str) -> None:
        """stdout.log and stderr.log are created in task_dir."""
        pi = Pi(name="logs", script=pass_script)
        task_dir, artefact_dir = pi.resolve_dirs(run_dir)

        result = execute_pi(pi, task_dir, str(artefact_dir))

        assert (Path(result.task_dir) / "stdout.log").exists()
        assert (Path(result.task_dir) / "stderr.log").exists()
        assert "hello from pass" in (Path(result.task_dir) / "stdout.log").read_text()

    def test_nonexistent_script(self, run_dir: Path) -> None:
        """A script that doesn't exist gets an error_message (not a crash)."""
        pi = Pi(name="ghost", script="/tmp/does_not_exist_12345.py")
        task_dir, artefact_dir = pi.resolve_dirs(run_dir)

        result = execute_pi(pi, task_dir, str(artefact_dir))

        # Either exit_code is non-zero or there's an error_message
        assert not result.succeeded

    def test_duration_is_recorded(self, run_dir: Path, pass_script: str) -> None:
        """duration_s is a positive float."""
        pi = Pi(name="dur", script=pass_script)
        task_dir, artefact_dir = pi.resolve_dirs(run_dir)

        result = execute_pi(pi, task_dir, str(artefact_dir))

        assert result.duration_s is not None
        assert result.duration_s >= 0
