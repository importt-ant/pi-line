"""Worker — executes a single Pi in a subprocess."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from piline.pi import Pi, PiResult


def execute_pi(pi: Pi, task_dir: Path, artefact_dir: str) -> PiResult:
    """Run a Pi as a subprocess and capture its output.

    Directory layout::

        task_dir/
            stdout.log
            stderr.log
            artefact/   # PILINE_ARTEFACT_DIR

    Designed for use inside a ProcessPoolExecutor.
    """
    task_dir.mkdir(parents=True, exist_ok=True)
    Path(artefact_dir).mkdir(parents=True, exist_ok=True)

    stdout_path = task_dir / "stdout.log"
    stderr_path = task_dir / "stderr.log"

    env = os.environ.copy()
    env.update(pi.env)
    env["PILINE_PI_ID"] = pi.id
    env["PILINE_PI_NAME"] = pi.name
    env["PILINE_TASK_DIR"] = str(task_dir)
    env["PILINE_ARTEFACT_DIR"] = artefact_dir

    # Resolve {task_dir} and {artefact_dir} placeholders in args
    resolved_args = [
        a.replace("{task_dir}", str(task_dir)).replace("{artefact_dir}", artefact_dir)
        for a in pi.args
    ]

    # Detect script type: use Python for .py files, run directly otherwise
    if pi.script.endswith(".py"):
        cmd = [sys.executable, pi.script, *resolved_args]
    else:
        cmd = [pi.script, *resolved_args]

    started_at = datetime.now(timezone.utc)
    exit_code: int | None = None
    error_message: str | None = None
    timed_out = False

    try:
        with (
            open(stdout_path, "w", encoding="utf-8") as f_out,
            open(stderr_path, "w", encoding="utf-8") as f_err,
        ):
            proc = subprocess.run(
                cmd,
                stdout=f_out,
                stderr=f_err,
                env=env,
                timeout=pi.timeout,
            )
            exit_code = proc.returncode
    except subprocess.TimeoutExpired:
        timed_out = True
        error_message = f"Pi '{pi.name}' timed out after {pi.timeout}s"
    except Exception as exc:
        error_message = f"Pi '{pi.name}' failed to launch: {exc}"

    finished_at = datetime.now(timezone.utc)
    duration_s = (finished_at - started_at).total_seconds()

    return PiResult(
        pi_id=pi.id,
        pi_name=pi.name,
        exit_code=exit_code,
        started_at=started_at,
        finished_at=finished_at,
        duration_s=round(duration_s, 3),
        task_dir=str(task_dir),
        artefact_dir=artefact_dir,
        error_message=error_message,
        timed_out=timed_out,
    )
