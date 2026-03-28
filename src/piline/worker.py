"""Single-Pi subprocess execution."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from piline.pi import Pi, Result


def execute_pi(pi: Pi, task_dir: Path, artefact_dir: str) -> Result:
    """Run a single Pi as a subprocess and return the outcome.

    Creates *task_dir* and *artefact_dir* if they don't exist, writes
    stdout and stderr to log files inside *task_dir*, and populates a
    :class:`Result` with exit code, timing, and error details.

    The subprocess receives four extra environment variables:
    ``PILINE_PI_ID``, ``PILINE_PI_NAME``, ``PILINE_TASK_DIR``, and
    ``PILINE_ARTEFACT_DIR``, plus any vars from ``pi.env``.
    ``{task_dir}`` and ``{artefact_dir}`` placeholders in ``pi.args``
    are resolved to real paths before the command is built.

    Meant to run inside a ``ProcessPoolExecutor`` via :class:`Runner`,
    but works fine standalone.

    Parameters
    ----------
    pi:
        The Pi to execute.
    task_dir:
        Directory for stdout/stderr logs.  Created if missing.
    artefact_dir:
        Directory the script can write output files to.  Passed to
        the subprocess via ``PILINE_ARTEFACT_DIR``.  Created if missing.

    Returns
    -------
    Result
        Contains exit code, timing, log paths, and any error or
        timeout information.
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

    return Result(
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
