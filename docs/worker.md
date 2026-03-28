<a id="piline.worker"></a>

# piline.worker

Single-Pi subprocess execution.

<a id="piline.worker.execute_pi"></a>

#### execute\_pi

```python
def execute_pi(pi: Pi, task_dir: Path, artefact_dir: str) -> Result
```

Run a single Pi as a subprocess and return the outcome.

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

