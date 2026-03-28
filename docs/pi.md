> Pi and Result data models.

---

## `Pi`

A unit of work that wraps a script for parallel execution.

Each Pi has a name, a script path, optional arguments and environment
variables, and an optional timeout in seconds.  A unique 12-character
hex ID is assigned on creation and cannot be overridden.

`.py` files run under the current Python interpreter; everything
else is executed directly (shell scripts, compiled binaries, etc.).

**Example**

```python
pi = Pi(name="train", script="train.py", args=["--lr", "0.01"])
print(pi.id)  # e.g. "a1b2c3d4e5f6"
```

**Parameters**

| Name | Description |
|---|---|
| `name` | Human-readable label, also used as the directory name in output. |
| `script` | Path to the script to run. |
| `args` | Command-line arguments passed to the script.  Supports `{task_dir}` and `{artefact_dir}` placeholders that are resolved to real paths before execution. |
| `env` | Extra environment variables merged into the subprocess env. |
| `timeout` | Maximum runtime in seconds.  The subprocess is killed if it exceeds this limit.  `None` means no limit. |

---

### `resolve_dirs(base_dir: Path | str) → tuple[Path, Path]`

Build the task and artefact directory paths for this Pi.

Directories are not created here; the worker handles that at
execution time.

**Parameters**

| Name | Description |
|---|---|
| `base_dir` | Root directory for all output.  Converted to `Path` if given as a string. |

**Returns**

`tuple[Path, Path]` — `(task_dir, artefact_dir)` where `task_dir` is `<base_dir>/<name>/<id>` and `artefact_dir` is `<task_dir>/artefact`.

---

## `Result`

Outcome of running a single Pi.

Created by the worker after a subprocess finishes (or fails to start).
Contains the exit code, timing, log paths, and any error information.

**Example**

```python
if result.succeeded:
    print(f"{result.pi_name} passed in {result.duration_s}s")
else:
    print(result.error_message)
```

**Parameters**

| Name | Description |
|---|---|
| `pi_id` | Unique ID of the Pi that produced this result. |
| `pi_name` | Name of the Pi that produced this result. |
| `exit_code` | Subprocess return code.  `None` when the process could not start or the worker crashed. |
| `started_at` | UTC timestamp when execution began. |
| `finished_at` | UTC timestamp when execution ended. |
| `duration_s` | Wall-clock duration in seconds, rounded to milliseconds. |
| `task_dir` | Absolute path to the task output directory. |
| `artefact_dir` | Absolute path to the artefact subdirectory. |
| `error_message` | Human-readable error for timeouts, launch failures, or worker crashes.  `None` on success. |
| `timed_out` | `True` if the subprocess was killed for exceeding its timeout. |

### `succeeded`

`True` when exit_code is 0 and the Pi did not time out.
