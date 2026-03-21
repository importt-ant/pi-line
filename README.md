# pi-line

A zero-dependency parallel script runner for Python.

Define units of work (**Pi**), execute them in parallel with a **Runner**, or feed them continuously through a **Line**.

## Install

```bash
pip install pi-line
```

## Quick start

### Run scripts in parallel

```python
from piline import Pi, Runner

pis = [
    Pi(name="train", script="train.py", args=["--epochs", "10"]),
    Pi(name="eval", script="eval.py"),
]

runner = Runner(max_workers=4)
results = runner.run(pis)

for r in results:
    print(f"{r.pi_name}: {'PASS' if r.succeeded else 'FAIL'}")
```

### Continuous feed with a Line

```python
from piline import Pi, Runner, Line

runner = Runner(max_workers=4)

with Line(runner, max_results=5000) as line:
    line.put(Pi(name="job1", script="job.py"))
    line.put(Pi(name="job2", script="job.py"))

    # look up a result by id
    result = line.get(pi_id)

    # flush results to external storage
    batch = line.drain_results()
# line.stop() called automatically
```

### Argument templates

Use `{artefact_dir}` and `{task_dir}` placeholders in args — they get resolved to the real paths before execution:

```python
Pi(
    name="train",
    script="train.py",
    args=["--output", "{artefact_dir}/model.pt"],
)
```

## Concepts

- **Pi** — A unit of work. Wraps a script with args, env vars, and an optional timeout. Gets a unique ID on creation. `.py` files run with Python; anything else runs directly.
- **PiResult** — Outcome of running a Pi. Exit code, timing, paths to stdout/stderr logs and artefacts.
- **Runner** — Executes a batch of Pi's in parallel using `ProcessPoolExecutor`.
- **Line** — Thread-safe queue that feeds Pi's to a Runner in batches. Results stored in a capped dict keyed by `pi_id`. Supports context manager, per-Pi and per-batch callbacks, and `drain_results()` for periodic flushing.

## Output layout

```
.piline/runs/<pi_name>/<pi_id>/
    stdout.log
    stderr.log
    artefact/
```

## Environment variables

Scripts receive these env vars automatically:

- `PILINE_PI_ID` — The Pi's unique ID
- `PILINE_PI_NAME` — The Pi's name
- `PILINE_TASK_DIR` — Path to the task directory
- `PILINE_ARTEFACT_DIR` — Path to the artefact subdirectory

## Callbacks

```python
Line(
    runner,
    on_pi_complete=lambda r: print(f"Done: {r.pi_name} ({'PASS' if r.succeeded else 'FAIL'})"),
    on_batch_complete=lambda results: print(f"Batch of {len(results)} finished"),
)
```