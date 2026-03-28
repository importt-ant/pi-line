> Parallel Pi executor using ProcessPoolExecutor.

---

## `Runner`

Runs a batch of Pi's in parallel using a process pool.

Each Pi gets its own output directory under *base_dir*:
`<base_dir>/<pi_name>/<pi_id>/`, containing `stdout.log`,
`stderr.log`, and an `artefact/` subdirectory.

`results` is reset on every call to `run`.

**Example**

```python
runner = Runner(base_dir="/tmp/runs", max_workers=4)
results = runner.run([
    Pi(name="train", script="train.py"),
    Pi(name="eval", script="eval.py"),
])
for r in results:
    print(r.pi_name, r.succeeded)
```

**Parameters**

| Name | Description |
|---|---|
| `base_dir` | Root directory for task output.  Defaults to `.piline/runs`. |
| `max_workers` | Maximum number of parallel processes.  Defaults to `os.cpu_count()`, falling back to 4. |

---

### `run(pis: list[Pi]) → list[Result]`

Execute *pis* in parallel and return one Result per Pi.

Clears `self.results` before starting. The number of worker
processes is capped at `min(max_workers, len(pis))` so short
lists don't spawn idle processes.

**Parameters**

| Name | Description |
|---|---|
| `pis` | Pi's to execute.  An empty list returns `[]` immediately. |

**Returns**

`list[Result]` — One Result per Pi, in completion order (not input order).
