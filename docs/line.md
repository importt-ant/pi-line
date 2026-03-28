> Thread-safe feed queue that dispatches Pi's to a Runner.

---

## `Line`

Thread-safe queue that feeds Pi's to a Runner from a background thread.

Put Pi's onto the Line and they get batched and dispatched to the
Runner in groups of up to `runner.max_workers`.  Results are stored
in an ordered dict keyed by `pi_id`.  When the dict exceeds
*max_results* entries, the oldest results are dropped (FIFO).

Use as a context manager to handle start/stop:

**Example**

```python
runner = Runner(max_workers=4)
with Line(runner, max_results=5000) as line:
    line.put(Pi(name="job1", script="job.py"))
    line.put(Pi(name="job2", script="job.py"))

    result = line.get(some_pi_id)
    batch = line.drain_results()
# consumer stops when the with-block exits
```

**Parameters**

| Name | Description |
|---|---|
| `runner` | The Runner that executes batches of Pi's. |
| `maxsize` | Maximum queue depth.  `0` (default) means unlimited.  If the queue is full, `put` blocks until space is available. |
| `max_results` | Maximum number of results to keep in memory.  Oldest results are evicted when this limit is exceeded.  Defaults to 2000. |
| `on_batch_complete` | Called after each batch finishes, with the list of Results from that batch. |
| `on_pi_complete` | Called once per finished Pi, with its Result. |

### `size`

Number of Pi's waiting in the queue (not yet dispatched).

### `empty`

`True` when nothing is queued.

### `total_enqueued`

Cumulative count of Pi's added since creation.  Keeps incrementing even after Pi's leave the queue, so it works as a progress counter.

### `result_count`

Number of results currently stored.  May be less than `total_enqueued` if results were drained or evicted.

### `running`

`True` while the background consumer thread is alive.

---

### `put(pi: Pi) → str`

Add a Pi to the queue.

If the consumer is running, the Pi will be picked up and
executed in the next batch.

**Parameters**

| Name | Description |
|---|---|
| `pi` | The Pi to enqueue. |

**Returns**

`str` — The Pi's unique ID, for later lookup via `get`.

---

### `put_many(pis: list[Pi]) → list[str]`

Add several Pi's to the queue at once.

**Parameters**

| Name | Description |
|---|---|
| `pis` | List of Pi's to enqueue. |

**Returns**

`list[str]` — IDs of the enqueued Pi's, in the same order as *pis*.

---

### `get(pi_id: str) → Result | None`

Look up a result by its Pi's ID.

**Parameters**

| Name | Description |
|---|---|
| `pi_id` | The ID returned by `put`. |

**Returns**

`Result | None` — The matching Result, or `None` if the Pi hasn't finished yet or has been evicted from the results dict.

---

### `drain_results() → dict[str, Result]`

Remove and return all stored results.

Useful for periodic flushing to a database or file without
losing data.  After this call, `result_count` is 0.

**Returns**

`dict[str, Result]` — Mapping of `pi_id` to Result for every result that was stored.  Empty dict if nothing was stored.

---

### `start() → None`

Start the background consumer thread.

The consumer polls the queue, collects batches of up to
`runner.max_workers` Pi's, and dispatches them to the Runner.
Call `stop` (or use the context manager) to shut it down.

**Raises**

| Exception | When |
|---|---|
| `RuntimeError` | If the consumer is already running. |

---

### `stop(timeout: float | None = None) → None`

Stop the background consumer and wait for it to finish.

Blocks until the consumer thread exits.  Any batch currently in
progress will complete before the thread ends.

**Parameters**

| Name | Description |
|---|---|
| `timeout` | Maximum seconds to wait for the thread to join.  `None` (default) waits indefinitely. |
