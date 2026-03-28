"""piline: zero-dependency parallel script runner.

Public API
----------
Pi
    A unit of work wrapping a script with args, env, and timeout.
Result
    Outcome of running a Pi (exit code, timing, log paths).
Runner
    Executes a batch of Pi's in parallel.
Line
    Thread-safe queue that feeds Pi's to a Runner continuously.
"""

from piline.line import Line
from piline.pi import Pi, Result
from piline.runner import Runner

__all__ = ["Line", "Pi", "Result", "Runner"]
