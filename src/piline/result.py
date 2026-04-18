"""Result data model."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class Result:
    """Outcome of running a single Pi.

    Created by the worker after a subprocess finishes (or fails to start).
    Contains the exit code, timing, log paths, and any error information.

    Example::

        if result.succeeded:
            print(f"{result.pi_name} passed in {result.duration_s}s")
        else:
            print(result.error_message)

    Parameters
    ----------
    pi_id:
        Unique ID of the Pi that produced this result.
    pi_name:
        Name of the Pi that produced this result.
    exit_code:
        Subprocess return code.  ``None`` when the process could not
        start or the worker crashed.
    started_at:
        UTC timestamp when execution began.
    finished_at:
        UTC timestamp when execution ended.
    duration_s:
        Wall-clock duration in seconds, rounded to milliseconds.
    task_dir:
        Absolute path to the task output directory.
    artefact_dir:
        Absolute path to the artefact subdirectory.
    error_message:
        Human-readable error for timeouts, launch failures, or worker
        crashes.  ``None`` on success.
    timed_out:
        ``True`` if the subprocess was killed for exceeding its timeout.
    """

    pi_id: str
    pi_name: str
    exit_code: int | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime | None = None
    duration_s: float | None = None
    task_dir: str | None = None
    artefact_dir: str | None = None
    error_message: str | None = None
    timed_out: bool = False

    @property
    def succeeded(self) -> bool:
        """``True`` when exit_code is 0 and the Pi did not time out."""
        return self.exit_code == 0 and not self.timed_out

    def to_dict(self) -> dict:
        """Serialise this Result to a plain dict.

        Datetime fields are stored as ISO 8601 strings (UTC).

        Returns
        -------
        dict
            JSON-serialisable dictionary.
        """
        return {
            "pi_id": self.pi_id,
            "pi_name": self.pi_name,
            "exit_code": self.exit_code,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "duration_s": self.duration_s,
            "task_dir": self.task_dir,
            "artefact_dir": self.artefact_dir,
            "error_message": self.error_message,
            "timed_out": self.timed_out,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Result:
        """Create a Result from a dict (e.g. one produced by :meth:`to_dict`).

        ISO 8601 datetime strings are parsed back into ``datetime``
        objects.

        Parameters
        ----------
        data:
            Dictionary with at least ``pi_id`` and ``pi_name`` keys.

        Returns
        -------
        Result
        """
        started_at = data.get("started_at")
        if isinstance(started_at, str):
            started_at = datetime.fromisoformat(started_at)

        finished_at = data.get("finished_at")
        if isinstance(finished_at, str):
            finished_at = datetime.fromisoformat(finished_at)

        return cls(
            pi_id=data["pi_id"],
            pi_name=data["pi_name"],
            exit_code=data.get("exit_code"),
            started_at=started_at or datetime.now(timezone.utc),
            finished_at=finished_at,
            duration_s=data.get("duration_s"),
            task_dir=data.get("task_dir"),
            artefact_dir=data.get("artefact_dir"),
            error_message=data.get("error_message"),
            timed_out=data.get("timed_out", False),
        )

    def to_json(self, **kwargs: object) -> str:
        """Serialise this Result to a JSON string.

        Extra keyword arguments are forwarded to :func:`json.dumps`.
        """
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, s: str) -> Result:
        """Create a Result from a JSON string."""
        return cls.from_dict(json.loads(s))
