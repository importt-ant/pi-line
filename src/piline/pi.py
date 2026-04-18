"""Pi data model."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path

# TODO: remove backward compatibility in next major release
# Backward compatibility: allow `from piline.pi import Result`
from piline.result import Result as Result  # noqa: F401


def _new_id() -> str:
    return uuid.uuid4().hex[:12]


@dataclass
class Pi:
    """A unit of work that wraps a script for parallel execution.

    Each Pi has a name, a script path, optional arguments and environment
    variables, and an optional timeout in seconds.  A unique 12-character
    hex ID is assigned on creation and cannot be overridden.

    ``.py`` files run under the current Python interpreter; everything
    else is executed directly (shell scripts, compiled binaries, etc.).

    Example::

        pi = Pi(name="train", script="train.py", args=["--lr", "0.01"])
        print(pi.id)  # e.g. "a1b2c3d4e5f6"

    Parameters
    ----------
    name:
        Human-readable label, also used as the directory name in output.
    script:
        Path to the script to run.
    args:
        Command-line arguments passed to the script.  Supports
        ``{task_dir}`` and ``{artefact_dir}`` placeholders that are
        resolved to real paths before execution.
    env:
        Extra environment variables merged into the subprocess env.
    timeout:
        Maximum runtime in seconds.  The subprocess is killed if it
        exceeds this limit.  ``None`` means no limit.
    """

    name: str
    script: str
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    timeout: int | None = None
    id: str = field(default_factory=_new_id, init=False)

    def __repr__(self) -> str:
        """Short representation showing name and id."""
        return f"Pi(name={self.name!r}, id={self.id!r})"

    def resolve_dirs(self, base_dir: Path | str) -> tuple[Path, Path]:
        """Build the task and artefact directory paths for this Pi.

        Directories are not created here; the worker handles that at
        execution time.

        Parameters
        ----------
        base_dir:
            Root directory for all output.  Converted to ``Path`` if
            given as a string.

        Returns
        -------
        tuple[Path, Path]
            ``(task_dir, artefact_dir)`` where ``task_dir`` is
            ``<base_dir>/<name>/<id>`` and ``artefact_dir`` is
            ``<task_dir>/artefact``.
        """
        task_dir = Path(base_dir) / self.name / self.id
        return task_dir, task_dir / "artefact"

    def to_dict(self) -> dict:
        """Serialise this Pi to a plain dict.

        The returned dict includes the auto-generated ``id`` so that
        a round-trip through :meth:`from_dict` restores an identical
        instance.

        Returns
        -------
        dict
            JSON-serialisable dictionary.
        """
        return {
            "name": self.name,
            "script": self.script,
            "args": list(self.args),
            "env": dict(self.env),
            "timeout": self.timeout,
            "id": self.id,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Pi:
        """Create a Pi from a dict (e.g. one produced by :meth:`to_dict`).

        If the dict contains an ``id`` key the original ID is restored;
        otherwise a new one is generated.

        Parameters
        ----------
        data:
            Dictionary with at least ``name`` and ``script`` keys.

        Returns
        -------
        Pi
        """
        pi = cls(
            name=data["name"],
            script=data["script"],
            args=data.get("args", []),
            env=data.get("env", {}),
            timeout=data.get("timeout"),
        )
        if "id" in data:
            object.__setattr__(pi, "id", data["id"])
        return pi

    def to_json(self, **kwargs: object) -> str:
        """Serialise this Pi to a JSON string.

        Extra keyword arguments are forwarded to :func:`json.dumps`.
        """
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, s: str) -> Pi:
        """Create a Pi from a JSON string."""
        return cls.from_dict(json.loads(s))
