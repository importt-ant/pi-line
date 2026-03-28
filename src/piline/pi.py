"""Core data models."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


def _new_id() -> str:
    return uuid.uuid4().hex[:12]


@dataclass
class Pi:
    """A unit of work — runs a Python script as a subprocess.

    Each instance gets a unique ``id`` at creation time.
    """

    name: str
    script: str
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    timeout: int | None = None
    id: str = field(default_factory=_new_id)

    def __repr__(self) -> str:
        return f"Pi(name={self.name!r}, id={self.id!r})"

    def resolve_dirs(self, base_dir: Path | str) -> tuple[Path, Path]:
        """Return ``(task_dir, artefact_dir)`` under *base_dir*.

        Layout: ``<base_dir>/<name>/<id>/artefact/``
        """
        task_dir = Path(base_dir) / self.name / self.id
        return task_dir, task_dir / "artefact"


@dataclass
class Result:
    """Outcome of executing a single Pi."""

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
        return self.exit_code == 0 and not self.timed_out
