"""Shared fixtures for piline tests."""

from __future__ import annotations

from pathlib import Path

import pytest

SCRIPTS_DIR = Path(__file__).parent / "scripts"


@pytest.fixture()
def pass_script() -> str:
    return str(SCRIPTS_DIR / "pass.py")


@pytest.fixture()
def fail_script() -> str:
    return str(SCRIPTS_DIR / "fail.py")


@pytest.fixture()
def slow_script() -> str:
    return str(SCRIPTS_DIR / "slow.py")


@pytest.fixture()
def env_echo_script() -> str:
    return str(SCRIPTS_DIR / "env_echo.py")


@pytest.fixture()
def artefact_script() -> str:
    return str(SCRIPTS_DIR / "write_artefact.py")


@pytest.fixture()
def echo_args_script() -> str:
    return str(SCRIPTS_DIR / "echo_args.py")


@pytest.fixture(autouse=True)
def run_dir(tmp_path: Path) -> Path:
    """Provide a fresh temp directory for each test's Runner base_dir."""
    d = tmp_path / "runs"
    d.mkdir()
    return d
