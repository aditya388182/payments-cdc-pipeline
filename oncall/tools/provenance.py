"""Shared provenance helper for results files (commit, dirty, generated_at_utc, command)."""
from __future__ import annotations

import datetime as _dt
import json
import subprocess
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[2]
RESULTS_DIR = REPO / "results" / "oncall"


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=REPO, check=True, capture_output=True, text=True
    ).stdout.strip()


def provenance(command: str) -> dict[str, Any]:
    # "dirty" ignores generated results/logs so writing a results file does not flip it.
    status = _git("status", "--porcelain", "--", ".", ":(exclude)results", ":(exclude)logs")
    return {
        "commit": _git("rev-parse", "--short", "HEAD"),
        "dirty": bool(status),
        "generated_at_utc": _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "command": command,
    }


def write_result(name: str, command: str, body: dict[str, Any]) -> Path:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / name
    doc = {**provenance(command), **body}
    path.write_text(json.dumps(doc, indent=2, sort_keys=False) + "\n")
    return path
