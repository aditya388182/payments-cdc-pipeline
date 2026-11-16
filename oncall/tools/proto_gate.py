"""Proto gate evidence: `buf lint`, `buf breaking` against the first commit that had the protos (must pass: only compatible
additions since), and `buf breaking` against a deliberately broken copy (must FAIL). Writes results/oncall/proto_gate.json.

  python oncall/tools/proto_gate.py
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from provenance import REPO, write_result  # noqa: E402

PROTO = REPO / "oncall" / "proto"
ENV = {**os.environ, "PATH": f"{Path.home()}/.local/bin:" + os.environ.get("PATH", "")}


def buf(*args: str, cwd: Path = PROTO) -> tuple[int, str]:
    p = subprocess.run(["buf", *args], cwd=cwd, env=ENV, capture_output=True, text=True)
    return p.returncode, (p.stdout + p.stderr).strip()


def main() -> None:
    version = buf("--version")[1]
    lint_rc, lint_out = buf("lint")
    first = subprocess.run(["git", "log", "--diff-filter=A", "--format=%h", "--", "oncall/proto/oncall/v1/replay.proto"], cwd=REPO, capture_output=True, text=True).stdout.split()[-1]
    br_rc, br_out = buf("breaking", "--against", f"{REPO}/.git#ref={first},subdir=oncall/proto")

    tmp = Path(tempfile.mkdtemp(prefix="proto-gate-", dir=os.environ.get("TMPDIR")))
    shutil.copytree(PROTO, tmp / "p")
    proto = tmp / "p" / "oncall" / "v1" / "replay.proto"
    text = proto.read_text()
    broken = text.replace("  string requested_by = 2;\n", "").replace("string dlq_id = 1;", "int32 dlq_id = 1;")
    proto.write_text(broken)
    bad_rc, bad_out = buf("breaking", str(tmp / "p"), "--against", str(PROTO))

    write_result("proto_gate.json", "python oncall/tools/proto_gate.py", {
        "buf_version": version,
        "lint": {"exit_code": lint_rc, "output": lint_out or "clean"},
        "breaking_against_first_proto_commit": {"against_commit": first, "exit_code": br_rc, "output": br_out or "no breaking changes"},
        "breaking_against_deliberately_broken_copy": {"exit_code": bad_rc, "detected": bad_rc != 0, "output": bad_out.splitlines()[:6]},
        "passed": lint_rc == 0 and br_rc == 0 and bad_rc != 0,
    })
    print(f"lint rc={lint_rc}  breaking vs {first} rc={br_rc}  broken copy rc={bad_rc} (non-zero is the expected result)")
    sys.exit(0 if (lint_rc == 0 and br_rc == 0 and bad_rc != 0) else 1)


if __name__ == "__main__":
    main()
