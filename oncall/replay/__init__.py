"""replay-svc: gRPC server wrapping the pipeline's existing validator and producer.

The protobuf stubs are generated into oncall/replay/gen/oncall/v1 (package `oncall.v1`). `oncall` is a
namespace package spanning the repo's oncall/ directory and that gen/ directory, so the generated
`from oncall.v1 import replay_pb2` imports resolve once gen/ is on sys.path.
"""
import sys
from pathlib import Path

_GEN = str(Path(__file__).resolve().parent / "gen")
if _GEN not in sys.path:
    sys.path.insert(0, _GEN)
