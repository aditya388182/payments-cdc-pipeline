"""The Avro compatibility gate (scripts/check_schema_compat.py): breaking fixtures fail with a clear message,
compatible ones pass, and it fails closed when it cannot decide."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location("check_schema_compat", ROOT / "scripts" / "check_schema_compat.py")
gate = importlib.util.module_from_spec(spec)
assert spec.loader
spec.loader.exec_module(gate)

BASELINE = ROOT / "schemas" / "transactions_v2.avsc"
FIX = ROOT / "schemas" / "fixtures"


def run(name: str) -> tuple[bool, list[str]]:
    return gate.check_local(BASELINE.read_text(), (FIX / name).read_text())


def test_baseline_is_a_real_schema_not_the_old_empty_file():
    assert BASELINE.stat().st_size > 1000
    assert json.loads(BASELINE.read_text())["name"] == "Envelope"


def test_baseline_against_itself_is_compatible():
    assert gate.check_local(BASELINE.read_text(), BASELINE.read_text()) == (True, [])


def test_new_optional_field_with_a_default_is_compatible():
    ok, msgs = run("compatible_add_optional_field.avsc")
    assert ok and msgs == []


def test_removing_a_field_is_compatible_under_backward():
    # the SPEC called this breaking; under BACKWARD the reader just ignores the extra data (breaking only under FORWARD)
    assert run("compatible_remove_field.avsc")[0] is True


@pytest.mark.parametrize(
    "fixture, needle",
    [
        ("breaking_rename_field.avsc", "Value.ccy: field added WITHOUT a default"),
        ("breaking_type_change.avsc", "Value.amount_minor: type changed from long to string"),
        ("breaking_add_required_field.avsc", "Value.risk_level: field added WITHOUT a default"),
    ],
)
def test_breaking_changes_fail_with_a_message_that_names_the_field(fixture, needle):
    ok, msgs = run(fixture)
    assert not ok
    assert any(needle in m for m in msgs), msgs


def test_rename_message_points_at_the_rename():
    _, msgs = run("breaking_rename_field.avsc")
    assert any("looks like a rename 'currency' -> 'ccy'" in m for m in msgs)


def test_cli_exit_codes_breaking_1_compatible_0_error_2(tmp_path, capsys):
    assert gate.main(["--proposed", str(FIX / "breaking_rename_field.avsc")]) == 1
    assert "BREAKING SCHEMA CHANGE" in capsys.readouterr().err
    assert gate.main(["--proposed", str(FIX / "compatible_add_optional_field.avsc")]) == 0
    empty = tmp_path / "empty.avsc"
    empty.write_text("")
    assert gate.main(["--proposed", str(empty)]) == 2, "an empty proposed schema must fail closed"
    notjson = tmp_path / "bad.avsc"
    notjson.write_text("{not json")
    assert gate.main(["--proposed", str(notjson)]) == 2
    assert gate.main(["--proposed", str(FIX / "compatible_add_optional_field.avsc"), "--baseline", str(empty)]) == 2


def test_an_empty_baseline_cannot_gate_anything(tmp_path):
    empty = tmp_path / "empty.avsc"
    empty.write_text("   \n")
    with pytest.raises(gate.GateError):
        gate.read_schema_text(empty)


def test_unreachable_registry_fails_closed():
    with pytest.raises(gate.GateError):
        gate.check_registry("http://127.0.0.1:9", "payments.public.transactions-value", BASELINE.read_text(), timeout=1)


def test_self_test_passes_on_the_committed_fixtures():
    rows, ok = gate.self_test(BASELINE, None)
    assert ok, [r for r in rows if not r["passed"]]
    assert {r["expect"] for r in rows} == {"compatible", "breaking"}
