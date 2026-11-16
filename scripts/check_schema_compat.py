#!/usr/bin/env python3
"""Avro schema compatibility gate (BACKWARD).

BACKWARD means: a consumer that uses the PROPOSED schema can still read every message written with the BASELINE schema.
That is the guarantee the streaming job needs, because it decodes old and new messages with whichever writer schema each
message carries (spark/utils/avro_deserializer.py) and merges them into one table.

What breaks BACKWARD: adding a field WITHOUT a default (old messages have no value for it), renaming a field (that is a
removal plus an addition without a default), changing a type to one the old type cannot be promoted to. What does NOT:
adding a field WITH a default, and removing a field (the reader simply ignores it; removal is breaking under FORWARD).

Two engines, same verdict on the fixtures (see results/oncall/schema_gate.json):
  * local (default): Apache Avro's reader/writer resolution rules (`avro.compatibility`). Hermetic, needs no services,
    so it can run in CI.
  * --registry URL: asks Confluent Schema Registry's /compatibility endpoint (what the older ci.yml did).

Fails closed: an unreadable/empty schema, an unreachable registry or invalid JSON exits 2, a breaking change exits 1.

  check_schema_compat.py --baseline schemas/transactions_v2.avsc --proposed new.avsc
  check_schema_compat.py --baseline ... --proposed ... --registry http://localhost:8081 --subject payments.public.transactions-value
  check_schema_compat.py --self-test [--json results/oncall/schema_gate.json] [--registry http://localhost:8081]
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "schemas" / "fixtures"
DEFAULT_SUBJECT = "payments.public.transactions-value"
EXIT_OK, EXIT_BREAKING, EXIT_ERROR = 0, 1, 2


class GateError(Exception):
    """Anything that stops the gate from giving a verdict (the caller must fail closed)."""


def read_schema_text(path: Path) -> str:
    try:
        text = path.read_text()
    except OSError as exc:
        raise GateError(f"cannot read {path}: {exc}") from exc
    if not text.strip():
        raise GateError(f"{path} is empty (a baseline dump that was never filled in cannot gate anything)")
    try:
        json.loads(text)
    except ValueError as exc:
        raise GateError(f"{path} is not valid JSON: {exc}") from exc
    return text


_PROMOTIONS = {("int", "long"), ("int", "float"), ("int", "double"), ("long", "float"), ("long", "double"), ("float", "double"),
               ("string", "bytes"), ("bytes", "string")}


def _named_records(node, out: dict) -> dict:
    if isinstance(node, dict):
        if node.get("type") == "record" and "name" in node and node["name"] not in out:
            out[node["name"]] = node
        for v in node.values():
            _named_records(v, out)
    elif isinstance(node, list):
        for v in node:
            _named_records(v, out)
    return out


def _type_label(t) -> str:
    if isinstance(t, str):
        return t
    if isinstance(t, list):
        return "[" + ", ".join(_type_label(x) for x in t) + "]"
    if isinstance(t, dict):
        return _type_label(t.get("logicalType") or t.get("type")) if not isinstance(t.get("type"), (dict, list)) else _type_label(t["type"])
    return str(t)


def _compatible_type(old, new) -> bool:
    """A conservative judgement used only to word the explanation (the real verdict is Avro's)."""
    o, n = _type_label(old), _type_label(new)
    return o == n or (o, n) in _PROMOTIONS or (isinstance(new, list) and any(_type_label(x) == o for x in new))


def explain(baseline_text: str, proposed_text: str) -> list[str]:
    """Human-readable reasons, per record and field, why proposed cannot read baseline data (BACKWARD)."""
    old, new = _named_records(json.loads(baseline_text), {}), _named_records(json.loads(proposed_text), {})
    msgs: list[str] = []
    for rec_name, new_rec in new.items():
        old_rec = old.get(rec_name)
        if not old_rec:
            continue
        old_fields = {f["name"]: f for f in old_rec.get("fields", [])}
        new_fields = {f["name"]: f for f in new_rec.get("fields", [])}
        added = [n for n in new_fields if n not in old_fields]
        removed = [n for n in old_fields if n not in new_fields]
        for n in added:
            if "default" not in new_fields[n]:
                hint = f" (together with the removal of '{removed[0]}' this looks like a rename '{removed[0]}' -> '{n}')" if len(removed) == 1 and len(added) == 1 else ""
                msgs.append(f"{rec_name}.{n}: field added WITHOUT a default; messages written before the change have no value for it{hint}")
        for n, f in new_fields.items():
            if n in old_fields and not _compatible_type(old_fields[n]["type"], f["type"]) and "default" not in f:
                msgs.append(f"{rec_name}.{n}: type changed from {_type_label(old_fields[n]['type'])} to {_type_label(f['type'])} and the new type cannot read the old one")
    return msgs


def check_local(baseline_text: str, proposed_text: str) -> tuple[bool, list[str]]:
    """BACKWARD: can a reader with `proposed` decode data written with `baseline`?"""
    try:
        from avro.compatibility import ReaderWriterCompatibilityChecker, SchemaCompatibilityType
        from avro.schema import parse
    except ImportError as exc:  # pragma: no cover
        raise GateError("the 'avro' package is required (pip install avro)") from exc
    try:
        writer, reader = parse(baseline_text), parse(proposed_text)
    except Exception as exc:  # avro raises SchemaParseException and friends
        raise GateError(f"schema does not parse as Avro: {exc}") from exc
    result = ReaderWriterCompatibilityChecker().get_compatibility(reader=reader, writer=writer)
    if result.compatibility == SchemaCompatibilityType.compatible:
        return True, []
    raw = sorted({f"{message} (at {location})" for message, location in zip(result.messages, result.locations)})
    # Avro reports a nested record inside a union only as "reader union lacking writer type"; name the actual fields
    return False, explain(baseline_text, proposed_text) + raw or ["incompatible"]


def check_registry(registry: str, subject: str, proposed_text: str, timeout: float = 15.0) -> tuple[bool, list[str]]:
    """Ask the Schema Registry's compatibility endpoint (uses the subject's configured level, BACKWARD by default)."""
    url = f"{registry.rstrip('/')}/compatibility/subjects/{subject}/versions/latest?verbose=true"
    body = json.dumps({"schema": proposed_text}).encode()
    req = urllib.request.Request(url, data=body, method="POST", headers={"Content-Type": "application/vnd.schemaregistry.v1+json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.load(resp)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:300]
        raise GateError(f"registry answered HTTP {exc.code}: {detail}") from exc
    except (urllib.error.URLError, OSError, ValueError) as exc:
        raise GateError(f"registry at {registry} is unreachable or answered garbage: {exc}") from exc
    ok = payload.get("is_compatible") is True
    return ok, [] if ok else list(payload.get("messages") or ["incompatible"])


def gate(baseline: Path, proposed: Path, registry: Optional[str] = None, subject: str = DEFAULT_SUBJECT) -> tuple[bool, list[str], str]:
    proposed_text = read_schema_text(proposed)
    if registry:
        ok, msgs = check_registry(registry, subject, proposed_text)
        return ok, msgs, f"registry:{registry}"
    ok, msgs = check_local(read_schema_text(baseline), proposed_text)
    return ok, msgs, "local"


def self_test(baseline: Path, registry: Optional[str]) -> tuple[list[dict], bool]:
    """Run every fixture in schemas/fixtures/manifest.json. A fixture passes when the gate's verdict matches `expect`."""
    manifest = json.loads((FIXTURES / "manifest.json").read_text())
    base_text = read_schema_text(baseline)
    rows: list[dict] = []
    ok_all = True
    cases = [{"file": baseline.name, "path": baseline, "expect": "compatible", "why": "the baseline against itself"}]
    cases += [{**m, "path": FIXTURES / m["file"]} for m in manifest["fixtures"]]
    for case in cases:
        text = read_schema_text(case["path"])
        local_ok, local_msgs = check_local(base_text, text)
        row = {
            "fixture": case["file"], "expect": case["expect"], "why": case["why"],
            "local_verdict": "compatible" if local_ok else "breaking", "local_messages": local_msgs,
        }
        passed = row["local_verdict"] == case["expect"]
        if registry:
            reg_ok, reg_msgs = check_registry(registry, DEFAULT_SUBJECT, text)
            row["registry_verdict"] = "compatible" if reg_ok else "breaking"
            row["registry_messages"] = reg_msgs
            passed = passed and row["registry_verdict"] == case["expect"]
        row["passed"] = passed
        ok_all &= passed
        rows.append(row)
    return rows, ok_all


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--baseline", type=Path, default=ROOT / "schemas" / "transactions_v2.avsc")
    ap.add_argument("--proposed", type=Path)
    ap.add_argument("--registry", help="Schema Registry URL: use its /compatibility endpoint instead of the local check")
    ap.add_argument("--subject", default=DEFAULT_SUBJECT)
    ap.add_argument("--self-test", action="store_true", help="run the fixtures: breaking ones must fail, compatible ones must pass")
    ap.add_argument("--json", type=Path, help="with --self-test: write the per-fixture results here")
    args = ap.parse_args(argv)

    try:
        if args.self_test:
            rows, ok = self_test(args.baseline, args.registry)
            for r in rows:
                extra = f"  registry={r['registry_verdict']}" if "registry_verdict" in r else ""
                print(f"{'PASS' if r['passed'] else 'FAIL'}  {r['fixture']:<42} expect={r['expect']:<10} local={r['local_verdict']}{extra}")
                for m in r["local_messages"][:2]:
                    print(f"        {m}")
            if args.json:
                sys.path.insert(0, str(ROOT / "oncall" / "tools"))
                from provenance import write_result  # noqa: E402

                breaking = [r for r in rows if r["expect"] == "breaking"]
                compatible = [r for r in rows if r["expect"] == "compatible"]
                write_result(args.json.name, "python scripts/check_schema_compat.py --self-test --json " + str(args.json), {
                    "compatibility_level": "BACKWARD",
                    "engines": ["local (apache avro)"] + (["schema registry /compatibility"] if args.registry else []),
                    "breaking_fixtures_rejected": sum(1 for r in breaking if r["passed"]),
                    "breaking_fixtures_total": len(breaking),
                    "compatible_fixtures_accepted": sum(1 for r in compatible if r["passed"]),
                    "compatible_fixtures_total": len(compatible),
                    "all_as_expected": ok,
                    "fixtures": rows,
                })
            print("SELF-TEST " + ("PASSED" if ok else "FAILED"))
            return EXIT_OK if ok else EXIT_BREAKING
        if not args.proposed:
            ap.error("--proposed is required (or use --self-test)")
        ok, msgs, engine = gate(args.baseline, args.proposed, args.registry, args.subject)
    except GateError as exc:
        print(f"SCHEMA GATE ERROR (failing closed): {exc}", file=sys.stderr)
        return EXIT_ERROR
    if ok:
        print(f"Schema is BACKWARD compatible ({engine}).")
        return EXIT_OK
    print(f"BREAKING SCHEMA CHANGE ({engine}): consumers using the proposed schema cannot read data written with the baseline.", file=sys.stderr)
    for m in msgs:
        print(f"  - {m}", file=sys.stderr)
    return EXIT_BREAKING


if __name__ == "__main__":
    sys.exit(main())
