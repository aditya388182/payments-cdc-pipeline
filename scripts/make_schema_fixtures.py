#!/usr/bin/env python3
"""Writes schemas/transactions_v2.avsc (the baseline) from the live Schema Registry and derives the gate's fixtures
from it by small mutations of the nested row record ("Value", the shape of `before` / `after`).

  python scripts/make_schema_fixtures.py [--registry http://localhost:8081]

Why this exists: the committed baseline was a 0-byte file (docs/oncall/DECISIONS.md D-006), so the CI gate could not work.
"""
from __future__ import annotations

import argparse
import copy
import json
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SUBJECT = "payments.public.transactions-value"


def fetch_latest(registry: str) -> dict:
    with urllib.request.urlopen(f"{registry}/subjects/{SUBJECT}/versions/latest", timeout=15) as r:
        return json.loads(json.load(r)["schema"])


def value_record(node):
    """The first record named 'Value' (the row inside before/after)."""
    if isinstance(node, dict):
        if node.get("type") == "record" and node.get("name") == "Value":
            return node
        for v in node.values():
            found = value_record(v)
            if found:
                return found
    elif isinstance(node, list):
        for v in node:
            found = value_record(v)
            if found:
                return found
    return None


def mutate(base: dict, fn) -> dict:
    s = copy.deepcopy(base)
    fn(value_record(s))
    return s


def field(rec: dict, name: str) -> dict:
    return next(f for f in rec["fields"] if f["name"] == name)


def add_optional(rec):
    rec["fields"].append({"name": "channel", "type": ["null", "string"], "default": None})


def remove_field(rec):
    rec["fields"] = [f for f in rec["fields"] if f["name"] != "event_type"]


def rename_currency(rec):
    field(rec, "currency")["name"] = "ccy"


def amount_to_string(rec):
    field(rec, "amount_minor")["type"] = "string"


def add_required(rec):
    rec["fields"].append({"name": "risk_level", "type": "string"})


FIXTURES = [
    ("compatible_add_optional_field.avsc", add_optional, "compatible",
     "a new nullable field WITH a default (what PR #4 added: `channel`). Old messages simply lack it and the reader fills the default."),
    ("compatible_remove_field.avsc", remove_field, "compatible",
     "a removed field is NOT breaking under BACKWARD (the reader ignores the extra data in old messages); it would be breaking under FORWARD."),
    ("breaking_rename_field.avsc", rename_currency, "breaking",
     "`currency` renamed to `ccy` (PR #5, the red run): a new field without a default that old messages do not have."),
    ("breaking_type_change.avsc", amount_to_string, "breaking",
     "`amount_minor` changed from long to string: old long values cannot be read as string."),
    ("breaking_add_required_field.avsc", add_required, "breaking",
     "a new field WITHOUT a default: old messages have no value for it."),
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--registry", default="http://localhost:8081")
    args = ap.parse_args()
    base = fetch_latest(args.registry)
    (ROOT / "schemas" / "transactions_v2.avsc").write_text(json.dumps(base, indent=2) + "\n")
    fx = ROOT / "schemas" / "fixtures"
    fx.mkdir(parents=True, exist_ok=True)
    manifest = []
    for name, fn, expect, why in FIXTURES:
        (fx / name).write_text(json.dumps(mutate(base, fn), indent=2) + "\n")
        manifest.append({"file": name, "expect": expect, "why": why})
    (fx / "manifest.json").write_text(json.dumps({"fixtures": manifest}, indent=2) + "\n")
    print(f"wrote schemas/transactions_v2.avsc and {len(FIXTURES)} fixtures")


if __name__ == "__main__":
    main()
