#!/usr/bin/env python3
"""Verify the MQTT5 numbered text index against explicit pinned OASIS HTML.

The text index is preparation for independent review, not conformance approval.
This command never downloads a source or infers semantic applicability.
"""
import argparse
import hashlib
from html.parser import HTMLParser
import importlib.util
import json
from pathlib import Path
import re


spec = importlib.util.spec_from_file_location("audit_text", Path(__file__).with_name("audit-mqtt311.py"))
audit_text = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit_text)


def inventory(raw, source):
    if hashlib.sha256(raw).hexdigest() != source["sha256"]:
        raise ValueError("source HTML SHA256 does not match pinned OASIS source")

    class Main(HTMLParser):
        def __init__(self):
            super().__init__()
            self.finished = False
            self.parts = []

        def handle_starttag(self, tag, attrs):
            if dict(attrs).get("class") == "AppendixHeading1":
                self.finished = True

        def handle_data(self, value):
            if not self.finished:
                self.parts.append(value)

    parser = Main()
    parser.feed(raw.decode("cp1252"))
    if not parser.finished:
        raise ValueError("main-body/appendix boundary missing")
    identifiers = sorted(set(re.findall(
        r"\[(MQTT[-–][0-9]+(?:[-.][0-9]+)+\.?-[0-9]+)\]", "".join(parser.parts),
    )))
    if not identifiers:
        raise ValueError("empty MQTT5 numbered source inventory")
    result = audit_text.numbered_inventory(raw, {"source": source, "clause_inventory": identifiers})
    result["claim"] = (
        "MQTT5 numbered main-body text index only; not a semantic MUST inventory, "
        "role/applicability review, table/FSM proof or independent conformance approval."
    )
    result["identifier_aliases"] = {}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-html", type=Path, required=True)
    parser.add_argument("--text-inventory", type=Path, default=Path("contracts/mqtt5-text.json"))
    args = parser.parse_args()
    try:
        controlled = json.loads(args.text_inventory.read_text())
        actual = inventory(args.source_html.read_bytes(), controlled["source"])
        if actual != controlled:
            raise ValueError("controlled MQTT5 text differs from pinned main-body source")
    except (OSError, ValueError, KeyError) as error:
        parser.exit(1, f"MQTT5 source verification failed: {error}\n")
    identifiers = {c for p in actual["paragraphs"] for c in p["clauses"]}
    print(f"Pinned MQTT5 numbered index: Passed ({len(identifiers)} identifiers, {len(actual['paragraphs'])} paragraphs)")
    print("Semantic/full conformance approval: Partial")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
