#!/usr/bin/env python3
"""Verify the controlled unnumbered MUST paragraph inventory against pinned HTML.

This extracts candidates, not semantic approval. It deliberately excludes the
non-normative appendices, whose repeated statements omit clause identifiers.
The source is an explicit local input; the script never downloads documents.
"""
import argparse
import hashlib
from html.parser import HTMLParser
import json
from pathlib import Path
import re


METHOD = "main-body-p-and-h1-h6-uppercase-normative-without-MQTT-ID-v1"


class Paragraphs(HTMLParser):
    def __init__(self):
        super().__init__()
        self.active = None
        self.finished = False
        self.items = []

    def handle_starttag(self, tag, attrs):
        if self.finished:
            return
        if dict(attrs).get("class") == "AppendixHeading1":
            self.finished = True
            self.active = None
        elif tag in {"p", "h1", "h2", "h3", "h4", "h5", "h6"}:
            self.active = []

    def handle_endtag(self, tag):
        if tag in {"p", "h1", "h2", "h3", "h4", "h5", "h6"} and self.active is not None:
            value = re.sub(r"\s+", " ", "".join(self.active)).strip()
            if re.search(r"\b(MUST|SHALL|REQUIRED)\b", value) and not re.search(r"MQTT[-–]", value):
                self.items.append(value)
            self.active = None

    def handle_data(self, text):
        if self.active is not None:
            self.active.append(text)


def extract(raw, expected_digest):
    if hashlib.sha256(raw).hexdigest() != expected_digest:
        raise ValueError("source HTML SHA256 does not match pinned OASIS source")
    parser = Paragraphs()
    parser.feed(raw.decode("cp1252"))
    if not parser.finished:
        raise ValueError("main-body/appendix boundary missing")
    return parser.items


def verify(audit, raw):
    if audit["version"] != 1 or audit["inventory_method"] != METHOD:
        raise ValueError("unknown audit inventory method")
    paragraphs = extract(raw, audit["source"]["sha256"])
    if paragraphs != [item["source_text"] for item in audit["unnumbered"]]:
        raise ValueError("unnumbered paragraph inventory differs from pinned source")
    return paragraphs



def numbered_inventory(raw, matrix):
    """Preserve whole numbered paragraphs, including shared/preceding obligations.

    This is a textual index, not a semantic split or conformance decision.
    """
    extract(raw, matrix["source"]["sha256"])
    class Blocks(HTMLParser):
        def __init__(self):
            super().__init__()
            self.blocks = []
            self.items = []
            self.finished = False

        def handle_starttag(self, tag, attrs):
            if dict(attrs).get("class") == "AppendixHeading1":
                self.finished = True
            if not self.finished and tag in {"p", "li", "h1", "h2", "h3", "h4", "h5", "h6"}:
                self.blocks.append((tag, []))

        def handle_data(self, text):
            if not self.finished:
                for _, fragments in self.blocks:
                    fragments.append(text)

        def handle_endtag(self, tag):
            if self.blocks and tag == self.blocks[-1][0]:
                _, fragments = self.blocks.pop()
                self.items.append(re.sub(r"\s+", " ", "".join(fragments)).strip())

    parser = Blocks()
    parser.feed(raw.decode("cp1252"))
    rows = []
    for text in parser.items:
        ids = re.findall(r"\[(MQTT[-–][0-9]+(?:[-.][0-9]+)+\.?-[0-9]+)\]", text)
        if ids:
            row = {"clauses": list(dict.fromkeys(i.replace("MQTT-3.3.1.-1", "MQTT-3.3.1-1") for i in ids)), "source_text": text}
            if row not in rows:
                rows.append(row)
    found = {clause for row in rows for clause in row["clauses"]}
    if found != set(matrix["clause_inventory"]):
        raise ValueError("numbered paragraph inventory differs from clause inventory")
    return {"version": 1, "source": matrix["source"],
            "claim": "Whole numbered main-body paragraphs; not semantic approval. Unnumbered continuations and tables remain in mqtt311-audit.json.",
            "identifier_aliases": {"MQTT-3.3.1.-1": "MQTT-3.3.1-1"},
            "paragraphs": rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-html", type=Path, required=True)
    parser.add_argument("--audit", type=Path, default=Path("contracts/mqtt311-audit.json"))
    parser.add_argument("--matrix", type=Path, default=Path("contracts/mqtt311.json"))
    parser.add_argument("--text-inventory", type=Path, default=Path("contracts/mqtt311-text.json"))
    args = parser.parse_args()
    try:
        raw = args.source_html.read_bytes()
        paragraphs = verify(json.loads(args.audit.read_text()), raw)
        numbered = numbered_inventory(raw, json.loads(args.matrix.read_text()))
        if numbered != json.loads(args.text_inventory.read_text()):
            raise ValueError("controlled numbered text differs from pinned source")
    except (OSError, ValueError, KeyError) as error:
        parser.exit(1, f"Audit source verification failed: {error}\n")
    print(f"Pinned main-body unnumbered normative candidate inventory: Passed ({len(paragraphs)} paragraphs)")
    print("Semantic/full conformance approval: Partial")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
