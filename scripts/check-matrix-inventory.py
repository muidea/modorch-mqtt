#!/usr/bin/env python3
"""Validate the F67-04 matrix inventory in the closure plan.

The inventory is a human-readable view of contracts/protocol-review.json. This
check prevents missing, extra, duplicated, stale or reordered baseline rows. It
does not approve the remaining combinations: those require F67-02/F67-03 review
and must be added as separate decisions rather than silently appended here.
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLAN = Path("docs/f6-f7-closure-plan.md")
MANIFEST = Path("contracts/protocol-review.json")
HEADING = "### F67-04 现有 18 矩阵覆盖对照（基线）"
ROW = re.compile(r"^\| (\d+) \| `([^`]+)` \| ([^|]+) \| (\d+) \| (.+) \|$", re.M)


def read(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result
    return json.loads(path.read_text(), object_pairs_hook=unique)


def dimensions(value):
    return " / ".join(f"{key}={count}" for key, count in value.items())


def section(text):
    try:
        start = text.index(HEADING) + len(HEADING)
    except ValueError as error:
        raise ValueError("missing F67-04 matrix inventory heading") from error
    rest = text[start:]
    end = rest.find("\n## ")
    return rest if end == -1 else rest[:end]


def evaluate(manifest, text):
    matrices = manifest["state_fault_matrix"]
    body = section(text)
    if "**18 个矩阵 / 222 个组合**" not in body:
        raise ValueError("matrix inventory must state 18 matrices / 222 combinations")
    rows = ROW.findall(body)
    if len(rows) != len(matrices):
        raise ValueError("matrix inventory row count differs from protocol-review.json")
    seen = set()
    for index, (number, identifier, row_dimensions, cases, tests) in enumerate(rows):
        expected = matrices[index]
        if int(number) != index + 1:
            raise ValueError("matrix inventory numbering is not contiguous")
        if identifier != expected["id"]:
            raise ValueError(f"matrix inventory order or id differs: {identifier}")
        if identifier in seen:
            raise ValueError(f"duplicate matrix inventory row: {identifier}")
        seen.add(identifier)
        if row_dimensions.strip() != dimensions(expected["dimensions"]):
            raise ValueError(f"matrix dimensions are stale: {identifier}")
        if int(cases) != expected["cases"]:
            raise ValueError(f"matrix case count is stale: {identifier}")
        for test in expected["tests"]:
            if f"`{test}`" not in tests:
                raise ValueError(f"matrix evidence test is missing: {identifier}:{test}")
    if sum(matrix["cases"] for matrix in matrices) != 222:
        raise ValueError("protocol-review.json matrix total is no longer 222")
    return {
        "validation_status": "Passed",
        "overall_status": "Partial",
        "baseline": "covered",
        "decision_status": "pending-semantic-review",
        "matrices": len(matrices),
        "cases": sum(matrix["cases"] for matrix in matrices),
    }


def main():
    result = evaluate(read(ROOT / MANIFEST), (ROOT / PLAN).read_text())
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
