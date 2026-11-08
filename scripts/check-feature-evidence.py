#!/usr/bin/env python3
"""Require exact executed tests for optional experimental feature scenarios."""
import argparse
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


def evaluate(catalog, log):
    if catalog.get("version") != 1 or not catalog.get("scenarios"):
        raise ValueError("invalid optional feature catalog")
    names = re.findall(r"^test ([\w:]+) \.\.\. ok$", log, re.MULTILINE)
    passed = set(names)
    bad = (len(passed) != len(names) or "test result: FAILED" in log
           or "error: could not compile" in log
           or bool(re.search(r"^test [\w:]+ \.\.\. (FAILED|ignored)", log, re.MULTILINE)))
    results, ids = [], set()
    for item in catalog["scenarios"]:
        if not item.get("tests") or not item.get("scope") or not item.get("id") or item["id"] in ids:
            raise ValueError("invalid or repeated scenario")
        ids.add(item["id"])
        missing = sorted(set(item["tests"]) - passed)
        results.append({**item, "status": "Failed" if bad or missing else "Passed", "missing": missing})
    return {"validation_status": "Passed" if all(r["status"] == "Passed" for r in results) else "Failed",
            "overall_status": "Partial", "claim": catalog["claim"], "scenarios": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    catalog = ROOT / "contracts/optional-functional.json"
    raw = args.results.read_bytes()
    result = evaluate(json.loads(catalog.read_text()), raw.decode())
    result["evidence"] = {"path": str(args.results), "sha256": hashlib.sha256(raw).hexdigest(),
                          "catalog_sha256": hashlib.sha256(catalog.read_bytes()).hexdigest()}
    text = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.write_text(text)
    else:
        print(text, end="")
    return 0 if result["validation_status"] == "Passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
