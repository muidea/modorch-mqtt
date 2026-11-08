#!/usr/bin/env python3
"""Match the controlled MQTT scenario map to actual Cargo test evidence (offline)."""
import argparse
import hashlib
import json
from pathlib import Path
import re


def evaluate_audit(audit, passed, bad_log):
    """Evaluate bounded audit traces without promoting aggregate conformance."""
    if audit["version"] != 1 or not audit["inventory_method"] or not audit["claim"]:
        raise ValueError("invalid normative audit")
    rows = []
    ids = set()
    for group in ("unnumbered", "structural"):
        if not audit[group]:
            raise ValueError("audit groups must not be empty")
        for item in audit[group]:
            if not item["id"] or item["id"] in ids:
                raise ValueError("duplicate or missing audit ID")
            ids.add(item["id"])
            if (
                item["role"] not in {"Server", "Client", "Both", "Context"}
                or item["coverage"] not in {"TargetedEvidence", "NotRun", "Context", "Informative", "Definition", "ClientOnly"}
                or not item["section"] or not item["scope"]
                or (group == "unnumbered" and not item["source_text"])
            ):
                raise ValueError("invalid audit role, coverage or scope")
            targeted = item["coverage"] == "TargetedEvidence"
            if targeted != bool(item["tests"]):
                raise ValueError("only targeted audit items must have test mappings")
            missing = sorted(set(item["tests"]) - passed)
            rows.append({
                **item,
                "validation_status": ("Failed" if missing or bad_log else "Passed") if targeted else "NotRun",
                "missing_pass_evidence": missing,
            })
    return rows


def exit_code(result, require_complete=False):
    """Keep passing development scenarios separate from full release approval."""
    return 0 if (
        result["validation_status"] == "Passed"
        and (not require_complete or result["overall_status"] == "Passed")
    ) else 1


def evaluate(matrix, log, audit=None):
    inventory = matrix["clause_inventory"]
    if matrix["version"] != 1 or not inventory or len(set(inventory)) != len(inventory):
        raise ValueError("invalid or duplicate clause inventory")
    scenarios = matrix["scenarios"]
    if not scenarios or len({s["id"] for s in scenarios}) != len(scenarios):
        raise ValueError("empty or duplicate scenarios")
    mapped = set()
    for scenario in scenarios:
        if not scenario["tests"] or not scenario["clauses"] or not scenario["scope"]:
            raise ValueError("every scenario needs tests, clauses and a bounded scope")
        mapped.update(scenario["clauses"])
    if not mapped <= set(inventory) or set(matrix["unmapped_clauses"]) != set(inventory) - mapped:
        raise ValueError("clause inventory and mappings disagree")
    applicability = matrix.get("unmapped_applicability", {})
    if "unmapped_applicability" in matrix:
        if set(applicability) != set(matrix["unmapped_clauses"]) or any(
            entry.get("category") not in {"ClientOnly", "OptionalWebSocket", "ConditionalBridge"}
            or not entry.get("reason")
            for entry in applicability.values()
        ):
            raise ValueError("unmapped applicability must cover exactly the unmapped clauses with reasons")
    passed_names = re.findall(r"^test ([\w:]+) \.\.\. ok$", log, re.MULTILINE)
    passed = set(passed_names)
    failed = set(re.findall(r"^test ([\w:]+) \.\.\. FAILED$", log, re.MULTILINE))
    ignored = set(re.findall(r"^test ([\w:]+) \.\.\. ignored.*$", log, re.MULTILINE))
    bad_log = bool(
        failed or passed & ignored or len(passed_names) != len(passed)
        or "test result: FAILED" in log or "error: could not compile" in log
    )
    results = []
    for scenario in scenarios:
        missing = sorted(set(scenario["tests"]) - passed)
        results.append({
            "id": scenario["id"],
            "status": "Failed" if missing or bad_log else "Passed",
            "scope": scenario["scope"],
            "clauses": scenario["clauses"],
            "tests": scenario["tests"],
            "missing_pass_evidence": missing,
        })
    audit_rows = evaluate_audit(audit, passed, bad_log) if audit else []
    valid = (
        not bad_log and all(row["status"] == "Passed" for row in results)
        and all(row["validation_status"] != "Failed" for row in audit_rows)
    )
    return {
        "validation_status": "Passed" if valid else "Failed",
        "overall_status": "Partial" if valid else "Failed",
        "claim": matrix["claim"],
        "scenarios": results,
        "audit_coverage": audit_rows,
        # A targeted passing scenario is not full approval of a normative clause.
        "clause_coverage": [
            {
                "id": clause,
                "coverage": "TargetedEvidence" if valid and clause in mapped else "NotRun",
                **({"applicability": applicability[clause]} if clause in applicability else {}),
            }
            for clause in inventory
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", type=Path, default=Path("contracts/mqtt311.json"))
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--audit", type=Path, default=Path("contracts/mqtt311-audit.json"))
    parser.add_argument(
        "--require-complete", action="store_true",
        help="Fail if normative conformance is Partial, even when all mapped scenarios pass",
    )
    args = parser.parse_args()
    raw_matrix, raw_log = args.matrix.read_bytes(), args.results.read_bytes()
    raw_audit = args.audit.read_bytes()
    matrix, audit = json.loads(raw_matrix), json.loads(raw_audit)
    if audit["source"]["sha256"] != matrix["source"]["sha256"]:
        raise ValueError("audit and numbered matrix refer to different normative sources")
    result = evaluate(matrix, raw_log.decode(), audit)
    result["inputs"] = {
        "matrix": str(args.matrix), "matrix_sha256": hashlib.sha256(raw_matrix).hexdigest(),
        "results": str(args.results), "results_sha256": hashlib.sha256(raw_log).hexdigest(),
        "audit": str(args.audit), "audit_sha256": hashlib.sha256(raw_audit).hexdigest(),
    }
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(f"Scenario evidence {result['validation_status']}; normative conformance {result['overall_status']}")
    return exit_code(result, args.require_complete)


if __name__ == "__main__":
    raise SystemExit(main())
