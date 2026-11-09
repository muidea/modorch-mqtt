#!/usr/bin/env python3
"""Check current-source evidence and independent reviews for the MQTT311 release.

Offline integrity gate, not a signature verifier or a substitute for reviewers.
Use --prepare to create an unapproved evidence plan; default exits nonzero until
all required reviews and machine-verifiable evidence have been supplied.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re


DESIGN = "docs/modorch_mqtt_broker_full_design_0.0.3.md"
POLICY = "contracts/release-policy.json"
PROJECT = "contracts/project-must.json"
PROFILE = "mqtt311-tcp-tls-sqlite"
EXCLUDED = {"ClientOnly", "OptionalWebSocket", "ConditionalBridge"}
GATES = {
    "F8-BUILD": ["workspace", "rustdoc", "host-smokes", "toml-cli"],
    "F8-COMPAT": ["framework-fingerprint", "msrv-1.85", "empty-default-features", "toml-only"],
    "F8-INTEROP": ["rumqttc", "paho-tcp", "paho-tls", "fault-matrix"],
    "F8-SECURITY": ["auth-acl", "tls", "namespace-isolation", "resource-abuse", "review"],
    "F8-PERFORMANCE": ["tls-auth-storage-qos2", "long-run-rss", "slow-consumer", "loaded-shutdown", "slo-review"],
    "F8-RECOVERY": ["sigkill", "capacity-peak", "hardware-power-loss", "filesystem-matrix"],
    "F8-PLATFORMS": ["linux", "macos", "windows"],
    "F8-TOPIC-CROSSIMPL": ["modorch", "magicCommon"],
    "F8-REMOTE-CI": ["immutable-run"],
    "F8-INDEPENDENT-AGENT": ["integration", "lifecycle", "faults", "report"],
}


def read_json(path):
    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError(f"duplicate JSON key: {key}")
            value[key] = item
        return value
    return json.loads(path.read_text(), object_pairs_hook=unique)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def object_digest(value):
    return digest(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode())


def source_inputs(root):
    """Discover inputs, including additions; validation outputs cannot hash themselves."""
    paths = [root / name for name in ("Cargo.toml", "Cargo.lock", "README.md")]
    for directory in ("crates", "examples", "scripts", "contracts", "docs", ".github", ".agents/skills"):
        paths.extend(p for p in (root / directory).rglob("*") if p.is_file()
                     and p.suffix in {".rs", ".toml", ".py", ".sh", ".json", ".md", ".yml", ".yaml", ".pem", ".txt", ".lock", ".in"}
                     and not p.relative_to(root).as_posix().startswith("docs/validation/")
                     and "__pycache__" not in p.parts)
    return {p.relative_to(root).as_posix(): digest(p.read_bytes()) for p in sorted(set(paths))}


def local_file(root, relative):
    path = Path(relative)
    if path.is_absolute() or ".." in path.parts or not relative:
        raise ValueError(f"unsafe evidence path: {relative}")
    candidate = root / path
    if not candidate.resolve().is_relative_to(root.resolve()) or not candidate.is_file():
        raise ValueError(f"missing or external evidence: {relative}")
    return candidate


def checked_file(root, reference):
    path = local_file(root, reference["path"])
    if digest(path.read_bytes()) != reference["sha256"]:
        raise ValueError(f"evidence digest mismatch: {reference['path']}")
    return path


def file_identity(path):
    """Compare actual files, including dot paths, symlinks and hard links."""
    stat = path.stat()
    return stat.st_dev, stat.st_ino


def excluded_evidence(root, references):
    identities, digests = set(), set()
    for reference in references:
        path = checked_file(root, reference)
        identities.add(file_identity(path))
        digests.add(reference['sha256'])
    return identities, digests


def execution_file(root, reference, forbidden):
    path = local_file(root, reference['path'])
    if file_identity(path) in forbidden[0]:
        raise ValueError('execution evidence cannot be a review or the report itself')
    path = checked_file(root, reference)
    if reference['sha256'] in forbidden[1]:
        raise ValueError('execution evidence cannot copy a review or the report itself')
    return path


def unique_rows(rows):
    if not rows or len({row["id"] for row in rows}) != len(rows):
        raise ValueError("empty or duplicate obligation IDs")
    return {row["id"]: row for row in rows}


def catalog(root):
    project = read_json(root / PROJECT)
    policy = read_json(root / POLICY)
    matrix = read_json(root / "contracts/mqtt311.json")
    audit = read_json(root / "contracts/mqtt311-audit.json")
    text = read_json(root / "contracts/mqtt311-text.json")
    if any(data["version"] != 1 for data in (project, policy, matrix, audit, text)):
        raise ValueError("unsupported catalog version")
    if policy["profile"] != PROFILE or policy["gates"] != GATES:
        raise ValueError("release policy differs from supported required gates")
    if matrix["source"] != text["source"] or matrix["source"]["sha256"] != audit["source"]["sha256"]:
        raise ValueError("normative sources disagree")
    if set(matrix["clause_inventory"]) != {c for p in text["paragraphs"] for c in p["clauses"]}:
        raise ValueError("normative text and numbered inventory disagree")
    # These official clause markers follow their normative lists; a paragraph
    # containing only the marker must never be offered as the whole obligation.
    for clause, count, role in [
        ("MQTT-4.3.2-1", 3, "Sender"), ("MQTT-4.3.2-2", 2, "Receiver"),
        ("MQTT-4.3.3-1", 6, "Sender"), ("MQTT-4.3.3-2", 4, "Receiver"),
    ]:
        groups = [p for p in text["paragraphs"] if clause in p["clauses"]]
        if (len(groups) != 1 or len(groups[0].get("preceding_normative", [])) != count
                or not groups[0].get("preceding_context", "").endswith(role)
                or any(not re.search(r"\bMUST\b", p) for p in groups[0]["preceding_normative"])):
            raise ValueError(f"missing QoS obligation text or sender/receiver context: {clause}")
    section = (root / DESIGN).read_text().split("## 15.", 1)[1].split("\n## ", 1)[0]
    design_rows = dict(re.findall(r"^\| (MQTT-[A-Z]+-\d+) \| (.*?) \|", section, re.MULTILINE))
    rows = unique_rows(project["obligations"])
    if set(rows) != set(design_rows) | {"MQTT-CONFIG-001"}:
        raise ValueError("project MUST IDs differ from design section 15 plus CONFIG-001")
    for key, row in rows.items():
        if key in design_rows and row["scope"] != design_rows[key]:
            raise ValueError(f"stale project MUST text: {key}")
        if not row["tests"] or len(row["tests"]) != len(set(row["tests"])) or not row["sources"]:
            raise ValueError(f"project MUST lacks bounded test/source mappings: {key}")
        for source in row["sources"]:
            local_file(root, source)
    excluded = matrix["unmapped_applicability"]
    if set(excluded) != set(matrix["unmapped_clauses"]) or any(
        item["category"] not in EXCLUDED or not item["reason"] for item in excluded.values()
    ):
        raise ValueError("unsupported normative applicability")
    for clause in matrix["clause_inventory"]:
        if clause in excluded:
            continue
        rows[clause] = {
            "id": clause, "scope": "Review whole paragraphs, continuations and related FSM; targeted traces alone are insufficient.",
            "paragraphs": [p for p in text["paragraphs"] if clause in p["clauses"]],
            "tests": sorted({t for s in matrix["scenarios"] if clause in s["clauses"] for t in s["tests"]}),
        }
    for item in audit["unnumbered"] + audit["structural"]:
        if item["coverage"] in {"TargetedEvidence", "NotRun"} and item["id"] != "UN-008":
            rows[item["id"]] = item
    for key, checks in GATES.items():
        rows[key] = {"id": key, "checks": checks, "scope": policy["scopes"][key]}
    return rows, excluded


def load_scenarios():
    spec = importlib.util.spec_from_file_location("release_scenarios", Path(__file__).with_name("check-conformance.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def evaluate(root, bundle):
    rows, excluded = catalog(root)
    current = source_inputs(root)
    if bundle["version"] != 1 or bundle["profile"] != PROFILE or not bundle["producer"].strip():
        raise ValueError("invalid bundle version/profile/producer")
    if bundle["source_inputs"] != current or bundle["source_digest"] != object_digest(current):
        raise ValueError("stale or incomplete source snapshot (including added/deleted files)")
    reviews = bundle["reviews"]
    if set(reviews) != set(rows):
        raise ValueError("review inventory differs from required obligations; omissions are not approvals")
    review_files = excluded_evidence(root, [ref for ref in reviews.values() if ref is not None])
    log_path = execution_file(root, bundle["results"], review_files)
    scenarios = load_scenarios().evaluate(
        read_json(root / "contracts/mqtt311.json"), log_path.read_text(),
        read_json(root / "contracts/mqtt311-audit.json"),
    )
    passed = set(re.findall(r"^test ([\w:]+) \.\.\. ok$", log_path.read_text(), re.MULTILINE))
    outcomes = []
    for key, row in rows.items():
        missing_tests = sorted(set(row.get("tests", [])) - passed)
        state, reason = "NotRun", "independent obligation review missing"
        reference = reviews[key]
        if missing_tests:
            state, reason = "Failed", f"missing exact Passed tests: {missing_tests}"
        if reference is not None:
            review = read_json(checked_file(root, reference))
            if (review["id"] != key or review["source_digest"] != bundle["source_digest"]
                    or review["obligation_sha256"] != object_digest(row)):
                raise ValueError(f"stale or wrong review: {key}")
            if not review["reviewer"].strip() or review["reviewer"].strip().casefold() == bundle["producer"].strip().casefold():
                raise ValueError(f"independent reviewer identity required: {key}")
            if review["decision"] not in {"Approved", "Rejected"} or not review["rationale"].strip() or not review["evidence"]:
                raise ValueError(f"invalid review decision/rationale/evidence: {key}")
            for evidence in review["evidence"]:
                execution_file(root, evidence, review_files)
            if key in GATES:
                report_ref = review["report"]
                if report_ref not in review["evidence"]:
                    raise ValueError(f"gate report must be bound as evidence: {key}")
                report = read_json(checked_file(root, report_ref))
                report_files = excluded_evidence(root, [report_ref])
                forbidden = (review_files[0] | report_files[0], review_files[1] | report_files[1])
                if report["gate_id"] != key or report["source_digest"] != bundle["source_digest"]:
                    raise ValueError(f"stale or wrong gate report: {key}")
                checks = unique_rows(report["checks"])
                if set(checks) != set(GATES[key]):
                    raise ValueError(f"gate checks incomplete: {key}")
                for check in checks.values():
                    if check["status"] not in {"Passed", "Failed", "NotRun"}:
                        raise ValueError(f"invalid gate check status: {key}")
                    if check["status"] != "Passed":
                        missing_tests.append(check["id"])
                    if not check["command"].strip() or not check["evidence"]:
                        raise ValueError(f"missing execution command/log: {key}")
                    for evidence in check["evidence"]:
                        execution_file(root, evidence, forbidden)
            state = "Passed" if review["decision"] == "Approved" and not missing_tests else "Failed"
            reason = review["rationale"] if state == "Passed" else "review rejected or required execution checks failed"
        outcomes.append({"id": key, "status": state, "reason": reason})
    invalid = scenarios["validation_status"] != "Passed" or any(r["status"] == "Failed" for r in outcomes)
    complete = not invalid and all(r["status"] == "Passed" for r in outcomes)
    normative = [r for r in outcomes if r["id"].startswith(("MQTT-1", "MQTT-2", "MQTT-3", "MQTT-4", "UN-", "TABLE-", "FSM-"))]
    normative_invalid = scenarios["validation_status"] != "Passed" or any(r["status"] == "Failed" for r in normative)
    normative_complete = not normative_invalid and all(r["status"] == "Passed" for r in normative)
    return {
        "profile": PROFILE, "source_digest": bundle["source_digest"],
        "validation_status": "Failed" if invalid else "Passed",
        "overall_status": "Failed" if invalid else "Passed" if complete else "Partial",
        "normative_status": "Failed" if normative_invalid else "Passed" if normative_complete else "Partial",
        "claim": "Local evidence integrity and independently reviewed declared scope; artifact provenance must be trusted separately.",
        "derived_aggregates": {"UN-008": "Passed" if normative_complete else "NotRun"},
        "excluded_clauses": excluded, "obligations": outcomes,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--bundle", type=Path)
    parser.add_argument("--check-catalog", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--prepare", action="store_true")
    parser.add_argument("--producer", default="local-developer")
    parser.add_argument("--results", type=Path)
    parser.add_argument("--allow-partial", action="store_true", help="Development inventory check only; never release approval")
    args = parser.parse_args()
    try:
        root = args.root.resolve()
        if args.check_catalog:
            rows, excluded = catalog(root)
            print(f"Release catalog: Passed ({len(rows)} obligations, {len(excluded)} scoped exclusions)")
            return 0
        if args.bundle is None:
            raise ValueError("--bundle is required for preparation/evaluation")
        if args.prepare:
            if args.bundle.exists():
                raise ValueError("refusing to overwrite an existing evidence plan")
            rows, _ = catalog(root)
            inputs = source_inputs(root)
            result_file = args.results.resolve().relative_to(root).as_posix()
            bundle = {"version": 1, "profile": PROFILE, "producer": args.producer,
                      "source_inputs": inputs, "source_digest": object_digest(inputs),
                      "results": {"path": result_file, "sha256": digest((root / result_file).read_bytes())},
                      "reviews": {key: None for key in rows},
                      "obligation_digests": {key: object_digest(row) for key, row in rows.items()}}
            args.bundle.write_text(json.dumps(bundle, ensure_ascii=False, indent=2) + "\n")
            print(f"Unapproved evidence plan: {len(rows)} obligations; all reviews NotRun")
            return 0
        result = evaluate(root, read_json(args.bundle))
        if args.output is None:
            raise ValueError("--output is required when evaluating")
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
        print(f"Release {result['overall_status']}; normative {result['normative_status']}; "
              f"{sum(r['status'] == 'Passed' for r in result['obligations'])}/{len(result['obligations'])} approved")
        return int(result["validation_status"] != "Passed" or (not args.allow_partial and result["overall_status"] != "Passed"))
    except (OSError, ValueError, KeyError, TypeError, AttributeError, IndexError) as error:
        if args.output:
            args.output.write_text(json.dumps({"validation_status": "Failed", "overall_status": "Failed", "error": str(error)}, ensure_ascii=False, indent=2) + "\n")
        parser.exit(1, f"Release evidence rejected: {error}\n")


if __name__ == "__main__":
    raise SystemExit(main())
