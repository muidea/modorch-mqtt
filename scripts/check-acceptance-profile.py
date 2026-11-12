#!/usr/bin/env python3
"""Validate the fixed MQTT3.1.1/MQTT5 acceptance scope against local contracts.

This check fixes implementation-side scope only. It deliberately does not
approve conformance: every profile must keep independent_review=NotRun and the
top-level independent_approvals=0 until separate review exists.
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROFILE = Path("contracts/acceptance-profile.json")
MANIFEST = Path("contracts/protocol-review.json")
FACADE_CARGO = Path("crates/modorch-mqtt/Cargo.toml")
ROOT_CARGO = Path("Cargo.toml")
TRANSPORTS = {"tcp", "tls", "websocket", "websocket-tls"}
STORAGE = {"memory", "sqlite"}
PLATFORM_EVIDENCE = {"TargetedEvidence", "NotRun", "Passed", "Failed"}
REQUIRED_NOT_CLAIMED = {"complete-conformance", "independent-approval", "production-slo", "remote-ci"}


def read(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result
    return json.loads(path.read_text(), object_pairs_hook=unique)


def local(root, name):
    path = (root / name).resolve()
    if not name or Path(name).is_absolute() or ".." in Path(name).parts or not path.is_relative_to(root) or not path.is_file():
        raise ValueError(f"missing or unsafe contract input: {name}")
    return path


def features(root):
    text = (root / FACADE_CARGO).read_text()
    section = re.search(r"^\[features\]\s*$(.*?)(?=^\[|\Z)", text, re.M | re.S)
    if not section:
        raise ValueError("facade Cargo.toml has no [features] section")
    return set(re.findall(r"^([A-Za-z0-9_-]+)\s*=", section.group(1), re.M))


def host_identity(root):
    text = (root / ROOT_CARGO).read_text()
    package = re.search(r"^\[workspace\.package\]\s*$(.*?)(?=^\[|\Z)", text, re.M | re.S)
    if not package:
        raise ValueError("Cargo.toml has no [workspace.package]")
    block = package.group(1)
    revision = re.search(r'modorch\s*=\s*\{[^}]*rev\s*=\s*"([0-9a-f]{40})"', text)
    rust_version = re.search(r'^rust-version\s*=\s*"([^"]+)"', block, re.M)
    edition = re.search(r'^edition\s*=\s*"([^"]+)"', block, re.M)
    lock = (root / "Cargo.lock").read_text()
    version = re.search(r'^name = "modorch"\nversion = "([^"]+)"', lock, re.M)
    if not revision or not rust_version or not edition or not version:
        raise ValueError("Cargo.toml/Cargo.lock lack the fixed framework identity")
    return {
        "version": version.group(1),
        "revision": revision.group(1),
        "rust_version": rust_version.group(1),
        "edition": edition.group(1),
    }


def contract_counts(root):
    manifest = read(local(root, MANIFEST))
    result = {}
    for spec in manifest["specifications"]:
        protocol = spec["protocol"]
        supplement = read(local(root, spec["supplement_source"]))
        result[protocol] = {
            "review_inputs": len(spec["rows"]),
            "table_rows": sum(len(table["rows"]) for table in supplement["tables"]),
        }
    return result


def require(condition, message):
    if not condition:
        raise ValueError(message)


def validate(root, data):
    root = Path(root).resolve()
    counts = contract_counts(root)
    actual_host = host_identity(root)
    cargo_features = features(root)

    require(data.get("version") == 1, "acceptance profile version must be 1")
    require(data.get("status") == "fixed-implementer", "acceptance profile status must be fixed-implementer")
    require(data.get("overall_status") == "Partial", "acceptance profile cannot claim complete conformance")
    require(data.get("independent_approvals") == 0, "independent approval must remain 0")
    host = data.get("host", {})
    require(host.get("framework") == "modorch", "host framework must be modorch")
    require(host.get("version") == actual_host["version"], "host framework version drifted from Cargo.lock")
    require(host.get("revision") == actual_host["revision"], "host framework revision drifted from Cargo.toml")
    require(host.get("rust_version") == actual_host["rust_version"], "host Rust version drifted from Cargo.toml")
    require(host.get("edition") == actual_host["edition"], "host edition drifted from Cargo.toml")

    profiles = data.get("profiles")
    require(isinstance(profiles, list) and len(profiles) == 2, "exactly two protocol profiles are required")
    require([p.get("protocol") for p in profiles] == ["mqtt311", "mqtt5"], "profiles must be ordered mqtt311,mqtt5")
    ids = [p.get("id") for p in profiles]
    require(all(isinstance(i, str) and i for i in ids) and len(set(ids)) == len(ids), "profile ids must be unique and non-empty")

    for profile in profiles:
        protocol = profile["protocol"]
        label = profile["id"]
        require(profile.get("role") == "server", f"{label}: role must be server")
        transports = profile.get("transports")
        require(isinstance(transports, list) and transports and set(transports) <= TRANSPORTS, f"{label}: invalid transport")
        storage = profile.get("storage")
        require(isinstance(storage, list) and storage and set(storage) <= STORAGE, f"{label}: invalid storage")
        require(profile.get("qos") == [0, 1, 2], f"{label}: QoS scope must be 0,1,2")
        selected = profile.get("features")
        require(isinstance(selected, list) and selected and len(set(selected)) == len(selected), f"{label}: features must be unique and non-empty")
        require(set(selected) <= cargo_features, f"{label}: unknown Cargo feature")
        clients = profile.get("clients")
        require(isinstance(clients, list) and clients and all(isinstance(c, str) and c for c in clients), f"{label}: clients must be non-empty")
        platforms = profile.get("platforms")
        require(isinstance(platforms, dict) and "linux-x86_64" in platforms, f"{label}: linux-x86_64 platform evidence is required")
        require(all(value in PLATFORM_EVIDENCE for value in platforms.values()), f"{label}: invalid platform evidence")
        expected = counts[protocol]
        require(profile.get("review_inputs") == expected["review_inputs"], f"{label}: review input count is stale")
        require(profile.get("table_rows") == expected["table_rows"], f"{label}: table row count is stale")
        require(isinstance(profile.get("out_of_scope"), list) and profile["out_of_scope"], f"{label}: out_of_scope must be explicit")
        require(isinstance(profile.get("conditional_unsupported"), list), f"{label}: conditional_unsupported must be a list")
        require(REQUIRED_NOT_CLAIMED <= set(profile.get("not_claimed", [])), f"{label}: required not_claimed boundaries are missing")
        require(profile.get("independent_review") == "NotRun", f"{label}: independent review cannot be self-approved")

    f6, f7 = profiles
    require(set(f6["transports"]) == {"tcp", "tls"}, "mqtt311 profile transports must remain tcp,tls")
    require({"mqtt5", "websocket", "bridge"} <= set(f6["out_of_scope"]), "mqtt311 profile must exclude mqtt5, websocket and bridge")
    require(not ({"mqtt5", "websocket", "bridge"} & set(f6["features"])), "mqtt311 profile cannot enable mqtt5, websocket or bridge")
    require("websocket" in f7["transports"], "mqtt5 profile must declare websocket transport")
    require({"mqtt5", "websocket"} <= set(f7["features"]), "mqtt5 profile must enable mqtt5 and websocket")
    require("bridge" in f7["out_of_scope"], "mqtt5 profile must exclude bridge")
    require("enhanced-auth" in f7["conditional_unsupported"], "mqtt5 profile must preserve enhanced-auth as unsupported")

    return {
        "validation_status": "Passed",
        "overall_status": "Partial",
        "claim": data["claim"],
        "status": data["status"],
        "independent_approvals": 0,
        "profiles": [{"id": p["id"], "protocol": p["protocol"], "review_inputs": p["review_inputs"],
                      "table_rows": p["table_rows"], "independent_review": p["independent_review"]} for p in profiles],
    }


def main():
    data = read(local(ROOT, PROFILE))
    result = validate(ROOT, data)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
