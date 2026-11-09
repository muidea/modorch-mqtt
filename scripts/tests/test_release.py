"""Synthetic fixtures test approval logic; they are never project certification."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("release", Path(__file__).parents[1] / "check-release.py")
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)
REPO = Path(__file__).resolve().parents[2]


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "contracts").mkdir()
        for name in ("mqtt311.json", "mqtt311-audit.json"):
            (self.root / "contracts" / name).write_bytes((REPO / "contracts" / name).read_bytes())
        matrix = gate.read_json(self.root / "contracts/mqtt311.json")
        audit = gate.read_json(self.root / "contracts/mqtt311-audit.json")
        names = {t for s in matrix["scenarios"] for t in s["tests"]}
        names.update(t for group in ("unnumbered", "structural") for s in audit[group] for t in s["tests"])
        self.log = "".join(f"test {name} ... ok\n" for name in sorted(names))
        self.rows = {
            "MQTT-1.5.3-1": {"id": "MQTT-1.5.3-1", "scope": "fixture", "tests": [sorted(names)[0]]},
            "MQTT-ARCH-001": {"id": "MQTT-ARCH-001", "scope": "fixture", "tests": []},
            "F8-REMOTE-CI": {"id": "F8-REMOTE-CI", "scope": "fixture", "checks": ["immutable-run"]},
        }
        self.snapshot = {"fixture.rs": "0" * 64}
        self.bundle = {
            "version": 1, "profile": gate.PROFILE, "producer": "fixture-author",
            "source_inputs": self.snapshot, "source_digest": gate.object_digest(self.snapshot),
            "results": self.put("cargo.log", self.log), "reviews": {k: None for k in self.rows},
        }
        self.addCleanup(patch.stopall)
        patch.object(gate, "catalog", return_value=(self.rows, {})).start()
        patch.object(gate, "source_inputs", return_value=self.snapshot).start()

    def put(self, name, value):
        raw = (json.dumps(value) if not isinstance(value, str) else value).encode()
        (self.root / name).write_bytes(raw)
        return {"path": name, "sha256": gate.digest(raw)}

    def approve(self):
        for key, row in self.rows.items():
            review = {
                "id": key, "source_digest": self.bundle["source_digest"],
                "obligation_sha256": gate.object_digest(row), "reviewer": "fixture-reviewer",
                "decision": "Approved", "rationale": "SYNTHETIC unit fixture only",
                "evidence": [self.bundle["results"]],
            }
            if key in gate.GATES:
                report = {"gate_id": key, "source_digest": self.bundle["source_digest"],
                          "checks": [{"id": check, "status": "Passed", "command": "synthetic fixture replay",
                                      "evidence": [self.bundle["results"]]} for check in gate.GATES[key]]}
                review["report"] = self.put(key + "-report.json", report)
                review["evidence"].append(review["report"])
            self.bundle["reviews"][key] = self.put(key + "-review.json", review)

    def update_review(self, key, change):
        reference = self.bundle["reviews"][key]
        value = gate.read_json(self.root / reference["path"])
        change(value)
        self.bundle["reviews"][key] = self.put(reference["path"], value)

    def test_complete_synthetic_evidence_has_a_real_pass_path(self):
        self.approve()
        result = gate.evaluate(self.root, self.bundle)
        self.assertEqual(result["overall_status"], "Passed")
        self.assertEqual(result["normative_status"], "Passed")
        self.assertEqual(result["derived_aggregates"]["UN-008"], "Passed")

    def test_mapped_tests_without_reviews_remain_partial(self):
        result = gate.evaluate(self.root, self.bundle)
        self.assertEqual(result["overall_status"], "Partial")
        self.assertTrue(all(row["status"] == "NotRun" for row in result["obligations"]))

    def test_deleted_or_unknown_obligation_is_not_waived(self):
        for key in ["MQTT-ARCH-001", "unknown"]:
            with self.subTest(key=key):
                bundle = copy.deepcopy(self.bundle)
                if key in bundle["reviews"]:
                    del bundle["reviews"][key]
                else:
                    bundle["reviews"][key] = None
                with self.assertRaises(ValueError):
                    gate.evaluate(self.root, bundle)

    def test_stale_snapshot_even_with_matching_self_declared_digest_is_rejected(self):
        bundle = copy.deepcopy(self.bundle)
        bundle["source_inputs"]["new.rs"] = "1" * 64
        bundle["source_digest"] = gate.object_digest(bundle["source_inputs"])
        with self.assertRaises(ValueError):
            gate.evaluate(self.root, bundle)

    def test_log_tampering_missing_and_ignored_or_failed_evidence_are_rejected(self):
        self.approve()
        (self.root / "cargo.log").write_text("tampered")
        with self.assertRaises(ValueError):
            gate.evaluate(self.root, self.bundle)
        name = self.rows["MQTT-1.5.3-1"]["tests"][0]
        for suffix in [f"test {name} ... FAILED\n", f"test {name} ... ignored\n", f"test {name} ... ok\n"]:
            self.bundle["results"] = self.put("cargo.log", self.log + suffix)
            self.approve()
            # No review can override contradictory raw execution evidence.
            self.assertEqual(gate.evaluate(self.root, self.bundle)["overall_status"], "Failed")
        self.bundle["results"] = self.put("cargo.log", "")
        self.approve()
        self.assertEqual(gate.evaluate(self.root, self.bundle)["overall_status"], "Failed")

    def test_changed_scope_source_identity_self_review_and_rejection(self):
        for field, value in [("obligation_sha256", "0" * 64), ("source_digest", "1" * 64),
                             ("reviewer", " Fixture-Author "), ("reviewer", ""), ("id", "other"),
                             ("evidence", []), ("rationale", "")]:
            with self.subTest(field=field):
                self.approve()
                self.update_review("MQTT-ARCH-001", lambda r: r.update({field: value}))
                with self.assertRaises(ValueError):
                    gate.evaluate(self.root, self.bundle)
        self.approve()
        self.update_review("MQTT-ARCH-001", lambda r: r.update(decision="Rejected"))
        self.assertEqual(gate.evaluate(self.root, self.bundle)["overall_status"], "Failed")

    def test_incomplete_gate_report_or_notrun_check_blocks_release(self):
        self.approve()
        reference = self.bundle["reviews"]["F8-REMOTE-CI"]
        review = gate.read_json(self.root / reference["path"])
        report = gate.read_json(self.root / review["report"]["path"])
        report["checks"] = []
        new_ref = self.put("F8-REMOTE-CI-report.json", report)
        self.update_review("F8-REMOTE-CI", lambda r: r.update(report=new_ref, evidence=[new_ref]))
        with self.assertRaises(ValueError):
            gate.evaluate(self.root, self.bundle)
        self.approve()
        review = gate.read_json(self.root / self.bundle["reviews"]["F8-REMOTE-CI"]["path"])
        report = gate.read_json(self.root / review["report"]["path"])
        report["checks"][0]["status"] = "NotRun"
        new_ref = self.put("F8-REMOTE-CI-report.json", report)
        self.update_review("F8-REMOTE-CI", lambda r: r.update(report=new_ref, evidence=[new_ref]))
        result = gate.evaluate(self.root, self.bundle)
        self.assertEqual(result["overall_status"], "Failed")
        self.assertEqual(result["normative_status"], "Passed")  # External CI failure is scoped separately.

    def test_review_and_report_cannot_approve_themselves(self):
        self.approve()
        self.update_review("MQTT-ARCH-001", lambda r: r.update(evidence=[self.bundle["reviews"]["MQTT-1.5.3-1"]]))
        with self.assertRaises(ValueError):
            gate.evaluate(self.root, self.bundle)

    def test_traversal_external_symlink_missing_and_duplicate_json_keys(self):
        for name in ["../outside", "/etc/passwd", "absent.log"]:
            with self.assertRaises(ValueError):
                gate.checked_file(self.root, {"path": name, "sha256": "0" * 64})
        (self.root / "external").symlink_to("/etc/passwd")
        with self.assertRaises(ValueError):
            gate.checked_file(self.root, {"path": "external", "sha256": "0" * 64})
        path = self.root / "duplicate.json"
        path.write_text('{"reviews": {}, "reviews": {}}')
        with self.assertRaises(ValueError):
            gate.read_json(path)

    def test_review_aliases_are_rejected_at_every_execution_boundary(self):
        import os
        for alias in ['direct', 'dot', 'symlink', 'hardlink']:
            for location in ['review', 'nested', 'results']:
                with self.subTest(alias=alias, location=location):
                    self.approve()
                    reference = dict(self.bundle['reviews']['MQTT-1.5.3-1'])
                    target = self.root / reference['path']
                    if alias == 'dot':
                        reference['path'] = './' + reference['path']
                    elif alias in {'symlink', 'hardlink'}:
                        link = self.root / f'{alias}-{location}.json'
                        if alias == 'symlink':
                            link.symlink_to(target)
                        else:
                            os.link(target, link)
                        reference['path'] = link.name
                    bundle = copy.deepcopy(self.bundle)
                    if location == 'results':
                        bundle['results'] = reference
                    elif location == 'review':
                        self.update_review('MQTT-ARCH-001', lambda r: r.update(evidence=[reference]))
                        bundle = self.bundle
                    else:
                        self.replace_gate_execution(reference)
                        bundle = self.bundle
                    with self.assertRaisesRegex(ValueError, 'execution evidence'):
                        gate.evaluate(self.root, bundle)

    def replace_gate_execution(self, reference):
        review = gate.read_json(self.root / self.bundle['reviews']['F8-REMOTE-CI']['path'])
        report = gate.read_json(self.root / review['report']['path'])
        report['checks'][0]['evidence'] = [reference]
        new_ref = self.put('F8-REMOTE-CI-report.json', report)
        self.update_review('F8-REMOTE-CI', lambda r: r.update(report=new_ref, evidence=[new_ref]))

    def test_nested_report_cannot_use_its_own_file_alias_as_execution(self):
        import os
        for alias in ['dot', 'symlink', 'hardlink']:
            with self.subTest(alias=alias):
                self.approve()
                target = self.root / 'F8-REMOTE-CI-report.json'
                reference = dict(path='./'+target.name, sha256='0'*64)
                if alias != 'dot':
                    link = self.root / f'report-{alias}.json'
                    if alias == 'symlink':
                        link.symlink_to(target)
                    else:
                        os.link(target, link)
                    reference['path'] = link.name
                # A self hash cannot be constructed; identity must be rejected
                # before attempting to validate that impossible recursive hash.
                self.replace_gate_execution(reference)
                with self.assertRaisesRegex(ValueError, 'execution evidence'):
                    gate.evaluate(self.root, self.bundle)

    def test_legal_log_aliases_keep_the_complete_pass_path(self):
        import os
        for alias in ['dot', 'symlink', 'hardlink']:
            with self.subTest(alias=alias):
                self.approve()
                reference = dict(self.bundle['results'], path='./cargo.log')
                if alias != 'dot':
                    link = self.root / f'log-{alias}'
                    if alias == 'symlink':
                        link.symlink_to(self.root / 'cargo.log')
                    else:
                        os.link(self.root / 'cargo.log', link)
                    reference['path'] = link.name
                self.replace_gate_execution(reference)
                self.assertEqual(gate.evaluate(self.root, self.bundle)['overall_status'], 'Passed')

    def test_review_copies_cannot_be_execution_evidence_at_any_boundary(self):
        import shutil
        for location in ['review', 'nested', 'results']:
            with self.subTest(location=location):
                self.approve()
                reference = dict(self.bundle['reviews']['MQTT-1.5.3-1'])
                copied = self.root / f'copied-{location}.json'
                shutil.copyfile(self.root / reference['path'], copied)
                reference['path'] = copied.name
                bundle = copy.deepcopy(self.bundle)
                if location == 'review':
                    self.update_review('MQTT-ARCH-001', lambda r: r.update(evidence=[reference]))
                    bundle = self.bundle
                elif location == 'nested':
                    self.replace_gate_execution(reference)
                    bundle = self.bundle
                else:
                    bundle['results'] = reference
                with self.assertRaisesRegex(ValueError, 'execution evidence'):
                    gate.evaluate(self.root, bundle)

    def test_legitimate_copied_logs_still_pass_nested_checks(self):
        import shutil
        self.approve()
        copied = self.root / 'copied-cargo.log'
        shutil.copyfile(self.root / 'cargo.log', copied)
        reference = dict(self.bundle['results'], path=copied.name)
        self.replace_gate_execution(reference)
        self.assertEqual(gate.evaluate(self.root, self.bundle)['overall_status'], 'Passed')


class CatalogTests(unittest.TestCase):
    def test_standalone_qos_markers_cannot_hide_missing_lists_or_wrong_role(self):
        read = gate.read_json
        for change in ["missing", "truncated", "wrong-role"]:
            with self.subTest(change=change):
                text = read(REPO / "contracts/mqtt311-text.json")
                row = next(p for p in text["paragraphs"] if "MQTT-4.3.3-2" in p["clauses"])
                if change == "missing":
                    row.pop("preceding_normative")
                elif change == "truncated":
                    row["preceding_normative"].pop()
                else:
                    row["preceding_context"] = "In the QoS 2 delivery protocol, the Sender"
                with patch.object(gate, "read_json", side_effect=lambda p: text if p.name == "mqtt311-text.json" else read(p)):
                    with self.assertRaisesRegex(ValueError, "QoS obligation text"):
                        gate.catalog(REPO)

    def test_real_catalog_preserves_all_design_must_and_all_normative_obligations(self):
        rows, excluded = gate.catalog(REPO)
        self.assertEqual(len(rows), 178)
        self.assertEqual(len(excluded), 12)
        self.assertIn("FSM-COMPLETE", rows)
        self.assertNotIn("UN-008", rows)  # Derived, never circular approval.
        self.assertEqual({k for k in rows if k.startswith("F8-")}, set(gate.GATES))

    def test_source_discovery_detects_addition_deletion_and_excludes_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("Cargo.toml", "Cargo.lock", "README.md"):
                (root / name).write_text("fixture")
            (root / "crates").mkdir()
            before = gate.source_inputs(root)
            new = root / "crates/new.rs"
            new.write_text("// fixture")
            self.assertNotEqual(before, gate.source_inputs(root))
            new.unlink()
            self.assertEqual(before, gate.source_inputs(root))
            (root / "docs/validation").mkdir(parents=True)
            (root / "docs/validation/report.json").write_text("{}")
            self.assertEqual(before, gate.source_inputs(root))
