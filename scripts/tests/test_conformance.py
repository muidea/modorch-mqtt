"""The evidence gate must reject missing, ignored and contradictory test results."""
import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("check_conformance", Path(__file__).parents[1] / "check-conformance.py")
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.matrix = {
            "version": 1, "claim": "targeted only", "clause_inventory": ["A", "B"],
            "scenarios": [{"id": "s", "tests": ["tests::case"], "clauses": ["A"], "scope": "one trace"}],
            "unmapped_clauses": ["B"],
        }

    def test_passed_trace_never_upgrades_unmapped_clauses_or_overall_conformance(self):
        result = gate.evaluate(self.matrix, "test tests::case ... ok\n")
        self.assertEqual(result["validation_status"], "Passed")
        self.assertEqual(result["overall_status"], "Partial")
        self.assertEqual(result["clause_coverage"][1], {"id": "B", "coverage": "NotRun"})

    def test_empty_ignored_substring_and_failed_results_do_not_count_as_pass(self):
        for log in ["", "test tests::case ... ignored\n", "test tests::case_more ... ok\n",
                    "test tests::case ... FAILED\n", "test result: ok. 100 passed\n"]:
            with self.subTest(log=log):
                self.assertEqual(gate.evaluate(self.matrix, log)["validation_status"], "Failed")

    def test_later_failure_invalidates_earlier_passing_line(self):
        result = gate.evaluate(self.matrix, "test tests::case ... ok\ntest tests::case ... FAILED\n")
        self.assertEqual(result["validation_status"], "Failed")

    def test_unknown_clauses_and_empty_or_duplicate_mappings_are_rejected(self):
        cases = []
        for key, value in [("clauses", ["unknown"]), ("tests", []), ("scope", "")]:
            matrix = copy.deepcopy(self.matrix)
            matrix["scenarios"][0][key] = value
            cases.append(matrix)
        matrix = copy.deepcopy(self.matrix)
        matrix["scenarios"] *= 2
        cases.append(matrix)
        for matrix in cases:
            with self.assertRaises(ValueError):
                gate.evaluate(matrix, "test tests::case ... ok\n")

    def test_client_or_optional_role_does_not_turn_missing_evidence_into_pass(self):
        self.matrix["unmapped_applicability"] = {
            "B": {"category": "ClientOnly", "reason": "Client-only obligation"}
        }
        result = gate.evaluate(self.matrix, "test tests::case ... ok\n")
        self.assertEqual(result["overall_status"], "Partial")
        self.assertEqual(result["clause_coverage"][1]["coverage"], "NotRun")
        self.assertEqual(result["clause_coverage"][1]["applicability"]["category"], "ClientOnly")

    def test_incomplete_or_invented_applicability_is_rejected(self):
        for applicability in [{}, {"B": {"category": "AlreadyPassed", "reason": "assumed"}},
                              {"B": {"category": "ClientOnly", "reason": ""}},
                              {"A": {"category": "ClientOnly", "reason": "mapped elsewhere"}}]:
            with self.subTest(applicability=applicability):
                self.matrix["unmapped_applicability"] = applicability
                with self.assertRaises(ValueError):
                    gate.evaluate(self.matrix, "test tests::case ... ok\n")

    def test_release_mode_rejects_partial_and_failed_even_with_passing_scenarios(self):
        result = gate.evaluate(self.matrix, "test tests::case ... ok\n")
        self.assertEqual(gate.exit_code(result), 0)
        self.assertEqual(gate.exit_code(result, require_complete=True), 1)
        for require_complete in [False, True]:
            result = gate.evaluate(self.matrix, "test tests::case ... ignored\n")
            self.assertEqual(gate.exit_code(result, require_complete), 1)

    def test_duplicate_or_conflicting_ignored_evidence_is_rejected(self):
        for suffix in ["test tests::case ... ok\n", "test tests::case ... ignored\n"]:
            self.assertEqual(gate.evaluate(self.matrix, "test tests::case ... ok\n" + suffix)["validation_status"], "Failed")

    def test_audit_scope_does_not_upgrade_unexecuted_aggregate_and_missing_test_fails(self):
        audit = {"version": 1, "inventory_method": "test", "claim": "bounded",
                 "unnumbered": [{"id": "U1", "section": "3", "role": "Server", "coverage": "TargetedEvidence",
                                  "scope": "one trace", "source_text": "Server MUST reply", "tests": ["tests::audit"]}],
                 "structural": [{"id": "complete", "section": "7", "role": "Server", "coverage": "NotRun",
                                 "scope": "full FSM not audited", "tests": []}]}
        log = "test tests::case ... ok\n"
        self.assertEqual(gate.evaluate(self.matrix, log, audit)["validation_status"], "Failed")
        result = gate.evaluate(self.matrix, log + "test tests::audit ... ok\n", audit)
        self.assertEqual(result["validation_status"], "Passed")
        self.assertEqual(result["overall_status"], "Partial")
        self.assertEqual(result["audit_coverage"][1]["validation_status"], "NotRun")
        broken = copy.deepcopy(audit)
        broken["structural"][0]["id"] = "U1"
        with self.assertRaises(ValueError):
            gate.evaluate(self.matrix, log, broken)
        broken = copy.deepcopy(audit)
        broken["unnumbered"][0]["coverage"] = "Passed"
        with self.assertRaises(ValueError):
            gate.evaluate(self.matrix, log, broken)


if __name__ == "__main__":
    unittest.main()
