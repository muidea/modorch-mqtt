import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("feature_evidence", Path(__file__).resolve().parents[1] / "check-feature-evidence.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class FeatureEvidenceTests(unittest.TestCase):
    def test_exact_execution_is_required(self):
        catalog = {"version": 1, "claim": "bounded", "scenarios": [{"id": "a", "scope": "one", "tests": ["real::test"]}]}
        self.assertEqual(module.evaluate(catalog, "test real::test ... ok\n")["validation_status"], "Passed")
        for log in ["1 passed", "test real::test ... ignored\n", "test real::test ... ok\ntest result: FAILED", "test real::test ... ok\ntest real::test ... ok\n"]:
            self.assertEqual(module.evaluate(catalog, log)["validation_status"], "Failed")
        self.assertEqual(module.evaluate(catalog, "test real::test ... ok\n")["overall_status"], "Partial")

    def test_repeated_or_empty_scenarios_are_invalid(self):
        for items in [[], [{"id": "a", "scope": "one", "tests": []}], [{"id": "a", "scope": "one", "tests": ["x"]}] * 2]:
            with self.assertRaises(ValueError):
                module.evaluate({"version": 1, "claim": "bounded", "scenarios": items}, "")
