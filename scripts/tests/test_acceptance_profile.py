import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    "acceptance_profile", Path(__file__).resolve().parents[1] / "check-acceptance-profile.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def current():
    return module.read(module.ROOT / module.PROFILE)


class AcceptanceProfileTests(unittest.TestCase):
    def test_current_scope_matches_contracts_and_cargo(self):
        result = module.validate(module.ROOT, current())
        self.assertEqual(result["validation_status"], "Passed")
        self.assertEqual(result["overall_status"], "Partial")
        self.assertEqual(result["independent_approvals"], 0)
        self.assertEqual([p["review_inputs"] for p in result["profiles"]], [437, 607])
        self.assertEqual([p["table_rows"] for p in result["profiles"]], [339, 505])

    def test_self_approval_and_stale_counts_are_rejected(self):
        data = current()
        data["independent_approvals"] = 1
        with self.assertRaises(ValueError):
            module.validate(module.ROOT, data)

        data = current()
        data["profiles"][0]["independent_review"] = "Approved"
        with self.assertRaises(ValueError):
            module.validate(module.ROOT, data)

        data = current()
        data["profiles"][0]["review_inputs"] += 1
        with self.assertRaises(ValueError):
            module.validate(module.ROOT, data)

    def test_feature_scope_and_host_drift_are_rejected(self):
        data = current()
        data["profiles"][0]["features"].append("not-a-feature")
        with self.assertRaises(ValueError):
            module.validate(module.ROOT, data)

        data = current()
        data["profiles"][0]["out_of_scope"].remove("mqtt5")
        with self.assertRaises(ValueError):
            module.validate(module.ROOT, data)

        data = current()
        data["host"]["revision"] = "0" * 40
        with self.assertRaises(ValueError):
            module.validate(module.ROOT, data)

    def test_mqtt5_conditional_boundaries_are_preserved(self):
        data = current()
        data["profiles"][1]["conditional_unsupported"].remove("enhanced-auth")
        with self.assertRaises(ValueError):
            module.validate(module.ROOT, data)

        data = current()
        data["profiles"][1]["features"].remove("websocket")
        with self.assertRaises(ValueError):
            module.validate(module.ROOT, data)

        data = copy.deepcopy(current())
        data["profiles"][1]["out_of_scope"].remove("bridge")
        with self.assertRaises(ValueError):
            module.validate(module.ROOT, data)


if __name__ == "__main__":
    unittest.main()
