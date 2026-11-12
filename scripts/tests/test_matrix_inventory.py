import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    "matrix_inventory", Path(__file__).resolve().parents[1] / "check-matrix-inventory.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def fixtures():
    manifest = module.read(module.ROOT / module.MANIFEST)
    text = (module.ROOT / module.PLAN).read_text()
    return manifest, text


class MatrixInventoryTests(unittest.TestCase):
    def test_current_inventory_matches_protocol_review(self):
        manifest, text = fixtures()
        result = module.evaluate(manifest, text)
        self.assertEqual(result["validation_status"], "Passed")
        self.assertEqual(result["matrices"], 18)
        self.assertEqual(result["cases"], 222)
        self.assertEqual(result["decision_status"], "pending-semantic-review")

    def test_missing_duplicate_or_reordered_rows_are_rejected(self):
        manifest, text = fixtures()
        rows = [line for line in text.splitlines() if line.startswith("| 1 |") or line.startswith("| 2 |")]
        self.assertEqual(len(rows), 2)
        with self.assertRaises(ValueError):
            module.evaluate(manifest, text.replace(rows[0] + "\n", "", 1))
        with self.assertRaises(ValueError):
            module.evaluate(manifest, text.replace(rows[0], rows[0] + "\n" + rows[0], 1))
        with self.assertRaises(ValueError):
            module.evaluate(manifest, text.replace(rows[0], rows[1], 1))

    def test_stale_dimensions_cases_and_test_evidence_are_rejected(self):
        manifest, text = fixtures()
        with self.assertRaises(ValueError):
            module.evaluate(manifest, text.replace("states=4 / acks=3", "states=5 / acks=3", 1))
        with self.assertRaises(ValueError):
            module.evaluate(manifest, text.replace(
                "| 1 | `SENDER-ACK` | states=4 / acks=3 | 12 |",
                "| 1 | `SENDER-ACK` | states=4 / acks=3 | 13 |", 1))
        with self.assertRaises(ValueError):
            module.evaluate(manifest, text.replace(
                "state::qos2::tests::sender_ack_transition_matrix_preserves_phase_and_identifier_ownership",
                "state::qos2::tests::missing", 1))

    def test_missing_heading_or_total_statement_is_rejected(self):
        manifest, text = fixtures()
        with self.assertRaises(ValueError):
            module.evaluate(manifest, text.replace(module.HEADING, "### missing", 1))
        with self.assertRaises(ValueError):
            module.evaluate(manifest, text.replace("**18 个矩阵 / 222 个组合**", "**18 个矩阵**", 1))


if __name__ == "__main__":
    unittest.main()
