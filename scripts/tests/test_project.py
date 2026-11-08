"""Protect required deliverables and local links from silent removal."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("check_project", Path(__file__).parents[1] / "check-project.py")
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


class ProjectTests(unittest.TestCase):
    def test_missing_deliverables_and_broken_local_links_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.assertTrue(gate.validate(root))
            for name in gate.REQUIRED:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("")
            self.assertEqual(gate.validate(root), [])
            (root / "docs/api.md").write_text("[missing](absent.md)\n[remote](https://example.com)\n")
            errors = gate.validate(root)
            self.assertEqual(len(errors), 1)
            self.assertIn("broken local link", errors[0])
            (root / "docs/absent.md").write_text("[API](api.md#details)\n")
            self.assertEqual(gate.validate(root), [])
            (root / ".agents/skills/modorch-mqtt-development/SKILL.md").unlink()
            self.assertIn("missing required deliverable", gate.validate(root)[0])


if __name__ == "__main__":
    unittest.main()
