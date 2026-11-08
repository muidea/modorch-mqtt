"""The source audit must keep appendices and numbered statements out of inventory."""
import copy
import hashlib
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("audit_mqtt311", Path(__file__).parents[1] / "audit-mqtt311.py")
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


class AuditTests(unittest.TestCase):
    def setUp(self):
        self.raw = b'''<p class="MsoToc1">Appendix A.</p>
<p>The Server <b>MUST</b> close.</p>
<p>MUST reply [MQTT-1.0-1].</p>
<p class="AppendixHeading1">Appendix A.</p>
<p>The Server MUST close.</p>'''
        self.digest = hashlib.sha256(self.raw).hexdigest()
        self.manifest = {"version": 1, "inventory_method": audit.METHOD,
                         "source": {"sha256": self.digest},
                         "unnumbered": [{"source_text": "The Server MUST close."}]}

    def test_main_body_only_preserves_inline_text_and_excludes_numbered_and_appendix(self):
        self.assertEqual(audit.verify(self.manifest, self.raw), ["The Server MUST close."])

    def test_changed_html_inventory_and_missing_boundary_are_rejected(self):
        with self.assertRaises(ValueError):
            audit.verify(self.manifest, self.raw + b"changed")
        manifest = copy.deepcopy(self.manifest)
        manifest["unnumbered"] = []
        with self.assertRaises(ValueError):
            audit.verify(manifest, self.raw)
        raw = b"<p>Server MUST close.</p>"
        with self.assertRaises(ValueError):
            audit.extract(raw, hashlib.sha256(raw).hexdigest())

    def test_numbered_inventory_captures_list_items_and_pinned_typo_without_changing_text(self):
        raw = b'<p>Sender MUST retry [MQTT-3.3.1.-1].</p><ul><li>Server MUST close [MQTT-3.14.4-1].</li></ul><p class="AppendixHeading1">Appendix</p><p>Repeated [MQTT-9.9.9-1]</p>'
        matrix = {"source": {"sha256": hashlib.sha256(raw).hexdigest()},
                  "clause_inventory": ["MQTT-3.3.1-1", "MQTT-3.14.4-1"]}
        result = audit.numbered_inventory(raw, matrix)
        self.assertEqual(len(result["paragraphs"]), 2)
        self.assertIn("MQTT-3.3.1.-1", result["paragraphs"][0]["source_text"])
        self.assertEqual(result["paragraphs"][0]["clauses"], ["MQTT-3.3.1-1"])
        matrix["clause_inventory"].append("MQTT-1.0-1")
        with self.assertRaises(ValueError):
            audit.numbered_inventory(raw, matrix)


if __name__ == "__main__":
    unittest.main()
