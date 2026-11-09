"""Reject silent normative omissions, stale text and fabricated execution/approval."""
import copy
import hashlib
import importlib.util
from pathlib import Path
import unittest


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


ROOT = Path(__file__).resolve().parents[2]
audit = module('protocol_source', ROOT / 'scripts/audit-protocol.py')
review = module('protocol_review', ROOT / 'scripts/check-protocol-review.py')


class ProtocolReviewTests(unittest.TestCase):
    def test_nested_normative_lists_context_tables_and_appendix_are_bound(self):
        raw = b'''<h2>4.3 Sender</h2><ul><li><p>Sender MUST retain ownership.</p></li></ul>
<p>Server MUST allow ClientID characters</p><p>"abc" [MQTT-3.1.3-5].</p>
<table><tr><td><p>Code</p></td><td><p>Server only MUST close</p></td></tr></table>
<p class="AppendixHeading1">Appendix</p><p>SHALL be excluded</p>'''
        result = audit.inventory(raw, {'sha256': hashlib.sha256(raw).hexdigest()})
        self.assertEqual(len(result['unnumbered']), 2)
        self.assertEqual(result['unnumbered'][0]['source_text'], 'Sender MUST retain ownership.')
        self.assertIn('MQTT-3.1.3-5', result['unnumbered'][1]['following_context'][0])
        self.assertEqual(len(result['tables']), 1)
        self.assertIn('Server only MUST close', result['tables'][0]['source_text'])
        self.assertNotIn('excluded', str(result))
        self.assertIn('Sender MUST retain ownership.', str(result['sections']))
        prose = raw.replace(b'Sender MUST retain ownership.', b'An offline member behaves like an ordinary subscription.')
        self.assertIn('An offline member behaves like an ordinary subscription.', str(audit.inventory(prose, {'sha256': hashlib.sha256(prose).hexdigest()})['sections']))
        with self.assertRaises(ValueError):
            audit.inventory(raw+b'changed', result['source'])

    def test_current_inventory_is_structural_evidence_and_cannot_approve_itself(self):
        manifest = review.read(ROOT / 'contracts/protocol-review.json')
        result = review.evaluate(ROOT, manifest)
        self.assertEqual(result['validation_status'], 'Passed')
        self.assertEqual(result['overall_status'], 'Partial')
        self.assertTrue(all(s['independent_approved'] == 0 for s in result['profiles']))
        for mutate in ['omit', 'duplicate', 'source', 'approval', 'empty_tests']:
            changed = copy.deepcopy(manifest)
            rows = changed['specifications'][0]['rows']
            row = next(r for r in rows if r['coverage'] == 'TargetedEvidence')
            if mutate == 'omit': rows.pop()
            if mutate == 'duplicate': rows.append(copy.deepcopy(rows[0]))
            if mutate == 'source': row['source_digest'] = '0'*64
            if mutate == 'approval': row['independent_review'] = 'Passed'
            if mutate == 'empty_tests': row['tests'] = []
            with self.assertRaises(ValueError, msg=mutate):
                review.evaluate(ROOT, changed)

    def test_server_obligations_and_conditional_context_cannot_be_client_only(self):
        self.assertFalse(review.server_obligation_context(
            'The first Control Packet sent from the Client to the Server MUST be CONNECT.'))
        self.assertTrue(review.server_obligation_context(
            'If the Server receives an invalid packet, it closes the connection.'))
        manifest = review.read(ROOT / 'contracts/protocol-review.json')
        identifiers = ['MQTT-3.2.2-11', 'MQTT-3.2.2-14', 'MQTT-3.2.2-15',
                       'MQTT-3.2.2-19', 'MQTT-3.2.2-20']
        for identifier in identifiers:
            row = next(r for r in manifest['specifications'][1]['rows'] if r['id'] == identifier)
            self.assertIn(row['role'], {'Server', 'Both'}, identifier)
            self.assertNotEqual(row['coverage'], 'ClientOnly', identifier)
            for field, value in [('role', 'Client'), ('coverage', 'ClientOnly')]:
                changed = copy.deepcopy(manifest)
                target = next(r for r in changed['specifications'][1]['rows'] if r['id'] == identifier)
                target[field] = value
                with self.assertRaisesRegex(ValueError, 'Server.*role|Server.*ClientOnly'):
                    review.evaluate(ROOT, changed)

    def test_missing_failed_duplicate_or_ignored_execution_is_not_accepted(self):
        manifest = review.read(ROOT / 'contracts/protocol-review.json')
        names = sorted({t for s in manifest['specifications'] for r in s['rows'] for t in r['tests']})
        log = '\n'.join(f'test {name} ... ok' for name in names)
        self.assertEqual(review.evaluate(ROOT, manifest, log)['validation_status'], 'Passed')
        for changed in ['', log.replace(f'test {names[0]} ... ok', f'test {names[0]} ... FAILED'),
                        log+'\n'+f'test {names[0]} ... ok', log+'\n'+f'test {names[0]} ... ignored']:
            self.assertEqual(review.evaluate(ROOT, manifest, changed)['validation_status'], 'Failed')


class ProtocolBundleTests(unittest.TestCase):
    """Synthetic reviews exercise the gate, never certify this repository."""
    def setUp(self):
        import tempfile
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.manifest = review.read(ROOT / 'contracts/protocol-review.json')
        # Tiny explicit synthetic catalogs exercise the same structural parser;
        # the real full inventory is separately checked by ProtocolReviewTests.
        for protocol in self.manifest['specifications']:
            name = protocol['protocol']
            source = dict(sha256='0'*64, title='synthetic unit fixture')
            text = dict(source=source, paragraphs=[dict(clauses=['MQTT-1.1-1'], source_text='Fixture MUST retain ownership.')])
            section = dict(id='SECTION-001', section='fixture', paragraphs=['Behavior also exists without uppercase keywords.'])
            table = dict(id='TABLE-001', section='fixture', source_text='Code Meaning 0 Ownership',
                         rows=[[dict(text='Code Meaning', colspan='2', rowspan='1')],
                               [dict(text='0 Ownership', colspan='1', rowspan='1')]])
            extra = dict(source=source, unnumbered=[], tables=[table], sections=[section])
            self.put(protocol['numbered_source'], text)
            self.put(protocol['supplement_source'], extra)
            protocol['rows'] = [dict(id='MQTT-1.1-1', kind='Numbered', source_digest=review.digest(text['paragraphs']),
                                     role='Server', coverage='TargetedEvidence', scope='Synthetic ownership assertion',
                                     implementation=['crates/fixture.rs'], tests=['fixture::'+name], independent_review='NotRun'),
                                dict(id=section['id'], kind='Section', source_digest=review.digest(section), role='Context',
                                     coverage='Context', scope='Whole synthetic section', implementation=[], tests=[], independent_review='NotRun'),
                                dict(id=table['id'], kind='Table', source_digest=review.digest(table), role='Server',
                                     coverage='TargetedEvidence', scope='Whole synthetic table', implementation=['crates/fixture.rs'],
                                     tests=['fixture::'+name], independent_review='NotRun')]
        path = self.root / 'crates/fixture.rs'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('synthetic fixture source\n')
        self.manifest['state_fault_matrix'] = [dict(id='fixture-fsm', dimensions=dict(state=2), cases=2, tests=['fixture::mqtt311'])]
        self.put('contracts/protocol-review.json', self.manifest)
        for name in ['Cargo.toml', 'Cargo.lock', 'README.md']:
            self.put(name, 'synthetic unit fixture\n')
        self.snapshot = review.FILES.source_inputs(self.root)
        self.rows = review.obligations(self.manifest, self.root)
        names = sorted({t for row in self.rows.values() for t in row.get('tests', [])})
        self.log = '\n'.join(f'test {name} ... ok' for name in names) + '\n'
        self.put('cargo.log', self.log)
        self.bundle = review.prepare(self.root, self.manifest, 'cargo.log', 'fixture-producer')

    def put(self, name, value):
        import json
        raw = (value if isinstance(value, str) else json.dumps(value)).encode()
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        return dict(path=name, sha256=hashlib.sha256(raw).hexdigest())

    def approve(self):
        for index, (key, row) in enumerate(self.rows.items()):
            checks = [dict(id=check, status='Passed', rationale='Synthetic fixture only',
                           command='synthetic fixture replay', assertions='fixture full-text and state assertions',
                           evidence=[self.bundle['results']], tests=row.get('tests', [])) for check in row['checks']]
            report = dict(id=key, source_digest=self.bundle['source_digest'], obligation_sha256=review.digest(row),
                          results=self.bundle['results'], reviewer='fixture-reviewer', decision='Approved',
                          rationale='Synthetic fixture only; not project approval', checks=checks)
            self.bundle['reviews'][key] = self.put(f'review-{index}.json', report)

    def change(self, key, update):
        reference = self.bundle['reviews'][key]
        report = review.read(self.root / reference['path'])
        update(report)
        self.bundle['reviews'][key] = self.put(reference['path'], report)

    def test_complete_fixture_passes_but_no_real_approval_is_generated(self):
        self.assertEqual(review.evaluate_bundle(self.root, self.manifest, self.bundle)['overall_status'], 'Partial')
        self.approve()
        result = review.evaluate_bundle(self.root, self.manifest, self.bundle)
        self.assertEqual(result['overall_status'], 'Passed')
        self.assertEqual(sum(s['independent_approved'] for s in result['profiles']),
                         sum(len(s['rows']) for s in self.manifest['specifications']))

    def test_strict_cli_accepts_complete_fixture_and_blocks_unreviewed(self):
        import json
        import subprocess
        import sys
        path = self.root / 'bundle.json'
        path.write_text(json.dumps(self.bundle))
        command = [sys.executable, str(ROOT / 'scripts/check-protocol-review.py'), '--root', str(self.root),
                   '--bundle', str(path), '--require-complete']
        self.assertEqual(subprocess.run(command, capture_output=True).returncode, 1)
        self.approve()
        path.write_text(json.dumps(self.bundle))
        result = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['overall_status'], 'Passed')
        # Adding a tracked source invalidates the snapshot even when old reviews remain.
        self.put('crates/added.rs', 'new fixture source')
        self.assertEqual(subprocess.run(command, capture_output=True).returncode, 1)

    def test_missing_or_extra_obligation_and_changed_snapshot_are_rejected(self):
        for mutation in ['missing', 'extra', 'stale']:
            with self.subTest(mutation=mutation):
                bundle = copy.deepcopy(self.bundle)
                if mutation == 'missing': bundle['reviews'].pop(next(iter(bundle['reviews'])))
                if mutation == 'extra': bundle['reviews']['unknown'] = None
                if mutation == 'stale':
                    bundle['source_inputs']['added.rs'] = '1'*64
                    bundle['source_digest'] = review.digest(bundle['source_inputs'])
                with self.assertRaises(ValueError):
                    review.evaluate_bundle(self.root, self.manifest, bundle)

    def test_review_copies_cannot_be_execution_evidence_or_main_results(self):
        import shutil
        for location in ['check', 'results']:
            with self.subTest(location=location):
                self.approve()
                keys = list(self.rows)
                reference = dict(self.bundle['reviews'][keys[-1]])
                copied = self.root / f'copied-{location}.json'
                shutil.copyfile(self.root / reference['path'], copied)
                reference['path'] = copied.name
                bundle = copy.deepcopy(self.bundle)
                if location == 'results':
                    bundle['results'] = reference
                else:
                    self.change(keys[0], lambda r: r['checks'][0].update(evidence=[reference]))
                    bundle = self.bundle
                with self.assertRaisesRegex(ValueError, 'execution evidence'):
                    review.evaluate_bundle(self.root, self.manifest, bundle)

    def test_legitimate_copied_logs_still_pass(self):
        import shutil
        self.approve()
        shutil.copyfile(self.root / 'cargo.log', self.root / 'copied-cargo.log')
        reference = dict(self.bundle['results'], path='copied-cargo.log')
        self.change(next(iter(self.rows)), lambda r: r['checks'][0].update(evidence=[reference]))
        self.assertEqual(review.evaluate_bundle(self.root, self.manifest, self.bundle)['overall_status'], 'Passed')

    def test_review_file_aliases_cannot_be_execution_evidence_or_results(self):
        import os
        for alias in ['direct', 'dot', 'symlink', 'hardlink']:
            for location in ['check', 'results']:
                with self.subTest(alias=alias, location=location):
                    self.approve()
                    keys = list(self.rows)
                    reference = dict(self.bundle['reviews'][keys[-1]])
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
                    if location == 'results':
                        bundle = copy.deepcopy(self.bundle)
                        bundle['results'] = reference
                    else:
                        self.change(keys[0], lambda r: r['checks'][0].update(evidence=[reference]))
                        bundle = self.bundle
                    with self.assertRaisesRegex(ValueError, 'execution evidence'):
                        review.evaluate_bundle(self.root, self.manifest, bundle)

    def test_each_table_row_requires_its_own_bound_check_and_passing_assertions(self):
        key = 'mqtt5:TABLE-001'
        self.assertEqual(self.rows[key]['checks'][-2:], ['table-row:0001', 'table-row:0002'])
        self.assertEqual(self.rows[key]['table_rows'][0][0]['colspan'], '2')
        for mutation in ['omitted', 'duplicate', 'notrun', 'failed', 'no_tests', 'no_assertions']:
            with self.subTest(mutation=mutation):
                self.approve()
                def change(report):
                    checks = report['checks']
                    if mutation == 'omitted': checks.pop()
                    if mutation == 'duplicate': checks[-1] = copy.deepcopy(checks[-2])
                    if mutation == 'notrun': checks[-1]['status'] = 'NotRun'
                    if mutation == 'failed': checks[-1]['status'] = 'Failed'
                    if mutation == 'no_tests': checks[-1]['tests'] = []
                    if mutation == 'no_assertions': checks[-1]['assertions'] = ''
                self.change(key, change)
                if mutation in {'notrun', 'failed'}:
                    result = review.evaluate_bundle(self.root, self.manifest, self.bundle)
                    self.assertNotEqual(result['overall_status'], 'Passed')
                else:
                    with self.assertRaises(ValueError):
                        review.evaluate_bundle(self.root, self.manifest, self.bundle)
        self.approve()
        def header_context(report):
            report['checks'][-2].update(status='NotApplicable', tests=[],
                                        rationale='Synthetic spanning column heading; no wire behavior.')
        self.change(key, header_context)
        self.assertEqual(review.evaluate_bundle(self.root, self.manifest, self.bundle)['overall_status'], 'Passed')

    def test_current_tables_have_all_physical_rows_bound_to_review_checks(self):
        manifest = review.read(ROOT / 'contracts/protocol-review.json')
        rows = review.obligations(manifest)
        for spec, expected in zip(manifest['specifications'], [339, 505]):
            tables = [value for key, value in rows.items()
                      if key.startswith(spec['protocol']+':') and value.get('kind') == 'Table']
            self.assertEqual(sum(len(t['table_rows']) for t in tables), expected)
            for table in tables:
                self.assertEqual(len(table['checks']), len(review.SEMANTIC_CHECKS) + len(table['table_rows']))

    def test_legal_execution_log_aliases_still_pass(self):
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
                self.change(next(iter(self.rows)), lambda r: r['checks'][0].update(evidence=[reference]))
                self.assertEqual(review.evaluate_bundle(self.root, self.manifest, self.bundle)['overall_status'], 'Passed')

    def test_identity_semantics_matrix_log_and_rejection_cannot_be_bypassed(self):
        self.approve()
        key = next(iter(self.rows))
        for field, value in [('reviewer', ' FIXTURE-PRODUCER '), ('reviewer', ''),
                             ('source_digest', '0'*64), ('obligation_sha256', '1'*64),
                             ('results', dict(path='other.log', sha256='0'*64)), ('rationale', '')]:
            with self.subTest(field=field):
                self.approve()
                self.change(key, lambda r: r.update({field: value}))
                with self.assertRaises(ValueError):
                    review.evaluate_bundle(self.root, self.manifest, self.bundle)
        for mutation in ['omission', 'duplicate', 'no_assertions', 'no_evidence', 'missing_test', 'waiver', 'self_evidence']:
            with self.subTest(mutation=mutation):
                self.approve()
                matrix = next(k for k in self.rows if k.startswith('matrix:'))
                def change(report):
                    checks = report['checks']
                    if mutation == 'omission': checks.pop()
                    if mutation == 'duplicate': checks.append(copy.deepcopy(checks[0]))
                    if mutation == 'no_assertions': checks[0]['assertions'] = ''
                    if mutation == 'no_evidence': checks[0]['evidence'] = []
                    if mutation == 'missing_test': checks[0]['tests'] = ['not::executed']
                    if mutation == 'waiver': checks[0]['status'] = 'NotApplicable'
                    if mutation == 'self_evidence': checks[0]['evidence'] = [self.bundle['reviews'][matrix]]
                self.change(matrix, change)
                with self.assertRaises(ValueError):
                    review.evaluate_bundle(self.root, self.manifest, self.bundle)
        self.approve()
        self.change('mqtt5:acceptance', lambda r: r['checks'][0].update(status='NotApplicable'))
        with self.assertRaises(ValueError):
            review.evaluate_bundle(self.root, self.manifest, self.bundle)
        self.approve()
        self.change(key, lambda r: r.update(decision='Rejected'))
        self.assertEqual(review.evaluate_bundle(self.root, self.manifest, self.bundle)['overall_status'], 'Failed')
        self.approve()
        def contradictory(report):
            report['checks'][0]['status'] = 'Failed'
            report['checks'][1]['status'] = 'NotRun'
        self.change(key, contradictory)
        self.assertEqual(review.evaluate_bundle(self.root, self.manifest, self.bundle)['overall_status'], 'Failed')
        self.approve()
        (self.root / 'cargo.log').write_text('tampered')
        with self.assertRaises(ValueError):
            review.evaluate_bundle(self.root, self.manifest, self.bundle)
        self.bundle['results'] = self.put('cargo.log', self.log + 'test duplicate ... ignored\n')
        self.approve()
        self.assertEqual(review.evaluate_bundle(self.root, self.manifest, self.bundle)['overall_status'], 'Failed')


if __name__ == '__main__':
    unittest.main()
