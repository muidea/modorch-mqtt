#!/usr/bin/env python3
"""Check every protocol review input and bounded test mapping without self-approval."""
import argparse
import hashlib
import importlib.util
import itertools
import json
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
COVERAGE = {'TargetedEvidence', 'NotRun', 'ClientOnly', 'OptionalWebSocket',
            'ConditionalBridge', 'UnsupportedConditional', 'Definition', 'Context', 'Informative', 'Aggregate'}


def server_obligation_context(prose):
    """Conservative regression guard for explicit subjects/conditions, not a semantic parser."""
    return bool(re.search(
        r'(?<!to )\b(?:The|the|A|a) Server\s+(?:MUST|SHALL|SHOULD)\b|'
        r'\bClient or Server\s+(?:MUST|SHALL|SHOULD)\b|'
        r'\b(?:If|if) (?:a|the) Server receives\b|'
        r'\bServer uses (?:a )?DISCONNECT\b', prose))


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(',', ':')).encode()).hexdigest()


def read(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f'duplicate JSON key: {key}')
            result[key] = value
        return result
    return json.loads(path.read_text(), object_pairs_hook=unique)


def local(root, name):
    path = (root / name).resolve()
    if not name or Path(name).is_absolute() or '..' in Path(name).parts or not path.is_relative_to(root) or not path.is_file():
        raise ValueError(f'missing or unsafe review input: {name}')
    return path


def evaluate(root, manifest, log=None):
    if manifest['version'] != 1 or not manifest['claim'] or not manifest['profile']:
        raise ValueError('invalid review manifest')
    if [s['protocol'] for s in manifest['specifications']] != ['mqtt311', 'mqtt5']:
        raise ValueError('both controlled protocol profiles must be present exactly once')
    passed_names = re.findall(r'^test ([\w:]+) \.\.\. ok$', log or '', re.M)
    passed = set(passed_names)
    bad = log is not None and (
        len(passed_names) != len(passed) or 'test result: FAILED' in log
        or bool(re.search(r'^test .* \.\.\. (?:FAILED|ignored)', log, re.M))
        or 'error: could not compile' in log
    )
    result = []
    missing = set()
    for spec in manifest['specifications']:
        text = read(local(root, spec['numbered_source']))
        extra = read(local(root, spec['supplement_source']))
        if text['source'] != extra['source']:
            raise ValueError('numbered and supplemental source identities differ')
        expected = {c: [p for p in text['paragraphs'] if c in p['clauses']]
                    for c in {c for p in text['paragraphs'] for c in p['clauses']}}
        expected.update({r['id']: r for r in extra['unnumbered'] + extra['tables'] + extra['sections']})
        rows = spec['rows']
        if len(rows) != len(expected) or {r['id'] for r in rows} != set(expected):
            raise ValueError('review inventory has missing, extra or duplicate rows')
        summary = {}
        for row in rows:
            if row['source_digest'] != digest(expected[row['id']]):
                raise ValueError(f'stale normative text/context: {row["id"]}')
            kind = 'Numbered' if row['id'].startswith('MQTT') else ('Table' if row['id'].startswith('TABLE') else ('Section' if row['id'].startswith('SECTION') else 'Unnumbered'))
            if (row['kind'] != kind or row['role'] not in {'Client', 'Server', 'Both', 'Context'}
                    or row['coverage'] not in COVERAGE or not row['scope']
                    or row['independent_review'] != 'NotRun'):
                raise ValueError('invalid role, scope, coverage or independent review claim')
            if not set(row.get('related_inputs', [])) <= set(expected):
                raise ValueError('related normative context is missing')
            if kind == 'Numbered':
                prose = ' '.join(p['source_text'] for p in expected[row['id']])
                server_context = server_obligation_context(prose)
                if server_context and row['role'] not in {'Server', 'Both'}:
                    raise ValueError(f"Server context has an excluded role: {row['id']}")
                if server_context and row['coverage'] == 'ClientOnly':
                    raise ValueError(f"Server context cannot be ClientOnly: {row['id']}")
            targeted = row['coverage'] == 'TargetedEvidence'
            if targeted != bool(row['tests']) or len(set(row['tests'])) != len(row['tests']):
                raise ValueError('only bounded targeted rows have unique executable tests')
            if targeted and not row['implementation']:
                raise ValueError('targeted row must identify implementation paths')
            for name in row['implementation']:
                local(root, name)
            if log is not None:
                missing.update(set(row['tests']) - passed)
            summary[row['coverage']] = summary.get(row['coverage'], 0) + 1
        result.append({'protocol': spec['protocol'], 'review_inputs': len(rows), 'coverage': summary,
                       'table_row_checks': sum(len(table['rows']) for table in extra['tables']),
                       'independent_approved': 0})
    import math
    model = manifest['state_fault_matrix']
    if len({r['id'] for r in model}) != len(model) or not model:
        raise ValueError('missing or duplicate finite state/fault matrix')
    for row in model:
        if (not row['dimensions'] or any(type(n) is not int or n < 1 for n in row['dimensions'].values())
                or row['cases'] != math.prod(row['dimensions'].values()) or not row['tests']):
            raise ValueError('invalid state/fault dimensions or case total')
        if log is not None:
            missing.update(set(row['tests']) - passed)
    valid = not bad and not missing
    return {'validation_status': 'Passed' if valid else 'Failed', 'overall_status': 'Partial' if valid else 'Failed',
            'claim': manifest['claim'], 'profiles': result, 'missing_pass_evidence': sorted(missing),
            'evidence_log_checked': log is not None}



def release_helpers():
    spec = importlib.util.spec_from_file_location('protocol_release_helpers', Path(__file__).with_name('check-release.py'))
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


FILES = release_helpers()
SEMANTIC_CHECKS = ['whole-text', 'related-context', 'applicability', 'assertion-sufficiency']
PROFILE_CHECKS = ['state-and-fault-coverage', 'independent-client-interoperability',
                  'persistence-and-recovery', 'remaining-conditional-features']


def obligations(manifest, root=ROOT):
    rows = {}
    for spec in manifest['specifications']:
        tables = {table['id']: table for table in read(local(root, spec['supplement_source']))['tables']}
        for row in spec['rows']:
            obligation = dict(row, checks=list(SEMANTIC_CHECKS))
            if row['kind'] == 'Table':
                table = tables[row['id']]
                # Bind cell text AND rowspan/colspan with the surrounding table;
                # an approval of a whole table must explicitly account for each
                # physical source row, including headers and reserved values.
                obligation['table_rows'] = table['rows']
                obligation['checks'].extend(f'table-row:{index:04}'
                                            for index in range(1, len(table['rows'])+1))
            rows[f"{spec['protocol']}:{row['id']}"] = obligation
        rows[f"{spec['protocol']}:acceptance"] = dict(protocol=spec['protocol'], checks=PROFILE_CHECKS)
    for row in manifest['state_fault_matrix']:
        dimensions = sorted(row['dimensions'])
        checks = ['case:' + ','.join(f'{key}={value}' for key, value in zip(dimensions, values))
                  for values in itertools.product(*(range(row['dimensions'][key]) for key in dimensions))]
        rows['matrix:' + row['id']] = dict(row, checks=checks)
    return rows


def prepare(root, manifest, results, producer):
    if not producer.strip():
        raise ValueError('evidence producer identity required')
    snapshot = FILES.source_inputs(root)
    return dict(version=1, profile=manifest['profile'], producer=producer,
                source_inputs=snapshot, source_digest=digest(snapshot),
                results=dict(path=results, sha256=hashlib.sha256(local(root, results).read_bytes()).hexdigest()),
                reviews={key: None for key in obligations(manifest, root)})


def evaluate_bundle(root, manifest, bundle):
    """Integrity and completeness check; identity authenticity requires external review."""
    current = FILES.source_inputs(root)
    if (bundle['version'] != 1 or bundle['profile'] != manifest['profile'] or not bundle['producer'].strip()
            or bundle['source_inputs'] != current or bundle['source_digest'] != digest(current)):
        raise ValueError('invalid profile/producer or stale source snapshot (including additions/deletions)')
    review_files = FILES.excluded_evidence(root, [ref for ref in bundle['reviews'].values() if ref is not None])
    log_path = FILES.execution_file(root, bundle['results'], review_files)
    log = log_path.read_text()
    result = evaluate(root, manifest, log)
    rows = obligations(manifest, root)
    if set(bundle['reviews']) != set(rows):
        raise ValueError('review inventory differs from required semantic/matrix/profile obligations')
    passed = set(re.findall(r'^test ([\w:]+) \.\.\. ok$', log, re.M))
    report_paths = set()
    states = {}
    for key, row in rows.items():
        reference = bundle['reviews'][key]
        if reference is None:
            states[key] = 'NotRun'
            continue
        report = read(FILES.checked_file(root, reference))
        if (report['id'] != key or report['source_digest'] != bundle['source_digest']
                or report['obligation_sha256'] != digest(row) or report['results'] != bundle['results']):
            raise ValueError(f'stale obligation/source/log review: {key}')
        reviewer = report['reviewer'].strip()
        if not reviewer or reviewer.casefold() == bundle['producer'].strip().casefold():
            raise ValueError(f'independent reviewer required: {key}')
        if report['decision'] not in {'Approved', 'Rejected'} or not report['rationale'].strip():
            raise ValueError(f'invalid decision or rationale: {key}')
        checks = report['checks']
        if len(checks) != len(row['checks']) or {c['id'] for c in checks} != set(row['checks']):
            raise ValueError(f'incomplete or duplicate semantic/state/profile checks: {key}')
        states[key] = 'Passed' if report['decision'] == 'Approved' else 'Failed'
        check_states = {states[key]}
        covered_tests = set()
        for check in checks:
            if (check['status'] not in {'Passed', 'NotApplicable', 'Failed', 'NotRun'}
                    or not check['rationale'].strip() or not check['evidence']):
                raise ValueError(f'invalid check status/rationale/evidence: {key}')
            if check['status'] not in {'Passed', 'NotApplicable'}:
                check_states.add('Failed' if check['status'] == 'Failed' else 'NotRun')
            # Applicable behavior needs concrete assertions; context/client-only
            # inputs may instead have an explicit, independently reviewed exclusion.
            if check['status'] == 'Passed' and (not check['command'].strip() or not check['assertions'].strip()):
                raise ValueError(f'executable command/assertions missing: {key}')
            for evidence in check['evidence']:
                FILES.execution_file(root, evidence, review_files)
                report_paths.add(evidence['path'])
            tests = check.get('tests', [])
            covered_tests.update(tests)
            if check['status'] == 'NotApplicable' and (
                key.startswith('matrix:') or key.endswith(':acceptance')
                or (row.get('coverage') == 'TargetedEvidence' and not check['id'].startswith('table-row:'))
            ):
                raise ValueError(f'applicable behavior/state cases cannot be waived: {key}')
            if key.startswith('matrix:') and check['status'] == 'Passed' and not tests:
                raise ValueError(f'matrix case requires exact passing tests: {key}')
            if check['id'].startswith('table-row:') and check['status'] == 'Passed' and not tests:
                raise ValueError(f'table row requires exact passing tests or a reviewed exclusion: {key}')
            if len(set(tests)) != len(tests) or not set(tests) <= passed:
                raise ValueError(f'check contains missing, duplicate or non-passing tests: {key}')
        if not set(row.get('tests', [])) <= covered_tests:
            raise ValueError(f'review does not bind mapped assertions: {key}')
        states[key] = 'Failed' if 'Failed' in check_states else ('NotRun' if 'NotRun' in check_states else 'Passed')
    for spec_result in result['profiles']:
        prefix = spec_result['protocol'] + ':'
        spec_result['independent_approved'] = sum(states[prefix + row['id']] == 'Passed'
                                                  for spec in manifest['specifications']
                                                  if spec['protocol'] == spec_result['protocol'] for row in spec['rows'])
        spec_result['acceptance'] = states[prefix + 'acceptance']
    failed = result['validation_status'] != 'Passed' or 'Failed' in states.values()
    result['overall_status'] = 'Failed' if failed else ('Passed' if all(s == 'Passed' for s in states.values()) else 'Partial')
    result['review_status'] = states
    result['source_digest'] = bundle['source_digest']
    result['results'] = bundle['results']
    result['reviewed_evidence_files'] = sorted(report_paths)
    result['approval_boundary'] = 'Offline integrity gate; reviewer identity authenticity and report truth require external verification.'
    return result

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--results', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--require-complete', action='store_true')
    parser.add_argument('--bundle', type=Path, help='Import source/log-bound independent reviews')
    parser.add_argument('--prepare', action='store_true', help='Write an unapproved review bundle')
    parser.add_argument('--producer', default='')
    args = parser.parse_args()
    try:
        root = args.root.resolve()
        manifest = read(root / 'contracts/protocol-review.json')
        if args.prepare:
            if not args.bundle or not args.results:
                raise ValueError('--prepare requires --bundle, --results and --producer')
            # Validate the controlled inventory before producing a plan.
            evaluate(root, manifest)
            results = args.results.resolve().relative_to(root).as_posix()
            bundle = prepare(root, manifest, results, args.producer)
            with args.bundle.open('x') as stream:
                stream.write(json.dumps(bundle, ensure_ascii=False, indent=2) + '\n')
            print(json.dumps({'overall_status': 'Partial', 'bundle': str(args.bundle),
                              'unapproved_obligations': len(bundle['reviews'])}))
            return 0
        if args.bundle and args.results:
            raise ValueError('a review bundle owns its bound log; do not override --results')
        result = (evaluate_bundle(root, manifest, read(args.bundle)) if args.bundle else
                  evaluate(root, manifest, args.results.read_text() if args.results else None))
        bound = ['contracts/protocol-review.json']
        manifest = read(root / bound[0])
        bound.extend(p for spec in manifest['specifications'] for p in (spec['numbered_source'], spec['supplement_source']))
        result['input_sha256'] = {p: hashlib.sha256(local(root, p).read_bytes()).hexdigest() for p in bound}
        if args.results:
            result['results_sha256'] = hashlib.sha256(args.results.read_bytes()).hexdigest()
        if args.output:
            args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
        print(json.dumps(result, ensure_ascii=False))
        return int(result['validation_status'] != 'Passed' or result['overall_status'] == 'Failed' or (args.require_complete and result['overall_status'] != 'Passed'))
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(1, f'Protocol review rejected: {error}\n')


if __name__ == '__main__':
    raise SystemExit(main())
