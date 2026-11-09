#!/usr/bin/env python3
"""Verify complete textual review inputs from explicit pinned OASIS HTML.

Inventory is structural evidence only. Roles, applicability and executable
coverage are reviewed separately; extraction cannot approve conformance.
"""
import argparse
import hashlib
from html.parser import HTMLParser
import json
from pathlib import Path
import re


BLOCKS = {'p', 'li', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'}
NORMATIVE = re.compile(r'\b(?:MUST|SHALL|REQUIRED)\b')
CLAUSE = re.compile(r'\[(MQTT[-–][0-9]+(?:[-.][0-9]+)+\.?-[0-9]+)\]')


def normalized(parts):
    return re.sub(r'\s+', ' ', ''.join(parts)).strip()


class Body(HTMLParser):
    def __init__(self):
        super().__init__()
        self.finished = False
        self.stack = []
        self.table = None
        self.table_rows = []
        self.cells = None
        self.cell = None
        self.section = 'front matter'
        self.blocks = []
        self.tables = []

    def handle_starttag(self, tag, attrs):
        if 'AppendixHeading1' in dict(attrs).get('class', '').split():
            self.finished = True
            self.stack.clear()
            self.table = None
        if self.finished:
            return
        if tag == 'table':
            if self.table is not None:
                raise ValueError('nested source table requires explicit parser review')
            self.table = []
            self.table_rows = []
        if self.table is not None and tag == 'tr':
            self.cells = []
        if self.table is not None and tag in {'td', 'th'}:
            values = dict(attrs)
            self.cell = {'text': [], 'colspan': values.get('colspan', '1'),
                         'rowspan': values.get('rowspan', '1')}
        if tag in BLOCKS:
            self.stack.append((tag, []))

    def handle_data(self, text):
        if not self.finished:
            for _, parts in self.stack:
                parts.append(text)
            if self.table is not None:
                self.table.append(text)
            if self.cell is not None:
                self.cell["text"].append(text)

    def handle_endtag(self, tag):
        if self.finished:
            return
        if self.table is not None and tag in {'td', 'th'} and self.cell is not None:
            self.cell['text'] = normalized(self.cell['text'])
            self.cells.append(self.cell)
            self.cell = None
        if self.table is not None and tag == 'tr' and self.cells is not None:
            self.table_rows.append(self.cells)
            self.cells = None
        if tag == 'table' and self.table is not None:
            self.tables.append({'section': self.section, 'source_text': normalized(self.table), 'rows': self.table_rows})
            self.table = None
        if self.stack and tag == self.stack[-1][0]:
            block, parts = self.stack.pop()
            text = normalized(parts)
            if block.startswith('h') and text:
                self.section = text
            if text and not self.stack and self.table is None:
                self.blocks.append({'section': self.section, 'source_text': text})


def inventory(raw, source):
    if hashlib.sha256(raw).hexdigest() != source['sha256']:
        raise ValueError('source HTML SHA256 does not match pinned OASIS source')
    parser = Body()
    parser.feed(raw.decode('cp1252'))
    if not parser.finished:
        raise ValueError('main-body/appendix boundary missing')
    candidates = []
    for at, row in enumerate(parser.blocks):
        if NORMATIVE.search(row['source_text']) and not CLAUSE.search(row['source_text']):
            candidates.append({
                'id': f'UN-{len(candidates) + 1:03}', **row,
                'preceding_context': [p['source_text'] for p in parser.blocks[max(0, at-2):at]],
                'following_context': [p['source_text'] for p in parser.blocks[at+1:at+3]],
            })
    sections = {}
    for row in parser.blocks:
        sections.setdefault(row['section'], []).append(row['source_text'])
    return {
        'version': 1, 'source': source,
        'claim': 'All main-body p/li/heading prose grouped by section, MUST/SHALL/REQUIRED candidates and whole tables; textual inventory, not semantic or independent approval.',
        'sections': [dict(id=f'SECTION-{i+1:03}', section=section, paragraphs=paragraphs)
                     for i, (section, paragraphs) in enumerate(sections.items())],
        'unnumbered': candidates,
        'tables': [{'id': f'TABLE-{i+1:03}', **t} for i, t in enumerate(parser.tables)],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-html', type=Path, required=True)
    parser.add_argument('--inventory', type=Path, required=True)
    args = parser.parse_args()
    try:
        controlled = json.loads(args.inventory.read_text())
        actual = inventory(args.source_html.read_bytes(), controlled['source'])
        if controlled != actual:
            raise ValueError('controlled complete textual inventory differs from source')
    except (OSError, ValueError, KeyError) as error:
        parser.exit(1, f'Protocol source review inputs rejected: {error}\n')
    print(f'Textual inputs verified: {len(actual["unnumbered"])} unnumbered candidates, {len(actual["tables"])} whole tables; conformance Partial')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
