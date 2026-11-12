#!/usr/bin/env python3
"""Check required developer deliverables and their local Markdown links offline."""
import argparse
from pathlib import Path
import re
from urllib.parse import unquote, urlsplit


REQUIRED = (
    "docs/magicdatahub-integration.md", "scripts/verify-existing-host.py",
    "scripts/load-soak.py", "docs/load-baseline.md", "docs/protocol-review.md",
    "contracts/protocol-review.json", "contracts/mqtt311-review-inputs.json",
    "contracts/mqtt5-review-inputs.json", "scripts/audit-protocol.py",
    "scripts/check-protocol-review.py", "contracts/acceptance-profile.json",
    "scripts/check-acceptance-profile.py", "scripts/check-matrix-inventory.py",
    "contracts/mqtt5-text.json", "scripts/audit-mqtt5.py", "docs/protocol-integrity.md",
    "contracts/optional-functional.json", "scripts/check-feature-evidence.py", "docs/optional-features.md",
    "contracts/mqtt311-outbound.json",
    "contracts/project-must.json", "contracts/release-policy.json", "contracts/mqtt311-text.json",
    "scripts/check-release.py", "docs/release.md",
    "docs/toml.md", "examples/standalone-broker/mqtt.toml", "scripts/verify-toml.py",
    "README.md", "docs/getting-started.md", "docs/architecture.md", "docs/api.md",
    "docs/configuration.md", "docs/reliability.md", "docs/operations.md",
    "docs/remaining-work.md", "contracts/README.md", "contracts/mqtt311.json",
    "contracts/topic-vectors.json", ".github/workflows/check.yml",
    "contracts/mqtt311-packets.json", "contracts/mqtt311-audit.json",
    ".agents/skills/modorch-mqtt-development/SKILL.md",
    ".agents/skills/modorch-app-development/SKILL.md",
    ".agents/skills/modorch-module-development/SKILL.md",
)


def validate(root):
    root = root.resolve()
    errors = [f"missing required deliverable: {p}" for p in REQUIRED if not (root / p).is_file()]
    documents = {root / p for p in REQUIRED if p.endswith(".md")}
    documents.update((root / "docs").glob("*.md"))
    documents.update((root / "contracts").glob("*.md"))
    for document in sorted(documents):
        if not document.is_file():
            continue
        # Inline Markdown links; headings and external URLs need no filesystem check.
        for link in re.findall(r"\[[^\]\n]*\]\(([^)\n]+)\)", document.read_text()):
            target = link.strip().strip("<>")
            parsed = urlsplit(target)
            if parsed.scheme or parsed.netloc or not parsed.path:
                continue
            path = (document.parent / unquote(parsed.path)).resolve()
            if not path.is_relative_to(root) or not path.exists():
                errors.append(f"broken local link: {document.relative_to(root)} -> {target}")
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    errors = validate(parser.parse_args().root)
    for error in errors:
        print(error)
    print(f"Project deliverables and local links: {'Failed' if errors else 'Passed'}")
    return int(bool(errors))


if __name__ == "__main__":
    raise SystemExit(main())
