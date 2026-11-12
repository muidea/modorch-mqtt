#!/bin/sh
set -eu
python3 scripts/check-project.py
python3 scripts/check-release.py --check-catalog
python3 scripts/check-protocol-review.py
python3 scripts/check-acceptance-profile.py
python3 scripts/check-matrix-inventory.py
cargo fmt --all -- --check
cargo check --workspace --all-targets --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
# The default build has neither MQTT5's skip loops nor TLS's optional field.
cargo clippy -p existing-modorch-server --all-targets --locked -- -D warnings
conformance_log=$(mktemp)
conformance_result=$(mktemp)
release_dir=$(mktemp -d)
trap 'rm -f "$conformance_log" "$conformance_result" "$release_dir/bundle.json" "$release_dir/result.json" "$release_dir/benchmark.json"; rmdir "$release_dir"' 0
if cargo test --workspace --all-features --locked >"$conformance_log" 2>&1; then
    cat "$conformance_log"
else
    cat "$conformance_log"
    exit 1
fi
python3 scripts/check-feature-evidence.py --results "$conformance_log"
python3 scripts/check-protocol-review.py --results "$conformance_log"
python3 -m unittest discover -s scripts/tests -v
python3 scripts/check-conformance.py --results "$conformance_log" --output "$conformance_result"
# Bind an unapproved plan to the same Cargo log; full release remains separate.
# The log must be local to the repository for the evidence path policy.
release_log="docs/validation/.release-check-$$.log"
cp "$conformance_log" "$release_log"
trap 'rm -f "$conformance_log" "$conformance_result" "$release_log" "$release_dir/bundle.json" "$release_dir/result.json" "$release_dir/benchmark.json"; rmdir "$release_dir"' 0
python3 scripts/check-release.py --prepare --results "$release_log" --bundle "$release_dir/bundle.json"
python3 scripts/check-release.py --bundle "$release_dir/bundle.json" --output "$release_dir/result.json" --allow-partial
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked
cargo run -p existing-modorch-server --locked -- --once
python3 scripts/verify-existing-host.py
cargo run -p secure-modorch-server --locked -- --demo --once
python3 scripts/benchmark.py --profile debug --clients 1 --messages 2 --qos 2 --compositions tls-auth-storage --output "$release_dir/benchmark.json"
cargo build -p standalone-broker --locked
python3 scripts/verify-toml.py
cargo build -p durable-broker --locked
python3 scripts/verify-storage.py
