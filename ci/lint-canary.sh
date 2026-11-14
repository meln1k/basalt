#!/usr/bin/env bash
# Proves the determinism lints in clippy.toml still fire in core code: runs clippy on
# ci/lint-canary, a crate of deliberate violations, and fails unless clippy rejects every one.
set -euo pipefail

cd "$(dirname "$0")/.."

if output=$(cargo clippy --quiet --manifest-path ci/lint-canary/Cargo.toml \
    --target-dir target/lint-canary -- -D warnings 2>&1); then
    echo "lint canary: clippy accepted the deliberate violations in ci/lint-canary" >&2
    exit 1
fi

status=0
for expected in \
    'disallowed method `std::time::Instant::now`' \
    'disallowed method `std::time::SystemTime::elapsed`' \
    'disallowed type `std::collections::HashMap`' \
    'disallowed method `std::thread::spawn`' \
    'disallowed method `std::fs::read`' \
    'disallowed method `std::env::var`' \
    'disallowed type `std::process::Command`' \
    'disallowed type `std::os::unix::net::UnixStream`' \
    'disallowed macro `std::thread_local`'; do
    if grep -qF "$expected" <<<"$output"; then
        echo "lint canary: rejected as expected: $expected"
    else
        echo "lint canary: clippy did not report: $expected" >&2
        status=1
    fi
done

if [ "$status" -ne 0 ]; then
    echo "--- clippy output ---" >&2
    echo "$output" >&2
fi
exit "$status"
