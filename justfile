lint:
    cargo +nightly clippy --lib -- -D warnings
    cargo +nightly fmt --all -- --check

fmt:
    cargo +nightly fmt --all
