# Run tests with optional test name and backend selection
# Usage: 
#   just test                   # Run all tests with log backend
#   just test test_name         # Run specific test with log backend
#   just test --log=tracing         # Run all tests with tracing backend
#   just test --log=tracing test_name # Run specific test with tracing backend
test *args="":
    #!/usr/bin/env bash
    set -e
    FEATURES="testing"
    LOG_LEVEL="debug"
    TEST_NAME=""
    RUN_IGNORED=""
    EXACT_MODIFIER=""
    
    # Parse arguments
    for arg in {{args}}; do
        if [[ "$arg" == "--log=tracing" ]]; then
            LOG_FEATURE="tracing"
        elif [[ "$arg" == "--log=log" ]]; then
            LOG_FEATURE="log"
        elif [[ "$arg" == "--info" ]]; then
            LOG_LEVEL="info"
        elif [[ "$arg" == "--warn" ]]; then
            LOG_LEVEL="warn"
        elif [[ "$arg" == "--error" ]]; then
            LOG_LEVEL="error"
        elif [[ "$arg" == "--debug" ]]; then
            LOG_LEVEL="debug"
        elif [[ "$arg" == "--trace" ]]; then
            LOG_LEVEL="trace"
        elif [[ "$arg" == "--ignored" ]]; then
            RUN_IGNORED="--run-ignored all"
        elif [[ "$arg" == "--exact" ]]; then
            EXACT_MODIFIER="="
        else
            TEST_NAME="$arg"
        fi
    done

    # If no test name is provided, set the logging level to "warn"
    if [[ -z "$TEST_NAME" ]]; then
        LOG_LEVEL="warn"
    fi
    
    # Run the tests with appropriate flags
    RUST_LOG=${LOG_LEVEL} PROPTEST_CASES=25 cargo nextest --color always run --no-fail-fast \
        $([[ -n "$TEST_NAME" ]] && echo "--nocapture" || echo "") \
        ${RUN_IGNORED} \
        --features "${FEATURES}" \
        $([[ -n "$TEST_NAME" ]] && echo "--filterset test(${TEST_NAME})" || echo "")

lint:
    cargo +nightly clippy --lib --tests -- -D warnings
    cargo +nightly fmt --all -- --check

fmt:
    cargo +nightly fmt --all
