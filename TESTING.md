# HoneyBadgerBFT Crate Testing Checklist

This document outlines the testing plan for the `honeybadger` crate, focusing on the protocol logic, state transitions, component interactions, and API contracts.

## I. Unit Tests

### A. `src/types.rs`

#### `Batch` Struct & `Batch::new`

- [ ] `Batch::new` correctly sorts transactions.
- [ ] `Batch::new` handles an empty list of transactions.
- [ ] `Batch::new` handles a single transaction.
- [ ] `Batch::new` handles multiple transactions.

#### `hash_batch` Function

- [ ] `hash_batch` produces consistent hashes for identical batch content.
- [ ] `hash_batch` produces different hashes for different batch content.
- [ ] `hash_batch` produces the same hash regardless of initial transaction order (due to `Batch::new` sorting).
- [ ] `hash_batch` correctly handles length prefixing to prevent ambiguities (e.g., `[[1,2],[3]]` vs. `[[1,2,3]]`).

### B. `src/config.rs` (`HoneyBadgerConfig`)

#### `HoneyBadgerConfig::new` Validation

- [ ] Test with a valid configuration.
- [ ] Test `Err` return for `num_total_nodes = 0`.
- [ ] Test `Err` return for `num_faulty_nodes >= num_total_nodes`.
- [ ] Test `Err` return for `num_total_nodes < 3 * num_faulty_nodes + 1`.
- [ ] Test `Err` return if `own_id` is not in `all_node_ids`.
- [ ] Test `Err` return if `epochs_to_retain_state = 0`.
- [ ] Test `Err` return if `default_aba_round_timeout_ms = 0`.
- [ ] Test `Err` return if `default_rbc_instance_timeout_ms = 0`.

#### Threshold Methods

- [ ] Verify `high_threshold()` returns `N - F`.
- [ ] Verify `low_threshold()` returns `F + 1`.
- [ ] Verify `aba_quorum_threshold()` returns the expected value (currently `N - F`, ensure this aligns with ABA logic if it changes).

### C. `src/rbc.rs` (`ReliableBroadcast`)

#### Proposer Behavior (`propose`)

- [ ] `propose` returns `VAL` and `ECHO` messages correctly when called by the proposer.
- [ ] `propose` returns an error if a non-proposer calls it.
- [ ] `propose` handles being called multiple times by the proposer (effectively proposes only once).

#### Message Handling (`handle_message`)

- **VAL Messages:**
  - [ ] Correct node sends `ECHO` upon receiving a valid `VAL` from the proposer.
  - [ ] Proposer equivocation (different `VAL` contents): instance sticks to the first `VAL`'s hash.
  - [ ] `VAL` message from a non-proposer is rejected/errors.

- **ECHO Messages:**
  - [ ] `ECHO` senders are correctly stored for the given hash.
  - [ ] Node sends its own `ECHO` (relay condition) upon receiving `F+1` `ECHO`s for a known payload hash, if it hasn't echoed yet.
  - [ ] Node sends `READY` upon receiving `N-F` `ECHO`s for a hash it has also sent `ECHO` for.
- **READY Messages:**
  - [ ] `READY` senders are correctly stored for the given hash.
  - [ ] Node sends its own `READY` (relay condition) upon receiving `F+1` `READY`s for a hash it has also sent `ECHO` for, if it hasn't sent `READY` yet.
  - [ ] Delivery condition: Node decides/accepts hash upon receiving `N-F` `READY`s for a hash it has the payload for and has processed (e.g., sent `ECHO` or `READY` for).

#### State Transitions

- [ ] `is_decided()` reflects the correct state after various message sequences leading to a decision.
- [ ] `output()` returns the correct batch after a decision, and `None` otherwise or after first call.

#### Idempotency

- [ ] Processing the same valid message multiple times does not lead to incorrect state or duplicate outgoing messages.

### D. `src/aba.rs` (`BinaryByzantineAgreement` with `DummyCommonCoin`)

#### `propose` Method

- [ ] `propose` sends the initial `Est` message for round 0 with the correct value.
- [ ] `propose` requests a timer for round 0 with the correct suggested duration.

#### Message Handling (`handle_message`)

- **EST Messages:**
  - [ ] `Est` values are correctly stored for the current round.
  - [ ] Node sends `Aux` upon receiving `F+1` `Est`s for a value in the current round.

- **AUX Messages:**
  - [ ] `Aux` values are correctly stored for the current round.
  - [ ] Decision logic:
    - [ ] Decides correctly if `N-F` `Aux` messages are received for a single value.
    - [ ] If CONF set is ambiguous (e.g., `N-F` `Aux` for multiple values), verify it calls `coin.request_coin_value()`.
    - [ ] With `DummyCommonCoin`, an ambiguous CONF set leads to an immediate coin value and starting the next round.
- **COIN Messages:**
  - [ ] `Coin` messages are processed by calling `coin.submit_coin_share()`.
  - [ ] With `DummyCommonCoin`, `submit_coin_share` returning `Some(value)` (though current dummy doesn't) should trigger `start_next_round`.

#### Round Advancement (`start_next_round`)

- [ ] `current_round` is correctly incremented.
- [ ] A new `Est` message is generated for the new round using the coin's output.
- [ ] A timer is requested for the new round with the correct suggested duration.

#### Future Round Message Queuing

- [ ] Send `Est` messages for `round > current_round`; verify they are stored but do not trigger immediate actions.
- [ ] Send `Aux` messages for `round > current_round`; verify they are stored but do not trigger immediate actions.
- [ ] Advance to a future round for which messages were queued.
- [ ] Verify `start_next_round` (via `process_potential_progress_for_current_round`) correctly processes these queued `Est` and `Aux` messages.

#### Timeout Handling (`handle_round_timeout`)

- [ ] `handle_round_timeout` calls `coin.get_coin_for_round_fallback()`.
- [ ] `handle_round_timeout` starts the next round using the fallback coin value.
- [ ] `handle_round_timeout` clears `waiting_for_coin_in_round`.

#### State Transitions

- [ ] `is_decided()` reflects the correct state after various sequences.
- [ ] `output()` returns the correct boolean value after a decision.

## II. Integration Tests (`HoneyBadgerBft`)

*(These tests require a test harness to simulate multiple `HoneyBadgerBft` instances, message routing, and timer management.)*

### Scenarios

- [ ] **Happy Path (All Honest Nodes):**
  - [ ] Single proposer epoch: all nodes decide on the proposed batch.
  - [ ] All N nodes propose: all nodes decide on the same set of N batches.
  - [ ] Multiple epochs: correct epoch advancement and decisions over several epochs.
  - [ ] Transaction flow: submitted transactions appear in decided batches.
- [ ] **RBC Timeout Scenarios:**
  - [ ] Proposer crashes/`VAL` lost: other nodes generate `SetRbcTimeout` requests.
  - [ ] Simulate RBC timer expiry: `handle_timer_expiry` leads to `initiate_aba_for_rbc(false)`.
  - [ ] Epoch finalizes, excluding batch from faulty/silent proposer.
- [ ] **ABA Round Timeout Scenarios:**
  - [ ] Simulate message loss causing ABA round stall: `SetAbaRoundTimeout` requests generated.
  - [ ] Simulate ABA timer expiry: `handle_timer_expiry` calls `aba_instance.handle_round_timeout`.
  - [ ] ABA progresses using `DummyCommonCoin` and eventually decides.
- [ ] **Automated Epoch Advancement:**
  - [ ] Upon epoch finalization, messages and timer requests for the *next* epoch are automatically generated.
- [ ] **State Pruning:**
  - [ ] Run for `M > config.epochs_to_retain_state` epochs.
  - [ ] Verify (e.g., by inspecting map sizes if possible, or inferring) that internal state maps (`rbc_instances`, `aba_instances`, etc.) are pruned.
- [ ] **Basic Fault Tolerance (Simulated Crash Faults):**
  - [ ] Up to `F` nodes stop participating.
  - [ ] Verify remaining correct nodes can still make progress and finalize epochs.
- [ ] **RBC Proposer Equivocation (Basic):**
  - [ ] Proposer sends different `VAL` messages.
  - [ ] Verify `ReliableBroadcast` instances handle this by sticking to the first valid `VAL`.
  - [ ] Verify HBBFT still agrees on a set of batches (equivocating proposer's batch might be excluded).

## III. Limitations of Testing Within This Crate

- [ ] **Security of the Real Common Coin:** The cryptographic security (unpredictability, unbiasability) of the common coin mechanism itself is NOT tested here, as `DummyCommonCoin` is used. This is the responsibility of the consuming application providing the `CommonCoin` implementation.
- [ ] **Network Adversary:** Complex network adversary behaviors (beyond simple message loss simulated in tests) are not covered.
- [ ] **Complex Byzantine Behaviors in External Coin Generation:** If the real common coin involves an interactive protocol, Byzantine nodes participating in *that* protocol are not simulated here.
- [ ] **Performance Benchmarking:** These tests focus on correctness, not performance.
