# Remaining Steps for HoneyBadgerBFT Implementation

This document outlines the remaining steps to achieve a complete and robust HoneyBadgerBFT (HBBFT) state machine, suitable for use as an independent crate.

## I. Core Protocol Completion and Robustness (ABA & RBC Interaction)

### 1. Full ABA Protocol Implementation

The current `BinaryByzantineAgreement` (ABA) is a simplified placeholder. A complete and robust ABA protocol is essential.

* **Choose and Implement a Specific ABA Protocol:**
  * Consider well-known asynchronous ABA protocols like Mosteiro and Cao's ABA or adaptations of Ben-Or's Randomized Agreement.
  * The chosen protocol will define precise rules for message types (e.g., EST, AUX, CONF), sending conditions, and collection thresholds.
* **Precise Round Logic:**
  * Implement the exact state transitions and message handling logic dictated by the chosen ABA protocol.
  * Refine quorum thresholds in `HoneyBadgerConfig` (e.g., `aba_quorum_threshold()`) or add more specific methods if needed by the protocol.
  * Detail how the common coin outcome influences the estimate for subsequent rounds.
* **ABA Round Timeouts:**
  * Implement a timeout mechanism for ABA rounds to ensure liveness.
  * If an ABA round stalls (e.g., insufficient messages received), a timeout should trigger:
    * Consultation of the common coin.
    * Advancement to the next round with an estimate based on the coin.
  * The HBBFT state machine will need to signal to the consuming application (e.g., libp2p layer) when to set these timers and be notified upon their expiry.

### 2. Handling Non-Delivering RBCs (RBC Timeouts)

To prevent the entire epoch from stalling if a proposer's `ReliableBroadcast` (RBC) instance fails to deliver:

* **Implement RBC Timeouts:**
  * Each node must set a timeout for every `RBC_i` instance (for each potential proposer `i`) it participates in within an epoch.
* **ABA Input Based on RBC Outcome:**
  * If `RBC_i` delivers a batch before its timeout, the node inputs `true` to the corresponding `ABA_i`.
  * If the timeout expires and `RBC_i` has *not* delivered, the node *must* input `false` to `ABA_i`.
* **Implementation Details:**
  * `HoneyBadgerBft` needs to track pending RBCs for each epoch.
  * It must signal to the consuming application to set these RBC timeouts.
  * A new method in `HoneyBadgerBft` (e.g., `handle_rbc_timeout(epoch, proposer_id)`) will be invoked by the application upon timer expiry, which will then trigger `aba_instance.propose(false)`.

## II. Liveness and Continuous Operation

### 3. Automated Epoch Advancement

For continuous operation:

* Once an epoch `e` is finalized (in `try_finalize_epoch`), `HoneyBadgerBft` should automatically prepare for the next epoch `e+1`.
* This involves invoking `prepare_and_propose_current_epoch()` if there are pending transactions or to propose an empty batch (as HBBFT requires all nodes to contribute to the Asynchronous Common Subset - ACS).

### 4. Handling Future Round/Epoch Messages

Improve robustness in message handling:

* **ABA:** Explicitly queue messages for future ABA rounds and process them only when the node enters that specific round.
* **HBBFT:** While messages for old, decided epochs are correctly ignored, refine handling for messages from future epochs (beyond `self.current_epoch`). Consider queuing or processing if `ensure_epoch_instances` has already set up for that future epoch.

## III. State Management and API

### 5. State Pruning (Garbage Collection)

To manage memory in long-running nodes:

* Implement a strategy to prune state for old, finalized epochs. This includes data in:
  * `rbc_instances`
  * `aba_instances`
  * `my_epoch_proposals`
  * `rbc_outputs`
  * `aba_outputs`
* `decided_epoch_outputs` might also be pruned based on application requirements (e.g., keep the last `K` epochs).

### 6. Refining the API for Timers (Interaction with Consumer)

Since the HBBFT crate should not manage OS-level timers directly:

* **Output Timer Requests:** When HBBFT (or its sub-components like ABA) determines a timeout is needed, it should return a request to the consuming application to set a timer.
  * Example: `TimerRequest::SetRbcTimeout { epoch, proposer_id, duration_ms }`
  * Example: `TimerRequest::SetAbaRoundTimeout { epoch, batch_proposer_id, round, duration_ms }`
* **Input Timer Expiries:** The consuming application will notify HBBFT when a previously requested timer expires.
  * Example: `handle_timer_expiry(TimerExpiry::RbcTimeout { epoch, proposer_id })`
* Modify the return type of methods like `handle_message` to include `Vec<TimerRequest>`.

## IV. Advanced Features and Refinements

### 7. Common Coin Implementation (Beyond Dummy)

Replace `DummyCommonCoin` with a secure threshold cryptosystem.

* **Choose a Scheme:** E.g., based on threshold signatures or Distributed Key Generation (DKG) for a threshold coin.
* **Implement Crypto Logic:**
  * Define `CoinShare` concretely.
  * Implement generation, distribution, and combination of shares.
  * Handle `AbaMessageContent::Coin` messages for share exchange.
  * This will involve significant work in the `thresh_crypto` module.

### 8. Enhanced Error Handling and Reporting

* Review and expand `HbbftError` for more specific error conditions.
* Consider mechanisms to report or handle detected byzantine behavior more explicitly.

### 9. Thorough Testing

Comprehensive testing is critical.

* **Unit Tests:** For RBC, the chosen ABA protocol, and key logic in `HoneyBadgerBft`.
* **Integration Tests:** Simulate multiple `HoneyBadgerBft` instances interacting, covering:
  * Happy path scenarios.
  * RBC timeouts leading to ABA(false).
  * ABA rounds requiring coin flips.
  * Epoch finalization and advancement.
  * Scenarios with `F` faulty nodes (crash faults, byzantine proposers).
* **Property-Based Testing:** Consider for components where applicable.

## Summary of Immediate Priorities

1. **ABA Protocol Selection & Full Implementation:** This is the most critical next step for core functionality.
2. **RBC Timeout Logic:** Essential for liveness when proposers fail or are slow.
3. **ABA Round Timeouts:** Crucial for ABA's own liveness.
4. **Timer API Definition and Integration:** Define how the HBBFT crate requests and responds to timers managed by the consumer.
5. **Automated Epoch Advancement:** For continuous, hands-off operation.

---

This outline should provide a clear roadmap for completing your HoneyBadgerBFT implementation.
