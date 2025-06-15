// This content is intended for tests/integration.rs

use std::collections::HashSet;

use honeybadger::testing::{DEFAULT_EPOCH, TestHarness};
use honeybadger::{Batch, EpochNum, NodeId, TimerExpiry, Transaction}; // Added Batch and Transaction // For comparing sets of decided batches

#[test]
fn happy_path_single_proposer_epoch() {
    let num_nodes = 4;
    let node_ids: Vec<NodeId> = (0..num_nodes as NodeId).collect();
    let mut harness = TestHarness::new(node_ids.clone());

    let proposer_id: NodeId = 0;
    let epoch_to_test: EpochNum = DEFAULT_EPOCH;

    let tx1: Transaction = vec![1, 2, 3];
    let tx2: Transaction = vec![4, 5, 6];
    let proposed_batch_txs = vec![tx1.clone(), tx2.clone()];
    let expected_batch = Batch::new(proposed_batch_txs.clone()); // Batch::new sorts them

    // Node 0 proposes a batch for epoch 0
    harness
        .propose_on_node(proposer_id, epoch_to_test, proposed_batch_txs)
        .expect("Propose failed");

    // Run the simulation loop
    let max_steps = 500; // Safety break
    for i in 0..max_steps {
        if harness.all_nodes_decided_epoch(epoch_to_test) {
            break;
        }
        if !harness.step() && harness.network_queue.is_empty() && harness.timers.is_empty() {
            // No progress, no messages, no timers - system might be stalled or done
            if harness.all_nodes_decided_epoch(epoch_to_test) {
                break;
            } else {
                // If not all decided, but no more actions, then it's a stall.
                // We might need to check individual node states or print debug info.
                eprintln!(
                    "Simulation stalled at step {i} before all nodes decided epoch {epoch_to_test}",
                );
                for (id, node_data) in &harness.finalized_outputs {
                    eprintln!("Node {id}: finalized epochs {keys:?}", keys = node_data.keys());
                }
                // Dump network queue and timers
                eprintln!("Network queue size: {}", harness.network_queue.len());
                for (time, timers) in &harness.timers {
                    eprintln!("Timers at {time:?}: {timer_count:?}", timer_count = timers.len());
                }

                panic!("Simulation stalled or did not complete as expected.");
            }
        }
        if i == max_steps - 1 {
            panic!("Max simulation steps reached, test did not complete.");
        }
    }

    // Assertions
    assert!(
        harness.all_nodes_decided_epoch(epoch_to_test),
        "Not all nodes decided on epoch {epoch_to_test}",
    );

    for node_id in node_ids {
        let node_output = harness
            .finalized_outputs
            .get(&node_id)
            .expect("Node should have output")
            .get(&epoch_to_test)
            .expect("Node should have output for the test epoch");

        // In a single proposer scenario, we expect exactly one batch.
        assert_eq!(
            node_output.len(),
            1,
            "Node {node_id} decided on {batch_count} batches, expected 1",
            batch_count = node_output.len()
        );
        let decided_batch_info = &node_output[0];
        assert_eq!(
            decided_batch_info.proposer_id, proposer_id,
            "Node {node_id} decided on batch from wrong proposer",
        );
        assert_eq!(
            decided_batch_info.batch, expected_batch,
            "Node {node_id} decided on incorrect batch content",
        );
        assert_eq!(
            decided_batch_info.epoch, epoch_to_test,
            "Node {node_id} decided on batch for wrong epoch",
        );
    }
}

#[test]
fn happy_path_all_n_nodes_propose() {
    let num_nodes = 4;
    let node_ids: Vec<NodeId> = (0..num_nodes as NodeId).collect();
    let mut harness = TestHarness::new(node_ids.clone());

    let epoch_to_test: EpochNum = DEFAULT_EPOCH;
    let mut expected_decided_batches_set = HashSet::new();

    // Each node proposes a unique batch
    for &proposer_id in &node_ids {
        let tx_content: u8 = proposer_id as u8 + 100; // Unique content for each proposer
        let tx: Transaction = vec![tx_content, tx_content + 1, tx_content + 2];
        let proposed_batch_txs = vec![tx.clone()];
        let expected_batch = Batch::new(proposed_batch_txs.clone());

        expected_decided_batches_set.insert((proposer_id, expected_batch.clone()));

        harness
            .propose_on_node(proposer_id, epoch_to_test, proposed_batch_txs)
            .unwrap_or_else(|_| panic!("Propose failed for node {proposer_id}"));
    }

    // Run the simulation loop
    let max_steps = 1000; // Might need more steps if N is larger or many messages
    for i in 0..max_steps {
        if harness.all_nodes_decided_epoch(epoch_to_test) {
            // Check if all nodes have N batches for this epoch
            let mut all_have_n_batches = true;
            for node_id_check in &node_ids {
                if let Some(epochs_data) = harness
                    .finalized_outputs
                    .get(node_id_check)
                {
                    if let Some(batches) = epochs_data.get(&epoch_to_test) {
                        if batches.len() != num_nodes {
                            all_have_n_batches = false;
                            break;
                        }
                    } else {
                        all_have_n_batches = false;
                        break;
                    }
                } else {
                    all_have_n_batches = false;
                    break;
                }
            }
            if all_have_n_batches {
                break;
            }
        }

        if !harness.step() && harness.network_queue.is_empty() && harness.timers.is_empty() {
            eprintln!(
                "Simulation stalled at step {i} before all nodes decided epoch {epoch_to_test} \
                 with {num_nodes} batches each.",
            );
            panic!("Simulation stalled or did not complete as expected.");
        }
        if i == max_steps - 1 {
            panic!("Max simulation steps reached, test did not complete.");
        }
    }

    // Assertions
    assert!(
        harness.all_nodes_decided_epoch(epoch_to_test),
        "Not all nodes decided on epoch {epoch_to_test}",
    );

    for node_id_check in &node_ids {
        let node_output_for_epoch =
            harness
                .finalized_outputs
                .get(node_id_check)
                .unwrap_or_else(|| panic!("Node {node_id_check} should have output"))
                .get(&epoch_to_test)
                .unwrap_or_else(|| {
                    panic!(
                        "Node {node_id_check} should have output for the test epoch \
                         {epoch_to_test}",
                    )
                });

        assert_eq!(
            node_output_for_epoch.len(),
            num_nodes,
            "Node {node_id_check} decided on {batch_count} batches, expected {num_nodes}",
            batch_count = node_output_for_epoch.len(),
        );

        let decided_set_for_node: HashSet<(NodeId, Batch)> = node_output_for_epoch
            .iter()
            .map(|db| (db.proposer_id, db.batch.clone()))
            .collect();
        assert_eq!(
            decided_set_for_node, expected_decided_batches_set,
            "Node {node_id_check} has a different set of decided batches",
        );
    }
    println!("Epoch 1 successfully finalized by all nodes.");
}

#[test]
fn rbc_timeout_proposer_silent() {
    let num_nodes = 4;
    // let f = (num_nodes - 1) / 3; // f=1
    let node_ids: Vec<NodeId> = (0..num_nodes as NodeId).collect();
    let mut harness = TestHarness::new(node_ids.clone());

    let silent_proposer_id: NodeId = 0;
    let epoch_to_test: EpochNum = DEFAULT_EPOCH;

    // Other nodes prepare to participate in epoch 0.
    // They will expect a proposal from silent_proposer_id.
    // Their own proposals will be empty.
    let mut expected_decided_batches_set = HashSet::new();
    for &node_id in &node_ids {
        if node_id != silent_proposer_id {
            harness
                .prepare_and_propose_on_node(node_id, epoch_to_test)
                .unwrap_or_else(|e| {
                    panic!(
                        "Prepare and propose failed for node {node_id} on epoch {epoch_to_test}: \
                         {e:?}"
                    )
                });
            let empty_batch = Batch::new(vec![]);
            expected_decided_batches_set.insert((node_id, empty_batch));
        }
    }

    // Check that SetRbcTimeout requests were generated for the silent proposer.
    // These timers are set when ensure_epoch_active_and_get_timers is called.
    let mut rbc_timeout_found_for_silent_proposer = false;
    for (_expiry_instant, timer_expiries) in harness.timers.iter() {
        for timer_expiry in timer_expiries {
            if let TimerExpiry::RbcTimeout {
                epoch,
                proposer_id: timer_proposer_id,
            } = timer_expiry
            {
                if *epoch == epoch_to_test && *timer_proposer_id == silent_proposer_id {
                    rbc_timeout_found_for_silent_proposer = true;
                    break;
                }
            }
        }
        if rbc_timeout_found_for_silent_proposer {
            break;
        }
    }
    assert!(
        rbc_timeout_found_for_silent_proposer,
        "SetRbcTimeout for silent proposer {silent_proposer_id} in epoch {epoch_to_test} was not \
         found."
    );

    // Run the simulation loop
    let max_steps = 1000;
    for i in 0..max_steps {
        if harness.all_nodes_decided_epoch(epoch_to_test) {
            let mut all_have_correct_num_batches = true;
            for node_id_check in &node_ids {
                if let Some(epochs_data) = harness
                    .finalized_outputs
                    .get(node_id_check)
                {
                    if let Some(batches) = epochs_data.get(&epoch_to_test) {
                        if batches.len() != num_nodes - 1 {
                            all_have_correct_num_batches = false;
                            break;
                        }
                    } else {
                        all_have_correct_num_batches = false;
                        break;
                    }
                } else {
                    all_have_correct_num_batches = false;
                    break;
                }
            }
            if all_have_correct_num_batches {
                break;
            }
        }

        if !harness.step() && harness.network_queue.is_empty() && harness.timers.is_empty() {
            panic!("RBC Timeout Test: Simulation stalled at step {i}");
        }
        if i == max_steps - 1 {
            panic!("RBC Timeout Test: Max simulation steps reached");
        }
    }

    assert!(
        harness.all_nodes_decided_epoch(epoch_to_test),
        "RBC Timeout Test: Not all nodes decided on epoch {epoch_to_test}",
    );

    for node_id_check in &node_ids {
        let node_output_for_epoch = harness
            .finalized_outputs
            .get(node_id_check)
            .unwrap_or_else(|| panic!("Node {node_id_check} should have output"))
            .get(&epoch_to_test)
            .unwrap_or_else(|| {
                panic!("Node {node_id_check} should have output for epoch {epoch_to_test}")
            });

        assert_eq!(
            node_output_for_epoch.len(),
            num_nodes - 1,
            "Node {node_id_check} decided on {count} batches, expected {expected_count}",
            count = node_output_for_epoch.len(),
            expected_count = num_nodes - 1
        );

        let decided_set_for_node: HashSet<(NodeId, Batch)> = node_output_for_epoch
            .iter()
            .map(|db| (db.proposer_id, db.batch.clone()))
            .collect();
        assert_eq!(decided_set_for_node, expected_decided_batches_set);

        for decided_batch_info in node_output_for_epoch {
            assert_ne!(decided_batch_info.proposer_id, silent_proposer_id);
        }
    }
    println!(
        "RBC Timeout Test for silent proposer {silent_proposer_id} in epoch {epoch_to_test} \
         passed."
    );
}

#[test]
fn aba_round_timeout_and_recovery() {
    let num_nodes = 4;
    let node_ids: Vec<NodeId> = (0..num_nodes as NodeId).collect();
    // Use a harness with DummyCommonCoin for this test, as we are testing
    // the timeout fallback and immediate coin paths, not share exchange.
    let mut harness =
        TestHarness::new_with_config_modifier(node_ids.clone(), |num_nodes, f, id| {
            honeybadger::testing::create_dummy_config(num_nodes, f, id)
        });

    let proposer_id: NodeId = 0;
    let epoch_to_test: EpochNum = DEFAULT_EPOCH;

    let tx1: Transaction = vec![7, 8, 9];
    let proposed_batch_txs = vec![tx1.clone()];
    let expected_batch = Batch::new(proposed_batch_txs.clone());

    harness
        .propose_on_node(proposer_id, epoch_to_test, proposed_batch_txs)
        .expect("Propose failed");

    // Phase 1: Let RBC deliver and ABA start (timer for round 0 set)
    // We need to ensure RBC for proposer_id has delivered on all nodes before
    // clearing the queue. We'll run for a fixed number of steps that should be
    // sufficient for N=4 RBC, and also check that the ABA timer gets set as an
    // indicator of progress.
    println!("ABA Timeout Test: Starting Phase 1.");
    let mut aba_r0_timer_is_set_in_harness = false;
    const SUFFICIENT_STEPS_FOR_RBC_AND_ABA_START: usize = 100; // Increased from typical few steps

    for _ in 0..SUFFICIENT_STEPS_FOR_RBC_AND_ABA_START {
        harness.step();
        // Check if the timer is set after each step, but continue for fixed steps
        // to give all nodes a chance for their RBC to complete.
        if !aba_r0_timer_is_set_in_harness {
            // Only check if not already found
            for timer_expiries in harness.timers.values() {
                for timer_expiry in timer_expiries {
                    if let TimerExpiry::AbaRoundTimeout {
                        epoch,
                        batch_proposer_id: bp_id,
                        round,
                    } = timer_expiry
                    {
                        if *epoch == epoch_to_test && *bp_id == proposer_id && *round == 0 {
                            aba_r0_timer_is_set_in_harness = true;
                            break;
                        }
                    }
                }
                if aba_r0_timer_is_set_in_harness {
                    break;
                }
            }
        }
    }

    // After sufficient steps, assert the timer is now set in the harness.
    // This implies at least one node (and by now, likely all) has started ABA for
    // proposer_id. The key is that SUFFICIENT_STEPS_FOR_RBC_AND_ABA_START
    // should have allowed RBC to deliver on all nodes.
    if !aba_r0_timer_is_set_in_harness {
        // Final check if not found during loop
        for timer_expiries in harness.timers.values() {
            for timer_expiry in timer_expiries {
                if let TimerExpiry::AbaRoundTimeout {
                    epoch,
                    batch_proposer_id: bp_id,
                    round,
                } = timer_expiry
                {
                    if *epoch == epoch_to_test && *bp_id == proposer_id && *round == 0 {
                        aba_r0_timer_is_set_in_harness = true;
                        break;
                    }
                }
            }
            if aba_r0_timer_is_set_in_harness {
                break;
            }
        }
    }
    assert!(
        aba_r0_timer_is_set_in_harness,
        "ABA R0 timer for proposer {proposer_id} was not set after \
         {SUFFICIENT_STEPS_FOR_RBC_AND_ABA_START} steps. RBC might not have completed on all \
         nodes."
    );
    println!(
        "ABA Timeout Test: Phase 1 completed. Assumed RBC for proposer {proposer_id} delivered on \
         all nodes and its ABA R0 timer is set in harness."
    );

    // Phase 2: Induce timeout for round 0 by clearing the message queue.
    // This ensures that the ABA round 0 timer will expire because EST/AUX messages
    // for round 0 won't be able to lead to a decision.
    harness.network_queue.clear();
    println!("ABA Timeout Test: Network queue cleared to induce ABA round 0 timeout.");

    // Phase 3: Run simulation until epoch finalizes
    let max_steps_phase3 = 1000;
    for i in 0..max_steps_phase3 {
        if harness.all_nodes_decided_epoch(epoch_to_test) {
            // Once all nodes have decided the epoch, we can break.
            // The assertions outside the loop will verify the content.
            println!(
                "ABA Timeout Test: All nodes decided epoch {epoch_to_test} at step {i} of phase 3."
            );
            break;
        }
        if !harness.step() && harness.network_queue.is_empty() && harness.timers.is_empty() {
            panic!("ABA Timeout Test: Simulation stalled in phase 3 at step {i}");
        }
        if i == max_steps_phase3 - 1 {
            panic!("ABA Timeout Test: Max simulation steps reached in phase 3");
        }
    }

    assert!(harness.all_nodes_decided_epoch(epoch_to_test));
    for node_id_check in &node_ids {
        let node_output = harness
            .finalized_outputs
            .get(node_id_check)
            .unwrap()
            .get(&epoch_to_test)
            .unwrap();
        assert_eq!(node_output.len(), 1);
        assert_eq!(node_output[0].proposer_id, proposer_id);
        assert_eq!(node_output[0].batch, expected_batch);
    }
    println!("ABA Timeout Test: Epoch {epoch_to_test} finalized successfully after ABA timeout.");
}

#[test]
fn happy_path_transaction_flow() {
    let num_nodes = 4;
    let node_ids: Vec<NodeId> = (0..num_nodes as NodeId).collect();
    let mut harness = TestHarness::new(node_ids.clone());

    // --- Epoch 0: Synchronize and advance ---
    let epoch0: EpochNum = DEFAULT_EPOCH;
    let proposer_id_e0: NodeId = 0; // Arbitrary proposer for epoch 0
    harness
        .propose_on_node(proposer_id_e0, epoch0, vec![]) // Propose empty batch for sync
        .expect("Propose failed for epoch 0");

    let max_steps_e0 = 500;
    for i in 0..max_steps_e0 {
        if harness.all_nodes_decided_epoch(epoch0) {
            break;
        }
        if !harness.step() && harness.network_queue.is_empty() && harness.timers.is_empty() {
            panic!("Epoch 0 (sync): Simulation stalled at step {i}");
        }
        if i == max_steps_e0 - 1 {
            panic!("Epoch 0 (sync): Max simulation steps reached");
        }
    }
    assert!(harness.all_nodes_decided_epoch(epoch0), "Epoch 0 (sync): Not all nodes decided");
    println!("Epoch 0 (for sync) successfully finalized by all nodes.");

    // --- Submit Transactions for Epoch 1 ---
    let epoch1: EpochNum = epoch0 + 1;
    let node_receiving_txs: NodeId = 0; // Node 0 will receive and propose these txs

    let tx_a: Transaction = vec![1, 1, 1];
    let tx_b: Transaction = vec![2, 2, 2];
    let submitted_txs = vec![tx_a.clone(), tx_b.clone()];

    harness.submit_tx_to_node(node_receiving_txs, tx_a.clone());
    harness.submit_tx_to_node(node_receiving_txs, tx_b.clone());

    // Prepare expected output for epoch 1
    let mut expected_decided_batches_e1_set = HashSet::new();
    let batch_with_txs = Batch::new(submitted_txs); // Batch::new sorts them
    expected_decided_batches_e1_set.insert((node_receiving_txs, batch_with_txs.clone()));

    for &node_id in &node_ids {
        if node_id != node_receiving_txs {
            // Other nodes will propose empty batches
            let empty_batch = Batch::new(vec![]);
            expected_decided_batches_e1_set.insert((node_id, empty_batch));
        }
    }

    // Ensure all nodes are at epoch1 before they are asked to propose for it.
    // This should be implicitly true if epoch0 finalized for all.
    for node_id_check in &node_ids {
        assert_eq!(
            harness
                .nodes
                .get(node_id_check)
                .unwrap()
                .hbbft
                .current_epoch(),
            epoch1,
            "Node {node_id_check} is not at epoch {epoch1} before proposing for it."
        );
    }

    // --- Epoch 1: Nodes automatically propose (Node 0 with txs, others empty) ---
    // After submitting transactions, we explicitly tell node_receiving_txs to
    // prepare its proposal for epoch1. This ensures it picks up the newly
    // submitted transactions.
    harness
        .prepare_and_propose_on_node(node_receiving_txs, epoch1)
        .expect("Prepare and propose failed for node_receiving_txs on epoch 1");

    // Explicitly tell other nodes to prepare their (empty) proposals for epoch1.
    for &other_node_id in &node_ids {
        if other_node_id != node_receiving_txs {
            harness
                .prepare_and_propose_on_node(other_node_id, epoch1)
                .unwrap_or_else(|e| {
                    panic!("Prepare and propose failed for node {other_node_id} on epoch 1: {e:?}")
                });
        }
    }

    let max_steps_e1 = 1000;
    for i in 0..max_steps_e1 {
        if harness.all_nodes_decided_epoch(epoch1) {
            let mut all_have_n_batches_e1 = true;
            for node_id_check in &node_ids {
                if let Some(epochs_data) = harness
                    .finalized_outputs
                    .get(node_id_check)
                {
                    if let Some(batches) = epochs_data.get(&epoch1) {
                        if batches.len() != num_nodes {
                            all_have_n_batches_e1 = false;
                            break;
                        }
                    } else {
                        all_have_n_batches_e1 = false;
                        break;
                    }
                } else {
                    all_have_n_batches_e1 = false;
                    break;
                }
            }
            if all_have_n_batches_e1 {
                break;
            }
        }
        if !harness.step() && harness.network_queue.is_empty() && harness.timers.is_empty() {
            panic!("Epoch 1 (tx flow): Simulation stalled at step {i}");
        }
        if i == max_steps_e1 - 1 {
            panic!("Epoch 1 (tx flow): Max simulation steps reached");
        }
    }

    assert!(harness.all_nodes_decided_epoch(epoch1), "Epoch 1 (tx flow): Not all nodes decided");

    for node_id_check in &node_ids {
        let node_output_e1 = harness
            .finalized_outputs
            .get(node_id_check)
            .unwrap()
            .get(&epoch1)
            .unwrap();
        assert_eq!(
            node_output_e1.len(),
            num_nodes,
            "Epoch 1, Node {node_id_check}: Expected {num_nodes} batches",
        );

        let decided_set_for_node_e1: HashSet<(NodeId, Batch)> = node_output_e1
            .iter()
            .map(|db| (db.proposer_id, db.batch.clone()))
            .collect();
        assert_eq!(
            decided_set_for_node_e1, expected_decided_batches_e1_set,
            "Epoch 1, Node {node_id_check}: Batch set mismatch",
        );
    }
    println!("Epoch 1 (transaction flow) successfully finalized by all nodes.");
}

#[test]
fn happy_path_multiple_epochs() {
    let num_nodes = 4;
    let node_ids: Vec<NodeId> = (0..num_nodes as NodeId).collect();
    let mut harness = TestHarness::new(node_ids.clone());

    // --- Epoch 0 ---
    let epoch0: EpochNum = DEFAULT_EPOCH;
    let proposer_id_e0: NodeId = 0;
    let tx_e0: Transaction = vec![10, 20, 30];
    let proposed_batch_txs_e0 = vec![tx_e0.clone()];
    let expected_batch_e0 = Batch::new(proposed_batch_txs_e0.clone());

    harness
        .propose_on_node(proposer_id_e0, epoch0, proposed_batch_txs_e0)
        .expect("Propose failed for epoch 0");

    let max_steps_e0 = 500;
    for i in 0..max_steps_e0 {
        if harness.all_nodes_decided_epoch(epoch0) {
            break;
        }
        if !harness.step() && harness.network_queue.is_empty() && harness.timers.is_empty() {
            panic!("Epoch 0: Simulation stalled at step {i}");
        }
        if i == max_steps_e0 - 1 {
            panic!("Epoch 0: Max simulation steps reached");
        }
    }

    assert!(harness.all_nodes_decided_epoch(epoch0), "Epoch 0: Not all nodes decided");
    for node_id_check in &node_ids {
        let node_output = harness
            .finalized_outputs
            .get(node_id_check)
            .unwrap()
            .get(&epoch0)
            .unwrap();
        assert_eq!(node_output.len(), 1, "Epoch 0, Node {node_id_check}: Expected 1 batch");
        assert_eq!(
            node_output[0].proposer_id, proposer_id_e0,
            "Epoch 0, Node {node_id_check}: Wrong proposer",
        );
        assert_eq!(
            node_output[0].batch, expected_batch_e0,
            "Epoch 0, Node {node_id_check}: Wrong batch content",
        );
    }
    println!("Epoch 0 successfully finalized by all nodes.");

    // --- Epoch 1 ---
    // HBBFT should automatically advance to epoch 1 and nodes should propose
    // (likely empty batches if no new transactions were submitted to the harness).
    let epoch1: EpochNum = epoch0 + 1;
    let mut expected_decided_batches_e1_set = HashSet::new();

    // Ensure all nodes have advanced to epoch1 after epoch0 finalization.
    // This check confirms that the core HBBFT logic correctly increments its epoch.
    for node_id_check in &node_ids {
        assert_eq!(
            harness
                .nodes
                .get(node_id_check)
                .unwrap()
                .hbbft
                .current_epoch(),
            epoch1,
            "Node {node_id_check} should be at epoch {epoch1} after epoch 0 finalized."
        );
    }

    // Explicitly make each node propose for epoch 1.
    // Since no new transactions were submitted to these nodes for epoch 1 via the
    // harness, they will propose their current pending_transactions (which should
    // be empty).
    for &proposer_id_e1 in &node_ids {
        let empty_batch_e1 = Batch::new(vec![]);
        expected_decided_batches_e1_set.insert((proposer_id_e1, empty_batch_e1));

        // Instruct node `proposer_id_e1` to prepare and propose for `epoch1`.
        // Its `hbbft.current_epoch()` should be `epoch1` at this point.
        harness
            .prepare_and_propose_on_node(proposer_id_e1, epoch1)
            .unwrap_or_else(|e| {
                panic!("Prepare and propose failed for node {proposer_id_e1} on epoch 1: {e:?}")
            });
    }

    let max_steps_e1 = 1000; // Give more steps for the second epoch
    for i in 0..max_steps_e1 {
        if harness.all_nodes_decided_epoch(epoch1) {
            let mut all_have_n_batches_e1 = true;
            for node_id_check in &node_ids {
                if let Some(epochs_data) = harness
                    .finalized_outputs
                    .get(node_id_check)
                {
                    if let Some(batches) = epochs_data.get(&epoch1) {
                        if batches.len() != num_nodes {
                            all_have_n_batches_e1 = false;
                            break;
                        }
                    } else {
                        all_have_n_batches_e1 = false;
                        break;
                    }
                } else {
                    all_have_n_batches_e1 = false;
                    break;
                }
            }
            if all_have_n_batches_e1 {
                break;
            }
        }
        if !harness.step() && harness.network_queue.is_empty() && harness.timers.is_empty() {
            panic!("Epoch 1: Simulation stalled at step {i}");
        }
        if i == max_steps_e1 - 1 {
            panic!("Epoch 1: Max simulation steps reached");
        }
    }

    assert!(harness.all_nodes_decided_epoch(epoch1), "Epoch 1: Not all nodes decided");
    for node_id_check in &node_ids {
        let node_output_e1 = harness
            .finalized_outputs
            .get(node_id_check)
            .unwrap()
            .get(&epoch1)
            .unwrap();
        assert_eq!(
            node_output_e1.len(),
            num_nodes,
            "Epoch 1, Node {node_id_check}: Expected {num_nodes} batches",
        );
        let decided_set_for_node_e1: HashSet<(NodeId, Batch)> = node_output_e1
            .iter()
            .map(|db| (db.proposer_id, db.batch.clone()))
            .collect();
        assert_eq!(
            decided_set_for_node_e1, expected_decided_batches_e1_set,
            "Epoch 1, Node {node_id_check}: Batch set mismatch",
        );
    }
    println!("Epoch 1 successfully finalized by all nodes.");
}
