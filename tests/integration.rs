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
fn state_pruning_works() {
    let num_nodes = 4;
    let node_ids: Vec<NodeId> = (0..num_nodes as NodeId).collect();
    let epochs_to_retain: EpochNum = 2; // Keep state for only 2 epochs

    let mut harness =
        TestHarness::new_with_config_modifier(node_ids.clone(), |num_nodes, f, id| {
            let all_node_ids_set: std::collections::BTreeSet<NodeId> =
                (0..num_nodes as NodeId).collect();
            let coin_factory =
                honeybadger::CoinFactory::new(std::sync::Arc::new(|_node_id, _epoch, _config| {
                    Box::new(honeybadger::testing::DummyCommonCoin {})
                }));
            std::sync::Arc::new(
                honeybadger::HoneyBadgerConfigBuilder::new(id, all_node_ids_set, f, coin_factory)
                    .epochs_to_retain_state(epochs_to_retain)
                    .build()
                    .unwrap(),
            )
        });

    let num_epochs_to_run = epochs_to_retain + 3; // e.g., 2 + 3 = 5 epochs (0, 1, 2, 3, 4)

    for current_epoch_num in 0..num_epochs_to_run {
        println!("State Pruning Test: Starting epoch {current_epoch_num}");

        // Each node proposes an empty batch in each epoch to ensure activity
        for &proposer_id in &node_ids {
            // Ensure node is at the current epoch before proposing
            // This might involve running steps if a node fell behind, but for this test,
            // we assume `prepare_and_propose_on_node` handles advancing the node.
            if harness
                .nodes
                .get(&proposer_id)
                .unwrap()
                .hbbft
                .current_epoch()
                <= current_epoch_num
            {
                harness
                    .prepare_and_propose_on_node(proposer_id, current_epoch_num)
                    .unwrap_or_else(|e| {
                        panic!(
                            "Prepare and propose failed for node {proposer_id} on epoch \
                             {current_epoch_num}: {e:?}",
                        )
                    });
            }
        }

        let max_steps_per_epoch = 1000;
        for i in 0..max_steps_per_epoch {
            if harness.all_nodes_decided_epoch(current_epoch_num) {
                // Ensure all nodes have num_nodes batches for this epoch
                let mut all_correct_for_epoch = true;
                for node_id_check in &node_ids {
                    if let Some(epoch_data) = harness
                        .finalized_outputs
                        .get(node_id_check)
                    {
                        if let Some(batches) = epoch_data.get(&current_epoch_num) {
                            if batches.len() != num_nodes {
                                all_correct_for_epoch = false;
                                break;
                            }
                        } else {
                            all_correct_for_epoch = false;
                            break;
                        }
                    } else {
                        all_correct_for_epoch = false;
                        break;
                    }
                }
                if all_correct_for_epoch {
                    break;
                }
            }
            if !harness.step() && harness.network_queue.is_empty() && harness.timers.is_empty() {
                panic!(
                    "State Pruning Test: Simulation stalled in epoch {current_epoch_num} at step \
                     {i}"
                );
            }
            if i == max_steps_per_epoch - 1 {
                panic!(
                    "State Pruning Test: Max simulation steps reached for epoch \
                     {current_epoch_num}"
                );
            }
        }
        assert!(
            harness.all_nodes_decided_epoch(current_epoch_num),
            "State Pruning Test: Not all nodes decided on epoch {current_epoch_num}"
        );
        println!("State Pruning Test: Epoch {current_epoch_num} finalized.");
    }

    // After all epochs, check pruning.
    // current_hbbft_epoch on nodes should be num_epochs_to_run.
    // max_finalized_epoch_num = num_epochs_to_run - 1.
    // oldest_retained_stated_epoch = max_finalized_epoch_num - (epochs_to_retain -
    // 1) = (num_epochs_to_run - 1) -
    // epochs_to_retain + 1                               = num_epochs_to_run -
    // epochs_to_retain
    let expected_oldest_retained_epoch = num_epochs_to_run - epochs_to_retain;

    for node_id in &node_ids {
        let node = harness.nodes.get(node_id).unwrap();
        assert_eq!(
            node.hbbft.current_epoch(),
            num_epochs_to_run,
            "Node {node_id} is on an unexpected current_epoch after test completion."
        );
        let (_, _, oldest_decided_in_map) = node
            .hbbft
            .get_oldest_retained_epochs_for_testing();
        assert_eq!(
            oldest_decided_in_map,
            Some(expected_oldest_retained_epoch),
            "Node {node_id}: Oldest decided epoch in map is incorrect. Pruning failed or acted \
             unexpectedly."
        );

        let (_, _, _, _, _, decided_map_len) = node
            .hbbft
            .get_internal_map_epoch_counts_for_testing();
        assert_eq!(
            decided_map_len, epochs_to_retain as usize,
            "Node {node_id}: `decided_epoch_outputs` map has an unexpected number of entries. \
             Expected {epochs_to_retain}, got {decided_map_len}"
        );
    }
    println!(
        "State Pruning Test: Pruning verified successfully for {epochs_to_retain} retained epochs \
         after {num_epochs_to_run} epochs run."
    );
}

#[test]
fn fault_tolerance_f_crashed_nodes() {
    let num_nodes = 4;
    let f = (num_nodes - 1) / 3; // f=1 for N=4
    assert_eq!(f, 1, "This test is designed for N=4, F=1");

    let node_ids: Vec<NodeId> = (0..num_nodes as NodeId).collect();
    let mut harness = TestHarness::new(node_ids.clone());

    // --- Epoch 0: All nodes are honest and propose ---
    let epoch0: EpochNum = DEFAULT_EPOCH;
    println!("Fault Tolerance Test: Starting Epoch 0 (all honest)");
    let mut expected_batches_e0_set = HashSet::new();
    for &proposer_id in &node_ids {
        let tx_content: u8 = proposer_id as u8; // Unique content
        let tx: Transaction = vec![tx_content];
        let batch = Batch::new(vec![tx]);
        expected_batches_e0_set.insert((proposer_id, batch.clone()));
        harness
            .propose_on_node(proposer_id, epoch0, vec![vec![tx_content]])
            .unwrap();
    }

    let max_steps_e0 = 500;
    for i in 0..max_steps_e0 {
        if harness.all_nodes_decided_epoch(epoch0)
            && node_ids.iter().all(|id| {
                harness
                    .finalized_outputs
                    .get(id)
                    .is_some_and(|e_map| {
                        e_map
                            .get(&epoch0)
                            .is_some_and(|b_vec| b_vec.len() == num_nodes)
                    })
            })
        {
            break;
        }
        if !harness.step() && harness.network_queue.is_empty() && harness.timers.is_empty() {
            panic!("Fault Tolerance Test: Epoch 0 stalled at step {i}");
        }
        if i == max_steps_e0 - 1 {
            panic!("Fault Tolerance Test: Epoch 0 max steps reached");
        }
    }
    assert!(harness.all_nodes_decided_epoch(epoch0), "Epoch 0 did not finalize for all nodes");
    println!("Fault Tolerance Test: Epoch 0 finalized by all nodes.");

    // --- Crash F nodes ---
    let nodes_to_crash: Vec<NodeId> = node_ids
        .iter()
        .take(f)
        .copied()
        .collect();
    harness.crash_nodes(&nodes_to_crash);
    let correct_node_ids: Vec<NodeId> = node_ids
        .iter()
        .filter(|id| !nodes_to_crash.contains(id))
        .copied()
        .collect();
    assert_eq!(correct_node_ids.len(), num_nodes - f);

    // --- Epoch 1: F nodes are crashed, N-F nodes propose ---
    let epoch1: EpochNum = epoch0 + 1;
    println!("Fault Tolerance Test: Starting Epoch 1 ({f} nodes crashed: {nodes_to_crash:?})");

    let mut expected_batches_e1_set = HashSet::new();
    for &proposer_id in &correct_node_ids {
        // Ensure correct nodes are at epoch1
        assert_eq!(
            harness
                .nodes
                .get(&proposer_id)
                .unwrap()
                .hbbft
                .current_epoch(),
            epoch1,
            "Node {proposer_id} not at epoch {epoch1}"
        );

        let tx_content: u8 = proposer_id as u8 + 10; // Unique content for epoch 1
        let tx: Transaction = vec![tx_content];
        let batch = Batch::new(vec![tx.clone()]);
        expected_batches_e1_set.insert((proposer_id, batch.clone()));

        // Submit the transaction to the node's pending buffer first
        harness.submit_tx_to_node(proposer_id, tx);

        harness
            .prepare_and_propose_on_node(proposer_id, epoch1) // Use prepare_and_propose for auto-proposal
            .unwrap();
    }

    let max_steps_e1 = 1000; // Give more steps due to timeouts for crashed nodes
    for i in 0..max_steps_e1 {
        if harness.all_correct_nodes_decided_epoch(epoch1)
            && correct_node_ids.iter().all(|id| {
                harness
                    .finalized_outputs
                    .get(id)
                    .is_some_and(|e_map| {
                        e_map
                            .get(&epoch1)
                            .is_some_and(|b_vec| b_vec.len() == num_nodes - f)
                    })
            })
        {
            break;
        }
        if !harness.step() && harness.network_queue.is_empty() && harness.timers.is_empty() {
            // Check if only crashed nodes' timers are left, or if correct nodes are
            // genuinely stalled
            let mut active_timers_for_correct_nodes = false;
            for expiries in harness.timers.values() {
                for expiry in expiries {
                    let (timer_epoch, timer_proposer) = match expiry {
                        TimerExpiry::AbaRoundTimeout { epoch, batch_proposer_id, .. } => {
                            (*epoch, *batch_proposer_id)
                        }
                        TimerExpiry::RbcTimeout { epoch, proposer_id } => (*epoch, *proposer_id),
                    };
                    if timer_epoch == epoch1 && !nodes_to_crash.contains(&timer_proposer) {
                        active_timers_for_correct_nodes = true;
                        break;
                    }
                }
                if active_timers_for_correct_nodes {
                    break;
                }
            }
            if !active_timers_for_correct_nodes && harness.network_queue.is_empty() {
                if harness.all_correct_nodes_decided_epoch(epoch1) {
                    break;
                } // They might have decided just now
                panic!("Fault Tolerance Test: Epoch 1 stalled for correct nodes at step {i}");
            }
        }
        if i == max_steps_e1 - 1 {
            panic!(
                "Fault Tolerance Test: Epoch 1 max steps reached. Correct nodes: \
                 {correct_node_ids:?}. Crashed: {nodes_to_crash:?}"
            );
        }
    }

    assert!(
        harness.all_correct_nodes_decided_epoch(epoch1),
        "Epoch 1 did not finalize for all correct nodes"
    );

    for &node_id in &correct_node_ids {
        let node_output_e1 = harness
            .finalized_outputs
            .get(&node_id)
            .unwrap()
            .get(&epoch1)
            .unwrap();
        assert_eq!(
            node_output_e1.len(),
            num_nodes - f,
            "Node {node_id} (correct) decided on wrong number of batches for epoch 1"
        );

        let decided_set_for_node: HashSet<(NodeId, Batch)> = node_output_e1
            .iter()
            .map(|db| (db.proposer_id, db.batch.clone()))
            .collect();
        assert_eq!(
            decided_set_for_node, expected_batches_e1_set,
            "Node {node_id} (correct) has incorrect batch set for epoch 1"
        );
    }

    for &crashed_node_id in &nodes_to_crash {
        assert!(
            !harness
                .finalized_outputs
                .get(&crashed_node_id)
                .is_some_and(|e_map| e_map.contains_key(&epoch1)),
            "Crashed node {crashed_node_id} should not have finalized epoch 1"
        );
    }

    println!(
        "Fault Tolerance Test: Epoch 1 finalized by correct nodes with {f} nodes crashed. Passed."
    );
}

#[test]
fn rbc_proposer_equivocation() {
    let num_nodes = 4;
    let node_ids: Vec<NodeId> = (0..num_nodes as NodeId).collect();
    // Use DummyCommonCoin for this test to make ABA behavior more predictable,
    // especially for the equivocator's own ABA instance.
    let mut harness =
        TestHarness::new_with_config_modifier(node_ids.clone(), |num_nodes, f, id| {
            honeybadger::testing::create_dummy_config(num_nodes, f, id)
        });

    let epoch_to_test: EpochNum = DEFAULT_EPOCH;
    let equivocating_proposer_id: NodeId = 0;

    println!(
        "RBC Equivocation Test: Proposer {equivocating_proposer_id} will equivocate in epoch \
         {epoch_to_test}."
    );

    // --- Craft equivocating VAL messages from equivocating_proposer_id ---
    let tx_v1: Transaction = vec![1, 0, 1];
    let batch_v1 = Batch::new(vec![tx_v1]);
    let val_message_v1 = honeybadger::HoneyBadgerMessage::Rbc(honeybadger::RbcMessage {
        pub_proposer_id: equivocating_proposer_id,
        pub_epoch: epoch_to_test,
        pub_content: honeybadger::RbcMessageContent::Val(batch_v1.clone()),
        pub_sender_id: equivocating_proposer_id,
    });

    let tx_v2: Transaction = vec![2, 0, 2];
    let batch_v2 = Batch::new(vec![tx_v2]);
    let val_message_v2 = honeybadger::HoneyBadgerMessage::Rbc(honeybadger::RbcMessage {
        pub_proposer_id: equivocating_proposer_id,
        pub_epoch: epoch_to_test,
        pub_content: honeybadger::RbcMessageContent::Val(batch_v2.clone()),
        pub_sender_id: equivocating_proposer_id,
    });

    // Inject both VAL messages from the equivocator
    harness.inject_message(val_message_v1);
    harness.inject_message(val_message_v2);

    // --- Honest nodes propose their batches ---
    let mut expected_decided_batches_set = HashSet::new();
    for &node_id in &node_ids {
        if node_id == equivocating_proposer_id {
            // Have the equivocator make a "normal" proposal first (e.g., empty).
            // This ensures its HBBFT instance initializes RBC/ABA for itself for this
            // epoch. Its own RBC for (0,0) will time out. Its ABA for (0,0)
            // will start false.
            harness
                .propose_on_node(node_id, epoch_to_test, vec![]) // Propose empty batch
                .expect("Propose for equivocator failed");
        } else {
            // Honest proposers
            let tx_content: u8 = node_id as u8 + 50;
            let tx_honest: Transaction = vec![tx_content];
            let batch_honest = Batch::new(vec![tx_honest.clone()]);
            expected_decided_batches_set.insert((node_id, batch_honest.clone()));
            harness
                .propose_on_node(node_id, epoch_to_test, vec![tx_honest])
                .unwrap_or_else(|e| panic!("Propose failed for honest node {node_id}: {e:?}"));
        }
    }

    // For honest nodes, they will decide on batch_v1 from the equivocator.
    let mut expected_decided_batches_set_for_honest_nodes = expected_decided_batches_set.clone();
    expected_decided_batches_set_for_honest_nodes
        .insert((equivocating_proposer_id, batch_v1.clone()));

    // For the equivocator node, its own RBC for its proposal will time out,
    // so it will exclude its own batch, even if its ABA for itself decides true.
    // So, expected_decided_batches_set (without equivocator's batch_v1) is correct
    // for node 0.

    // --- Run simulation ---
    let max_steps = 1500; // Equivocation might cause more timeouts
    for i in 0..max_steps {
        // Wait until all nodes have decided the epoch. The number of batches might
        // differ.
        if harness.all_nodes_decided_epoch(epoch_to_test) {
            break;
        }
        if !harness.step() && harness.network_queue.is_empty() && harness.timers.is_empty() {
            panic!("RBC Equivocation Test: Simulation stalled at step {i}");
        }
        if i == max_steps - 1 {
            panic!("RBC Equivocation Test: Max simulation steps reached");
        }
    }

    // --- Assertions ---
    assert!(
        harness.all_nodes_decided_epoch(epoch_to_test),
        "RBC Equivocation Test: Not all nodes decided on epoch {epoch_to_test}"
    );

    for &node_id_check in &node_ids {
        let node_output_for_epoch = harness
            .finalized_outputs
            .get(&node_id_check)
            .unwrap()
            .get(&epoch_to_test)
            .unwrap();
        let decided_set_for_node: HashSet<(NodeId, Batch)> = node_output_for_epoch
            .iter()
            .map(|db| (db.proposer_id, db.batch.clone()))
            .collect();

        if node_id_check == equivocating_proposer_id {
            // Equivocator excludes its own batch due to its RBC timeout for its "empty"
            // proposal even if its ABA for itself decided true.
            assert_eq!(
                node_output_for_epoch.len(),
                num_nodes - 1,
                "Equivocating Node {node_id_check} decided on {count} batches, expected \
                 {expected_count}",
                count = node_output_for_epoch.len(),
                expected_count = num_nodes - 1
            );
            assert_eq!(
                decided_set_for_node, expected_decided_batches_set,
                "Equivocating Node {node_id_check} has incorrect set of decided batches"
            );
        } else {
            // Honest nodes include batch_v1 from the equivocator
            assert_eq!(
                node_output_for_epoch.len(),
                num_nodes,
                "Honest Node {node_id_check} decided on {count} batches, expected {expected_count}",
                count = node_output_for_epoch.len(),
                expected_count = num_nodes
            );
            assert_eq!(
                decided_set_for_node, expected_decided_batches_set_for_honest_nodes,
                "Honest Node {node_id_check} has incorrect set of decided batches"
            );
            assert!(
                node_output_for_epoch
                    .iter()
                    .any(|db| db.proposer_id == equivocating_proposer_id && db.batch == batch_v1)
            );
        }
    }
    println!(
        "RBC Equivocation Test: Passed. Honest nodes agreed on one version of equivocator's \
         batch; equivocator excluded its own."
    );
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
