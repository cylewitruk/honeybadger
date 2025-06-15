// This content is intended for tests/integration.rs

use honeybadger::testing::{DEFAULT_EPOCH, TestHarness};
use honeybadger::{EpochNum, NodeId};

mod integration_tests {
    use honeybadger::{Batch, Transaction};

    use super::*; // Ensure Transaction is in scope

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
                        "Simulation stalled at step {i} before all nodes decided epoch \
                         {epoch_to_test}",
                    );
                    for (id, node_data) in &harness.finalized_outputs {
                        eprintln!("Node {id}: finalized epochs {:?}", node_data.keys());
                    }
                    // Dump network queue and timers
                    eprintln!("Network queue size: {}", harness.network_queue.len());
                    for (time, timers) in &harness.timers {
                        eprintln!("Timers at {time:?}: {:?}", timers.len());
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
                "Node {node_id} decided on {} batches, expected 1",
                node_output.len()
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
}
