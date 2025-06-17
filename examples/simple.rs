use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use honeybadger::{
    CoinFactory, DecidedBatch, EpochNum, HbbftProcessOutput, HoneyBadgerBft,
    HoneyBadgerConfigBuilder, HoneyBadgerMessage, NodeId, TimerExpiry, TimerRequest, Transaction,
};

/// A simple demonstration of the HoneyBadgerBFT library.
///
/// This example simulates a small network of 4 nodes and runs them through
/// a single epoch where one node proposes a batch of transactions. It manually
/// handles message passing and timer events to illustrate the core API usage.
fn main() {
    println!("--- HoneyBadgerBFT Simple Example ---");

    // --- Configuration ---
    let num_nodes: usize = 4;
    let f: usize = (num_nodes - 1) / 3; // Max faulty nodes (N=4, F=1)
    let node_ids: Vec<NodeId> = (0..num_nodes as NodeId).collect();
    let all_node_ids_set: BTreeSet<NodeId> = node_ids.iter().copied().collect();

    // For this simple example, we'll use a DummyCommonCoin.
    // In a real application, you'd provide a secure common coin implementation.
    let coin_factory = CoinFactory::new(Arc::new(|_node_id, _epoch, _config| {
        Box::new(honeybadger::testing::DummyCommonCoin {})
    }));

    // Create configurations and HBBFT instances for each node
    let mut nodes: HashMap<NodeId, HoneyBadgerBft> = HashMap::new();
    for &id in &node_ids {
        let config = Arc::new(
            HoneyBadgerConfigBuilder::new(id, all_node_ids_set.clone(), f, coin_factory.clone())
                .build()
                .expect("Failed to build config"),
        );
        nodes.insert(id, HoneyBadgerBft::new(config.as_ref().clone(), 0));
    }
    println!("Initialized {num_nodes} HoneyBadgerBFT nodes.");

    // --- Simulation Setup ---
    let epoch_to_run: EpochNum = 0;
    let proposer_id: NodeId = 0;

    // Network simulation: a simple queue for messages
    let mut network_queue: VecDeque<HoneyBadgerMessage> = VecDeque::new();
    // Timer simulation: (expiry_time, node_id_timer_is_for, timer_event)
    // In a real app, the application's event loop would manage timers.
    let mut timer_queue: BTreeMap<Instant, Vec<(NodeId, TimerExpiry)>> = BTreeMap::new();
    let mut current_sim_time = Instant::now();

    // --- Propose a Batch ---
    let tx1: Transaction = vec![1, 2, 3];
    let tx2: Transaction = vec![4, 5, 6];
    let proposed_batch_txs = vec![tx1.clone(), tx2.clone()];

    if let Some(proposer_node) = nodes.get_mut(&proposer_id) {
        match proposer_node.propose_batch(proposed_batch_txs.clone(), epoch_to_run) {
            Ok(output) => process_hbbft_output(
                proposer_id,
                output,
                &mut network_queue,
                &mut timer_queue,
                current_sim_time,
                None, // No decided batches expected from just proposing
            ),
            Err(e) => eprintln!("Proposer {proposer_id} failed to propose: {e:?}"),
        }
        println!("Node {proposer_id} proposed a batch for epoch {epoch_to_run}.");
    }

    // --- Simulation Loop ---
    let max_simulation_steps = 1000;
    let mut decided_outputs: HashMap<NodeId, Vec<DecidedBatch>> = HashMap::new();

    for step in 0..max_simulation_steps {
        let mut progress_made_this_step = false;

        // 1. Process a message from the network queue
        if let Some(message) = network_queue.pop_front() {
            progress_made_this_step = true;
            // Broadcast message to all nodes (except sender, though HBBFT handles that)
            for (id, node) in nodes.iter_mut() {
                if *id != message.sender_id() || message.epoch() == epoch_to_run {
                    // Allow self-delivery for own proposals
                    match node.handle_message(message.clone()) {
                        Ok(output) => process_hbbft_output(
                            *id,
                            output,
                            &mut network_queue,
                            &mut timer_queue,
                            current_sim_time,
                            Some(&mut decided_outputs),
                        ),
                        Err(e) => eprintln!("Node {id} error handling message: {e:?}"),
                    }
                }
            }
        }

        // 2. Process expired timers
        // Iterate as long as the earliest timer is due
        while let Some((&expiry_time, _)) = timer_queue.iter().next() {
            // Peek at the first element
            if expiry_time <= current_sim_time {
                progress_made_this_step = true;
                // Actually remove the entry. This is safe because pop_first() modifies the map
                // and the next iter().next() will see the updated state.
                if let Some((_popped_expiry_time, mut expired_events_for_this_instant)) =
                    timer_queue.pop_first()
                {
                    for (node_id_for_timer, expiry_event) in
                        expired_events_for_this_instant.drain(..)
                    {
                        if let Some(node) = nodes.get_mut(&node_id_for_timer) {
                            match node.handle_timer_expiry(expiry_event) {
                                Ok(output) => process_hbbft_output(
                                    node_id_for_timer,
                                    output,
                                    &mut network_queue,
                                    &mut timer_queue, // Pass the main timer_queue
                                    current_sim_time,
                                    Some(&mut decided_outputs),
                                ),
                                Err(e) => eprintln!(
                                    "Node {node_id_for_timer} error handling timer: {e:?}"
                                ),
                            }
                        }
                    }
                }
            } else {
                break; // No more timers expired at current_sim_time
            }
        }
        // New timers are added directly to `timer_queue` by `process_hbbft_output`

        // Check if all nodes have decided for the epoch
        if decided_outputs.len() == num_nodes
            && decided_outputs
                .values()
                .all(|v| !v.is_empty() && v[0].epoch == epoch_to_run)
        {
            println!("\n--- Epoch {epoch_to_run} Finalized by All Nodes (Step {step}) ---");
            break;
        }

        // If no messages and no timers processed, advance time to next timer event or
        // break
        if !progress_made_this_step {
            if let Some((&next_timer_time, _)) = timer_queue.iter().next() {
                if next_timer_time > current_sim_time {
                    current_sim_time = next_timer_time;
                } else {
                    // Should not happen if logic is correct, but as a fallback
                    current_sim_time += Duration::from_millis(10); // Small tick if stuck
                }
            } else if network_queue.is_empty() {
                println!("Simulation ended: No more messages or timers. Step {step}.");
                break; // No more actions possible
            }
        }
        if step == max_simulation_steps - 1 {
            println!("Max simulation steps reached.");
        }
    }

    // --- Output Results ---
    for (node_id, batches) in decided_outputs.iter() {
        println!("Node {node_id} decided on epoch {epoch_to_run} with batches:");
        for decided_batch in batches {
            println!(
                "  - Proposer: {}, Batch Transactions: {:?}",
                decided_batch.proposer_id, decided_batch.batch.transactions
            );
            // In a real app, you'd compare `decided_batch.batch` with the expected batch.
            if decided_batch.proposer_id == proposer_id {
                assert_eq!(
                    decided_batch.batch.transactions, proposed_batch_txs,
                    "Batch content mismatch for node {node_id}"
                );
            }
        }
        assert_eq!(
            batches.len(),
            1,
            "Node {node_id} should have decided on 1 batch for this scenario."
        );
    }
    if decided_outputs.len() != num_nodes {
        eprintln!("Error: Not all nodes finalized the epoch.");
    }

    println!("\n--- HoneyBadgerBFT Simple Example End ---");
}

/// Helper function to process the output of an HBBFT operation.
/// It adds messages to the network queue and sets timers.
fn process_hbbft_output(
    node_id: NodeId, // The node that produced this output
    output: HbbftProcessOutput,
    network_queue: &mut VecDeque<HoneyBadgerMessage>,
    timer_queue: &mut BTreeMap<Instant, Vec<(NodeId, TimerExpiry)>>,
    current_sim_time: Instant,
    decided_outputs_opt: Option<&mut HashMap<NodeId, Vec<DecidedBatch>>>,
) {
    for msg in output.messages_to_send {
        network_queue.push_back(msg);
    }
    for timer_req in output.timer_requests_to_set {
        let (duration_ms, expiry_event_template) = match timer_req {
            TimerRequest::SetAbaRoundTimeout {
                epoch,
                batch_proposer_id,
                round,
                suggested_duration_ms,
            } => (
                suggested_duration_ms,
                TimerExpiry::AbaRoundTimeout {
                    epoch,
                    batch_proposer_id,
                    round,
                },
            ),
            TimerRequest::SetRbcTimeout {
                epoch,
                proposer_id,
                suggested_duration_ms,
            } => (suggested_duration_ms, TimerExpiry::RbcTimeout { epoch, proposer_id }),
        };
        let expiry_time = current_sim_time + Duration::from_millis(duration_ms);
        timer_queue
            .entry(expiry_time)
            .or_default()
            .push((node_id, expiry_event_template));
    }
    if let Some(decided_outputs) = decided_outputs_opt {
        if !output.newly_decided_batches.is_empty() {
            decided_outputs
                .entry(node_id)
                .or_default()
                .extend(output.newly_decided_batches);
        }
    }
}
