use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::aba::CommonCoin;
use crate::config::{CoinFactory, HoneyBadgerConfig, HoneyBadgerConfigBuilder};
use crate::types::{CoinOutput, CoinShare, NodeIdentifier};
use crate::{
    AbaRound, DecidedBatch, EpochNum, HbbftError, HbbftProcessOutput, HoneyBadgerBft,
    HoneyBadgerMessage, TimerExpiry, TimerRequest, Transaction,
};

// usize is generally not appropriate for NodeIdentifier in a real system,
// but for testing purposes, we can use it as a simple identifier.
impl NodeIdentifier for usize {}

#[derive(Debug)]
pub struct DummyCommonCoin;

impl CommonCoin for DummyCommonCoin {
    type NodeId = usize;
    fn request_coin_value(&mut self, _epoch: EpochNum, round: AbaRound) -> CoinOutput {
        // No shares to send, coin is immediately known
        CoinOutput {
            share_to_broadcast: None,
            decided_value: Some(round % 2 == 0),
        }
    }

    fn submit_coin_share(
        &mut self,
        _epoch: EpochNum,
        _round: AbaRound,
        _share_content: CoinShare,
        _sender_id: Self::NodeId,
    ) -> CoinOutput {
        // Dummy coin doesn't process shares or decide based on them
        CoinOutput {
            share_to_broadcast: None,
            decided_value: None,
        }
    }

    fn get_coin_for_round_fallback(&mut self, _epoch: EpochNum, round: AbaRound) -> bool {
        round % 2 == 0 // Fallback is the same predictable logic
    }
}

/// A mock common coin that requires F+1 shares to decide.
/// The coin value itself is deterministic based on the round number.
#[derive(Debug)]
pub struct MockInteractiveCoin<NodeId> {
    config: Arc<HoneyBadgerConfig<NodeId>>, // To access F+1 threshold
    own_id: NodeId,
    // Stores received shares for each round: round -> set of sender_ids
    received_shares_for_round: HashMap<AbaRound, HashSet<NodeId>>,
}

impl<NodeId> MockInteractiveCoin<NodeId> {
    pub fn new(config: Arc<HoneyBadgerConfig<NodeId>>, own_id: NodeId) -> Self {
        MockInteractiveCoin {
            config,
            own_id,
            received_shares_for_round: HashMap::new(),
        }
    }
}

impl<NodeId: NodeIdentifier> CommonCoin for MockInteractiveCoin<NodeId> {
    type NodeId = NodeId;
    fn request_coin_value(&mut self, _epoch: EpochNum, round: AbaRound) -> CoinOutput {
        // Generate a unique "share" for this node and round
        let own_share = format!("share_ep{}_r{}_id{}", _epoch, round, self.own_id).into_bytes();
        CoinOutput {
            share_to_broadcast: Some(own_share),
            decided_value: None, // No immediate decision, shares must be exchanged
        }
    }

    fn submit_coin_share(
        &mut self,
        _epoch: EpochNum,
        round: AbaRound,
        _share_content: CoinShare, // We don't inspect the content for this mock
        sender_id: NodeId,
    ) -> CoinOutput {
        let shares_for_this_round = self
            .received_shares_for_round
            .entry(round)
            .or_default();
        shares_for_this_round.insert(sender_id);

        let decided_value = if shares_for_this_round.len() >= self.config.low_threshold() {
            // F+1 shares
            Some(round % 2 == 0) // Deterministic coin outcome
        } else {
            None // Not enough shares yet
        };

        CoinOutput {
            share_to_broadcast: None, // submit_coin_share in this mock doesn't trigger new shares
            decided_value,
        }
    }

    fn get_coin_for_round_fallback(&mut self, _epoch: EpochNum, round: AbaRound) -> bool {
        round % 2 == 0 // Fallback is the same predictable logic
    }
}

/// Creates a default `HoneyBadgerConfig` using `DummyCommonCoin` for testing
/// purposes.
pub fn create_dummy_config(n: usize, f: usize, own_id: usize) -> Arc<HoneyBadgerConfig<usize>> {
    let all_node_ids = (0..n).collect::<BTreeSet<usize>>();
    let dummy_coin_factory = CoinFactory::new(Arc::new(|_node_id, _epoch, _config| {
        Box::new(DummyCommonCoin {}) as Box<dyn CommonCoin<NodeId = usize>>
    }));

    Arc::new(
        HoneyBadgerConfigBuilder::new(own_id, all_node_ids, f, dummy_coin_factory)
            .epochs_to_retain_state(3)
            .default_aba_round_timeout(Duration::from_millis(1_000))
            .default_rbc_instance_timeout(Duration::from_millis(2_000))
            .aba_max_future_round_window(5)
            .build()
            .unwrap(),
    )
}

/// Creates a `HoneyBadgerConfig` using `MockInteractiveCoin` for integration
/// testing.
pub fn create_interactive_mock_config<NodeId>(
    n: usize,
    f: usize,
    own_id: NodeId,
) -> Arc<HoneyBadgerConfig<NodeId>>
where
    NodeId: NodeIdentifier + From<usize> + 'static,
{
    let all_node_ids: BTreeSet<NodeId> = (0..n).map(NodeId::from).collect();
    let interactive_coin_factory =
        CoinFactory::new(Arc::new(move |node_id, _epoch, config_for_coin| {
            Box::new(MockInteractiveCoin::new(config_for_coin, node_id))
                as Box<dyn CommonCoin<NodeId = NodeId>>
        }));

    Arc::new(
        HoneyBadgerConfigBuilder::new(own_id, all_node_ids.clone(), f, interactive_coin_factory)
            .build()
            .unwrap(),
    )
}

/// Creates a default `HoneyBadgerConfig` for testing purposes.
/// Uses `DummyCommonCoin` and sets explicit default values consistent
/// across tests.
pub fn create_test_config(n: usize, f: usize, own_id: usize) -> Arc<HoneyBadgerConfig<usize>> {
    let all_node_ids = (0..n).collect::<BTreeSet<usize>>();
    let dummy_coin_factory = CoinFactory::new(Arc::new(|_node_id, _epoch, _config| {
        Box::new(DummyCommonCoin {}) as Box<dyn CommonCoin<NodeId = usize>>
    }));

    Arc::new(
        HoneyBadgerConfigBuilder::new(own_id, all_node_ids, f, dummy_coin_factory)
            .epochs_to_retain_state(3)
            .default_aba_round_timeout(Duration::from_millis(1_000))
            .default_rbc_instance_timeout(Duration::from_millis(2_000))
            .aba_max_future_round_window(5)
            .build()
            .unwrap(),
    )
}

pub const DEFAULT_EPOCH: EpochNum = 0;

pub struct SimulatedNode<NodeId> {
    pub hbbft: HoneyBadgerBft<NodeId>,
    pub id: NodeId,
}

pub struct TestHarness<NodeId> {
    pub nodes: HashMap<NodeId, SimulatedNode<NodeId>>,
    /// Global message queue for simplicity. Messages are (sender,
    /// message_content). A real harness might have per-node inboxes.
    pub network_queue: VecDeque<HoneyBadgerMessage<NodeId>>,
    /// Timers: (expiry_time, specific_timer_expiry_event)
    pub timers: BTreeMap<Instant, Vec<TimerExpiry<NodeId>>>,
    pub current_time: Instant,
    /// Tracks which epoch outputs have been collected from each node
    pub finalized_outputs: HashMap<NodeId, HashMap<EpochNum, Vec<DecidedBatch<NodeId>>>>,
    pub num_nodes: usize,
    pub crashed_nodes: HashSet<NodeId>,
}

impl<NodeId> TestHarness<NodeId>
where
    NodeId: NodeIdentifier + From<usize> + 'static,
{
    pub fn new(node_ids: Vec<NodeId>) -> Self {
        let mut nodes = HashMap::new();
        let num_nodes = node_ids.len();
        let f = (num_nodes - 1) / 3; // Max faulty nodes

        for &id in &node_ids {
            // For integration tests, we'll use the MockInteractiveCoin
            let config = create_interactive_mock_config(num_nodes, f, id);

            let hbbft = HoneyBadgerBft::new(config.as_ref().clone(), DEFAULT_EPOCH);
            nodes.insert(id, SimulatedNode { hbbft, id });
        }

        TestHarness {
            nodes,
            network_queue: VecDeque::new(),
            timers: BTreeMap::new(),
            current_time: Instant::now(),
            finalized_outputs: HashMap::new(),
            num_nodes,
            crashed_nodes: HashSet::new(),
        }
    }

    pub fn new_with_config_modifier(
        node_ids: Vec<NodeId>,
        config_modifier: impl Fn(usize, usize, NodeId) -> Arc<HoneyBadgerConfig<NodeId>>,
    ) -> Self {
        let mut nodes = HashMap::new();
        let num_nodes = node_ids.len();
        let f = (num_nodes - 1) / 3; // Max faulty nodes

        for &id in &node_ids {
            // For integration tests, we'll use the MockInteractiveCoin
            let config = config_modifier(num_nodes, f, id);

            let hbbft = HoneyBadgerBft::new(config.as_ref().clone(), DEFAULT_EPOCH);
            nodes.insert(id, SimulatedNode { hbbft, id });
        }

        TestHarness {
            nodes,
            network_queue: VecDeque::new(),
            timers: BTreeMap::new(),
            current_time: Instant::now(),
            finalized_outputs: HashMap::new(),
            num_nodes,
            crashed_nodes: HashSet::new(),
        }
    }

    pub fn crash_nodes(&mut self, nodes_to_crash: &[NodeId]) {
        for &node_id in nodes_to_crash {
            self.crashed_nodes.insert(node_id);
            println!("[TestHarness] Node {node_id} marked as crashed.");
        }
    }

    /// Directly injects a message into the global network queue.
    pub fn inject_message(&mut self, message: HoneyBadgerMessage<NodeId>) {
        self.network_queue.push_back(message);
    }

    pub fn submit_tx_to_node(&mut self, node_id: NodeId, tx: Transaction) {
        if self.crashed_nodes.contains(&node_id) {
            return;
        }

        if let Some(node) = self.nodes.get_mut(&node_id) {
            match node.hbbft.submit_transaction(tx) {
                Ok(output) => self.process_hbbft_output(node_id, output),
                Err(e) => eprintln!("Error submitting tx to node {node_id}: {e:?}"),
            }
        }
    }

    pub fn propose_on_node(
        &mut self,
        node_id: NodeId,
        epoch: EpochNum,
        batch_txs: Vec<Transaction>,
    ) -> Result<(), HbbftError<NodeId>> {
        if self.crashed_nodes.contains(&node_id) {
            return Ok(());
        }

        if let Some(node) = self.nodes.get_mut(&node_id) {
            let output = node
                .hbbft
                .propose_batch(batch_txs, epoch)?;
            self.process_hbbft_output(node_id, output);
        }
        Ok(())
    }

    pub fn prepare_and_propose_on_node(
        &mut self,
        node_id_to_propose: NodeId,
        target_epoch: EpochNum,
    ) -> Result<(), HbbftError<NodeId>> {
        if self
            .crashed_nodes
            .contains(&node_id_to_propose)
        {
            return Ok(());
        }

        let mut outputs_to_process: Vec<(NodeId, HbbftProcessOutput<NodeId>)> = Vec::new();

        if let Some(node) = self.nodes.get_mut(&node_id_to_propose) {
            // Ensure the node is at the correct epoch for proposing.
            while node.hbbft.current_epoch() < target_epoch {
                let advance_output = node
                    .hbbft
                    .prepare_and_propose_current_epoch()?;
                // Collect output to process after the borrow on `node` is released
                outputs_to_process.push((node_id_to_propose, advance_output));
            }

            if node.hbbft.current_epoch() == target_epoch {
                let output = node
                    .hbbft
                    .prepare_and_propose_current_epoch()?;
                outputs_to_process.push((node_id_to_propose, output));
            } else {
                eprintln!(
                    "Node {} is at epoch {} but expected to propose for epoch {}",
                    node_id_to_propose,
                    node.hbbft.current_epoch(),
                    target_epoch
                );
            }
        }

        // Process collected outputs after the mutable borrow on self.nodes (via node)
        // is dropped
        for (id, output) in outputs_to_process {
            self.process_hbbft_output(id, output);
        }
        Ok(())
    }

    pub fn process_hbbft_output(
        &mut self,
        producer_node_id: NodeId,
        output: HbbftProcessOutput<NodeId>,
    ) {
        for msg in output.messages_to_send {
            self.network_queue.push_back(msg);
        }
        for timer_req in output.timer_requests_to_set {
            let duration = match timer_req {
                TimerRequest::SetAbaRoundTimeout { suggested_duration, .. } => suggested_duration,
                TimerRequest::SetRbcTimeout { suggested_duration, .. } => suggested_duration,
            };
            let expiry_time = self.current_time + duration;
            let expiry_event = match timer_req {
                TimerRequest::SetAbaRoundTimeout {
                    epoch,
                    batch_proposer_id,
                    round,
                    ..
                } => TimerExpiry::AbaRoundTimeout {
                    epoch,
                    batch_proposer_id,
                    round,
                },
                TimerRequest::SetRbcTimeout { epoch, proposer_id, .. } => {
                    TimerExpiry::RbcTimeout { epoch, proposer_id }
                }
            };
            self.timers
                .entry(expiry_time)
                .or_default()
                .push(expiry_event);
        }
        if !output.newly_decided_batches.is_empty() {
            let node_finalized = self
                .finalized_outputs
                .entry(producer_node_id)
                .or_default();
            for decided_batch in output.newly_decided_batches {
                // Consumes from output
                node_finalized
                    .entry(decided_batch.epoch)
                    .or_default()
                    .push(decided_batch);
            }
        }
    }

    /// Delivers one message from the queue. Returns true if a message was
    /// delivered.
    pub fn deliver_one_message(&mut self) -> bool {
        let mut outputs_to_process: Vec<(NodeId, HbbftProcessOutput<NodeId>)> = Vec::new();

        if let Some(message) = self.network_queue.pop_front() {
            if self
                .crashed_nodes
                .contains(&message.sender_id())
            {
                // Message from a crashed node, discard it.
                // eprintln!("[TestHarness] Discarding message from crashed sender {}",
                // message.sender_id());
                return true; // Progress was made (message discarded)
            }

            // Deliver to all nodes (except sender, though HBBFT should handle that)
            // In a real network, you'd route to specific recipients if applicable,
            // or broadcast. HBBFT messages are generally broadcast.
            for (id, node) in self.nodes.iter_mut() {
                if self.crashed_nodes.contains(id) {
                    continue;
                } // Don't deliver to crashed nodes

                // Clone message for each recipient
                match node
                    .hbbft
                    .handle_message(message.clone())
                {
                    Ok(output) => outputs_to_process.push((*id, output)),
                    Err(e) => eprintln!("Error handling message for node {id}: {e:?}"),
                }
            }
            // Process outputs after iterating through nodes
            for (id, output) in outputs_to_process {
                self.process_hbbft_output(id, output);
            }

            true
        } else {
            false
        }
    }

    /// Advances time and processes any expired timers.
    pub fn process_timers(&mut self) -> bool {
        let mut processed_timer = false;
        let mut outputs_to_process: Vec<(NodeId, HbbftProcessOutput<NodeId>)> = Vec::new();

        // Find the next timer to expire
        if let Some((&expiry_time, _)) = self.timers.iter().next() {
            if expiry_time <= self.current_time {
                // Remove and process
                if let Some((_, expiries)) = self.timers.pop_first() {
                    for expiry_event in expiries {
                        processed_timer = true;
                        // Deliver expiry to the relevant node(s)
                        // For ABA, it's specific to batch_proposer_id's ABA instance on each node.
                        // For RBC, it's specific to proposer_id's RBC instance on each node.
                        // The HBBFT instance itself will route it internally.
                        for (id, node) in self.nodes.iter_mut() {
                            if self.crashed_nodes.contains(id) {
                                continue;
                            } // Don't deliver timer expiries to crashed nodes
                            match node
                                .hbbft
                                .handle_timer_expiry(expiry_event.clone())
                            {
                                Ok(output) => outputs_to_process.push((*id, output)),
                                Err(e) => {
                                    eprintln!("Error handling timer expiry for node {id}: {e:?}")
                                }
                            }
                        }
                    }
                }
            }
        }
        // Process outputs after iterating through nodes (if any timers expired)
        for (id, output) in outputs_to_process {
            self.process_hbbft_output(id, output);
        }
        processed_timer
    }

    /// Runs the simulation until a condition is met or a number of steps.
    /// Returns true if progress was made (message delivered or timer
    /// processed).
    pub fn step(&mut self) -> bool {
        let mut progress_made = false;
        if self.deliver_one_message() {
            progress_made = true;
        }
        if self.process_timers() {
            // Process timers after message delivery
            progress_made = true;
        }
        if !progress_made && !self.timers.is_empty() {
            // If no messages and timers exist, advance time to the next timer event.
            if let Some((&next_timer_time, _)) = self.timers.iter().next() {
                if next_timer_time > self.current_time {
                    self.current_time = next_timer_time;
                    // After advancing time, try processing timers again in the next step call
                    // This ensures that if advancing time makes a timer current, it gets processed.
                    // Or, we can call self.process_timers() here.
                    if self.process_timers() {
                        progress_made = true;
                    }
                }
            }
        }
        progress_made
    }

    pub fn all_nodes_decided_epoch(&self, epoch: EpochNum) -> bool {
        // This method now implicitly checks only non-crashed nodes if they are expected
        // to decide. For a more general "all *active* nodes decided", the
        // calling test needs to manage expectations. Here, we check if *all
        // originally configured* nodes have an entry, which might be too strict
        // if some are crashed and not expected to decide.
        // Let's adjust to check only non-crashed nodes.
        for node_id in self.nodes.keys() {
            if self.crashed_nodes.contains(node_id) {
                continue;
            }
            match self.finalized_outputs.get(node_id) {
                Some(epochs_data) => {
                    if !epochs_data.contains_key(&epoch) {
                        return false; // This node hasn't decided this epoch
                    }
                }
                None => return false, // This node has no finalized data
            }
        }
        true
    }

    pub fn all_correct_nodes_decided_epoch(&self, epoch: EpochNum) -> bool {
        for node_id in self.nodes.keys() {
            if self.crashed_nodes.contains(node_id) {
                continue; // Skip crashed nodes
            }
            match self.finalized_outputs.get(node_id) {
                Some(epochs_data) => {
                    if !epochs_data.contains_key(&epoch) {
                        return false;
                    }
                }
                None => return false, // A correct node has no finalized data at all
            }
        }
        // Ensure at least one correct node decided if all are crashed (edge case)
        self.nodes
            .keys()
            .any(|id| !self.crashed_nodes.contains(id))
    }
}
